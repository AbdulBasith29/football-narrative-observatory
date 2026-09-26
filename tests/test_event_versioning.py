import json
from datetime import datetime, timezone
import pytest
from unittest.mock import MagicMock
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))

from sync_config import sync_events, normalize_terms_list, normalize_datetime_utc


class MockSnowflakeDatabase:
    def __init__(self):
        self.events = {}  # ext_id -> dict
        self.event_versions = []  # list of dicts
        self.event_windows = []  # list of dicts
        self.committed = False

    def get_cursor(self):
        cursor = MagicMock()
        cursor.execute.side_effect = self.execute
        cursor.fetchone.side_effect = self.fetchone
        cursor.fetchall.side_effect = self.fetchall
        return cursor

    def get_connection(self):
        conn = MagicMock()
        conn.cursor.return_value = self.get_cursor()
        conn.commit.side_effect = self.commit
        return conn

    def commit(self):
        self.committed = True

    def execute(self, sql, params=()):
        self._last_result = None
        sql_clean = " ".join(sql.strip().split())

        # 1. SELECT event_key FROM CORE.DIM_EVENT
        if "FROM CORE.DIM_EVENT WHERE external_event_id" in sql_clean:
            ext_id = params[0]
            if ext_id in self.events:
                self._last_result = [(self.events[ext_id]["event_key"],)]
            else:
                self._last_result = []

        # 2. INSERT INTO CORE.DIM_EVENT_VERSION
        elif "INSERT INTO CORE.DIM_EVENT_VERSION" in sql_clean:
            ev_key, e_key, name, occ_at, rat, e_terms, b_terms, val_from = params
            self.event_versions.append({
                "event_version_key": ev_key,
                "event_key": e_key,
                "event_name": name,
                "occurred_at": occ_at,
                "inclusion_rationale": rat,
                "event_terms": e_terms,
                "baseline_terms": b_terms,
                "valid_from": val_from,
                "valid_to": None,
                "is_current": True
            })

        # 3. INSERT INTO CORE.DIM_EVENT
        elif "INSERT INTO CORE.DIM_EVENT (" in sql_clean:
            event_key, ext_id, event_type = params
            self.events[ext_id] = {
                "event_key": event_key,
                "external_event_id": ext_id,
                "event_type": event_type
            }

        # 3. SELECT event_version_key ... FROM CORE.DIM_EVENT_VERSION WHERE event_key = %s AND is_current = TRUE
        elif "FROM CORE.DIM_EVENT_VERSION WHERE event_key = %s AND is_current = TRUE" in sql_clean:
            event_key = params[0]
            current_vers = [
                v for v in self.event_versions
                if v["event_key"] == event_key and v["is_current"] is True
            ]
            if current_vers:
                v = current_vers[0]
                self._last_result = [(
                    v["event_version_key"],
                    v["event_name"],
                    v["occurred_at"],
                    v["inclusion_rationale"],
                    v["event_terms"],
                    v["baseline_terms"]
                )]
            else:
                self._last_result = []

        # 4. UPDATE CORE.DIM_EVENT_VERSION SET is_current = FALSE, valid_to = %s WHERE event_version_key = %s
        elif "UPDATE CORE.DIM_EVENT_VERSION SET is_current = FALSE" in sql_clean:
            valid_to, ev_key = params
            for v in self.event_versions:
                if v["event_version_key"] == ev_key:
                    v["is_current"] = False
                    v["valid_to"] = valid_to


        # 6. SELECT window_key FROM CORE.DIM_EVENT_WINDOW
        elif "FROM CORE.DIM_EVENT_WINDOW WHERE event_version_key = %s AND window_type = %s" in sql_clean:
            ev_key, w_type = params
            matching = [
                w for w in self.event_windows
                if w["event_version_key"] == ev_key and w["window_type"] == w_type
            ]
            if matching:
                self._last_result = [(matching[0]["window_key"],)]
            else:
                self._last_result = []

        # 7. UPDATE CORE.DIM_EVENT_WINDOW
        elif "UPDATE CORE.DIM_EVENT_WINDOW" in sql_clean:
            rel_start, rel_end, abs_start, abs_end, w_key = params
            for w in self.event_windows:
                if w["window_key"] == w_key:
                    w["relative_offset_start"] = rel_start
                    w["relative_offset_end"] = rel_end
                    w["absolute_start_at"] = abs_start
                    w["absolute_end_at"] = abs_end

        # 8. INSERT INTO CORE.DIM_EVENT_WINDOW
        elif "INSERT INTO CORE.DIM_EVENT_WINDOW" in sql_clean:
            w_key, ev_key, w_type, rel_start, rel_end, abs_start, abs_end = params
            self.event_windows.append({
                "window_key": w_key,
                "event_version_key": ev_key,
                "window_type": w_type,
                "relative_offset_start": rel_start,
                "relative_offset_end": rel_end,
                "absolute_start_at": abs_start,
                "absolute_end_at": abs_end
            })

    def fetchone(self):
        if self._last_result:
            return self._last_result[0]
        return None

    def fetchall(self):
        if self._last_result is not None:
            return self._last_result
        return []


def write_test_event_config(path, baseline_terms=None, event_terms=None, event_name=None):
    b_terms = baseline_terms if baseline_terms is not None else ["world cup", "fifa"]
    e_terms = event_terms if event_terms is not None else ["final", "argentina vs france"]
    name = event_name if event_name is not None else "2022 FIFA World Cup Final"

    content = f"""
events:
  - event_name: "{name}"
    external_event_id: "WC_2022_FINAL"
    event_type: "PIPELINE_PILOT"
    occurred_at: "2022-12-18T15:00:00Z"
    inclusion_rationale: "Pipeline validation pilot event"
    event_terms: {json.dumps(e_terms)}
    baseline_terms: {json.dumps(b_terms)}
    windows:
      - window_type: "PRE_EVENT"
        relative_offset_start_hours: -336
        relative_offset_end_hours: -1
      - window_type: "EVENT"
        relative_offset_start_hours: -1
        relative_offset_end_hours: 6
"""
    path.write_text(content.strip(), encoding="utf-8")


# A. Identical config synced twice: same event_version_key reused, no duplicate created
def test_sync_events_identical_config_reuses_version(tmp_path):
    cfg_file = tmp_path / "event_windows.yml"
    write_test_event_config(cfg_file)

    db = MockSnowflakeDatabase()
    conn = db.get_connection()

    # First sync
    sync_events(conn, str(cfg_file))
    assert len(db.event_versions) == 1
    first_v_key = db.event_versions[0]["event_version_key"]
    assert db.event_versions[0]["is_current"] is True

    # Second sync with identical config
    sync_events(conn, str(cfg_file))
    assert len(db.event_versions) == 1, "Identical config must not create a duplicate version"
    assert db.event_versions[0]["event_version_key"] == first_v_key
    assert db.event_versions[0]["is_current"] is True


# B. baseline_terms changes: Type-2 SCD versioning
def test_sync_events_baseline_terms_change_creates_new_version(tmp_path):
    cfg_file = tmp_path / "event_windows.yml"
    write_test_event_config(cfg_file, baseline_terms=["world cup", "fifa"])

    db = MockSnowflakeDatabase()
    conn = db.get_connection()

    # Initial sync
    sync_events(conn, str(cfg_file))
    assert len(db.event_versions) == 1
    v1 = db.event_versions[0]
    v1_key = v1["event_version_key"]
    assert v1["is_current"] is True

    # Windows initially reference v1
    v1_windows = [w for w in db.event_windows if w["event_version_key"] == v1_key]
    assert len(v1_windows) == 2

    # Change baseline_terms in config
    write_test_event_config(cfg_file, baseline_terms=["world cup", "fifa", "qatar 2022"])
    sync_events(conn, str(cfg_file))

    # Must have exactly 2 versions now
    assert len(db.event_versions) == 2
    old_v = [v for v in db.event_versions if v["event_version_key"] == v1_key][0]
    new_v = [v for v in db.event_versions if v["event_version_key"] != v1_key][0]

    # Old version closed
    assert old_v["is_current"] is False
    assert old_v["valid_to"] is not None
    # Old baseline_terms remain unchanged on historical version
    assert json.loads(old_v["baseline_terms"]) == ["world cup", "fifa"]

    # New version is current
    assert new_v["is_current"] is True
    assert new_v["valid_to"] is None
    assert json.loads(new_v["baseline_terms"]) == ["world cup", "fifa", "qatar 2022"]

    # New event windows reference the new event_version_key
    v2_windows = [w for w in db.event_windows if w["event_version_key"] == new_v["event_version_key"]]
    assert len(v2_windows) == 2

    # Historical windows still reference old version key
    v1_windows_post = [w for w in db.event_windows if w["event_version_key"] == v1_key]
    assert len(v1_windows_post) == 2


# C. event_terms changes: Type-2 SCD versioning
def test_sync_events_event_terms_change_creates_new_version(tmp_path):
    cfg_file = tmp_path / "event_windows.yml"
    write_test_event_config(cfg_file, event_terms=["final"])

    db = MockSnowflakeDatabase()
    conn = db.get_connection()

    sync_events(conn, str(cfg_file))
    v1_key = db.event_versions[0]["event_version_key"]

    write_test_event_config(cfg_file, event_terms=["final", "world cup final 2022"])
    sync_events(conn, str(cfg_file))

    assert len(db.event_versions) == 2
    old_v = [v for v in db.event_versions if v["event_version_key"] == v1_key][0]
    new_v = [v for v in db.event_versions if v["event_version_key"] != v1_key][0]

    assert old_v["is_current"] is False
    assert new_v["is_current"] is True
    assert json.loads(old_v["event_terms"]) == ["final"]
    assert json.loads(new_v["event_terms"]) == ["final", "world cup final 2022"]


# D. Historical version cannot be silently mutated by later config sync
def test_historical_version_immutable_across_multiple_syncs(tmp_path):
    cfg_file = tmp_path / "event_windows.yml"
    write_test_event_config(cfg_file, baseline_terms=["term_v1"])

    db = MockSnowflakeDatabase()
    conn = db.get_connection()

    # V1
    sync_events(conn, str(cfg_file))
    v1_key = db.event_versions[0]["event_version_key"]

    # V2
    write_test_event_config(cfg_file, baseline_terms=["term_v2"])
    sync_events(conn, str(cfg_file))
    v2_key = [v["event_version_key"] for v in db.event_versions if v["is_current"] is True][0]

    # V3
    write_test_event_config(cfg_file, baseline_terms=["term_v3"])
    sync_events(conn, str(cfg_file))

    assert len(db.event_versions) == 3

    # Check V1 remains exactly as originally created
    v1 = [v for v in db.event_versions if v["event_version_key"] == v1_key][0]
    assert v1["is_current"] is False
    assert json.loads(v1["baseline_terms"]) == ["term_v1"]

    # Check V2 remains as created
    v2 = [v for v in db.event_versions if v["event_version_key"] == v2_key][0]
    assert v2["is_current"] is False
    assert json.loads(v2["baseline_terms"]) == ["term_v2"]

    # Stable DIM_EVENT identity preserved
    event_keys = {v["event_key"] for v in db.event_versions}
    assert len(event_keys) == 1, "Stable DIM_EVENT identity must be preserved across versions"


# E. Exactly one is_current=TRUE version remains after version change
def test_exactly_one_current_version_remains_after_sync(tmp_path):
    cfg_file = tmp_path / "event_windows.yml"
    write_test_event_config(cfg_file, event_name="Initial Name")

    db = MockSnowflakeDatabase()
    conn = db.get_connection()

    sync_events(conn, str(cfg_file))
    assert sum(1 for v in db.event_versions if v["is_current"] is True) == 1

    write_test_event_config(cfg_file, event_name="Updated Name")
    sync_events(conn, str(cfg_file))

    current_versions = [v for v in db.event_versions if v["is_current"] is True]
    assert len(current_versions) == 1, "Exactly one is_current=TRUE version must remain"
    assert current_versions[0]["event_name"] == "Updated Name"
