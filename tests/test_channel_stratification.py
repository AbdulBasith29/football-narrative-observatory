import pytest
import uuid
import json
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

from channel_classifier import (
    classify_channel_type,
    classify_player_focus,
    classify_channel_assessment,
    create_or_verify_event_channel_snapshot,
    compute_canonical_manifest_hash,
    UnapprovedParameterError,
    SnapshotMutationViolationError,
    DeduplicationRuleMissingError,
    register_deduplication_rule,
    normalize_ntz_timestamp
)
from channel_evidence_acquisition import (
    get_or_create_acquisition_state,
    execute_channel_evidence_harvest
)
from inspect_candidate_strata import inspect_candidate_strata
from apply_channel_override import apply_channel_override
from sync_config import sync_strata


class MockSnowflakeDatabase:
    def __init__(self):
        self.strata = {}  # (type, focus) -> dict
        self.dim_channels = {}
        self.dim_videos = {}
        self.fact_video_snapshots = []
        self.channel_stratum_versions = []
        self.fact_assessments = []
        self.evidence_manifest_items = []
        self.acquisition_states = {}
        self.event_snapshots = {}
        self.manual_overrides = []
        self.fact_ingestion_runs = {}
        self.fact_api_requests = []
        self.raw_responses = []
        self.candidate_events = []
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

        # 1. DIM_CHANNEL_STRATUM operations
        if "FROM CORE.DIM_CHANNEL_STRATUM WHERE channel_type_value =" in sql_clean:
            ct, pf = params[0], params[1]
            key = (ct, pf)
            if key in self.strata:
                self._last_result = [(self.strata[key]["stratum_key"],)]
            else:
                self._last_result = []

        # 1. DIM_CHANNEL_STRATUM operations
        elif "INSERT INTO CORE.DIM_CHANNEL_STRATUM_VERSION" in sql_clean:
            sv_key, ch_key, st_key, val_from = params
            self.channel_stratum_versions.append({
                "stratum_version_key": sv_key,
                "channel_key": ch_key,
                "stratum_key": st_key,
                "valid_from": val_from,
                "valid_to": None,
                "is_current": True
            })

        elif "UPDATE CORE.DIM_CHANNEL_STRATUM_VERSION" in sql_clean:
            val_to, sv_key = params
            for v in self.channel_stratum_versions:
                if v["stratum_version_key"] == sv_key:
                    v["is_current"] = False
                    v["valid_to"] = val_to

        elif "INSERT INTO CORE.DIM_CHANNEL_STRATUM" in sql_clean:
            sk, ct, pf, name, elig = params
            self.strata[(ct, pf)] = {
                "stratum_key": sk,
                "channel_type_value": ct,
                "player_focus_value": pf,
                "stratum_name": name,
                "analytical_eligibility": elig
            }

        elif "UPDATE CORE.DIM_CHANNEL_STRATUM" in sql_clean:
            name, elig, sk = params
            for k, v in self.strata.items():
                if v["stratum_key"] == sk:
                    v["stratum_name"] = name
                    v["analytical_eligibility"] = elig

        # 2. DIM_CHANNEL operations
        elif "FROM CORE.DIM_CHANNEL WHERE channel_key =" in sql_clean:
            ch_k = params[0]
            if ch_k in self.dim_channels:
                ch = self.dim_channels[ch_k]
                self._last_result = [(ch["source_id"], ch["channel_name"])]
            else:
                self._last_result = []

        # 3. DIM_VIDEO count and pre-event retrieval
        elif "SELECT COUNT(DISTINCT v.video_key)" in sql_clean:
            count_val = getattr(self, "eligible_count_override", 15)
            self._last_result = [(count_val,)]

        elif "FROM CORE.DIM_VIDEO v" in sql_clean and "LEFT JOIN" in sql_clean and "WHERE v.channel_key =" in sql_clean:
            ch_k, w_start, w_end = params[0], params[1], params[2]
            results = []
            for v_key, v in self.dim_videos.items():
                if v["channel_key"] == ch_k and w_start <= v["published_at"] < w_end:
                    snaps = [s for s in self.fact_video_snapshots if s["video_key"] == v_key]
                    snap = snaps[-1] if snaps else {}
                    results.append((
                        v_key, v["source_id"], datetime.fromisoformat(v["published_at"]),
                        v.get("duration_seconds", 120), v.get("is_short", False),
                        snap.get("title", ""), snap.get("description", ""),
                        snap.get("raw_response_id", "raw-1"), snap.get("api_request_id", "req-1")
                    ))
            self._last_result = results

        # 4. FACT_CHANNEL_CLASSIFICATION_ASSESSMENT
        elif "INSERT INTO OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT" in sql_clean:
            self.fact_assessments.append(params)

        # 5. FACT_CHANNEL_ASSESSMENT_EVIDENCE_ITEM
        elif "INSERT INTO OPS.FACT_CHANNEL_ASSESSMENT_EVIDENCE_ITEM" in sql_clean:
            self.evidence_manifest_items.append(params)

        # 6. DIM_CHANNEL_STRATUM_VERSION (Type-2 SCD)
        elif "FROM CORE.DIM_CHANNEL_STRATUM_VERSION" in sql_clean and "is_current = TRUE" in sql_clean:
            ch_k = params[0]
            current_v = [v for v in self.channel_stratum_versions if v["channel_key"] == ch_k and v["is_current"]]
            if current_v:
                if "JOIN CORE.DIM_CHANNEL_STRATUM" in sql_clean:
                    st_key = current_v[0]["stratum_key"]
                    matching_st = next((s for s in self.strata.values() if s["stratum_key"] == st_key), {})
                    self._last_result = [(
                        current_v[0]["stratum_version_key"],
                        matching_st.get("channel_type_value", "INDEPENDENT_CREATOR"),
                        matching_st.get("player_focus_value", "NO_STRONG_DOMINANT_PLAYER_FOCUS")
                    )]
                else:
                    self._last_result = [(current_v[0]["stratum_version_key"], current_v[0]["stratum_key"])]
            else:
                self._last_result = []


        # 7. BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT
        elif "FROM CORE.BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT" in sql_clean:
            ev_k, ch_k, prot_ver = params
            key = (ev_k, ch_k, prot_ver)
            if key in self.event_snapshots:
                sn = self.event_snapshots[key]
                self._last_result = [(
                    sn["snapshot_key"], sn["channel_type_value"], sn["player_focus_value"],
                    sn["channel_type_confidence"], sn["player_focus_confidence"],
                    sn.get("reference_period_start"), sn.get("reference_period_end"),
                    sn.get("effective_window_days"), sn.get("eligible_video_count"),
                    sn.get("messi_video_count"), sn.get("ronaldo_video_count"),
                    sn.get("messi_prevalence"), sn.get("ronaldo_prevalence"),
                    sn.get("focus_ratio"), sn["fallback_used"],
                    sn["headline_decomposition_eligible"], sn.get("override_id")
                )]
            else:
                self._last_result = []

        elif "INSERT INTO CORE.BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT" in sql_clean:
            sn_key, ev_k, ch_k, prot_ver, ct, pf, ct_c, pf_c, ref_s, ref_e, eff_d, elig_c, vm, vr, pm, pr, fr, fb, h_elig, ov_id = params
            self.event_snapshots[(ev_k, ch_k, prot_ver)] = {
                "snapshot_key": sn_key,
                "channel_type_value": ct,
                "player_focus_value": pf,
                "channel_type_confidence": ct_c,
                "player_focus_confidence": pf_c,
                "reference_period_start": ref_s,
                "reference_period_end": ref_e,
                "effective_window_days": eff_d,
                "eligible_video_count": elig_c,
                "messi_video_count": vm,
                "ronaldo_video_count": vr,
                "messi_prevalence": pm,
                "ronaldo_prevalence": pr,
                "focus_ratio": fr,
                "fallback_used": fb,
                "headline_decomposition_eligible": h_elig,
                "override_id": ov_id
            }

        # 8. CHANNEL_EVIDENCE_ACQUISITION_STATE
        elif "FROM OPS.CHANNEL_EVIDENCE_ACQUISITION_STATE" in sql_clean:
            fv_k, ch_k, w_end, scope = params
            key = (fv_k, ch_k, w_end, scope)
            if key in self.acquisition_states:
                st = self.acquisition_states[key]
                self._last_result = [(
                    st["state_key"], st["status"], st["next_page_token"], st["pages_completed"],
                    st["oldest_observed"], st["items_observed"], st["eligible_videos_observed"],
                    st["api_calls_consumed"], st["retry_count"]
                )]
            else:
                self._last_result = []

        elif "INSERT INTO OPS.CHANNEL_EVIDENCE_ACQUISITION_STATE" in sql_clean:
            st_key, fv_k, ch_k, src_ch, pl_id, scope, w_start, w_end, first_run, lat_run, st_at, up_at = params
            self.acquisition_states[(fv_k, ch_k, w_end, scope)] = {
                "state_key": st_key,
                "status": "PENDING",
                "next_page_token": None,
                "pages_completed": 0,
                "oldest_observed": None,
                "items_observed": 0,
                "eligible_videos_observed": 0,
                "api_calls_consumed": 0,
                "retry_count": 0
            }

        elif "UPDATE OPS.CHANNEL_EVIDENCE_ACQUISITION_STATE" in sql_clean:
            # Handles both status=IN_PROGRESS and final update
            pass

        elif "SELECT COUNT(DISTINCT v.video_key) FROM CORE.DIM_VIDEO" in sql_clean:
            count_val = getattr(self, "eligible_count_override", 15)
            self._last_result = [(count_val,)]

        # 9. MANUAL_OVERRIDE
        elif "INSERT INTO OPS.MANUAL_OVERRIDE" in sql_clean:
            self.manual_overrides.append(params)

        # 10. inspect_candidate_strata query
        elif "FROM CORE.BRIDGE_VIDEO_EVENT b" in sql_clean and "JOIN CORE.DIM_VIDEO v" in sql_clean:
            self._last_result = self.candidate_events

        # 11. Ingestion run & API requests
        elif "INSERT INTO OPS.FACT_INGESTION_RUN" in sql_clean:
            self.fact_ingestion_runs[params[0]] = {"run_id": params[0], "status": "IN_PROGRESS"}
        elif "UPDATE OPS.FACT_INGESTION_RUN" in sql_clean:
            pass
        elif "INSERT INTO OPS.FACT_API_REQUEST" in sql_clean:
            self.fact_api_requests.append(params)
        elif "INSERT INTO RAW.YOUTUBE_API_RESPONSE" in sql_clean:
            canonical_raw_cols = [
                "raw_response_id", "api_request_id", "ingestion_run_id", "source_system", "endpoint",
                "resource_scope_type", "resource_scope_id", "request_parameters", "http_status",
                "page_token_used", "next_page_token_returned", "retrieved_at", "raw_json_payload",
                "payload_hash", "parser_version", "source_schema_version"
            ]
            import re
            cols_match = re.search(r'INSERT INTO RAW\.YOUTUBE_API_RESPONSE \((.*?)\)', sql_clean, re.IGNORECASE)
            if cols_match:
                cols = [c.strip().lower() for c in cols_match.group(1).split(',')]
                assert cols == canonical_raw_cols, f"RAW SQL columns {cols} do not match canonical {canonical_raw_cols}"
            self.raw_responses.append(params)
        elif "FROM RAW.YOUTUBE_API_RESPONSE" in sql_clean and "channels.list" in sql_clean:
            src_ch_id = params[0]
            matches = [r for r in self.raw_responses if len(r) >= 14 and r[5] == src_ch_id and r[3] == 'channels.list']
            if matches:
                m = matches[-1]
                self._last_result = [(m[11], m[0])]
            else:
                self._last_result = []

    def fetchone(self):
        if self._last_result is not None:
            if self._last_result:
                return self._last_result[0]
            return None
        return None

    def fetchall(self):
        if self._last_result is not None:
            return self._last_result
        return []


# =========================================================================
# TEST SUITE
# =========================================================================

def test_channel_stratum_seed_dimensions():
    """
    Verifies that sync_strata deterministically seeds all 30 composite strata,
    where exactly 20 are analytically eligible and 10 are ineligible (UNCLASSIFIED on either axis).
    """
    db = MockSnowflakeDatabase()
    conn = db.get_connection()
    sync_strata(conn)

    assert len(db.strata) == 30
    eligible_count = sum(1 for s in db.strata.values() if s["analytical_eligibility"] is True)
    ineligible_count = sum(1 for s in db.strata.values() if s["analytical_eligibility"] is False)
    assert eligible_count == 20
    assert ineligible_count == 10

    # Verify specific keys
    assert db.strata[("BROAD_REACH_PUBLISHER", "MESSI_FOCUSED")]["analytical_eligibility"] is True
    assert db.strata[("UNCLASSIFIED", "MESSI_FOCUSED")]["analytical_eligibility"] is False
    assert db.strata[("BROAD_REACH_PUBLISHER", "UNCLASSIFIED")]["analytical_eligibility"] is False


def test_unapproved_parameters_fail_closed():
    """
    Verifies that classifiers strictly fail closed when encountering unapproved configuration parameters.
    """
    strat_cfg = {
        "frozen_parameters": {
            "smoothing_alpha": 1.0,
            "player_focus_ratio_threshold": 4.0,
            "player_prevalence_floor": 0.15,
            "club_dominance_threshold": 0.70
        },
        "club_dominance_winning_margin": None,  # UNAPPROVED
        "combined_focus_threshold": None        # UNAPPROVED
    }

    # 1. Dual player prevalence triggers fail-closed for unapproved combined_focus_threshold
    videos = [
        {"published_at_dt": datetime.now(timezone.utc), "messi_matched": True, "ronaldo_matched": False}
        for _ in range(5)
    ] + [
        {"published_at_dt": datetime.now(timezone.utc), "messi_matched": False, "ronaldo_matched": True}
        for _ in range(5)
    ]
    with pytest.raises(UnapprovedParameterError) as exc_info:
        classify_player_focus(videos, datetime.now(timezone.utc), 90, strat_cfg)
    assert "combined_focus_threshold" in str(exc_info.value)

    # 2. Club dominance triggers fail-closed for unapproved club margin
    strat_cfg["club_registry"] = {"Arsenal": ["arsenal", "gunners"]}
    club_videos = [
        {"title": "Arsenal match analysis", "description": "gunners highlights"}
        for _ in range(10)
    ]
    with pytest.raises(UnapprovedParameterError) as exc_info:
        classify_channel_type("ch1", "Arsenal Fan TV", "About Arsenal", club_videos, strat_cfg)
    assert "club_dominance_winning_margin" in str(exc_info.value)


def test_channel_type_minimum_evidence_gating():
    """
    Verifies Channel Type minimum evidence rules (identity signal AND >= 10 eligible videos).
    """
    strat_cfg = {"frozen_parameters": {"club_dominance_threshold": 0.70}}

    # Case A: Missing identity signal
    res_no_id = classify_channel_type("ch1", "", "", [{"title": "Video"} for _ in range(15)], strat_cfg)
    assert res_no_id["channel_type"] == "UNCLASSIFIED"
    assert res_no_id["confidence"] == "LOW"

    # Case B: Insufficient video evidence (< 10 videos)
    res_sparse = classify_channel_type("ch1", "Footy Daily", "Great channel", [{"title": "Video"} for _ in range(8)], strat_cfg)
    assert res_sparse["channel_type"] == "UNCLASSIFIED"
    assert res_sparse["confidence"] == "LOW"

    # Case C: Valid identity and >= 10 videos -> INDEPENDENT_CREATOR fallback
    res_valid = classify_channel_type("ch1", "Footy Daily", "Great channel", [{"title": "Video"} for _ in range(12)], strat_cfg)
    assert res_valid["channel_type"] == "INDEPENDENT_CREATOR"


def test_channel_type_reproducibility():
    """
    Verifies broadcaster registry match reproducibility and structured evidence logging.
    """
    strat_cfg = {
        "broadcaster_registry_version": "broadcaster_v1",
        "broadcasters_allowlist": ["UC_SKY_SPORTS"],
        "frozen_parameters": {"club_dominance_threshold": 0.70}
    }
    videos = [{"title": "General highlights"} for _ in range(15)]
    res = classify_channel_type("UC_SKY_SPORTS", "Sky Sports", "Official sports", videos, strat_cfg)

    assert res["channel_type"] == "BROAD_REACH_PUBLISHER"
    assert res["confidence"] == "HIGH"
    assert res["evidence"]["registry_matched"] is True
    assert res["evidence"]["broadcaster_registry_version"] == "broadcaster_v1"


def test_player_focus_smoothing_and_dominance():
    """
    Verifies smoothed ratio R_M:R > 4.0 and P_M > 0.15 rules for MESSI_FOCUSED and RONALDO_FOCUSED.
    """
    strat_cfg = {
        "frozen_parameters": {
            "smoothing_alpha": 1.0,
            "player_focus_ratio_threshold": 4.0,
            "player_prevalence_floor": 0.15
        }
    }
    now = datetime.now(timezone.utc)

    # 1. Messi dominance: 9 Messi videos out of 10 (V_M=9, V_R=0).
    # R_M:R = (9 + 1) / (0 + 1) = 10.0 > 4.0, P_M = 0.90 > 0.15 -> MESSI_FOCUSED
    messi_vids = [
        {"published_at_dt": now - timedelta(days=i * 5), "messi_matched": True, "ronaldo_matched": False}
        for i in range(9)
    ] + [
        {"published_at_dt": now - timedelta(days=50), "messi_matched": False, "ronaldo_matched": False}
    ]
    res_m = classify_player_focus(messi_vids, now, 90, strat_cfg)
    assert res_m["player_focus"] == "MESSI_FOCUSED"
    assert res_m["messi_prevalence"] == 0.90
    assert res_m["focus_ratio"] == 10.0

    # 2. Ronaldo dominance: 9 Ronaldo videos out of 10 (V_R=9, V_M=0).
    # R_R:M = (9 + 1) / (0 + 1) = 10.0 > 4.0, P_R = 0.90 > 0.15 -> RONALDO_FOCUSED
    ronaldo_vids = [
        {"published_at_dt": now - timedelta(days=i * 5), "messi_matched": False, "ronaldo_matched": True}
        for i in range(9)
    ] + [
        {"published_at_dt": now - timedelta(days=50), "messi_matched": False, "ronaldo_matched": False}
    ]
    res_r = classify_player_focus(ronaldo_vids, now, 90, strat_cfg)
    assert res_r["player_focus"] == "RONALDO_FOCUSED"
    assert res_r["ronaldo_prevalence"] == 0.90


def test_player_focus_minimum_evidence_and_subwindow_stability():
    """
    Verifies temporal stability across 3x30d subwindows and minimum evidence (<10 videos -> UNCLASSIFIED).
    """
    strat_cfg = {
        "frozen_parameters": {
            "smoothing_alpha": 1.0,
            "player_focus_ratio_threshold": 4.0,
            "player_prevalence_floor": 0.15
        }
    }
    now = datetime(2022, 12, 17, 15, 0, 0, tzinfo=timezone.utc)

    # < 10 videos -> UNCLASSIFIED
    sparse_vids = [
        {"published_at_dt": now - timedelta(days=10), "messi_matched": True, "ronaldo_matched": False}
        for _ in range(5)
    ]
    res_sparse = classify_player_focus(sparse_vids, now, 90, strat_cfg)
    assert res_sparse["player_focus"] == "UNCLASSIFIED"
    assert res_sparse["confidence"] == "LOW"

    # Consistent Messi focus across all three 30d subwindows: [0, 30), [30, 60), [60, 90)
    stable_vids = (
        [{"published_at_dt": now - timedelta(days=5), "messi_matched": True, "ronaldo_matched": False} for _ in range(4)] +
        [{"published_at_dt": now - timedelta(days=35), "messi_matched": True, "ronaldo_matched": False} for _ in range(4)] +
        [{"published_at_dt": now - timedelta(days=65), "messi_matched": True, "ronaldo_matched": False} for _ in range(4)]
    )
    res_stable = classify_player_focus(stable_vids, now, 90, strat_cfg)
    assert res_stable["player_focus"] == "MESSI_FOCUSED"
    # When large-margin rules are unapproved, subwindow agreement defaults deterministically to MEDIUM
    assert res_stable["confidence"] == "MEDIUM"
    assert len(res_stable["evidence"]["subwindow_evaluations"]) == 3
    assert all(sw == "MESSI_FOCUSED" for sw in res_stable["evidence"]["subwindow_evaluations"])


@pytest.fixture
def approved_test_config(tmp_path):
    import yaml
    cfg_path = tmp_path / "approved_channel_stratification.yml"
    with open("config/channel_stratification.yml", "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    data["deduplication_rule_version"] = "EXPLICIT_NONE"
    data["deduplication_rule_status"] = "APPROVED_EXPLICIT_NONE"
    with open(cfg_path, "w", encoding="utf-8") as f:
        yaml.dump(data, f)
    return str(cfg_path)


def test_static_vs_event_analytical_eligibility():
    """
    Verifies that when fallback_used = TRUE (180d fallback), headline_decomposition_eligible is FALSE,
    even though the composite stratum itself is statically eligible in DIM_CHANNEL_STRATUM.
    """
    db = MockSnowflakeDatabase()
    conn = db.get_connection()

    assessment_res_fallback = {
        "channel_type_value": "BROAD_REACH_PUBLISHER",
        "player_focus_value": "MESSI_FOCUSED",
        "channel_type_confidence": "HIGH",
        "player_focus_confidence": "LOW",
        "fallback_used": True,
        "effective_window_days": 180,
        "eligible_video_count": 12,
        "messi_video_count": 10,
        "ronaldo_video_count": 0,
        "messi_prevalence": 0.83333,
        "ronaldo_prevalence": 0.0,
        "focus_ratio": 11.0
    }
    ref_start = datetime(2022, 6, 20, 15, 0, 0, tzinfo=timezone.utc)
    ref_end = datetime(2022, 12, 17, 15, 0, 0, tzinfo=timezone.utc)

    snap = create_or_verify_event_channel_snapshot(
        conn, "ev-1", "ch-1", "1.1", assessment_res_fallback, ref_start, ref_end
    )
    assert snap["status"] == "INSERTED"
    # Even though BROAD_REACH_PUBLISHER x MESSI_FOCUSED is statically eligible, 180d fallback is excluded from M019
    assert snap["payload"]["headline_decomposition_eligible"] is False
    assert snap["payload"]["fallback_used"] is True


def test_snapshot_immutability_fail_closed():
    """
    Verifies application-level immutability for BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT:
    - Identical payload returns IDEMPOTENT_NOOP.
    - Differing payload raises SnapshotMutationViolationError.
    """
    db = MockSnowflakeDatabase()
    conn = db.get_connection()

    assessment_res = {
        "channel_type_value": "INDEPENDENT_CREATOR",
        "player_focus_value": "MESSI_FOCUSED",
        "channel_type_confidence": "LOW",
        "player_focus_confidence": "MEDIUM",
        "fallback_used": False,
        "effective_window_days": 90,
        "eligible_video_count": 15,
        "messi_video_count": 12,
        "ronaldo_video_count": 1,
        "messi_prevalence": 0.8,
        "ronaldo_prevalence": 0.06667,
        "focus_ratio": 6.5
    }
    ref_start = datetime(2022, 9, 18, 15, 0, 0, tzinfo=timezone.utc)
    ref_end = datetime(2022, 12, 17, 15, 0, 0, tzinfo=timezone.utc)

    # First insert
    res1 = create_or_verify_event_channel_snapshot(conn, "ev-1", "ch-1", "1.1", assessment_res, ref_start, ref_end)
    assert res1["status"] == "INSERTED"

    # Second insert with identical payload -> NOOP
    res2 = create_or_verify_event_channel_snapshot(conn, "ev-1", "ch-1", "1.1", assessment_res, ref_start, ref_end)
    assert res2["status"] == "IDEMPOTENT_NOOP"

    # Third insert with differing payload -> Fail closed
    mutated_res = dict(assessment_res)
    mutated_res["player_focus_value"] = "RONALDO_FOCUSED"
    with pytest.raises(SnapshotMutationViolationError) as exc_info:
        create_or_verify_event_channel_snapshot(conn, "ev-1", "ch-1", "1.1", mutated_res, ref_start, ref_end)
    assert "Cannot mutate existing event snapshot" in str(exc_info.value)


def test_type_2_scd_grain_and_lifecycle(approved_test_config):
    """
    Verifies Type-2 SCD maintenance in CORE.DIM_CHANNEL_STRATUM_VERSION (channel x validity interval):
    - Initial insert opens version.
    - Identical re-assessment does not mutate dimension.
    - Different stratum closes prior version and opens new version.
    """
    db = MockSnowflakeDatabase()
    conn = db.get_connection()
    sync_strata(conn)

    # Register channel
    db.dim_channels["ch-1"] = {"source_id": "UC123", "channel_name": "Test Channel"}
    
    # 12 Messi videos
    now = datetime(2022, 12, 17, 15, 0, 0, tzinfo=timezone.utc)
    for i in range(12):
        v_key = f"vid-{i}"
        db.dim_videos[v_key] = {
            "channel_key": "ch-1",
            "source_id": f"src-{i}",
            "published_at": (now - timedelta(days=i * 2 + 1)).isoformat(),
            "is_short": False
        }
        db.fact_video_snapshots.append({
            "video_key": v_key,
            "title": "Leo Messi masterclass",
            "description": "Lionel Messi analysis"
        })

    # Assessment 1: MESSI_FOCUSED
    res1 = classify_channel_assessment(
        conn, "frame-1", "ch-1", now,
        config_path=approved_test_config,
        about_description="Comprehensive football channel"
    )
    assert res1["player_focus_value"] == "MESSI_FOCUSED"
    assert len(db.channel_stratum_versions) == 1
    v1 = db.channel_stratum_versions[0]
    assert v1["is_current"] is True
    assert v1["valid_to"] is None

    # Assessment 2: Re-assessment with identical stratum -> No new version opened
    res2 = classify_channel_assessment(
        conn, "frame-1", "ch-1", now,
        config_path=approved_test_config,
        about_description="Comprehensive football channel"
    )
    assert len(db.channel_stratum_versions) == 1

    # Assessment 3: Channel shifts orientation to RONALDO_FOCUSED
    # Clear previous videos and populate with 15 Ronaldo videos (V_R=15, V_M=0)
    db.dim_videos.clear()
    db.fact_video_snapshots.clear()
    for i in range(15):
        v_key = f"vid-r-{i}"
        db.dim_videos[v_key] = {
            "channel_key": "ch-1",
            "source_id": f"src-r-{i}",
            "published_at": (now - timedelta(days=i * 2 + 1)).isoformat(),
            "is_short": False
        }
        db.fact_video_snapshots.append({
            "video_key": v_key,
            "title": "Cristiano Ronaldo celebration",
            "description": "CR7 highlights"
        })

    res3 = classify_channel_assessment(
        conn, "frame-1", "ch-1", now,
        config_path=approved_test_config,
        about_description="Comprehensive football channel"
    )
    assert res3["player_focus_value"] == "RONALDO_FOCUSED"
    assert len(db.channel_stratum_versions) == 2
    assert db.channel_stratum_versions[0]["is_current"] is False
    assert db.channel_stratum_versions[0]["valid_to"] is not None
    assert db.channel_stratum_versions[1]["is_current"] is True
    assert db.channel_stratum_versions[1]["valid_to"] is None


def test_evidence_manifest_exact_lineage(approved_test_config):
    """
    Verifies that every evaluated video observation is recorded in OPS.FACT_CHANNEL_ASSESSMENT_EVIDENCE_ITEM
    with alias matches, exact deduplication flags, and canonical hash alignment.
    """
    db = MockSnowflakeDatabase()
    conn = db.get_connection()
    sync_strata(conn)

    db.dim_channels["ch-1"] = {"source_id": "UC123", "channel_name": "Manifest Test Channel"}
    now = datetime(2022, 12, 17, 15, 0, 0, tzinfo=timezone.utc)
    for i in range(10):
        v_key = f"vid-{i}"
        db.dim_videos[v_key] = {
            "channel_key": "ch-1",
            "source_id": f"src-{i}",
            "published_at": (now - timedelta(days=i * 2 + 1)).isoformat(),
            "is_short": False
        }
        db.fact_video_snapshots.append({
            "video_key": v_key,
            "title": "Lionel Messi best goals",
            "description": "Leo Messi skills"
        })

    res = classify_channel_assessment(
        conn, "frame-1", "ch-1", now,
        config_path=approved_test_config,
        about_description="Football highlights channel"
    )
    assert len(db.evidence_manifest_items) == 10
    assert len(res["evidence_manifest_hash"]) == 64  # Valid SHA-256


def test_exact_video_id_deduplication(approved_test_config):
    """
    Verifies that identical source_video_id observations in the window are collapsed to 1 observation.
    """
    db = MockSnowflakeDatabase()
    conn = db.get_connection()
    sync_strata(conn)

    db.dim_channels["ch-1"] = {"source_id": "UC123", "channel_name": "Dedup Channel"}
    now = datetime(2022, 12, 17, 15, 0, 0, tzinfo=timezone.utc)

    # 10 videos, with video 0 and video 1 having identical source_video_id "src-dup"
    for i in range(10):
        v_key = f"vid-{i}"
        src_id = "src-dup" if i in [0, 1] else f"src-{i}"
        db.dim_videos[v_key] = {
            "channel_key": "ch-1",
            "source_id": src_id,
            "published_at": (now - timedelta(days=i * 2 + 1)).isoformat(),
            "is_short": False
        }
        db.fact_video_snapshots.append({
            "video_key": v_key,
            "title": "Messi highlight",
            "description": "Leo Messi"
        })

    res = classify_channel_assessment(
        conn, "frame-1", "ch-1", now,
        config_path=approved_test_config,
        about_description="Football channel about"
    )
    # Total distinct videos should be 9
    assert res["eligible_video_count"] == 9
    # Exactly one item marked as deduplicated duplicate in manifest
    dup_manifest_items = [it for it in db.evidence_manifest_items if it[13] is True]
    assert len(dup_manifest_items) == 1


def test_state_machine_partial_quota_and_resume():
    """
    Verifies OPS.CHANNEL_EVIDENCE_ACQUISITION_STATE lifecycle and resumption from next_page_token.
    """
    db = MockSnowflakeDatabase()
    conn = db.get_connection()

    ref_end = datetime(2022, 12, 17, 15, 0, 0, tzinfo=timezone.utc)
    w_start = ref_end - timedelta(days=90)

    # 1. Initialize state
    st1 = get_or_create_acquisition_state(
        conn, "frame-1", "ch-1", "UC123", "UU123", "90D_PRIMARY", w_start, ref_end, "run-1"
    )
    assert st1["status"] == "PENDING"
    assert st1["is_new"] is True

    # 2. Simulate interruption with quota limit: save token
    db.acquisition_states[("frame-1", "ch-1", ref_end.isoformat(), "90D_PRIMARY")]["status"] = "PARTIAL_QUOTA_LIMIT"
    db.acquisition_states[("frame-1", "ch-1", ref_end.isoformat(), "90D_PRIMARY")]["next_page_token"] = "TOKEN_PAGE_2"

    # 3. Resume: retrieves existing state with token
    st2 = get_or_create_acquisition_state(
        conn, "frame-1", "ch-1", "UC123", "UU123", "90D_PRIMARY", w_start, ref_end, "run-2"
    )
    assert st2["status"] == "PARTIAL_QUOTA_LIMIT"
    assert st2["next_page_token"] == "TOKEN_PAGE_2"
    assert st2["is_new"] is False


def test_inspect_candidate_strata_neutral_reporting():
    """
    Verifies inspect_candidate_strata outputs descriptive distributions at grain
    (event_version_key x composite stratum x cohort_type) reporting both aggregate
    totals and per-channel candidate distributions with zero recommendatory logic.
    """
    db = MockSnowflakeDatabase()
    conn = db.get_connection()

    # Seed 3 channels with candidate counts
    # Row format: (cohort_type, stratum_name, channel_key, channel_display_name, video_count)
    db.candidate_events = [
        ("EVENT", "BROAD_REACH_PUBLISHER x MESSI_FOCUSED", "ch-1", "Sky Sports", 10),
        ("EVENT", "BROAD_REACH_PUBLISHER x MESSI_FOCUSED", "ch-2", "ESPN FC", 20),
        ("BASELINE", "BROAD_REACH_PUBLISHER x MESSI_FOCUSED", "ch-1", "Sky Sports", 5),
    ]

    report = inspect_candidate_strata(conn, "ev-1", "1.1")
    assert len(report) == 2

    event_row = next(r for r in report if r["cohort_type"] == "EVENT")
    assert event_row["stratum_name"] == "BROAD_REACH_PUBLISHER x MESSI_FOCUSED"
    assert event_row["participating_channel_count"] == 2
    assert event_row["aggregate_candidate_count"] == 30
    assert event_row["per_channel_distribution"]["min"] == 10
    assert event_row["per_channel_distribution"]["max"] == 20
    assert event_row["per_channel_distribution"]["median"] == 15.0

    # Ensure no recommendatory fields exist in the output dictionary
    for r in report:
        assert "recommended_k" not in r
        assert "capping_rate" not in r


def test_manual_override_audit():
    """
    Verifies that apply_channel_override logs structured audit fields into OPS.MANUAL_OVERRIDE
    and creates a new audited version in CORE.DIM_CHANNEL_STRATUM_VERSION without mutating the old record.
    """
    db = MockSnowflakeDatabase()
    conn = db.get_connection()
    sync_strata(conn)

    # Initial active version
    db.channel_stratum_versions.append({
        "stratum_version_key": "v-1",
        "channel_key": "ch-1",
        "stratum_key": db.strata[("INDEPENDENT_CREATOR", "NO_STRONG_DOMINANT_PLAYER_FOCUS")]["stratum_key"],
        "valid_from": "2022-01-01T00:00:00Z",
        "valid_to": None,
        "is_current": True
    })

    override_res = apply_channel_override(
        conn,
        channel_key="ch-1",
        axis_to_override="PLAYER_FOCUS",
        original_value="NO_STRONG_DOMINANT_PLAYER_FOCUS",
        proposed_value="MESSI_FOCUSED",
        override_reason="Misleading clickbait titles suppressed true Messi focus",
        supporting_evidence="Catalog audit report 2022-12-18",
        submitted_by="analyst_alice",
        approved_by="senior_reviewer_bob"
    )

    assert override_res["review_status"] == "APPROVED_OVERRIDE"
    assert len(db.manual_overrides) == 1
    assert len(db.channel_stratum_versions) == 2
    assert db.channel_stratum_versions[0]["is_current"] is False
    assert db.channel_stratum_versions[1]["is_current"] is True


def test_canonical_raw_youtube_response_persistence():
    """
    Blocker 1: Validates that RAW response persistence strictly conforms to the canonical
    RAW.YOUTUBE_API_RESPONSE schema (16 exact columns) prior to any parsing operations.
    """
    from channel_evidence_acquisition import persist_raw_youtube_response, persist_raw_playlist_response

    db = MockSnowflakeDatabase()
    conn = db.get_connection()

    sample_payload = {"kind": "youtube#playlistItemListResponse", "items": [{"id": "v1"}]}
    raw_id = persist_raw_playlist_response(
        conn=conn,
        raw_payload=sample_payload,
        playlist_id="UU12345",
        ingestion_run_id="run-test",
        api_request_id="req-test",
        page_token_used="token-prev",
        next_page_token_returned="token-next"
    )

    assert len(db.raw_responses) == 1
    inserted_params = db.raw_responses[0]
    # Parameter order: raw_response_id, api_request_id, ingestion_run_id, endpoint,
    # resource_scope_type, resource_scope_id, req_params_str, http_status,
    # page_token_used, next_page_token_returned, retrieved_at, raw_payload_str,
    # payload_hash, parser_version, source_schema_version
    assert inserted_params[0] == raw_id
    assert inserted_params[1] == "req-test"
    assert inserted_params[2] == "run-test"
    assert inserted_params[3] == "playlistItems.list"
    assert inserted_params[4] == "PLAYLIST"
    assert inserted_params[5] == "UU12345"
    assert inserted_params[7] == 200
    assert inserted_params[8] == "token-prev"
    assert inserted_params[9] == "token-next"
    assert inserted_params[13] == "1.0"
    assert inserted_params[14] == "v3"


def test_cross_video_dedup_fail_closed_in_classify_channel_assessment():
    """
    Blocker 2: Validates that when cross-video deduplication methodology remains unapproved,
    classify_channel_assessment raises DeduplicationRuleMissingError and strictly fails closed,
    preventing prevalence calculation with raw distinct video IDs.
    """
    db = MockSnowflakeDatabase()
    conn = db.get_connection()
    sync_strata(conn)

    db.dim_channels["ch-1"] = {"source_id": "UC123", "channel_name": "Test Dedup Fail-Closed"}
    now = datetime(2022, 12, 17, 15, 0, 0, tzinfo=timezone.utc)
    for i in range(12):
        v_key = f"vid-{i}"
        db.dim_videos[v_key] = {
            "channel_key": "ch-1",
            "source_id": f"src-{i}",
            "published_at": (now - timedelta(days=i + 1)).isoformat(),
            "is_short": False
        }
        db.fact_video_snapshots.append({
            "video_key": v_key,
            "title": "Lionel Messi best goals",
            "description": "Skills"
        })

    # Call with production config (deduplication_rule_version: null)
    with pytest.raises(DeduplicationRuleMissingError) as exc_info:
        classify_channel_assessment(
            conn, "frame-1", "ch-1", now,
            config_path="config/channel_stratification.yml",
            about_description="Football channel"
        )
    assert "Cross-video deduplication methodology is required" in str(exc_info.value)


def test_reliable_channel_identity_evidence_enforcement(approved_test_config):
    """
    Blocker 3: Validates that channel_name alone does NOT satisfy reliable identity requirements.
    Without an About description or third-party categorisation, Channel Type is UNCLASSIFIED.
    When About description is present, Channel Type resolves.
    Player Focus evaluates independently on both runs.
    """
    db = MockSnowflakeDatabase()
    conn = db.get_connection()
    sync_strata(conn)

    db.dim_channels["ch-1"] = {"source_id": "UC_IDENTITY_TEST", "channel_name": "Famous Football Hub"}
    now = datetime(2022, 12, 17, 15, 0, 0, tzinfo=timezone.utc)
    for i in range(12):
        v_key = f"vid-{i}"
        db.dim_videos[v_key] = {
            "channel_key": "ch-1",
            "source_id": f"src-{i}",
            "published_at": (now - timedelta(days=i + 1)).isoformat(),
            "is_short": False
        }
        db.fact_video_snapshots.append({
            "video_key": v_key,
            "title": "Lionel Messi goal",
            "description": "Match highlights"
        })

    # Case A: Missing About description (empty string) -> Channel Type must be UNCLASSIFIED
    res_no_identity = classify_channel_assessment(
        conn, "frame-1", "ch-1", now,
        config_path=approved_test_config,
        about_description=""
    )
    assert res_no_identity["channel_type_value"] == "UNCLASSIFIED"
    assert res_no_identity["channel_type_confidence"] == "LOW"
    # Player focus evaluates independently!
    assert res_no_identity["player_focus_value"] == "MESSI_FOCUSED"

    # Case B: With reliable About description -> Channel Type resolves
    res_with_identity = classify_channel_assessment(
        conn, "frame-1", "ch-1", now,
        config_path=approved_test_config,
        about_description="Comprehensive football tactical analysis and creator channel."
    )
    assert res_with_identity["channel_type_value"] == "INDEPENDENT_CREATOR"
    assert res_with_identity["player_focus_value"] == "MESSI_FOCUSED"


def test_event_snapshot_immutability_and_full_payload_mutation_detection():
    """
    Blocker 4: Validates complete canonical snapshot payload comparison and fail-closed immutability:
    - Missing required statistics -> ValueError
    - Changed counts / prevalences with UNCHANGED labels -> SnapshotMutationViolationError
    - Changed reference period with UNCHANGED labels -> SnapshotMutationViolationError
    - Exact complete payload match -> IDEMPOTENT_NOOP
    """
    db = MockSnowflakeDatabase()
    conn = db.get_connection()

    base_assessment = {
        "channel_type_value": "INDEPENDENT_CREATOR",
        "player_focus_value": "MESSI_FOCUSED",
        "channel_type_confidence": "LOW",
        "player_focus_confidence": "HIGH",
        "fallback_used": False,
        "effective_window_days": 90,
        "eligible_video_count": 15,
        "messi_video_count": 12,
        "ronaldo_video_count": 0,
        "messi_prevalence": 0.8,
        "ronaldo_prevalence": 0.0,
        "focus_ratio": 13.0
    }
    ref_start = datetime(2022, 9, 18, 15, 0, 0, tzinfo=timezone.utc)
    ref_end = datetime(2022, 12, 17, 15, 0, 0, tzinfo=timezone.utc)

    # 1. Missing statistic raises ValueError
    incomplete_assessment = dict(base_assessment)
    del incomplete_assessment["messi_video_count"]
    with pytest.raises(ValueError) as val_err:
        create_or_verify_event_channel_snapshot(conn, "ev-1", "ch-1", "1.1", incomplete_assessment, ref_start, ref_end)
    assert "Missing required assessment statistic 'messi_video_count'" in str(val_err.value)

    # 2. Initial insert succeeds
    res_insert = create_or_verify_event_channel_snapshot(conn, "ev-1", "ch-1", "1.1", base_assessment, ref_start, ref_end)
    assert res_insert["status"] == "INSERTED"

    # 3. Exact match is IDEMPOTENT_NOOP
    res_noop = create_or_verify_event_channel_snapshot(conn, "ev-1", "ch-1", "1.1", base_assessment, ref_start, ref_end)
    assert res_noop["status"] == "IDEMPOTENT_NOOP"

    # 4. Changed statistics with UNCHANGED labels -> SnapshotMutationViolationError
    mutated_stats = dict(base_assessment)
    mutated_stats["messi_video_count"] = 14  # Count changed from 12 to 14
    mutated_stats["messi_prevalence"] = 0.93333
    with pytest.raises(SnapshotMutationViolationError) as exc_mut_stats:
        create_or_verify_event_channel_snapshot(conn, "ev-1", "ch-1", "1.1", mutated_stats, ref_start, ref_end)
    assert "Cannot mutate existing event snapshot" in str(exc_mut_stats.value)

    # 5. Changed reference period with UNCHANGED labels -> SnapshotMutationViolationError
    mutated_ref = ref_start - timedelta(days=1)
    with pytest.raises(SnapshotMutationViolationError) as exc_mut_ref:
        create_or_verify_event_channel_snapshot(conn, "ev-1", "ch-1", "1.1", base_assessment, mutated_ref, ref_end)
    assert "Cannot mutate existing event snapshot" in str(exc_mut_ref.value)


def test_acquisition_fallback_progression_lifecycle():
    """
    Blocker 5: Validates 90D -> 180D fallback progression:
    - completed 90D >= 10 videos -> no fallback initialized
    - completed 90D < 10 videos -> 180D fallback initialized
    - partial 90D < 10 videos -> no fallback yet
    """
    from channel_evidence_acquisition import execute_channel_evidence_harvest

    db = MockSnowflakeDatabase()
    conn = db.get_connection()

    now = datetime(2022, 12, 17, 15, 0, 0, tzinfo=timezone.utc)
    mock_yt = MagicMock()
    # Mock playlistItems returning empty list (stops immediately as COMPLETED)
    mock_yt.playlistItems().list().execute.return_value = {
        "items": [],
        "nextPageToken": None
    }
    mock_yt.channels().list().execute.return_value = {
        "items": [{"contentDetails": {"relatedPlaylists": {"uploads": "UU123"}}}]
    }

    # Case 1: Completed 90D with >= 10 videos (override returns 15) -> No fallback initialized
    db.eligible_count_override = 15
    res1 = execute_channel_evidence_harvest(
        conn, mock_yt, "frame-1", "ch-1", "UC123", now, scan_scope="90D_PRIMARY"
    )
    assert res1["status"] == "COMPLETED"
    assert res1["eligible_videos_observed"] == 15
    assert res1["fallback_initialized"] is False
    assert res1["fallback_state_key"] is None

    # Case 2: Completed 90D with < 10 videos (override returns 6) -> 180D fallback initialized
    db.eligible_count_override = 6
    res2 = execute_channel_evidence_harvest(
        conn, mock_yt, "frame-1", "ch-2", "UC456", now, scan_scope="90D_PRIMARY"
    )
    assert res2["status"] == "COMPLETED"
    assert res2["eligible_videos_observed"] == 6
    assert res2["fallback_initialized"] is True
    assert res2["fallback_state_key"] is not None
    # Confirm 180D_FALLBACK state exists in DB with PENDING
    assert ("frame-1", "ch-2", now.isoformat(), "180D_FALLBACK") in db.acquisition_states

    # Case 3: Partial 90D with < 10 videos (quota exception) -> No fallback yet
    mock_yt_err = MagicMock()
    mock_yt_err.channels().list().execute.return_value = {
        "items": [{"contentDetails": {"relatedPlaylists": {"uploads": "UU789"}}}]
    }
    from googleapiclient.errors import HttpError
    mock_resp = MagicMock(status=403, reason="Quota Exceeded")
    mock_yt_err.playlistItems().list().execute.side_effect = HttpError(
        resp=mock_resp, content=b'{"error": {"errors": [{"reason": "quotaExceeded"}]}}'
    )
    db.eligible_count_override = 3
    res3 = execute_channel_evidence_harvest(
        conn, mock_yt_err, "frame-1", "ch-3", "UC789", now, scan_scope="90D_PRIMARY"
    )
    assert res3["status"] == "PARTIAL_QUOTA_LIMIT"
    assert res3["fallback_initialized"] is False
    assert ("frame-1", "ch-3", now.isoformat(), "180D_FALLBACK") not in db.acquisition_states


def test_exclusion_buffer_human_approval_required():
    """
    Blocker 6: Validates that exclusion_buffer_hours is not silently frozen and requires human decision.
    """
    import yaml
    with open("config/channel_stratification.yml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    assert cfg.get("exclusion_buffer_hours") is None
    assert cfg.get("exclusion_buffer_hours_status") == "HUMAN_APPROVAL_REQUIRED"
    assert "exclusion_buffer_hours" not in cfg.get("frozen_parameters", {})


def test_unapproved_draft_dedup_version_fails_closed(tmp_path):
    """
    Blocker 1A: Validates that an unapproved draft deduplication rule version
    (e.g., deduplication_rule_version: 'draft_v1', deduplication_rule_status: 'HUMAN_APPROVAL_REQUIRED')
    cannot bypass fail-closed gating and raises DeduplicationRuleMissingError.
    """
    import yaml
    cfg_path = tmp_path / "unapproved_draft_config.yml"
    with open("config/channel_stratification.yml", "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    data["deduplication_rule_version"] = "draft_v1"
    data["deduplication_rule_status"] = "HUMAN_APPROVAL_REQUIRED"
    with open(cfg_path, "w", encoding="utf-8") as f:
        yaml.dump(data, f)

    db = MockSnowflakeDatabase()
    conn = db.get_connection()
    now = datetime(2022, 12, 17, 15, 0, 0, tzinfo=timezone.utc)
    db.dim_channels["ch-1"] = {"source_id": "UC1", "channel_name": "Test Draft Dedup"}

    with pytest.raises(DeduplicationRuleMissingError) as exc_info:
        classify_channel_assessment(
            conn, "frame-1", "ch-1", now,
            config_path=str(cfg_path),
            about_description="Football Channel"
        )
    assert "Cross-video deduplication methodology is required" in str(exc_info.value)
    assert "draft_v1" in str(exc_info.value)
    assert "HUMAN_APPROVAL_REQUIRED" in str(exc_info.value)


def test_approved_dedup_rule_execution(tmp_path):
    """
    Blocker 1B: Validates that when an approved versioned deduplication rule is configured,
    the registered rule algorithm executes cross-video deduplication across different video IDs,
    marking duplicate items with is_deduplicated_duplicate = True and deduplication_cluster_id,
    and excluding them from prevalence calculation.
    """
    import yaml
    rule_ver = "test_title_prefix_dedup_v1"

    @register_deduplication_rule(rule_ver)
    def title_prefix_dedup(items):
        seen_titles = {}
        for it in items:
            title_norm = it["title"].strip().lower()
            if title_norm in seen_titles:
                it["is_deduplicated_duplicate"] = True
                it["deduplication_cluster_id"] = seen_titles[title_norm]
                it["included_in_prevalence"] = False
            else:
                seen_titles[title_norm] = it["source_video_id"]
                it["deduplication_cluster_id"] = it["source_video_id"]
        return items

    cfg_path = tmp_path / "approved_rule_config.yml"
    with open("config/channel_stratification.yml", "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    data["deduplication_rule_version"] = rule_ver
    data["deduplication_rule_status"] = "APPROVED"
    with open(cfg_path, "w", encoding="utf-8") as f:
        yaml.dump(data, f)

    db = MockSnowflakeDatabase()
    conn = db.get_connection()
    sync_strata(conn)
    db.dim_channels["ch-1"] = {"source_id": "UC1", "channel_name": "Test Dedup Execution"}
    now = datetime(2022, 12, 17, 15, 0, 0, tzinfo=timezone.utc)

    # 12 videos, two have DIFFERENT source IDs but identical title (cross-video re-upload)
    for i in range(12):
        v_key = f"vid-{i}"
        title = "Identical Re-upload Title" if i in (0, 1) else f"Lionel Messi unique match {i}"
        db.dim_videos[v_key] = {
            "channel_key": "ch-1",
            "source_id": f"src-{i}",
            "published_at": (now - timedelta(days=i + 1)).isoformat(),
            "is_short": False
        }
        db.fact_video_snapshots.append({
            "video_key": v_key,
            "title": title,
            "description": "Footy"
        })

    res = classify_channel_assessment(
        conn, "frame-1", "ch-1", now,
        config_path=str(cfg_path),
        about_description="Football Channel"
    )
    # Total evaluated was 12, 1 was cross-video deduplicated -> eligible count should be 11
    assert res["eligible_video_count"] == 11
    # Verify evidence items in DB received deduplication_cluster_id
    dedup_items = [it for it in db.evidence_manifest_items if it[13] is True]  # is_deduplicated_duplicate is index 13
    assert len(dedup_items) == 1
    assert dedup_items[0][4] == "src-1"  # second video was flagged
    assert dedup_items[0][14] == "src-0"  # clustered to src-0


def test_sparse_evidence_retains_actual_statistics():
    """
    Blocker 2: Validates that when eligible videos < 10 (e.g. 9 Messi videos),
    classify_player_focus retains actual ground-truth statistics in the return dict
    and evidence payload rather than overwriting with fabricated zeros and ratio 1.0,
    while keeping the classification label safely UNCLASSIFIED and confidence LOW.
    """
    strat_cfg = {
        "frozen_parameters": {
            "smoothing_alpha": 1.0,
            "player_focus_ratio_threshold": 4.0,
            "player_prevalence_floor": 0.15
        }
    }
    now = datetime(2022, 12, 17, 15, 0, 0, tzinfo=timezone.utc)
    # 9 Messi videos, 0 Ronaldo videos
    videos = [
        {
            "video_key": f"v-{i}",
            "source_video_id": f"s-{i}",
            "messi_matched": True,
            "ronaldo_matched": False,
            "published_at_dt": now - timedelta(days=i + 1)
        }
        for i in range(9)
    ]

    res = classify_player_focus(
        eligible_videos=videos,
        reference_period_end=now,
        effective_window_days=90,
        strat_cfg=strat_cfg
    )

    assert res["player_focus"] == "UNCLASSIFIED"
    assert res["confidence"] == "LOW"
    # Ground-truth statistics MUST NOT be fabricated to 0
    assert res["messi_count"] == 9
    assert res["ronaldo_count"] == 0
    assert res["messi_prevalence"] == 1.0
    assert res["ronaldo_prevalence"] == 0.0
    assert res["focus_ratio"] == 10.0  # (9 + 1) / (0 + 1) = 10.0
    assert res["evidence"]["video_count"] == 9
    assert res["evidence"]["reason"] == "LESS_THAN_10_VIDEOS"
    assert res["evidence"]["messi_count"] == 9
    assert res["evidence"]["focus_ratio"] == 10.0


def test_event_channel_snapshot_roundtrip_ntz_timestamp_normalization():
    """
    Blocker 3: Validates that timezone-aware candidate timestamps and naive
    TIMESTAMP_NTZ returned by Snowflake connector are normalized consistently,
    preventing spurious SnapshotMutationViolationError on identical retries.
    """
    db = MockSnowflakeDatabase()
    conn = db.get_connection()
    now_aware = datetime(2022, 12, 17, 15, 0, 0, tzinfo=timezone.utc)
    start_aware = now_aware - timedelta(days=90)

    # Unit verification of normalizer helper across equivalent representations and offsets
    assert normalize_ntz_timestamp(now_aware) == "2022-12-17T15:00:00"
    assert normalize_ntz_timestamp(datetime(2022, 12, 17, 15, 0, 0)) == "2022-12-17T15:00:00"
    assert normalize_ntz_timestamp("2022-12-17T15:00:00+00:00") == "2022-12-17T15:00:00"
    assert normalize_ntz_timestamp("2022-12-17 15:00:00") == "2022-12-17T15:00:00"
    assert normalize_ntz_timestamp("2022-12-17T15:00:00Z") == "2022-12-17T15:00:00"

    # Equivalent offset strings and aware datetimes normalize consistently to UTC
    aware_plus_3 = datetime(2022, 12, 17, 18, 0, 0, tzinfo=timezone(timedelta(hours=3)))
    str_plus_3 = "2022-12-17T18:00:00+03:00"
    aware_minus_5 = datetime(2022, 12, 17, 10, 0, 0, tzinfo=timezone(timedelta(hours=-5)))
    str_minus_5 = "2022-12-17T10:00:00-05:00"
    assert normalize_ntz_timestamp(aware_plus_3) == "2022-12-17T15:00:00"
    assert normalize_ntz_timestamp(str_plus_3) == "2022-12-17T15:00:00"
    assert normalize_ntz_timestamp(aware_minus_5) == "2022-12-17T15:00:00"
    assert normalize_ntz_timestamp(str_minus_5) == "2022-12-17T15:00:00"

    # Fractional-second precision preservation (microseconds)
    aware_500ms = datetime(2022, 12, 17, 15, 0, 0, 500000, tzinfo=timezone.utc)
    naive_500ms = datetime(2022, 12, 17, 15, 0, 0, 500000)
    aware_plus_3_500ms = datetime(2022, 12, 17, 18, 0, 0, 500000, tzinfo=timezone(timedelta(hours=3)))
    str_500ms_z = "2022-12-17T15:00:00.500Z"
    str_500ms_offset = "2022-12-17T18:00:00.500+03:00"
    str_500ms_space = "2022-12-17 15:00:00.500"
    str_500000ms = "2022-12-17T15:00:00.500000"

    expected_500ms = "2022-12-17T15:00:00.500000"
    assert normalize_ntz_timestamp(aware_500ms) == expected_500ms
    assert normalize_ntz_timestamp(naive_500ms) == expected_500ms
    assert normalize_ntz_timestamp(aware_plus_3_500ms) == expected_500ms
    assert normalize_ntz_timestamp(str_500ms_z) == expected_500ms
    assert normalize_ntz_timestamp(str_500ms_offset) == expected_500ms
    assert normalize_ntz_timestamp(str_500ms_space) == expected_500ms
    assert normalize_ntz_timestamp(str_500000ms) == expected_500ms

    assessment = {
        "channel_type_value": "BROAD_REACH_PUBLISHER",
        "player_focus_value": "MESSI_FOCUSED",
        "channel_type_confidence": "HIGH",
        "player_focus_confidence": "HIGH",
        "effective_window_days": 90,
        "eligible_video_count": 25,
        "messi_video_count": 20,
        "ronaldo_video_count": 1,
        "messi_prevalence": 0.80,
        "ronaldo_prevalence": 0.04,
        "focus_ratio": 10.5,
        "fallback_used": False
    }

    # Initial insertion with timezone-aware datetimes
    res1 = create_or_verify_event_channel_snapshot(
        conn, "ev-1", "ch-1", "1.1", assessment, start_aware, now_aware
    )
    assert res1["status"] == "INSERTED"

    # Simulate Snowflake connector returning naive TIMESTAMP_NTZ datetime
    snapshot_key = ("ev-1", "ch-1", "1.1")
    stored = db.event_snapshots[snapshot_key]
    stored["reference_period_start"] = datetime(2022, 9, 18, 15, 0, 0)  # naive datetime from DB
    stored["reference_period_end"] = datetime(2022, 12, 17, 15, 0, 0)    # naive datetime from DB

    # Second call with timezone-aware candidate datetimes should succeed idempotently
    res2 = create_or_verify_event_channel_snapshot(
        conn, "ev-1", "ch-1", "1.1", assessment, start_aware, now_aware
    )
    assert res2["status"] == "IDEMPOTENT_NOOP"
    assert res2["snapshot_key"] == res1["snapshot_key"]

    # Third call with equivalent offset string (+03:00) should also succeed idempotently
    res3 = create_or_verify_event_channel_snapshot(
        conn, "ev-1", "ch-1", "1.1", assessment, "2022-09-18T18:00:00+03:00", "2022-12-17T18:00:00+03:00"
    )
    assert res3["status"] == "IDEMPOTENT_NOOP"


def test_snapshot_immutability_violates_on_subsecond_drift():
    """
    Validates that changing candidate reference period end by 500 milliseconds
    triggers SnapshotMutationViolationError instead of returning IDEMPOTENT_NOOP,
    protecting snapshot immutability against subsecond drift.
    """
    db = MockSnowflakeDatabase()
    conn = db.get_connection()
    now_aware = datetime(2022, 12, 17, 15, 0, 0, tzinfo=timezone.utc)
    start_aware = now_aware - timedelta(days=90)

    assessment = {
        "channel_type_value": "BROAD_REACH_PUBLISHER",
        "player_focus_value": "MESSI_FOCUSED",
        "channel_type_confidence": "HIGH",
        "player_focus_confidence": "HIGH",
        "effective_window_days": 90,
        "eligible_video_count": 25,
        "messi_video_count": 20,
        "ronaldo_video_count": 1,
        "messi_prevalence": 0.80,
        "ronaldo_prevalence": 0.04,
        "focus_ratio": 10.5,
        "fallback_used": False
    }

    # Initial insert with 0 microseconds
    res1 = create_or_verify_event_channel_snapshot(
        conn, "ev-sub", "ch-sub", "1.1", assessment, start_aware, now_aware
    )
    assert res1["status"] == "INSERTED"

    # Attempt re-verification with reference-period end drifted by 500ms
    drifted_end_aware = now_aware + timedelta(milliseconds=500)
    with pytest.raises(SnapshotMutationViolationError) as exc_info:
        create_or_verify_event_channel_snapshot(
            conn, "ev-sub", "ch-sub", "1.1", assessment, start_aware, drifted_end_aware
        )
    assert "Cannot mutate existing event snapshot" in str(exc_info.value)
    assert "2022-12-17T15:00:00.500000" in str(exc_info.value)

    # Attempt re-verification with string containing 500ms fraction
    with pytest.raises(SnapshotMutationViolationError) as exc_info2:
        create_or_verify_event_channel_snapshot(
            conn, "ev-sub", "ch-sub", "1.1", assessment, start_aware, "2022-12-17T15:00:00.500Z"
        )
    assert "Cannot mutate existing event snapshot" in str(exc_info2.value)


def test_focus_ratio_supports_values_exceeding_ten():
    """
    Blocker 4: Validates that focus ratios > 9.99999 (e.g. 10 Messi, 0 Ronaldo -> 11.0)
    are supported by the classifier, snapshot logic, and persisted column definitions (NUMBER(10,5)).
    """
    strat_cfg = {
        "frozen_parameters": {
            "smoothing_alpha": 1.0,
            "player_focus_ratio_threshold": 4.0,
            "player_prevalence_floor": 0.15
        }
    }
    now = datetime(2022, 12, 17, 15, 0, 0, tzinfo=timezone.utc)
    # 10 Messi videos, 0 Ronaldo videos -> ratio = (10 + 1) / (0 + 1) = 11.0
    videos = [
        {
            "video_key": f"v-{i}",
            "source_video_id": f"s-{i}",
            "messi_matched": True,
            "ronaldo_matched": False,
            "published_at_dt": now - timedelta(days=i + 1)
        }
        for i in range(10)
    ]
    res = classify_player_focus(
        eligible_videos=videos,
        reference_period_end=now,
        effective_window_days=90,
        strat_cfg=strat_cfg
    )
    assert res["focus_ratio"] == 11.0
    assert res["player_focus"] == "MESSI_FOCUSED"

    # Verify DDL column specifications use NUMBER(10,5)
    with open("infra/snowflake/ddl/02_ops.sql", "r", encoding="utf-8") as f:
        ops_ddl = f.read()
    assert "focus_ratio NUMBER(10,5)" in ops_ddl

    with open("infra/snowflake/ddl/05_core_bridges.sql", "r", encoding="utf-8") as f:
        core_ddl = f.read()
    assert "focus_ratio NUMBER(10,5)" in core_ddl

    with open("infra/snowflake/migrations/V009__channel_stratification_and_overrides.sql", "r", encoding="utf-8") as f:
        v009 = f.read()
    assert "focus_ratio NUMBER(10,5)" in v009


def test_v009_idempotent_constraint_replay():
    """
    Validates that V009 migration handles uq_event_channel_stratum_snapshot
    idempotently, suppressing only expected duplicate constraint errors (SQLSTATE 42710 / SQLCODE 2002 / already exists)
    and re-raising all other failures via Snowflake exception semantics.
    """
    with open("infra/snowflake/migrations/V009__channel_stratification_and_overrides.sql", "r", encoding="utf-8") as f:
        v009 = f.read()

    assert "EXECUTE IMMEDIATE '" in v009
    assert "uq_event_channel_stratum_snapshot" in v009
    assert "EXCEPTION" in v009
    assert "WHEN OTHER THEN" in v009
    assert "RAISE;" in v009
    assert "already exists" in v009
    assert "SQLSTATE = ''42710''" in v009
    assert "WHEN OTHER THEN\n        NULL;" not in v009
