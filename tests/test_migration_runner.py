"""Unit tests for Snowflake Migration Runner and Ledger Management.

Governing Specification: docs/migration-runner-specification.md
Layer Invariant: OPS.SCHEMA_VERSION tracks all applied migrations with SHA-256
checksums, and OPS.SCHEMA_LOCK coordinates distributed execution.
"""

from __future__ import annotations

import datetime
import os
import uuid
from typing import Any, Callable, Dict, List, Optional, Set
from unittest.mock import MagicMock

import pytest

from migration_runner import (
    BaselineAdoptionError,
    ConcurrentMigrationError,
    InterruptedMigrationError,
    LockLostError,
    MigrationChecksumMismatchError,
    MigrationExecutionError,
    MigrationRunner,
    UnresolvedFailedMigrationError,
    compute_file_checksum,
)

ALL_BASELINE_OBJECTS = {
    "DIM_CHANNEL",
    "DIM_EVENT",
    "DIM_VIDEO",
    "FACT_API_REQUEST",
    "FACT_INGESTION_RUN",
    "BRIDGE_VIDEO_EVENT",
    "DIM_CHANNEL_FRAME_VERSION",
    "BRIDGE_FRAME_CHANNEL",
    "FRAME_CHANNEL_SOURCE_OBSERVATION",
    "DIM_EVENT_VERSION",
    "DISCOVERY_UNIT_STATE",
    "VIDEO_METADATA_RESOLUTION_STATE",
    "FACT_VIDEO_SNAPSHOT",
    "FACT_CHANNEL_ASSESSMENT_EVIDENCE_ITEM",
    "CHANNEL_EVIDENCE_ACQUISITION_STATE",
    "DIM_CHANNEL_STRATUM",
    "BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT",
    "FACT_CHANNEL_CLASSIFICATION_ASSESSMENT",
    "MANUAL_OVERRIDE",
    "YOUTUBE_API_RESPONSE",
    "CURRENT_CHECKPOINT",
    "CHECKPOINT_HISTORY",
}


class MockSnowflakeDatabase:
    """In-memory mock database simulating Snowflake behavior and catalog."""

    def __init__(
        self,
        existing_tables: Optional[Set[str]] = None,
        target_db: str = "FOOTBALL_NARRATIVE_DEV",
    ) -> None:
        self.target_db = target_db
        if existing_tables is None:
            self.existing_tables = set(ALL_BASELINE_OBJECTS)
        else:
            self.existing_tables = set(existing_tables)

        self.strata_seed_count: int = 30
        self.missing_columns: Set[str] = set()
        self.schema_version: List[Dict[str, Any]] = []
        self.schema_lock: Optional[Dict[str, Any]] = None
        self.executed_strings: List[str] = []
        self.executed_queries: List[tuple] = []
        self.created_tables: Set[str] = set()
        self.execute_string_handler: Optional[Callable[[str], None]] = None
        self._last_result: List[tuple] = []
        self.rowcount: int = 0

    @property
    def is_locked(self) -> bool:
        return self.schema_lock is not None and self.schema_lock.get("locked_by") is not None

    def get_cursor(self) -> MagicMock:
        cursor = MagicMock()
        cursor.execute.side_effect = lambda sql, params=(): self.execute(sql, params, cursor)
        cursor.fetchone.side_effect = self.fetchone
        cursor.fetchall.side_effect = self.fetchall
        cursor.close.return_value = None
        cursor.rowcount = 0
        return cursor

    def get_connection(self) -> MagicMock:
        conn = MagicMock()
        conn.cursor.side_effect = self.get_cursor
        conn.execute_string.side_effect = self.execute_string
        return conn

    def execute_string(self, sql_content: str) -> None:
        self.executed_strings.append(sql_content)
        if self.execute_string_handler is not None:
            self.execute_string_handler(sql_content)

    def execute(self, sql: str, params: tuple = (), cursor: Optional[MagicMock] = None) -> None:
        self.executed_queries.append((sql, params))
        clean = " ".join(sql.strip().split())
        clean_upper = clean.upper()

        # 1. Atomic UPDATE lock acquisition
        if clean_upper.startswith("UPDATE") and "OPS.SCHEMA_LOCK" in clean_upper and "LOCKED_AT = CURRENT_TIMESTAMP()" in clean_upper and "LOCKED_BY =" in clean_upper:
            user_id = params[0]
            token = params[1]
            lease_sec = params[2]
            lock_id = params[3]

            is_unlocked = self.schema_lock is None or self.schema_lock.get("locked_by") is None
            is_expired = False
            if self.schema_lock and self.schema_lock.get("locked_at"):
                age = (datetime.datetime.now(datetime.timezone.utc) - self.schema_lock["locked_at"]).total_seconds()
                if age >= self.schema_lock.get("lock_timeout_sec", 900):
                    is_expired = True

            if is_unlocked or is_expired:
                self.schema_lock = {
                    "lock_id": lock_id,
                    "locked_at": datetime.datetime.now(datetime.timezone.utc),
                    "locked_by": user_id,
                    "lock_token": token,
                    "lock_timeout_sec": lease_sec,
                }
                self.rowcount = 1
            else:
                self.rowcount = 0
            self._last_result = []

        # 1b. Lease renewal by active holder
        elif clean_upper.startswith("UPDATE") and "OPS.SCHEMA_LOCK" in clean_upper and "LOCKED_AT = CURRENT_TIMESTAMP()" in clean_upper and "LOCKED_BY =" not in clean_upper:
            lock_id = params[0]
            token = params[1]
            if self.schema_lock and self.schema_lock.get("lock_id") == lock_id and self.schema_lock.get("lock_token") == token:
                self.schema_lock["locked_at"] = datetime.datetime.now(datetime.timezone.utc)
                self.rowcount = 1
            else:
                self.rowcount = 0
            self._last_result = []

        # 2. Release lock with matching token (UPDATE SET locked_at = NULL ... WHERE lock_id = %s AND lock_token = %s)
        elif clean_upper.startswith("UPDATE") and "OPS.SCHEMA_LOCK" in clean_upper and "LOCKED_AT = NULL" in clean_upper:
            lock_id = params[0]
            token = params[1]
            if self.schema_lock and self.schema_lock.get("lock_id") == lock_id and self.schema_lock.get("lock_token") == token:
                self.schema_lock["locked_by"] = None
                self.schema_lock["lock_token"] = None
                self.schema_lock["locked_at"] = None
                self.rowcount = 1
            else:
                self.rowcount = 0
            self._last_result = []

        # 3. Query current lock holder (SELECT locked_by, lock_token, locked_at, lock_timeout_sec FROM ... OPS.SCHEMA_LOCK)
        elif clean_upper.startswith("SELECT") and "OPS.SCHEMA_LOCK" in clean_upper and "WHERE LOCK_ID =" in clean_upper:
            if self.schema_lock and self.schema_lock.get("lock_id") == params[0]:
                self._last_result = [(
                    self.schema_lock.get("locked_by"),
                    self.schema_lock.get("lock_token"),
                    self.schema_lock.get("locked_at"),
                    self.schema_lock.get("lock_timeout_sec", 900),
                )]
            else:
                self._last_result = []
            self.rowcount = len(self._last_result)

        # 3b. Query seed rows in DIM_CHANNEL_STRATUM
        elif "CORE.DIM_CHANNEL_STRATUM" in clean_upper and "COUNT(*)" in clean_upper:
            if "DIM_CHANNEL_STRATUM" in self.existing_tables:
                self._last_result = [(self.strata_seed_count,)]
            else:
                self._last_result = [(0,)]
            self.rowcount = 1

        # 4. Information schema columns
        elif "INFORMATION_SCHEMA.COLUMNS" in clean_upper:
            if "PAGE_TOKEN_USED" in clean_upper:
                has_col = any("FACT_API_REQUEST" in t for t in self.existing_tables) and "PAGE_TOKEN_USED" not in self.missing_columns
                self._last_result = [(1024,)] if has_col else []
            elif "RUN_PURPOSE" in clean_upper:
                count = 1 if any("FACT_INGESTION_RUN" in t for t in self.existing_tables) and "RUN_PURPOSE" not in self.missing_columns else 0
                self._last_result = [(count,)]
            elif "PRIMARY_EXCLUSION_REASON" in clean_upper:
                count = 3 if any("BRIDGE_VIDEO_EVENT" in t for t in self.existing_tables) and "PRIMARY_EXCLUSION_REASON" not in self.missing_columns else 0
                self._last_result = [(count,)]
            elif "EVENT_TERMS" in clean_upper and "BASELINE_TERMS" not in clean_upper:
                count = 1 if any("DIM_EVENT_VERSION" in t for t in self.existing_tables) and "EVENT_TERMS" not in self.missing_columns else 0
                self._last_result = [(count,)]
            elif "LAST_ERROR_CODE" in clean_upper:
                count = 2 if any("DISCOVERY_UNIT_STATE" in t for t in self.existing_tables) and "LAST_ERROR_CODE" not in self.missing_columns else 0
                self._last_result = [(count,)]
            elif "API_REQUEST_ID" in clean_upper and "FACT_VIDEO_SNAPSHOT" in clean_upper:
                count = 2 if any("FACT_VIDEO_SNAPSHOT" in t for t in self.existing_tables) and "API_REQUEST_ID" not in self.missing_columns else 0
                self._last_result = [(count,)]
            elif "BASELINE_TERMS" in clean_upper:
                count = 1 if any("DIM_EVENT_VERSION" in t for t in self.existing_tables) and "BASELINE_TERMS" not in self.missing_columns else 0
                self._last_result = [(count,)]
            elif "HEADLINE_DECOMPOSITION_ELIGIBLE" in clean_upper:
                count = 1 if any("BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT" in t for t in self.existing_tables) and "HEADLINE_DECOMPOSITION_ELIGIBLE" not in self.missing_columns else 0
                self._last_result = [(count,)]
            elif "FOCUS_RATIO" in clean_upper and "NUMERIC_SCALE" in clean_upper:
                scale = 5 if any("BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT" in t for t in self.existing_tables) and "FOCUS_RATIO_SCALE" not in self.missing_columns else 4
                self._last_result = [(scale,)]
            elif "FACT_CHANNEL_CLASSIFICATION_ASSESSMENT" in clean_upper and "ASSESSMENT_TRIGGER" in clean_upper:
                count = 20 if any("FACT_CHANNEL_CLASSIFICATION_ASSESSMENT" in t for t in self.existing_tables) and "ASSESSMENT_TRIGGER" not in self.missing_columns else 0
                self._last_result = [(count,)]
            elif "MANUAL_OVERRIDE" in clean_upper and "CLASSIFICATION_AXIS_OVERRIDDEN" in clean_upper:
                count = 7 if any("MANUAL_OVERRIDE" in t for t in self.existing_tables) and "CLASSIFICATION_AXIS_OVERRIDDEN" not in self.missing_columns else 0
                self._last_result = [(count,)]
            else:
                self._last_result = [(1,)]
            self.rowcount = len(self._last_result)

        # 5. Information schema tables
        elif "INFORMATION_SCHEMA.TABLES" in clean_upper:
            if "VIDEO_METADATA_RESOLUTION_STATE" in clean_upper and "FACT_CHANNEL_ASSESSMENT_EVIDENCE_ITEM" not in clean_upper:
                count = 1 if any("VIDEO_METADATA_RESOLUTION_STATE" in t for t in self.existing_tables) else 0
                self._last_result = [(count,)]
            elif "FACT_CHANNEL_ASSESSMENT_EVIDENCE_ITEM" in clean_upper:
                matched_count = sum(
                    1 for t in ["FACT_CHANNEL_ASSESSMENT_EVIDENCE_ITEM", "CHANNEL_EVIDENCE_ACQUISITION_STATE", "DIM_CHANNEL_STRATUM"]
                    if t in self.existing_tables
                )
                self._last_result = [(matched_count,)]
            elif "DIM_CHANNEL_FRAME_VERSION" in clean_upper:
                matched_count = sum(
                    1 for t in ["DIM_CHANNEL_FRAME_VERSION", "BRIDGE_FRAME_CHANNEL", "FRAME_CHANNEL_SOURCE_OBSERVATION"]
                    if t in self.existing_tables
                )
                self._last_result = [(matched_count,)]
            elif "DISCOVERY_UNIT_STATE" in clean_upper:
                count = 1 if any("DISCOVERY_UNIT_STATE" in t for t in self.existing_tables) else 0
                self._last_result = [(count,)]
            else:
                matched_count = sum(1 for t in ["DIM_CHANNEL", "DIM_EVENT", "DIM_VIDEO"] if t in self.existing_tables)
                self._last_result = [(matched_count,)]
            self.rowcount = len(self._last_result)

        # 6. Retrieve ledger entries
        elif (
            "FROM" in clean
            and "OPS.SCHEMA_VERSION" in clean
            and "ORDER BY INSTALLED_RANK" in clean_upper
        ):
            rows = []
            for e in sorted(
                self.schema_version, key=lambda x: x["installed_rank"]
            ):
                rows.append((
                    e["installed_rank"],
                    e["version"],
                    e["description"],
                    e["script_name"],
                    e["checksum"],
                    e["execution_type"],
                    e["installed_by"],
                    e["installed_on"],
                    e["execution_time_ms"],
                    e["status"],
                    e["error_message"],
                ))
            self._last_result = rows
            self.rowcount = len(rows)

        # 7. Ledger insert
        elif "INSERT INTO" in clean and "OPS.SCHEMA_VERSION" in clean:
            self.schema_version.append({
                "installed_rank": params[0],
                "version": params[1],
                "description": params[2],
                "script_name": params[3],
                "checksum": params[4],
                "execution_type": params[5],
                "installed_by": params[6],
                "installed_on": params[7],
                "execution_time_ms": params[8],
                "status": params[9],
                "error_message": params[10],
            })
            self.rowcount = 1
            self._last_result = []

        # 8. Update ledger status
        elif "UPDATE" in clean and "OPS.SCHEMA_VERSION" in clean:
            if "STATUS = 'SUCCESS'" in clean_upper:
                ver = params[1]
                matched = next(
                    (e for e in self.schema_version if e["version"] == ver),
                    None,
                )
                if matched:
                    matched["status"] = "SUCCESS"
                    matched["execution_time_ms"] = params[0]
                    matched["error_message"] = None
            elif "STATUS = 'FAILED'" in clean_upper:
                ver = params[2]
                matched = next(
                    (e for e in self.schema_version if e["version"] == ver),
                    None,
                )
                if matched:
                    matched["status"] = "FAILED"
                    matched["execution_time_ms"] = params[0]
                    matched["error_message"] = params[1]
            self.rowcount = 1
            self._last_result = []

        else:
            self.rowcount = 0
            self._last_result = []

        if cursor is not None:
            cursor.rowcount = self.rowcount

    def fetchone(self) -> Optional[tuple]:
        if not self._last_result:
            return None
        return self._last_result[0]

    def fetchall(self) -> List[tuple]:
        if not self._last_result:
            return []
        return list(self._last_result)


def test_discover_migrations(tmp_path: Any) -> None:
    """Discovers V001..V010 in ascending integer order; verifies checksums."""
    # 1. Discover actual repository migrations
    real_migrations_dir = os.path.join(
        os.path.dirname(__file__), "..", "infra", "snowflake", "migrations"
    )
    if os.path.exists(real_migrations_dir):
        mock_db = MockSnowflakeDatabase()
        runner = MigrationRunner(
            mock_db.get_connection(),
            migrations_dir=real_migrations_dir,
        )
        discovered = runner.discover_migrations()
        assert len(discovered) == 10
        expected_versions = [f"V{i:03d}" for i in range(1, 11)]
        assert [m.version_str for m in discovered] == expected_versions
        assert [m.version_num for m in discovered] == list(range(1, 11))

        for m in discovered:
            assert os.path.isfile(m.filepath)
            expected_hash = compute_file_checksum(m.filepath)
            assert m.checksum == expected_hash
            assert len(m.checksum) == 64
            assert int(m.checksum, 16) >= 0
            assert m.filename.startswith(m.version_str)
            assert m.filename.endswith(".sql")

    # 2. Verify sorting by integer order and filtering non-matching files
    synth_dir = tmp_path / "migrations_synth"
    synth_dir.mkdir()
    (synth_dir / "V10__ten.sql").write_text("SELECT 10;", encoding="utf-8")
    (synth_dir / "V2__two.sql").write_text("SELECT 2;", encoding="utf-8")
    (synth_dir / "V001__one.sql").write_text("SELECT 1;", encoding="utf-8")
    (synth_dir / "README.md").write_text("# Documentation", encoding="utf-8")
    (synth_dir / "helper.sql").write_text("SELECT 0;", encoding="utf-8")
    (synth_dir / "v3_bad_prefix.sql").write_text("SELECT 3;", encoding="utf-8")

    mock_db = MockSnowflakeDatabase()
    runner = MigrationRunner(
        mock_db.get_connection(),
        migrations_dir=str(synth_dir),
    )
    discovered = runner.discover_migrations()
    assert len(discovered) == 3
    # Strict integer sorting: 1, 2, 10
    assert [m.version_num for m in discovered] == [1, 2, 10]
    assert [m.version_str for m in discovered] == ["V001", "V002", "V010"]
    assert discovered[0].description == "one"
    assert discovered[1].description == "two"
    assert discovered[2].description == "ten"


def test_fresh_database_execution(tmp_path: Any) -> None:
    """On fresh DB, ensure_ledger_tables runs, executes migrations, releases lock."""
    migrations_dir = tmp_path / "fresh_migrations"
    migrations_dir.mkdir()
    for i in range(1, 4):
        p = migrations_dir / f"V{i:03d}__feature_{i}.sql"
        p.write_text(f"CREATE TABLE T_{i} (id INT);", encoding="utf-8")

    mock_db = MockSnowflakeDatabase()
    assert len(mock_db.schema_version) == 0
    assert not mock_db.is_locked

    runner = MigrationRunner(
        mock_db.get_connection(),
        target_db="FOOTBALL_NARRATIVE_DEV",
        migrations_dir=str(migrations_dir),
    )

    result = runner.run_migrations()

    # Verify return summary
    assert result["status"] == "SUCCESS"
    assert result["applied_count"] == 3
    assert result["applied_versions"] == ["V001", "V002", "V003"]
    assert result["total_migrations"] == 3

    # Verify ledger tables created
    executed_all = " ".join(mock_db.executed_strings)
    assert (
        "CREATE TABLE IF NOT EXISTS FOOTBALL_NARRATIVE_DEV.OPS.SCHEMA_VERSION"
        in executed_all
    )
    assert (
        "CREATE TABLE IF NOT EXISTS FOOTBALL_NARRATIVE_DEV.OPS.SCHEMA_LOCK"
        in executed_all
    )

    # Verify lock lifecycle: lock acquired then released
    assert not mock_db.is_locked

    # Verify ledger rows in OPS.SCHEMA_VERSION
    assert len(mock_db.schema_version) == 3
    for idx, entry in enumerate(mock_db.schema_version, start=1):
        v_str = f"V{idx:03d}"
        file_path = str(migrations_dir / f"{v_str}__feature_{idx}.sql")
        expected_hash = compute_file_checksum(file_path)

        assert entry["installed_rank"] == idx
        assert entry["version"] == v_str
        assert entry["status"] == "SUCCESS"
        assert entry["execution_type"] == "MIGRATION"
        assert entry["error_message"] is None
        assert entry["execution_time_ms"] >= 0
        assert entry["checksum"] == expected_hash


def test_existing_database_baseline_adoption(tmp_path: Any) -> None:
    """adopt_baseline records migrations as BASELINE_ADOPTION; checks core tables."""
    migrations_dir = tmp_path / "baseline_migrations"
    migrations_dir.mkdir()
    for i in range(1, 11):
        p = migrations_dir / f"V{i:03d}__step_{i}.sql"
        p.write_text(f"CREATE TABLE STEP_{i} (id INT);", encoding="utf-8")

    # 1. Rejection when core tables are missing (only 2 exist)
    mock_db_missing = MockSnowflakeDatabase(
        existing_tables={"DIM_CHANNEL", "DIM_EVENT"}
    )
    runner_missing = MigrationRunner(
        mock_db_missing.get_connection(),
        migrations_dir=str(migrations_dir),
    )
    with pytest.raises(BaselineAdoptionError) as exc_info:
        runner_missing.adopt_baseline(up_to_version_num=9)
    assert "Cannot adopt baseline" in str(exc_info.value)
    assert len(mock_db_missing.schema_version) == 0
    assert not mock_db_missing.is_locked

    # 2. Successful adoption when core tables exist
    mock_db = MockSnowflakeDatabase(
        existing_tables=ALL_BASELINE_OBJECTS
    )
    runner = MigrationRunner(
        mock_db.get_connection(),
        migrations_dir=str(migrations_dir),
    )
    adopt_result = runner.adopt_baseline(
        up_to_version_num=9, installed_by="SYS_BASELINE"
    )

    assert adopt_result["status"] == "BASELINE_ADOPTED"
    assert adopt_result["adopted_count"] == 9
    assert adopt_result["up_to_version"] == "V009"
    assert adopt_result["adopted_versions"] == [
        f"V{i:03d}" for i in range(1, 10)
    ]
    assert not mock_db.is_locked

    # Verify ledger records
    assert len(mock_db.schema_version) == 9
    for idx, entry in enumerate(mock_db.schema_version, start=1):
        v_str = f"V{idx:03d}"
        assert entry["installed_rank"] == idx
        assert entry["version"] == v_str
        assert entry["status"] == "SUCCESS"
        assert entry["execution_type"] == "BASELINE_ADOPTION"
        assert entry["installed_by"] == "SYS_BASELINE"
        assert entry["execution_time_ms"] == 0
        assert entry["error_message"] is None

    # 3. Subsequent migration executes only pending V010
    migrate_result = runner.run_migrations()
    assert migrate_result["status"] == "SUCCESS"
    assert migrate_result["applied_count"] == 1
    assert migrate_result["applied_versions"] == ["V010"]

    ledger = runner.get_ledger()
    assert ledger["V010"]["execution_type"] == "MIGRATION"
    assert ledger["V010"]["status"] == "SUCCESS"
    assert ledger["V009"]["execution_type"] == "BASELINE_ADOPTION"


def test_checksum_mismatch_fails_closed(tmp_path: Any) -> None:
    """If applied migration is altered on disk, raises error; applies zero more."""
    migrations_dir = tmp_path / "checksum_migrations"
    migrations_dir.mkdir()
    f1 = migrations_dir / "V001__table_a.sql"
    f2 = migrations_dir / "V002__table_b.sql"
    f1.write_text("CREATE TABLE A (id INT);", encoding="utf-8")
    f2.write_text("CREATE TABLE B (id INT);", encoding="utf-8")

    mock_db = MockSnowflakeDatabase()
    runner = MigrationRunner(
        mock_db.get_connection(),
        migrations_dir=str(migrations_dir),
    )

    # Initial migration succeeds for V001
    runner.run_migrations(target_version_num=1)
    assert len(mock_db.schema_version) == 1
    assert mock_db.schema_version[0]["version"] == "V001"
    orig_hash = mock_db.schema_version[0]["checksum"]

    # Tamper with V001 file on disk
    f1.write_text("CREATE TABLE A (id INT, tampered INT);", encoding="utf-8")
    new_hash = compute_file_checksum(str(f1))
    assert orig_hash != new_hash

    executed_count_before = len(mock_db.executed_strings)

    # Subsequent run must fail closed
    with pytest.raises(MigrationChecksumMismatchError) as exc_info:
        runner.run_migrations()

    err_msg = str(exc_info.value)
    assert "Checksum mismatch for migration V001" in err_msg
    assert orig_hash in err_msg
    assert new_hash in err_msg
    assert "Applied migrations are immutable" in err_msg

    # Verify zero subsequent migrations applied (V002 never executed)
    executed_since = mock_db.executed_strings[executed_count_before:]
    assert not any("CREATE TABLE B" in s for s in executed_since)
    assert "V002" not in [e["version"] for e in mock_db.schema_version]
    assert not mock_db.is_locked


def test_partially_applied_migration_records_failure(tmp_path: Any) -> None:
    """Halfway failure updates ledger to FAILED, releases lock, raises error."""
    migrations_dir = tmp_path / "failure_migrations"
    migrations_dir.mkdir()
    (migrations_dir / "V001__first.sql").write_text(
        "SELECT 1;", encoding="utf-8"
    )
    (migrations_dir / "V002__fails.sql").write_text(
        "SYNTAX ERROR HERE;", encoding="utf-8"
    )
    (migrations_dir / "V003__third.sql").write_text(
        "SELECT 3;", encoding="utf-8"
    )

    mock_db = MockSnowflakeDatabase()

    def simulate_failure(sql: str) -> None:
        if "SYNTAX ERROR HERE" in sql:
            raise RuntimeError("001003 (42000): SQL compilation error")

    mock_db.execute_string_handler = simulate_failure

    runner = MigrationRunner(
        mock_db.get_connection(),
        migrations_dir=str(migrations_dir),
    )

    with pytest.raises(MigrationExecutionError) as exc_info:
        runner.run_migrations()

    err_msg = str(exc_info.value)
    assert "Failed executing migration V002" in err_msg
    assert isinstance(exc_info.value.__cause__, RuntimeError)

    # Verify ledger state
    ledger = {e["version"]: e for e in mock_db.schema_version}
    assert "V001" in ledger
    assert ledger["V001"]["status"] == "SUCCESS"

    assert "V002" in ledger
    assert ledger["V002"]["status"] == "FAILED"
    assert "SQL compilation error" in ledger["V002"]["error_message"]
    assert ledger["V002"]["execution_time_ms"] >= 0

    # V003 was never started or executed
    assert "V003" not in ledger
    assert not mock_db.is_locked


def test_unresolved_failed_migration_blocks_execution(tmp_path: Any) -> None:
    """Prior FAILED migration blocks execution with UnresolvedFailedMigrationError."""
    migrations_dir = tmp_path / "unresolved_migrations"
    migrations_dir.mkdir()
    f1 = migrations_dir / "V001__init.sql"
    f2 = migrations_dir / "V002__broken.sql"
    f3 = migrations_dir / "V003__subsequent.sql"
    f1.write_text("SELECT 1;", encoding="utf-8")
    f2.write_text("SELECT 2;", encoding="utf-8")
    f3.write_text("SELECT 3;", encoding="utf-8")

    mock_db = MockSnowflakeDatabase()
    # Pre-populate ledger with V001 SUCCESS and V002 FAILED
    mock_db.schema_version = [
        {
            "installed_rank": 1,
            "version": "V001",
            "description": "init",
            "script_name": "V001__init.sql",
            "checksum": compute_file_checksum(str(f1)),
            "execution_type": "MIGRATION",
            "installed_by": "DEV",
            "installed_on": "2026-10-01 10:00:00",
            "execution_time_ms": 15,
            "status": "SUCCESS",
            "error_message": None,
        },
        {
            "installed_rank": 2,
            "version": "V002",
            "description": "broken",
            "script_name": "V002__broken.sql",
            "checksum": compute_file_checksum(str(f2)),
            "execution_type": "MIGRATION",
            "installed_by": "DEV",
            "installed_on": "2026-10-01 10:01:00",
            "execution_time_ms": 5,
            "status": "FAILED",
            "error_message": "Table already exists",
        },
    ]

    runner = MigrationRunner(
        mock_db.get_connection(),
        migrations_dir=str(migrations_dir),
    )

    executed_count_before = len(mock_db.executed_strings)

    with pytest.raises(UnresolvedFailedMigrationError) as exc_info:
        runner.run_migrations()

    err_msg = str(exc_info.value)
    assert (
        "Migration V002 previously failed: Table already exists" in err_msg
    )
    assert "Manual operator intervention required" in err_msg

    # Verify zero migration scripts were executed
    migration_ddls = [
        s for s in mock_db.executed_strings[executed_count_before:]
        if "SELECT 1" in s or "SELECT 2" in s or "SELECT 3" in s
    ]
    assert len(migration_ddls) == 0
    assert not mock_db.is_locked


def test_repeated_invocation_is_idempotent_noop(tmp_path: Any) -> None:
    """When migrations are applied and match, returns UP_TO_DATE with 0 DDL."""
    migrations_dir = tmp_path / "idempotent_migrations"
    migrations_dir.mkdir()
    for i in range(1, 4):
        p = migrations_dir / f"V{i:03d}__feature_{i}.sql"
        p.write_text(f"CREATE TABLE F_{i} (id INT);", encoding="utf-8")

    mock_db = MockSnowflakeDatabase()
    runner = MigrationRunner(
        mock_db.get_connection(),
        migrations_dir=str(migrations_dir),
    )

    # Initial migration
    first_res = runner.run_migrations()
    assert first_res["status"] == "SUCCESS"
    assert first_res["applied_count"] == 3

    # Reset executed statements tracking
    mock_db.executed_strings.clear()

    # Second invocation
    second_res = runner.run_migrations()
    assert second_res["status"] == "UP_TO_DATE"
    assert second_res["applied_count"] == 0
    assert second_res["applied_versions"] == []
    assert second_res["total_migrations"] == 3

    # Zero migration DDL executed
    migration_ddls = [
        s for s in mock_db.executed_strings
        if any(f"CREATE TABLE F_{i}" in s for i in range(1, 4))
    ]
    assert len(migration_ddls) == 0
    assert not mock_db.is_locked


def test_concurrent_invocation_lock_contention(tmp_path: Any) -> None:
    """Active lock raises ConcurrentMigrationError without altering lock."""
    migrations_dir = tmp_path / "concurrent_migrations"
    migrations_dir.mkdir()
    (migrations_dir / "V001__test.sql").write_text(
        "SELECT 1;", encoding="utf-8"
    )

    mock_db = MockSnowflakeDatabase()
    active_token = str(uuid.uuid4())
    recent_time = datetime.datetime.now(
        datetime.timezone.utc
    ) - datetime.timedelta(seconds=30)

    mock_db.schema_lock = {
        "lock_id": MigrationRunner.LOCK_ID,
        "locked_at": recent_time,
        "locked_by": "CONCURRENT_OPERATOR",
        "lock_token": active_token,
        "lock_timeout_sec": 900,
    }

    runner = MigrationRunner(
        mock_db.get_connection(),
        target_db="FOOTBALL_NARRATIVE_DEV",
        migrations_dir=str(migrations_dir),
    )

    with pytest.raises(ConcurrentMigrationError) as exc_info:
        runner.run_migrations()

    err_msg = str(exc_info.value)
    assert (
        "Database FOOTBALL_NARRATIVE_DEV is locked by CONCURRENT_OPERATOR"
        in err_msg
    )
    assert active_token in err_msg
    assert "lease: 900s" in err_msg

    # Existing active lock remains preserved
    assert mock_db.schema_lock is not None
    assert mock_db.schema_lock["lock_token"] == active_token
    assert mock_db.schema_lock["locked_by"] == "CONCURRENT_OPERATOR"
    assert len(mock_db.schema_version) == 0


def test_stale_lock_lease_expiration(tmp_path: Any) -> None:
    """When existing lock exceeds lock_timeout_sec, runner breaks lease."""
    migrations_dir = tmp_path / "stale_lock_migrations"
    migrations_dir.mkdir()
    (migrations_dir / "V001__test.sql").write_text(
        "SELECT 1;", encoding="utf-8"
    )

    mock_db = MockSnowflakeDatabase()
    stale_token = "stale-dead-token-123"
    stale_time = datetime.datetime.now(
        datetime.timezone.utc
    ) - datetime.timedelta(seconds=1200)

    mock_db.schema_lock = {
        "lock_id": MigrationRunner.LOCK_ID,
        "locked_at": stale_time,
        "locked_by": "CRASHED_OPERATOR",
        "lock_token": stale_token,
        "lock_timeout_sec": 900,
    }

    runner = MigrationRunner(
        mock_db.get_connection(),
        target_db="FOOTBALL_NARRATIVE_DEV",
        migrations_dir=str(migrations_dir),
    )

    # 1. Direct acquire_lock breaks stale lease
    acquired_token = runner.acquire_lock(timeout_sec=900)
    assert acquired_token != stale_token
    assert mock_db.schema_lock is not None
    assert mock_db.schema_lock["lock_token"] == acquired_token
    expected_user = os.environ.get("SNOWFLAKE_USER", "OBSERVATORY_RUNNER")
    assert mock_db.schema_lock["locked_by"] == expected_user

    runner.release_lock(acquired_token)
    assert not mock_db.is_locked

    # 2. run_migrations breaks stale lease and runs successfully
    mock_db.schema_lock = {
        "lock_id": MigrationRunner.LOCK_ID,
        "locked_at": stale_time,
        "locked_by": "CRASHED_OPERATOR_2",
        "lock_token": "stale-dead-token-456",
        "lock_timeout_sec": 900,
    }
    res = runner.run_migrations()
    assert res["status"] == "SUCCESS"
    assert res["applied_count"] == 1
    assert not mock_db.is_locked


def test_ddl_auto_commit_independence(tmp_path: Any) -> None:
    """Verifies handling of Snowflake DDL autonomous commit semantics.

    In Snowflake, DDL statements commit immediately and cannot be rolled
    back. If a migration script fails after executing one or more DDL
    statements, those structural changes remain committed in the database,
    and the runner must record status = 'FAILED' for that version.
    """
    migrations_dir = tmp_path / "autocommit_migrations"
    migrations_dir.mkdir()
    script_content = (
        "CREATE TABLE FOOTBALL_NARRATIVE_DEV.CORE.STAGE_RECON (id INT);\n"
        "ALTER TABLE FOOTBALL_NARRATIVE_DEV.CORE.STAGE_RECON "
        "ADD COLUMN code VARCHAR(10);\n"
        "FAIL_EXECUTION_HERE;\n"
    )
    (migrations_dir / "V001__partial_ddl.sql").write_text(
        script_content, encoding="utf-8"
    )

    mock_db = MockSnowflakeDatabase()

    def autocommit_handler(sql: str) -> None:
        # Prior statements execute and commit immediately
        if "CREATE TABLE" in sql and "STAGE_RECON" in sql:
            mock_db.created_tables.add("STAGE_RECON")
        if "FAIL_EXECUTION_HERE" in sql:
            raise RuntimeError(
                "001003 (42000): SQL compilation error: unknown command"
            )

    mock_db.execute_string_handler = autocommit_handler

    runner = MigrationRunner(
        mock_db.get_connection(),
        target_db="FOOTBALL_NARRATIVE_DEV",
        migrations_dir=str(migrations_dir),
    )

    with pytest.raises(MigrationExecutionError) as exc_info:
        runner.run_migrations()

    assert "Failed executing migration V001" in str(exc_info.value)

    # 1. Prior DDL side effects remain in database (auto-commit committed them)
    assert "STAGE_RECON" in mock_db.created_tables

    # 2. Ledger recorded FAILED status despite partial commit
    ledger = runner.get_ledger()
    assert "V001" in ledger
    assert ledger["V001"]["status"] == "FAILED"
    assert "SQL compilation error" in ledger["V001"]["error_message"]
    assert ledger["V001"]["execution_time_ms"] >= 0

    # 3. Lock is cleanly released
    assert not mock_db.is_locked

    # 4. Subsequent run is immediately blocked by the unresolved failure
    with pytest.raises(UnresolvedFailedMigrationError):
        runner.run_migrations()


def test_baseline_adoption_fails_if_v007_to_v009_missing(tmp_path: Any) -> None:
    """Verifies baseline adoption fails closed if physical objects for V007..V009 are missing."""
    migrations_dir = tmp_path / "baseline_gap_migrations"
    migrations_dir.mkdir()
    for i in range(1, 10):
        (migrations_dir / f"V{i:03d}__feature_{i}.sql").write_text(
            f"SELECT {i};", encoding="utf-8"
        )

    # Deployment has only V001..V006 objects, missing V007 (resolution state), V009 (stratification)
    only_v001_to_v006 = {
        "DIM_CHANNEL", "DIM_EVENT", "DIM_VIDEO",
        "FACT_API_REQUEST", "FACT_INGESTION_RUN", "BRIDGE_VIDEO_EVENT",
        "DIM_CHANNEL_FRAME_VERSION", "BRIDGE_FRAME_CHANNEL", "FRAME_CHANNEL_SOURCE_OBSERVATION",
        "DIM_EVENT_VERSION", "DISCOVERY_UNIT_STATE",
    }
    mock_db = MockSnowflakeDatabase(existing_tables=only_v001_to_v006)
    runner = MigrationRunner(
        mock_db.get_connection(),
        migrations_dir=str(migrations_dir),
    )

    with pytest.raises(BaselineAdoptionError) as exc_info:
        runner.adopt_baseline(up_to_version_num=9)

    assert "Cannot adopt baseline V007" in str(exc_info.value)
    # Zero records stamped in ledger
    assert len(mock_db.schema_version) == 0


def test_baseline_adoption_validates_existing_ledger_records(tmp_path: Any) -> None:
    """Verifies baseline adoption checks checksum and status of pre-existing ledger rows."""
    migrations_dir = tmp_path / "baseline_checksum_migrations"
    migrations_dir.mkdir()
    f1 = migrations_dir / "V001__init.sql"
    f1.write_text("SELECT 1;", encoding="utf-8")
    actual_hash = compute_file_checksum(str(f1))

    # Case A: Checksum mismatch on pre-existing row
    mock_db_tampered = MockSnowflakeDatabase(existing_tables=ALL_BASELINE_OBJECTS)
    mock_db_tampered.schema_version.append({
        "installed_rank": 1,
        "version": "V001",
        "description": "init",
        "script_name": "V001__init.sql",
        "checksum": "tampered_old_hash_12345",
        "execution_type": "BASELINE_ADOPTION",
        "installed_by": "SYSTEM",
        "installed_on": "2026-10-01 00:00:00",
        "execution_time_ms": 0,
        "status": "SUCCESS",
        "error_message": None,
    })
    runner_tampered = MigrationRunner(
        mock_db_tampered.get_connection(),
        migrations_dir=str(migrations_dir),
    )
    with pytest.raises(MigrationChecksumMismatchError) as exc_info:
        runner_tampered.adopt_baseline(up_to_version_num=1)
    assert "Checksum mismatch for migration V001" in str(exc_info.value)

    # Case B: Pre-existing row in FAILED state
    mock_db_failed = MockSnowflakeDatabase(existing_tables=ALL_BASELINE_OBJECTS)
    mock_db_failed.schema_version.append({
        "installed_rank": 1,
        "version": "V001",
        "description": "init",
        "script_name": "V001__init.sql",
        "checksum": actual_hash,
        "execution_type": "BASELINE_ADOPTION",
        "installed_by": "SYSTEM",
        "installed_on": "2026-10-01 00:00:00",
        "execution_time_ms": 0,
        "status": "FAILED",
        "error_message": "Prior error",
    })
    runner_failed = MigrationRunner(
        mock_db_failed.get_connection(),
        migrations_dir=str(migrations_dir),
    )
    with pytest.raises(UnresolvedFailedMigrationError):
        runner_failed.adopt_baseline(up_to_version_num=1)

    # Case C: Pre-existing row in IN_PROGRESS state
    mock_db_inp = MockSnowflakeDatabase(existing_tables=ALL_BASELINE_OBJECTS)
    mock_db_inp.schema_version.append({
        "installed_rank": 1,
        "version": "V001",
        "description": "init",
        "script_name": "V001__init.sql",
        "checksum": actual_hash,
        "execution_type": "MIGRATION",
        "installed_by": "SYSTEM",
        "installed_on": "2026-10-01 00:00:00",
        "execution_time_ms": 0,
        "status": "IN_PROGRESS",
        "error_message": None,
    })
    runner_inp = MigrationRunner(
        mock_db_inp.get_connection(),
        migrations_dir=str(migrations_dir),
    )
    with pytest.raises(InterruptedMigrationError):
        runner_inp.adopt_baseline(up_to_version_num=1)


def test_interrupted_migration_in_progress_fails_closed(tmp_path: Any) -> None:
    """Verifies that IN_PROGRESS ledger entries fail closed without automatic replay."""
    migrations_dir = tmp_path / "interrupted_migrations"
    migrations_dir.mkdir()
    f1 = migrations_dir / "V001__create_table.sql"
    f1.write_text("CREATE TABLE INT_TEST (id INT);", encoding="utf-8")
    actual_hash = compute_file_checksum(str(f1))

    mock_db = MockSnowflakeDatabase()
    mock_db.schema_version.append({
        "installed_rank": 1,
        "version": "V001",
        "description": "create_table",
        "script_name": "V001__create_table.sql",
        "checksum": actual_hash,
        "execution_type": "MIGRATION",
        "installed_by": "CRASHED_RUNNER",
        "installed_on": "2026-10-01 00:00:00",
        "execution_time_ms": 0,
        "status": "IN_PROGRESS",
        "error_message": None,
    })
    runner = MigrationRunner(
        mock_db.get_connection(),
        migrations_dir=str(migrations_dir),
    )

    # 1. When checksum matches: raises InterruptedMigrationError (never silently replayed)
    with pytest.raises(InterruptedMigrationError) as exc_info:
        runner.run_migrations()
    assert "is in IN_PROGRESS state from an interrupted or crashed run" in str(exc_info.value)
    # Status remains IN_PROGRESS; not converted to SUCCESS
    assert mock_db.schema_version[0]["status"] == "IN_PROGRESS"
    assert not any("INT_TEST" in s for s in mock_db.executed_strings)

    # 2. When file on disk changed: raises MigrationChecksumMismatchError
    f1.write_text("CREATE TABLE INT_TEST (id INT, tampered INT);", encoding="utf-8")
    with pytest.raises(MigrationChecksumMismatchError) as exc_info2:
        runner.run_migrations()
    assert "Checksum mismatch for migration V001" in str(exc_info2.value)


def test_migration_lock_atomic_race_and_ownership(tmp_path: Any) -> None:
    """Verifies atomic UPDATE concurrency locking and strict token ownership on release."""
    mock_db = MockSnowflakeDatabase()
    runner1 = MigrationRunner(mock_db.get_connection(), target_db="FOOTBALL_NARRATIVE_DEV")
    runner2 = MigrationRunner(mock_db.get_connection(), target_db="FOOTBALL_NARRATIVE_DEV")

    # 1. Runner 1 acquires lock
    token1 = runner1.acquire_lock(timeout_sec=900)
    assert token1 is not None
    assert mock_db.schema_lock["lock_token"] == token1

    # 2. Runner 2 attempts acquire_lock concurrently -> fails with ConcurrentMigrationError
    with pytest.raises(ConcurrentMigrationError) as exc_info:
        runner2.acquire_lock(timeout_sec=900)
    assert "is locked by" in str(exc_info.value)

    # 3. Simulate Runner 1 lock expiry and Runner 2 claiming it
    stale_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=1000)
    mock_db.schema_lock["locked_at"] = stale_time
    token2 = runner2.acquire_lock(timeout_sec=900)
    assert token2 != token1
    assert mock_db.schema_lock["lock_token"] == token2

    # 4. Runner 1 attempts to release with old token1 -> affects 0 rows, does NOT release Runner 2's lock
    runner1.release_lock(token1)
    assert mock_db.schema_lock["lock_token"] == token2
    assert mock_db.schema_lock["locked_by"] is not None

    # 5. Runner 2 releases with valid token2 -> successfully released
    runner2.release_lock(token2)
    assert mock_db.schema_lock["locked_by"] is None


def test_baseline_adoption_rejects_partial_v009_seed_data(tmp_path: Any) -> None:
    """Verifies baseline adoption rejects deployment if V009 seed data in DIM_CHANNEL_STRATUM < 30."""
    migrations_dir = tmp_path / "seed_migrations"
    migrations_dir.mkdir()
    for i in range(1, 10):
        (migrations_dir / f"V{i:03d}__step_{i}.sql").write_text(f"SELECT {i};", encoding="utf-8")

    mock_db = MockSnowflakeDatabase()
    # Partial seed data: only 15 strata seeded instead of 30
    mock_db.strata_seed_count = 15

    runner = MigrationRunner(mock_db.get_connection(), migrations_dir=str(migrations_dir))

    with pytest.raises(BaselineAdoptionError) as exc_info:
        runner.adopt_baseline(up_to_version_num=9)

    err = str(exc_info.value)
    assert "Cannot adopt baseline V009: CORE.DIM_CHANNEL_STRATUM contains only 15 rows" in err
    assert "expected at least 30 seeded composite strata" in err
    # Strict reject: zero rows stamped into ledger
    assert len(mock_db.schema_version) == 0


def test_baseline_adoption_rejects_missing_v009_schema_elements(tmp_path: Any) -> None:
    """Verifies baseline adoption rejects partial V009 deployments before stamping any rows."""
    migrations_dir = tmp_path / "schema_migrations"
    migrations_dir.mkdir()
    for i in range(1, 10):
        (migrations_dir / f"V{i:03d}__step_{i}.sql").write_text(f"SELECT {i};", encoding="utf-8")

    # 1. Missing headline_decomposition_eligible on BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT
    mock_db1 = MockSnowflakeDatabase()
    mock_db1.missing_columns = {"HEADLINE_DECOMPOSITION_ELIGIBLE"}
    runner1 = MigrationRunner(mock_db1.get_connection(), migrations_dir=str(migrations_dir))
    with pytest.raises(BaselineAdoptionError) as exc1:
        runner1.adopt_baseline(up_to_version_num=9)
    assert "HEADLINE_DECOMPOSITION_ELIGIBLE missing" in str(exc1.value)
    assert len(mock_db1.schema_version) == 0

    # 2. Insufficient focus_ratio numeric scale (< 5) on snapshot table
    mock_db2 = MockSnowflakeDatabase()
    mock_db2.missing_columns = {"FOCUS_RATIO_SCALE"}
    runner2 = MigrationRunner(mock_db2.get_connection(), migrations_dir=str(migrations_dir))
    with pytest.raises(BaselineAdoptionError) as exc2:
        runner2.adopt_baseline(up_to_version_num=9)
    assert "FOCUS_RATIO scale is not >= 5" in str(exc2.value)
    assert len(mock_db2.schema_version) == 0

    # 3. Missing enhanced telemetry columns on FACT_CHANNEL_CLASSIFICATION_ASSESSMENT
    mock_db3 = MockSnowflakeDatabase()
    mock_db3.missing_columns = {"ASSESSMENT_TRIGGER"}
    runner3 = MigrationRunner(mock_db3.get_connection(), migrations_dir=str(migrations_dir))
    with pytest.raises(BaselineAdoptionError) as exc3:
        runner3.adopt_baseline(up_to_version_num=9)
    assert "enhanced telemetry columns missing on OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT" in str(exc3.value)
    assert len(mock_db3.schema_version) == 0

    # 4. Missing classification review columns on MANUAL_OVERRIDE
    mock_db4 = MockSnowflakeDatabase()
    mock_db4.missing_columns = {"CLASSIFICATION_AXIS_OVERRIDDEN"}
    runner4 = MigrationRunner(mock_db4.get_connection(), migrations_dir=str(migrations_dir))
    with pytest.raises(BaselineAdoptionError) as exc4:
        runner4.adopt_baseline(up_to_version_num=9)
    assert "classification review columns missing on OPS.MANUAL_OVERRIDE" in str(exc4.value)
    assert len(mock_db4.schema_version) == 0


def test_active_run_exceeding_lease_during_baseline_adoption_fails_closed(tmp_path: Any) -> None:
    """Verifies that if an active baseline adoption run exceeds its lease, it refuses to stamp ledger rows."""
    migrations_dir = tmp_path / "exceeded_lease_baseline"
    migrations_dir.mkdir()
    for i in range(1, 10):
        (migrations_dir / f"V{i:03d}__step_{i}.sql").write_text(f"SELECT {i};", encoding="utf-8")

    mock_db = MockSnowflakeDatabase()
    runner = MigrationRunner(mock_db.get_connection(), migrations_dir=str(migrations_dir))

    # Pre-expire the lock lease before stamping rows by simulating an elapsed duration > timeout
    def simulate_slow_run_exceeding_lease():
        stale_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=950)
        if mock_db.schema_lock:
            mock_db.schema_lock["locked_at"] = stale_time

    # Hijack fetchone or handler to expire lease right after verification
    orig_execute = mock_db.execute

    def expiring_execute(sql: str, params: tuple = (), cursor: Optional[MagicMock] = None):
        orig_execute(sql, params, cursor)
        # When checking columns/tables during baseline adoption, advance clock to expire lease
        if "DIM_CHANNEL_STRATUM" in sql.upper():
            simulate_slow_run_exceeding_lease()

    mock_db.execute = expiring_execute

    with pytest.raises(LockLostError) as exc_info:
        runner.adopt_baseline(up_to_version_num=9)

    assert "Active run exceeded its lease timeout" in str(exc_info.value)
    assert "Refusing to continue writing" in str(exc_info.value)
    # Crucial guarantee: ZERO rows stamped into OPS.SCHEMA_VERSION
    assert len(mock_db.schema_version) == 0


def test_active_run_exceeding_lease_during_migration_execution_fails_closed(tmp_path: Any) -> None:
    """Verifies that if an active migration run exceeds its lease, it halts and refuses to execute further DDL."""
    migrations_dir = tmp_path / "exceeded_lease_migration"
    migrations_dir.mkdir()
    f1 = migrations_dir / "V001__first.sql"
    f2 = migrations_dir / "V002__second.sql"
    f1.write_text("CREATE TABLE T1 (id INT);", encoding="utf-8")
    f2.write_text("CREATE TABLE T2 (id INT);", encoding="utf-8")

    mock_db = MockSnowflakeDatabase()
    runner = MigrationRunner(mock_db.get_connection(), migrations_dir=str(migrations_dir))

    orig_execute = mock_db.execute

    def expiring_execute(sql: str, params: tuple = (), cursor: Optional[MagicMock] = None):
        orig_execute(sql, params, cursor)
        # Expire lease during first migration processing
        if "SCHEMA_VERSION" in sql.upper() and mock_db.schema_lock:
            mock_db.schema_lock["locked_at"] = (
                datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=950)
            )

    mock_db.execute = expiring_execute

    with pytest.raises(LockLostError) as exc_info:
        runner.run_migrations()

    assert "Active run exceeded its lease timeout" in str(exc_info.value)
    assert "Refusing to continue writing" in str(exc_info.value)
    # T2 was never executed
    assert not any("CREATE TABLE T2" in s for s in mock_db.executed_strings)


def test_lease_expiry_prevents_takeover_and_fences_active_writes(tmp_path: Any) -> None:
    """Verifies takeover prevention: an active run cannot have lock stolen, and if taken over, fences writes."""
    mock_db = MockSnowflakeDatabase()
    runner1 = MigrationRunner(mock_db.get_connection(), target_db="FOOTBALL_NARRATIVE_DEV")
    runner2 = MigrationRunner(mock_db.get_connection(), target_db="FOOTBALL_NARRATIVE_DEV")

    # 1. Runner 1 acquires lock
    token1 = runner1.acquire_lock(timeout_sec=900)

    # 2. While Runner 1 is active, Runner 2 cannot acquire lock (fails closed)
    with pytest.raises(ConcurrentMigrationError):
        runner2.acquire_lock(timeout_sec=900)

    # 3. Simulate Runner 1 stalled and Runner 2 taking over expired lease
    mock_db.schema_lock["locked_at"] = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=1200)
    )
    token2 = runner2.acquire_lock(timeout_sec=900)
    assert token2 != token1

    # 4. If Runner 1 attempts to verify lock or write, it detects token mismatch and fails closed
    with pytest.raises(LockLostError) as exc_info:
        runner1.verify_active_lock(token1)

    assert "Lock lease was lost or taken over" in str(exc_info.value)
    assert f"expected '{token1}'" in str(exc_info.value)
