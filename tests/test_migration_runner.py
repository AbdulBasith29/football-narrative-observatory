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
    MigrationChecksumMismatchError,
    MigrationExecutionError,
    MigrationRunner,
    UnresolvedFailedMigrationError,
    compute_file_checksum,
)


class MockSnowflakeDatabase:
    """In-memory mock database simulating Snowflake behavior and catalog."""

    def __init__(
        self,
        existing_tables: Optional[Set[str]] = None,
        target_db: str = "FOOTBALL_NARRATIVE_DEV",
    ) -> None:
        self.target_db = target_db
        if existing_tables is None:
            self.existing_tables = {"DIM_CHANNEL", "DIM_EVENT", "DIM_VIDEO"}
        else:
            self.existing_tables = set(existing_tables)

        self.schema_version: List[Dict[str, Any]] = []
        self.schema_lock: Optional[Dict[str, Any]] = None
        self.executed_strings: List[str] = []
        self.executed_queries: List[tuple] = []
        self.created_tables: Set[str] = set()
        self.execute_string_handler: Optional[Callable[[str], None]] = None
        self._last_result: List[tuple] = []

    def get_cursor(self) -> MagicMock:
        cursor = MagicMock()
        cursor.execute.side_effect = self.execute
        cursor.fetchone.side_effect = self.fetchone
        cursor.fetchall.side_effect = self.fetchall
        cursor.close.return_value = None
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

    def execute(self, sql: str, params: tuple = ()) -> None:
        self.executed_queries.append((sql, params))
        clean = " ".join(sql.strip().split())

        # 1. Lock check query (SELECT)
        if (
            clean.upper().startswith("SELECT")
            and "OPS.SCHEMA_LOCK" in clean.upper()
            and "WHERE LOCK_ID =" in clean.upper()
        ):
            if (
                self.schema_lock
                and self.schema_lock.get("lock_id") == params[0]
            ):
                self._last_result = [(
                    self.schema_lock["lock_id"],
                    self.schema_lock["locked_at"],
                    self.schema_lock["locked_by"],
                    self.schema_lock["lock_token"],
                    self.schema_lock["lock_timeout_sec"],
                )]
            else:
                self._last_result = []

        # 2. Release lock with matching token (DELETE)
        elif (
            clean.upper().startswith("DELETE")
            and "OPS.SCHEMA_LOCK" in clean.upper()
            and "LOCK_TOKEN" in clean.upper()
        ):
            if (
                self.schema_lock
                and self.schema_lock.get("lock_id") == params[0]
                and self.schema_lock.get("lock_token") == params[1]
            ):
                self.schema_lock = None
            self._last_result = []

        # 3. Delete lock without token (stale lease break)
        elif (
            clean.upper().startswith("DELETE")
            and "OPS.SCHEMA_LOCK" in clean.upper()
            and "WHERE LOCK_ID =" in clean.upper()
        ):
            if (
                self.schema_lock
                and self.schema_lock.get("lock_id") == params[0]
            ):
                self.schema_lock = None
            self._last_result = []

        # 4. Insert lock
        elif "INSERT INTO" in clean and "OPS.SCHEMA_LOCK" in clean:
            locked_at_val = params[1]
            if isinstance(locked_at_val, str):
                try:
                    dt = datetime.datetime.fromisoformat(
                        locked_at_val.replace("Z", "+00:00")
                    )
                except Exception:
                    dt = locked_at_val
            else:
                dt = locked_at_val
            self.schema_lock = {
                "lock_id": params[0],
                "locked_at": dt,
                "locked_by": params[2],
                "lock_token": params[3],
                "lock_timeout_sec": params[4],
            }
            self._last_result = []

        # 5. Information schema core table count
        elif (
            "information_schema.tables" in clean
            and "table_schema = 'CORE'" in clean
        ):
            count = sum(
                1 for t in ["DIM_CHANNEL", "DIM_EVENT", "DIM_VIDEO"]
                if t in self.existing_tables
            )
            self._last_result = [(count,)]

        # 6. Retrieve ledger entries
        elif (
            "FROM" in clean
            and "OPS.SCHEMA_VERSION" in clean
            and "ORDER BY installed_rank" in clean
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

        # 7. Baseline adoption ledger insert
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
            self._last_result = []

        # 8. Merge into ledger (IN_PROGRESS)
        elif "MERGE INTO" in clean and "OPS.SCHEMA_VERSION" in clean:
            ver = params[0]
            matched = next(
                (e for e in self.schema_version if e["version"] == ver),
                None,
            )
            if matched:
                matched["status"] = "IN_PROGRESS"
                matched["installed_on"] = params[1]
                matched["error_message"] = None
            else:
                self.schema_version.append({
                    "installed_rank": params[2],
                    "version": params[3],
                    "description": params[4],
                    "script_name": params[5],
                    "checksum": params[6],
                    "execution_type": params[7],
                    "installed_by": params[8],
                    "installed_on": params[9],
                    "execution_time_ms": params[10],
                    "status": params[11],
                    "error_message": params[12],
                })
            self._last_result = []

        # 9. Update ledger status
        elif "UPDATE" in clean and "OPS.SCHEMA_VERSION" in clean:
            if "status = 'SUCCESS'" in clean:
                ver = params[1]
                matched = next(
                    (e for e in self.schema_version if e["version"] == ver),
                    None,
                )
                if matched:
                    matched["status"] = "SUCCESS"
                    matched["execution_time_ms"] = params[0]
                    matched["error_message"] = None
            elif "status = 'FAILED'" in clean:
                ver = params[2]
                matched = next(
                    (e for e in self.schema_version if e["version"] == ver),
                    None,
                )
                if matched:
                    matched["status"] = "FAILED"
                    matched["execution_time_ms"] = params[0]
                    matched["error_message"] = params[1]
            self._last_result = []

        else:
            self._last_result = []

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
    assert mock_db.schema_lock is None

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
    assert mock_db.schema_lock is None

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
    assert "core tables do not exist" in str(exc_info.value)
    assert len(mock_db_missing.schema_version) == 0
    assert mock_db_missing.schema_lock is None

    # 2. Successful adoption when core tables exist
    mock_db = MockSnowflakeDatabase(
        existing_tables={"DIM_CHANNEL", "DIM_EVENT", "DIM_VIDEO"}
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
    assert mock_db.schema_lock is None

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
    assert mock_db.schema_lock is None


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
    assert mock_db.schema_lock is None


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
    assert mock_db.schema_lock is None


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
    assert mock_db.schema_lock is None


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
    assert mock_db.schema_lock is None

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
    assert mock_db.schema_lock is None


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
    assert mock_db.schema_lock is None

    # 4. Subsequent run is immediately blocked by the unresolved failure
    with pytest.raises(UnresolvedFailedMigrationError):
        runner.run_migrations()
