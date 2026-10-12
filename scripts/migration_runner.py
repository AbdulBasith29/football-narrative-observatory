"""Observatory Schema Migration Runner and Ledger Management.

Governing Specification: docs/migration-runner-specification.md
Layer Invariant: OPS.SCHEMA_VERSION tracks all applied migrations with SHA-256 checksums,
and OPS.SCHEMA_LOCK coordinates distributed execution.
"""

from __future__ import annotations

import dataclasses
import datetime
import hashlib
import os
import re
import time
import uuid
from typing import Any, Dict, List, Optional


class MigrationError(Exception):
    """Base exception for migration runner operations."""
    pass


class MigrationChecksumMismatchError(MigrationError):
    """Raised when an already applied migration file's checksum differs from recorded state."""
    pass


class MigrationExecutionError(MigrationError):
    """Raised when execution of a migration script fails against Snowflake."""
    pass


class UnresolvedFailedMigrationError(MigrationError):
    """Raised when attempting to run migrations while a prior migration remains in FAILED state."""
    pass


class ConcurrentMigrationError(MigrationError):
    """Raised when an active migration lock is held by another process."""
    pass


class BaselineAdoptionError(MigrationError):
    """Raised when baseline adoption validation fails."""
    pass


class InterruptedMigrationError(MigrationError):
    """Raised when a migration is in IN_PROGRESS state from a prior crashed or interrupted run."""
    pass


@dataclasses.dataclass(frozen=True)
class MigrationFile:
    """Represents a discovered migration script on disk."""
    version_num: int
    version_str: str
    description: str
    filename: str
    filepath: str
    checksum: str


def compute_file_checksum(filepath: str) -> str:
    """Computes SHA-256 hex digest of file contents."""
    hasher = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


class MigrationRunner:
    """Orchestrates deterministic schema migration execution and ledger tracking."""

    MIGRATION_PATTERN = re.compile(r"^V(\d+)__(.+)\.sql$", re.IGNORECASE)
    LOCK_ID = "MIGRATION_RUNNER_LOCK"
    DEFAULT_LOCK_TIMEOUT_SEC = 900  # 15 minutes

    def __init__(
        self,
        conn: Any,
        target_db: str = "FOOTBALL_NARRATIVE_DEV",
        migrations_dir: str = "infra/snowflake/migrations",
    ):
        self.conn = conn
        self.target_db = target_db
        self.migrations_dir = migrations_dir

    def discover_migrations(self) -> List[MigrationFile]:
        """Discovers and sorts all valid migration scripts in the migrations directory."""
        if not os.path.exists(self.migrations_dir):
            return []

        discovered: List[MigrationFile] = []
        for fname in os.listdir(self.migrations_dir):
            match = self.MIGRATION_PATTERN.match(fname)
            if match:
                v_num = int(match.group(1))
                v_str = f"V{v_num:03d}"
                desc = match.group(2)
                fpath = os.path.join(self.migrations_dir, fname)
                checksum = compute_file_checksum(fpath)
                discovered.append(
                    MigrationFile(
                        version_num=v_num,
                        version_str=v_str,
                        description=desc,
                        filename=fname,
                        filepath=fpath,
                        checksum=checksum,
                    )
                )

        discovered.sort(key=lambda m: m.version_num)
        return discovered

    def ensure_ledger_tables(self) -> None:
        """Ensures OPS.SCHEMA_VERSION and OPS.SCHEMA_LOCK exist in the target database."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.target_db}.OPS.SCHEMA_VERSION (
            installed_rank      NUMBER(38,0) NOT NULL,
            version             VARCHAR(32) NOT NULL,
            description         VARCHAR(256) NOT NULL,
            script_name         VARCHAR(256) NOT NULL,
            checksum            VARCHAR(64) NOT NULL,
            execution_type      VARCHAR(32) NOT NULL,
            installed_by        VARCHAR(128) NOT NULL,
            installed_on        TIMESTAMP_NTZ NOT NULL,
            execution_time_ms   NUMBER(38,0) NOT NULL,
            status              VARCHAR(32) NOT NULL,
            error_message       VARCHAR(2048),
            CONSTRAINT pk_schema_version PRIMARY KEY (version),
            CONSTRAINT uq_schema_version_rank UNIQUE (installed_rank)
        );

        CREATE TABLE IF NOT EXISTS {self.target_db}.OPS.SCHEMA_LOCK (
            lock_id             VARCHAR(32) NOT NULL,
            locked_at           TIMESTAMP_NTZ,
            locked_by           VARCHAR(128),
            lock_token          VARCHAR(36),
            lock_timeout_sec    NUMBER(38,0) NOT NULL DEFAULT 900,
            CONSTRAINT pk_schema_lock PRIMARY KEY (lock_id)
        );

        MERGE INTO {self.target_db}.OPS.SCHEMA_LOCK AS target
        USING (SELECT '{self.LOCK_ID}' AS lock_id) AS src
        ON target.lock_id = src.lock_id
        WHEN NOT MATCHED THEN
            INSERT (lock_id, locked_at, locked_by, lock_token, lock_timeout_sec)
            VALUES (src.lock_id, NULL, NULL, NULL, 900);
        """
        self.conn.execute_string(ddl)

    def acquire_lock(self, timeout_sec: int = DEFAULT_LOCK_TIMEOUT_SEC) -> str:
        """Acquires the distributed migration lock atomically or raises ConcurrentMigrationError."""
        lock_token = str(uuid.uuid4())
        user_identity = os.environ.get("SNOWFLAKE_USER", "OBSERVATORY_RUNNER")

        cur = self.conn.cursor()
        try:
            # Atomic test-and-set: update the single lock row only if currently unlocked or lease has expired
            cur.execute(
                f"UPDATE {self.target_db}.OPS.SCHEMA_LOCK "
                f"SET locked_at = CURRENT_TIMESTAMP(), "
                f"    locked_by = %s, "
                f"    lock_token = %s, "
                f"    lock_timeout_sec = %s "
                f"WHERE lock_id = %s "
                f"  AND (locked_by IS NULL OR DATEADD(second, lock_timeout_sec, locked_at) < CURRENT_TIMESTAMP())",
                (user_identity, lock_token, timeout_sec, self.LOCK_ID)
            )
            if getattr(cur, "rowcount", None) == 1:
                return lock_token

            # Lock was not acquired: inspect who holds the active lock
            cur.execute(
                f"SELECT locked_by, lock_token, locked_at, lock_timeout_sec "
                f"FROM {self.target_db}.OPS.SCHEMA_LOCK WHERE lock_id = %s",
                (self.LOCK_ID,)
            )
            holder = cur.fetchone()
            if holder and holder[0]:
                raise ConcurrentMigrationError(
                    f"Database {self.target_db} is locked by {holder[0]} "
                    f"(lock token: {holder[1]}, acquired at: {holder[2]}, lease: {holder[3]}s)."
                )
            raise ConcurrentMigrationError(f"Database {self.target_db} lock acquisition failed.")
        finally:
            cur.close()

    def release_lock(self, lock_token: str) -> None:
        """Releases the migration lock if the token matches (strict ownership check)."""
        cur = self.conn.cursor()
        try:
            cur.execute(
                f"UPDATE {self.target_db}.OPS.SCHEMA_LOCK "
                f"SET locked_at = NULL, "
                f"    locked_by = NULL, "
                f"    lock_token = NULL "
                f"WHERE lock_id = %s AND lock_token = %s",
                (self.LOCK_ID, lock_token)
            )
        finally:
            cur.close()

    def get_ledger(self) -> Dict[str, dict]:
        """Retrieves existing schema version entries keyed by version string."""
        cur = self.conn.cursor()
        try:
            cur.execute(
                f"SELECT installed_rank, version, description, script_name, checksum, "
                f"execution_type, installed_by, installed_on, execution_time_ms, status, error_message "
                f"FROM {self.target_db}.OPS.SCHEMA_VERSION ORDER BY installed_rank"
            )
            rows = cur.fetchall()
            ledger = {}
            for r in rows:
                ledger[r[1]] = {
                    "installed_rank": r[0],
                    "version": r[1],
                    "description": r[2],
                    "script_name": r[3],
                    "checksum": r[4],
                    "execution_type": r[5],
                    "installed_by": r[6],
                    "installed_on": r[7],
                    "execution_time_ms": r[8],
                    "status": r[9],
                    "error_message": r[10],
                }
            return ledger
        finally:
            cur.close()

    def _verify_migration_objects_exist(self, cur: Any, version_num: int) -> None:
        """Verifies that physical database objects for a given migration version actually exist."""
        if version_num == 1:
            cur.execute(
                f"SELECT character_maximum_length FROM {self.target_db}.information_schema.columns "
                f"WHERE table_schema = 'OPS' AND table_name = 'FACT_API_REQUEST' AND column_name = 'PAGE_TOKEN_USED'"
            )
            row = cur.fetchone()
            if not row or (row[0] is not None and row[0] < 1024):
                raise BaselineAdoptionError(
                    f"Cannot adopt baseline V001: OPS.FACT_API_REQUEST.PAGE_TOKEN_USED is missing or length < 1024 in {self.target_db}."
                )
        elif version_num == 2:
            cur.execute(
                f"SELECT count(*) FROM {self.target_db}.information_schema.columns "
                f"WHERE table_schema = 'OPS' AND table_name = 'FACT_INGESTION_RUN' AND column_name = 'RUN_PURPOSE'"
            )
            if cur.fetchone()[0] == 0:
                raise BaselineAdoptionError(
                    f"Cannot adopt baseline V002: column OPS.FACT_INGESTION_RUN.RUN_PURPOSE does not exist in {self.target_db}."
                )
        elif version_num == 3:
            cur.execute(
                f"SELECT count(*) FROM {self.target_db}.information_schema.columns "
                f"WHERE table_schema = 'CORE' AND table_name = 'BRIDGE_VIDEO_EVENT' AND column_name = 'PRIMARY_EXCLUSION_REASON'"
            )
            if cur.fetchone()[0] == 0:
                raise BaselineAdoptionError(
                    f"Cannot adopt baseline V003: column CORE.BRIDGE_VIDEO_EVENT.PRIMARY_EXCLUSION_REASON does not exist in {self.target_db}."
                )
        elif version_num == 4:
            cur.execute(
                f"SELECT count(*) FROM {self.target_db}.information_schema.tables "
                f"WHERE table_schema = 'CORE' AND table_name IN ('DIM_CHANNEL_FRAME_VERSION', 'BRIDGE_FRAME_CHANNEL')"
            )
            if cur.fetchone()[0] < 2:
                raise BaselineAdoptionError(
                    f"Cannot adopt baseline V004: frame tables do not exist in {self.target_db}."
                )
        elif version_num == 5:
            cur.execute(
                f"SELECT count(*) FROM {self.target_db}.information_schema.columns "
                f"WHERE table_schema = 'CORE' AND table_name = 'DIM_EVENT_VERSION' AND column_name = 'EVENT_TERMS'"
            )
            if cur.fetchone()[0] == 0:
                raise BaselineAdoptionError(
                    f"Cannot adopt baseline V005: column CORE.DIM_EVENT_VERSION.EVENT_TERMS does not exist in {self.target_db}."
                )
        elif version_num == 6:
            cur.execute(
                f"SELECT count(*) FROM {self.target_db}.information_schema.columns "
                f"WHERE table_schema = 'OPS' AND table_name = 'DISCOVERY_UNIT_STATE' AND column_name = 'LAST_ERROR_CODE'"
            )
            if cur.fetchone()[0] == 0:
                raise BaselineAdoptionError(
                    f"Cannot adopt baseline V006: OPS.DISCOVERY_UNIT_STATE does not exist or lacks LAST_ERROR_CODE in {self.target_db}."
                )
        elif version_num == 7:
            cur.execute(
                f"SELECT count(*) FROM {self.target_db}.information_schema.tables "
                f"WHERE table_schema = 'OPS' AND table_name = 'VIDEO_METADATA_RESOLUTION_STATE'"
            )
            t_count = cur.fetchone()[0]
            cur.execute(
                f"SELECT count(*) FROM {self.target_db}.information_schema.columns "
                f"WHERE table_schema = 'CORE' AND table_name = 'FACT_VIDEO_SNAPSHOT' AND column_name = 'API_REQUEST_ID'"
            )
            c_count = cur.fetchone()[0]
            if t_count == 0 or c_count == 0:
                raise BaselineAdoptionError(
                    f"Cannot adopt baseline V007: OPS.VIDEO_METADATA_RESOLUTION_STATE or CORE.FACT_VIDEO_SNAPSHOT lineage missing in {self.target_db}."
                )
        elif version_num == 8:
            cur.execute(
                f"SELECT count(*) FROM {self.target_db}.information_schema.columns "
                f"WHERE table_schema = 'CORE' AND table_name = 'DIM_EVENT_VERSION' AND column_name = 'BASELINE_TERMS'"
            )
            if cur.fetchone()[0] == 0:
                raise BaselineAdoptionError(
                    f"Cannot adopt baseline V008: column CORE.DIM_EVENT_VERSION.BASELINE_TERMS does not exist in {self.target_db}."
                )
        elif version_num == 9:
            cur.execute(
                f"SELECT count(*) FROM {self.target_db}.information_schema.tables "
                f"WHERE (table_schema = 'OPS' AND table_name IN ('FACT_CHANNEL_ASSESSMENT_EVIDENCE_ITEM', 'CHANNEL_EVIDENCE_ACQUISITION_STATE')) "
                f"   OR (table_schema = 'CORE' AND table_name = 'DIM_CHANNEL_STRATUM')"
            )
            if cur.fetchone()[0] < 3:
                raise BaselineAdoptionError(
                    f"Cannot adopt baseline V009: stratification tables do not exist in {self.target_db}."
                )

    def adopt_baseline(self, up_to_version_num: int = 9, installed_by: Optional[str] = None) -> dict:
        """Explicitly records historical migrations up to version as BASELINE_ADOPTION.

        Validates physical existence of objects per migration version and checks existing ledger records.
        """
        self.ensure_ledger_tables()
        lock_token = self.acquire_lock()
        try:
            migrations = self.discover_migrations()
            target_migrations = [m for m in migrations if m.version_num <= up_to_version_num]
            if not target_migrations:
                raise BaselineAdoptionError(f"No migrations found up to V{up_to_version_num:03d}")

            ledger = self.get_ledger()

            # 1. Validate health and checksums of any existing ledger entries in target range
            for m in target_migrations:
                if m.version_str in ledger:
                    entry = ledger[m.version_str]
                    if entry["checksum"] != m.checksum:
                        raise MigrationChecksumMismatchError(
                            f"Checksum mismatch for migration {m.version_str} ({m.filename})! "
                            f"Recorded in ledger ({entry['status']}): {entry['checksum']}, On Disk: {m.checksum}."
                        )
                    if entry["status"] == "FAILED":
                        raise UnresolvedFailedMigrationError(
                            f"Migration {m.version_str} in ledger is in FAILED state: {entry['error_message']}."
                        )
                    if entry["status"] == "IN_PROGRESS":
                        raise InterruptedMigrationError(
                            f"Migration {m.version_str} in ledger is in IN_PROGRESS state from an interrupted run."
                        )

            # 2. Verify physical database objects for each migration being baselined that is not yet in ledger
            cur = self.conn.cursor()
            try:
                for m in target_migrations:
                    if m.version_str not in ledger:
                        self._verify_migration_objects_exist(cur, m.version_num)
            finally:
                cur.close()

            # 3. Record baseline adoption entries for unrecorded migrations
            installer = installed_by or os.environ.get("SNOWFLAKE_USER", "SYSTEM")
            now_iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            adopted = []
            cur = self.conn.cursor()
            try:
                for rank, m in enumerate(target_migrations, start=1):
                    if m.version_str in ledger:
                        continue
                    cur.execute(
                        f"INSERT INTO {self.target_db}.OPS.SCHEMA_VERSION ("
                        f"installed_rank, version, description, script_name, checksum, "
                        f"execution_type, installed_by, installed_on, execution_time_ms, status, error_message"
                        f") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (
                            rank,
                            m.version_str,
                            m.description,
                            m.filename,
                            m.checksum,
                            "BASELINE_ADOPTION",
                            installer,
                            now_iso,
                            0,
                            "SUCCESS",
                            None,
                        )
                    )
                    adopted.append(m.version_str)
            finally:
                cur.close()

            return {
                "status": "BASELINE_ADOPTED",
                "adopted_count": len(adopted),
                "adopted_versions": adopted,
                "up_to_version": f"V{up_to_version_num:03d}",
            }
        finally:
            self.release_lock(lock_token)

    def run_migrations(self, target_version_num: Optional[int] = None) -> dict:
        """Executes all pending migrations in strict ascending numerical order."""
        self.ensure_ledger_tables()
        lock_token = self.acquire_lock()
        try:
            migrations = self.discover_migrations()
            ledger = self.get_ledger()

            # Step 1: Verify checksums and health of all existing ledger records
            for m in migrations:
                if m.version_str in ledger:
                    entry = ledger[m.version_str]
                    # Checksum verification applies to ALL existing entries
                    if entry["checksum"] != m.checksum:
                        raise MigrationChecksumMismatchError(
                            f"Checksum mismatch for migration {m.version_str} ({m.filename})! "
                            f"Recorded in ledger ({entry['status']}): {entry['checksum']}, On Disk: {m.checksum}. "
                            f"Applied migrations are immutable."
                        )
                    if entry["status"] == "FAILED":
                        raise UnresolvedFailedMigrationError(
                            f"Migration {m.version_str} previously failed: {entry['error_message']}. "
                            f"Manual operator intervention required before continuing."
                        )
                    if entry["status"] == "IN_PROGRESS":
                        raise InterruptedMigrationError(
                            f"Migration {m.version_str} is in IN_PROGRESS state from an interrupted or crashed run. "
                            f"Snowflake DDL commits independently; manual operator inspection is required before resuming."
                        )

            # Step 2: Determine pending migrations (strictly unapplied)
            pending: List[MigrationFile] = []
            for m in migrations:
                if target_version_num is not None and m.version_num > target_version_num:
                    continue
                if m.version_str not in ledger:
                    pending.append(m)

            if not pending:
                return {
                    "status": "UP_TO_DATE",
                    "applied_count": 0,
                    "applied_versions": [],
                    "total_migrations": len(migrations),
                }

            # Step 3: Execute pending migrations sequentially
            user_identity = os.environ.get("SNOWFLAKE_USER", "OBSERVATORY_RUNNER")
            next_rank = max([e["installed_rank"] for e in ledger.values()], default=0) + 1
            applied = []

            for m in pending:
                start_time = time.time()
                now_iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

                # Record IN_PROGRESS in ledger with initial checksum
                cur = self.conn.cursor()
                try:
                    cur.execute(
                        f"INSERT INTO {self.target_db}.OPS.SCHEMA_VERSION ("
                        f"installed_rank, version, description, script_name, checksum, "
                        f"execution_type, installed_by, installed_on, execution_time_ms, status, error_message"
                        f") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (
                            next_rank,
                            m.version_str,
                            m.description,
                            m.filename,
                            m.checksum,
                            "MIGRATION",
                            user_identity,
                            now_iso,
                            0,
                            "IN_PROGRESS",
                            None,
                        )
                    )
                finally:
                    cur.close()

                # Read script and substitute target DB
                with open(m.filepath, "r", encoding="utf-8") as f:
                    sql_content = f.read().replace("FOOTBALL_NARRATIVE_DEV", self.target_db)

                # Execute migration
                try:
                    self.conn.execute_string(sql_content)
                    elapsed_ms = int((time.time() - start_time) * 1000)

                    # Update ledger to SUCCESS
                    cur = self.conn.cursor()
                    try:
                        cur.execute(
                            f"UPDATE {self.target_db}.OPS.SCHEMA_VERSION SET "
                            f"status = 'SUCCESS', execution_time_ms = %s, error_message = NULL "
                            f"WHERE version = %s",
                            (elapsed_ms, m.version_str)
                        )
                    finally:
                        cur.close()

                    applied.append(m.version_str)
                    next_rank += 1

                except Exception as ex:
                    elapsed_ms = int((time.time() - start_time) * 1000)
                    error_msg = str(ex)[:2040]

                    # Record failure in ledger
                    cur = self.conn.cursor()
                    try:
                        cur.execute(
                            f"UPDATE {self.target_db}.OPS.SCHEMA_VERSION SET "
                            f"status = 'FAILED', execution_time_ms = %s, error_message = %s "
                            f"WHERE version = %s",
                            (elapsed_ms, error_msg, m.version_str)
                        )
                    finally:
                        cur.close()

                    raise MigrationExecutionError(
                        f"Failed executing migration {m.version_str} ({m.filename}): {ex}"
                    ) from ex

            return {
                "status": "SUCCESS",
                "applied_count": len(applied),
                "applied_versions": applied,
                "total_migrations": len(migrations),
            }
        finally:
            self.release_lock(lock_token)
