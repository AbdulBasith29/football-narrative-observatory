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


class LockLostError(ConcurrentMigrationError):
    """Raised when an active run loses its lock or exceeds its lease timeout before completing writes."""
    pass


class BaselineAdoptionError(MigrationError):
    """Raised when baseline adoption validation fails."""
    pass


class BaselineAdoptionDisabledError(BaselineAdoptionError):
    """Raised when baseline adoption is attempted while disabled for this release."""
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
        """Acquires the distributed migration lock atomically or raises ConcurrentMigrationError.

        Automatic expired-lock takeover is disabled: an occupied lock blocks another runner
        until explicitly recovered by an operator via recover_lock().
        """
        lock_token = str(uuid.uuid4())
        user_identity = os.environ.get("SNOWFLAKE_USER", "OBSERVATORY_RUNNER")

        cur = self.conn.cursor()
        try:
            # Atomic test-and-set: update the single lock row ONLY if currently unlocked (locked_by IS NULL).
            cur.execute(
                f"UPDATE {self.target_db}.OPS.SCHEMA_LOCK "
                f"SET locked_at = CURRENT_TIMESTAMP(), "
                f"    locked_by = %s, "
                f"    lock_token = %s, "
                f"    lock_timeout_sec = %s "
                f"WHERE lock_id = %s "
                f"  AND locked_by IS NULL",
                (user_identity, lock_token, timeout_sec, self.LOCK_ID)
            )
            if getattr(cur, "rowcount", None) == 1:
                self.active_lock_token = lock_token
                self.active_lock_timeout_sec = timeout_sec
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
                    f"(lock token: {holder[1]}, acquired at: {holder[2]}). "
                    f"Automatic expired-lock takeover is disabled. An occupied lock blocks execution "
                    f"until explicitly recovered by an operator via recover_lock()."
                )
            raise ConcurrentMigrationError(f"Database {self.target_db} lock acquisition failed.")
        finally:
            cur.close()

    def recover_lock(self, operator_identity: Optional[str] = None) -> None:
        """Explicit operator recovery to clear an occupied or orphaned migration lock.

        Since automatic expired-lock takeover is disabled, an operator must explicitly
        clear the lock if a prior process crashed or terminated abnormally.
        """
        cur = self.conn.cursor()
        try:
            cur.execute(
                f"UPDATE {self.target_db}.OPS.SCHEMA_LOCK "
                f"SET locked_at = NULL, "
                f"    locked_by = NULL, "
                f"    lock_token = NULL "
                f"WHERE lock_id = %s",
                (self.LOCK_ID,)
            )
            self.active_lock_token = None
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
            if getattr(self, "active_lock_token", None) == lock_token:
                self.active_lock_token = None
        finally:
            cur.close()

    def verify_active_lock(self, lock_token: Optional[str] = None) -> None:
        """Verifies that this runner holds the active lock.

        Raises LockLostError if the lock was cleared by operator recovery or taken over.
        """
        tok = lock_token or getattr(self, "active_lock_token", None)
        if not tok:
            raise LockLostError("No active lock token held by this runner.")
        cur = self.conn.cursor()
        try:
            cur.execute(
                f"SELECT locked_by, lock_token, locked_at, lock_timeout_sec "
                f"FROM {self.target_db}.OPS.SCHEMA_LOCK WHERE lock_id = %s",
                (self.LOCK_ID,),
            )
            row = cur.fetchone()
            if not row or row[1] != tok:
                active_tok = row[1] if row else None
                raise LockLostError(
                    f"Lock was lost or cleared; active holder token is '{active_tok}', expected '{tok}'."
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
        """Verifies that physical database objects and deterministic seed data for a given migration actually exist."""
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
            cur.execute(
                f"SELECT character_maximum_length FROM {self.target_db}.information_schema.columns "
                f"WHERE table_schema = 'RAW' AND table_name = 'YOUTUBE_API_RESPONSE' AND column_name = 'PAGE_TOKEN_USED'"
            )
            row2 = cur.fetchone()
            if not row2 or (row2[0] is not None and row2[0] < 1024):
                raise BaselineAdoptionError(
                    f"Cannot adopt baseline V001: RAW.YOUTUBE_API_RESPONSE.PAGE_TOKEN_USED is missing or length < 1024 in {self.target_db}."
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
                f"WHERE table_schema = 'CORE' AND table_name = 'BRIDGE_VIDEO_EVENT' "
                f"  AND column_name IN ('PRIMARY_EXCLUSION_REASON', 'DISCOVERY_METHOD', 'DISCOVERY_PROVENANCE')"
            )
            if cur.fetchone()[0] < 3:
                raise BaselineAdoptionError(
                    f"Cannot adopt baseline V003: required columns on CORE.BRIDGE_VIDEO_EVENT missing in {self.target_db}."
                )
        elif version_num == 4:
            cur.execute(
                f"SELECT count(*) FROM {self.target_db}.information_schema.tables "
                f"WHERE table_schema = 'CORE' "
                f"  AND table_name IN ('DIM_CHANNEL_FRAME_VERSION', 'BRIDGE_FRAME_CHANNEL', 'FRAME_CHANNEL_SOURCE_OBSERVATION')"
            )
            if cur.fetchone()[0] < 3:
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
                f"SELECT count(*) FROM {self.target_db}.information_schema.tables "
                f"WHERE table_schema = 'OPS' AND table_name = 'DISCOVERY_UNIT_STATE'"
            )
            if cur.fetchone()[0] == 0:
                raise BaselineAdoptionError(
                    f"Cannot adopt baseline V006: table OPS.DISCOVERY_UNIT_STATE does not exist in {self.target_db}."
                )
            cur.execute(
                f"SELECT count(*) FROM {self.target_db}.information_schema.columns "
                f"WHERE table_schema = 'OPS' AND table_name = 'DISCOVERY_UNIT_STATE' "
                f"  AND column_name IN ('LAST_ERROR_CODE', 'LAST_ERROR_MESSAGE')"
            )
            if cur.fetchone()[0] < 2:
                raise BaselineAdoptionError(
                    f"Cannot adopt baseline V006: error columns on OPS.DISCOVERY_UNIT_STATE missing in {self.target_db}."
                )
        elif version_num == 7:
            cur.execute(
                f"SELECT count(*) FROM {self.target_db}.information_schema.tables "
                f"WHERE table_schema = 'OPS' AND table_name = 'VIDEO_METADATA_RESOLUTION_STATE'"
            )
            t_count = cur.fetchone()[0]
            cur.execute(
                f"SELECT count(*) FROM {self.target_db}.information_schema.columns "
                f"WHERE table_schema = 'CORE' AND table_name = 'FACT_VIDEO_SNAPSHOT' "
                f"  AND column_name IN ('API_REQUEST_ID', 'RAW_RESPONSE_ID')"
            )
            c_count = cur.fetchone()[0]
            if t_count == 0 or c_count < 2:
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
            # 1. Verify tables exist
            cur.execute(
                f"SELECT count(*) FROM {self.target_db}.information_schema.tables "
                f"WHERE (table_schema = 'OPS' AND table_name IN ('FACT_CHANNEL_ASSESSMENT_EVIDENCE_ITEM', 'CHANNEL_EVIDENCE_ACQUISITION_STATE')) "
                f"   OR (table_schema = 'CORE' AND table_name = 'DIM_CHANNEL_STRATUM')"
            )
            if cur.fetchone()[0] < 3:
                raise BaselineAdoptionError(
                    f"Cannot adopt baseline V009: stratification tables do not exist in {self.target_db}."
                )

            # 2. Verify deterministic seed data in DIM_CHANNEL_STRATUM (30 composite strata)
            cur.execute(
                f"SELECT count(*) FROM {self.target_db}.CORE.DIM_CHANNEL_STRATUM"
            )
            seed_count = cur.fetchone()[0]
            if seed_count < 30:
                raise BaselineAdoptionError(
                    f"Cannot adopt baseline V009: CORE.DIM_CHANNEL_STRATUM contains only {seed_count} rows; expected at least 30 seeded composite strata in {self.target_db}."
                )

            # 3. Verify CORE.BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT schema changes
            cur.execute(
                f"SELECT count(*) FROM {self.target_db}.information_schema.columns "
                f"WHERE table_schema = 'CORE' AND table_name = 'BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT' "
                f"  AND column_name = 'HEADLINE_DECOMPOSITION_ELIGIBLE'"
            )
            if cur.fetchone()[0] == 0:
                raise BaselineAdoptionError(
                    f"Cannot adopt baseline V009: column CORE.BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT.HEADLINE_DECOMPOSITION_ELIGIBLE missing in {self.target_db}."
                )

            cur.execute(
                f"SELECT numeric_scale FROM {self.target_db}.information_schema.columns "
                f"WHERE table_schema = 'CORE' AND table_name = 'BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT' "
                f"  AND column_name = 'FOCUS_RATIO'"
            )
            row_fr = cur.fetchone()
            if not row_fr or (row_fr[0] is not None and row_fr[0] < 5):
                raise BaselineAdoptionError(
                    f"Cannot adopt baseline V009: column CORE.BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT.FOCUS_RATIO scale is not >= 5 in {self.target_db}."
                )

            # 4. Verify OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT enhanced telemetry columns
            v009_assessment_cols = [
                'ASSESSMENT_TRIGGER', 'EVENT_VERSION_KEY', 'CLASSIFICATION_PROTOCOL_VERSION',
                'RULE_CONFIG_VERSION', 'RULE_CONFIG_HASH', 'EVIDENCE_MANIFEST_HASH',
                'REFERENCE_PERIOD_START', 'REFERENCE_PERIOD_END', 'EFFECTIVE_WINDOW_DAYS',
                'MESSI_VIDEO_COUNT', 'RONALDO_VIDEO_COUNT', 'MESSI_PREVALENCE', 'RONALDO_PREVALENCE',
                'FOCUS_RATIO', 'FALLBACK_USED', 'DEDUPLICATION_RULE_VERSION',
                'CHANNEL_TYPE_EVIDENCE', 'PLAYER_FOCUS_EVIDENCE', 'GIT_COMMIT_SHA', 'INGESTION_RUN_ID'
            ]
            cols_clause = ", ".join(f"'{c}'" for c in v009_assessment_cols)
            cur.execute(
                f"SELECT count(*) FROM {self.target_db}.information_schema.columns "
                f"WHERE table_schema = 'OPS' AND table_name = 'FACT_CHANNEL_CLASSIFICATION_ASSESSMENT' "
                f"  AND column_name IN ({cols_clause})"
            )
            if cur.fetchone()[0] < len(v009_assessment_cols):
                raise BaselineAdoptionError(
                    f"Cannot adopt baseline V009: enhanced telemetry columns missing on OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT in {self.target_db}."
                )

            # 5. Verify OPS.MANUAL_OVERRIDE classification review columns
            v009_override_cols = [
                'CLASSIFICATION_AXIS_OVERRIDDEN', 'ORIGINAL_AXIS_VALUE', 'PROPOSED_AXIS_VALUE',
                'SUPPORTING_EVIDENCE', 'REVIEW_STATUS', 'APPROVED_BY_REVIEWER_IDENTITY', 'APPROVED_AT'
            ]
            ov_clause = ", ".join(f"'{c}'" for c in v009_override_cols)
            cur.execute(
                f"SELECT count(*) FROM {self.target_db}.information_schema.columns "
                f"WHERE table_schema = 'OPS' AND table_name = 'MANUAL_OVERRIDE' "
                f"  AND column_name IN ({ov_clause})"
            )
            if cur.fetchone()[0] < len(v009_override_cols):
                raise BaselineAdoptionError(
                    f"Cannot adopt baseline V009: classification review columns missing on OPS.MANUAL_OVERRIDE in {self.target_db}."
                )

    def adopt_baseline(self, up_to_version_num: int = 9, installed_by: Optional[str] = None) -> dict:
        """Baseline adoption is disabled for this release.

        Attempts fail closed immediately without stamping historical migrations as successful.
        Refer to docs/migration-runner-specification.md for the reviewed operator deployment procedure.
        """
        raise BaselineAdoptionDisabledError(
            f"Baseline adoption is disabled for this release. "
            f"Historical migrations up to V{up_to_version_num:03d} cannot be adopted or stamped "
            f"as successful in {self.target_db}. "
            f"Refer to docs/migration-runner-specification.md for the reviewed operator deployment procedure."
        )

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
                # Fencing check: verify active lock and unexpired lease before writing
                self.verify_active_lock(lock_token)

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

                # Fencing check: verify lock again right before executing statements
                self.verify_active_lock(lock_token)

                # Execute migration
                try:
                    self.conn.execute_string(sql_content)
                    elapsed_ms = int((time.time() - start_time) * 1000)

                    # Fencing check: verify lock before marking SUCCESS
                    self.verify_active_lock(lock_token)

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
                    # In case of LockLostError, don't attempt to record failed status with lost lock
                    if isinstance(ex, LockLostError):
                        raise
                    elapsed_ms = int((time.time() - start_time) * 1000)
                    error_msg = str(ex)[:2040]

                    # Record failure in ledger
                    try:
                        self.verify_active_lock(lock_token)
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
                    except Exception:
                        pass

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
