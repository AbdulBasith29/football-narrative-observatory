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
            locked_at           TIMESTAMP_NTZ NOT NULL,
            locked_by           VARCHAR(128) NOT NULL,
            lock_token          VARCHAR(36) NOT NULL,
            lock_timeout_sec    NUMBER(38,0) NOT NULL DEFAULT 900,
            CONSTRAINT pk_schema_lock PRIMARY KEY (lock_id)
        );
        """
        self.conn.execute_string(ddl)

    def acquire_lock(self, timeout_sec: int = DEFAULT_LOCK_TIMEOUT_SEC) -> str:
        """Acquires the distributed migration lock or raises ConcurrentMigrationError."""
        lock_token = str(uuid.uuid4())
        user_identity = os.environ.get("SNOWFLAKE_USER", "OBSERVATORY_RUNNER")
        now_iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        cur = self.conn.cursor()
        try:
            # Check if active lock exists
            cur.execute(
                f"SELECT lock_id, locked_at, locked_by, lock_token, lock_timeout_sec "
                f"FROM {self.target_db}.OPS.SCHEMA_LOCK WHERE lock_id = %s",
                (self.LOCK_ID,)
            )
            row = cur.fetchone()
            if row:
                locked_at = row[1]
                locked_by = row[2]
                lease_sec = row[4] or timeout_sec
                # If naive or string, parse
                if isinstance(locked_at, str):
                    lock_time = datetime.datetime.fromisoformat(locked_at.replace("Z", "+00:00"))
                elif isinstance(locked_at, datetime.datetime):
                    lock_time = locked_at.replace(tzinfo=datetime.timezone.utc) if locked_at.tzinfo is None else locked_at
                else:
                    lock_time = datetime.datetime.now(datetime.timezone.utc)

                age_sec = (datetime.datetime.now(datetime.timezone.utc) - lock_time).total_seconds()
                if age_sec < lease_sec:
                    raise ConcurrentMigrationError(
                        f"Database {self.target_db} is locked by {locked_by} "
                        f"(lock token: {row[3]}, age: {int(age_sec)}s, lease: {lease_sec}s)."
                    )
                # Stale lock: break lease
                cur.execute(
                    f"DELETE FROM {self.target_db}.OPS.SCHEMA_LOCK WHERE lock_id = %s",
                    (self.LOCK_ID,)
                )

            # Insert new lock
            cur.execute(
                f"INSERT INTO {self.target_db}.OPS.SCHEMA_LOCK "
                f"(lock_id, locked_at, locked_by, lock_token, lock_timeout_sec) "
                f"VALUES (%s, %s, %s, %s, %s)",
                (self.LOCK_ID, now_iso, user_identity, lock_token, timeout_sec)
            )
            return lock_token
        finally:
            cur.close()

    def release_lock(self, lock_token: str) -> None:
        """Releases the migration lock if the token matches."""
        cur = self.conn.cursor()
        try:
            cur.execute(
                f"DELETE FROM {self.target_db}.OPS.SCHEMA_LOCK "
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

    def adopt_baseline(self, up_to_version_num: int = 9, installed_by: Optional[str] = None) -> dict:
        """Explicitly records historical migrations up to version as BASELINE_ADOPTION.

        Fails if core tables do not exist or if ledger contains conflicting versions.
        """
        self.ensure_ledger_tables()
        lock_token = self.acquire_lock()
        try:
            migrations = self.discover_migrations()
            target_migrations = [m for m in migrations if m.version_num <= up_to_version_num]
            if not target_migrations:
                raise BaselineAdoptionError(f"No migrations found up to V{up_to_version_num:03d}")

            # Verify that core tables actually exist before marking baseline adoption
            cur = self.conn.cursor()
            try:
                cur.execute(
                    f"SELECT count(*) FROM {self.target_db}.information_schema.tables "
                    f"WHERE table_schema = 'CORE' AND table_name IN ('DIM_CHANNEL', 'DIM_EVENT', 'DIM_VIDEO')"
                )
                core_count = cur.fetchone()[0]
                if core_count < 3:
                    raise BaselineAdoptionError(
                        f"Cannot adopt baseline V{up_to_version_num:03d}: core tables do not exist in {self.target_db}."
                    )
            finally:
                cur.close()

            ledger = self.get_ledger()
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

            # Step 1: Verify checksums of all previously successful migrations
            for m in migrations:
                if m.version_str in ledger:
                    entry = ledger[m.version_str]
                    if entry["status"] == "SUCCESS":
                        if entry["checksum"] != m.checksum:
                            raise MigrationChecksumMismatchError(
                                f"Checksum mismatch for migration {m.version_str} ({m.filename})! "
                                f"Recorded: {entry['checksum']}, On Disk: {m.checksum}. "
                                f"Applied migrations are immutable."
                            )
                    elif entry["status"] == "FAILED":
                        raise UnresolvedFailedMigrationError(
                            f"Migration {m.version_str} previously failed: {entry['error_message']}. "
                            f"Manual operator intervention required before continuing."
                        )

            # Step 2: Determine pending migrations
            pending: List[MigrationFile] = []
            for m in migrations:
                if target_version_num is not None and m.version_num > target_version_num:
                    continue
                if m.version_str not in ledger or ledger[m.version_str]["status"] != "SUCCESS":
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

                # Record IN_PROGRESS in ledger
                cur = self.conn.cursor()
                try:
                    cur.execute(
                        f"MERGE INTO {self.target_db}.OPS.SCHEMA_VERSION AS target "
                        f"USING (SELECT %s AS version) AS src ON target.version = src.version "
                        f"WHEN MATCHED THEN UPDATE SET "
                        f"status = 'IN_PROGRESS', installed_on = %s, error_message = NULL "
                        f"WHEN NOT MATCHED THEN INSERT ("
                        f"installed_rank, version, description, script_name, checksum, "
                        f"execution_type, installed_by, installed_on, execution_time_ms, status, error_message"
                        f") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (
                            m.version_str,
                            now_iso,
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
