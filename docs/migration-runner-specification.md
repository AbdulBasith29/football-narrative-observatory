# Observatory Migration Ledger & Runner Specification

**Module**: `scripts/migration_runner.py` & `infra/snowflake/migrations/`  
**Governing Standard**: [`docs/11-data-warehouse-design.md`](file:///c:/Users/abdul/Documents/football-narrative-observatory/docs/11-data-warehouse-design.md) & [`.agents/rules/architecture.md`](file:///c:/Users/abdul/Documents/football-narrative-observatory/.agents/rules/architecture.md)  
**Status**: APPROVED FOUNDATION (Package 2A)

---

## 1. Architectural Purpose & Snowflake Realities

The Football Narrative Observatory requires deterministic, verifiable schema evolution across development (`FOOTBALL_NARRATIVE_DEV`), test (`FOOTBALL_NARRATIVE_TEST`), and future production environments.

### Critical Snowflake DDL Execution Realities:
1. **Implicit Autonomous DDL Commits**: In Snowflake, all DDL commands (`CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, etc.) execute in their own isolated auto-commit transactions. Even within an explicit SQL transaction block (`BEGIN ... COMMIT`), a DDL statement immediately commits prior DML work and commits its own structural changes unconditionally.
2. **DDL-Ledger Decoupling**: If a migration script executes 5 DDL statements and fails on the 4th, statements 1 through 3 **remain committed in the database**. The ledger update cannot "roll back" the committed DDL.
3. **No Database-Level Lock for DDL**: Snowflake does not provide distributed database-level table locks for custom migration tools. Concurrency must be governed via an atomic advisory lock pattern.
4. **Informational Constraints**: `PRIMARY KEY`, `FOREIGN KEY`, and `UNIQUE` constraints are declared for query optimization and tools metadata, but Snowflake does not enforce them at write time.

Therefore, the migration runner must:
* Maintain an immutable execution ledger (`OPS.SCHEMA_VERSION`).
* Enforce distributed concurrency protection (`OPS.SCHEMA_LOCK`).
* Distinguish actual migration execution from verified baseline adoption.
* Fail closed upon script error, checksum mismatch, or concurrent conflict.

---

## 2. Ledger Schema Architecture

### 2.1 `OPS.SCHEMA_VERSION`
Tracks the complete provenance, checksum, execution type, and outcome of every migration script.

```sql
CREATE TABLE IF NOT EXISTS OPS.SCHEMA_VERSION (
    installed_rank      NUMBER(38,0) NOT NULL,
    version             VARCHAR(32) NOT NULL,
    description         VARCHAR(256) NOT NULL,
    script_name         VARCHAR(256) NOT NULL,
    checksum            VARCHAR(64) NOT NULL, -- SHA-256 hex digest of verbatim file content
    execution_type      VARCHAR(32) NOT NULL, -- 'MIGRATION', 'BASELINE_ADOPTION', 'REPLAY'
    installed_by        VARCHAR(128) NOT NULL,
    installed_on        TIMESTAMP_NTZ NOT NULL,
    execution_time_ms   NUMBER(38,0) NOT NULL,
    status              VARCHAR(32) NOT NULL, -- 'SUCCESS', 'FAILED', 'IN_PROGRESS'
    error_message       VARCHAR(2048),
    CONSTRAINT pk_schema_version PRIMARY KEY (version),
    CONSTRAINT uq_schema_version_rank UNIQUE (installed_rank)
);
```

### 2.2 `OPS.SCHEMA_LOCK`
Coordinates distributed concurrency to prevent simultaneous execution or statement interleaving. Because Snowflake **does not enforce PRIMARY KEY or UNIQUE constraints** at write time on standard tables, concurrency cannot rely on constraint violation errors from concurrent `INSERT` statements. Instead, `OPS.SCHEMA_LOCK` is pre-seeded with a single persistent row (`MIGRATION_RUNNER_LOCK`). Processes acquire the lock via an atomic conditional `UPDATE` checking `rowcount == 1`.

```sql
CREATE TABLE IF NOT EXISTS OPS.SCHEMA_LOCK (
    lock_id             VARCHAR(64) NOT NULL, -- Constant: 'MIGRATION_RUNNER_LOCK'
    locked_at           TIMESTAMP_NTZ,        -- NULL when lock is free
    locked_by           VARCHAR(128),         -- NULL when lock is free
    lock_token          VARCHAR(36),          -- UUIDv4 unique to running process, NULL when free
    lock_timeout_sec    NUMBER(38,0) NOT NULL DEFAULT 900, -- 15-minute lease duration
    CONSTRAINT pk_schema_lock PRIMARY KEY (lock_id)
);
```

---

## 3. Migration Runner Lifecycle & Scenarios

The runner discovers migration scripts from `infra/snowflake/migrations/` matching the pattern:
`V<version_number>__<description>.sql` (e.g. `V001__expand_pagination_token_columns.sql`, `V010__contract_hardening_and_schema_version.sql`).

Scripts are ordered strictly by integer version number ($1, 2, \dots, 9, 10, \dots$).

---

### Scenario 1: Fresh Database with No Ledger
* **Initial State**: Empty or newly created database; `OPS.SCHEMA_VERSION` does not exist.
* **Runner Action**:
  1. Ensures `OPS.SCHEMA_VERSION` and `OPS.SCHEMA_LOCK` exist (and seeds singleton `MIGRATION_RUNNER_LOCK` row via idempotent `MERGE`).
  2. Acquires migration lock with unique UUID token via atomic `UPDATE`.
  3. Discovers all available migration scripts on disk (`V001` through `V010`+).
  4. For each migration in ascending numerical order:
     - Calculates SHA-256 checksum of script file.
     - Inserts record into `OPS.SCHEMA_VERSION` with `status = 'IN_PROGRESS'`, `execution_type = 'MIGRATION'`.
     - Executes script SQL statements against the target database.
     - On completion: updates record to `status = 'SUCCESS'`, records elapsed time in milliseconds.
  5. Releases migration lock via atomic token-verified `UPDATE`.
* **Outcome**: Database fully upgraded with complete ledger tracking every applied script.

---

### Scenario 2: Existing Database Deployment (Baseline Adoption Disabled)
* **Initial State**: An existing database (e.g. `FOOTBALL_NARRATIVE_DEV`, `FOOTBALL_NARRATIVE_TEST`) where Phase 1E physical objects already exist, but `OPS.SCHEMA_VERSION` does not exist or requires baseline adoption.
* **Release Decision**: **Baseline adoption is disabled for this release**.
  Attempts to invoke `runner.adopt_baseline()` or `--adopt-baseline` fail immediately with `BaselineAdoptionDisabledError`. Zero historical migrations are stamped as successful automatically.
* **Reviewed Deployment Procedure for Existing Databases**:
  To deploy the migration ledger onto existing databases containing Phase 1E objects, human operators must follow this reviewed, reproducible procedure:
  1. **Pre-Deployment Drift Reconciliation**:
     Run the read-only drift reconciler:
     ```bash
     python scripts/reconcile_deployment_drift.py --target-db FOOTBALL_NARRATIVE_DEV
     ```
     Inspect the emitted reconciliation report. Verify that tables, columns, numeric scale/precision, nullability, and primary/unique key column compositions match Phase 1E repository DDL.
  2. **Human Operator Sign-Off**:
     A human reviewer verifies that physical objects corresponding to migrations `V001` through `V009` are present, correctly formed, and consistent with frozen methodology.
  3. **Explicit Operator Ledger Seeding (Human-Executed SQL)**:
     An operator executes an explicit, reviewed DDL/DML script directly in Snowflake to initialize the ledger tables and record verified baseline entries:
     ```sql
     -- 1. Create OPS schema and ledger tables
     CREATE SCHEMA IF NOT EXISTS OPS;
     CREATE TABLE IF NOT EXISTS OPS.SCHEMA_VERSION (
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

     CREATE TABLE IF NOT EXISTS OPS.SCHEMA_LOCK (
         lock_id             VARCHAR(64) NOT NULL,
         locked_at           TIMESTAMP_NTZ,
         locked_by           VARCHAR(128),
         lock_token          VARCHAR(36),
         lock_timeout_sec    NUMBER(38,0) NOT NULL DEFAULT 900,
         CONSTRAINT pk_schema_lock PRIMARY KEY (lock_id)
     );

     MERGE INTO OPS.SCHEMA_LOCK AS target
     USING (SELECT 'MIGRATION_RUNNER_LOCK' AS lock_id) AS src
     ON target.lock_id = src.lock_id
     WHEN NOT MATCHED THEN
         INSERT (lock_id, locked_at, locked_by, lock_token, lock_timeout_sec)
         VALUES (src.lock_id, NULL, NULL, NULL, 900);

     -- 2. Populate reviewed baseline records with verbatim disk SHA-256 hashes
     INSERT INTO OPS.SCHEMA_VERSION (
         installed_rank, version, description, script_name, checksum,
         execution_type, installed_by, installed_on, execution_time_ms, status
     ) VALUES
         (1, 'V001', 'expand_pagination_token_columns', 'V001__expand_pagination_token_columns.sql', '<V001_SHA256>', 'REVIEWED_BASELINE', CURRENT_USER(), CURRENT_TIMESTAMP(), 0, 'SUCCESS'),
         (2, 'V002', 'add_run_purpose', 'V002__add_run_purpose.sql', '<V002_SHA256>', 'REVIEWED_BASELINE', CURRENT_USER(), CURRENT_TIMESTAMP(), 0, 'SUCCESS'),
         (3, 'V003', 'add_exclusion_reason', 'V003__add_exclusion_reason.sql', '<V003_SHA256>', 'REVIEWED_BASELINE', CURRENT_USER(), CURRENT_TIMESTAMP(), 0, 'SUCCESS'),
         (4, 'V004', 'add_channel_frame_tables', 'V004__add_channel_frame_tables.sql', '<V004_SHA256>', 'REVIEWED_BASELINE', CURRENT_USER(), CURRENT_TIMESTAMP(), 0, 'SUCCESS'),
         (5, 'V005', 'add_event_terms', 'V005__add_event_terms.sql', '<V005_SHA256>', 'REVIEWED_BASELINE', CURRENT_USER(), CURRENT_TIMESTAMP(), 0, 'SUCCESS'),
         (6, 'V006', 'create_discovery_unit_state', 'V006__create_discovery_unit_state.sql', '<V006_SHA256>', 'REVIEWED_BASELINE', CURRENT_USER(), CURRENT_TIMESTAMP(), 0, 'SUCCESS'),
         (7, 'V007', 'add_metadata_resolution_and_snapshot_lineage', 'V007__add_metadata_resolution_and_snapshot_lineage.sql', '<V007_SHA256>', 'REVIEWED_BASELINE', CURRENT_USER(), CURRENT_TIMESTAMP(), 0, 'SUCCESS'),
         (8, 'V008', 'add_baseline_terms_to_event_version', 'V008__add_baseline_terms_to_event_version.sql', '<V008_SHA256>', 'REVIEWED_BASELINE', CURRENT_USER(), CURRENT_TIMESTAMP(), 0, 'SUCCESS'),
         (9, 'V009', 'channel_stratification_and_overrides', 'V009__channel_stratification_and_overrides.sql', '<V009_SHA256>', 'REVIEWED_BASELINE', CURRENT_USER(), CURRENT_TIMESTAMP(), 0, 'SUCCESS');
     ```
  4. **Resume Automated Upgrades**:
     Once `OPS.SCHEMA_VERSION` is populated by the operator, `MigrationRunner.run_migrations()`:
     - Validates the recorded checksums against disk for `V001` through `V009`.
     - Identifies `V010` as the first pending script.
     - Acquires the lock and executes `V010` and subsequent migrations automatically.

---

### Scenario 3: Already-Applied Migrations and Checksum Changes
* **Initial State**: Migrations `V001`–`V008` exist in `OPS.SCHEMA_VERSION` with `status = 'SUCCESS'`.
* **Runner Action**:
  1. For every migration already recorded in the ledger:
     - Reads the script file on disk.
     - Computes current SHA-256 checksum.
     - Compares disk checksum against the recorded `checksum` column.
  2. **Checksum Integrity Check**:
     - If disk checksum == recorded checksum: **Verification passes**.
     - If disk checksum != recorded checksum: **FAIL CLOSED**.
       Raises `MigrationChecksumMismatchError`:
       ```text
       FATAL: Checksum mismatch for migration V004 (V004__add_channel_frame_tables.sql)!
       Recorded in ledger (SUCCESS): 3a9f...c120, On Disk: 8e1b...d944.
       Applied migrations are immutable.
       ```
       Execution terminates immediately. No further migrations are applied.

---

### Scenario 4: Partially Applied Migrations and Failed Execution
* **Initial State**: A migration `V010` contains 5 statements. Statement 3 fails (e.g. syntax error or permission failure).
* **Snowflake Reality**: Statements 1 and 2 **have already committed** in Snowflake due to auto-commit DDL.
* **Runner Action**:
  1. Catches the execution exception.
  2. Updates `OPS.SCHEMA_VERSION` for `V010`:
     - `status = 'FAILED'`
     - `error_message = <exact exception text and statement details>`
     - `execution_time_ms = <elapsed>`
  3. Releases migration lock.
  4. Raises `MigrationExecutionError` and halts runner execution.
* **Interrupted / Crashed Migrations (`IN_PROGRESS`)**:
  - If a runner crashed or was killed midway through executing statements, `OPS.SCHEMA_VERSION` retains `status = 'IN_PROGRESS'`.
  - On restart, the runner **never automatically replays or marks successful** an `IN_PROGRESS` migration.
  - The runner validates the disk checksum against the `IN_PROGRESS` entry. If mismatched, raises `MigrationChecksumMismatchError`.
  - If matched, raises `InterruptedMigrationError`.
  - Because prior DDL committed autonomously, manual operator verification of the database state is required before resuming.

---

### Scenario 5: Repeated Invocation (Idempotency)
* **Initial State**: Database is up to date; all migrations `V001` through `V010` are applied with `status = 'SUCCESS'`.
* **Runner Action**:
  1. Acquires lock.
  2. Verifies checksums of all applied migrations against disk files (zero drift).
  3. Identifies that pending migration list is empty.
  4. Releases lock.
  5. Returns clean status summary:
     ```json
     {"status": "UP_TO_DATE", "applied_count": 0, "total_migrations": 10}
     ```
  6. Zero DDL statements are executed against Snowflake.

---

### Scenario 6: Concurrent Invocation & Removal of Automatic Expired-Lock Takeover
* **Initial State**: Process A and Process B attempt to run migrations on the same target database simultaneously.
* **Snowflake Lock Realities**: Because standard Snowflake tables do not enforce primary key uniqueness, two concurrent runners performing `INSERT` could both succeed, creating duplicate lock rows.
* **Atomic UPDATE Pattern**:
  1. Both processes execute an atomic `UPDATE` against the pre-seeded singleton row requiring `locked_by IS NULL`:
     ```sql
     UPDATE OPS.SCHEMA_LOCK
     SET locked_at = CURRENT_TIMESTAMP(),
         locked_by = :user,
         lock_token = :token,
         lock_timeout_sec = 900
     WHERE lock_id = 'MIGRATION_RUNNER_LOCK'
       AND locked_by IS NULL;
     ```
  2. In Snowflake, row-level updates are serialized.
     - Process A updates the row: `cursor.rowcount == 1`. Process A acquired the lock.
     - Process B attempts the update: `locked_by` is not NULL. The condition evaluates to false: `cursor.rowcount == 0`. Process B did not acquire the lock.
  3. **Automatic Expired-Lock Takeover is Removed**:
     - Process B inspects `OPS.SCHEMA_LOCK`.
     - An occupied lock (`locked_by IS NOT NULL`) blocks Process B unconditionally, regardless of how much time has elapsed since `locked_at`.
     - Process B raises `ConcurrentMigrationError`:
       ```text
       Database FOOTBALL_NARRATIVE_DEV is locked by ANALYST_A (lock token: 5a2c..., acquired at: 2026-10-01T22:15:00).
       Automatic expired-lock takeover is disabled. An occupied lock blocks execution until explicitly recovered by an operator via recover_lock().
       ```
     - Process B terminates cleanly without executing statements.
  4. **Strict Token-Protected Release**:
     When Process A finishes normally, it releases the lock using:
     ```sql
     UPDATE OPS.SCHEMA_LOCK
     SET locked_at = NULL, locked_by = NULL, lock_token = NULL
     WHERE lock_id = 'MIGRATION_RUNNER_LOCK' AND lock_token = :token_a;
     ```
     Verifying `lock_token = :token_a` ensures that only the acquiring process can release its lock.

---

### Scenario 7: Operator Lock Recovery & Write Fencing
* **Problem Addressed**: If Process A crashed abnormally or was terminated without releasing its lock, the lock remains occupied and blocks subsequent runs indefinitely.
* **Operator Recovery Procedure**:
  1. An operator investigates whether Process A is truly dead or aborted.
  2. Once confirmed, the operator clears the orphaned lock using `MigrationRunner.recover_lock()` or executing:
     ```sql
     UPDATE OPS.SCHEMA_LOCK
     SET locked_at = NULL, locked_by = NULL, lock_token = NULL
     WHERE lock_id = 'MIGRATION_RUNNER_LOCK';
     ```
  3. Subsequent runners can now acquire the lock cleanly.
* **Write Fencing via `verify_active_lock()`**:
  Before every write operation (stamping `IN_PROGRESS`, executing DDL statements, recording `SUCCESS`/`FAILED`), the active runner verifies that `lock_token == self.active_lock_token`.
  If an operator cleared the lock or another runner acquired it, the original process halts immediately with `LockLostError(ConcurrentMigrationError)`, guaranteeing zero orphan writes.

---

## 4. Summary Table of Runner Guarantees

| Invariant | Enforcement Mechanism | Failure Mode |
| :--- | :--- | :--- |
| **Script Immutability** | SHA-256 checksum comparison on every run | `MigrationChecksumMismatchError` (fail closed) |
| **Lock Atomicity** | Atomic `UPDATE OPS.SCHEMA_LOCK WHERE locked_by IS NULL` (`rowcount == 1`) | `ConcurrentMigrationError` (fail closed; unenforced PK immune) |
| **No Automatic Takeover** | Occupied locks block all runners indefinitely | `ConcurrentMigrationError` (requires explicit operator recovery) |
| **Operator Lock Recovery** | `recover_lock()` clears orphaned lock | Operator-initiated unblocking of crashed runners |
| **Lock Ownership** | `UPDATE ... WHERE lock_token = %s` on release | Prevents unauthorized or cross-process release |
| **Write Fencing** | `verify_active_lock()` before all DDL and ledger writes | `LockLostError` (halts immediately if lock cleared or overtaken) |
| **Execution Order** | Integer version sorting (`V001` < `V002` < ... < `V010`) | Disallows out-of-order execution |
| **Failure Halting** | Catches DDL error, updates `status = 'FAILED'`, halts runner | `MigrationExecutionError`, blocks subsequent scripts |
| **Interrupted Safety** | Blocks replay on `IN_PROGRESS` ledger state | `InterruptedMigrationError` (fail closed on crash) |
| **Baseline Disabled** | `adopt_baseline()` disabled for this release | `BaselineAdoptionDisabledError` (requires reviewed manual deployment) |
| **DDL Auto-Commit Decoupling** | Per-migration ledger accounting with elapsed time and failure reason | Accurately records partial execution state |
