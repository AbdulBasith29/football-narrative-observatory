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
Coordinates distributed concurrency to prevent simultaneous execution or statement interleaving. Because Snowflake **does not enforce PRIMARY KEY or UNIQUE constraints** at write time on standard tables, concurrency cannot rely on constraint violation errors from concurrent `INSERT` statements. Instead, `OPS.SCHEMA_LOCK` is pre-seeded with a single persistent row (`GLOBAL_MIGRATION_LOCK`). Processes acquire the lock via an atomic conditional `UPDATE` checking `rowcount == 1`.

```sql
CREATE TABLE IF NOT EXISTS OPS.SCHEMA_LOCK (
    lock_id             VARCHAR(64) NOT NULL, -- Constant: 'GLOBAL_MIGRATION_LOCK'
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
  1. Ensures `OPS.SCHEMA_VERSION` and `OPS.SCHEMA_LOCK` exist (and seeds singleton `GLOBAL_MIGRATION_LOCK` row via idempotent `MERGE`).
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

### Scenario 2: Existing Database with No Ledger (Baseline Adoption)
* **Initial State**: A database where Phase 1E objects already exist, but `OPS.SCHEMA_VERSION` was never created.
* **Crucial Invariant**: The runner **NEVER marks historical migrations successful merely because some expected objects exist**.
* **Runner Action**:
  1. Creates `OPS.SCHEMA_VERSION` and `OPS.SCHEMA_LOCK` (seeding singleton row).
  2. Acquires lock with lease.
  3. If invoked with explicit baseline adoption (`--adopt-baseline V009`):
     - Validates that target migrations `V001` through `V009` exist on disk.
     - **Physical Signature Verification**: For every version being adopted, inspects Snowflake metadata to verify that the complete physical database objects, column contracts, and deterministic seed data created by that migration exist in the target database:
       * `V001`: `OPS.FACT_API_REQUEST.PAGE_TOKEN_USED` (length $\ge 1024$) and `RAW.YOUTUBE_API_RESPONSE.PAGE_TOKEN_USED` (length $\ge 1024$).
       * `V002`: `OPS.FACT_INGESTION_RUN.RUN_PURPOSE`.
       * `V003`: `CORE.BRIDGE_VIDEO_EVENT` columns `PRIMARY_EXCLUSION_REASON`, `OBSERVED_AT`, `IS_WINDOW_OVERLAP`.
       * `V004`: `CORE.DIM_CHANNEL_FRAME_VERSION`, `CORE.BRIDGE_FRAME_CHANNEL`, `CORE.FRAME_CHANNEL_SOURCE_OBSERVATION`.
       * `V005`: `OPS.DISCOVERY_UNIT_STATE` table + `LAST_ERROR_CODE`.
       * `V006`: `OPS.VIDEO_METADATA_RESOLUTION_STATE` table + `LAST_ERROR_CODE`.
       * `V007`: `CORE.FACT_VIDEO_SNAPSHOT.API_REQUEST_ID` and `CORE.FACT_VIDEO_SNAPSHOT.RAW_RESPONSE_ID`.
       * `V008`: `CORE.DIM_EVENT_VERSION.BASELINE_TERMS`.
       * `V009`: Complete V009 schema changes and deterministic seed data:
         - Tables: `OPS.FACT_CHANNEL_ASSESSMENT_EVIDENCE_ITEM`, `OPS.CHANNEL_EVIDENCE_ACQUISITION_STATE`, `CORE.DIM_CHANNEL_STRATUM`.
         - Deterministic seed data: `CORE.DIM_CHANNEL_STRATUM` row count $\ge 30$ (verifying the 30 composite strata seeded by V009).
         - Schema extensions: `CORE.BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT` columns `HEADLINE_DECOMPOSITION_ELIGIBLE` and `FOCUS_RATIO` (numeric scale $\ge 5$).
         - Telemetry columns: `OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT` has all 20 classification/telemetry columns.
         - Override columns: `OPS.MANUAL_OVERRIDE` has all 7 override columns.
       If any signature or seed data check is missing, baseline adoption is rejected immediately (`BaselineAdoptionError`) before stamping any ledger records.
     - **Existing Ledger Validation**: For any historical migration already present in `OPS.SCHEMA_VERSION`:
       * Verifies disk SHA-256 matches recorded checksum (rejects with `MigrationChecksumMismatchError` on mismatch).
       * Verifies recorded status is not `FAILED` (rejects with `UnresolvedFailedMigrationError`).
       * Verifies recorded status is not `IN_PROGRESS` (rejects with `InterruptedMigrationError`).
     - **Lock Fencing**: Verifies active lock lease before starting verification and before inserting each baseline record (`verify_active_lock()`).
     - For unrecorded versions: inserts ledger records with `execution_type = 'BASELINE_ADOPTION'`, `status = 'SUCCESS'`, `execution_time_ms = 0`.
     - Releases lock.
     - Subsequent `--migrate` calls resume sequentially from `V010`.

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
       FATAL: Checksum mismatch for migration V004 (V004__expand_event_terms.sql)!
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

### Scenario 6: Concurrent Invocation (Atomic Distributed Lock)
* **Initial State**: Process A and Process B attempt to run migrations on the same target database simultaneously.
* **Snowflake Lock Realities**: Because standard Snowflake tables do not enforce primary key uniqueness, two concurrent runners performing `INSERT` could both succeed, creating duplicate lock rows.
* **Atomic UPDATE Pattern**:
  1. Both processes execute an atomic `UPDATE` against the pre-seeded singleton row:
     ```sql
     UPDATE OPS.SCHEMA_LOCK
     SET locked_at = CURRENT_TIMESTAMP(),
         locked_by = :user,
         lock_token = :token,
         lock_timeout_sec = 900
     WHERE lock_id = 'GLOBAL_MIGRATION_LOCK'
       AND (locked_by IS NULL OR DATEADD(second, lock_timeout_sec, locked_at) < CURRENT_TIMESTAMP());
     ```
  2. In Snowflake, row-level updates are serialized.
     - Process A updates the row: `cursor.rowcount == 1`. Process A acquired the lock.
     - Process B attempts the update: `locked_by` is not NULL and lease has not expired. The condition evaluates to false: `cursor.rowcount == 0`. Process B did not acquire the lock.
  3. Process B inspects `OPS.SCHEMA_LOCK`:
     - Reads current `locked_by`, `lock_token`, `locked_at`, `lock_timeout_sec`.
     - If the lease is active ($< 900$ seconds elapsed):
       Raises `ConcurrentMigrationError`:
       ```text
       Database FOOTBALL_NARRATIVE_DEV is currently locked by user 'ANALYST_A' (token: 5a2c...).
       Lock acquired at 2026-10-01T22:15:00. Lease active for 900 seconds.
       ```
       Process B terminates cleanly without executing statements.
     - If lease has expired ($\ge 900$ seconds elapsed):
       Process B breaks the stale lease and acquires the lock atomically.
  4. **Strict Ownership Verification on Release**:
     When Process A finishes, it releases the lock using:
     ```sql
     UPDATE OPS.SCHEMA_LOCK
     SET locked_at = NULL, locked_by = NULL, lock_token = NULL
     WHERE lock_id = 'GLOBAL_MIGRATION_LOCK' AND lock_token = :token_a;
     ```
     Verifying `lock_token = :token_a` ensures that a slow process whose lease expired cannot inadvertently release a lock that was subsequently acquired by another process.

---

### Scenario 7: Active Run Exceeding Lease & Continuous Write Fencing
* **Initial State**: Process A acquires the lock with lease duration 900 seconds (`token_a`). Due to slow network, heavy catalog queries, or long-running DDL, Process A takes longer than 900 seconds.
* **Problem Addressed**: If lease expiry allows Process B to break the lease and acquire `token_b`, Process A must NOT continue writing to `OPS.SCHEMA_VERSION` or executing further DDL/seed statements concurrently with Process B.
* **Runner Action**:
  1. **Continuous Fencing via `verify_active_lock()`**:
     Before every write operation (stamping `IN_PROGRESS`, executing a migration script, recording `SUCCESS`/`FAILED`, or stamping baseline adoption rows), the runner inspects `OPS.SCHEMA_LOCK`:
     ```sql
     SELECT locked_by, lock_token, locked_at, lock_timeout_sec
     FROM OPS.SCHEMA_LOCK
     WHERE lock_id = 'GLOBAL_MIGRATION_LOCK';
     ```
  2. **Token & Lease Validation**:
     - The runner verifies that `lock_token == self.active_lock_token`.
     - When valid and approaching expiry, the runner can proactively renew the lease via `renew_lease()`:
       ```sql
       UPDATE OPS.SCHEMA_LOCK
       SET locked_at = CURRENT_TIMESTAMP(), lock_timeout_sec = :timeout
       WHERE lock_id = 'GLOBAL_MIGRATION_LOCK' AND lock_token = :my_token;
       ```
     - If the lock token does not match (or the lock was cleared or broken by another runner), the runner halts immediately and raises `LockLostError` (subclass of `ConcurrentMigrationError`):
       ```text
       FATAL: Active lock lease was lost or overtaken! Expected token 5a2c..., but lock held by 'ANALYST_B' (token 9b1e...). Halting execution immediately to prevent concurrent writes.
       ```
  3. **Zero Orphan Writes**: No further SQL statements or ledger mutations are permitted once lock ownership is lost, preventing brain-split conflicts.

---

## 4. Summary Table of Runner Guarantees

| Invariant | Enforcement Mechanism | Failure Mode |
| :--- | :--- | :--- |
| **Script Immutability** | SHA-256 checksum comparison on every run | `MigrationChecksumMismatchError` (fail closed) |
| **Lock Atomicity** | `UPDATE OPS.SCHEMA_LOCK` singleton row condition (`rowcount == 1`) | `ConcurrentMigrationError` (fail closed; unenforced PK immune) |
| **Lock Ownership** | `UPDATE ... WHERE lock_token = %s` on release | Prevents stale process from releasing active lock |
| **Active Run Fencing** | `verify_active_lock()` before all DDL and ledger writes | `LockLostError` (halts immediately if lease overtaken) |
| **Execution Order** | Integer version sorting (`V001` < `V002` < ... < `V010`) | Disallows out-of-order execution |
| **Failure Halting** | Catches DDL error, updates `status = 'FAILED'`, halts runner | `MigrationExecutionError`, blocks subsequent scripts |
| **Interrupted Safety** | Blocks replay on `IN_PROGRESS` ledger state | `InterruptedMigrationError` (fail closed on crash) |
| **Baseline Distinction** | Explicit `execution_type`: `'MIGRATION'` vs `'BASELINE_ADOPTION'` | Never conflates inferred state with actual run |
| **Signature Certification** | Physical table, column, and seed data checks for V001–V009 | `BaselineAdoptionError` (rejects unverified baseline) |
| **DDL Auto-Commit Decoupling** | Per-migration ledger accounting with elapsed time and failure reason | Accurately records partial execution state |
