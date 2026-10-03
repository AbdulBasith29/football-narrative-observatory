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
Coordinates distributed concurrency to prevent simultaneous execution or statement interleaving.

```sql
CREATE TABLE IF NOT EXISTS OPS.SCHEMA_LOCK (
    lock_id             VARCHAR(32) NOT NULL, -- Constant: 'MIGRATION_RUNNER_LOCK'
    locked_at           TIMESTAMP_NTZ NOT NULL,
    locked_by           VARCHAR(128) NOT NULL,
    lock_token          VARCHAR(36) NOT NULL, -- UUIDv4 unique to running process
    lock_timeout_sec    NUMBER(38,0) NOT NULL DEFAULT 900, -- 15-minute lease
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
  1. Executes baseline bootstrap DDL scripts `00_database_and_schemas.sql` through `05_core_bridges.sql`.
  2. Ensures `OPS.SCHEMA_VERSION` and `OPS.SCHEMA_LOCK` exist.
  3. Acquires migration lock with unique UUID token.
  4. Discovers all available migration scripts on disk (`V001` through `V010`+).
  5. For each migration in ascending numerical order:
     - Calculates SHA-256 checksum of script file.
     - Inserts record into `OPS.SCHEMA_VERSION` with `status = 'IN_PROGRESS'`, `execution_type = 'MIGRATION'`.
     - Executes script SQL statements against the target database.
     - On completion: updates record to `status = 'SUCCESS'`, records elapsed time in milliseconds.
  6. Releases migration lock.
* **Outcome**: Database fully upgraded with complete ledger tracking every applied script.

---

### Scenario 2: Existing Database with No Ledger (Baseline Adoption)
* **Initial State**: A database where Phase 1E objects already exist, but `OPS.SCHEMA_VERSION` was never created.
* **Crucial Rule**: The runner **NEVER marks historical migrations successful merely because some expected objects exist**.
* **Runner Action**:
  1. Creates `OPS.SCHEMA_VERSION` and `OPS.SCHEMA_LOCK`.
  2. If invoked with standard `--migrate`:
     - Discovers unapplied migrations starting at `V001`.
     - Because `V001`–`V009` are idempotent (`IF NOT EXISTS`, `MERGE`, conditional constraints), they can be executed safely, or the database can be baselined explicitly.
  3. If invoked with explicit baseline adoption (`--adopt-baseline V009`):
     - Validates that target migrations `V001` through `V009` exist on disk.
     - Computes their exact SHA-256 checksums.
     - Inserts ledger records for `V001` through `V009` with `execution_type = 'BASELINE_ADOPTION'`, `status = 'SUCCESS'`, and `execution_time_ms = 0`.
     - **Verification Guard**: Baseline adoption verifies that core tables declared by the baseline exist before stamping the ledger. If expected core tables are missing, baseline adoption is rejected.
     - Subsequent `--migrate` calls will resume sequentially from `V010`.

---

### Scenario 3: Already-Applied Migrations and Checksum Changes
* **Initial State**: Migrations `V001`–`V008` exist in `OPS.SCHEMA_VERSION` with `status = 'SUCCESS'`.
* **Runner Action**:
  1. For every migration already recorded as `SUCCESS` in the ledger:
     - Reads the script file on disk.
     - Computes current SHA-256 checksum.
     - Compares disk checksum against the recorded `checksum` column.
  2. **Checksum Integrity Check**:
     - If disk checksum == recorded checksum: **Verification passes**.
     - If disk checksum != recorded checksum: **FAIL CLOSED**.
       Raises `MigrationChecksumMismatchError`:
       ```text
       FATAL: Migration V004 checksum mismatch!
       Recorded: 3a9f...c120
       On Disk:  8e1b...d944
       Applied migration files are immutable and must never be altered after deployment.
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
* **Subsequent Invocations**:
  - The runner detects a record in `OPS.SCHEMA_VERSION` with `status = 'FAILED'`.
  - The runner **refuses to run subsequent migrations** while any migration is in `FAILED` state.
  - Raises `UnresolvedFailedMigrationError`.
  - Requires operator intervention: resolve the root cause, ensure idempotency, and re-run (or repair via explicit administrator intervention).

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
     {"status": "UP_TO_DATE", "applied_count": 0, "verified_count": 10}
     ```
  6. Zero DDL statements are executed against Snowflake.

---

### Scenario 6: Concurrent Invocation (Distributed Lock)
* **Initial State**: Process A and Process B attempt to run migrations on the same target database simultaneously.
* **Runner Action**:
  1. Process A executes:
     ```sql
     INSERT INTO OPS.SCHEMA_LOCK (lock_id, locked_at, locked_by, lock_token, lock_timeout_sec)
     VALUES ('MIGRATION_RUNNER_LOCK', CURRENT_TIMESTAMP(), :user, :token_a, 900);
     ```
     Insert succeeds. Process A holds the lock.
  2. Process B attempts the same insert:
     - Primary key constraint on `lock_id = 'MIGRATION_RUNNER_LOCK'` causes insert collision error (`SQLSTATE 23505` / duplicate key).
  3. Process B inspects `OPS.SCHEMA_LOCK`:
     - Reads `locked_at`, `locked_by`, `lock_timeout_sec`.
     - Calculates elapsed time since `locked_at`.
     - If elapsed time < `lock_timeout_sec`:
       Raises `ConcurrentMigrationError`:
       ```text
       Database FOOTBALL_NARRATIVE_DEV is currently locked by user 'ANALYST_A' (token: 5a2c...).
       Lock acquired at 2026-10-01T22:15:00. Lease active for 900 seconds.
       ```
       Process B terminates cleanly without executing statements.
     - If elapsed time >= `lock_timeout_sec` (abandoned/crashed process):
       Process B logs a stale lock warning, breaks the expired lease, takes ownership, and proceeds.
  4. When Process A finishes, it executes:
     ```sql
     DELETE FROM OPS.SCHEMA_LOCK WHERE lock_id = 'MIGRATION_RUNNER_LOCK' AND lock_token = :token_a;
     ```
     Lock is cleanly released in a `finally` block.

---

## 4. Summary Table of Runner Guarantees

| Invariant | Enforcement Mechanism | Failure Mode |
| :--- | :--- | :--- |
| **Script Immutability** | SHA-256 checksum comparison on every run | `MigrationChecksumMismatchError` (fail closed) |
| **Concurrency Safety** | `OPS.SCHEMA_LOCK` atomic insert with 15-minute lease | `ConcurrentMigrationError` (fail closed) |
| **Execution Order** | Integer version sorting (`V001` < `V002` < ... < `V010`) | Disallows out-of-order execution |
| **Failure Halting** | Catches DDL error, updates `status = 'FAILED'`, halts runner | `MigrationExecutionError`, blocks subsequent scripts |
| **Baseline Distinction** | Explicit `execution_type`: `'MIGRATION'` vs `'BASELINE_ADOPTION'` | Never conflates inferred state with actual run |
| **DDL Auto-Commit Decoupling** | Per-migration ledger accounting with elapsed time and failure reason | Accurately records partial execution state |
