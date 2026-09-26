---
trigger: glob
globs: "infra/snowflake/**, scripts/*snowflake*.py"
description: Snowflake database security, schema migration rules, DDL conventions, and session/connection safety.
---

# Snowflake Database & Migration Guidelines

These rules govern all database operations, migrations, DDL definitions, and connector code.

## 1. Migration Discipline
- **Sequential Versioning**: All schema alterations must be scripted as versioned SQL migration files under `infra/snowflake/migrations/` using `V<NNN>__<description>.sql` (e.g., `V006__create_discovery_unit_state.sql`).
- **No In-Place Breaking Edits**: Never modify existing, executed migration files. New schema modifications must always be introduced as a new migration step.
- **Idempotency**: All DDL statements must be idempotent (`CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`).
- **DDL Sync**: Mirror any migration changes into the base DDL scripts under `infra/snowflake/ddl/` so fresh environments bootstrap cleanly.

## 2. Safety & Data Protection
- **Zero Destruction**: NEVER execute `DROP DATABASE`, `DROP SCHEMA`, `DROP TABLE`, or `TRUNCATE` in migration scripts or automated routines without explicit user authorization.
- **Environment Targeting & Isolation**:
  - Integration/Unit testing: Target `FOOTBALL_NARRATIVE_TEST` exclusively (`run_purpose = 'INTEGRATION_TEST'`).
  - Pipeline development: Target `FOOTBALL_NARRATIVE_DEV` (or configured database from `.env` `SNOWFLAKE_DATABASE`).
  - Research runs: Must never consume `PIPELINE_PILOT` frames, and test/pilot data must never contaminate research tables.

## 3. Connector & Session Management
- **Single-Session Authentication**: Re-use authenticated Snowflake connections during bootstrap and multi-table operations. Accounts protected with TOTP/MFA invalidate one-time passcodes after 30 seconds; opening multiple connections in parallel will fail.
- **Resource Management**: Always close cursors and connections cleanly in `finally` blocks or context managers.
- **JSON / VARIANT Parsing**: Use `PARSE_JSON(%s)` when inserting structured provenance or telemetry into `VARIANT` columns.
