# Engineering Debug & Incident Log (Phase 1A – Phase 1C)

This document chronicles the major technical incidents, architectural contradictions, root cause analyses, and permanent resolutions encountered during the engineering of the **Football Narrative Observatory** (Phase 1A to Phase 1C).

---

## Incident 1: Snowflake Warehouse Suspension, MFA Enforcement, and Session Reuse

### Context
During test execution and database bootstrapping, connections to the primary Snowflake instance failed across multiple failure modes:
1. `DatabaseError: 390913 (08001)`: Free trial warehouse suspended and expired.
2. `DatabaseError: 390197 (08001)`: Multi-factor authentication (MFA) required on the new account.
3. `DatabaseError: 394508 (08001)`: Failed to authenticate: MFA with TOTP is required.
4. `DatabaseError: 390190 (08001)`: SAML Identity Provider account parameter error when attempting `authenticator=externalbrowser`.

### Root Cause Analysis
- **Account Transition**: The original Snowflake trial expired, requiring migration of DDL and migrations to a new account.
- **Mandatory MFA Policy**: Modern Snowflake trial accounts mandate MFA enrollment for administrative accounts upon initial creation.
- **SAML IdP Incompatibility**: Attempting `authenticator=externalbrowser` assumes an enterprise federated Single Sign-On (Okta / Azure AD SAML IdP) configuration, which is not present on standalone Snowflake accounts.
- **Ephemeral TOTP Lifetime**: TOTP passcodes expire every 30 seconds and cannot be reused across multiple successive connection handshakes (e.g., separate handshakes for DDL, migrations, and config sync).

### Resolution
- **TOTP Parameter Support**: Added `passcode` and `passcode_in_password` parameter resolution to `get_snowflake_connection` in [`scripts/ingestion_snowflake.py`](file:///c:/Users/abdul/Documents/football-narrative-observatory/scripts/ingestion_snowflake.py).
- **Single-Session Bootstrapper**: Implemented [`scripts/setup_snowflake.py`](file:///c:/Users/abdul/Documents/football-narrative-observatory/scripts/setup_snowflake.py) to authenticate exactly once using the single 6-digit TOTP passcode and reuse that authenticated connection across all DDL, migration, configuration synchronization, and schema verification steps for both `FOOTBALL_NARRATIVE_DEV` and `FOOTBALL_NARRATIVE_TEST`.

---

## Incident 2: Pytest Automatic Test Collection Conflicts

### Context
Running `pytest` without explicit arguments failed during test collection:
```
ERROR scripts/bootstrap_test.py - snowflake.connector.errors.DatabaseError
ERROR scripts/test_live_interruption_resume.py - snowflake.connector.errors.DatabaseError
```

### Root Cause Analysis
By default, `pytest` searches recursively from the repository root for any Python file matching `test_*.py` or `*_test.py`.
- `scripts/test_live_interruption_resume.py` and `scripts/test_youtube_api.py` matched this naming pattern and contained live integration execution logic at the module level.
- `scripts/bootstrap_test.py` had top-level executable code without `if __name__ == "__main__":`.
- No root test configuration was present in `pyproject.toml`.

### Resolution
- **Explicit Test Scoping**: Configured `[tool.pytest.ini_options]` in [`pyproject.toml`](file:///c:/Users/abdul/Documents/football-narrative-observatory/pyproject.toml) specifying `testpaths = ["tests"]`, `python_files = ["test_*.py", "*_test.py"]`, and `pythonpath = ["scripts"]`.
- **Script Sanitization**: Renamed manual test runners to non-matching names (`live_interruption_resume.py`, `verify_youtube_api.py`), removed obsolete bootstrap scripts, and wrapped all execution logic inside `if __name__ == "__main__":` entrypoint guards.

---

## Incident 3: In-Place Table Upgrade Without Data Loss (Migration V006)

### Context
During Phase 1C, the schema for `OPS.DISCOVERY_UNIT_STATE` needed column type modifications (`next_page_token VARCHAR(2048)`, nullable `started_at`, and error diagnostics `last_error_code`, `last_error_message`). Early drafts proposed dropping and recreating the table.

### Root Cause Analysis
Dropping `OPS.DISCOVERY_UNIT_STATE` during schema migrations destroys historical discovery execution records, breaks auditability, and resets pagination tokens for in-flight units.

### Resolution
Updated [`infra/snowflake/migrations/V006__create_discovery_unit_state.sql`](file:///c:/Users/abdul/Documents/football-narrative-observatory/infra/snowflake/migrations/V006__create_discovery_unit_state.sql) to use idempotent, in-place table modifications:
```sql
ALTER TABLE OPS.DISCOVERY_UNIT_STATE MODIFY COLUMN next_page_token VARCHAR(2048);
ALTER TABLE OPS.DISCOVERY_UNIT_STATE ALTER COLUMN started_at DROP NOT NULL;
ALTER TABLE OPS.DISCOVERY_UNIT_STATE ADD COLUMN IF NOT EXISTS last_error_code VARCHAR(128);
ALTER TABLE OPS.DISCOVERY_UNIT_STATE ADD COLUMN IF NOT EXISTS last_error_message VARCHAR(1024);
```
Verified via `test_migration_v006_inplace_alteration` in [`tests/test_discovery.py`](file:///c:/Users/abdul/Documents/football-narrative-observatory/tests/test_discovery.py).

---

## Incident 4: Pagination Token Loss on Partial Errors & HTTP 403 Conflation

### Context
In fallback search discovery (`search.list`), two critical issues were identified:
1. When a unit succeeded on page 1 and failed on page 2 due to a non-quota error (e.g., transient network disconnection), `next_page_token` was discarded and saved as `NULL`. Resuming the unit forced it to re-fetch page 1, wasting quota and creating duplicate processing.
2. Every HTTP 403 response was classified as quota exhaustion (`PARTIAL_QUOTA_LIMIT`), masking permission, authentication, and API-disabled errors as false quota limits.
3. `unique_video_ids_observed` was recalculated from scratch per attempt rather than accumulating monotonically across resumed runs.

### Root Cause Analysis
- **Token Preservation**: The loop set `next_page_token` only when `hit_limit` (quota limit) was true, ignoring non-quota interruptions.
- **Error Discrimination**: Google API returns HTTP 403 for both quota limits (`quotaExceeded`, `rateLimitExceeded`) and general authorization failures (`accessNotConfigured`, `forbidden`, `insufficientPermissions`).
- **Metric Grain**: `unit_items` was re-initialized to an empty dictionary on each attempt, wiping out previous page observations from the metric count.

### Resolution
- **Error Classifier**: Implemented `is_quota_or_rate_limit_error(err)` in [`scripts/discovery_youtube.py`](file:///c:/Users/abdul/Documents/football-narrative-observatory/scripts/discovery_youtube.py) to inspect structured error details and strings for documented quota reasons (`quotaExceeded`, `rateLimitExceeded`, `userRateLimitExceeded`, `dailyLimitExceeded`). All other 403s are classified as operational errors (`PARTIAL_ERROR` with `HTTP_403`).
- **Token Preservation**: Preserved `next_page_token` in `OPS.DISCOVERY_UNIT_STATE` whenever a page succeeded prior to an operational error (`PARTIAL_ERROR`).
- **Cumulative Unique Video Counting**: Computed `unique_video_ids_observed` via set union of previously persisted database records and newly observed video IDs, guaranteeing monotonicity across resumed runs.
- **Offline Regression Tests**: Added `test_partial_error_page_token_preservation_and_resume`, `test_http_403_distinguishes_quota_from_forbidden`, and `test_unique_video_ids_observed_cumulative_dedup` to [`tests/test_discovery.py`](file:///c:/Users/abdul/Documents/football-narrative-observatory/tests/test_discovery.py).

---

## Incident 5: Query Hash Determinism & Mock Alignment

### Context
In `tests/test_discovery.py::test_partial_error_recovery_lineage`, the test asserted `run_status == "COMPLETE"` but received `"FAILED"`.

### Root Cause Analysis
The test fixture hardcoded a legacy SHA-256 hash string (`414842db...`) computed from `b"messi"` (without quotes). The pipeline implementation uses quoted search tokens (`'"messi"'`), producing hash `9bcdae8e...`. At run completion, the completeness audit compared expected unit keys with post-run state keys; the hash mismatch caused the unit to be classified as incomplete, triggering run outcome `FAILED`.

### Resolution
Replaced hardcoded hash strings in test fixtures with dynamic calls to `compute_query_hash('"messi"')`, ensuring 100% deterministic key alignment between generation, state persistence, and audit reconciliation.
