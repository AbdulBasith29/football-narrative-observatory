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

---

## Incident 6: Discovery Unit Grain Isolation for Unique Video Counting & Frame Binding

### Context
1. In cross-unit runs for the same channel and event window, historical video membership queries at `channel_key × event_version_key` grain erroneously caused videos observed by Unit A (query hash 1) to leak into Unit B's (query hash 2) `unique_video_ids_observed`.
2. Live and offline test runners had a hard-coded UUID (`69ea3d0b-6766-43d2-a34d-58811f5be278`) in `scripts/live_interruption_resume.py` and lacked explicit `frame_purpose = 'PIPELINE_PILOT'` filtering in `scripts/run_test_discovery.py`.

### Root Cause Analysis
- `DIM_VIDEO` joined with `BRIDGE_VIDEO_EVENT` without filtering on the exact discovery unit provenance (`frame_version_key`, `channel_key`, `window_key`, `discovery_policy_version`, `query_hash`). Consequently, any video discovered for that channel and event under any query hash was returned as historical membership.
- `live_interruption_resume.py` referenced a static frame UUID created during earlier local bootstrapping rather than dynamically querying Snowflake for the active pilot frame.

### Resolution
- **Exact Provenance Scoping**: Refactored the historical video query in [`scripts/discovery_youtube.py`](file:///c:/Users/abdul/Documents/football-narrative-observatory/scripts/discovery_youtube.py) using Snowflake `LATERAL FLATTEN(input => b.discovery_provenance:queries)` to strictly match `(frame_version_key, channel_key, window_key, discovery_policy_version, query_hash)`. Discovery provenance is enriched with all unit coordinates and appended when videos bridge across queries.
- **Dynamic Pilot Frame Resolution**: Updated [`scripts/live_interruption_resume.py`](file:///c:/Users/abdul/Documents/football-narrative-observatory/scripts/live_interruption_resume.py) and [`scripts/run_test_discovery.py`](file:///c:/Users/abdul/Documents/football-narrative-observatory/scripts/run_test_discovery.py) to dynamically query `CORE.DIM_CHANNEL_FRAME_VERSION` with `WHERE frame_purpose = 'PIPELINE_PILOT' ORDER BY constructed_at DESC LIMIT 1`.
- **Regression Test**: Added `test_unique_video_ids_observed_scoped_to_exact_discovery_unit` to [`tests/test_discovery.py`](file:///c:/Users/abdul/Documents/football-narrative-observatory/tests/test_discovery.py), asserting that for two discovery units sharing channel and event with different query hashes, Unit A's observed videos cannot increase Unit B's unique video count.

---

## Incident 7: Mock SQL Dispatch Collision and Manifest Evidence Serialization (Phase 1E)

### Context
During offline unit test suite development for Phase 1E (`tests/test_channel_stratification.py`):
1. State machine transition tests intermittently fell back to raw table inserts when checking `DIM_CHANNEL_STRATUM_VERSION` because queries were misrouted to `DIM_CHANNEL_STRATUM`.
2. Calculating the canonical SHA-256 evidence manifest hash raised `TypeError: Object of type datetime is not JSON serializable` when `published_at` datetime objects from database results were included directly in the item dictionary.

### Root Cause Analysis
- **Prefix Matching in In-Memory Mocks**: In `MockSnowflakeDatabase.execute`, simple substring checking (`if "FROM CORE.DIM_CHANNEL_STRATUM" in sql`) matched both `CORE.DIM_CHANNEL_STRATUM` and `CORE.DIM_CHANNEL_STRATUM_VERSION`, returning stratum definition records instead of version history records.
- **JSON Serialization of Temporal Types**: Python `json.dumps(..., sort_keys=True)` does not natively serialize Python `datetime` instances without an explicit `default` serializer. Additionally, including raw Python datetime objects in canonical evidence items can cause non-deterministic formatting variations across environments.

### Resolution
- **Ordered Mock Query Dispatch**: In `MockSnowflakeDatabase`, placed more specific table name checks (`CORE.DIM_CHANNEL_STRATUM_VERSION`) before general table checks (`CORE.DIM_CHANNEL_STRATUM`), ensuring unambiguous routing.
- **Canonical Datetime Normalization**: In `scripts/channel_classifier.py`, stripped runtime-parsed datetime fields before manifest hashing, relying strictly on ISO-8601 string timestamps (`published_at`), and added `default=str` to `json.dumps`, ensuring deterministic hash calculation across platforms.
- **Test Coverage**: Added `test_evidence_manifest_exact_reproducibility` and `test_type_2_scd_grain_and_lifecycle` to [`tests/test_channel_stratification.py`](file:///c:/Users/abdul/Documents/football-narrative-observatory/tests/test_channel_stratification.py).

---

## Incident 8: Phase 1E Review Hardening — RAW Persistence Schema, Dedup Fail-Closed & Snapshot Payload Immutability

### Context
During external review of Phase 1E implementation (HEAD `767e9ce`):
1. `channel_evidence_acquisition.py` inserted into `RAW.YOUTUBE_API_RESPONSE` using an ad-hoc column set rather than the canonical 16-column schema defined in `01_raw.sql`.
2. Cross-video deduplication was bypassing fail-closed behavior when the rule was unapproved (`deduplication_rule_version: null`), continuing prevalence calculations using raw distinct video IDs.
3. Event snapshot creation in `BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT` was comparing only subset classification labels rather than the full canonical payload (dates, video counts, prevalences, ratios, fallback status, override lineage), and defaulted missing values to 0/0.0.
4. Acquisition state machine did not progress completed 90D units with < 10 videos into the 180D fallback state.

### Root Cause Analysis
- **Schema Parity**: Ad-hoc insert syntax drifted from `RAW.YOUTUBE_API_RESPONSE` table DDL.
- **Fail-Closed Methodology**: Incomplete configuration must raise `DeduplicationRuleMissingError` and prevent prevalence estimation rather than silently substituting raw video counts.
- **Immutability Grain**: Snapshots represent historical epistemic anchors; any change in underlying statistics (even with unchanged classification labels) must trigger `SnapshotMutationViolationError`.

### Resolution
- **Canonical RAW Persistence**: Implemented `persist_raw_youtube_response` enforcing all 16 canonical columns before parsing downstream.
- **Fail-Closed Gate**: Enforced `DeduplicationRuleMissingError` in `classify_channel_assessment` unless an approved versioned rule exists or `EXPLICIT_NONE` is human-approved.
- **Full Payload Idempotency**: Verified all 17 snapshot fields; any mutation in counts, dates, or ratios raises `SnapshotMutationViolationError`.
- **Automatic Fallback Progression**: Implemented automatic initialization of `180D_FALLBACK` acquisition state when 90D primary units complete with < 10 eligible videos.
- **Test Coverage**: Added 6 dedicated regression tests in [`tests/test_channel_stratification.py`](file:///c:/Users/abdul/Documents/football-narrative-observatory/tests/test_channel_stratification.py) (68/68 tests passing).

---

## Incident 9: Phase 1E Review Hardening — Deduplication Status Gating, Sparse Statistics Ground-Truth, Snapshot NTZ Normalization, Numeric Column Range & Migration Replay Idempotency

### Context
During exact-SHA inspection of HEAD `edc3f3a` (Actions run 36675202548):
1. **Deduplication Gate Bypass & Execution**: `scripts/channel_classifier.py` accepted any non-empty rule version even when `deduplication_rule_status` was `HUMAN_APPROVAL_REQUIRED`. Furthermore, accepted versions only executed exact source-ID deduplication rather than executing the approved cross-video deduplication algorithm.
2. **Sparse Evidence Fabricated Statistics**: In `classify_player_focus`, evidence with fewer than 10 videos fabricated `messi_count = 0`, `ronaldo_count = 0`, prevalences `0.0`, and ratio `1.0`. While the classification label must remain `UNCLASSIFIED` and confidence `LOW`, assessments and snapshots must preserve actual ground-truth statistics.
3. **Snapshot TIMESTAMP_NTZ Comparison Mismatch**: `create_or_verify_event_channel_snapshot` compared timezone-aware candidate strings (e.g. `+00:00`) against naive `datetime` or strings returned by the Snowflake Python connector for `TIMESTAMP_NTZ` columns, triggering false `SnapshotMutationViolationError` on identical snapshot replays.
4. **focus_ratio Column Range Overflow**: A channel with 10 Messi videos and 0 Ronaldo videos produces smoothed ratio `(10 + 1) / (0 + 1) = 11.0`. Persistent columns in `02_ops.sql`, `05_core_bridges.sql`, and `V009` migration were typed as `NUMBER(6,5)`, whose maximum representable positive value is `9.99999`, causing insertion failures.
5. **Constraint Replay Error in V009 Migration**: `V009` line 61 unconditionally added `uq_event_channel_stratum_snapshot`, which is already declared in bootstrap DDL `05_core_bridges.sql`. When `setup_snowflake.py` runs bootstrap followed by migrations, Snowflake raised duplicate constraint errors because Snowflake SQL lacks `ADD CONSTRAINT IF NOT EXISTS`.

### Root Cause Analysis
- **Approval Gating**: Evaluated `is_rule_approved` solely on version string truthiness rather than checking `deduplication_rule_status in ("APPROVED", "APPROVED_EXPLICIT_NONE")`.
- **Methodology Metric Integrity**: Conflated unclassified categorical assignments with zeroed numerical statistics. Frozen methodology requires maintaining true observed counts and smoothed ratios even when sample size is insufficient for confident classification.
- **Warehouse Type Mapping**: Snowflake `TIMESTAMP_NTZ` is naive; Python connector returns naive `datetime`. Comparing with `.isoformat()` from timezone-aware candidate strings caused string inequality.
- **Column Precision**: `NUMBER(6,5)` allocates 1 integer digit ($6 - 5 = 1$), capping values at $9.99999$.
- **Migration Idempotency**: Snowflake Scripting requires exception handling (`EXCEPTION WHEN OTHER THEN NULL;`) inside single-statement anonymous blocks (`EXECUTE IMMEDIATE '...';`) to handle pre-existing constraints across bootstrap and replay runs.

### Resolution
- **Strict Deduplication Gating & Execution**: Enforced `dedup_status in ("APPROVED", "APPROVED_EXPLICIT_NONE")` in `classify_channel_assessment`. Introduced `DEDUPLICATION_RULE_REGISTRY` and `@register_deduplication_rule` decorator; when an approved rule version is configured, its registered logic is executed across video IDs, populating `deduplication_cluster_id` and excluding duplicates from prevalence. Unregistered or unapproved rules fail closed to `DeduplicationRuleMissingError`.
- **Ground-Truth Preservation in Sparse Regimes**: Computed actual `v_m`, `v_r`, `p_m`, `p_r`, and `primary_ratio` prior to sample size evaluation in `classify_player_focus`, returning actual statistics while setting label `UNCLASSIFIED` and confidence `LOW`.
- **Timestamp Normalization**: Implemented `normalize_ntz_timestamp` converting aware datetimes, naive datetimes, and ISO strings to naive UTC ISO strings (`YYYY-MM-DDTHH:MM:SS`) on both candidate and existing payloads.
- **Column Range Expansion**: Widened `focus_ratio` from `NUMBER(6,5)` to `NUMBER(10,5)` in `02_ops.sql`, `05_core_bridges.sql`, and `V009` (including `MODIFY COLUMN` clauses).
- **Idempotent Migration Constraint**: Wrapped `ADD CONSTRAINT uq_event_channel_stratum_snapshot` in an `EXECUTE IMMEDIATE 'BEGIN ... EXCEPTION WHEN OTHER THEN NULL; END;';` block in `V009`.
- **Test Coverage**: Added 6 targeted regression tests in [`tests/test_channel_stratification.py`](file:///c:/Users/abdul/Documents/football-narrative-observatory/tests/test_channel_stratification.py) (74/74 passing offline).

---

## Incident 10: Timestamp Normalization Precision & Snowflake Exception Semantics Hardening

### Context
During exact-SHA inspection of HEAD `d34cd23` (Actions run 36697399828):
1. **Weakened Snapshot Immutability via Timestamp Truncation**: `normalize_ntz_timestamp` in [`scripts/channel_classifier.py`](file:///c:/Users/abdul/Documents/football-narrative-observatory/scripts/channel_classifier.py) stripped fractional seconds via `.split(".")[0]` and string formatting. An exact-code probe drifting reference-period end by 500 milliseconds received `IDEMPOTENT_NOOP` instead of triggering `SnapshotMutationViolationError`. Furthermore, string timezone offsets (e.g. `+03:00`, `-05:00`) were stripped without converting to UTC, causing equivalent offset strings and timezone-aware datetimes to normalize to different values.
2. **Silent Failure Suppression in V009 Migration**: Lines 61–70 of [`infra/snowflake/migrations/V009__channel_stratification_and_overrides.sql`](file:///c:/Users/abdul/Documents/football-narrative-observatory/infra/snowflake/migrations/V009__channel_stratification_and_overrides.sql) used `WHEN OTHER THEN NULL;`. This catch-all suppressed all errors indiscriminately (such as missing tables, permission denials, or malformed DDL), allowing a migration to falsely report success while `uq_event_channel_stratum_snapshot` remained unapplied.

### Root Cause Analysis
- **Subsecond Precision & Offset Handling**: The normalization helper assumed integer seconds was sufficient for `TIMESTAMP_NTZ` comparisons and used crude string splitting on `+` and `-`. Frozen snapshot immutability requires exact temporal boundaries; discarding fractional seconds allowed subsecond temporal mutations to evade validation. Offset strings must be parsed and converted to UTC using standard datetime conversion rather than stripped as substrings.
- **Snowflake Exception Handling Semantics**: Snowflake Scripting allows specific inspection of `SQLSTATE`, `SQLCODE`, and `SQLERRM` within `WHEN OTHER` or `WHEN STATEMENT_ERROR` blocks. Catching all exceptions with a bare `NULL;` violates fail-closed database migration discipline. Unexpected failures must be rethrown via `RAISE;`.

### Resolution
- **Subsecond Precision & Consistent UTC Conversion**: Refactored `normalize_ntz_timestamp` to parse ISO strings, convert timezone-aware representations to UTC before casting to naive, and format using `.isoformat()`, preserving microseconds when non-zero. Confirmed that candidate payloads with 500ms drift raise `SnapshotMutationViolationError` and that equivalent aware datetimes and offset strings produce identical normalized representations.
- **Selective Duplicate Constraint Handling in V009**: Replaced `WHEN OTHER THEN NULL;` in V009 with conditional error discrimination checking `(SQLSTATE = ''42710'' OR SQLCODE = 2002 OR SQLERRM ILIKE ''%already exists%'') THEN NULL; ELSE RAISE; END IF;`, ensuring unhandled errors immediately halt migration execution.
- **Test Coverage**: Added tests in [`tests/test_channel_stratification.py`](file:///c:/Users/abdul/Documents/football-narrative-observatory/tests/test_channel_stratification.py) verifying fractional-second preservation, offset conversions, subsecond snapshot mutation violation, and V009 exception rethrow semantics (75/75 passing offline).



