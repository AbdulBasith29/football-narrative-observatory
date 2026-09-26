---
name: verify-observatory-feature
description: Comprehensive verification protocol for validating Observatory features, ensuring architectural compliance, methodology protection, database grain correctness, offline test coverage, and CI parity before external review.
---

# Observatory Feature Verification Protocol

Use this skill when a feature branch, pipeline stage, or bugfix is believed to be complete and is being prepared for external ChatGPT or human review.

This protocol enforces deterministic validation, methodology protection, and strict git discipline.

---

## Verification Steps

### 1. Diff & Branch Inspection
- Inspect the current branch name (`git branch --show-current`). Ensure work is on `feature/<name>`, `chore/<name>`, or `fix/<name>`, never `main`.
- Resolve the base commit SHA (`git merge-base main HEAD` or target PR base) and the current HEAD SHA (`git rev-parse HEAD`).
- Inspect the file diff against base (`git diff --stat <base_sha>..HEAD`). Identify all modified, added, or deleted files.

### 2. Required Invariants Verification
Verify that the implementation strictly adheres to the core observatory rules:
- **Architecture Check**: Do table changes and data operations respect schema boundaries (`RAW` $\rightarrow$ `OPS` $\rightarrow$ `CORE` $\rightarrow$ `ML` $\rightarrow$ `MARTS`)?
- **Methodology Check**: Has frozen methodology been preserved? No unauthorized changes to query budgets, window calculations, or event taxonomies.
- **Data-Grain Check**: Does every modified or queried table operate at its documented grain? (e.g., `DISCOVERY_UNIT_STATE` at `frame × channel × window × policy × query_hash`).
- **Isolation Check**: Are `RESEARCH` runs strictly separated from `PIPELINE_PILOT` frames and `INTEGRATION_TEST` databases?
- **Lineage & Provenance Check**: Are `started_at`, `first_ingestion_run_id`, and full query unit coordinates preserved across multi-attempt executions?
- **Resumability & Idempotency Check**: If an ingestion component was touched, does it safely resume from `next_page_token` on partial failure without duplicating records?
- **Migration & DDL Check**: If schema changes were introduced, are they scripted as versioned, idempotent files in `infra/snowflake/migrations/` and reflected in `infra/snowflake/ddl/`?

### 3. Deterministic Local Verification
Execute the local deterministic verification suite:
```bash
python scripts/verify_feature.py
```
Or run the equivalent discrete checks:
- **Offline Pytest Suite**:
  ```bash
  pytest tests/
  ```
  All unit and regression tests must pass completely offline with zero live network calls.
- **Fatal Flake8 Check**:
  ```bash
  flake8 . --count --select=E9,F63,F7,F82 --exclude=.venv --show-source --statistics
  ```
  Must exit with 0 errors.

### 4. Documentation & Debug Log
- Check whether any non-obvious bug, incident, edge-case failure, or recovery mechanism was resolved during the feature. If so, verify that [`docs/engineering-debug-log.md`](file:///c:/Users/abdul/Documents/football-narrative-observatory/docs/engineering-debug-log.md) has an incident entry documenting context, root cause, and resolution.
- Verify that documentation and docstrings maintain integrity and do not make unsupported claims.

### 5. Git Status & CI Audit
- Inspect working tree state (`git status`). Confirm whether uncommitted changes exist.
- If changes have already been pushed, query the GitHub Actions API or run status for the **exact HEAD SHA**.
- **Rule**: NEVER claim GitHub CI succeeded unless a workflow run belonging to the exact HEAD SHA has completed with conclusion `success`.

### 6. Safety & Boundary Invariants
- **NEVER merge** the branch autonomously. Leave the Pull Request for human review.
- **NEVER begin the next pipeline phase** (e.g., video metadata ingestion, comment harvesting) merely because the current phase appears complete.
- Only commit and push when the user explicitly instructs you to do so.

---

## Final Verification Report Format

Upon completing the verification protocol, present the findings in this exact compact format:

```text
============================================================
OBSERVATORY FEATURE VERIFICATION REPORT
============================================================
FEATURE:                     <Branch name or Feature description>
BASE SHA:                    <Base commit SHA>
HEAD SHA:                    <HEAD commit SHA>
FILES CHANGED:               <Count and list of files>
TEST RESULT:                 <e.g. 26/26 passed in 1.2s>
LINT RESULT:                 <e.g. Passed (0 errors)>
CI RESULT:                   <e.g. success (Run ID: ...) OR Pending / Not pushed>
ARCHITECTURE CHECK:          <PASS / FAIL - brief notes>
METHODOLOGY CHECK:           <PASS / FAIL - brief notes>
DATA-GRAIN CHECK:            <PASS / FAIL - brief notes>
ISOLATION CHECK:             <PASS / FAIL - brief notes>
LINEAGE/PROVENANCE CHECK:    <PASS / FAIL - brief notes>
RESUMABILITY/IDEMPOTENCY:    <PASS / N/A - brief notes>
KNOWN LIMITATIONS:           <Honest list of known edge cases or constraints>
UNRESOLVED RISKS:            <Honest list of open risks, if any>
READY FOR EXTERNAL REVIEW:   <YES / NO>
============================================================
```
