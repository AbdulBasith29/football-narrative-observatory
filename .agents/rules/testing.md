---
trigger: glob
globs: "tests/**, scripts/verify*.py"
description: Testing requirements, offline execution guarantees, test isolation, and CI parity.
---

# Testing Standards & Verification Discipline

These rules define test isolation, regression testing, and code quality expectations across the repository.

## 1. Offline Execution & Test Isolation
- **100% Offline Guarantee**: All unit and regression tests in `tests/` must run completely offline without active internet connections, Google API credentials, or live Snowflake database access.
- **Mock All External Boundaries**: Mock `googleapiclient.discovery.build`, `snowflake.connector.connect`, and environment variables in unit tests.
- **Isolated Directory Boundaries**: Test files belong exclusively in `tests/test_*.py`. Never name ad-hoc or manual test scripts with `test_*.py` under `scripts/` to prevent pytest collision during automatic discovery.
- **Configuration Parity**: `pyproject.toml` explicitly sets `testpaths = ["tests"]` and `pythonpath = ["scripts"]`.

## 2. Regression Testing Requirements
- Every bug fix or edge-case resolution must include an offline regression test proving:
  1. The failure scenario reproduces without the fix.
  2. The fix produces the correct state, lineage, and return contract.
- Deterministic Assertions: Assert specific state attributes, parameter lengths, and query structures rather than generic truthiness.

## 3. Lint & Quality Gates
- **Zero Fatal Syntax / Name Errors**: The codebase must pass the fatal flake8 check with zero errors:
  ```bash
  flake8 . --count --select=E9,F63,F7,F82 --exclude=.venv --show-source --statistics
  ```
- **Local / CI Equivalence**: Local verification must execute commands identical to `.github/workflows/ci.yml`.
