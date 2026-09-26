---
trigger: always_on
description: Git branch discipline, commit guidelines, and human review boundaries.
---

# Git & Branch Discipline

All development must strictly adhere to the project Git workflow.

## 1. Branch Lifecycle
- **Clean Base**: Always ensure local `main` is up to date (`git pull origin main`) before creating a new branch.
- **Branch Naming**: All work must occur on dedicated branches:
  - Features: `feature/<feature-name>`
  - Maintenance / Infrastructure: `chore/<chore-name>`
  - Bug fixes: `fix/<bug-name>`
- **No Direct Commits**: NEVER commit directly to `main`.

## 2. Review & Merge Boundaries
- **No Autonomous Merges**: NEVER merge a branch into `main` autonomously. All merges require explicit human review and approval.
- **Commit Explicitly**: Only commit and push when the user explicitly instructs you to do so. Stop and report proposed changes before committing when requested.
- **Completion Standard**: A feature or task is NOT complete merely because unit tests pass. It requires verification of architecture, data grain, lineage, error handling, and CI status.
- **Pull Requests**: Open a Pull Request targeting `main` only after deterministic offline tests and lint pass. Leave PR open for human review.
