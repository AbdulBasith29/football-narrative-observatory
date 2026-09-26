---
trigger: glob
globs: "scripts/*discovery*.py, scripts/*ingestion*.py, config/*.yml"
description: Ingestion pipeline requirements: resumability, idempotency, provenance/lineage, raw-before-parse, and partial failure handling.
---

# Ingestion Pipeline & Discovery Standards

These guidelines govern YouTube discovery, video ingestion, comment harvesting, and operational checkpointing.

## 1. State & Resumability
- **Granular Checkpointing**: Discovery progress is tracked per discovery unit `(frame_version_key, channel_key, window_key, discovery_policy_version, query_hash)` in `OPS.DISCOVERY_UNIT_STATE`.
- **Token Preservation on Partial Error**: If one or more pages succeed before encountering an operational error, preserve `next_page_token` with status `PARTIAL_ERROR` to enable seamless resumption.
- **Lineage Preservation**: Never overwrite `started_at` or `first_ingestion_run_id` during resumed executions. Update `latest_ingestion_run_id` and `updated_at`.
- **Completeness Audit**: A pipeline run cannot declare `COMPLETE` unless all expected discovery units for the frame are in `COMPLETED` status.

## 2. Metric Correctness
- **Cumulative Unique Videos**: `unique_video_ids_observed` must be strictly monotonic across resumed executions. Compute it via set union of previously persisted records scoped to the exact discovery unit and newly observed video IDs (`len(existing_video_ids | current_attempt_unique_ids)`).
- **Exact Provenance Scoping**: Queries retrieving historical video membership must filter on the full discovery unit coordinates (`frame_version_key`, `channel_key`, `window_key`, `discovery_policy_version`, `query_hash`). Videos observed under one query hash must never increase the metric of a different query hash.

## 3. Error Classification
- **HTTP 403 Discrimination**: Do NOT classify all HTTP 403 responses as quota exhaustion.
  - Quota/rate-limit reasons (`quotaExceeded`, `rateLimitExceeded`, `userRateLimitExceeded`, `dailyLimitExceeded`) map to `PARTIAL_QUOTA_LIMIT`.
  - Ordinary authentication, forbidden, or configuration failures map to `PARTIAL_ERROR` with error code `HTTP_403`.

## 4. Raw Preservation & Idempotency
- **Raw-Before-Parse**: Persist verbatim JSON API responses in `RAW` tables before normalizing into `CORE` entities where required by ingestion design.
- **Idempotent Ingestion**: Re-executing a discovery unit or ingestion run must produce identical dimensional entities without duplicating records in `CORE.DIM_VIDEO` or `CORE.BRIDGE_VIDEO_EVENT`.
