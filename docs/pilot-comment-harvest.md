# Pilot Comment Harvest (PIPELINE_PILOT, local only)

`scripts/harvest_pilot_comments.py` collects a bounded set of **top-level** comments for the
pilot videos selected from a verified discovery report. It never writes to Snowflake; all output
lives under `reports/` (gitignored). Comments are those retrievable **now** on historical videos,
not reconstructed 2022 reactions. Sentiment/NLP is out of scope.

## 1. Build the manifest (no API calls)

```bash
python scripts/harvest_pilot_comments.py manifest \
  --report reports/live_pilot_candidate_coverage_report_verified.json \
  --manifest-version pilot-comments-v1 \
  --max-comments-per-video 100 \
  --output reports/comment_pilot/manifest_pilot-comments-v1.json
```

Selection is mechanical: `status == ELIGIBLE` and `relevance_tier == PLAYER_MATCH`, deduplicated by
exact `video_id`. Expected from the verified report: **23 videos, at most 23 requests / 23 quota
units** (`commentThreads.list` at 1 unit; verify against current official documentation and your
project allocation). The manifest stores the source report SHA-256 and its own content hash; an
edited manifest is rejected. Review `videos` and `scope` before harvesting.

## 2. Harvest (live YouTube API — only after the scope is accepted)

Requires `YOUTUBE_API_KEY` and `HMAC_SECRET` in `.env`.

```bash
# optional smoke test: first 2 manifest videos into a throwaway root
python scripts/harvest_pilot_comments.py harvest \
  --manifest reports/comment_pilot/manifest_pilot-comments-v1.json \
  --output-root reports/comment_pilot/smoke \
  --scope-accepted-by "<your name>" --video-limit 2

python scripts/harvest_pilot_comments.py harvest \
  --manifest reports/comment_pilot/manifest_pilot-comments-v1.json \
  --output-root reports/comment_pilot/pilot-comments-v1 \
  --scope-accepted-by "<your name>"
```

Per run, under `<output-root>/runs/<run_id>/`:

- `raw/<video_id>/page_NNN.json` — verbatim response body (success **or** error), written before parsing.
- `raw/<video_id>/page_NNN.meta.json` — request parameters, requested/retrieved timestamps, HTTP status,
  payload SHA-256, item count, next page token, error code/reasons.
- `parsed_comments.jsonl` — one line per observed comment with lineage (`run_id`, `raw_file`,
  `payload_sha256`, `page_number`, `source_item_position`, `observed_at`). Author channel IDs are
  HMAC-hashed (`salt_version_id` `v1.0`, same construction as `ingestion_snowflake.py`); display names
  and raw IDs are not carried.
- `run.json` — scope acceptance, quota estimate, per-video outcomes and the store merge summary.

Per-video outcomes are distinct: `COLLECTED`, `EMPTY`, `COMMENTS_DISABLED`, `VIDEO_NOT_FOUND`,
`QUOTA_LIMIT` / `PARTIAL_QUOTA_LIMIT` (stops the run; remaining videos `NOT_ATTEMPTED`), `ERROR` /
`PARTIAL_ERROR` (e.g. `HTTP_403` for non-quota forbidden). `pagination_availability` is `EXHAUSTED`
or `MORE_AVAILABLE_CAP_REACHED`. Exit code is 0 only for `COMPLETED`.

`<output-root>/comment_store.json` accumulates runs at the warehouse grains: one identity per
`comment_id`, text versions per `(comment_id, text_hash)`, engagement snapshots and sample
observations per observation. Reruns add new comments and new text versions without duplicating IDs.
One output root per manifest; mixing manifests is refused.

## 3. Verify (offline)

```bash
python scripts/harvest_pilot_comments.py verify \
  --manifest reports/comment_pilot/manifest_pilot-comments-v1.json \
  --output-root reports/comment_pilot/pilot-comments-v1
```

Checks: parseable timestamps; `updated_at >= published_at`; no comment published before its video;
`video_id` matches the manifest video; exactly one latest text version; zero duplicate comment IDs
within each run and no duplicate sample observations; every lineage reference resolves to an existing
raw file whose SHA-256 matches and whose item at `source_item_position` has that comment ID.
