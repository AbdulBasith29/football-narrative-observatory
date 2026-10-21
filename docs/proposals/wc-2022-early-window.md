# Proposal: Early-Window Collection for the Existing World Cup Final Videos

**Snapshot:** `WC_2022_FINAL_EARLY_V1` · **Status:** PROPOSED, not run · **Frame:** PIPELINE_PILOT, local only

## Why

The first pilot (`pilot-comments-v1`, manifest `e11cc18c…6503b`) collected the newest 100 top-level
comments per video. 17 of the 23 videos hit that cap, so their early comments were not retrieved. The
other 6 ran out of pages, so their histories are already complete. A reviewed fixed sample showed
contemporary discussion mixed with comments posted years later. 938 of 1,976 comments (47.5%) were
published on 18–20 December 2022 UTC, but that calendar window also contains comments posted **before**
the final. It cannot be labelled post-final reaction.

This is the smallest next collection that separates event-time discussion from later comments: the same
23 videos, with no new discovery and no search calls, collecting each video's early window.

## Scope

| Item | Proposal |
|---|---|
| Videos | The same 23 ELIGIBLE PLAYER_MATCH videos, selected mechanically from the saved verified report. |
| Existing data | Untouched. The new manifest is `wc-2022-early-v1` with a new output root; the stores are not merged. |
| Event label | `--event-id WC_2022_FINAL`, recorded as `event_id_source: MANIFEST_ARGUMENT` because the legacy report has none. |
| Window | `[video published_at, published_at + 72h)` for each video. |
| Discovery | None. 0 `search.list` calls. |
| Per-video cap | `K` requests, chosen from the offline estimate below. **Hard ceiling K ≤ 50.** |
| Maximum requests | 23 × K. **At most 1,150 `commentThreads.list` requests = 1,150 default-bucket units** (11.5% of the default 10,000/day), at 1 unit per request per the official quota calculator (last updated 2026-10-15). |
| Expected use | Lower than the maximum: a video stops as soon as its pages run out or its window is passed. The 6 already-complete videos should need about 1 request each. |

### Choosing K (offline, no API calls)

```powershell
python scripts/harvest_pilot_comments.py estimate `
  --report reports/live_pilot_candidate_coverage_report_verified.json `
  --max-requests-per-video 10 20 50
```

The estimate uses each video's `commentCount` saved at discovery. That count includes replies, so it
gives an upper bound on pages. Collection is newest-first, so reaching a video's first 72 hours means
paging through every newer comment.

Decision rule: take the smallest K in {10, 20, 50} with zero expected truncations. If none qualifies,
use K = 50 and accept that the listed videos will be reported as truncated. They could be resumed later
from their checkpoints, but only after a separate approval. The chosen K, the estimate output and the
manifest hash go in the review record before any live run.

### Commands (after approval)

```powershell
python scripts/harvest_pilot_comments.py manifest --mode early-window --event-id WC_2022_FINAL `
  --report reports/live_pilot_candidate_coverage_report_verified.json `
  --manifest-version wc-2022-early-v1 --window-hours 72 --max-requests-per-video <K> `
  --output reports/event_snapshots/WC_2022_FINAL_EARLY_V1/manifest.json

python scripts/harvest_pilot_comments.py harvest `
  --manifest reports/event_snapshots/WC_2022_FINAL_EARLY_V1/manifest.json `
  --output-root reports/event_snapshots/WC_2022_FINAL_EARLY_V1/smoke --scope-accepted-by "<name>" --video-limit 2

python scripts/harvest_pilot_comments.py harvest `
  --manifest reports/event_snapshots/WC_2022_FINAL_EARLY_V1/manifest.json `
  --output-root reports/event_snapshots/WC_2022_FINAL_EARLY_V1/collection --scope-accepted-by "<name>"
```

Then run `verify` and `explore_comment_pilot.py` on that output root.

## How incomplete coverage is reported

Each video gets exactly one coverage status. It appears in the run record, the store, the checkpoint
and the explorer:

- `COMPLETE_ALL_PAGES` / `COMPLETE_WINDOW_PASSED`: the window count is complete. These are the **only**
  counts that may be summed as "all surviving top-level comments in the window".
- `BUDGET_TRUNCATED_BEFORE_WINDOW`: the cap ran out before the window was reached. Reported as **not
  reached**, never as zero comments.
- `BUDGET_TRUNCATED_WITHIN_WINDOW`: partial. Reported as a lower bound covering `window_covered_from`
  to the window end; the earliest part of the window is missing.
- `INCOMPLETE_ERROR`, `UNAVAILABLE`, `NOT_ATTEMPTED`: reported as such, with no count.

A run with any truncation ends `COMPLETED_BUDGET_TRUNCATED`. Totals are reported in two lines: the
complete-coverage window total (n videos) and the partial lower bound (m videos). The two are never
combined into a single "volume" figure.

## Event-time vs later: labelling rule

"Within 72h of upload" is not the same as "post-final". Comment timestamps should be labelled against
kickoff (`occurred_at` 2022-12-18T15:00Z) using the phases already defined in
`config/event_windows.yml`:

| Phase | From (UTC) | To (UTC) |
|---|---|---|
| PRE_EVENT | 2022-12-04 15:00 | 2022-12-18 14:00 |
| EVENT | 2022-12-18 14:00 | 2022-12-18 21:00 |
| EARLY_POST_EVENT | 2022-12-18 21:00 | 2022-12-19 15:00 |
| LATE_POST_EVENT | 2022-12-19 15:00 | 2022-12-21 15:00 |
| RECOVERY_WINDOW | 2022-12-21 15:00 | 2023-01-01 15:00 |
| (later) | 2023-01-01 15:00 | — |

The 18–20 December calendar window used earlier spans PRE_EVENT (00:00–14:00 on the 18th), EVENT,
EARLY_POST_EVENT and part of LATE_POST_EVENT, which is why it cannot be called post-final reaction.

**Implemented in the explorer** (`--event-id WC_2022_FINAL` for the v1 manifest): phase counts with exact
UTC boundaries, an outside-window row, complete and partial collection shown separately, and the phase
of each audit-sample comment. Descriptions say before / after / spans kickoff, never "during the match"
or "post-match".

## Limits

- Only comments that still exist at harvest time are retrieved. Deleted comments and accounts are missing,
  and that loss grows with age.
- Likely-pinned first items are flagged. The 1-second `updatedAt`/`publishedAt` skew is reported as a warning.
- PIPELINE_PILOT channels only (ESPN FC 20 of 23 videos). The results describe this sample, not football fans.
- No sentiment, narrative classification or Snowflake writes.
