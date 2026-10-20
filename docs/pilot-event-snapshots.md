# Pilot Event Snapshots (PIPELINE_PILOT, local only)

Three events from `config/event_windows.yml`, each collected as a **separate pilot snapshot** with the
PIPELINE_PILOT channel frame. Snapshots are not merged and are not a research dataset. Parameters live in
`config/discovery/pilot_event_snapshots.yml` (status: PROPOSED).

| Snapshot | Event | Discovery window | Comment collection |
|---|---|---|---|
| `WC_2022_FINAL_PILOT_V1` | `WC_2022_FINAL` | legacy 2022-12-18..2022-12-20 | collected: newest 100 per video |
| `AL_NASSR_2023_PILOT_V1` | `TRANSFER_AL_NASSR_2023` | 2023-01-03 15:00 → 2023-01-06 16:00 UTC | early window, pending review |
| `BALLON_DOR_2023_PILOT_V1` | `BALLON_DOR_2023` | 2023-10-30 18:45 → 2023-11-02 19:45 UTC | early window, pending review |

The new discovery windows follow a proposed rule: the event's `EVENT` window start (−1h) to its
`LATE_POST_EVENT` window end (+72h). This differs from the calendar-day window used for the World Cup
final, so the snapshots are not perfectly symmetric.

## 0. Explore the existing World Cup final snapshot (offline)

```powershell
python scripts/explore_comment_pilot.py `
  --manifest reports/comment_pilot/manifest_pilot-comments-v1.json `
  --output-root reports/comment_pilot/pilot-comments-v1
start reports/comment_pilot/pilot-comments-v1/explorer.html
```

The page shows verification status (including the 13 one-second timestamp warnings), comment counts,
publication months, time from upload to comment, channels, per-video coverage and a fixed audit sample
of up to 10 comments per video. Every count is labelled as retrieved comments with its coverage; none is
total discussion volume.

## 1. Discovery plan (no API calls)

```powershell
python scripts/discover_pilot_coverage.py --snapshot AL_NASSR_2023_PILOT_V1 --estimate-only
python scripts/discover_pilot_coverage.py --snapshot BALLON_DOR_2023_PILOT_V1 --estimate-only
```

Each prints the resolved event, window, terms and an upper bound: 1 `channels.list`, 14 `search.list`
(100 units each) and 14 `videos.list` = **at most 1,415 quota units per event**. Search is used
because the uploads-playlist walk would need thousands of pages to reach 2023.

## 2. Discovery (live, after the plan is approved)

```powershell
python scripts/discover_pilot_coverage.py --snapshot AL_NASSR_2023_PILOT_V1
python scripts/discover_pilot_coverage.py --snapshot BALLON_DOR_2023_PILOT_V1
```

Reports are written to `reports/event_snapshots/<snapshot>/discovery_report.json` and never overwritten
(an existing file stops the run before any API call). Report metadata records `event_id`,
`event_occurred_at` and `window_derivation`. Audit the candidates as before, especially relevance,
player category and channel balance.

## 3. Collection cost estimate (offline, from discovered candidates)

```powershell
python scripts/harvest_pilot_comments.py estimate `
  --report reports/event_snapshots/AL_NASSR_2023_PILOT_V1/discovery_report.json `
  --max-requests-per-video 5 10 20 50
```

`commentThreads.list` returns comments newest first, so reaching a video's earliest comments means
paging through every newer one. The estimate uses each video's `commentCount` at discovery time.
`commentCount` includes replies, so ceil(count/100) is an upper bound on pages. For each candidate cap
it shows:
- the planned maximum requests;
- the full-retrieval upper bound;
- how many videos would be budget-truncated;
- how many videos have no count (assumed to use the full cap).

Choose `--window-hours` and `--max-requests-per-video` from this output and record them in the
snapshot config.

## 4. Early-window manifest and collection

```powershell
python scripts/harvest_pilot_comments.py manifest --mode early-window `
  --report reports/event_snapshots/AL_NASSR_2023_PILOT_V1/discovery_report.json `
  --manifest-version al-nassr-2023-early-v1 --window-hours 72 --max-requests-per-video <N> `
  --output reports/event_snapshots/AL_NASSR_2023_PILOT_V1/manifest.json

python scripts/harvest_pilot_comments.py harvest `
  --manifest reports/event_snapshots/AL_NASSR_2023_PILOT_V1/manifest.json `
  --output-root reports/event_snapshots/AL_NASSR_2023_PILOT_V1/collection `
  --scope-accepted-by "<name>" --video-limit 2      # smoke test first, in its own output root

python scripts/harvest_pilot_comments.py verify `
  --manifest reports/event_snapshots/AL_NASSR_2023_PILOT_V1/manifest.json `
  --output-root reports/event_snapshots/AL_NASSR_2023_PILOT_V1/collection
python scripts/explore_comment_pilot.py `
  --manifest reports/event_snapshots/AL_NASSR_2023_PILOT_V1/manifest.json `
  --output-root reports/event_snapshots/AL_NASSR_2023_PILOT_V1/collection
```

The window for each video is `[video published_at, published_at + window_hours)`. Every retrieved
comment is kept with an `in_early_window` flag. Each video ends with one coverage status:

| Coverage | Meaning | Window count |
|---|---|---|
| `COMPLETE_ALL_PAGES` | every page retrieved | complete |
| `COMPLETE_WINDOW_PASSED` | paging reached comments older than the window start | complete |
| `BUDGET_TRUNCATED_BEFORE_WINDOW` | request cap hit before reaching the window | **0 collected, partial** |
| `BUDGET_TRUNCATED_WITHIN_WINDOW` | cap hit inside the window; covered from `window_covered_from` | **partial** |
| `INCOMPLETE_ERROR` | an error stopped paging | partial |
| `UNAVAILABLE` | comments disabled or video not found | none |

Only `COMPLETE_*` window counts may be read as the full set of surviving top-level comments in that
window. Truncated counts are partial and are never total discussion volume. A run with any truncated
video ends `COMPLETED_BUDGET_TRUNCATED` (exit code 0, with a note).

A first item older than the one after it on page 1 is flagged `likely_pinned` and does not move the
paging frontier.

### Checkpoints and resuming

After every successful page, `runs/<run_id>/checkpoints/<video_id>.json` records the next page token,
the frontier (oldest comment seen) and the coverage status. To continue truncated videos with a fresh
budget, for example on another quota day:

```powershell
python scripts/harvest_pilot_comments.py harvest --manifest <manifest> --output-root <root> `
  --scope-accepted-by "<name>" --resume-from-run <previous run_id>
```

Videos already complete in that run are skipped with no requests. Truncated videos continue from the
saved token and carry the earlier frontier. Everything else starts fresh. YouTube page tokens can
expire; an expired token shows up as an `INCOMPLETE_ERROR` for that video.

## 5. Audit checklist (per snapshot)

- `verify` passes. Warnings, such as 1-second timestamp skew, are listed rather than hidden.
- Coverage table: how many videos are complete and how many are truncated, and why.
- Read the audit sample for each video: relevance, language, spam and abuse.
- Compare the player mix and channel mix with the discovery report.
- Record quota actually used (from `run.json`) against the estimate.
