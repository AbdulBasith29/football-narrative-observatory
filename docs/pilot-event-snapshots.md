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
  --output-root reports/comment_pilot/pilot-comments-v1 --event-id WC_2022_FINAL
start reports/comment_pilot/pilot-comments-v1/explorer.html
```

The page shows verification status (including the 13 one-second timestamp warnings), comment counts,
publication months, time from upload to comment, channels, per-video coverage and a fixed audit sample
of up to 10 comments per video. Every count is labelled as retrieved comments with its coverage; none is
total discussion volume.

`--event-id` is needed only for the v1 manifest, which does not record its event; v2 manifests carry
it, and a conflicting `--event-id` is rejected. With an event, the explorer adds **event-relative
phases** from `config/event_windows.yml`:
- each phase's exact UTC boundaries (From inclusive, To exclusive);
- an `OUTSIDE_CONFIGURED_WINDOWS` row, split into before / after;
- counts shown separately for videos with complete and partial collection;
- the phase of each comment in the audit sample.

Phase descriptions come from the configured offsets only. A phase is described as "before", "after" or
"spans" kickoff (WC_2022_FINAL) or event start (other events). Nothing is labelled "during the match"
or "post-match", because the config has no match-end time; the EVENT window, for example, runs from
1h before to 6h after kickoff. The video-relative view (time since each video's upload, including the
first 72 hours used for early-window collection) is a separate section.

## 1. Discovery plan (no API calls)

```powershell
python scripts/discover_pilot_coverage.py --snapshot AL_NASSR_2023_PILOT_V1 --estimate-only
python scripts/discover_pilot_coverage.py --snapshot BALLON_DOR_2023_PILOT_V1 --estimate-only
```

Each prints the resolved event, window, terms and an upper bound per quota bucket. Per the official
quota calculator (last updated 2026-10-15), `search.list` has its own bucket (default 100 calls/day, 1
per call), and everything else shares the default 10,000 units/day. Per event: **at most 14
`search.list` calls** plus **at most 15 default-bucket units** (1 `channels.list` + 14 `videos.list`).
Confirm the project's actual allocation in the Cloud console Quotas page. Search is used because the
uploads-playlist walk would need thousands of pages to reach 2023.

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

### Repeated comments and pre-publication comments

**Repeated comment IDs within a run.** The API can return the same comment on two consecutive pages.
- Every raw occurrence is kept: the raw page, the parsed line and a lineage record in the store.
- Each repeat is classified from the raw item: `DUPLICATE_IDENTICAL` if byte-for-byte equal (canonical
  JSON) to the first occurrence, otherwise `DUPLICATE_CHANGED`.
- An identical repeat adds no engagement snapshot; a changed one does.
- All analytical counts (per video, per phase, per month) are unique by comment ID. Run records give
  `raw_occurrences`, `unique_comments_observed` and `occurrence_counts`.
- `verify` re-derives occurrence kinds from the raw pages, so runs made before kinds were recorded are
  covered too. It lists repeats as `[WARN] duplicate_occurrences_*` with both lineages, and fails only
  if an occurrence's lineage is missing or inconsistent.

**Comments published before the video's `publishedAt`.**
- These stay flagged (`[WARN] comment_before_video_published`, with the video and the gap in seconds),
  with timestamps exactly as returned.
- They lie outside the configured window, which stays anchored on `publishedAt`. They may be
  livestream- or premiere-era comments; that is not verified. Changing the anchor needs evidence (for
  example a livestream's actual start time) and explicit approval.
- Each video reports `pre_publication_coverage` separately from `coverage_status`:
  - `COMPLETE`: all pages retrieved, pre-publication comments present.
  - `NONE_RETRIEVABLE`: all pages retrieved, none present.
  - `PARTIAL_STOPPED_AT_WINDOW_START`: the configured window is complete, but paging stopped once it
    passed the window start, so older pre-publication comments were not retrieved.
  - `PARTIAL`, `NOT_REACHED`, `UNAVAILABLE`.

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

## Proposal: early-window collection for the existing World Cup videos

See `docs/proposals/wc-2022-early-window.md` (status: PROPOSED, not run).
