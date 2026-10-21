#!/usr/bin/env python3
"""scripts/explore_comment_pilot.py
Offline local explorer for a pilot comment harvest output root.

Renders one self-contained HTML file (no network, no JavaScript) from a manifest and its output
root: verification status and timestamp warnings, comment counts, publication dates, channels,
videos, collection coverage, and a deterministic per-video sample of comments for manual audit.

Governing Invariants:
1. Read-only: reads the manifest, comment_store.json and run records; never calls an API.
2. Coverage honesty: every count is labelled as retrieved comments with its coverage. Capped or
   budget-truncated videos are shown as partial; nothing is presented as total discussion volume.
3. Privacy: shows comment text (local file under gitignored reports/) but never author identifiers.
4. No narrative classification or sentiment.
5. Two separate time frames: video-relative (time since each video's upload) and event-relative phases
   from config/event_windows.yml. Phase descriptions come from the configured offsets only (before /
   after / spanning the anchor time); periods are never called "during the match" or "post-match",
   because the configuration has no match-end time.
"""

from __future__ import annotations

import argparse
import collections
import datetime
import hashlib
import html
import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

import yaml

try:
    import harvest_pilot_comments as hpc
except ImportError:
    from scripts import harvest_pilot_comments as hpc

SAMPLE_PER_VIDEO = 10
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_EVENT_CONFIG = os.path.join(REPO_ROOT, "config", "event_windows.yml")
# Name for an event's occurred_at in phase descriptions. Only events whose occurred_at is a kickoff get
# "kickoff" (WC_2022_FINAL: 2022-12-18T15:00Z); everything else is described relative to occurred_at.
EVENT_ANCHOR_TERMS = {"WC_2022_FINAL": "kickoff"}
DEFAULT_ANCHOR_TERM = "event start (occurred_at)"
OUTSIDE_PHASE = "OUTSIDE_CONFIGURED_WINDOWS"
UNPARSEABLE_PHASE = "UNPARSEABLE_TIMESTAMP"
AGE_BUCKETS = [  # (label, upper bound in hours since the video was published; None = no bound)
    ("< 1 day", 24), ("1–3 days", 72), ("3–7 days", 168), ("1–4 weeks", 24 * 30),
    ("1–12 months", 24 * 365), ("> 1 year", None),
]


def _esc(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def _date(value: Optional[str]) -> str:
    return (value or "")[:10]


def load_run_records(output_root: str, store: Dict[str, Any]) -> List[Dict[str, Any]]:
    records = []
    for run in store.get("runs", []):
        path = os.path.join(output_root, "runs", run["run_id"], "run.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                records.append(json.load(f))
    return records


def latest_video_outcomes(run_records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Latest attempted outcome per video across runs (skips leave the earlier outcome in place)."""
    latest: Dict[str, Dict[str, Any]] = {}
    for rec in run_records:
        for vo in rec.get("video_outcomes", []):
            if vo.get("outcome") in (hpc.OUTCOME_NOT_ATTEMPTED, hpc.OUTCOME_SKIPPED_COMPLETE) and vo["video_id"] in latest:
                continue
            latest[vo["video_id"]] = {**vo, "run_id": rec["run_id"]}
    return latest


def coverage_label(mode: str, outcome: Optional[Dict[str, Any]], store_cov: Optional[Dict[str, Any]]) -> Tuple[str, bool]:
    """Returns (human label, is_complete) for one video's retrieved set."""
    if outcome is None:
        return "Not collected", False
    if mode == hpc.SCOPE_EARLY_WINDOW:
        status = (store_cov or {}).get("coverage_status") or outcome.get("coverage_status")
        labels = {
            hpc.COVERAGE_COMPLETE_ALL_PAGES: "Complete (all pages retrieved)",
            hpc.COVERAGE_COMPLETE_WINDOW_PASSED: "Complete (window passed)",
            hpc.COVERAGE_TRUNCATED_BEFORE_WINDOW: "Budget-truncated before window (partial)",
            hpc.COVERAGE_TRUNCATED_WITHIN_WINDOW: "Budget-truncated within window (partial)",
            hpc.COVERAGE_INCOMPLETE_ERROR: "Incomplete (error)",
            hpc.COVERAGE_UNAVAILABLE: "Unavailable (disabled / not found)",
            hpc.COVERAGE_NOT_ATTEMPTED: "Not attempted",
        }
        return labels.get(status, str(status)), status in hpc.COMPLETE_COVERAGE
    if outcome.get("outcome") in (hpc.OUTCOME_COMMENTS_DISABLED, hpc.OUTCOME_VIDEO_NOT_FOUND):
        return "Unavailable (disabled / not found)", False
    if outcome.get("outcome") not in (hpc.OUTCOME_COLLECTED, hpc.OUTCOME_EMPTY):
        return f"Incomplete ({outcome.get('outcome')})", False
    pag = outcome.get("pagination_availability")
    if pag == hpc.PAGINATION_EXHAUSTED:
        return "All retrieved (pages exhausted)", True
    if pag in (hpc.PAGINATION_MORE_AVAILABLE, hpc.PAGINATION_REQUEST_CAP):
        return "Newest only (cap reached, older comments not retrieved)", False
    return "Unknown pagination", False


def hours_between(start: Optional[str], end: Optional[str]) -> Optional[float]:
    a, b = hpc.parse_api_timestamp(start), hpc.parse_api_timestamp(end)
    if a is None or b is None:
        return None
    return (b - a).total_seconds() / 3600.0


def age_bucket(hours: Optional[float]) -> Optional[str]:
    if hours is None:
        return None
    for label, bound in AGE_BUCKETS:
        if bound is None or hours < bound:
            return label
    return None


def latest_text(comment: Dict[str, Any]) -> str:
    for v in comment.get("text_versions", []):
        if v.get("is_latest_version"):
            return v.get("text", "")
    return ""


def deterministic_sample(comments: List[Dict[str, Any]], k: int) -> List[Dict[str, Any]]:
    """Stable pseudo-random sample (by hashed comment ID) so audits are reproducible."""
    return sorted(comments, key=lambda c: hashlib.sha256(c["comment_id"].encode("utf-8")).hexdigest())[:k]


def _format_offset(hours: float) -> str:
    text = f"{hours:+g}h"
    if abs(hours) >= 48 and hours % 24 == 0:
        text += f" ({hours / 24:+g}d)"
    return text


def load_event_phases(event_id: str, config_path: str = DEFAULT_EVENT_CONFIG) -> Dict[str, Any]:
    """Builds event-relative phases with exact UTC boundaries from config/event_windows.yml.

    Each phase is [start, end): start inclusive, end exclusive. The description is derived from the
    offsets alone: entirely before, entirely after, or spanning the anchor time.
    """
    with open(config_path, "r", encoding="utf-8") as f:
        events = (yaml.safe_load(f) or {}).get("events", [])
    event = next((e for e in events if e.get("external_event_id") == event_id), None)
    if event is None:
        raise ValueError(f"Event {event_id!r} not found in {config_path}")
    occurred = hpc.parse_api_timestamp(event.get("occurred_at"))
    if occurred is None:
        raise ValueError(f"Event {event_id!r} has no parseable occurred_at")
    anchor = EVENT_ANCHOR_TERMS.get(event_id, DEFAULT_ANCHOR_TERM)
    phases = []
    for w in event.get("windows", []):
        start_h, end_h = w["relative_offset_start_hours"], w["relative_offset_end_hours"]
        if end_h <= start_h:
            raise ValueError(f"Window {w['window_type']} of {event_id} has end <= start")
        relation = (f"before {anchor}" if end_h <= 0 else f"after {anchor}" if start_h >= 0 else f"spans {anchor}")
        phases.append({
            "window_type": w["window_type"],
            "start_offset_hours": start_h,
            "end_offset_hours": end_h,
            "start": occurred + datetime.timedelta(hours=start_h),
            "end": occurred + datetime.timedelta(hours=end_h),
            "relation": relation,
            "description": f"{_format_offset(start_h)} to {_format_offset(end_h)} relative to {anchor}; {relation}",
        })
    ordered = sorted(phases, key=lambda p: p["start"])
    overlaps = [(a["window_type"], b["window_type"]) for a, b in zip(ordered, ordered[1:]) if b["start"] < a["end"]]
    gaps = [(a["window_type"], b["window_type"]) for a, b in zip(ordered, ordered[1:]) if b["start"] > a["end"]]
    return {
        "event_id": event_id,
        "event_name": event.get("event_name"),
        "occurred_at": occurred,
        "anchor": anchor,
        "phases": phases,
        "first_start": ordered[0]["start"] if ordered else None,
        "last_end": max(p["end"] for p in phases) if phases else None,
        "overlapping_windows": overlaps,
        "gaps_between_windows": gaps,
    }


def assign_phase(published_at: Optional[str], event_phases: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    """Returns (phase, side). phase is a window_type, OUTSIDE_PHASE or UNPARSEABLE_PHASE; side is
    'before' / 'after' / 'between' for OUTSIDE_PHASE. The first configured window that contains the
    timestamp wins if windows ever overlap."""
    ts = hpc.parse_api_timestamp(published_at)
    if ts is None:
        return UNPARSEABLE_PHASE, None
    for phase in event_phases["phases"]:
        if phase["start"] <= ts < phase["end"]:
            return phase["window_type"], None
    if event_phases["first_start"] is not None and ts < event_phases["first_start"]:
        return OUTSIDE_PHASE, "before"
    if event_phases["last_end"] is not None and ts >= event_phases["last_end"]:
        return OUTSIDE_PHASE, "after"
    return OUTSIDE_PHASE, "between"


def resolve_event_id(manifest: Dict[str, Any], event_id: Optional[str]) -> Optional[str]:
    recorded = manifest.get("event_id")
    if event_id and recorded and event_id != recorded:
        raise ValueError(f"--event-id {event_id!r} conflicts with the manifest's event_id {recorded!r}")
    return recorded or event_id


def build_explorer_data(manifest: Dict[str, Any], output_root: str, event_id: Optional[str] = None,
                        event_config_path: str = DEFAULT_EVENT_CONFIG) -> Dict[str, Any]:
    hpc.verify_manifest_integrity(manifest)
    resolved_event = resolve_event_id(manifest, event_id)
    event_phases = load_event_phases(resolved_event, event_config_path) if resolved_event else None
    verification = hpc.verify_store(output_root, manifest)
    store = hpc.load_store(os.path.join(output_root, "comment_store.json"), must_exist=True)
    run_records = load_run_records(output_root, store)
    mode = hpc.manifest_scope_mode(manifest)
    outcomes = latest_video_outcomes(run_records)
    store_cov = store.get("video_coverage", {})

    by_video: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
    for c in store.get("comments", {}).values():
        by_video[c.get("manifest_video_id")].append(c)

    videos = []
    for v in manifest["videos"]:
        vid = v["video_id"]
        comments = by_video.get(vid, [])
        label, complete = coverage_label(mode, outcomes.get(vid), store_cov.get(vid))
        pubs = sorted(c["published_at"] for c in comments if c.get("published_at"))
        ages = [hours_between(v.get("video_published_at"), c.get("published_at")) for c in comments]
        videos.append({
            **v,
            "retrieved": len(comments),
            "in_window": sum(1 for c in comments if c.get("in_early_window")) if mode == hpc.SCOPE_EARLY_WINDOW else None,
            "within_72h": sum(1 for a in ages if a is not None and 0 <= a < 72),
            "earliest": pubs[0] if pubs else None,
            "latest": pubs[-1] if pubs else None,
            "coverage_label": label,
            "coverage_complete": complete,
            "collected": vid in outcomes,
            "likely_pinned": sum(1 for c in comments if c.get("likely_pinned")),
            "sample": deterministic_sample(comments, SAMPLE_PER_VIDEO),
        })

    complete_ids = {v["video_id"] for v in videos if v["coverage_complete"]}
    monthly: Dict[str, List[int]] = collections.defaultdict(lambda: [0, 0])  # [complete, partial]
    age_counts: Dict[str, List[int]] = {label: [0, 0] for label, _ in AGE_BUCKETS}
    pub_by_vid = {v["video_id"]: v.get("video_published_at") for v in manifest["videos"]}
    phase_counts: Dict[str, List[int]] = {}
    outside_sides: Dict[str, List[int]] = {"before": [0, 0], "between": [0, 0], "after": [0, 0]}
    if event_phases:
        phase_counts = {p["window_type"]: [0, 0] for p in event_phases["phases"]}
        phase_counts[OUTSIDE_PHASE] = [0, 0]
        phase_counts[UNPARSEABLE_PHASE] = [0, 0]
    comment_phase: Dict[str, str] = {}
    for c in store.get("comments", {}).values():
        idx = 0 if c.get("manifest_video_id") in complete_ids else 1
        if event_phases:
            phase, side = assign_phase(c.get("published_at"), event_phases)
            phase_counts[phase][idx] += 1
            if side:
                outside_sides[side][idx] += 1
            comment_phase[c["comment_id"]] = phase if not side else f"{phase} ({side})"
        if c.get("published_at"):
            monthly[c["published_at"][:7]][idx] += 1
        bucket = age_bucket(hours_between(pub_by_vid.get(c.get("manifest_video_id")), c.get("published_at")))
        if bucket:
            age_counts[bucket][idx] += 1

    channels: Dict[str, Dict[str, int]] = collections.defaultdict(lambda: {"videos": 0, "retrieved": 0, "complete": 0})
    for v in videos:
        ch = channels[v.get("channel_title") or "UNKNOWN"]
        ch["videos"] += 1
        ch["retrieved"] += v["retrieved"]
        ch["complete"] += int(v["coverage_complete"])

    all_pubs = sorted(c["published_at"] for c in store.get("comments", {}).values() if c.get("published_at"))
    return {
        "manifest": manifest,
        "mode": mode,
        "verification": verification,
        "runs": run_records,
        "videos": videos,
        "channels": dict(sorted(channels.items(), key=lambda kv: -kv[1]["retrieved"])),
        "monthly": dict(sorted(monthly.items())),
        "age_counts": age_counts,
        "total_comments": len(store.get("comments", {})),
        "date_range": (all_pubs[0], all_pubs[-1]) if all_pubs else (None, None),
        "comment_lookup": store.get("comments", {}),
        "event_phases": event_phases,
        "phase_counts": phase_counts,
        "outside_sides": outside_sides,
        "comment_phase": comment_phase,
    }


# =============================================================================
# HTML rendering
# =============================================================================

CSS = """
:root { --bg:#fbfbfa; --fg:#1d1d1b; --muted:#6b6b66; --line:#e3e2dd; --card:#ffffff;
  --complete:#2f6f8f; --partial:#d08a3a; --warn-bg:#fff6e0; --warn-fg:#7a5200; --bad-bg:#fde8e6; --bad-fg:#8a1c12;
  --ok-bg:#e6f4ea; --ok-fg:#1e5e2f; }
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) { --bg:#161615; --fg:#ecebe6; --muted:#a3a29b;
  --line:#33332f; --card:#1f1f1d; --complete:#6fb0d0; --partial:#e6a85e; --warn-bg:#3a2f12; --warn-fg:#f3d58f;
  --bad-bg:#3d1714; --bad-fg:#f4b4ab; --ok-bg:#15301d; --ok-fg:#a6dcb3; } }
* { box-sizing:border-box; }
body { margin:0; background:var(--bg); color:var(--fg); font:14px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif; }
main { max-width:1100px; margin:0 auto; padding:24px 16px 64px; }
h1 { font-size:22px; margin:0 0 4px; } h2 { font-size:17px; margin:32px 0 8px; }
.muted { color:var(--muted); } .mono { font-family:ui-monospace,Consolas,monospace; font-size:12px; }
.banner { padding:10px 14px; border-radius:6px; margin:12px 0; }
.ok { background:var(--ok-bg); color:var(--ok-fg); } .warn { background:var(--warn-bg); color:var(--warn-fg); }
.bad { background:var(--bad-bg); color:var(--bad-fg); }
.kpis { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:10px; margin:16px 0; }
.kpi { background:var(--card); border:1px solid var(--line); border-radius:6px; padding:10px 12px; }
.kpi b { display:block; font-size:20px; } .kpi span { color:var(--muted); font-size:12px; }
.scroll { overflow-x:auto; } table.wide { min-width:1000px; } td.ts { white-space:nowrap; } td.desc { min-width:240px; }
table { border-collapse:collapse; width:100%; background:var(--card); }
th, td { border-bottom:1px solid var(--line); padding:6px 8px; text-align:left; vertical-align:top; }
th { font-weight:600; font-size:12px; color:var(--muted); } td.num, th.num { text-align:right; }
.pill { display:inline-block; padding:1px 8px; border-radius:10px; font-size:12px; }
.pill.complete { background:var(--ok-bg); color:var(--ok-fg); } .pill.partial { background:var(--warn-bg); color:var(--warn-fg); }
.legend span { display:inline-flex; align-items:center; gap:6px; margin-right:16px; font-size:12px; color:var(--muted); }
.sw { width:10px; height:10px; border-radius:2px; display:inline-block; }
svg text { fill:var(--muted); font-size:10px; }
details { background:var(--card); border:1px solid var(--line); border-radius:6px; margin:6px 0; padding:6px 10px; }
summary { cursor:pointer; } .comment { border-top:1px solid var(--line); padding:6px 0; white-space:pre-wrap; }
a { color:var(--complete); }
"""


def _stacked_bar_svg(series: List[Tuple[str, int, int]], tick_labels: Optional[Dict[int, str]] = None) -> str:
    """series: (label, complete_count, partial_count). tick_labels maps bar index -> axis label
    (default: every bar's own label). Returns an inline SVG stacked bar chart."""
    if not series:
        return "<p class='muted'>No data.</p>"
    width, height, pad_l, pad_b, pad_t = 1000, 220, 40, 34, 10
    n = len(series)
    peak = max(c + p for _, c, p in series) or 1
    bar_w = (width - pad_l) / n
    plot_h = height - pad_b - pad_t
    parts = [f"<svg viewBox='0 0 {width} {height}' width='100%' role='img' aria-label='stacked bar chart'>"]
    for tick in (0, 0.5, 1):
        y = pad_t + plot_h * (1 - tick)
        parts.append(f"<line x1='{pad_l}' x2='{width}' y1='{y:.1f}' y2='{y:.1f}' stroke='var(--line)'/>")
        parts.append(f"<text x='{pad_l - 6}' y='{y + 3:.1f}' text-anchor='end'>{int(peak * tick)}</text>")
    for i, (label, complete, partial) in enumerate(series):
        x = pad_l + i * bar_w + 1
        w = max(bar_w - 2, 1)
        h_c = plot_h * complete / peak
        h_p = plot_h * partial / peak
        y_c = pad_t + plot_h - h_c
        y_p = y_c - h_p
        tip = _esc(f"{label}: {complete} complete-coverage, {partial} partial-coverage")
        parts.append(f"<g><title>{tip}</title>"
                     f"<rect x='{x:.1f}' y='{y_c:.1f}' width='{w:.1f}' height='{h_c:.1f}' fill='var(--complete)'/>"
                     f"<rect x='{x:.1f}' y='{y_p:.1f}' width='{w:.1f}' height='{h_p:.1f}' fill='var(--partial)'/></g>")
        tick = label if tick_labels is None else tick_labels.get(i)
        if tick:
            parts.append(f"<text x='{x + w / 2:.1f}' y='{height - pad_b + 14}' text-anchor='middle'>{_esc(tick)}</text>")
    parts.append("</svg>")
    return "".join(parts)


def month_tick_labels(months: List[str]) -> Dict[int, str]:
    """Short ranges label every month; longer ranges label the first month and each January by year."""
    if len(months) <= 18:
        return {i: mo for i, mo in enumerate(months)} if len(months) <= 9 else \
            {i: mo for i, mo in enumerate(months) if i % 2 == 0}
    return {i: (mo if i == 0 else mo[:4]) for i, mo in enumerate(months) if i == 0 or mo.endswith("-01")
            and i >= 3}


def _month_range(first: str, last: str) -> List[str]:
    y, m = int(first[:4]), int(first[5:7])
    out = []
    while (y, m) <= (int(last[:4]), int(last[5:7])):
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def _utc(ts: Optional[datetime.datetime]) -> str:
    return ts.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC") if ts else ""


def render_phase_section(data: Dict[str, Any]) -> str:
    ev = data["event_phases"]
    out = ["<h2>Event-relative phases</h2>"]
    if not ev:
        out.append("<p class='muted'>No event selected, so no event-relative phases are shown. Re-run with "
                   "<span class='mono'>--event-id</span> (for example WC_2022_FINAL).</p>")
        return "".join(out)
    out.append(f"<p class='muted'>Phases come from config/event_windows.yml for {_esc(ev['event_id'])}, anchored on "
               f"occurred_at = {_esc(_utc(ev['occurred_at']))} (described here as “{_esc(ev['anchor'])}”). Each phase "
               "includes its From time and excludes its To time. Descriptions use only the configured offsets; no "
               "period is labelled “during the match” or “post-match”. Counts are retrieved comments, shown "
               "separately for videos with complete and partial collection, and are not total discussion volume.</p>")
    if ev["overlapping_windows"]:
        out.append(f"<div class='banner warn'>Configured windows overlap: {_esc(ev['overlapping_windows'])}. "
                   "A comment is counted in the first matching window in configuration order.</div>")
    out.append("<div class='scroll'><table class='wide'><tr><th>Phase (config window_type)</th><th>Description</th>"
               "<th>From (UTC, inclusive)</th><th>To (UTC, exclusive)</th><th class='num'>From complete-collection videos</th>"
               "<th class='num'>From partial-collection videos</th></tr>")
    counts = data["phase_counts"]
    for p in ev["phases"]:
        c, pt = counts[p["window_type"]]
        out.append(f"<tr><td class='mono'>{_esc(p['window_type'])}</td><td class='desc'>{_esc(p['description'])}</td>"
                   f"<td class='mono ts'>{_esc(_utc(p['start']))}</td><td class='mono ts'>{_esc(_utc(p['end']))}</td>"
                   f"<td class='num'>{c}</td><td class='num'>{pt}</td></tr>")
    sides = data["outside_sides"]
    side_text = "; ".join(f"{side}: {sides[side][0]} complete / {sides[side][1]} partial"
                          for side in ("before", "between", "after") if sum(sides[side]))
    c, pt = counts[OUTSIDE_PHASE]
    out.append(f"<tr><td class='mono'>{OUTSIDE_PHASE}</td><td>Outside every configured window"
               f"{_esc(' (' + side_text + ')') if side_text else ''}</td>"
               f"<td class='mono ts'>before {_esc(_utc(ev['first_start']))}</td><td class='mono ts'>or from "
               f"{_esc(_utc(ev['last_end']))}</td><td class='num'>{c}</td><td class='num'>{pt}</td></tr>")
    if sum(counts[UNPARSEABLE_PHASE]):
        c, pt = counts[UNPARSEABLE_PHASE]
        out.append(f"<tr><td class='mono'>{UNPARSEABLE_PHASE}</td><td>published_at could not be parsed</td><td></td>"
                   f"<td></td><td class='num'>{c}</td><td class='num'>{pt}</td></tr>")
    out.append("</table></div>")
    if ev["gaps_between_windows"]:
        out.append(f"<p class='muted'>Gaps between configured windows: {_esc(ev['gaps_between_windows'])} "
                   "(counted as outside, “between”).</p>")
    return "".join(out)


def render_html(data: Dict[str, Any]) -> str:
    m, ver, mode = data["manifest"], data["verification"], data["mode"]
    videos = data["videos"]
    n_complete = sum(1 for v in videos if v["coverage_complete"])
    n_collected = sum(1 for v in videos if v["collected"])
    warnings = ver.get("warnings", {}).get("updated_before_published_within_tolerance", [])
    out = ["<!doctype html><html lang='en'><head><meta charset='utf-8'>",
           "<meta name='viewport' content='width=device-width,initial-scale=1'>",
           f"<title>Comment Pilot Explorer</title><style>{CSS}</style></head><body><main>"]

    out.append(f"<h1>Comment pilot explorer — {_esc(m['manifest_version'])}</h1>")
    ev = data["event_phases"]
    event_text = (ev["event_id"] + ("" if m.get("event_id") else " (from --event-id; not recorded in v1 manifest)")
                  if ev else "none selected (pass --event-id for event-relative phases)")
    out.append(f"<div class='muted'>Event: {_esc(event_text)} · "
               f"mode {_esc(mode)} · manifest <span class='mono'>{_esc(m['manifest_sha256'][:16])}…</span> · "
               f"generated {_esc(datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M UTC'))}</div>")

    if ver["passed"]:
        out.append("<div class='banner ok'>Offline verification passed.</div>")
    else:
        failing = {k: c for k, c in ver.get("problem_counts", {}).items() if c}
        out.append(f"<div class='banner bad'><b>Verification failed:</b> {_esc(failing)}</div>")
    if warnings:
        out.append(f"<div class='banner warn'><b>{len(warnings)} timestamp warning(s):</b> updated_at earlier than "
                   f"published_at by ≤ {ver.get('timestamp_skew_tolerance_seconds')}s, as returned by the API. "
                   "Raw values kept unchanged; listed below.</div>")

    if mode == hpc.SCOPE_EARLY_WINDOW:
        scope_note = (f"Comments published within {m['scope']['window_hours']}h of each video's upload. Window counts are "
                      "complete only for videos marked Complete; budget-truncated videos are partial.")
    else:
        scope_note = (f"Newest {m['scope']['max_comments_per_video']} top-level comments per video, retrieved at harvest "
                      "time. For videos marked “Newest only”, older comments exist but were not retrieved, so "
                      "early comments are under-represented for busy videos.")
    out.append(f"<div class='banner warn'><b>These are retrieved comments, not total discussion volume.</b> "
               f"{_esc(scope_note)}</div>")

    first, last = data["date_range"]
    out.append("<div class='kpis'>")
    for value, label in [
        (data["total_comments"], "comments retrieved (unique IDs)"),
        (f"{n_collected} / {len(videos)}", "manifest videos collected"),
        (f"{n_complete}", "videos with complete coverage"),
        (len(data["channels"]), "channels"),
        (f"{_date(first)} → {_date(last)}", "comment publication range"),
        (len(warnings), "timestamp warnings"),
    ]:
        out.append(f"<div class='kpi'><b>{_esc(value)}</b><span>{_esc(label)}</span></div>")
    out.append("</div>")

    legend = ("<div class='legend'><span><i class='sw' style='background:var(--complete)'></i>from videos with complete "
              "coverage</span><span><i class='sw' style='background:var(--partial)'></i>from videos with partial coverage"
              "</span></div>")
    out.append("<h2>Comments by publication month</h2>")
    out.append("<p class='muted'>Months are when comments were posted. Partial-coverage videos are missing their older "
               "comments, so earlier months are undercounted for them.</p>" + legend)
    if first and last:
        months = _month_range(first[:7], last[:7])
        series = [(mo, *data["monthly"].get(mo, [0, 0])) for mo in months]
        out.append(f"<div class='scroll'>{_stacked_bar_svg(series, month_tick_labels(months))}</div>")

    out.append(render_phase_section(data))

    out.append("<h2>Video-relative: time from each video's upload to comment</h2>")
    out.append("<p class='muted'>Measured from each video's own upload time, independent of the event phases above. "
               "The “first 72 hours after upload” window used for early-window collection is the first two "
               "buckets here.</p>" + legend)
    out.append(_stacked_bar_svg([(label, *data["age_counts"][label]) for label, _ in AGE_BUCKETS]))

    out.append("<h2>Channels</h2><div class='scroll'><table><tr><th>Channel</th><th class='num'>Videos</th>"
               "<th class='num'>Complete coverage</th><th class='num'>Comments retrieved</th></tr>")
    for name, ch in data["channels"].items():
        out.append(f"<tr><td>{_esc(name)}</td><td class='num'>{ch['videos']}</td><td class='num'>{ch['complete']}</td>"
                   f"<td class='num'>{ch['retrieved']}</td></tr>")
    out.append("</table></div>")

    window_head = "<th class='num'>In window</th>" if mode == hpc.SCOPE_EARLY_WINDOW else ""
    out.append("<h2>Videos</h2><div class='scroll'><table><tr><th>Video</th><th>Channel</th><th>Player</th>"
               "<th>Uploaded</th><th class='num'>Retrieved</th>" + window_head +
               "<th class='num'>First 72h after upload (video-relative)</th><th>Comment dates</th><th>Coverage</th></tr>")
    for v in sorted(videos, key=lambda x: x.get("video_published_at") or ""):
        pill = "complete" if v["coverage_complete"] else "partial"
        window_cell = f"<td class='num'>{v['in_window']}</td>" if mode == hpc.SCOPE_EARLY_WINDOW else ""
        pinned = f" · {v['likely_pinned']} likely pinned" if v["likely_pinned"] else ""
        out.append(
            f"<tr><td><a href='https://www.youtube.com/watch?v={_esc(v['video_id'])}'>{_esc((v.get('title') or '')[:70])}</a>"
            f"<div class='mono muted'>{_esc(v['video_id'])}</div></td><td>{_esc(v.get('channel_title'))}</td>"
            f"<td>{_esc(v.get('player_category'))}</td><td>{_esc(_date(v.get('video_published_at')))}</td>"
            f"<td class='num'>{v['retrieved']}</td>{window_cell}<td class='num'>{v['within_72h']}</td>"
            f"<td>{_esc(_date(v['earliest']))} → {_esc(_date(v['latest']))}</td>"
            f"<td><span class='pill {pill}'>{_esc(v['coverage_label'])}</span>{_esc(pinned)}</td></tr>")
    out.append("</table></div>")

    out.append("<h2>Timestamp warnings</h2>")
    if warnings:
        lookup = data["comment_lookup"]
        out.append("<div class='scroll'><table><tr><th>Comment ID</th><th>Video</th><th>published_at</th>"
                   "<th>updated_at</th><th class='num'>Gap (s)</th></tr>")
        for cid, gap in warnings:
            c = lookup.get(cid, {})
            out.append(f"<tr><td class='mono'>{_esc(cid)}</td><td class='mono'>{_esc(c.get('manifest_video_id'))}</td>"
                       f"<td>{_esc(c.get('published_at'))}</td><td>{_esc(c.get('latest_updated_at'))}</td>"
                       f"<td class='num'>{gap:g}</td></tr>")
        out.append("</table></div>")
    else:
        out.append("<p class='muted'>None.</p>")

    out.append(f"<h2>Audit sample</h2><p class='muted'>Up to {SAMPLE_PER_VIDEO} comments per video, chosen by hashed "
               "comment ID so the same sample is shown every time. Author identifiers are never shown.</p>")
    for v in sorted(videos, key=lambda x: x.get("video_published_at") or ""):
        out.append(f"<details><summary>{_esc((v.get('title') or '')[:80])} <span class='muted'>({len(v['sample'])} of "
                   f"{v['retrieved']})</span></summary>")
        for c in v["sample"]:
            flags = []
            if c.get("in_early_window"):
                flags.append("in window")
            if c.get("likely_pinned"):
                flags.append("likely pinned")
            if len(c.get("text_versions", [])) > 1:
                flags.append(f"{len(c['text_versions'])} text versions")
            if c["comment_id"] in data["comment_phase"]:
                flags.append(f"phase {data['comment_phase'][c['comment_id']]}")
            meta = f"{_date(c.get('published_at'))} · {c['comment_id']}" + (f" · {', '.join(flags)}" if flags else "")
            out.append(f"<div class='comment'><div class='muted mono'>{_esc(meta)}</div>{_esc(latest_text(c))}</div>")
        out.append("</details>")

    out.append("<h2>Runs</h2><div class='scroll'><table><tr><th>Run</th><th>Status</th><th class='num'>Requests</th>"
               "<th class='num'>Comments observed</th><th>Accepted by</th></tr>")
    for r in data["runs"]:
        out.append(f"<tr><td class='mono'>{_esc(r['run_id'])}</td><td>{_esc(r['run_status'])}</td>"
                   f"<td class='num'>{r['telemetry']['requests_by_endpoint'].get('commentThreads.list', 0)}</td>"
                   f"<td class='num'>{r['comments_observed']}</td><td>{_esc(r.get('scope_accepted_by'))}</td></tr>")
    out.append("</table></div></main></body></html>")
    return "".join(out)


def main(argv: Optional[List[str]] = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description="Render an offline HTML explorer for a pilot comment harvest")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--out", default=None, help="HTML output path (default: <output-root>/explorer.html)")
    parser.add_argument("--event-id", default=None,
                        help="Event for event-relative phases when the manifest has none (v1); must match if present")
    parser.add_argument("--event-config", default=DEFAULT_EVENT_CONFIG, help="Event windows config path")
    args = parser.parse_args(argv)

    with open(args.manifest, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    try:
        data = build_explorer_data(manifest, args.output_root, args.event_id, args.event_config)
    except ValueError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1
    out_path = args.out or os.path.join(args.output_root, "explorer.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(render_html(data))
    ver = data["verification"]
    print(f"[OK] Explorer written to {out_path}")
    print(f"     comments={data['total_comments']} videos={len(data['videos'])} verification_passed={ver['passed']} "
          f"timestamp_warnings={ver['warning_counts'].get('updated_before_published_within_tolerance', 0)}")
    if data["event_phases"]:
        summary = {k: v for k, v in data["phase_counts"].items() if sum(v)}
        print(f"     event={data['event_phases']['event_id']} phase_counts[complete, partial]={json.dumps(summary)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
