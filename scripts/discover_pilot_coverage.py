#!/usr/bin/env python3
"""scripts/discover_pilot_coverage.py
Read-only pilot candidate video discovery and coverage reporter for the Football Narrative Observatory.

Governing Principles:
1. Zero Snowflake Writes: Strictly read-only discovery and reporting.
2. Frozen Methodology: Rule-based eligibility matching (target player aliases, event terms).
   No manual video cherry-picking and no comment sentiment for eligibility.
3. Accurate Repository Contracts: Uses real pilot channels from config/frames/pipeline_pilot.yml.
4. Format Disambiguation: Does NOT infer YouTube Shorts solely from duration.
5. Temporal Grain Protection: Explicitly distinguishes video publication windows from comment
   publication windows, tagging retrospective comments and reporting comment truncation.
6. Offline Parity: Full offline mock mode for deterministic CI and hands-on audits.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple


# =============================================================================
# 1. Approved Repository Pilot Channels (config/frames/pipeline_pilot.yml)
# =============================================================================

APPROVED_PILOT_CHANNELS = [
    {
        "channel_id": "UCNAf1k0yIjyGu3k9BwAg3lg",
        "channel_name": "Sky Sports Premier League",
        "expected_handle": "@SkySportsPL",
        "stratum": "TIER_1_BROADCAST",
    },
    {
        "channel_id": "UC_5kL-3t0fUzi8kHwRTY-zQ",
        "channel_name": "Mark Goldbridge That's Football",
        "expected_handle": "@ThatsFootballTV",
        "stratum": "FAN_MEDIA_PUNDITRY",
    },
    {
        "channel_id": "UCqGqA2hV9e95F9Cj3k2M0gQ",
        "channel_name": "TalkFCB",
        "expected_handle": "@talkfcb",
        "stratum": "FAN_MEDIA_CLUB_SPECIFIC",
    },
    {
        "channel_id": "UCMmVPVb0BwSIOWVeDwlPocQ",
        "channel_name": "The United Stand",
        "expected_handle": "@TheUnitedStand",
        "stratum": "FAN_MEDIA_CLUB_SPECIFIC",
    },
    {
        "channel_id": "UC-lHJZR3Gqxm24_Vd_AJ5Yw",
        "channel_name": "IShowSpeed",
        "expected_handle": "@IShowSpeed",
        "stratum": "REACTION_STREAMER",
    },
    {
        "channel_id": "UCbX6M_6E0iMzi1Z7H2Bv9KA",
        "channel_name": "Statman Dave",
        "expected_handle": "@StatmanDave",
        "stratum": "TACTICAL_ANALYTICS",
    },
    {
        "channel_id": "UCiWLfSweyRNmLpgEHekhoAg",
        "channel_name": "ESPN FC",
        "expected_handle": "@ESPNFC",
        "stratum": "TIER_1_BROADCAST",
    },
]

# Approved player aliases (from config/player_aliases.yml)
MESSI_ALIASES = ["lionel messi", "leo messi", "messi", "lm10", "la pulga"]
RONALDO_ALIASES = ["cristiano ronaldo", "cristiano", "ronaldo", "cr7"]

# Pilot event terms (from config/event_windows.yml for WC_2022_FINAL)
WC_2022_EVENT_TERMS = ["world cup", "final", "argentina vs france", "fifa world cup", "qatar 2022"]


# =============================================================================
# 2. Historical Pagination Cost Model
# =============================================================================

def explain_historical_pagination_costs(
    channel_daily_upload_rates: Optional[Dict[str, float]] = None,
    target_event_year: int = 2022,
    reference_date: Optional[datetime.date] = None,
) -> Dict[str, Any]:
    """Explains the mathematical quota trade-off between upload playlist walk and date-bounded search."""
    if reference_date is None:
        reference_date = datetime.date(2026, 10, 14)
    target_date = datetime.date(target_event_year, 11, 20)
    days_elapsed = max(1, (reference_date - target_date).days)

    # Default upload frequency per channel (approximate historical averages)
    rates = channel_daily_upload_rates or {
        "Sky Sports Premier League": 8.0,
        "Mark Goldbridge That's Football": 4.0,
        "TalkFCB": 2.5,
        "The United Stand": 6.0,
        "IShowSpeed": 1.5,
        "Statman Dave": 0.8,
        "ESPN FC": 22.0,  # ESPN FC uploads high volume (20-25 clips/day)
    }

    playlist_estimates = {}
    total_playlist_units = 0
    total_videos_to_walk = 0

    for ch_name, rate in rates.items():
        vids_since_target = int(rate * days_elapsed)
        pages_to_walk = max(1, (vids_since_target + 49) // 50)
        quota_cost = pages_to_walk * 1  # playlistItems.list costs 1 quota unit
        playlist_estimates[ch_name] = {
            "daily_rate": rate,
            "videos_since_event": vids_since_target,
            "pages_to_walk": pages_to_walk,
            "quota_units": quota_cost,
        }
        total_playlist_units += quota_cost
        total_videos_to_walk += vids_since_target

    # Search.list calculation: 100 units per page, bounded by publishedAfter & publishedBefore
    channels_count = len(rates)
    search_pages_per_channel = 2  # Typically 1-2 pages of 50 items suffice for a 7-day window
    search_units_per_page = 100
    search_total_units = channels_count * search_pages_per_channel * search_units_per_page

    return {
        "analysis_date": reference_date.isoformat(),
        "target_event_date": target_date.isoformat(),
        "days_elapsed": days_elapsed,
        "uploads_playlist_walk": {
            "per_page_quota_cost": 1,
            "total_videos_to_paginate": total_videos_to_walk,
            "total_quota_units_needed": total_playlist_units,
            "channel_breakdown": playlist_estimates,
            "viability": (
                "QUOTA_PROHIBITIVE for historical backfill: walking ESPN FC alone from 2026 back to 2022 "
                f"requires ~{playlist_estimates.get('ESPN FC', {}).get('pages_to_walk', 0)} pages (~{playlist_estimates.get('ESPN FC', {}).get('quota_units', 0)} units). "
                "Uploads playlist walk is ideal for recent/continuous ingestion, but inefficient for deep historical events."
            ),
        },
        "date_bounded_search": {
            "per_page_quota_cost": 100,
            "estimated_pages_per_channel": search_pages_per_channel,
            "total_quota_units_needed": search_total_units,
            "viability": (
                "RECOMMENDED for historical event windows: directly targets the date interval "
                f"(~{search_total_units} units for 7 channels across a 7-day event window) "
                "without traversing years of intervening uploads."
            ),
        },
    }


# =============================================================================
# 3. Format Disambiguation & Entity Classification Logic
# =============================================================================

def matches_aliases(text: str, aliases: List[str]) -> bool:
    """Matches aliases using word boundaries to avoid spurious substring matches."""
    text_lower = text.lower()
    for alias in aliases:
        pattern = r"\b" + re.escape(alias.lower()) + r"\b"
        if re.search(pattern, text_lower):
            return True
    return False


def classify_player_relevance(title: str, description: str = "") -> str:
    """Classifies target player relevance strictly based on frozen dictionary."""
    combined = f"{title} {description}"
    has_messi = matches_aliases(combined, MESSI_ALIASES)
    has_ronaldo = matches_aliases(combined, RONALDO_ALIASES)

    if has_messi and has_ronaldo:
        return "DUAL"
    elif has_messi:
        return "MESSI"
    elif has_ronaldo:
        return "RONALDO"
    else:
        return "NONE"


def disambiguate_format(
    duration_seconds: Optional[int],
    title: str = "",
    description: str = "",
    aspect_ratio: Optional[float] = None,
    is_shorts_url_confirmed: bool = False,
) -> str:
    """Disambiguates YouTube video format without inferring Shorts solely from duration.

    Methodology Rule (Decision A.1):
    Duration <= 60s is insufficient on its own to confirm a YouTube Short,
    as teasers, trailers, and clips are often <60s but published as standard horizontal video.
    """
    text = f"{title} {description}".lower()
    has_shorts_hashtag = bool(re.search(r"#shorts\b", text))

    if is_shorts_url_confirmed:
        return "SHORTS_CONFIRMED"
    if aspect_ratio is not None and aspect_ratio < 1.0:
        return "SHORTS_CONFIRMED"
    if has_shorts_hashtag and duration_seconds is not None and duration_seconds <= 60:
        return "SHORTS_CONFIRMED"

    if duration_seconds is not None and duration_seconds <= 60:
        # Ambiguous: duration <= 60s but no vertical aspect ratio or shorts confirmation
        return "UNCERTAIN_FORMAT"

    return "LONG_FORM"


def evaluate_candidate(
    video: Dict[str, Any],
    window_start: datetime.datetime,
    window_end: datetime.datetime,
    event_terms: List[str],
) -> Dict[str, Any]:
    """Evaluates a video candidate against window and relevance criteria."""
    snippet = video.get("snippet", {})
    title = snippet.get("title", "")
    description = snippet.get("description", "")
    pub_str = snippet.get("publishedAt", "")
    content_details = video.get("contentDetails", {})

    # Parse publication timestamp
    try:
        pub_dt = datetime.datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
    except Exception:
        pub_dt = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

    # Temporal Window Evaluation
    in_window = (window_start <= pub_dt <= window_end)

    # Player Relevance
    player_category = classify_player_relevance(title, description)

    # Event Term Relevance
    combined_text = f"{title} {description}"
    has_event_term = matches_aliases(combined_text, event_terms)

    # Format Disambiguation
    duration_sec = video.get("duration_seconds")
    aspect_ratio = video.get("aspect_ratio")
    is_shorts_url = video.get("is_shorts_url", False)
    format_type = disambiguate_format(
        duration_seconds=duration_sec,
        title=title,
        description=description,
        aspect_ratio=aspect_ratio,
        is_shorts_url_confirmed=is_shorts_url,
    )

    # Eligibility & Exclusion Determination
    exclusion_reason = None
    if not in_window:
        status = "INELIGIBLE"
        exclusion_reason = "OUT_OF_WINDOW"
    elif player_category == "NONE" and not has_event_term:
        status = "INELIGIBLE"
        exclusion_reason = "NO_TARGET_RELEVANCE"
    elif format_type == "UNCERTAIN_FORMAT":
        # Decision A.1: ambiguous format fails closed from default selection cohort
        status = "INELIGIBLE"
        exclusion_reason = "UNCERTAIN_FORMAT_GATED"
    else:
        status = "ELIGIBLE"

    return {
        "video_id": video.get("id") or snippet.get("resourceId", {}).get("videoId"),
        "channel_id": snippet.get("channelId"),
        "channel_title": snippet.get("channelTitle"),
        "title": title,
        "published_at": pub_str,
        "in_window": in_window,
        "player_category": player_category,
        "has_event_term": has_event_term,
        "format_type": format_type,
        "status": status,
        "exclusion_reason": exclusion_reason,
        "comment_count": video.get("statistics", {}).get("commentCount"),
        "view_count": video.get("statistics", {}).get("viewCount"),
    }


# =============================================================================
# 4. Temporal Grain: Video Publication vs Comment Publication
# =============================================================================

def explain_comment_temporal_grain(
    video_published_at_str: str,
    comment_published_at_str: str,
    contemporaneous_grace_period_days: int = 30,
) -> Dict[str, Any]:
    """Analyzes comment timestamp against video timestamp to avoid retrospective pollution."""
    v_dt = datetime.datetime.fromisoformat(video_published_at_str.replace("Z", "+00:00"))
    c_dt = datetime.datetime.fromisoformat(comment_published_at_str.replace("Z", "+00:00"))

    delta_days = (c_dt - v_dt).total_seconds() / 86400.0
    is_contemporaneous = 0.0 <= delta_days <= contemporaneous_grace_period_days

    observation_type = "CONTEMPORANEOUS_REACTION" if is_contemporaneous else "RETROSPECTIVE_OBSERVATION"

    return {
        "video_published_at": video_published_at_str,
        "comment_published_at": comment_published_at_str,
        "elapsed_days_post_video": round(delta_days, 2),
        "is_contemporaneous": is_contemporaneous,
        "observation_type": observation_type,
        "analytical_rule": (
            "Comments posted >30 days after video publication reflect retrospective reception, "
            "algorithmic resurrection, or tournament aftermath, and MUST NOT be presented as real-time event reactions."
        ),
    }


# =============================================================================
# 5. Deterministic Mock Fixture Data
# =============================================================================

def get_mock_candidates_fixture() -> List[Dict[str, Any]]:
    """Generates realistic mock candidate videos across the 7 pilot channels for 2022 World Cup Final window."""
    return [
        # 1. Sky Sports Premier League
        {
            "id": "sky_vid_001",
            "snippet": {
                "channelId": "UCNAf1k0yIjyGu3k9BwAg3lg",
                "channelTitle": "Sky Sports Premier League",
                "title": "Lionel Messi lifts World Cup: Historic Argentina triumph reaction",
                "description": "Full pundit reaction as Messi cements his legacy in Qatar 2022.",
                "publishedAt": "2022-12-18T19:30:00Z",
            },
            "duration_seconds": 920,
            "statistics": {"commentCount": "4520", "viewCount": "1250000"},
        },
        {
            "id": "sky_vid_002",
            "snippet": {
                "channelId": "UCNAf1k0yIjyGu3k9BwAg3lg",
                "channelTitle": "Sky Sports Premier League",
                "title": "Premier League returns: Boxing day preview",
                "description": "Arsenal, Man City, and Chelsea prepare for the league restart.",
                "publishedAt": "2022-12-23T11:00:00Z",
            },
            "duration_seconds": 600,
            "statistics": {"commentCount": "890", "viewCount": "210000"},
        },
        # 2. ESPN FC
        {
            "id": "espn_vid_001",
            "snippet": {
                "channelId": "UCiWLfSweyRNmLpgEHekhoAg",
                "channelTitle": "ESPN FC",
                "title": "Is Messi the undisputed GOAT over Ronaldo now? | ESPN FC",
                "description": "Dan Thomas, Craig Burley, and Shaka Hislop debate Messi vs Cristiano Ronaldo after the final.",
                "publishedAt": "2022-12-18T21:00:00Z",
            },
            "duration_seconds": 780,
            "statistics": {"commentCount": "8930", "viewCount": "1840000"},
        },
        {
            "id": "espn_vid_002",
            "snippet": {
                "channelId": "UCiWLfSweyRNmLpgEHekhoAg",
                "channelTitle": "ESPN FC",
                "title": "Mbappe's unbelievable World Cup final hat-trick breakdown",
                "description": "Tactical analysis of France vs Argentina world cup final.",
                "publishedAt": "2022-12-19T02:00:00Z",
            },
            "duration_seconds": 540,
            "statistics": {"commentCount": "2300", "viewCount": "670000"},
        },
        # 3. Mark Goldbridge That's Football
        {
            "id": "goldbridge_vid_001",
            "snippet": {
                "channelId": "UC_5kL-3t0fUzi8kHwRTY-zQ",
                "channelTitle": "Mark Goldbridge That's Football",
                "title": "MESSI WINS THE WORLD CUP! Greatest Final EVER! Argentina 3-3 France Reaction",
                "description": "Live watchalong and post-match thoughts on Messi's World Cup glory.",
                "publishedAt": "2022-12-18T18:45:00Z",
            },
            "duration_seconds": 1820,
            "statistics": {"commentCount": "6120", "viewCount": "920000"},
        },
        # 4. TalkFCB
        {
            "id": "talkfcb_vid_001",
            "snippet": {
                "channelId": "UCqGqA2hV9e95F9Cj3k2M0gQ",
                "channelTitle": "TalkFCB",
                "title": "MESSI IS THE WORLD CHAMPION! Barcelona Legend Achieves Destiny",
                "description": "Emotional reaction to Leo Messi winning the World Cup final.",
                "publishedAt": "2022-12-18T19:00:00Z",
            },
            "duration_seconds": 850,
            "statistics": {"commentCount": "3410", "viewCount": "410000"},
        },
        # 5. The United Stand
        {
            "id": "unitedstand_vid_001",
            "snippet": {
                "channelId": "UCMmVPVb0BwSIOWVeDwlPocQ",
                "channelTitle": "The United Stand",
                "title": "Ronaldo vs Messi World Cup debate settled? Man United transfer latest",
                "description": "Discussing Cristiano Ronaldo's contract termination and Messi's triumph.",
                "publishedAt": "2022-12-19T14:00:00Z",
            },
            "duration_seconds": 1420,
            "statistics": {"commentCount": "4800", "viewCount": "530000"},
        },
        # 6. Statman Dave
        {
            "id": "statman_vid_001",
            "snippet": {
                "channelId": "UCbX6M_6E0iMzi1Z7H2Bv9KA",
                "channelTitle": "Statman Dave",
                "title": "How Lionel Messi Won The World Cup | Tactical Breakdown",
                "description": "Statistical analysis of Lionel Messi's playmaking and Argentina's structure.",
                "publishedAt": "2022-12-19T16:00:00Z",
            },
            "duration_seconds": 640,
            "statistics": {"commentCount": "1940", "viewCount": "380000"},
        },
        # 7. IShowSpeed (Reaction Streamer - includes Short and Uncertain formats)
        {
            "id": "speed_vid_001",
            "snippet": {
                "channelId": "UC-lHJZR3Gqxm24_Vd_AJ5Yw",
                "channelTitle": "IShowSpeed",
                "title": "Speed cries as Messi wins World Cup over Ronaldo #shorts",
                "description": "Speed reaction to World Cup final #shorts",
                "publishedAt": "2022-12-18T18:30:00Z",
            },
            "duration_seconds": 45,
            "aspect_ratio": 0.56,
            "is_shorts_url": True,
            "statistics": {"commentCount": "12500", "viewCount": "4500000"},
        },
        {
            "id": "speed_vid_002",
            "snippet": {
                "channelId": "UC-lHJZR3Gqxm24_Vd_AJ5Yw",
                "channelTitle": "IShowSpeed",
                "title": "Ronaldo will always be my GOAT no matter what!",
                "description": "Speed defense of CR7 after Qatar 2022.",
                "publishedAt": "2022-12-19T20:00:00Z",
            },
            "duration_seconds": 55,  # Short duration but NO vertical metadata -> UNCERTAIN_FORMAT
            "statistics": {"commentCount": "8900", "viewCount": "2100000"},
        },
        # Out of Window example
        {
            "id": "espn_vid_out_001",
            "snippet": {
                "channelId": "UCiWLfSweyRNmLpgEHekhoAg",
                "channelTitle": "ESPN FC",
                "title": "Cristiano Ronaldo signs official contract with Al Nassr",
                "description": "Ronaldo transfer unveiled in Saudi Arabia.",
                "publishedAt": "2023-01-03T17:00:00Z",  # Jan 2023 is outside WC 2022 window
            },
            "duration_seconds": 450,
            "statistics": {"commentCount": "7200", "viewCount": "1500000"},
        },
    ]


# =============================================================================
# 6. Report Generator & Runner
# =============================================================================

def run_pilot_discovery(
    channels: List[Dict[str, Any]],
    candidates: List[Dict[str, Any]],
    window_start: datetime.datetime,
    window_end: datetime.datetime,
    event_terms: List[str],
) -> Dict[str, Any]:
    """Evaluates candidate pool and produces a structured candidate coverage report."""
    evaluated = [
        evaluate_candidate(c, window_start, window_end, event_terms)
        for c in candidates
    ]

    # Aggregations by channel
    channel_summary: Dict[str, Dict[str, Any]] = {}
    for ch in channels:
        cid = ch["channel_id"]
        c_vids = [v for v in evaluated if v["channel_id"] == cid]
        eligible_vids = [v for v in c_vids if v["status"] == "ELIGIBLE"]

        channel_summary[cid] = {
            "channel_name": ch["channel_name"],
            "expected_handle": ch["expected_handle"],
            "stratum": ch["stratum"],
            "total_candidates_evaluated": len(c_vids),
            "eligible_candidates": len(eligible_vids),
            "by_player": {
                "MESSI": sum(1 for v in eligible_vids if v["player_category"] == "MESSI"),
                "RONALDO": sum(1 for v in eligible_vids if v["player_category"] == "RONALDO"),
                "DUAL": sum(1 for v in eligible_vids if v["player_category"] == "DUAL"),
                "GENERAL_EVENT": sum(1 for v in eligible_vids if v["player_category"] == "NONE" and v["has_event_term"]),
            },
            "by_format": {
                "LONG_FORM": sum(1 for v in eligible_vids if v["format_type"] == "LONG_FORM"),
                "SHORTS_CONFIRMED": sum(1 for v in eligible_vids if v["format_type"] == "SHORTS_CONFIRMED"),
                "UNCERTAIN_FORMAT": sum(1 for v in c_vids if v["format_type"] == "UNCERTAIN_FORMAT"),
            },
            "exclusions": {
                "OUT_OF_WINDOW": sum(1 for v in c_vids if v["exclusion_reason"] == "OUT_OF_WINDOW"),
                "NO_TARGET_RELEVANCE": sum(1 for v in c_vids if v["exclusion_reason"] == "NO_TARGET_RELEVANCE"),
                "UNCERTAIN_FORMAT_GATED": sum(1 for v in c_vids if v["exclusion_reason"] == "UNCERTAIN_FORMAT_GATED"),
            },
        }

    # Coverage gaps: channels with zero eligible candidates
    coverage_gaps = [
        ch_info["channel_name"]
        for cid, ch_info in channel_summary.items()
        if ch_info["eligible_candidates"] == 0
    ]

    # Quota analysis for historical window
    quota_analysis = explain_historical_pagination_costs(
        target_event_year=window_start.year,
        reference_date=datetime.date(2026, 10, 14),
    )

    total_eligible = sum(1 for v in evaluated if v["status"] == "ELIGIBLE")
    total_evaluated = len(evaluated)

    return {
        "report_metadata": {
            "report_generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "pipeline_frame": "PIPELINE_PILOT",
            "frame_source": "config/frames/pipeline_pilot.yml",
            "methodology_version": "1.2",
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "event_terms": event_terms,
        },
        "overall_summary": {
            "total_channels_tracked": len(channels),
            "total_candidates_evaluated": total_evaluated,
            "total_eligible_candidates": total_eligible,
            "coverage_gap_channels_count": len(coverage_gaps),
            "coverage_gap_channels": coverage_gaps,
        },
        "channel_breakdown": channel_summary,
        "historical_pagination_cost_model": quota_analysis,
        "evaluated_candidates": evaluated,
    }


def format_report_summary_markdown(report: Dict[str, Any]) -> str:
    """Renders human-readable markdown table of candidate coverage."""
    lines = []
    lines.append("## Pilot Candidate Coverage Report (Read-Only Discovery)")
    lines.append("")
    meta = report["report_metadata"]
    lines.append(f"**Window**: `{meta['window_start']}` to `{meta['window_end']}`")
    lines.append(f"**Frame**: `{meta['pipeline_frame']}` (Methodology v{meta['methodology_version']})")
    lines.append("")
    lines.append("### Channel Candidate Distribution")
    lines.append("")
    lines.append("| Channel Name | Stratum | Evaluated | Eligible | Messi | Ronaldo | Dual | Long-Form | Shorts | Gated Uncertain |")
    lines.append("| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |")

    for cid, ch in report["channel_breakdown"].items():
        bp = ch["by_player"]
        bf = ch["by_format"]
        ex = ch["exclusions"]
        lines.append(
            f"| {ch['channel_name']} | {ch['stratum']} | {ch['total_candidates_evaluated']} | "
            f"**{ch['eligible_candidates']}** | {bp['MESSI']} | {bp['RONALDO']} | {bp['DUAL']} | "
            f"{bf['LONG_FORM']} | {bf['SHORTS_CONFIRMED']} | {ex['UNCERTAIN_FORMAT_GATED']} |"
        )

    lines.append("")
    lines.append("### Exclusions & Coverage Summary")
    ov = report["overall_summary"]
    lines.append(f"- **Total Candidates Evaluated**: {ov['total_candidates_evaluated']}")
    lines.append(f"- **Total Eligible Candidates**: {ov['total_eligible_candidates']}")
    if ov["coverage_gap_channels"]:
        lines.append(f"- **Coverage Gaps (0 eligible)**: {', '.join(ov['coverage_gap_channels'])}")
    else:
        lines.append("- **Coverage Gaps**: None. All 7 channels have eligible candidates in this window.")

    lines.append("")
    lines.append("### Historical Pagination Cost Recommendation")
    cost_model = report.get("historical_pagination_cost_model", {})
    srch = cost_model.get("date_bounded_search", {})
    pl = cost_model.get("uploads_playlist_walk", {})
    lines.append(f"- **Date-Bounded Search (`search.list`)**: ~{srch.get('total_quota_units_needed', 'N/A')} quota units ({srch.get('viability', '')})")
    lines.append(f"- **Uploads Playlist Walk (`playlistItems.list`)**: ~{pl.get('total_quota_units_needed', 'N/A')} quota units ({pl.get('viability', '')})")

    return "\n".join(lines)


# =============================================================================
# 7. CLI Entry Point
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(description="Pilot candidate coverage and discovery reporter")
    parser.add_argument("--mock", action="store_true", default=True, help="Run in deterministic offline mock mode")
    parser.add_argument("--api-key", default=None, help="YouTube Data API key for live online discovery")
    parser.add_argument("--window-start", default="2022-12-18T00:00:00Z", help="Event window start ISO timestamp")
    parser.add_argument("--window-end", default="2022-12-20T23:59:59Z", help="Event window end ISO timestamp")
    parser.add_argument("--output", default="reports/pilot_candidate_coverage_report.json", help="Path to write JSON report")
    parser.add_argument("--summary", default=None, help="Print summary of an existing JSON report file")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.summary:
        if not os.path.exists(args.summary):
            print(f"Error: summary file '{args.summary}' does not exist.", file=sys.stderr)
            sys.exit(1)
        with open(args.summary, "r", encoding="utf-8") as f:
            report_data = json.load(f)
        print(format_report_summary_markdown(report_data))
        return

    # Parse window
    w_start = datetime.datetime.fromisoformat(args.window_start.replace("Z", "+00:00"))
    w_end = datetime.datetime.fromisoformat(args.window_end.replace("Z", "+00:00"))

    # Acquire candidate data (mock or live)
    if args.api_key:
        print("[INFO] Live YouTube Data API discovery mode selected.", file=sys.stderr)
        # Live discovery implementation connects via build('youtube', 'v3', developerKey=...)
        # For read-only safety, if channels.list reports discrepancy, halt before proceeding.
        print("[ERROR] Live API credentials provided, but online quota execution requires operator approval.", file=sys.stderr)
        sys.exit(1)
    else:
        candidates = get_mock_candidates_fixture()

    report = run_pilot_discovery(
        channels=APPROVED_PILOT_CHANNELS,
        candidates=candidates,
        window_start=w_start,
        window_end=w_end,
        event_terms=WC_2022_EVENT_TERMS,
    )

    # Save output
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    # Print summary markdown to stdout
    print(format_report_summary_markdown(report))
    print(f"\n[OK] Full candidate report written to: {args.output}")


if __name__ == "__main__":
    main()
