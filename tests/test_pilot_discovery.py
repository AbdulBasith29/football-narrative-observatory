"""tests/test_pilot_discovery.py
Unit and regression tests for pilot candidate discovery and coverage reporting.

Validates:
1. Approved channel registry matching config/frames/pipeline_pilot.yml.
2. Word boundary alias matching for target players (preventing false substrings).
3. YouTube Shorts format disambiguation (Decision A.1: no duration-only inference).
4. Candidate evaluation, eligibility classification, and exclusion tagging.
5. Temporal grain analysis distinguishing contemporaneous event comments from retrospective discourse.
6. Historical pagination cost model for uploads playlist walk vs date-bounded search.
7. Full report generation and markdown summary.
"""

from __future__ import annotations

import datetime
import os
import sys
import pytest

# Ensure scripts/ is in pythonpath
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts")))

from discover_pilot_coverage import (
    APPROVED_PILOT_CHANNELS,
    MESSI_ALIASES,
    RONALDO_ALIASES,
    WC_2022_EVENT_TERMS,
    matches_aliases,
    classify_player_relevance,
    disambiguate_format,
    evaluate_candidate,
    explain_comment_temporal_grain,
    explain_historical_pagination_costs,
    run_pilot_discovery,
    format_report_summary_markdown,
)


# =============================================================================
# 1. Pilot Channel Registry Tests
# =============================================================================

def test_approved_pilot_channels_count_and_keys():
    """Verifies that the 7 pilot channels match repository configuration."""
    assert len(APPROVED_PILOT_CHANNELS) == 7
    expected_ids = {
        "UCNAf1k0yIjyGu3k9BwAg3lg",  # Sky Sports PL
        "UC_5kL-3t0fUzi8kHwRTY-zQ",  # Mark Goldbridge That's Football
        "UCqGqA2hV9e95F9Cj3k2M0gQ",  # TalkFCB
        "UCMmVPVb0BwSIOWVeDwlPocQ",  # The United Stand
        "UC-lHJZR3Gqxm24_Vd_AJ5Yw",  # IShowSpeed
        "UCbX6M_6E0iMzi1Z7H2Bv9KA",  # Statman Dave
        "UCiWLfSweyRNmLpgEHekhoAg",  # ESPN FC
    }
    actual_ids = {ch["channel_id"] for ch in APPROVED_PILOT_CHANNELS}
    assert actual_ids == expected_ids


def test_channel_strata_coverage():
    """Ensures diverse strata representation across pilot channels."""
    strata = {ch["stratum"] for ch in APPROVED_PILOT_CHANNELS}
    assert "TIER_1_BROADCAST" in strata
    assert "FAN_MEDIA_PUNDITRY" in strata
    assert "FAN_MEDIA_CLUB_SPECIFIC" in strata
    assert "REACTION_STREAMER" in strata
    assert "TACTICAL_ANALYTICS" in strata


# =============================================================================
# 2. Word Boundary Matching & Player Relevance
# =============================================================================

def test_word_boundary_matching():
    """Ensures alias matching respects word boundaries and ignores partial substrings."""
    assert matches_aliases("Great goal by CR7 tonight!", RONALDO_ALIASES) is True
    assert matches_aliases("Looking at the CR700 camera model", RONALDO_ALIASES) is False
    assert matches_aliases("Messi magic in Qatar", MESSI_ALIASES) is True
    assert matches_aliases("Messidor calendar month", MESSI_ALIASES) is False


def test_classify_player_relevance():
    """Tests Messi, Ronaldo, Dual, and None classifications."""
    assert classify_player_relevance("Lionel Messi masterclass") == "MESSI"
    assert classify_player_relevance("Cristiano Ronaldo scores penalty") == "RONALDO"
    assert classify_player_relevance("Messi vs Ronaldo: Who is the GOAT?") == "DUAL"
    assert classify_player_relevance("Kylian Mbappe hat-trick highlights") == "NONE"


# =============================================================================
# 3. Format Disambiguation Tests (Decision A.1)
# =============================================================================

def test_disambiguate_format_long_form():
    """Videos longer than 60s without vertical metadata are LONG_FORM."""
    assert disambiguate_format(duration_seconds=120) == "LONG_FORM"
    assert disambiguate_format(duration_seconds=600, title="Match Highlights") == "LONG_FORM"


def test_disambiguate_format_shorts_confirmed():
    """Shorts are confirmed only with vertical aspect ratio, hashtag, or URL resolution."""
    # Case 1: Vertical aspect ratio
    assert disambiguate_format(duration_seconds=45, aspect_ratio=0.56) == "SHORTS_CONFIRMED"
    # Case 2: Explicit #shorts hashtag + duration <= 60s
    assert disambiguate_format(duration_seconds=30, title="Insane skill #shorts") == "SHORTS_CONFIRMED"
    # Case 3: URL resolution confirmed
    assert disambiguate_format(duration_seconds=50, is_shorts_url_confirmed=True) == "SHORTS_CONFIRMED"


def test_disambiguate_format_uncertain():
    """Short videos (<= 60s) without vertical confirmation fail closed as UNCERTAIN_FORMAT."""
    # Under Decision A.1, cannot infer Shorts solely from duration
    assert disambiguate_format(duration_seconds=45, title="Quick match clip") == "UNCERTAIN_FORMAT"
    assert disambiguate_format(duration_seconds=59) == "UNCERTAIN_FORMAT"


# =============================================================================
# 4. Candidate Evaluation & Exclusion Tagging
# =============================================================================

def test_evaluate_candidate_eligible():
    """A video in window with player relevance and long-form format is ELIGIBLE."""
    video = {
        "id": "v1",
        "snippet": {
            "channelId": "UCNAf1k0yIjyGu3k9BwAg3lg",
            "channelTitle": "Sky Sports Premier League",
            "title": "Lionel Messi lifts World Cup",
            "publishedAt": "2022-12-18T19:00:00Z",
        },
        "duration_seconds": 600,
    }
    w_start = datetime.datetime(2022, 12, 18, 0, 0, tzinfo=datetime.timezone.utc)
    w_end = datetime.datetime(2022, 12, 20, 23, 59, tzinfo=datetime.timezone.utc)

    eval_res = evaluate_candidate(video, w_start, w_end, WC_2022_EVENT_TERMS)
    assert eval_res["status"] == "ELIGIBLE"
    assert eval_res["player_category"] == "MESSI"
    assert eval_res["exclusion_reason"] is None


def test_evaluate_candidate_out_of_window():
    """A video outside the date window is tagged OUT_OF_WINDOW."""
    video = {
        "id": "v2",
        "snippet": {
            "channelId": "UCiWLfSweyRNmLpgEHekhoAg",
            "title": "Ronaldo signs for Al Nassr",
            "publishedAt": "2023-01-03T12:00:00Z",
        },
        "duration_seconds": 300,
    }
    w_start = datetime.datetime(2022, 12, 18, 0, 0, tzinfo=datetime.timezone.utc)
    w_end = datetime.datetime(2022, 12, 20, 23, 59, tzinfo=datetime.timezone.utc)

    eval_res = evaluate_candidate(video, w_start, w_end, WC_2022_EVENT_TERMS)
    assert eval_res["status"] == "INELIGIBLE"
    assert eval_res["exclusion_reason"] == "OUT_OF_WINDOW"


def test_evaluate_candidate_no_relevance():
    """A video in window with neither player nor event term is tagged NO_TARGET_RELEVANCE."""
    video = {
        "id": "v3",
        "snippet": {
            "channelId": "UCNAf1k0yIjyGu3k9BwAg3lg",
            "title": "Premier League tactical corner review",
            "publishedAt": "2022-12-19T10:00:00Z",
        },
        "duration_seconds": 400,
    }
    w_start = datetime.datetime(2022, 12, 18, 0, 0, tzinfo=datetime.timezone.utc)
    w_end = datetime.datetime(2022, 12, 20, 23, 59, tzinfo=datetime.timezone.utc)

    eval_res = evaluate_candidate(video, w_start, w_end, WC_2022_EVENT_TERMS)
    assert eval_res["status"] == "INELIGIBLE"
    assert eval_res["exclusion_reason"] == "NO_TARGET_RELEVANCE"


def test_evaluate_candidate_uncertain_format_gated():
    """A video with ambiguous format fails closed from selection (Decision A.1)."""
    video = {
        "id": "v4",
        "snippet": {
            "channelId": "UC-lHJZR3Gqxm24_Vd_AJ5Yw",
            "title": "Speed reacts to Messi goal",
            "publishedAt": "2022-12-18T19:30:00Z",
        },
        "duration_seconds": 40,  # <=60s without vertical confirmation
    }
    w_start = datetime.datetime(2022, 12, 18, 0, 0, tzinfo=datetime.timezone.utc)
    w_end = datetime.datetime(2022, 12, 20, 23, 59, tzinfo=datetime.timezone.utc)

    eval_res = evaluate_candidate(video, w_start, w_end, WC_2022_EVENT_TERMS)
    assert eval_res["status"] == "INELIGIBLE"
    assert eval_res["exclusion_reason"] == "UNCERTAIN_FORMAT_GATED"


# =============================================================================
# 5. Temporal Grain & Comment Distinction Tests
# =============================================================================

def test_comment_temporal_grain_contemporaneous():
    """Comments within 30 days of video publication are contemporaneous."""
    analysis = explain_comment_temporal_grain(
        video_published_at_str="2022-12-18T20:00:00Z",
        comment_published_at_str="2022-12-20T10:00:00Z",
        contemporaneous_grace_period_days=30,
    )
    assert analysis["is_contemporaneous"] is True
    assert analysis["observation_type"] == "CONTEMPORANEOUS_REACTION"
    assert analysis["elapsed_days_post_video"] < 30.0


def test_comment_temporal_grain_retrospective():
    """Comments left years later on historical videos are tagged RETROSPECTIVE_OBSERVATION."""
    analysis = explain_comment_temporal_grain(
        video_published_at_str="2022-12-18T20:00:00Z",
        comment_published_at_str="2026-05-10T14:30:00Z",
        contemporaneous_grace_period_days=30,
    )
    assert analysis["is_contemporaneous"] is False
    assert analysis["observation_type"] == "RETROSPECTIVE_OBSERVATION"
    assert analysis["elapsed_days_post_video"] > 1000.0


# =============================================================================
# 6. Historical Pagination Cost Model Tests
# =============================================================================

def test_explain_historical_pagination_costs():
    """Verifies quota trade-offs between uploads playlist walk and date-bounded search."""
    costs = explain_historical_pagination_costs(
        target_event_year=2022,
        reference_date=datetime.date(2026, 10, 14),
    )
    pl_walk = costs["uploads_playlist_walk"]
    srch = costs["date_bounded_search"]

    # Traversal across ~1400 days requires hundreds of pages for high-frequency channels
    assert pl_walk["total_quota_units_needed"] > 500
    assert "QUOTA_PROHIBITIVE" in pl_walk["viability"]

    # Date-bounded search targets the exact 7-day window directly
    assert srch["total_quota_units_needed"] == 1400  # 7 channels * 2 pages * 100 units
    assert "RECOMMENDED" in srch["viability"]


# =============================================================================
# 7. End-to-End Discovery Report Generation
# =============================================================================

def test_run_pilot_discovery_end_to_end():
    """Tests end-to-end report generation with mock candidates."""
    from discover_pilot_coverage import get_mock_candidates_fixture

    w_start = datetime.datetime(2022, 12, 18, 0, 0, tzinfo=datetime.timezone.utc)
    w_end = datetime.datetime(2022, 12, 20, 23, 59, 59, tzinfo=datetime.timezone.utc)
    candidates = get_mock_candidates_fixture()

    report = run_pilot_discovery(
        channels=APPROVED_PILOT_CHANNELS,
        candidates=candidates,
        window_start=w_start,
        window_end=w_end,
        event_terms=WC_2022_EVENT_TERMS,
    )

    assert "report_metadata" in report
    assert "overall_summary" in report
    assert "channel_breakdown" in report
    assert report["overall_summary"]["total_eligible_candidates"] == 8
    assert len(report["channel_breakdown"]) == 7

    md_summary = format_report_summary_markdown(report)
    assert "Pilot Candidate Coverage Report" in md_summary
    assert "Sky Sports Premier League" in md_summary
    assert "ESPN FC" in md_summary
