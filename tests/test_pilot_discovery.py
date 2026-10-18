"""tests/test_pilot_discovery.py
Unit and regression tests for working pilot candidate discovery and coverage reporting.

Validates:
1. Loading repository configuration from YAML (channels, player aliases, event windows).
2. Word-boundary alias matching and focal player classification.
3. Format uncertainty evaluation under Decision A.1 (is_short = None/UNKNOWN for <=60s without vertical confirmation).
4. Telemetry tracking and official quota calculation.
5. Live mode client construction failing closed when YOUTUBE_API_KEY is missing.
6. Identity validation detecting channel title/handle discrepancies without dropping frame membership.
7. Paginated candidate discovery (uploads walk and search) with early stopping and truncation detection.
8. Metadata hydration batching and unavailable video handling via videos.list.
9. Explicit synthetic mock mode labeling all outputs SYNTHETIC_MOCK_DATA.
10. Markdown summary formatting.
"""

from __future__ import annotations

import datetime
import json
import os
import sys
from unittest.mock import MagicMock, patch
import pytest

# Ensure scripts/ is in pythonpath
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts")))

from discover_pilot_coverage import (
    OFFICIAL_QUOTA_COSTS,
    DiscoveryTelemetry,
    build_youtube_client,
    classify_focal_players,
    classify_hydrated_candidate,
    deduplicate_candidate_occurrences,
    discover_channel_candidates_date_bounded_search,
    discover_channel_candidates_uploads_walk,
    evaluate_format_uncertainty,
    extract_match_excerpt,
    find_matching_terms,
    format_report_summary_markdown,
    hydrate_video_metadata_live,
    load_repository_configuration,
    matches_aliases,
    parse_iso8601_duration,
    regenerate_from_saved_report,
    run_synthetic_mock_discovery,
    validate_channel_identities_live,
)


# =============================================================================
# 1. Repository Configuration Loader Tests
# =============================================================================

def test_load_repository_configuration():
    """Verifies loading real config files without hardcoding in Python dicts."""
    config = load_repository_configuration()
    assert "channels" in config
    assert len(config["channels"]) == 7
    assert "player_aliases" in config
    assert "Lionel Messi" in config["player_aliases"]
    assert "Cristiano Ronaldo" in config["player_aliases"]
    assert "events" in config
    assert len(config["events"]) >= 1

    # Invariant: channels must contain the 7 canonical pilot channels
    cids = {ch["channel_id"] for ch in config["channels"]}
    assert "UCNAf1k0yIjyGu3k9BwAg3lg" in cids  # Sky Sports PL
    assert "UC6c1z7bA__85CIWZ_jpCK-Q" in cids  # ESPN FC (verified ID)


# =============================================================================
# 2. Helpers: Duration Parsing & Word Boundaries
# =============================================================================

def test_parse_iso8601_duration():
    """Parses standard ISO 8601 duration strings."""
    assert parse_iso8601_duration("PT15M33S") == 933
    assert parse_iso8601_duration("PT45S") == 45
    assert parse_iso8601_duration("PT1H2M10S") == 3730
    assert parse_iso8601_duration("P1DT2H") == 93600
    assert parse_iso8601_duration("") == 0
    assert parse_iso8601_duration(None) == 0


def test_matches_aliases_word_boundaries():
    """Ensures alias matching respects word boundaries."""
    aliases = ["cr7", "ronaldo"]
    assert matches_aliases("Superb freekick by CR7!", aliases) is True
    assert matches_aliases("Checking out the CR700 model", aliases) is False
    assert matches_aliases("Ronaldo with a header", aliases) is True
    assert matches_aliases("Ronaldov match summary", aliases) is False


def test_classify_focal_players():
    """Tests player classification across MESSI, RONALDO, DUAL, and NONE."""
    aliases = {
        "Lionel Messi": ["messi", "leo messi", "lionel messi"],
        "Cristiano Ronaldo": ["ronaldo", "cristiano", "cr7"],
    }
    assert classify_focal_players("Messi hat trick", "", aliases) == "MESSI"
    assert classify_focal_players("Cristiano penalty", "", aliases) == "RONALDO"
    assert classify_focal_players("Messi vs Ronaldo debate", "", aliases) == "DUAL"
    assert classify_focal_players("Haaland goals", "Manchester City", aliases) == "NONE"


# =============================================================================
# 3. Format Disambiguation under Decision A.1
# =============================================================================

def test_evaluate_format_uncertainty():
    """Decision A.1: status cannot be inferred from duration alone."""
    # Long form (>60s)
    lbl, is_short = evaluate_format_uncertainty(duration_seconds=120)
    assert lbl == "LONG_FORM"
    assert is_short is False

    # Short confirmed via vertical aspect ratio (<1.0)
    lbl, is_short = evaluate_format_uncertainty(duration_seconds=45, aspect_ratio=0.56)
    assert lbl == "SHORTS_CONFIRMED"
    assert is_short is True

    # Short confirmed via #shorts tag
    lbl, is_short = evaluate_format_uncertainty(duration_seconds=45, title="Skill moves #shorts")
    assert lbl == "SHORTS_CONFIRMED"
    assert is_short is True

    # Ambiguous: <=60s without vertical confirmation or hashtag -> UNKNOWN (is_short = None)
    lbl, is_short = evaluate_format_uncertainty(duration_seconds=45, title="Short highlight clip")
    assert lbl == "FORMAT_UNKNOWN"
    assert is_short is None


# =============================================================================
# 4. Telemetry & Official Quota Calculation
# =============================================================================

def test_telemetry_tracking_and_quota_costs():
    """Validates quota cost computation against official rates."""
    telem = DiscoveryTelemetry()
    telem.record_request("channels.list")
    telem.record_request("playlistItems.list")
    telem.record_request("playlistItems.list")
    telem.record_request("videos.list")
    telem.record_request("search.list")

    # channels.list (1) + 2 * playlistItems.list (2) + videos.list (1) + search.list (100) = 104
    assert telem.calculate_actual_quota_units() == 104
    assert telem.total_requests() == 5
    d = telem.to_dict()
    assert d["total_api_requests"] == 5
    assert d["actual_quota_units_consumed"] == 104


# =============================================================================
# 5. Live Client Construction Fail-Closed
# =============================================================================

def test_build_youtube_client_fail_closed_when_key_missing():
    """Ensures live client raises clear RuntimeError if no API key is in environment."""
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(RuntimeError, match="YOUTUBE_API_KEY environment variable is missing"):
            build_youtube_client(api_key=None)


# =============================================================================
# 6. Channel Identity Validation & Discrepancy Reporting
# =============================================================================

def test_validate_channel_identities_live_detects_mismatches():
    """Verifies that identity differences are detected and reported without mutating frame."""
    mock_youtube = MagicMock()
    mock_req = MagicMock()
    mock_youtube.channels().list.return_value = mock_req
    mock_req.execute.return_value = {
        "items": [
            {
                "id": "UCNAf1k0yIjyGu3k9BwAg3lg",
                "snippet": {"title": "Sky Sports Premier League", "customUrl": "@SkySportsPL"},
                "contentDetails": {"relatedPlaylists": {"uploads": "UUNAf1k0yIjyGu3k9BwAg3lg"}},
            },
            {
                "id": "UCiWLfSweyRNmLpgEHekhoAg",
                "snippet": {"title": "ESPN", "customUrl": "@espn"},  # Live title differs from 'ESPN FC'
                "contentDetails": {"relatedPlaylists": {"uploads": "UUiWLfSweyRNmLpgEHekhoAg"}},
            },
        ]
    }

    configured = [
        {"channel_id": "UCNAf1k0yIjyGu3k9BwAg3lg", "channel_name": "Sky Sports Premier League"},
        {"channel_id": "UCiWLfSweyRNmLpgEHekhoAg", "channel_name": "ESPN FC"},
        {"channel_id": "UC_missing", "channel_name": "Missing Channel"},
    ]

    telem = DiscoveryTelemetry()
    validated, discrepancies = validate_channel_identities_live(mock_youtube, configured, telem)

    assert "UCNAf1k0yIjyGu3k9BwAg3lg" in validated
    assert validated["UCNAf1k0yIjyGu3k9BwAg3lg"]["uploads_playlist_id"] == "UUNAf1k0yIjyGu3k9BwAg3lg"

    # TITLE_MISMATCH on ESPN FC
    assert any(d["channel_id"] == "UCiWLfSweyRNmLpgEHekhoAg" and d["discrepancy_type"] == "TITLE_MISMATCH" for d in discrepancies)
    # CHANNEL_NOT_FOUND on UC_missing
    assert any(d["channel_id"] == "UC_missing" and d["discrepancy_type"] == "CHANNEL_NOT_FOUND" for d in discrepancies)
    # Discrepancy resolution must preserve membership
    assert "preserved" in discrepancies[0]["resolution"].lower()


# =============================================================================
# 7. Paginated Candidate Discovery
# =============================================================================

def test_discover_channel_candidates_uploads_walk_early_stopping():
    """Tests reverse-chronological playlist walk early stopping when passing window start."""
    mock_youtube = MagicMock()
    mock_req = MagicMock()
    mock_youtube.playlistItems().list.return_value = mock_req

    # 3 items: 1 in window, 1 in window, 1 before window (triggers early stop)
    mock_req.execute.return_value = {
        "items": [
            {
                "snippet": {"title": "Final Reaction", "publishedAt": "2022-12-18T20:00:00Z"},
                "contentDetails": {"videoId": "v1", "videoPublishedAt": "2022-12-18T20:00:00Z"},
            },
            {
                "snippet": {"title": "Post Match Analysis", "publishedAt": "2022-12-18T18:00:00Z"},
                "contentDetails": {"videoId": "v2", "videoPublishedAt": "2022-12-18T18:00:00Z"},
            },
            {
                "snippet": {"title": "Older video", "publishedAt": "2022-12-10T12:00:00Z"},  # Older than window start
                "contentDetails": {"videoId": "v3", "videoPublishedAt": "2022-12-10T12:00:00Z"},
            },
        ],
        "nextPageToken": "token_page_2",
    }

    ch_info = {"channel_id": "UC_TEST", "uploads_playlist_id": "UU_TEST"}
    w_start = datetime.datetime(2022, 12, 18, 0, 0, tzinfo=datetime.timezone.utc)
    w_end = datetime.datetime(2022, 12, 20, 23, 59, tzinfo=datetime.timezone.utc)
    telem = DiscoveryTelemetry()

    res = discover_channel_candidates_uploads_walk(
        youtube=mock_youtube,
        channel_info=ch_info,
        window_start=w_start,
        window_end=w_end,
        max_pages=5,
        telemetry=telem,
    )

    assert res["completeness_status"] == "RETURNED_PAGES_EXHAUSTED"
    assert len(res["items"]) == 2
    assert res["is_truncated"] is False
    assert res["pages_fetched"] == 1


def test_discover_channel_candidates_truncation_detection():
    """Detects truncation when pagination hits max_pages limit while nextPageToken remains."""
    mock_youtube = MagicMock()
    mock_req = MagicMock()
    mock_youtube.playlistItems().list.return_value = mock_req

    mock_req.execute.return_value = {
        "items": [
            {
                "snippet": {"title": "Recent Video", "publishedAt": "2026-01-01T10:00:00Z"},
                "contentDetails": {"videoId": "v_recent", "videoPublishedAt": "2026-01-01T10:00:00Z"},
            }
        ],
        "nextPageToken": "next_page_exists",
    }

    ch_info = {"channel_id": "UC_TEST", "uploads_playlist_id": "UU_TEST"}
    w_start = datetime.datetime(2022, 12, 18, 0, 0, tzinfo=datetime.timezone.utc)
    w_end = datetime.datetime(2022, 12, 20, 23, 59, tzinfo=datetime.timezone.utc)
    telem = DiscoveryTelemetry()

    res = discover_channel_candidates_uploads_walk(
        youtube=mock_youtube,
        channel_info=ch_info,
        window_start=w_start,
        window_end=w_end,
        max_pages=2,
        telemetry=telem,
    )

    assert res["is_truncated"] is True
    assert res["completeness_status"] == "TRUNCATED_AT_PAGE_LIMIT"
    assert res["pages_fetched"] == 2


# =============================================================================
# 8. Metadata Hydration via videos.list
# =============================================================================

def test_hydrate_video_metadata_live_batching():
    """Verifies that videos are batched into chunks of up to 50 and hydrated."""
    mock_youtube = MagicMock()
    mock_req = MagicMock()
    mock_youtube.videos().list.return_value = mock_req

    mock_req.execute.return_value = {
        "items": [
            {
                "id": "v1",
                "snippet": {
                    "title": "Messi Trophy Lift",
                    "description": "Qatar 2022 final",
                    "publishedAt": "2022-12-18T19:00:00Z",
                    "channelId": "UCNAf1k0yIjyGu3k9BwAg3lg",
                    "channelTitle": "Sky Sports Premier League",
                    "defaultAudioLanguage": "en",
                },
                "contentDetails": {"duration": "PT12M30S"},
                "statistics": {"viewCount": "100000", "commentCount": "500"},
            }
        ]
    }

    telem = DiscoveryTelemetry()
    hydrated = hydrate_video_metadata_live(mock_youtube, ["v1", "v_deleted"], telem)

    assert "v1" in hydrated
    assert hydrated["v1"]["duration_seconds"] == 750
    assert hydrated["v1"]["is_available"] is True
    assert hydrated["v1"]["primary_language"] == "en"

    # Missing video detected and flagged UNAVAILABLE
    assert "v_deleted" in hydrated
    assert hydrated["v_deleted"]["is_available"] is False
    assert hydrated["v_deleted"]["exclusion_reason"] == "VIDEO_UNAVAILABLE"


# =============================================================================
# 9. Candidate Classification against Methodology
# =============================================================================

def test_classify_hydrated_candidate():
    """Verifies classification against frozen rules without adding unsupported exclusions."""
    w_start = datetime.datetime(2022, 12, 18, 0, 0, tzinfo=datetime.timezone.utc)
    w_end = datetime.datetime(2022, 12, 20, 23, 59, tzinfo=datetime.timezone.utc)
    aliases = {"Lionel Messi": ["messi", "leo messi"]}
    event_terms = ["world cup", "final"]

    # 1. Eligible long-form video
    cand_1 = {
        "video_id": "v1",
        "title": "Messi wins the World Cup final",
        "description": "Full reaction",
        "published_at": "2022-12-18T20:00:00Z",
        "duration_seconds": 600,
        "is_available": True,
    }
    res_1 = classify_hydrated_candidate(cand_1, w_start, w_end, aliases, event_terms)
    assert res_1["status"] == "ELIGIBLE"
    assert res_1["player_relevance"] == "MESSI"
    assert res_1["is_short"] is False

    # 2. Eligible short video with UNKNOWN format (<=60s) -> PRESERVED without exclusion!
    cand_2 = {
        "video_id": "v2",
        "title": "Messi quick goal celebration",
        "description": "",
        "published_at": "2022-12-18T20:00:00Z",
        "duration_seconds": 45,  # <=60s without vertical confirmation
        "is_available": True,
    }
    res_2 = classify_hydrated_candidate(cand_2, w_start, w_end, aliases, event_terms)
    assert res_2["status"] == "ELIGIBLE"
    assert res_2["is_short"] is None  # Uncertainty preserved under Decision A.1
    assert res_2["format_label"] == "FORMAT_UNKNOWN"

    # 3. Out of window
    cand_3 = {
        "video_id": "v3",
        "title": "Messi match",
        "published_at": "2023-01-15T10:00:00Z",
        "duration_seconds": 500,
        "is_available": True,
    }
    res_3 = classify_hydrated_candidate(cand_3, w_start, w_end, aliases, event_terms)
    assert res_3["status"] == "INELIGIBLE"
    assert res_3["exclusion_reason"] == "OUT_OF_WINDOW"


def test_relevance_tiers_separation():
    """Validates separation of PLAYER_MATCH, EVENT_SPECIFIC, BROAD_EVENT_UNCERTAIN, and NO_MATCH."""
    w_start = datetime.datetime(2022, 12, 18, 0, 0, tzinfo=datetime.timezone.utc)
    w_end = datetime.datetime(2022, 12, 20, 23, 59, tzinfo=datetime.timezone.utc)
    aliases = {
        "Lionel Messi": ["messi", "leo messi"],
        "Cristiano Ronaldo": ["ronaldo", "cristiano", "cr7"],
    }
    event_terms = ["world cup", "final", "argentina vs france"]

    # 1. Focal player match (Messi)
    messi_cand = {
        "video_id": "v_messi",
        "title": "Messi lifts the World Cup trophy",
        "description": "Argentina win 2022 World Cup",
        "published_at": "2022-12-18T20:00:00Z",
        "duration_seconds": 600,
        "is_available": True,
    }
    res_messi = classify_hydrated_candidate(messi_cand, w_start, w_end, aliases, event_terms)
    assert res_messi["status"] == "ELIGIBLE"
    assert res_messi["relevance_tier"] == "PLAYER_MATCH"
    assert res_messi["player_relevance"] == "MESSI"

    # 2. Event-specific without focal player (Ten Hag / Haaland case)
    event_cand = {
        "video_id": "v_event",
        "title": "Haaland reaction to tournament",
        "description": "Erling Haaland interview following the World Cup",
        "published_at": "2022-12-19T10:00:00Z",
        "duration_seconds": 300,
        "is_available": True,
    }
    res_event = classify_hydrated_candidate(event_cand, w_start, w_end, aliases, event_terms)
    assert res_event["status"] == "ELIGIBLE"
    assert res_event["relevance_tier"] == "EVENT_SPECIFIC"
    assert res_event["player_relevance"] == "NONE"

    # 3. Broad generic term only (LeBron dunk case: matched only "final" in description)
    lebron_cand = {
        "video_id": "v_lebron",
        "title": "LeBron puts Lakers ahead with dunk",
        "description": "LeBron James thunderous dunk in the final 30 seconds of play",
        "published_at": "2022-12-19T04:00:00Z",
        "duration_seconds": 90,
        "is_available": True,
    }
    res_lebron = classify_hydrated_candidate(lebron_cand, w_start, w_end, aliases, event_terms)
    assert res_lebron["status"] == "ELIGIBLE"
    assert res_lebron["relevance_tier"] == "BROAD_EVENT_UNCERTAIN"
    assert res_lebron["matched_event_terms"] == ["final"]
    assert res_lebron["matched_player_aliases"] == []

    # 4. Completely unrelated
    unrelated_cand = {
        "video_id": "v_unrelated",
        "title": "Cooking pasta recipe",
        "description": "How to make bolognese",
        "published_at": "2022-12-19T12:00:00Z",
        "duration_seconds": 400,
        "is_available": True,
    }
    res_unrelated = classify_hydrated_candidate(unrelated_cand, w_start, w_end, aliases, event_terms)
    assert res_unrelated["status"] == "INELIGIBLE"
    assert res_unrelated["relevance_tier"] == "NO_MATCH"
    assert res_unrelated["exclusion_reason"] == "NO_TARGET_RELEVANCE"


def test_extract_match_excerpt_and_matched_field():
    """Verifies that contextual excerpts and matched fields are accurately identified."""
    w_start = datetime.datetime(2022, 12, 18, 0, 0, tzinfo=datetime.timezone.utc)
    w_end = datetime.datetime(2022, 12, 20, 23, 59, tzinfo=datetime.timezone.utc)
    aliases = {"Lionel Messi": ["messi"]}
    event_terms = ["world cup"]

    # Match in title only
    cand_title = {
        "video_id": "vt",
        "title": "Incredible goal from Messi today",
        "description": "Just another great evening of football",
        "published_at": "2022-12-18T18:00:00Z",
        "is_available": True,
    }
    res_title = classify_hydrated_candidate(cand_title, w_start, w_end, aliases, event_terms)
    assert res_title["matched_field"] == "title"
    assert "Messi" in res_title["match_excerpt"]

    # Match in description only
    cand_desc = {
        "video_id": "vd",
        "title": "Post Match Press Conference",
        "description": "Highlights from the world cup final match",
        "published_at": "2022-12-18T18:00:00Z",
        "is_available": True,
    }
    res_desc = classify_hydrated_candidate(cand_desc, w_start, w_end, aliases, event_terms)
    assert res_desc["matched_field"] == "description"
    assert "world cup" in res_desc["match_excerpt"]

    # Match in both title and description
    cand_both = {
        "video_id": "vb",
        "title": "Messi wins the world cup",
        "description": "Full analysis of Messi and the squad",
        "published_at": "2022-12-18T18:00:00Z",
        "is_available": True,
    }
    res_both = classify_hydrated_candidate(cand_both, w_start, w_end, aliases, event_terms)
    assert res_both["matched_field"] == "both"


def test_unresolved_channel_identity_gated_from_eligible():
    """Unresolved channel identities must be set INELIGIBLE and gated from totals."""
    w_start = datetime.datetime(2022, 12, 18, 0, 0, tzinfo=datetime.timezone.utc)
    w_end = datetime.datetime(2022, 12, 20, 23, 59, tzinfo=datetime.timezone.utc)
    aliases = {"Lionel Messi": ["messi"]}
    event_terms = ["world cup"]

    cand = {
        "video_id": "v_unresolved",
        "channel_id": "UC_unresolved",
        "title": "Messi brilliant goal",
        "description": "World Cup highlight",
        "published_at": "2022-12-18T18:00:00Z",
        "is_available": True,
    }
    res = classify_hydrated_candidate(
        cand, w_start, w_end, aliases, event_terms, channel_is_validated=False
    )
    assert res["status"] == "INELIGIBLE"
    assert res["exclusion_reason"] == "UNRESOLVED_CHANNEL_IDENTITY"
    assert res["relevance_tier"] == "UNRESOLVED_CHANNEL"


# =============================================================================
# 10. Synthetic Mock Mode Labeling & Markdown Summary
# =============================================================================

def test_run_synthetic_mock_discovery_labels():
    """Ensures mock discovery outputs are prominently stamped SYNTHETIC."""
    config = load_repository_configuration()
    w_start = datetime.datetime(2022, 12, 18, 0, 0, tzinfo=datetime.timezone.utc)
    w_end = datetime.datetime(2022, 12, 20, 23, 59, tzinfo=datetime.timezone.utc)

    report = run_synthetic_mock_discovery(config, w_start, w_end, ["world cup", "final"])
    assert report["report_metadata"]["execution_mode"] == "SYNTHETIC_MOCK"
    assert report["report_metadata"]["data_provenance"] == "SYNTHETIC_FIXTURE_DATA_NOT_LIVE_API"
    assert "WARNING" in report["report_metadata"]["warning"]

    md = format_report_summary_markdown(report)
    assert "WARNING: SYNTHETIC DATA - NOT LIVE YOUTUBE API OBSERVATIONS" in md
    assert "SYNTHETIC_MOCK" in md


# =============================================================================
# 11. Deduplication, Baseline Term Separation & Report Regeneration Tests
# =============================================================================

def test_deduplicate_candidate_occurrences_overlapping_pages():
    """Ensures multi-page candidate occurrences are deduplicated by video_id with full provenance."""
    raw_candidates = [
        {
            "video_id": "vid_overlap_1",
            "channel_id": "UC_test",
            "channel_title": "Test Channel",
            "title": "Video 1 on Page 1",
            "page_number": 1,
            "retrieval_index": 1,
            "discovery_method": "DATE_BOUNDED_SEARCH",
            "published_at": "2022-12-18T18:00:00Z",
        },
        {
            "video_id": "vid_unique_2",
            "channel_id": "UC_test",
            "channel_title": "Test Channel",
            "title": "Video 2 on Page 1",
            "page_number": 1,
            "retrieval_index": 2,
            "discovery_method": "DATE_BOUNDED_SEARCH",
            "published_at": "2022-12-18T19:00:00Z",
        },
        {
            "video_id": "vid_overlap_1",
            "channel_id": "UC_test",
            "channel_title": "Test Channel",
            "title": "Video 1 on Page 2 (offset overlap)",
            "page_number": 2,
            "retrieval_index": 3,
            "discovery_method": "DATE_BOUNDED_SEARCH",
            "published_at": "2022-12-18T18:00:00Z",
        },
    ]

    unique_cands, telem = deduplicate_candidate_occurrences(raw_candidates, default_method="DATE_BOUNDED_SEARCH")

    assert telem["total_raw_occurrences"] == 3
    assert telem["total_unique_candidates"] == 2
    assert telem["total_duplicate_occurrences_collapsed"] == 1
    assert telem["duplicate_video_ids"] == ["vid_overlap_1"]

    # Verify occurrence tracking on collapsed video
    overlap_cand = next(c for c in unique_cands if c["video_id"] == "vid_overlap_1")
    assert overlap_cand["occurrence_count"] == 2
    assert len(overlap_cand["discovery_occurrences"]) == 2
    assert overlap_cand["discovery_occurrences"][0]["page_number"] == 1
    assert overlap_cand["discovery_occurrences"][1]["page_number"] == 2


def test_classify_hydrated_candidate_baseline_only():
    """Baseline-only matches (e.g. FIFA 23 career mode) remain ELIGIBLE under frozen rules,

    with proposed methodology action PROPOSE_EXCLUSION_NON_EVENT_BASELINE recorded separately.
    """
    w_start = datetime.datetime(2022, 12, 18, 0, 0, tzinfo=datetime.timezone.utc)
    w_end = datetime.datetime(2022, 12, 20, 23, 59, tzinfo=datetime.timezone.utc)
    aliases = {"Lionel Messi": ["messi"], "Cristiano Ronaldo": ["ronaldo"]}
    event_terms = ["world cup", "final"]
    baseline_terms = ["world cup", "fifa", "qatar 2022"]

    cand = {
        "video_id": "gaming_01",
        "channel_id": "UC_gaming",
        "channel_title": "Overlap Daily",
        "title": "FIFA 23 Create a Club Career Mode! Episode 27",
        "description": "Playing career mode on the latest fifa game update.",
        "published_at": "2022-12-19T12:00:00Z",
        "is_available": True,
    }

    res = classify_hydrated_candidate(
        candidate=cand,
        window_start=w_start,
        window_end=w_end,
        player_aliases=aliases,
        event_terms=event_terms,
        baseline_terms=baseline_terms,
        channel_is_validated=True,
    )

    # Invariant: Frozen methodology eligibility preserved
    assert res["status"] == "ELIGIBLE"
    assert res["relevance_tier"] == "BASELINE_ONLY"
    assert res["is_baseline_only"] is True
    assert res["matched_baseline_terms"] == ["fifa"]
    assert res["matched_player_aliases"] == []
    assert res["matched_event_terms"] == []
    # Proposed action recorded separately
    assert res["proposed_methodology_action"] == "PROPOSE_EXCLUSION_NON_EVENT_BASELINE"
    assert "gaming/unrelated media" in res["audit_note"]


def test_classify_hydrated_candidate_implicit_debate():
    """In-window videos without explicit keywords remain INELIGIBLE under frozen rules,

    but implicit GOAT debate content is flagged with PROPOSE_INCLUSION_IMPLICIT_DEBATE.
    """
    w_start = datetime.datetime(2022, 12, 18, 0, 0, tzinfo=datetime.timezone.utc)
    w_end = datetime.datetime(2022, 12, 20, 23, 59, tzinfo=datetime.timezone.utc)
    aliases = {"Lionel Messi": ["messi"], "Cristiano Ronaldo": ["ronaldo"]}
    event_terms = ["world cup", "final"]
    baseline_terms = ["world cup", "fifa", "qatar 2022"]

    cand = {
        "video_id": "goat_shorts_01",
        "channel_id": "UC_espn",
        "channel_title": "ESPN FC",
        "title": "The GOAT debate is officially over 🤯 🐐 #shorts",
        "description": "Post-match reactions and legacy debate.",
        "published_at": "2022-12-18T22:30:00Z",
        "is_available": True,
    }

    res = classify_hydrated_candidate(
        candidate=cand,
        window_start=w_start,
        window_end=w_end,
        player_aliases=aliases,
        event_terms=event_terms,
        baseline_terms=baseline_terms,
        channel_is_validated=True,
    )

    # Invariant: Frozen methodology exclusion preserved
    assert res["status"] == "INELIGIBLE"
    assert res["exclusion_reason"] == "NO_TARGET_RELEVANCE"
    assert res["relevance_tier"] == "NO_MATCH"
    # Proposed action recorded separately for human review
    assert res["proposed_methodology_action"] == "PROPOSE_INCLUSION_IMPLICIT_DEBATE"
    assert "GOAT debate" in res["audit_note"]


def test_regenerate_from_saved_report_counts():
    """Replays the saved live candidate coverage report offline (zero quota) and validates exact deduplicated counts."""
    saved_report_path = os.path.join(
        os.path.dirname(__file__), "..", "reports", "live_pilot_candidate_coverage_report_v2.json"
    )
    if not os.path.exists(saved_report_path):
        pytest.skip("Saved live report fixture not available for replay test.")

    report = regenerate_from_saved_report(saved_report_path)

    # Telemetry and deduplication assertions
    dedup = report["deduplication_summary"]
    assert dedup["total_raw_occurrences"] == 71
    assert dedup["total_unique_candidates"] == 68
    assert dedup["total_duplicate_occurrences_collapsed"] == 3
    assert set(dedup["duplicate_video_ids"]) == {"8PqQzR45AZc", "n-K3ajSqUY8", "VIeV_cAw5Dg"}

    # Overall summary counts derived from unique candidates
    ov = report["overall_summary"]
    assert ov["total_raw_occurrences_discovered"] == 71
    assert ov["total_unique_candidates_evaluated"] == 68
    assert ov["total_duplicate_occurrences_collapsed"] == 3
    assert ov["total_eligible_candidates"] == 45
    assert ov["player_focal_candidates"] == 16
    assert ov["event_specific_candidates"] == 26
    assert ov["baseline_only_candidates"] == 2
    assert ov["broad_generic_uncertain_candidates"] == 1

    # Uncertain inclusions & exclusions audit tables
    inc_ids = {u["video_id"] for u in report.get("uncertain_inclusions", [])}
    assert "gZXC0n__OH4" in inc_ids
    assert "XYw10VvtpJE" in inc_ids
    assert "TQ_CIbXDxHY" in inc_ids

    exc_ids = {u["video_id"] for u in report.get("uncertain_exclusions", [])}
    assert "I8JUK83KpuI" in exc_ids


def test_regenerate_from_saved_report_synthetic(tmp_path):
    """Verifies that regenerate_from_saved_report accurately deduplicates and classifies arbitrary reports."""
    mini_report = {
        "report_metadata": {"execution_mode": "LIVE_YOUTUBE_API"},
        "evaluated_candidates": [
            {
                "video_id": "vid_1",
                "channel_id": "UC6c1z7bA__85CIWZ_jpCK-Q",
                "channel_title": "ESPN FC",
                "title": "Match discussion",
                "description": "world cup final",
                "published_at": "2022-12-18T20:00:00Z",
                "is_available": True,
                "duration_seconds": 600,
                "view_count": "1000",
                "comment_count": "10",
                "page_number": 1,
                "retrieval_index": 1,
                "discovery_method": "DATE_BOUNDED_SEARCH",
            },
            {
                "video_id": "vid_2",
                "channel_id": "UCFxG3MxHC1ogUglvJeMHmVQ",
                "channel_title": "Overlap Daily",
                "title": "FIFA 23 Career Mode Episode 1",
                "description": "gaming",
                "published_at": "2022-12-19T10:00:00Z",
                "is_available": True,
                "duration_seconds": 1200,
                "view_count": "500",
                "comment_count": "5",
                "page_number": 1,
                "retrieval_index": 2,
                "discovery_method": "DATE_BOUNDED_SEARCH",
            },
            {
                "video_id": "vid_3",
                "channel_id": "UC6c1z7bA__85CIWZ_jpCK-Q",
                "channel_title": "ESPN FC",
                "title": "The GOAT debate is officially over 🤯 🐐 #shorts",
                "description": "discussion",
                "published_at": "2022-12-18T21:00:00Z",
                "is_available": True,
                "duration_seconds": 45,
                "view_count": "5000",
                "comment_count": "50",
                "page_number": 1,
                "retrieval_index": 3,
                "discovery_method": "DATE_BOUNDED_SEARCH",
            },
            {
                "video_id": "vid_1",
                "channel_id": "UC6c1z7bA__85CIWZ_jpCK-Q",
                "channel_title": "ESPN FC",
                "title": "Match discussion (page 2 overlap)",
                "description": "world cup final",
                "published_at": "2022-12-18T20:00:00Z",
                "is_available": True,
                "duration_seconds": 600,
                "view_count": "1000",
                "comment_count": "10",
                "page_number": 2,
                "retrieval_index": 4,
                "discovery_method": "DATE_BOUNDED_SEARCH",
            },
        ],
        "channel_identity_validations": [
            {"channel_id": "UC6c1z7bA__85CIWZ_jpCK-Q", "status": "VALIDATED", "live_title": "ESPN FC"},
            {"channel_id": "UCFxG3MxHC1ogUglvJeMHmVQ", "status": "VALIDATED", "live_title": "Overlap Daily"},
        ],
        "channel_breakdown": {
            "UC6c1z7bA__85CIWZ_jpCK-Q": {"channel_id": "UC6c1z7bA__85CIWZ_jpCK-Q", "pages_fetched": 2, "completeness_status": "RETURNED_PAGES_EXHAUSTED", "is_truncated": False, "discovery_method": "DATE_BOUNDED_SEARCH"},
            "UCFxG3MxHC1ogUglvJeMHmVQ": {"channel_id": "UCFxG3MxHC1ogUglvJeMHmVQ", "pages_fetched": 1, "completeness_status": "RETURNED_PAGES_EXHAUSTED", "is_truncated": False, "discovery_method": "DATE_BOUNDED_SEARCH"},
        },
        "telemetry": {"total_api_requests": 11, "actual_quota_units_consumed": 803, "requests_by_endpoint": {}},
    }
    report_file = tmp_path / "saved_report.json"
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(mini_report, f)

    report = regenerate_from_saved_report(str(report_file))
    assert report["deduplication_summary"]["total_raw_occurrences"] == 4
    assert report["deduplication_summary"]["total_unique_candidates"] == 3
    assert report["deduplication_summary"]["total_duplicate_occurrences_collapsed"] == 1
    assert report["deduplication_summary"]["duplicate_video_ids"] == ["vid_1"]
    assert report["overall_summary"]["total_eligible_candidates"] == 2
    assert report["overall_summary"]["baseline_only_candidates"] == 1
    assert len(report["uncertain_inclusions"]) == 1
    assert len(report["uncertain_exclusions"]) == 1


