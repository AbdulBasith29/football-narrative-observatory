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
    discover_channel_candidates_date_bounded_search,
    discover_channel_candidates_uploads_walk,
    evaluate_format_uncertainty,
    format_report_summary_markdown,
    hydrate_video_metadata_live,
    load_repository_configuration,
    matches_aliases,
    parse_iso8601_duration,
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
    assert "UCiWLfSweyRNmLpgEHekhoAg" in cids  # ESPN FC


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
    assert "Preserved" in discrepancies[0]["resolution"]


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

    assert res["completeness_status"] == "EXHAUSTIVE"
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
