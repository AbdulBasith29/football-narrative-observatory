import json
import uuid
from datetime import datetime, timezone
import pytest
from unittest.mock import MagicMock, patch

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))

from ingestion_video_metadata import (
    parse_iso8601_duration,
    chunk_video_ids,
    is_quota_or_rate_limit_error,
    persist_raw_video_response,
    process_video_metadata_payload,
    replay_raw_video_responses,
    ingest_video_metadata,
)
from cohort_selection import (
    repair_legacy_uploads_frame_lineage,
    evaluate_video_eligibility,
    rank_and_select_cohorts,
)


# 1. ISO 8601 Duration Parser Tests
def test_parse_iso8601_duration():
    assert parse_iso8601_duration("PT58S") == 58
    assert parse_iso8601_duration("PT1M15S") == 75
    assert parse_iso8601_duration("PT1H2M10S") == 3730
    assert parse_iso8601_duration("P1D") == 86400
    assert parse_iso8601_duration("") == 0
    assert parse_iso8601_duration(None) == 0
    assert parse_iso8601_duration("INVALID") == 0


# 2. Batch Chunking Tests (Conservative 50 Batching)
def test_chunk_video_ids_exact_50_limit():
    raw_ids = [f"vid_{i:03d}" for i in range(135)]
    batches = list(chunk_video_ids(raw_ids, chunk_size=50))
    assert len(batches) == 3
    assert len(batches[0]) == 50
    assert len(batches[1]) == 50
    assert len(batches[2]) == 35
    # Verify deterministic sorting
    assert batches[0][0] == "vid_000"
    assert batches[0][-1] == "vid_049"


# 3. videos.list Contract: Must OMIT maxResults with id parameter
def test_videos_list_request_contract_omits_maxresults():
    mock_youtube = MagicMock()
    mock_videos = MagicMock()
    mock_list = MagicMock()
    mock_youtube.videos.return_value = mock_videos
    mock_videos.list.return_value = mock_list
    mock_list.execute.return_value = {"items": []}

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    # Candidate query returns 2 IDs
    mock_cursor.fetchall.return_value = [("vid_001",), ("vid_002",)]

    with patch("ingestion_video_metadata.get_snowflake_connection", return_value=mock_conn):
        ingest_video_metadata(
            target_db="FOOTBALL_NARRATIVE_TEST",
            run_purpose="INTEGRATION_TEST",
            frame_version_key="test_frame_123",
            youtube_client=mock_youtube
        )

    # Verify videos.list call kwargs
    assert mock_videos.list.called
    call_kwargs = mock_videos.list.call_args[1]
    assert "id" in call_kwargs
    assert call_kwargs["part"] == "snippet,contentDetails,statistics,status"
    assert "maxResults" not in call_kwargs, "maxResults must NOT be passed to videos.list when id is specified"


# 4. Raw-Before-Parse Persistence
def test_raw_before_parse_persistence():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    raw_payload = {
        "items": [
            {
                "id": "vid_001",
                "snippet": {"title": "Test Title", "publishedAt": "2022-12-18T15:00:00Z"},
                "contentDetails": {"duration": "PT10M"},
                "statistics": {"viewCount": "1000", "likeCount": "50"}
            }
        ]
    }
    raw_resp_id = persist_raw_video_response(
        mock_conn, raw_payload, ["vid_001"], "run_123", "req_123",
        "2022-12-18T15:00:00Z", "2022-12-18T15:00:01Z", http_status=200
    )
    assert raw_resp_id is not None
    assert mock_conn.commit.called
    
    # Verify RAW.YOUTUBE_API_RESPONSE insert executed
    exec_statements = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert any("INSERT INTO RAW.YOUTUBE_API_RESPONSE" in stmt for stmt in exec_statements)
    assert any("INSERT INTO OPS.FACT_API_REQUEST" in stmt for stmt in exec_statements)


# 5. Missing Videos Availability Handling & Neutral Rationale
def test_missing_videos_neutral_availability_handling():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = None

    raw_payload = {
        "items": [
            {
                "id": "vid_present",
                "snippet": {"title": "Present", "publishedAt": "2022-12-18T15:00:00Z"},
                "contentDetails": {"duration": "PT5M"},
                "statistics": {"viewCount": "500"}
            }
        ]
    }
    requested_ids = ["vid_present", "vid_missing"]

    res_cnt, unavail_cnt = process_video_metadata_payload(
        mock_conn, raw_payload, requested_ids, "run_123", "req_123", "raw_123"
    )
    assert res_cnt == 1
    assert unavail_cnt == 1

    exec_calls = mock_cursor.execute.call_args_list
    call_pairs = [(c[0][0], c[0][1] if len(c[0]) > 1 else ()) for c in exec_calls]

    # Check UNAVAILABLE recorded in OPS.VIDEO_METADATA_RESOLUTION_STATE
    assert any("INSERT INTO OPS.VIDEO_METADATA_RESOLUTION_STATE" in s and ("UNAVAILABLE" in s or "UNAVAILABLE" in p) for s, p in call_pairs)

    # Check neutral rationale in BRIDGE_VIDEO_EVENT without guessing cause
    neutral_text = "Video ID was not returned by the videos.list request; underlying cause was not observable from this response."
    bridge_updates = [p for s, p in call_pairs if "UPDATE CORE.BRIDGE_VIDEO_EVENT" in s]
    assert any(neutral_text in str(p) for p in bridge_updates)

    # Verify DIM_VIDEO.content_type was NOT overwritten to UNAVAILABLE
    dim_updates = [s for s, p in call_pairs if "UPDATE CORE.DIM_VIDEO" in s]
    for s in dim_updates:
        assert "content_type" not in s or "UNAVAILABLE" not in s


# 6. Snapshot Idempotency: Replay Does Not Duplicate Rows
def test_snapshot_idempotency_on_replay():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    # Existing video key
    mock_cursor.fetchone.return_value = ("video_key_123", "2022-12-18T10:00:00Z")

    raw_payload = {
        "items": [
            {
                "id": "vid_replay",
                "snippet": {"title": "Replay", "publishedAt": "2022-12-18T15:00:00Z"},
                "contentDetails": {"duration": "PT5M"},
                "statistics": {"viewCount": "500", "likeCount": "20"}
            }
        ]
    }
    process_video_metadata_payload(
        mock_conn, raw_payload, ["vid_replay"], "run_123", "req_123", "raw_123"
    )

    # Check INSERT INTO CORE.FACT_VIDEO_SNAPSHOT uses WHERE NOT EXISTS with api_request_id
    snapshot_inserts = [c[0][0] for c in mock_cursor.execute.call_args_list if "INSERT INTO CORE.FACT_VIDEO_SNAPSHOT" in c[0][0]]
    assert len(snapshot_inserts) >= 1
    assert "WHERE NOT EXISTS" in snapshot_inserts[0]
    assert "api_request_id" in snapshot_inserts[0]


# 7. Shorts Fail-Closed Semantics (Decision A.1)
def test_shorts_fail_closed_semantics():
    windows = [{
        "window_type": "EVENT",
        "start": datetime(2022, 12, 18, 14, 0, tzinfo=timezone.utc),
        "end": datetime(2022, 12, 18, 22, 0, tzinfo=timezone.utc)
    }]
    event_dt = datetime(2022, 12, 18, 15, 0, tzinfo=timezone.utc)
    terms = ["final"]
    aliases = ["messi"]

    # Case A: is_short is None -> fails closed as SHORTS_UNRESOLVED
    vid_unknown_short = {
        "title": "Messi Final Highlights",
        "published_at": "2022-12-18T16:00:00Z",
        "is_short": None,
        "primary_language_code": "en",
        "resolution_status": "RESOLVED"
    }
    res_unknown = evaluate_video_eligibility(vid_unknown_short, aliases, terms, event_dt, windows)
    assert not res_unknown["is_eligible"]
    assert res_unknown["reason"] == "SHORTS_UNRESOLVED"

    # Case B: is_short is True -> fails as YOUTUBE_SHORT
    vid_short = {
        "title": "Messi Final Highlights",
        "published_at": "2022-12-18T16:00:00Z",
        "is_short": True,
        "primary_language_code": "en",
        "resolution_status": "RESOLVED"
    }
    res_short = evaluate_video_eligibility(vid_short, aliases, terms, event_dt, windows)
    assert not res_short["is_eligible"]
    assert res_short["reason"] == "YOUTUBE_SHORT"

    # Case C: is_short is False -> eligible
    vid_long = {
        "title": "Messi Final Highlights",
        "published_at": "2022-12-18T16:00:00Z",
        "is_short": False,
        "primary_language_code": "en",
        "resolution_status": "RESOLVED"
    }
    res_long = evaluate_video_eligibility(vid_long, aliases, terms, event_dt, windows)
    assert res_long["is_eligible"]
    assert res_long["reason"] is None


# 8. Language Policy (Decision A.3)
def test_unknown_language_passes_metadata_eligibility():
    windows = [{
        "window_type": "EVENT",
        "start": datetime(2022, 12, 18, 14, 0, tzinfo=timezone.utc),
        "end": datetime(2022, 12, 18, 22, 0, tzinfo=timezone.utc)
    }]
    event_dt = datetime(2022, 12, 18, 15, 0, tzinfo=timezone.utc)
    terms = ["final"]
    aliases = ["messi"]

    # UNKNOWN source language passes metadata cohort stage
    vid_unknown_lang = {
        "title": "Messi Final Review",
        "published_at": "2022-12-18T16:00:00Z",
        "is_short": False,
        "primary_language_code": "UNKNOWN",
        "resolution_status": "RESOLVED"
    }
    res_unk = evaluate_video_eligibility(vid_unknown_lang, aliases, terms, event_dt, windows)
    assert res_unk["is_eligible"]

    # Explicit non-English fails
    vid_spanish = {
        "title": "Messi Final Review",
        "published_at": "2022-12-18T16:00:00Z",
        "is_short": False,
        "primary_language_code": "es",
        "resolution_status": "RESOLVED"
    }
    res_es = evaluate_video_eligibility(vid_spanish, aliases, terms, event_dt, windows)
    assert not res_es["is_eligible"]
    assert res_es["reason"] == "NON_ENGLISH_METADATA"


# 9. Legacy uploads_playlist Lineage Repair
def test_legacy_uploads_lineage_repair():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # Row 1: exactly 1 originating frame -> repaired
    # Row 2: 0 frames -> unresolved
    # Row 3: 2 frames -> unresolved
    mock_cursor.fetchall.side_effect = [
        # Initial query for unrepaired rows
        [
            ("ve_001", "v_001", json.dumps({"method": "uploads_playlist"})),
            ("ve_002", "v_002", json.dumps({"method": "uploads_playlist"})),
            ("ve_003", "v_003", json.dumps({"method": "uploads_playlist"}))
        ],
        # ve_001 frame candidates (exactly 1 match)
        [("frame_target_123",)],
        # ve_002 frame candidates (0 matches)
        [],
        # ve_003 frame candidates (2 matches)
        [("frame_target_123",), ("other_frame_456",)]
    ]

    result = repair_legacy_uploads_frame_lineage(mock_conn, "frame_target_123")
    assert result["repaired"] == 1
    assert result["unresolved"] == 2
    assert mock_conn.commit.called


# 10. Cohort Selection Gated on Unapproved K
def test_cohort_selection_gated_on_unapproved_k(tmp_path):
    # Create temp config with unapproved K
    cfg_file = tmp_path / "sampling_policy.yml"
    cfg_file.write_text("k_capacity: null\nk_capacity_status: UNAPPROVED_PENDING_STRATA_INSPECTION\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Cohort selection is GATED.*unapproved K capacity"):
        rank_and_select_cohorts(
            target_db="FOOTBALL_NARRATIVE_TEST",
            run_purpose="INTEGRATION_TEST",
            frame_version_key="frame_123",
            event_version_key="ev_123",
            sampling_policy_path=str(cfg_file)
        )


# 11. Cohort Selection Gated on Missing Channel Stratum Snapshot
def test_cohort_selection_gated_on_missing_channel_stratum_snapshot(tmp_path):
    # Config with approved K for test
    cfg_file = tmp_path / "sampling_policy.yml"
    cfg_file.write_text("k_capacity: 5\nk_capacity_status: APPROVED\n", encoding="utf-8")

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # Event terms, aliases, windows return valid records
    mock_cursor.fetchone.side_effect = [
        # Event version
        (datetime(2022, 12, 18, 15, 0), json.dumps(["final"])),
        # Stratum snapshot query: return None (missing snapshot!)
        None
    ]
    mock_cursor.fetchall.side_effect = [
        # Aliases
        [("messi",)],
        # Windows
        [("w1", "EVENT", datetime(2022, 12, 18, 14, 0), datetime(2022, 12, 18, 22, 0))],
        # Candidates query returns 1 candidate for channel ch_123
        [("v_1", "vid_1", "ch_123", datetime(2022, 12, 18, 16, 0), 300, False, "en", "ve_1", "sp_1", "RESOLVED", "Title", "Desc")]
    ]

    with patch("cohort_selection.get_snowflake_connection", return_value=mock_conn):
        with pytest.raises(ValueError, match="Cohort selection is GATED.*lacks active BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT"):
            rank_and_select_cohorts(
                target_db="FOOTBALL_NARRATIVE_TEST",
                run_purpose="INTEGRATION_TEST",
                frame_version_key="frame_123",
                event_version_key="ev_123",
                sampling_policy_path=str(cfg_file)
            )


# 12. Frozen 5-Step Binary Ranking (No Title-over-Description Precedence)
def test_frozen_ranking_binary_relevance_no_title_over_description():
    windows = [{
        "window_type": "EVENT",
        "start": datetime(2022, 12, 18, 14, 0, tzinfo=timezone.utc),
        "end": datetime(2022, 12, 18, 22, 0, tzinfo=timezone.utc)
    }]
    event_dt = datetime(2022, 12, 18, 15, 0, tzinfo=timezone.utc)
    terms = ["final"]
    aliases = ["messi"]

    # Candidate A: Title has "Messi", description has "Final"
    cand_a = {
        "title": "Messi Masterclass",
        "description": "Analysis of the World Cup Final match",
        "published_at": "2022-12-18T16:00:00Z",
        "is_short": False,
        "primary_language_code": "en",
        "resolution_status": "RESOLVED"
    }
    # Candidate B: Title has "World Cup Final", description has "Messi"
    cand_b = {
        "title": "World Cup Final Tactical Analysis",
        "description": "How Messi influenced the game",
        "published_at": "2022-12-18T16:00:00Z",
        "is_short": False,
        "primary_language_code": "en",
        "resolution_status": "RESOLVED"
    }

    res_a = evaluate_video_eligibility(cand_a, aliases, terms, event_dt, windows)
    res_b = evaluate_video_eligibility(cand_b, aliases, terms, event_dt, windows)

    # Both must evaluate as equally eligible with identical binary matches
    assert res_a["is_eligible"] and res_b["is_eligible"]
    assert res_a["event_matched"] is True and res_b["event_matched"] is True
    assert res_a["player_matched"] is True and res_b["player_matched"] is True
    assert res_a["proximity_seconds"] == res_b["proximity_seconds"]


# 13. Structural Isolation: Reject PIPELINE_PILOT frames for RESEARCH runs
def test_research_rejects_pipeline_pilot_frame():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = ("PIPELINE_PILOT",)

    with patch("ingestion_video_metadata.get_snowflake_connection", return_value=mock_conn):
        with pytest.raises(ValueError, match="RESEARCH runs structurally reject PIPELINE_PILOT frames"):
            ingest_video_metadata(
                target_db="FOOTBALL_NARRATIVE_DEV",
                run_purpose="RESEARCH",
                frame_version_key="pilot_frame_key"
            )
