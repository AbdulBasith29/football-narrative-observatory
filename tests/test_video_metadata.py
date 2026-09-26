import json
from datetime import datetime, timezone
import pytest
from unittest.mock import MagicMock, patch

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))

from ingestion_video_metadata import (
    parse_iso8601_duration,
    chunk_video_ids,
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
    mock_cursor.fetchall.return_value = [("vid_001",), ("vid_002",)]

    with patch("ingestion_video_metadata.get_snowflake_connection", return_value=mock_conn):
        ingest_video_metadata(
            target_db="FOOTBALL_NARRATIVE_TEST",
            run_purpose="INTEGRATION_TEST",
            frame_version_key="test_frame_123",
            youtube_client=mock_youtube
        )

    assert mock_videos.list.called
    call_kwargs = mock_videos.list.call_args[1]
    assert "id" in call_kwargs
    assert call_kwargs["part"] == "snippet,contentDetails,statistics,status"
    assert "maxResults" not in call_kwargs


# 4. RAW Replay Must Preserve Requested IDs (Regression Test)
def test_raw_replay_preserves_requested_ids_and_reconciles_missing():
    """
    Regression test:
    Request A, B, C; response contains A, B.
    RAW replay must reconstruct requested IDs from request_parameters, NOT response items,
    and reconcile C as UNAVAILABLE.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    raw_payload = {
        "items": [
            {"id": "vid_A", "snippet": {"title": "Title A", "publishedAt": "2022-12-18T15:00:00Z"}, "contentDetails": {"duration": "PT5M"}, "statistics": {}},
            {"id": "vid_B", "snippet": {"title": "Title B", "publishedAt": "2022-12-18T15:00:00Z"}, "contentDetails": {"duration": "PT5M"}, "statistics": {}}
        ]
    }
    requested_ids = ["vid_A", "vid_B", "vid_C"]

    # 1. Persist raw response
    persist_raw_video_response(
        mock_conn, raw_payload, requested_ids, "run_test", "req_test",
        "2022-12-18T15:00:00Z", "2022-12-18T15:00:01Z"
    )

    # Verify request_parameters includes exact requested_video_ids
    raw_insert_call = [c for c in mock_cursor.execute.call_args_list if "INSERT INTO RAW.YOUTUBE_API_RESPONSE" in c[0][0]][0]
    req_params_json = raw_insert_call[0][1][4]
    req_params = json.loads(req_params_json)
    assert req_params["requested_video_ids"] == requested_ids
    assert req_params["id_count"] == 3

    # 2. Replay raw responses
    mock_cursor.fetchall.return_value = [
        ("raw_resp_123", "req_test", "run_test", json.dumps(raw_payload), req_params_json)
    ]
    # For process_video_metadata_payload lookups
    mock_cursor.fetchone.return_value = None

    replayed_count = replay_raw_video_responses(mock_conn, raw_response_id="raw_resp_123")
    assert replayed_count == 3  # All 3 requested IDs replayed

    # Verify vid_C was reconciled as UNAVAILABLE in OPS.VIDEO_METADATA_RESOLUTION_STATE
    all_execs = [(c[0][0], c[0][1] if len(c[0]) > 1 else ()) for c in mock_cursor.execute.call_args_list]
    unavailable_inserts = [
        p for s, p in all_execs
        if "INSERT INTO OPS.VIDEO_METADATA_RESOLUTION_STATE" in s and "UNAVAILABLE" in s
    ]
    assert any("vid_C" in p for p in unavailable_inserts), "vid_C must be reconciled as UNAVAILABLE during RAW replay"


# 5. Failure-State Model: Bounded Retries for 5xx and FACT_API_REQUEST Telemetry
def test_transient_5xx_produces_telemetry_and_bounded_retry():
    mock_youtube = MagicMock()
    mock_videos = MagicMock()
    mock_list = MagicMock()
    mock_youtube.videos.return_value = mock_videos
    mock_videos.list.return_value = mock_list

    # Mock 500 error on all attempts
    mock_err_resp = MagicMock()
    mock_err_resp.status = 500
    mock_err = Exception("500 Internal Server Error")
    mock_err.resp = mock_err_resp
    mock_list.execute.side_effect = mock_err

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("vid_fail_1",)]
    mock_cursor.fetchone.return_value = None

    with patch("ingestion_video_metadata.get_snowflake_connection", return_value=mock_conn):
        result = ingest_video_metadata(
            target_db="FOOTBALL_NARRATIVE_TEST",
            run_purpose="INTEGRATION_TEST",
            frame_version_key="test_frame_123",
            youtube_client=mock_youtube
        )

    assert result["run_status"] == "PARTIAL_ERROR"
    assert result["calls_attempted"] == 3  # 3 bounded retries attempted
    assert result["calls_succeeded"] == 0

    all_execs = [(c[0][0], c[0][1] if len(c[0]) > 1 else ()) for c in mock_cursor.execute.call_args_list]

    # Every attempt produced FACT_API_REQUEST telemetry
    api_request_inserts = [p for s, p in all_execs if "INSERT INTO OPS.FACT_API_REQUEST" in s]
    assert len(api_request_inserts) == 3
    assert all(p[5] == 500 for p in api_request_inserts)
    assert [p[6] for p in api_request_inserts] == [0, 1, 2]  # retry_number 0, 1, 2

    # Resolution state transitioned to RETRYABLE_ERROR
    state_updates = [p for s, p in all_execs if "INSERT INTO OPS.VIDEO_METADATA_RESOLUTION_STATE" in s]
    assert any("RETRYABLE_ERROR" in p for p in state_updates)

    # Ingestion run telemetry: pages_requested (3) != pages_succeeded (0)
    run_inserts = [p for s, p in all_execs if "INSERT INTO OPS.FACT_INGESTION_RUN" in s]
    assert len(run_inserts) == 1
    assert run_inserts[0][4] == 3  # pages_requested
    assert run_inserts[0][5] == 0  # pages_succeeded


# 6. Failure-State Model: Ordinary Auth/Config 403 Does Not Retry Indefinitely
def test_ordinary_auth_403_does_not_retry():
    mock_youtube = MagicMock()
    mock_videos = MagicMock()
    mock_list = MagicMock()
    mock_youtube.videos.return_value = mock_videos
    mock_videos.list.return_value = mock_list

    mock_err_resp = MagicMock()
    mock_err_resp.status = 403
    mock_err = Exception("The request cannot be completed because you have exceeded your quota? No, invalid API key.")
    mock_err.resp = mock_err_resp
    mock_err.content = b"The API key is invalid."
    mock_list.execute.side_effect = mock_err

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("vid_auth_fail",)]
    mock_cursor.fetchone.return_value = None

    with patch("ingestion_video_metadata.get_snowflake_connection", return_value=mock_conn):
        result = ingest_video_metadata(
            target_db="FOOTBALL_NARRATIVE_TEST",
            run_purpose="INTEGRATION_TEST",
            frame_version_key="test_frame_123",
            youtube_client=mock_youtube
        )

    # Must fail immediately without retrying indefinitely
    assert result["run_status"] == "PARTIAL_ERROR"
    assert result["calls_attempted"] == 1
    assert result["calls_succeeded"] == 0

    all_execs = [(c[0][0], c[0][1] if len(c[0]) > 1 else ()) for c in mock_cursor.execute.call_args_list]
    api_request_inserts = [p for s, p in all_execs if "INSERT INTO OPS.FACT_API_REQUEST" in s]
    assert len(api_request_inserts) == 1
    assert api_request_inserts[0][7] == "AUTH_OR_CONFIG_ERROR"


# 7. Failure-State Model: Downstream Parser Failure Preserves RAW and Transitions to PARSE_ERROR
def test_parser_failure_preserves_raw_and_logs_dead_letter():
    mock_youtube = MagicMock()
    mock_videos = MagicMock()
    mock_list = MagicMock()
    mock_youtube.videos.return_value = mock_videos
    mock_videos.list.return_value = mock_list
    mock_list.execute.return_value = {
        "items": [{"id": "vid_bad_parse", "snippet": {"title": "Test"}}]
    }

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("vid_bad_parse",)]
    mock_cursor.fetchone.return_value = None

    # Inject parser failure after raw persistence
    with patch("ingestion_video_metadata.process_video_metadata_payload", side_effect=ValueError("Corrupt snippet structure")):
        with patch("ingestion_video_metadata.get_snowflake_connection", return_value=mock_conn):
            result = ingest_video_metadata(
                target_db="FOOTBALL_NARRATIVE_TEST",
                run_purpose="INTEGRATION_TEST",
                frame_version_key="test_frame_123",
                youtube_client=mock_youtube
            )

    assert result["run_status"] == "PARTIAL_ERROR"
    all_execs = [(c[0][0], c[0][1] if len(c[0]) > 1 else ()) for c in mock_cursor.execute.call_args_list]

    # RAW response was preserved before parse failure
    assert any("INSERT INTO RAW.YOUTUBE_API_RESPONSE" in s for s, p in all_execs)

    # DEAD_LETTER_RECORD was logged
    dead_letters = [p for s, p in all_execs if "INSERT INTO OPS.DEAD_LETTER_RECORD" in s]
    assert len(dead_letters) == 1
    assert dead_letters[0][3] == "PARSE"
    assert dead_letters[0][4] == "PARSE_ERROR"

    # Candidate transitioned to PARSE_ERROR
    state_updates = [p for s, p in all_execs if "INSERT INTO OPS.VIDEO_METADATA_RESOLUTION_STATE" in s]
    assert any("PARSE_ERROR" in p for p in state_updates)


# 8. Managed Lifecycle Enrichment: Freeze Semantics on Refresh
def test_managed_lifecycle_freeze_semantics_preserves_resolved_metadata():
    """
    Regression test:
    Normal metadata refresh where the Shorts classifier returns UNKNOWN (None)
    must NEVER erase a previously resolved TRUE/FALSE is_short value in DIM_VIDEO.
    Existing resolved language must also not be overwritten by UNKNOWN.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # Video already has resolved metadata: is_short=False, rule='frozen_v1.0', lang='en', duration=300
    mock_cursor.fetchone.side_effect = [
        ("video_key_123", "2022-12-18T10:00:00Z", False, "frozen_v1.0", "en", 300),  # DIM_VIDEO lookup
        ("res_state_123",)  # RESOLUTION_STATE lookup
    ]

    # Incoming refresh has no language info (returns UNKNOWN) and is_short=None
    refresh_payload = {
        "items": [
            {
                "id": "vid_stable",
                "snippet": {"title": "Updated Title", "publishedAt": "2022-12-18T15:00:00Z"},
                "contentDetails": {"duration": "PT5M"},
                "statistics": {"viewCount": "1200"}
            }
        ]
    }

    process_video_metadata_payload(
        mock_conn, refresh_payload, ["vid_stable"], "run_refresh", "req_refresh", "raw_refresh"
    )

    all_execs = [(c[0][0], c[0][1] if len(c[0]) > 1 else ()) for c in mock_cursor.execute.call_args_list]
    dim_updates = [p for s, p in all_execs if "UPDATE CORE.DIM_VIDEO" in s]
    assert len(dim_updates) == 1

    # Parameters: (pub_at_str, final_duration, final_is_short, final_rule, final_lang, v_key)
    update_params = dim_updates[0]
    assert update_params[1] == 300  # duration preserved
    assert update_params[2] is False, "Previously resolved is_short=False must NOT be overwritten by None"
    assert update_params[3] == "frozen_v1.0", "Original rule version must be preserved"
    assert update_params[4] == "en", "Resolved language 'en' must NOT be overwritten by 'UNKNOWN'"


# 9. Cohort Eligibility: Separate Baseline vs Event Rules and Temporal Boundaries
def test_cohort_eligibility_boundaries_and_overlap_rule():
    """
    Regression test for docs/04-sampling-methodology.md Section 7.1:
    - Pure Baseline: [T-14d, T-24h) -> BASELINE if player/competition/topic relevant
    - Overlap: [T-24h, T-1h] -> EVENT if event-relevant; else BASELINE if baseline-relevant; else ineligible
    - Pure Event: (T-1h, T+72h] -> EVENT if event-relevant
    - Outside [T-14d, T+72h] -> OUT_OF_WINDOW
    """
    event_dt = datetime(2022, 12, 18, 15, 0, tzinfo=timezone.utc)
    terms = ["final", "world cup"]
    aliases = ["messi"]

    def make_video(published_at, title, desc=""):
        return {
            "title": title,
            "description": desc,
            "published_at": published_at,
            "is_short": False,
            "primary_language_code": "en",
            "resolution_status": "RESOLVED"
        }

    # 1. Exact boundary T-14d (2022-12-04T15:00:00Z):
    # Pure baseline: Discusses player only -> BASELINE
    v_t_minus_14d = make_video("2022-12-04T15:00:00Z", "Messi training report")
    res = evaluate_video_eligibility(v_t_minus_14d, aliases, terms, event_dt)
    assert res["is_eligible"]
    assert res["cohort_type"] == "BASELINE"

    # Outside T-14d by 1 second -> OUT_OF_WINDOW
    v_before_14d = make_video("2022-12-04T14:59:59Z", "Messi training report")
    res = evaluate_video_eligibility(v_before_14d, aliases, terms, event_dt)
    assert not res["is_eligible"]
    assert res["reason"] == "OUT_OF_WINDOW"

    # 2. Overlap Start: Exact boundary T-24h (2022-12-17T15:00:00Z):
    # Case A: Discusses EVENT -> assigned to EVENT (event takes precedence)
    v_t_minus_24h_event = make_video("2022-12-17T15:00:00Z", "World Cup Final tactical preview")
    res = evaluate_video_eligibility(v_t_minus_24h_event, aliases, terms, event_dt)
    assert res["is_eligible"]
    assert res["cohort_type"] == "EVENT"

    # Case B: Discusses PLAYER only -> assigned to BASELINE
    v_t_minus_24h_player = make_video("2022-12-17T15:00:00Z", "Lionel Messi career retrospection")
    res = evaluate_video_eligibility(v_t_minus_24h_player, aliases, terms, event_dt)
    assert res["is_eligible"]
    assert res["cohort_type"] == "BASELINE"

    # Case C: Irrelevant -> INELIGIBLE
    v_t_minus_24h_irrel = make_video("2022-12-17T15:00:00Z", "Unrelated football news")
    res = evaluate_video_eligibility(v_t_minus_24h_irrel, aliases, terms, event_dt)
    assert not res["is_eligible"]
    assert res["reason"] == "NO_TARGET_RELEVANCE"

    # 3. Overlap End: Exact boundary T-1h (2022-12-18T14:00:00Z):
    # Discusses EVENT -> EVENT
    v_t_minus_1h_event = make_video("2022-12-18T14:00:00Z", "Pre-match Final buildup")
    res = evaluate_video_eligibility(v_t_minus_1h_event, aliases, terms, event_dt)
    assert res["is_eligible"]
    assert res["cohort_type"] == "EVENT"

    # Discusses PLAYER only -> BASELINE
    v_t_minus_1h_player = make_video("2022-12-18T14:00:00Z", "Messi warm up routine")
    res = evaluate_video_eligibility(v_t_minus_1h_player, aliases, terms, event_dt)
    assert res["is_eligible"]
    assert res["cohort_type"] == "BASELINE"

    # 4. Pure Event: Event Timestamp T=0 (2022-12-18T15:00:00Z):
    # Event relevance -> EVENT
    v_t0_event = make_video("2022-12-18T15:00:00Z", "Live World Cup Final kick off")
    res = evaluate_video_eligibility(v_t0_event, aliases, terms, event_dt)
    assert res["is_eligible"]
    assert res["cohort_type"] == "EVENT"

    # Discusses player only without event terms after T-1h -> INELIGIBLE (baseline has ended)
    v_t0_player_only = make_video("2022-12-18T15:00:00Z", "Messi career compilation")
    res = evaluate_video_eligibility(v_t0_player_only, aliases, terms, event_dt)
    assert not res["is_eligible"]
    assert res["reason"] == "NO_TARGET_RELEVANCE"

    # 5. Exact boundary T+72h (2022-12-21T15:00:00Z):
    v_t_plus_72h = make_video("2022-12-21T15:00:00Z", "Final aftermath analysis")
    res = evaluate_video_eligibility(v_t_plus_72h, aliases, terms, event_dt)
    assert res["is_eligible"]
    assert res["cohort_type"] == "EVENT"

    # Outside T+72h by 1 second -> OUT_OF_WINDOW
    v_after_72h = make_video("2022-12-21T15:00:01Z", "Final aftermath analysis")
    res = evaluate_video_eligibility(v_after_72h, aliases, terms, event_dt)
    assert not res["is_eligible"]
    assert res["reason"] == "OUT_OF_WINDOW"


# 10. Scope Target Aliases to the Current Event
def test_aliases_scoped_to_current_event(tmp_path):
    """
    Regression test:
    Target aliases must be resolved via BRIDGE_EVENT_ENTITY for THIS event.
    An alias belonging to another target entity cannot satisfy target relevance.
    """
    cfg_file = tmp_path / "sampling_policy.yml"
    cfg_file.write_text(
        "sampling_policy_version: '1.2'\n"
        "sampling_policy_version_key: 'sp_v1_key'\n"
        "k_capacity: 5\nk_capacity_status: APPROVED\n",
        encoding="utf-8"
    )

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_cursor.fetchone.side_effect = [
        # Event version occurred_at and terms
        (datetime(2022, 12, 18, 15, 0), json.dumps(["final"])),
        # Stratum snapshot query
        ("snap_1", "ANALYST", "MESSI_FOCUSED")
    ]
    mock_cursor.fetchall.side_effect = [
        # Scoped aliases: only "messi" is attached to THIS event (Ronaldo is NOT attached)
        [("messi",)],
        # Windows
        [("w1", "EVENT", datetime(2022, 12, 18, 14, 0), datetime(2022, 12, 18, 22, 0))],
        # Candidate videos: one video mentions Ronaldo only during pre-event baseline window
        [("v_ronaldo", "vid_ronaldo", "ch_1", datetime(2022, 12, 10, 12, 0), 300, False, "en", "ve_r", "sp_v1_key", "RESOLVED", "Cristiano Ronaldo skills", "Only CR7 skills")]
    ]

    with patch("cohort_selection.get_snowflake_connection", return_value=mock_conn):
        rank_and_select_cohorts(
            target_db="FOOTBALL_NARRATIVE_TEST",
            run_purpose="INTEGRATION_TEST",
            frame_version_key="frame_123",
            event_version_key="ev_wc22",
            sampling_policy_path=str(cfg_file)
        )

    # Verify query for aliases joins BRIDGE_EVENT_ENTITY with event_version_key
    alias_queries = [c[0][0] for c in mock_cursor.execute.call_args_list if "CORE.DIM_TARGET_ENTITY_ALIAS" in c[0][0]]
    assert len(alias_queries) == 1
    assert "BRIDGE_EVENT_ENTITY" in alias_queries[0]

    # Verify Ronaldo video was marked INELIGIBLE with NO_TARGET_RELEVANCE because Ronaldo is not attached to WC22
    all_execs = [(c[0][0], c[0][1] if len(c[0]) > 1 else ()) for c in mock_cursor.execute.call_args_list]
    ineligible_updates = [p for s, p in all_execs if "UPDATE CORE.BRIDGE_VIDEO_EVENT" in s and "NO_TARGET_RELEVANCE" in p]
    assert len(ineligible_updates) == 1


# 11. Pin Sampling Policy Version: Two Versions Cannot Be Mixed
def test_pin_sampling_policy_version_cannot_be_mixed(tmp_path):
    """
    Regression test:
    rank_and_select_cohorts must pin selection to one explicit sampling_policy_version_key
    and filter BRIDGE_VIDEO_EVENT to it.
    """
    cfg_file = tmp_path / "sampling_policy.yml"
    cfg_file.write_text(
        "sampling_policy_version: '1.2'\n"
        "sampling_policy_version_key: 'policy_v1_approved'\n"
        "k_capacity: 5\nk_capacity_status: APPROVED\n",
        encoding="utf-8"
    )

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_cursor.fetchone.side_effect = [
        (datetime(2022, 12, 18, 15, 0), json.dumps(["final"])),
        ("snap_1", "ANALYST", "MESSI_FOCUSED")
    ]
    mock_cursor.fetchall.side_effect = [
        [("messi",)],
        [("w1", "EVENT", datetime(2022, 12, 18, 14, 0), datetime(2022, 12, 18, 22, 0))],
        []  # 0 candidates
    ]

    with patch("cohort_selection.get_snowflake_connection", return_value=mock_conn):
        rank_and_select_cohorts(
            target_db="FOOTBALL_NARRATIVE_TEST",
            run_purpose="INTEGRATION_TEST",
            frame_version_key="frame_123",
            event_version_key="ev_123",
            sampling_policy_path=str(cfg_file)
        )

    # Candidate query must filter by sampling_policy_version_key
    candidate_queries = [
        (c[0][0], c[0][1]) for c in mock_cursor.execute.call_args_list
        if "FROM CORE.DIM_VIDEO v" in c[0][0] and "JOIN CORE.BRIDGE_VIDEO_EVENT b" in c[0][0]
    ]
    assert len(candidate_queries) == 1
    query_sql, query_params = candidate_queries[0]
    assert "b.sampling_policy_version_key = %s" in query_sql
    assert "policy_v1_approved" in query_params


# 12. Test Database Isolation: INTEGRATION_TEST Requires FOOTBALL_NARRATIVE_TEST
def test_test_database_isolation_enforced():
    """
    Regression test:
    run_purpose = INTEGRATION_TEST targeting FOOTBALL_NARRATIVE_DEV must fail
    structurally before any database/API processing in both entry points.
    """
    with pytest.raises(ValueError, match="run_purpose 'INTEGRATION_TEST' requires target_db 'FOOTBALL_NARRATIVE_TEST' exclusively"):
        ingest_video_metadata(
            target_db="FOOTBALL_NARRATIVE_DEV",
            run_purpose="INTEGRATION_TEST",
            frame_version_key="frame_123"
        )

    with pytest.raises(ValueError, match="run_purpose 'INTEGRATION_TEST' requires target_db 'FOOTBALL_NARRATIVE_TEST' exclusively"):
        rank_and_select_cohorts(
            target_db="FOOTBALL_NARRATIVE_DEV",
            run_purpose="INTEGRATION_TEST",
            frame_version_key="frame_123",
            event_version_key="ev_123",
            sampling_policy_path="config/sampling_policy.yml"
        )
