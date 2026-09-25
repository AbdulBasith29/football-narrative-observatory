import pytest
from datetime import datetime, timezone
import sys
import os
from unittest.mock import patch, MagicMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))
from discovery_youtube import (
    evaluate_video_against_events, discover_videos, fetch_playlist_items, 
    fetch_search_items_for_windows, generate_discovery_units, estimate_search_calls,
    compute_query_hash
)

def test_evaluate_video_against_events():
    aliases = ["messi", "ronaldo"]
    
    event_to_windows = {
        'ev1': [{
            'window_key': 'w1',
            'event_version_key': 'ev1',
            'start': datetime(2022, 12, 1, tzinfo=timezone.utc),
            'end': datetime(2022, 12, 31, tzinfo=timezone.utc),
            'terms': ['world cup']
        }]
    }

    # Case 1: In window, has alias
    video_1 = {
        'snippet': {
            'title': 'Messi scores a great goal',
            'description': '',
            'publishedAt': '2022-12-15T10:00:00Z'
        }
    }
    res_1 = evaluate_video_against_events(video_1, aliases, event_to_windows)
    assert len(res_1) == 1
    assert res_1[0] == ('ev1', 'ELIGIBLE', None)
    
    # Case 2: In window, NO alias but HAS event term (baseline relevance)
    video_2 = {
        'snippet': {
            'title': 'France vs Argentina',
            'description': 'Amazing world cup final football',
            'publishedAt': '2022-12-15T10:00:00Z'
        }
    }
    res_2 = evaluate_video_against_events(video_2, aliases, event_to_windows)
    assert res_2[0] == ('ev1', 'ELIGIBLE', None)

    # Case 3: In window, no alias, no event term
    video_3 = {
        'snippet': {
            'title': 'Mbappe scores a great goal',
            'description': 'Amazing football',
            'publishedAt': '2022-12-15T10:00:00Z'
        }
    }
    res_3 = evaluate_video_against_events(video_3, aliases, event_to_windows)
    assert res_3[0] == ('ev1', 'INELIGIBLE', 'NO_TARGET_RELEVANCE')
    
    # Case 4: Has alias, OUT of window
    video_4 = {
        'snippet': {
            'title': 'Ronaldo highlights',
            'description': '',
            'publishedAt': '2023-01-15T10:00:00Z'
        }
    }
    res_4 = evaluate_video_against_events(video_4, aliases, event_to_windows)
    assert res_4[0] == ('ev1', 'INELIGIBLE', 'OUT_OF_WINDOW')

def test_alias_word_boundaries():
    aliases = ["cr7"]
    event_to_windows = {
        'ev1': [{
            'window_key': 'w1',
            'event_version_key': 'ev1',
            'start': datetime(2022, 1, 1, tzinfo=timezone.utc),
            'end': datetime(2023, 1, 1, tzinfo=timezone.utc),
            'terms': []
        }]
    }
    
    # Exact match
    video_1 = {
        'snippet': {
            'title': 'The best is CR7!',
            'description': '',
            'publishedAt': '2022-06-15T10:00:00Z'
        }
    }
    res_1 = evaluate_video_against_events(video_1, aliases, event_to_windows)
    assert res_1[0] == ('ev1', 'ELIGIBLE', None)
    
    # Substring match shouldn't trigger
    video_2 = {
        'snippet': {
            'title': 'CR700 model specs',
            'description': '',
            'publishedAt': '2022-06-15T10:00:00Z'
        }
    }
    res_2 = evaluate_video_against_events(video_2, aliases, event_to_windows)
    assert res_2[0] == ('ev1', 'INELIGIBLE', 'NO_TARGET_RELEVANCE')

@patch("discovery_youtube.get_snowflake_connection")
def test_research_pilot_isolation(mock_get_conn):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    mock_cursor.fetchone.return_value = ("PIPELINE_PILOT",)
    
    with pytest.raises(ValueError, match="RESEARCH discovery structurally rejects PIPELINE_PILOT frames."):
        discover_videos(run_purpose="RESEARCH", frame_version_key="pilot-frame-id")
        
    mock_cursor.fetchone.return_value = ("PIPELINE_PILOT",)
    mock_cursor.fetchall.return_value = []
    try:
        discover_videos(run_purpose="INTEGRATION_TEST", frame_version_key="pilot-frame-id")
    except ValueError as e:
        if "structurally rejects" in str(e):
            pytest.fail("INTEGRATION_TEST should not reject PIPELINE_PILOT frames")

def test_pagination_playlist_items():
    mock_youtube = MagicMock()
    mock_req_1 = MagicMock()
    mock_req_2 = MagicMock()
    mock_req_3 = MagicMock()
    
    mock_youtube.playlistItems().list.side_effect = [mock_req_1, mock_req_2, mock_req_3]
    
    mock_req_1.execute.return_value = {
        "items": [
            {"id": "skip_2026", "snippet": {"publishedAt": "2026-06-15T10:00:00Z"}},
            {"id": "skip_2023", "snippet": {"publishedAt": "2023-06-15T10:00:00Z"}}
        ],
        "nextPageToken": "token1"
    }
    mock_req_2.execute.return_value = {
        "items": [
            {"id": "retain_2022", "snippet": {"publishedAt": "2022-06-15T10:00:00Z"}},
            {"id": "stop_2021", "snippet": {"publishedAt": "2021-06-15T10:00:00Z"}}
        ]
    }
    
    min_date = datetime(2022, 1, 1, tzinfo=timezone.utc)
    max_date = datetime(2022, 12, 31, tzinfo=timezone.utc)
    items = fetch_playlist_items(mock_youtube, "playlist_id", min_date, max_date)
    
    assert len(items) == 1
    assert items[0]["id"] == "retain_2022"
    assert mock_youtube.playlistItems().list.call_count == 2

def test_pagination_monotonic_invariant():
    mock_youtube = MagicMock()
    mock_req_1 = MagicMock()
    mock_req_2 = MagicMock()
    
    mock_youtube.playlistItems().list.side_effect = [mock_req_1, mock_req_2]
    
    mock_req_1.execute.return_value = {
        "items": [
            {"id": "v1", "snippet": {"publishedAt": "2022-06-15T10:00:00Z"}},
            {"id": "v2_out_of_order", "snippet": {"publishedAt": "2022-07-15T10:00:00Z"}},
            {"id": "v3_old", "snippet": {"publishedAt": "2021-06-15T10:00:00Z"}}
        ],
        "nextPageToken": "token1"
    }
    mock_req_2.execute.return_value = {
        "items": [
            {"id": "v4_recovered", "snippet": {"publishedAt": "2022-08-15T10:00:00Z"}}
        ]
    }
    
    min_date = datetime(2022, 1, 1, tzinfo=timezone.utc)
    max_date = datetime(2022, 12, 31, tzinfo=timezone.utc)
    items = fetch_playlist_items(mock_youtube, "playlist_id", min_date, max_date)
    
    assert len(items) == 3
    assert {i["id"] for i in items} == {"v1", "v2_out_of_order", "v4_recovered"}
    assert mock_youtube.playlistItems().list.call_count == 2

def test_fetch_search_items_for_windows():
    mock_youtube = MagicMock()
    mock_req_1 = MagicMock()
    mock_req_2 = MagicMock()
    
    mock_youtube.search().list.side_effect = [mock_req_1, mock_req_2]
    
    mock_req_1.execute.return_value = {
        "items": [{"id": {"videoId": "vid1"}, "snippet": {}}],
        "nextPageToken": "token1"
    }
    mock_req_2.execute.return_value = {
        "items": [{"id": {"videoId": "vid2"}, "snippet": {}}]
    }
    
    windows = [{
        'window_key': 'w1',
        'event_version_key': 'ev1',
        'start': datetime(2022, 12, 1, tzinfo=timezone.utc),
        'end': datetime(2022, 12, 31, tzinfo=timezone.utc),
        'terms': ['world cup']
    }]
    
    items, prov = fetch_search_items_for_windows(mock_youtube, "ch1", ["messi"], windows)
    assert len(items) == 2
    assert items[0]["snippet"]["resourceId"]["videoId"] == "vid1"
    assert items[1]["snippet"]["resourceId"]["videoId"] == "vid2"
    assert mock_youtube.search().list.call_count == 2
    
    call_kwargs = mock_youtube.search().list.call_args_list[0][1]
    assert call_kwargs['q'] == '"messi" | "world cup"'
    assert prov[0]["batch_number"] == 1
    assert prov[0]["query"] == '"messi" | "world cup"'

def test_fetch_search_items_batching():
    mock_youtube = MagicMock()
    mock_req_1 = MagicMock()
    mock_req_2 = MagicMock()
    
    mock_youtube.search().list.side_effect = [mock_req_1, mock_req_2]
    
    mock_req_1.execute.return_value = {
        "items": [{"id": {"videoId": "vid1"}, "snippet": {}}]
    }
    mock_req_2.execute.return_value = {
        "items": [{"id": {"videoId": "vid2"}, "snippet": {}}]
    }
    
    windows = [{
        'window_key': 'w1',
        'event_version_key': 'ev1',
        'start': datetime(2022, 12, 1, tzinfo=timezone.utc),
        'end': datetime(2022, 12, 31, tzinfo=timezone.utc),
        'terms': ['b_term', 'c_term']
    }]
    
    items, prov = fetch_search_items_for_windows(mock_youtube, "ch1", ["a_alias"], windows, search_query_character_budget=20)
    
    assert len(items) == 2
    assert len(prov) == 2
    assert prov[0]["batch_number"] == 1
    assert prov[0]["query"] == '"a_alias" | "b_term"'
    assert prov[1]["batch_number"] == 2
    assert prov[1]["query"] == '"c_term"'

def test_pre_run_estimation():
    channels = [("ch1_key", "ch1_id"), ("ch2_key", "ch2_id")]
    windows = [
        {
            'window_key': 'w1',
            'event_version_key': 'ev1',
            'start': datetime(2022, 12, 1, tzinfo=timezone.utc),
            'end': datetime(2022, 12, 31, tzinfo=timezone.utc),
            'terms': ['term1']
        }
    ]
    aliases = ["alias1"]
    
    expected_units = generate_discovery_units("frame1", channels, windows, aliases, "sp1", discovery_policy_version="1.0")
    assert len(expected_units) == 2
    
    metrics = estimate_search_calls(expected_units, {})
    assert metrics["total_expected_units"] == 2
    assert metrics["completed_units_count"] == 0
    assert metrics["remaining_units_count"] == 2
    assert metrics["minimum_required_calls"] == 2
    
    unit1 = expected_units[0]
    unit1_key = (unit1["frame_version_key"], unit1["channel_key"], unit1["window_key"], unit1["discovery_policy_version"], unit1["query_hash"])
    existing_states = {
        unit1_key: {"status": "COMPLETED"}
    }
    metrics_2 = estimate_search_calls(expected_units, existing_states)
    assert metrics_2["total_expected_units"] == 2
    assert metrics_2["completed_units_count"] == 1
    assert metrics_2["remaining_units_count"] == 1
    assert metrics_2["minimum_required_calls"] == 1

@patch("discovery_youtube.build")
@patch("discovery_youtube.os.getenv")
@patch("discovery_youtube.get_snowflake_connection")
@patch("discovery_youtube.get_aliases")
@patch("discovery_youtube.get_event_windows_and_terms")
def test_budget_clean_stop(mock_windows, mock_aliases, mock_get_conn, mock_getenv, mock_build):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    def fetchone_impl():
        if not mock_cursor.execute.call_args:
            return None
        sql = mock_cursor.execute.call_args[0][0]
        if "DIM_CHANNEL_FRAME_VERSION" in sql:
            return ("RESEARCH",)
        if "DIM_SAMPLING_POLICY_VERSION" in sql:
            return ("sp_key_1",)
        return None

    def fetchall_impl():
        if not mock_cursor.execute.call_args:
            return []
        sql = mock_cursor.execute.call_args[0][0]
        if "BRIDGE_FRAME_CHANNEL" in sql:
            return [("ch_key_1", "ch_id_1")]
        return []

    mock_cursor.fetchone.side_effect = fetchone_impl
    mock_cursor.fetchall.side_effect = fetchall_impl
    
    mock_youtube = MagicMock()
    mock_build.return_value = mock_youtube
    mock_req = MagicMock()
    mock_youtube.search().list.return_value = mock_req
    mock_req.execute.return_value = {
        "items": [{"id": {"videoId": "v1"}, "snippet": {"publishedAt": "2022-12-15T10:00:00Z", "title": "t", "description": ""}}]
    }
    
    mock_aliases.return_value = ["messi"]
    mock_windows.return_value = [{
        'window_key': 'w1',
        'event_version_key': 'ev1',
        'start': datetime(2022, 12, 1, tzinfo=timezone.utc),
        'end': datetime(2022, 12, 31, tzinfo=timezone.utc),
        'terms': ['term1', 'term2']
    }]
    
    result = discover_videos(
        run_purpose="RESEARCH", 
        frame_version_key="frame_1", 
        use_search_fallback=True, 
        run_search_call_budget=1,
        search_query_character_budget=20
    )
    
    assert result["run_status"] == "PARTIAL_QUOTA_LIMIT"
    assert result["actual_search_calls"] == 1

@patch("discovery_youtube.build")
@patch("discovery_youtube.os.getenv")
@patch("discovery_youtube.get_snowflake_connection")
@patch("discovery_youtube.get_aliases")
@patch("discovery_youtube.get_event_windows_and_terms")
def test_mid_pagination_resume(mock_windows, mock_aliases, mock_get_conn, mock_getenv, mock_build):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    q_hash = compute_query_hash('"messi"')
    unit_state_calls = 0
    
    def fetchone_impl():
        if not mock_cursor.execute.call_args:
            return None
        sql = mock_cursor.execute.call_args[0][0]
        if "DIM_CHANNEL_FRAME_VERSION" in sql:
            return ("RESEARCH",)
        if "DIM_SAMPLING_POLICY_VERSION" in sql:
            return ("sp_key_1",)
        if "DISCOVERY_UNIT_STATE" in sql and "SELECT discovery_unit_key" in sql:
            return ("unit_1", "2022-12-01T00:00:00Z")
        return None

    def fetchall_impl():
        nonlocal unit_state_calls
        if not mock_cursor.execute.call_args:
            return []
        sql = mock_cursor.execute.call_args[0][0]
        if "BRIDGE_FRAME_CHANNEL" in sql:
            return [("ch_key_1", "ch_id_1")]
        if "DISCOVERY_UNIT_STATE" in sql:
            unit_state_calls += 1
            if unit_state_calls == 1:
                return [("unit_1", "frame_1", "ch_key_1", "w1", "sp_key_1", "1.0", 1, q_hash, '"messi"', "PARTIAL_QUOTA_LIMIT", 1, "resume_token_123", 10, 10, 1, "2022-12-01T00:00:00Z", "2022-12-01T00:00:00Z", None, None, None, "run1", "run1")]
            else:
                return [("unit_1", "frame_1", "ch_key_1", "w1", "sp_key_1", "1.0", 1, q_hash, '"messi"', "COMPLETED", 2, None, 20, 20, 2, "2022-12-01T00:00:00Z", "2022-12-01T00:00:00Z", "2022-12-01T00:00:00Z", None, None, "run1", "run2")]
        return []

    mock_cursor.fetchone.side_effect = fetchone_impl
    mock_cursor.fetchall.side_effect = fetchall_impl
    
    mock_youtube = MagicMock()
    mock_build.return_value = mock_youtube
    mock_req = MagicMock()
    mock_youtube.search().list.return_value = mock_req
    mock_req.execute.return_value = {
        "items": [{"id": {"videoId": "v2"}, "snippet": {"publishedAt": "2022-12-15T10:00:00Z", "title": "t2", "description": ""}}]
    }
    
    mock_aliases.return_value = ["messi"]
    mock_windows.return_value = [{
        'window_key': 'w1',
        'event_version_key': 'ev1',
        'start': datetime(2022, 12, 1, tzinfo=timezone.utc),
        'end': datetime(2022, 12, 31, tzinfo=timezone.utc),
        'terms': []
    }]
    
    result = discover_videos(
        run_purpose="RESEARCH",
        frame_version_key="frame_1",
        use_search_fallback=True,
        run_search_call_budget=10
    )
    
    call_kwargs = mock_youtube.search().list.call_args_list[0][1]
    assert call_kwargs["pageToken"] == "resume_token_123"
    assert result["run_status"] == "COMPLETE"

@patch("discovery_youtube.build")
@patch("discovery_youtube.os.getenv")
@patch("discovery_youtube.get_snowflake_connection")
@patch("discovery_youtube.get_aliases")
@patch("discovery_youtube.get_event_windows_and_terms")
def test_http_429_quota_handling(mock_windows, mock_aliases, mock_get_conn, mock_getenv, mock_build):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    def fetchone_impl():
        if not mock_cursor.execute.call_args:
            return None
        sql = mock_cursor.execute.call_args[0][0]
        if "DIM_CHANNEL_FRAME_VERSION" in sql:
            return ("RESEARCH",)
        if "DIM_SAMPLING_POLICY_VERSION" in sql:
            return ("sp_key_1",)
        return None

    def fetchall_impl():
        if not mock_cursor.execute.call_args:
            return []
        sql = mock_cursor.execute.call_args[0][0]
        if "BRIDGE_FRAME_CHANNEL" in sql:
            return [("ch_key_1", "ch_id_1")]
        return []

    mock_cursor.fetchone.side_effect = fetchone_impl
    mock_cursor.fetchall.side_effect = fetchall_impl
    
    mock_youtube = MagicMock()
    mock_build.return_value = mock_youtube
    mock_req = MagicMock()
    mock_youtube.search().list.return_value = mock_req
    
    mock_resp = MagicMock()
    mock_resp.status = 429
    from googleapiclient.errors import HttpError
    mock_req.execute.side_effect = HttpError(mock_resp, b'{"error": {"message": "quotaExceeded"}}')
    
    mock_aliases.return_value = ["messi"]
    mock_windows.return_value = [{
        'window_key': 'w1',
        'event_version_key': 'ev1',
        'start': datetime(2022, 12, 1, tzinfo=timezone.utc),
        'end': datetime(2022, 12, 31, tzinfo=timezone.utc),
        'terms': []
    }]
    
    result = discover_videos(
        run_purpose="RESEARCH",
        frame_version_key="frame_1",
        use_search_fallback=True,
        run_search_call_budget=10
    )
    
    assert result["run_status"] == "PARTIAL_QUOTA_LIMIT"

@patch("discovery_youtube.build")
@patch("discovery_youtube.os.getenv")
@patch("discovery_youtube.get_snowflake_connection")
@patch("discovery_youtube.get_aliases")
@patch("discovery_youtube.get_event_windows_and_terms")
def test_non_quota_operational_failure(mock_windows, mock_aliases, mock_get_conn, mock_getenv, mock_build):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    def fetchone_impl():
        if not mock_cursor.execute.call_args:
            return None
        sql = mock_cursor.execute.call_args[0][0]
        if "DIM_CHANNEL_FRAME_VERSION" in sql:
            return ("RESEARCH",)
        if "DIM_SAMPLING_POLICY_VERSION" in sql:
            return ("sp_key_1",)
        return None

    def fetchall_impl():
        if not mock_cursor.execute.call_args:
            return []
        sql = mock_cursor.execute.call_args[0][0]
        if "BRIDGE_FRAME_CHANNEL" in sql:
            return [("ch_key_1", "ch_id_1")]
        return []

    mock_cursor.fetchone.side_effect = fetchone_impl
    mock_cursor.fetchall.side_effect = fetchall_impl
    
    mock_youtube = MagicMock()
    mock_build.return_value = mock_youtube
    mock_req = MagicMock()
    mock_youtube.search().list.return_value = mock_req
    
    mock_resp = MagicMock()
    mock_resp.status = 500
    from googleapiclient.errors import HttpError
    mock_req.execute.side_effect = HttpError(mock_resp, b'{"error": {"message": "Internal Server Error"}}')
    
    mock_aliases.return_value = ["messi"]
    mock_windows.return_value = [{
        'window_key': 'w1',
        'event_version_key': 'ev1',
        'start': datetime(2022, 12, 1, tzinfo=timezone.utc),
        'end': datetime(2022, 12, 31, tzinfo=timezone.utc),
        'terms': []
    }]
    
    result = discover_videos(
        run_purpose="RESEARCH",
        frame_version_key="frame_1",
        use_search_fallback=True,
        run_search_call_budget=10
    )
    
    assert result["run_status"] == "PARTIAL_ERROR"

def test_long_page_token_persistence():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = None
    
    long_token = "A" * 1500  # Token > 512 chars (e.g. 1500 chars)
    st_record = {
        "frame_version_key": "frame_1",
        "channel_key": "ch_1",
        "window_key": "w_1",
        "sampling_policy_version_key": "sp_1",
        "discovery_policy_version": "1.0",
        "query_batch_number": 1,
        "query_hash": "hash_123",
        "search_query": "test query",
        "status": "PARTIAL_QUOTA_LIMIT",
        "pages_completed": 2,
        "next_page_token": long_token,
        "items_observed": 50,
        "unique_video_ids_observed": 45,
        "search_calls_consumed": 2,
        "started_at": "2022-12-01T00:00:00Z",
        "completed_at": None,
        "ingestion_run_id": "run_1"
    }
    
    from discovery_youtube import upsert_discovery_unit_state
    unit_key = upsert_discovery_unit_state(mock_conn, st_record)
    assert unit_key is not None
    assert mock_cursor.execute.call_count >= 2

def test_pending_unit_null_started_at():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = None
    
    st_record = {
        "frame_version_key": "frame_1",
        "channel_key": "ch_1",
        "window_key": "w_1",
        "sampling_policy_version_key": "sp_1",
        "discovery_policy_version": "1.0",
        "query_batch_number": 1,
        "query_hash": "hash_123",
        "search_query": "test query",
        "status": "PENDING",
        "pages_completed": 0,
        "next_page_token": None,
        "items_observed": 0,
        "unique_video_ids_observed": 0,
        "search_calls_consumed": 0,
        "started_at": None,
        "completed_at": None,
        "ingestion_run_id": "run_1"
    }
    
    from discovery_youtube import upsert_discovery_unit_state
    upsert_discovery_unit_state(mock_conn, st_record)
    insert_args = mock_cursor.execute.call_args_list[1][0][1]
    started_at_arg = insert_args[15]
    assert started_at_arg is None

def test_discovery_policy_version_invalidation():
    channels = [("ch1_key", "ch1_id")]
    windows = [{
        'window_key': 'w1',
        'event_version_key': 'ev1',
        'start': datetime(2022, 12, 1, tzinfo=timezone.utc),
        'end': datetime(2022, 12, 31, tzinfo=timezone.utc),
        'terms': ['term1']
    }]
    aliases = ["alias1"]
    
    units_v1 = generate_discovery_units("frame1", channels, windows, aliases, "sp1", discovery_policy_version="1.0")
    units_v2 = generate_discovery_units("frame1", channels, windows, aliases, "sp1", discovery_policy_version="2.0")
    
    q_hash = units_v1[0]["query_hash"]
    key_v1 = (units_v1[0]["frame_version_key"], units_v1[0]["channel_key"], units_v1[0]["window_key"], "1.0", q_hash)
    
    existing_states = {
        key_v1: {"status": "COMPLETED"}
    }
    
    metrics_v2 = estimate_search_calls(units_v2, existing_states)
    assert metrics_v2["completed_units_count"] == 0
    assert metrics_v2["remaining_units_count"] == 1

def test_resumed_unit_clears_stale_errors():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = ("unit_1", "2022-12-01T00:00:00Z")
    
    st_record = {
        "frame_version_key": "frame_1",
        "channel_key": "ch_1",
        "window_key": "w_1",
        "sampling_policy_version_key": "sp_1",
        "discovery_policy_version": "1.0",
        "query_batch_number": 1,
        "query_hash": "hash_123",
        "search_query": "test query",
        "status": "COMPLETED",
        "pages_completed": 2,
        "next_page_token": None,
        "items_observed": 50,
        "unique_video_ids_observed": 45,
        "search_calls_consumed": 2,
        "started_at": "2022-12-01T00:00:00Z",
        "completed_at": "2022-12-01T01:00:00Z",
        "last_error_code": "HTTP_500",
        "last_error_message": "Stale Error",
        "ingestion_run_id": "run_2"
    }
    
    from discovery_youtube import upsert_discovery_unit_state
    upsert_discovery_unit_state(mock_conn, st_record)
    
    update_args = mock_cursor.execute.call_args_list[1][0][1]
    # Check that last_error_code and last_error_message are passed as None when status == COMPLETED
    last_err_code_arg = update_args[9]
    last_err_msg_arg = update_args[10]
    assert last_err_code_arg is None
    assert last_err_msg_arg is None

def test_v006_migration_upgrades_existing_table_without_data_loss():
    mig_path = os.path.join(os.path.dirname(__file__), '..', 'infra', 'snowflake', 'migrations', 'V006__create_discovery_unit_state.sql')
    assert os.path.exists(mig_path)
    with open(mig_path, 'r') as f:
        sql = f.read()
    assert "ALTER TABLE OPS.DISCOVERY_UNIT_STATE MODIFY COLUMN next_page_token VARCHAR(2048);" in sql
    assert "ALTER TABLE OPS.DISCOVERY_UNIT_STATE ALTER COLUMN started_at DROP NOT NULL;" in sql
    assert "ALTER TABLE OPS.DISCOVERY_UNIT_STATE ADD COLUMN IF NOT EXISTS last_error_code VARCHAR(128);" in sql
    assert "ALTER TABLE OPS.DISCOVERY_UNIT_STATE ADD COLUMN IF NOT EXISTS last_error_message VARCHAR(1024);" in sql

@patch("discovery_youtube.build")
@patch("discovery_youtube.os.getenv")
@patch("discovery_youtube.get_snowflake_connection")
@patch("discovery_youtube.get_aliases")
@patch("discovery_youtube.get_event_windows_and_terms")
def test_partial_error_recovery_lineage(mock_windows, mock_aliases, mock_get_conn, mock_getenv, mock_build):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    mock_aliases.return_value = ["messi"]
    mock_windows.return_value = [{
        'window_key': 'w1',
        'event_version_key': 'ev1',
        'start': datetime(2022, 12, 1, tzinfo=timezone.utc),
        'end': datetime(2022, 12, 31, tzinfo=timezone.utc),
        'terms': []
    }]

    q_hash = compute_query_hash('"messi"')
    unit_calls = 0
    def fetchall_impl():
        nonlocal unit_calls
        sql = mock_cursor.execute.call_args[0][0] if mock_cursor.execute.call_args else ""
        if "BRIDGE_FRAME_CHANNEL" in sql:
            return [("ch_key_1", "ch_id_1")]
        if "FROM OPS.DISCOVERY_UNIT_STATE" in sql:
            unit_calls += 1
            if unit_calls == 1:
                return [(
                    "unit_key_100", "frame_1", "ch_key_1", "w1", "sp_key_1", "1.0", 1,
                    q_hash,
                    '"messi"', "PARTIAL_ERROR", 0, None, 0, 0, 1,
                    "2022-12-01T00:00:00Z", "2022-12-01T00:01:00Z", None,
                    "API_ERROR", "[WinError 10054] connection closed",
                    "run_failed_001", "run_failed_001"
                )]
            else:
                return [(
                    "unit_key_100", "frame_1", "ch_key_1", "w1", "sp_key_1", "1.0", 1,
                    q_hash,
                    '"messi"', "COMPLETED", 1, None, 0, 0, 1,
                    "2022-12-01T00:00:00Z", "2022-12-01T00:01:00Z", "2022-12-01T00:02:00Z",
                    None, None,
                    "run_failed_001", "run_new"
                )]
        return []


    def fetchone_impl():
        sql = mock_cursor.execute.call_args[0][0] if mock_cursor.execute.call_args else ""
        if "DIM_CHANNEL_FRAME_VERSION" in sql:
            return ("RESEARCH",)
        if "DIM_SAMPLING_POLICY_VERSION" in sql:
            return ("sp_key_1",)
        if "FROM OPS.DISCOVERY_UNIT_STATE" in sql:
            return ("unit_key_100", "2022-12-01T00:00:00Z")
        return None

    mock_cursor.fetchall.side_effect = fetchall_impl
    mock_cursor.fetchone.side_effect = fetchone_impl

    mock_youtube = MagicMock()
    mock_build.return_value = mock_youtube
    mock_req = MagicMock()
    mock_youtube.search().list.return_value = mock_req
    mock_req.execute.return_value = {
        "items": [],
        "nextPageToken": None
    }

    result = discover_videos(
        run_purpose="RESEARCH",
        frame_version_key="frame_1",
        use_search_fallback=True,
        run_search_call_budget=10
    )

    assert result["run_status"] == "COMPLETE"

    # Verify upsert_discovery_unit_state issued UPDATE with cleared errors and new ingestion_run_id
    update_calls = [
        call for call in mock_cursor.execute.call_args_list 
        if "UPDATE OPS.DISCOVERY_UNIT_STATE" in call[0][0]
    ]
    assert len(update_calls) == 1
    update_args = update_calls[0][0][1]
    status_arg, pages_arg, token_arg, items_arg, unique_arg, calls_arg, started_arg, updated_arg, completed_arg, err_code_arg, err_msg_arg, run_id_arg, unit_key_arg = update_args

    assert status_arg == "COMPLETED"
    assert token_arg is None
    assert completed_arg is not None
    assert err_code_arg is None
    assert err_msg_arg is None
    assert run_id_arg == result["ingestion_run_id"]
    assert run_id_arg != "run_failed_001"
    assert unit_key_arg == "unit_key_100"


@patch("discovery_youtube.build")
@patch("discovery_youtube.os.getenv")
@patch("discovery_youtube.get_snowflake_connection")
@patch("discovery_youtube.get_aliases")
@patch("discovery_youtube.get_event_windows_and_terms")
def test_partial_error_page_token_preservation_and_resume(mock_windows, mock_aliases, mock_get_conn, mock_getenv, mock_build):
    """
    Offline regression test proving:
    Page 1 success -> Page 2 failure -> next_page_token preserved in PARTIAL_ERROR ->
    Resume from Page 2 -> COMPLETED while preserving first/latest run lineage.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    mock_aliases.return_value = ["messi"]
    mock_windows.return_value = [{
        'window_key': 'w1',
        'event_version_key': 'ev1',
        'start': datetime(2022, 12, 1, tzinfo=timezone.utc),
        'end': datetime(2022, 12, 31, tzinfo=timezone.utc),
        'terms': []
    }]
    q_hash = compute_query_hash('"messi"')

    # STEP 1: Execute Run 1 where Page 1 succeeds and Page 2 encounters a non-quota connection failure
    def fetchone_run1():
        sql = mock_cursor.execute.call_args[0][0] if mock_cursor.execute.call_args else ""
        if "DIM_CHANNEL_FRAME_VERSION" in sql:
            return ("RESEARCH",)
        if "DIM_SAMPLING_POLICY_VERSION" in sql:
            return ("sp_key_1",)
        if "FROM OPS.DISCOVERY_UNIT_STATE" in sql:
            return None
        return None

    def fetchall_run1():
        sql = mock_cursor.execute.call_args[0][0] if mock_cursor.execute.call_args else ""
        if "BRIDGE_FRAME_CHANNEL" in sql:
            return [("ch_key_1", "ch_id_1")]
        if "FROM OPS.DISCOVERY_UNIT_STATE" in sql:
            return []
        return []

    mock_cursor.fetchone.side_effect = fetchone_run1
    mock_cursor.fetchall.side_effect = fetchall_run1

    mock_youtube = MagicMock()
    mock_build.return_value = mock_youtube
    req_page1 = MagicMock()
    req_page2 = MagicMock()
    mock_youtube.search().list.side_effect = [req_page1, req_page2]

    req_page1.execute.return_value = {
        "items": [{"id": {"videoId": "v1"}, "snippet": {"publishedAt": "2022-12-05T12:00:00Z", "title": "v1", "description": ""}}],
        "nextPageToken": "token_for_page_2"
    }
    req_page2.execute.side_effect = ConnectionResetError("Connection dropped mid-batch")

    res1 = discover_videos(
        run_purpose="RESEARCH",
        frame_version_key="frame_1",
        use_search_fallback=True,
        run_search_call_budget=10
    )

    assert res1["run_status"] == "PARTIAL_ERROR"

    insert_calls = [
        call for call in mock_cursor.execute.call_args_list 
        if "INSERT INTO OPS.DISCOVERY_UNIT_STATE" in call[0][0]
    ]
    assert len(insert_calls) == 1
    ins_args = insert_calls[0][0][1]
    (u_key, f_key, ch_key, w_key, sp_key, dp_ver, q_batch, qh, query,
     status, pages_comp, token, items_obs, uniq_obs, calls_used,
     start_at, upd_at, comp_at, err_code, err_msg, first_run_id, latest_run_id) = ins_args

    assert status == "PARTIAL_ERROR"
    assert pages_comp == 1
    assert token == "token_for_page_2", "next_page_token must be preserved on PARTIAL_ERROR when page 1 succeeded"
    assert items_obs == 1
    assert uniq_obs == 1
    assert err_code == "API_ERROR"
    assert "Connection dropped" in err_msg
    assert first_run_id == res1["ingestion_run_id"]
    assert latest_run_id == res1["ingestion_run_id"]

    # STEP 2: Resume in Run 2 from the persisted PARTIAL_ERROR state
    mock_cursor.reset_mock()
    unit_calls = 0

    def fetchone_run2():
        sql = mock_cursor.execute.call_args[0][0] if mock_cursor.execute.call_args else ""
        if "DIM_CHANNEL_FRAME_VERSION" in sql:
            return ("RESEARCH",)
        if "DIM_SAMPLING_POLICY_VERSION" in sql:
            return ("sp_key_1",)
        if "FROM OPS.DISCOVERY_UNIT_STATE" in sql:
            return (u_key, start_at)
        return None

    def fetchall_run2():
        nonlocal unit_calls
        sql = mock_cursor.execute.call_args[0][0] if mock_cursor.execute.call_args else ""
        if "BRIDGE_FRAME_CHANNEL" in sql:
            return [("ch_key_1", "ch_id_1")]
        if "FROM OPS.DISCOVERY_UNIT_STATE" in sql:
            unit_calls += 1
            if unit_calls == 1:
                return [(
                    u_key, "frame_1", "ch_key_1", "w1", "sp_key_1", "1.0", 1,
                    q_hash, '"messi"', "PARTIAL_ERROR", 1, "token_for_page_2", 1, 1, 2,
                    start_at, upd_at, None, "API_ERROR", "Connection dropped",
                    res1["ingestion_run_id"], res1["ingestion_run_id"]
                )]
            else:
                return [(
                    u_key, "frame_1", "ch_key_1", "w1", "sp_key_1", "1.0", 1,
                    q_hash, '"messi"', "COMPLETED", 2, None, 2, 2, 3,
                    start_at, upd_at, "2022-12-05T12:05:00Z", None, None,
                    res1["ingestion_run_id"], "run2_id"
                )]
        return []

    mock_cursor.fetchone.side_effect = fetchone_run2
    mock_cursor.fetchall.side_effect = fetchall_run2

    req_page2_resume = MagicMock()
    mock_youtube.search().list.side_effect = None
    mock_youtube.search().list.return_value = req_page2_resume
    req_page2_resume.execute.return_value = {
        "items": [{"id": {"videoId": "v2"}, "snippet": {"publishedAt": "2022-12-06T12:00:00Z", "title": "v2", "description": ""}}],
        "nextPageToken": None
    }

    res2 = discover_videos(
        run_purpose="RESEARCH",
        frame_version_key="frame_1",
        use_search_fallback=True,
        run_search_call_budget=10
    )

    call_kwargs = mock_youtube.search().list.call_args[1]
    assert call_kwargs["pageToken"] == "token_for_page_2"
    assert res2["run_status"] == "COMPLETE"

    update_calls = [
        call for call in mock_cursor.execute.call_args_list 
        if "UPDATE OPS.DISCOVERY_UNIT_STATE" in call[0][0]
    ]
    assert len(update_calls) == 1
    upd_args = update_calls[0][0][1]
    (upd_status, upd_pages, upd_token, upd_items, upd_uniq, upd_calls,
     upd_started, upd_updated, upd_completed, upd_err_code, upd_err_msg,
     upd_run_id, upd_key) = upd_args

    assert upd_status == "COMPLETED"
    assert upd_pages == 2
    assert upd_token is None
    assert upd_items == 2
    assert upd_uniq == 2
    assert upd_completed is not None
    assert upd_err_code is None
    assert upd_err_msg is None
    assert upd_run_id == res2["ingestion_run_id"]
    assert upd_run_id != res1["ingestion_run_id"]
    assert upd_key == u_key


@patch("discovery_youtube.build")
@patch("discovery_youtube.os.getenv")
@patch("discovery_youtube.get_snowflake_connection")
@patch("discovery_youtube.get_aliases")
@patch("discovery_youtube.get_event_windows_and_terms")
def test_http_403_distinguishes_quota_from_forbidden(mock_windows, mock_aliases, mock_get_conn, mock_getenv, mock_build):
    """
    Proves that HTTP 403 is NOT automatically classified as quota exhaustion.
    Ordinary 403 (accessNotConfigured/forbidden) becomes PARTIAL_ERROR with HTTP_403.
    Only documented quota reasons (quotaExceeded) become PARTIAL_QUOTA_LIMIT.
    """
    from googleapiclient.errors import HttpError
    from httplib2 import Response

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    mock_aliases.return_value = ["messi"]
    mock_windows.return_value = [{
        'window_key': 'w1',
        'event_version_key': 'ev1',
        'start': datetime(2022, 12, 1, tzinfo=timezone.utc),
        'end': datetime(2022, 12, 31, tzinfo=timezone.utc),
        'terms': []
    }]

    def fetchone_impl():
        sql = mock_cursor.execute.call_args[0][0] if mock_cursor.execute.call_args else ""
        if "DIM_CHANNEL_FRAME_VERSION" in sql:
            return ("RESEARCH",)
        if "DIM_SAMPLING_POLICY_VERSION" in sql:
            return ("sp_key_1",)
        return None

    def fetchall_impl():
        sql = mock_cursor.execute.call_args[0][0] if mock_cursor.execute.call_args else ""
        if "BRIDGE_FRAME_CHANNEL" in sql:
            return [("ch_key_1", "ch_id_1")]
        return []

    mock_cursor.fetchone.side_effect = fetchone_impl
    mock_cursor.fetchall.side_effect = fetchall_impl

    mock_youtube = MagicMock()
    mock_build.return_value = mock_youtube
    mock_req = MagicMock()
    mock_youtube.search().list.return_value = mock_req

    # Case 1: HTTP 403 with ordinary auth/forbidden reason (e.g., accessNotConfigured)
    resp_forbidden = Response({"status": "403"})
    err_forbidden = HttpError(resp_forbidden, b'{"error": {"errors": [{"reason": "accessNotConfigured", "message": "API not enabled"}]}}')
    mock_req.execute.side_effect = err_forbidden

    res_auth = discover_videos(
        run_purpose="RESEARCH",
        frame_version_key="frame_1",
        use_search_fallback=True,
        run_search_call_budget=10
    )
    assert res_auth["run_status"] == "PARTIAL_ERROR", "Ordinary HTTP 403 must be PARTIAL_ERROR, not quota limit"

    # Case 2: HTTP 403 with documented quota reason (quotaExceeded)
    resp_quota = Response({"status": "403"})
    err_quota = HttpError(resp_quota, b'{"error": {"errors": [{"reason": "quotaExceeded", "message": "The request cannot be completed because you have exceeded your quota."}]}}')
    mock_req.execute.side_effect = err_quota

    res_quota = discover_videos(
        run_purpose="RESEARCH",
        frame_version_key="frame_1",
        use_search_fallback=True,
        run_search_call_budget=10
    )
    assert res_quota["run_status"] == "PARTIAL_QUOTA_LIMIT", "HTTP 403 with quotaExceeded must be PARTIAL_QUOTA_LIMIT"


@patch("discovery_youtube.build")
@patch("discovery_youtube.os.getenv")
@patch("discovery_youtube.get_snowflake_connection")
@patch("discovery_youtube.get_aliases")
@patch("discovery_youtube.get_event_windows_and_terms")
def test_unique_video_ids_observed_cumulative_dedup(mock_windows, mock_aliases, mock_get_conn, mock_getenv, mock_build):
    """
    Proves that unique_video_ids_observed is semantically correct across resumed executions.
    If run 1 observed video 'v1' and resumed run 2 observes 'v1' and 'v2',
    unique_video_ids_observed is 2 (cumulative and deduplicated), not 1 or 3.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    mock_aliases.return_value = ["messi"]
    mock_windows.return_value = [{
        'window_key': 'w1',
        'event_version_key': 'ev1',
        'start': datetime(2022, 12, 1, tzinfo=timezone.utc),
        'end': datetime(2022, 12, 31, tzinfo=timezone.utc),
        'terms': []
    }]
    q_hash = compute_query_hash('"messi"')

    unit_calls = 0

    def fetchone_impl():
        sql = mock_cursor.execute.call_args[0][0] if mock_cursor.execute.call_args else ""
        if "DIM_CHANNEL_FRAME_VERSION" in sql:
            return ("RESEARCH",)
        if "DIM_SAMPLING_POLICY_VERSION" in sql:
            return ("sp_key_1",)
        if "FROM OPS.DISCOVERY_UNIT_STATE" in sql:
            return ("u1", "2022-12-01T00:00:00Z")
        return None

    def fetchall_impl():
        nonlocal unit_calls
        sql = mock_cursor.execute.call_args[0][0] if mock_cursor.execute.call_args else ""
        if "BRIDGE_FRAME_CHANNEL" in sql:
            return [("ch_key_1", "ch_id_1")]
        if "SELECT DISTINCT v.source_id" in sql:
            # Return previously observed video 'v1' from database
            return [("v1",)]
        if "FROM OPS.DISCOVERY_UNIT_STATE" in sql:
            unit_calls += 1
            if unit_calls == 1:
                # Prior state had 1 item, 1 unique video ('v1')
                return [(
                    "u1", "frame_1", "ch_key_1", "w1", "sp_key_1", "1.0", 1,
                    q_hash, '"messi"', "PARTIAL_QUOTA_LIMIT", 1, "tok2", 1, 1, 1,
                    "2022-12-01T00:00:00Z", "2022-12-01T00:01:00Z", None, None, None,
                    "run1", "run1"
                )]
            else:
                return [(
                    "u1", "frame_1", "ch_key_1", "w1", "sp_key_1", "1.0", 1,
                    q_hash, '"messi"', "COMPLETED", 2, None, 3, 2, 2,
                    "2022-12-01T00:00:00Z", "2022-12-01T00:02:00Z", "2022-12-01T00:02:00Z", None, None,
                    "run1", "run2"
                )]
        return []

    mock_cursor.fetchone.side_effect = fetchone_impl
    mock_cursor.fetchall.side_effect = fetchall_impl

    mock_youtube = MagicMock()
    mock_build.return_value = mock_youtube
    req = MagicMock()
    mock_youtube.search().list.return_value = req
    # Resumed page 2 returns 'v1' (duplicate from page 1) and 'v2' (new)
    req.execute.return_value = {
        "items": [
            {"id": {"videoId": "v1"}, "snippet": {"publishedAt": "2022-12-05T12:00:00Z", "title": "v1", "description": ""}},
            {"id": {"videoId": "v2"}, "snippet": {"publishedAt": "2022-12-06T12:00:00Z", "title": "v2", "description": ""}}
        ],
        "nextPageToken": None
    }

    res = discover_videos(
        run_purpose="RESEARCH",
        frame_version_key="frame_1",
        use_search_fallback=True,
        run_search_call_budget=10
    )

    assert res["run_status"] == "COMPLETE"

    update_calls = [
        call for call in mock_cursor.execute.call_args_list 
        if "UPDATE OPS.DISCOVERY_UNIT_STATE" in call[0][0]
    ]
    assert len(update_calls) == 1
    upd_args = update_calls[0][0][1]
    upd_items = upd_args[3]
    upd_uniq = upd_args[4]

    # items_observed: 1 (from prior) + 2 (from page 2) = 3
    assert upd_items == 3
    # unique_video_ids_observed: set union of {'v1'} | {'v1', 'v2'} = 2
    assert upd_uniq == 2
