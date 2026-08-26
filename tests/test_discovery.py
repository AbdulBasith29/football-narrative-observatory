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
    
    expected_units = generate_discovery_units("frame1", channels, windows, aliases, "sp1")
    assert len(expected_units) == 2
    
    metrics = estimate_search_calls(expected_units, {})
    assert metrics["total_expected_units"] == 2
    assert metrics["completed_units_count"] == 0
    assert metrics["remaining_units_count"] == 2
    assert metrics["minimum_required_calls"] == 2
    
    unit1_key = (expected_units[0]["channel_key"], expected_units[0]["window_key"], expected_units[0]["query_hash"])
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
            return ("unit_1",)
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
                return [("unit_1", "ch_key_1", "w1", q_hash, "PARTIAL_QUOTA_LIMIT", 1, "resume_token_123", 1, 10, 10)]
            else:
                return [("unit_1", "ch_key_1", "w1", q_hash, "COMPLETED", 2, None, 2, 20, 20)]
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
