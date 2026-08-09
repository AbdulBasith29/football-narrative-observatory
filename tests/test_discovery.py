import pytest
from datetime import datetime, timezone
import sys
import os
from unittest.mock import patch, MagicMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))
from discovery_youtube import evaluate_video_against_events, discover_videos, fetch_playlist_items, fetch_search_items

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

    # Simulate that the DB returns 'PIPELINE_PILOT' for this frame_version_key
    mock_cursor.fetchone.return_value = ("PIPELINE_PILOT",)
    
    # Running RESEARCH purpose on a PIPELINE_PILOT frame must structurally fail
    with pytest.raises(ValueError, match="RESEARCH discovery structurally rejects PIPELINE_PILOT frames."):
        discover_videos(run_purpose="RESEARCH", frame_version_key="pilot-frame-id")
        
    # Running INTEGRATION_TEST on a PIPELINE_PILOT should not raise this specific error
    # (It will fail later in the mock because get_aliases etc., but we can catch that)
    mock_cursor.fetchone.return_value = ("PIPELINE_PILOT",)
    mock_cursor.fetchall.return_value = [] # no eligible channels
    try:
        discover_videos(run_purpose="INTEGRATION_TEST", frame_version_key="pilot-frame-id")
    except ValueError as e:
        if "structurally rejects" in str(e):
            pytest.fail("INTEGRATION_TEST should not reject PIPELINE_PILOT frames")

def test_pagination_playlist_items():
    mock_youtube = MagicMock()
    mock_req_1 = MagicMock()
    mock_req_2 = MagicMock()
    
    mock_youtube.playlistItems().list.side_effect = [mock_req_1, mock_req_2]
    
    mock_req_1.execute.return_value = {
        "items": [{"id": "1"}],
        "nextPageToken": "token1"
    }
    mock_req_2.execute.return_value = {
        "items": [{"id": "2"}]
    }
    
    items = fetch_playlist_items(mock_youtube, "playlist_id")
    assert len(items) == 2
    assert items[0]["id"] == "1"
    assert items[1]["id"] == "2"
    assert mock_youtube.playlistItems().list.call_count == 2

def test_pagination_search_items():
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
    
    items = fetch_search_items(mock_youtube, "ch1", "query", "min", "max")
    assert len(items) == 2
    assert items[0]["snippet"]["resourceId"]["videoId"] == "vid1"
    assert items[1]["snippet"]["resourceId"]["videoId"] == "vid2"
    assert mock_youtube.search().list.call_count == 2

@patch("discovery_youtube.build")
@patch("discovery_youtube.os.getenv")
@patch("discovery_youtube.get_snowflake_connection")
@patch("discovery_youtube.get_aliases")
@patch("discovery_youtube.get_event_windows_and_terms")
def test_idempotency_check(mock_windows, mock_aliases, mock_get_conn, mock_getenv, mock_build):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    # 1. frame_purpose (not PIPELINE_PILOT)
    # 2. eligible channels
    # 3. sampling policy version key
    # 4. video existence check
    # 5. idempotency check (BRIDGE_VIDEO_EVENT)
    mock_cursor.fetchone.side_effect = [
        ("RESEARCH",),       # frame_purpose
        ("sp_key_1",),       # sampling_policy
        ("vid_key_1",),      # video exists
        ("bve_key_1",)       # BRIDGE_VIDEO_EVENT exists -> should TRIGGER IDEMPOTENCY CONTINUE
    ]
    
    mock_cursor.fetchall.side_effect = [
        [("ch_key_1", "ch_id_1")] # channels
    ]
    
    mock_youtube = MagicMock()
    mock_build.return_value = mock_youtube
    mock_ch_resp = MagicMock()
    mock_ch_resp.execute.return_value = {"items": [{"contentDetails": {"relatedPlaylists": {"uploads": "up1"}}}]}
    mock_youtube.channels().list.return_value = mock_ch_resp
    
    mock_pl_resp = MagicMock()
    mock_pl_resp.execute.return_value = {
        "items": [{
            "snippet": {
                "resourceId": {"videoId": "v1"},
                "publishedAt": "2022-12-15T10:00:00Z",
                "title": "messi",
                "description": ""
            }
        }]
    }
    mock_youtube.playlistItems().list.return_value = mock_pl_resp
    
    mock_aliases.return_value = ["messi"]
    mock_windows.return_value = [{
        'window_key': 'w1',
        'event_version_key': 'ev1',
        'start': datetime(2022, 12, 1, tzinfo=timezone.utc),
        'end': datetime(2022, 12, 31, tzinfo=timezone.utc),
        'terms': []
    }]
    
    discover_videos(run_purpose="RESEARCH", frame_version_key="frame_1")
    
    # We should see NO INSERT INTO CORE.BRIDGE_VIDEO_EVENT because it hit continue
    insert_calls = [call for call in mock_cursor.execute.call_args_list if "INSERT INTO CORE.BRIDGE_VIDEO_EVENT" in call[0][0]]
    assert len(insert_calls) == 0
