import pytest
from datetime import datetime, timezone
import sys
import os
from unittest.mock import patch, MagicMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))
from discovery_youtube import evaluate_video_against_events, discover_videos

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
