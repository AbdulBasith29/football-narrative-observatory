import pytest
from datetime import datetime, timezone
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))
from discovery_youtube import check_eligibility

def test_alias_resolution_and_exclusion():
    aliases = ["messi", "ronaldo"]
    
    windows = [{
        'window_key': 'w1',
        'event_version_key': 'ev1',
        'start': datetime(2022, 12, 1, tzinfo=timezone.utc),
        'end': datetime(2022, 12, 31, tzinfo=timezone.utc),
        'terms': ['world cup']
    }]

    # Case 1: In window, has alias
    video_1 = {
        'snippet': {
            'title': 'Messi scores a great goal',
            'description': '',
            'publishedAt': '2022-12-15T10:00:00Z'
        }
    }
    status, reason, ev_key = check_eligibility(video_1, aliases, windows)
    assert status == 'ELIGIBLE'
    assert reason is None
    assert ev_key == 'ev1'
    
    # Case 2: In window, NO alias but HAS event term (baseline relevance)
    video_2 = {
        'snippet': {
            'title': 'France vs Argentina',
            'description': 'Amazing world cup final football',
            'publishedAt': '2022-12-15T10:00:00Z'
        }
    }
    status, reason, ev_key = check_eligibility(video_2, aliases, windows)
    assert status == 'ELIGIBLE'
    assert reason is None
    assert ev_key == 'ev1'

    # Case 3: In window, no alias, no event term
    video_3 = {
        'snippet': {
            'title': 'Mbappe scores a great goal',
            'description': 'Amazing football',
            'publishedAt': '2022-12-15T10:00:00Z'
        }
    }
    status, reason, ev_key = check_eligibility(video_3, aliases, windows)
    assert status == 'INELIGIBLE'
    assert reason == 'NO_TARGET_RELEVANCE'
    
    # Case 4: Has alias, OUT of window
    video_4 = {
        'snippet': {
            'title': 'Ronaldo highlights',
            'description': '',
            'publishedAt': '2023-01-15T10:00:00Z'
        }
    }
    status, reason, ev_key = check_eligibility(video_4, aliases, windows)
    assert status == 'INELIGIBLE'
    assert reason == 'OUT_OF_WINDOW'

def test_alias_word_boundaries():
    aliases = ["cr7"]
    windows = [{
        'window_key': 'w1',
        'event_version_key': 'ev1',
        'start': datetime(2022, 1, 1, tzinfo=timezone.utc),
        'end': datetime(2023, 1, 1, tzinfo=timezone.utc),
        'terms': []
    }]
    
    # Exact match
    video_1 = {
        'snippet': {
            'title': 'The best is CR7!',
            'description': '',
            'publishedAt': '2022-06-15T10:00:00Z'
        }
    }
    status, _, _ = check_eligibility(video_1, aliases, windows)
    assert status == 'ELIGIBLE'
    
    # Substring match shouldn't trigger
    video_2 = {
        'snippet': {
            'title': 'CR700 model specs',
            'description': '',
            'publishedAt': '2022-06-15T10:00:00Z'
        }
    }
    status, reason, _ = check_eligibility(video_2, aliases, windows)
    assert status == 'INELIGIBLE'
    assert reason == 'NO_TARGET_RELEVANCE'

def test_discovery_method_equivalence():
    # Proves the same logical video gets same outcome regardless of discovery method
    aliases = ["messi"]
    windows = [{
        'window_key': 'w1',
        'event_version_key': 'ev1',
        'start': datetime(2022, 12, 1, tzinfo=timezone.utc),
        'end': datetime(2022, 12, 31, tzinfo=timezone.utc),
        'terms': []
    }]
    
    # Mock video as returned by playlistItems.list
    video_playlist = {
        'snippet': {
            'title': 'Messi is great',
            'description': '',
            'publishedAt': '2022-12-15T10:00:00Z',
            'resourceId': {'videoId': 'vid123'}
        }
    }
    
    # Mock video as returned by search.list
    video_search = {
        'id': {'videoId': 'vid123'},
        'snippet': {
            'title': 'Messi is great',
            'description': '',
            'publishedAt': '2022-12-15T10:00:00Z'
        }
    }
    
    # Check playlist method
    status_1, reason_1, ev_key_1 = check_eligibility(video_playlist, aliases, windows)
    
    # The discovery_youtube.py maps search.list videoId to snippet.resourceId.videoId before checking eligibility
    # So we simulate that mapping here:
    video_search['snippet']['resourceId'] = {'videoId': video_search['id']['videoId']}
    status_2, reason_2, ev_key_2 = check_eligibility(video_search, aliases, windows)
    
    assert status_1 == status_2 == 'ELIGIBLE'
    assert reason_1 == reason_2 == None
    assert ev_key_1 == ev_key_2 == 'ev1'
