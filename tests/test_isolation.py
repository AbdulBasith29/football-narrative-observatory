import pytest
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))
from discovery_youtube import discover_videos

def test_run_purpose_enforcement():
    # Should raise ValueError if not INTEGRATION_TEST or RESEARCH
    with pytest.raises(ValueError, match="run_purpose must be 'INTEGRATION_TEST' or 'RESEARCH'"):
        discover_videos(run_purpose="DEV_TEST")
    
    with pytest.raises(ValueError):
        discover_videos(run_purpose=None)
