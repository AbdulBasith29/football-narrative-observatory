import pytest
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))
from frame_validator import (
    validate_bridge_frame_channel_uniqueness,
    validate_frame_channel_source_observation_uniqueness,
    FrameConstraintViolation
)

def test_bridge_frame_channel_uniqueness():
    valid_records = [
        {'frame_version_key': 'v1', 'channel_key': 'ch1'},
        {'frame_version_key': 'v1', 'channel_key': 'ch2'},
        {'frame_version_key': 'v2', 'channel_key': 'ch1'},
    ]
    assert validate_bridge_frame_channel_uniqueness(valid_records) == True
    
    invalid_records = [
        {'frame_version_key': 'v1', 'channel_key': 'ch1'},
        {'frame_version_key': 'v1', 'channel_key': 'ch1'}, # Duplicate
    ]
    with pytest.raises(FrameConstraintViolation):
        validate_bridge_frame_channel_uniqueness(invalid_records)

def test_frame_channel_source_observation_uniqueness():
    valid_records = [
        {'frame_channel_key': 'fc1', 'discovery_source': 'wiki', 'discovery_source_version': '2023'},
        {'frame_channel_key': 'fc1', 'discovery_source': 'socialblade', 'discovery_source_version': '2023'},
        {'frame_channel_key': 'fc2', 'discovery_source': 'wiki', 'discovery_source_version': '2023'},
    ]
    assert validate_frame_channel_source_observation_uniqueness(valid_records) == True
    
    invalid_records = [
        {'frame_channel_key': 'fc1', 'discovery_source': 'wiki', 'discovery_source_version': '2023'},
        {'frame_channel_key': 'fc1', 'discovery_source': 'wiki', 'discovery_source_version': '2023'}, # Duplicate
    ]
    with pytest.raises(FrameConstraintViolation):
        validate_frame_channel_source_observation_uniqueness(invalid_records)
