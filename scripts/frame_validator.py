class FrameConstraintViolation(Exception):
    pass

def validate_bridge_frame_channel_uniqueness(records):
    """
    Validates that (frame_version_key, channel_key) is unique across the records.
    """
    seen = set()
    for rec in records:
        key = (rec['frame_version_key'], rec['channel_key'])
        if key in seen:
            raise FrameConstraintViolation(f"Duplicate bridge frame channel entry found: {key}")
        seen.add(key)
    return True

def validate_frame_channel_source_observation_uniqueness(records):
    """
    Validates that (frame_channel_key, discovery_source, discovery_source_version) is unique.
    """
    seen = set()
    for rec in records:
        key = (rec['frame_channel_key'], rec['discovery_source'], rec['discovery_source_version'])
        if key in seen:
            raise FrameConstraintViolation(f"Duplicate frame channel source observation found: {key}")
        seen.add(key)
    return True
