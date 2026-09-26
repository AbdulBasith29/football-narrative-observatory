USE DATABASE FOOTBALL_NARRATIVE_DEV;
USE SCHEMA CORE;

CREATE TABLE IF NOT EXISTS DIM_CHANNEL_FRAME_VERSION (
    frame_version_key VARCHAR(36) NOT NULL PRIMARY KEY,
    frame_name VARCHAR(256) NOT NULL,
    frame_purpose VARCHAR(64) NOT NULL,
    methodology_version VARCHAR(64) NOT NULL,
    configuration_version VARCHAR(64) NOT NULL,
    configuration_hash VARCHAR(128) NOT NULL,
    git_commit_sha VARCHAR(40) NOT NULL,
    constructed_at TIMESTAMP_NTZ NOT NULL,
    configuration_provenance VARIANT
);

CREATE TABLE IF NOT EXISTS BRIDGE_FRAME_CHANNEL (
    frame_channel_key VARCHAR(36) NOT NULL PRIMARY KEY,
    frame_version_key VARCHAR(36) NOT NULL,
    channel_key VARCHAR(36) NOT NULL,
    evaluated_at TIMESTAMP_NTZ NOT NULL,
    eligibility_rule_version VARCHAR(64) NOT NULL,
    frame_inclusion_status VARCHAR(64) NOT NULL,
    general_exclusion_reason VARCHAR(128),
    CONSTRAINT uq_bridge_frame_channel UNIQUE (frame_version_key, channel_key)
);

CREATE TABLE IF NOT EXISTS FRAME_CHANNEL_SOURCE_OBSERVATION (
    frame_channel_source_observation_key VARCHAR(36) NOT NULL PRIMARY KEY,
    frame_channel_key VARCHAR(36) NOT NULL,
    discovery_source VARCHAR(256) NOT NULL,
    discovery_source_version VARCHAR(64) NOT NULL,
    source_record_identifier VARCHAR(128),
    source_retrieved_at TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT uq_frame_channel_source_observation UNIQUE (frame_channel_key, discovery_source, discovery_source_version)
);
