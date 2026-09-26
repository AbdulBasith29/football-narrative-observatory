-- 05_core_bridges.sql
-- Bootstraps the CORE schema bridge tables.

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

CREATE TABLE IF NOT EXISTS BRIDGE_EVENT_ENTITY (
    event_entity_key VARCHAR(36) NOT NULL PRIMARY KEY,
    event_version_key VARCHAR(36) NOT NULL,
    target_entity_key VARCHAR(36) NOT NULL,
    entity_role VARCHAR(128) NOT NULL
);

CREATE TABLE IF NOT EXISTS BRIDGE_VIDEO_EVENT (
    video_event_key VARCHAR(36) NOT NULL PRIMARY KEY,
    video_key VARCHAR(36) NOT NULL,
    event_version_key VARCHAR(36) NOT NULL,
    sampling_policy_version_key VARCHAR(36) NOT NULL,
    cohort_type VARCHAR(128),
    selection_rank NUMBER(38,0),
    inclusion_status VARCHAR(64),
    inclusion_rationale VARCHAR(512),
    primary_exclusion_reason VARCHAR(128),
    discovery_method VARCHAR(64),
    discovery_provenance VARIANT,
    candidate_list_lineage VARCHAR(256),
    override_id VARCHAR(36)
);

CREATE TABLE IF NOT EXISTS BRIDGE_COMMENT_EVENT (
    comment_event_key VARCHAR(36) NOT NULL PRIMARY KEY,
    text_version_key VARCHAR(36) NOT NULL,
    event_version_key VARCHAR(36) NOT NULL,
    association_model_version_key VARCHAR(36) NOT NULL,
    association_method VARCHAR(128),
    association_score NUMBER(6,5),
    time_distance NUMBER(38,0),
    is_primary_event BOOLEAN,
    classification_status VARCHAR(64),
    margin_over_second_best NUMBER(6,5)
);

CREATE TABLE IF NOT EXISTS BRIDGE_COMMENT_SAMPLE_OBSERVATION (
    comment_sample_observation_key VARCHAR(36) NOT NULL PRIMARY KEY,
    comment_key VARCHAR(36) NOT NULL,
    ingestion_run_id VARCHAR(36),
    api_request_id VARCHAR(36),
    raw_response_id VARCHAR(36) NOT NULL,
    sample_type VARCHAR(64),
    page_number NUMBER(38,0),
    rank_position NUMBER(38,0),
    source_item_position NUMBER(38,0) NOT NULL,
    observed_at TIMESTAMP_NTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS BRIDGE_EVENT_COMPARISON_STRATUM (
    event_comparison_stratum_key VARCHAR(36) NOT NULL PRIMARY KEY,
    event_version_key VARCHAR(36) NOT NULL,
    target_entity_key VARCHAR(36) NOT NULL,
    comparison_stratum_key VARCHAR(36) NOT NULL
);

CREATE TABLE IF NOT EXISTS BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT (
    event_channel_stratum_snapshot_key VARCHAR(36) NOT NULL PRIMARY KEY,
    event_version_key VARCHAR(36) NOT NULL,
    channel_key VARCHAR(36) NOT NULL,
    classification_protocol_version VARCHAR(64) NOT NULL,
    channel_type_value VARCHAR(128),
    player_focus_value VARCHAR(128),
    channel_type_confidence VARCHAR(64),
    player_focus_confidence VARCHAR(64),
    reference_period_start TIMESTAMP_NTZ,
    reference_period_end TIMESTAMP_NTZ,
    effective_window_days NUMBER(38,0),
    eligible_video_count NUMBER(38,0),
    messi_video_count NUMBER(38,0),
    ronaldo_video_count NUMBER(38,0),
    messi_prevalence NUMBER(6,5),
    ronaldo_prevalence NUMBER(6,5),
    focus_ratio NUMBER(6,5),
    fallback_used BOOLEAN,
    override_id VARCHAR(36)
);

CREATE TABLE IF NOT EXISTS BRIDGE_COMMENT_TARGET (
    comment_target_key VARCHAR(36) NOT NULL PRIMARY KEY,
    text_version_key VARCHAR(36) NOT NULL,
    target_entity_key VARCHAR(36) NOT NULL,
    target_resolution_model_version_key VARCHAR(36) NOT NULL,
    resolution_method VARCHAR(128),
    confidence NUMBER(6,5),
    classification_status VARCHAR(64),
    is_primary_target BOOLEAN
);
