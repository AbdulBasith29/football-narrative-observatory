-- 03_core_dimensions.sql
-- Bootstraps the CORE schema dimension tables.

USE DATABASE FOOTBALL_NARRATIVE_DEV;
USE SCHEMA CORE;

CREATE TABLE IF NOT EXISTS DIM_DATE (
    date_key DATE NOT NULL PRIMARY KEY,
    day_of_week NUMBER(38,0),
    month NUMBER(38,0),
    year NUMBER(38,0)
);

CREATE TABLE IF NOT EXISTS DIM_TIME (
    time_key VARCHAR(8) NOT NULL PRIMARY KEY,
    hour NUMBER(38,0),
    minute NUMBER(38,0),
    second NUMBER(38,0)
);

CREATE TABLE IF NOT EXISTS DIM_CHANNEL (
    channel_key VARCHAR(36) NOT NULL PRIMARY KEY,
    source_system VARCHAR(128) NOT NULL,
    source_id VARCHAR(128) NOT NULL,
    channel_name VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS DIM_VIDEO (
    video_key VARCHAR(36) NOT NULL PRIMARY KEY,
    channel_key VARCHAR(36) NOT NULL,
    source_system VARCHAR(128) NOT NULL,
    source_id VARCHAR(128) NOT NULL,
    published_at TIMESTAMP_NTZ,
    duration_seconds NUMBER(38,0),
    is_short BOOLEAN,
    short_classification_rule_version VARCHAR(64),
    primary_language_code VARCHAR(16),
    content_type VARCHAR(64),
    first_observed_at TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS DIM_AUTHOR (
    author_key VARCHAR(36) NOT NULL PRIMARY KEY,
    author_hash VARCHAR(64) NOT NULL,
    salt_version_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS DIM_EVENT (
    event_key VARCHAR(36) NOT NULL PRIMARY KEY,
    external_event_id VARCHAR(128) NOT NULL,
    event_type VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS DIM_EVENT_VERSION (
    event_version_key VARCHAR(36) NOT NULL PRIMARY KEY,
    event_key VARCHAR(36) NOT NULL,
    event_name VARCHAR(256),
    occurred_at TIMESTAMP_NTZ,
    inclusion_rationale VARCHAR(512),
    event_terms VARIANT,
    valid_from TIMESTAMP_NTZ NOT NULL,
    valid_to TIMESTAMP_NTZ,
    is_current BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS DIM_EVENT_WINDOW (
    window_key VARCHAR(36) NOT NULL PRIMARY KEY,
    event_version_key VARCHAR(36) NOT NULL,
    event_catalogue_version_key VARCHAR(36),
    window_definition_version_key VARCHAR(36),
    window_type VARCHAR(64) NOT NULL,
    window_sequence NUMBER(38,0),
    relative_offset_start NUMBER(38,0),
    relative_offset_end NUMBER(38,0),
    absolute_start_at TIMESTAMP_NTZ,
    absolute_end_at TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS DIM_TARGET_ENTITY (
    target_entity_key VARCHAR(36) NOT NULL PRIMARY KEY,
    entity_name VARCHAR(256) NOT NULL,
    entity_type VARCHAR(128),
    wikidata_id VARCHAR(128),
    transfermarkt_id VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS DIM_TARGET_ENTITY_ALIAS (
    alias_key VARCHAR(36) NOT NULL PRIMARY KEY,
    target_entity_key VARCHAR(36) NOT NULL,
    alias_text VARCHAR(256) NOT NULL,
    alias_type VARCHAR(64),
    valid_from TIMESTAMP_NTZ NOT NULL,
    valid_to TIMESTAMP_NTZ,
    is_active BOOLEAN,
    source VARCHAR(128),
    review_status VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS DIM_CHANNEL_STRATUM (
    stratum_key VARCHAR(36) NOT NULL PRIMARY KEY,
    channel_type_value VARCHAR(128) NOT NULL,
    player_focus_value VARCHAR(128) NOT NULL,
    stratum_name VARCHAR(256),
    analytical_eligibility BOOLEAN
);

CREATE TABLE IF NOT EXISTS DIM_CHANNEL_STRATUM_VERSION (
    stratum_version_key VARCHAR(36) NOT NULL PRIMARY KEY,
    channel_key VARCHAR(36) NOT NULL,
    stratum_key VARCHAR(36) NOT NULL,
    valid_from TIMESTAMP_NTZ NOT NULL,
    valid_to TIMESTAMP_NTZ,
    is_current BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS DIM_MODEL_VERSION (
    model_version_key VARCHAR(36) NOT NULL PRIMARY KEY,
    model_name VARCHAR(256) NOT NULL,
    task_name VARCHAR(128),
    model_type VARCHAR(128),
    provider VARCHAR(128),
    checkpoint VARCHAR(256),
    prompt_version VARCHAR(64),
    training_data_version VARCHAR(64),
    label_schema_version VARCHAR(64),
    threshold_config_version VARCHAR(64),
    code_commit_sha VARCHAR(64),
    created_at TIMESTAMP_NTZ,
    retired_at TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS DIM_MODEL_BUNDLE_VERSION (
    model_bundle_version_key VARCHAR(36) NOT NULL PRIMARY KEY,
    bundle_name VARCHAR(256) NOT NULL,
    valence_model_version_key VARCHAR(36),
    derision_model_version_key VARCHAR(36),
    toxicity_model_version_key VARCHAR(36),
    sarcasm_model_version_key VARCHAR(36),
    target_resolution_model_version_key VARCHAR(36),
    language_model_version_key VARCHAR(36),
    narrative_model_version_key VARCHAR(36),
    stance_model_version_key VARCHAR(36),
    threshold_config_version_key VARCHAR(36),
    valid_from TIMESTAMP_NTZ NOT NULL,
    valid_to TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS DIM_CONSTRUCT (
    construct_key VARCHAR(36) NOT NULL PRIMARY KEY,
    construct_name VARCHAR(128) NOT NULL,
    description VARCHAR(512)
);

CREATE TABLE IF NOT EXISTS DIM_TAXONOMY_VERSION (
    taxonomy_version_key VARCHAR(36) NOT NULL PRIMARY KEY,
    taxonomy_name VARCHAR(256) NOT NULL,
    taxonomy_version VARCHAR(64) NOT NULL,
    valid_from TIMESTAMP_NTZ NOT NULL,
    valid_to TIMESTAMP_NTZ,
    definition_hash VARCHAR(64),
    created_at TIMESTAMP_NTZ,
    retired_at TIMESTAMP_NTZ,
    code_commit_sha VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS DIM_SAMPLING_POLICY_VERSION (
    sampling_policy_version_key VARCHAR(36) NOT NULL PRIMARY KEY,
    policy_name VARCHAR(256),
    valid_from TIMESTAMP_NTZ NOT NULL,
    valid_to TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS DIM_EVENT_CATALOGUE_VERSION (
    event_catalogue_version_key VARCHAR(36) NOT NULL PRIMARY KEY,
    catalogue_name VARCHAR(256),
    valid_from TIMESTAMP_NTZ NOT NULL,
    valid_to TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS DIM_THRESHOLD_CONFIG_VERSION (
    threshold_config_version_key VARCHAR(36) NOT NULL PRIMARY KEY,
    config_name VARCHAR(256),
    valid_from TIMESTAMP_NTZ NOT NULL,
    valid_to TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS DIM_NARRATIVE_FAMILY (
    narrative_family_key VARCHAR(36) NOT NULL PRIMARY KEY,
    family_name VARCHAR(256) NOT NULL,
    family_description VARCHAR(512)
);

CREATE TABLE IF NOT EXISTS DIM_NARRATIVE_INSTANCE (
    narrative_instance_key VARCHAR(36) NOT NULL PRIMARY KEY,
    taxonomy_version_key VARCHAR(36) NOT NULL,
    canonical_name VARCHAR(256) NOT NULL,
    semantic_definition VARCHAR(512),
    origin_event_version_key VARCHAR(36),
    originated_at TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS DIM_COMPARISON_STRATUM (
    comparison_stratum_key VARCHAR(36) NOT NULL PRIMARY KEY,
    stratum_name VARCHAR(256) NOT NULL
);
