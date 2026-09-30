-- 02_ops.sql
-- Bootstraps the OPS schema tables.

USE DATABASE FOOTBALL_NARRATIVE_DEV;
USE SCHEMA OPS;

CREATE TABLE IF NOT EXISTS FACT_INGESTION_RUN (
    ingestion_run_id VARCHAR(36) NOT NULL,
    source_system VARCHAR(128) NOT NULL,
    endpoint VARCHAR(128) NOT NULL,
    resource_scope_type VARCHAR(128) NOT NULL,
    resource_scope_id VARCHAR(128) NOT NULL,
    retrieval_mode VARCHAR(64),
    sample_type VARCHAR(64),
    execution_attempt NUMBER(38,0),
    started_at TIMESTAMP_NTZ NOT NULL,
    completed_at TIMESTAMP_NTZ,
    pages_requested NUMBER(38,0),
    pages_succeeded NUMBER(38,0),
    records_observed NUMBER(38,0),
    records_inserted NUMBER(38,0),
    duplicate_records_observed NUMBER(38,0),
    estimated_quota_consumed NUMBER(38,0),
    error_code VARCHAR(128),
    run_outcome VARCHAR(64),
    continuity_status VARCHAR(64),
    source_availability_status VARCHAR(64),
    backfill_status VARCHAR(64),
    run_purpose VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS FACT_API_REQUEST (
    api_request_id VARCHAR(36) NOT NULL,
    ingestion_run_id VARCHAR(36) NOT NULL,
    endpoint VARCHAR(128) NOT NULL,
    requested_at TIMESTAMP_NTZ NOT NULL,
    completed_at TIMESTAMP_NTZ,
    http_status NUMBER(3,0),
    page_token_used VARCHAR(1024),
    next_page_token_returned VARCHAR(1024),
    retry_number NUMBER(38,0),
    error_code VARCHAR(128),
    estimated_quota_cost NUMBER(38,0)
);

CREATE TABLE IF NOT EXISTS CURRENT_CHECKPOINT (
    checkpoint_id VARCHAR(36) NOT NULL,
    source_system VARCHAR(128) NOT NULL,
    endpoint VARCHAR(128) NOT NULL,
    resource_scope_type VARCHAR(128) NOT NULL,
    resource_scope_id VARCHAR(128) NOT NULL,
    retrieval_mode VARCHAR(64),
    sample_type VARCHAR(64),
    committed_watermark_at TIMESTAMP_NTZ,
    candidate_watermark_at TIMESTAMP_NTZ,
    last_complete_scan_at TIMESTAMP_NTZ,
    continuation_token_hint VARCHAR(1024),
    current_continuity_state VARCHAR(64),
    checkpoint_transaction_lineage VARCHAR(256),
    updated_at TIMESTAMP_NTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS CHECKPOINT_HISTORY (
    checkpoint_history_id VARCHAR(36) NOT NULL,
    checkpoint_id VARCHAR(36) NOT NULL,
    committed_watermark_at TIMESTAMP_NTZ,
    candidate_watermark_at TIMESTAMP_NTZ,
    last_complete_scan_at TIMESTAMP_NTZ,
    continuation_token_hint VARCHAR(1024),
    current_continuity_state VARCHAR(64),
    checkpoint_transaction_lineage VARCHAR(256),
    updated_at TIMESTAMP_NTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS CHECKPOINT_WATERMARK_COMMENT (
    checkpoint_id VARCHAR(36) NOT NULL,
    source_comment_id VARCHAR(128) NOT NULL,
    watermark_type VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS CHECKPOINT_WATERMARK_COMMENT_HISTORY (
    checkpoint_history_id VARCHAR(36) NOT NULL,
    source_comment_id VARCHAR(128) NOT NULL,
    watermark_type VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS FACT_CHANNEL_CLASSIFICATION_ASSESSMENT (
    assessment_id VARCHAR(36) NOT NULL PRIMARY KEY,
    channel_key VARCHAR(36) NOT NULL,
    assessed_at TIMESTAMP_NTZ NOT NULL,
    assessment_trigger VARCHAR(64),
    event_version_key VARCHAR(36),
    classification_protocol_version VARCHAR(64),
    rule_config_version VARCHAR(64),
    rule_config_hash VARCHAR(64),
    evidence_manifest_hash VARCHAR(64),
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
    focus_ratio NUMBER(10,5),
    fallback_used BOOLEAN,
    deduplication_rule_version VARCHAR(64),
    channel_type_evidence VARIANT,
    player_focus_evidence VARIANT,
    git_commit_sha VARCHAR(40),
    ingestion_run_id VARCHAR(36)
);

CREATE TABLE IF NOT EXISTS FACT_CHANNEL_ASSESSMENT_EVIDENCE_ITEM (
    assessment_evidence_item_key VARCHAR(36) NOT NULL PRIMARY KEY,
    assessment_id VARCHAR(36) NOT NULL,
    channel_key VARCHAR(36) NOT NULL,
    video_key VARCHAR(36) NOT NULL,
    source_video_id VARCHAR(128) NOT NULL,
    published_at TIMESTAMP_NTZ NOT NULL,
    duration_seconds NUMBER(38,0),
    is_short BOOLEAN,
    raw_response_id VARCHAR(36) NOT NULL,
    api_request_id VARCHAR(36) NOT NULL,
    messi_alias_matched BOOLEAN NOT NULL,
    ronaldo_alias_matched BOOLEAN NOT NULL,
    matched_player_aliases VARIANT,
    matched_club_entities VARIANT,
    matched_analysis_topics VARIANT,
    is_deduplicated_duplicate BOOLEAN NOT NULL,
    deduplication_cluster_id VARCHAR(64),
    included_in_prevalence BOOLEAN NOT NULL,
    CONSTRAINT uq_assessment_video_evidence UNIQUE (assessment_id, video_key)
);

CREATE TABLE IF NOT EXISTS CHANNEL_EVIDENCE_ACQUISITION_STATE (
    acquisition_state_key VARCHAR(36) NOT NULL PRIMARY KEY,
    frame_version_key VARCHAR(36) NOT NULL,
    channel_key VARCHAR(36) NOT NULL,
    source_channel_id VARCHAR(128) NOT NULL,
    uploads_playlist_id VARCHAR(128) NOT NULL,
    scan_scope VARCHAR(32) NOT NULL,
    window_start_at TIMESTAMP_NTZ NOT NULL,
    window_end_at TIMESTAMP_NTZ NOT NULL,
    status VARCHAR(32) NOT NULL,
    next_page_token VARCHAR(2048),
    pages_completed NUMBER(38,0) DEFAULT 0,
    oldest_observed_published_at TIMESTAMP_NTZ,
    items_observed NUMBER(38,0) DEFAULT 0,
    eligible_videos_observed NUMBER(38,0) DEFAULT 0,
    api_calls_consumed NUMBER(38,0) DEFAULT 0,
    first_ingestion_run_id VARCHAR(36) NOT NULL,
    latest_ingestion_run_id VARCHAR(36) NOT NULL,
    last_api_request_id VARCHAR(36),
    retry_count NUMBER(38,0) DEFAULT 0,
    last_error_code VARCHAR(128),
    last_error_message VARCHAR(1024),
    started_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ NOT NULL,
    completed_at TIMESTAMP_NTZ,
    CONSTRAINT uq_channel_evidence_scope UNIQUE (frame_version_key, channel_key, window_end_at, scan_scope)
);

CREATE TABLE IF NOT EXISTS FACT_BACKFILL_JOB (
    backfill_run_id VARCHAR(36) NOT NULL,
    originating_hot_path_run_id VARCHAR(36),
    gap_start_at TIMESTAMP_NTZ,
    gap_end_at TIMESTAMP_NTZ,
    oldest_recovered_at TIMESTAMP_NTZ,
    backfill_status VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS DEAD_LETTER_RECORD (
    dead_letter_id VARCHAR(36) NOT NULL,
    raw_response_id VARCHAR(36),
    source_record_id VARCHAR(128),
    ingestion_run_id VARCHAR(36),
    processing_stage VARCHAR(64),
    error_code VARCHAR(128),
    error_message VARCHAR(512),
    first_failure_at TIMESTAMP_NTZ NOT NULL,
    retry_count NUMBER(38,0),
    resolution_status VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS MANUAL_OVERRIDE (
    override_id VARCHAR(36) NOT NULL PRIMARY KEY,
    target_table VARCHAR(128) NOT NULL,
    target_key VARCHAR(128) NOT NULL,
    override_reason VARCHAR(512),
    reviewer_identity VARCHAR(128),
    override_timestamp TIMESTAMP_NTZ NOT NULL,
    classification_axis_overridden VARCHAR(64),
    original_axis_value VARCHAR(128),
    proposed_axis_value VARCHAR(128),
    supporting_evidence VARCHAR(1024),
    review_status VARCHAR(64),
    approved_by_reviewer_identity VARCHAR(128),
    approved_at TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS DISCOVERY_UNIT_STATE (
    discovery_unit_key VARCHAR(36) NOT NULL PRIMARY KEY,
    frame_version_key VARCHAR(36) NOT NULL,
    channel_key VARCHAR(36) NOT NULL,
    window_key VARCHAR(64) NOT NULL,
    sampling_policy_version_key VARCHAR(36) NOT NULL,
    discovery_policy_version VARCHAR(32) NOT NULL,
    query_batch_number NUMBER(38,0) NOT NULL,
    query_hash VARCHAR(64) NOT NULL,
    search_query VARCHAR(1024) NOT NULL,
    status VARCHAR(32) NOT NULL,
    pages_completed NUMBER(38,0) DEFAULT 0,
    next_page_token VARCHAR(2048),
    items_observed NUMBER(38,0) DEFAULT 0,
    unique_video_ids_observed NUMBER(38,0) DEFAULT 0,
    search_calls_consumed NUMBER(38,0) DEFAULT 0,
    started_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ NOT NULL,
    completed_at TIMESTAMP_NTZ,
    last_error_code VARCHAR(128),
    last_error_message VARCHAR(1024),
    first_ingestion_run_id VARCHAR(36),
    latest_ingestion_run_id VARCHAR(36),
    CONSTRAINT uq_discovery_unit_state UNIQUE (frame_version_key, channel_key, window_key, discovery_policy_version, query_hash)
);

CREATE TABLE IF NOT EXISTS VIDEO_METADATA_RESOLUTION_STATE (
    resolution_state_key VARCHAR(36) NOT NULL PRIMARY KEY,
    source_system VARCHAR(128) NOT NULL,
    source_id VARCHAR(128) NOT NULL,
    -- Approved states: PENDING, RESOLVED, UNAVAILABLE, RETRYABLE_ERROR, PARSE_ERROR, FATAL_ERROR
    resolution_status VARCHAR(32) NOT NULL,
    first_discovered_at TIMESTAMP_NTZ NOT NULL,
    resolved_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ NOT NULL,
    api_request_id VARCHAR(36),
    raw_response_id VARCHAR(36),
    first_ingestion_run_id VARCHAR(36) NOT NULL,
    latest_ingestion_run_id VARCHAR(36) NOT NULL,
    last_error_code VARCHAR(128),
    last_error_message VARCHAR(1024),
    retry_count NUMBER(38,0) DEFAULT 0,
    CONSTRAINT uq_video_metadata_resolution_state UNIQUE (source_system, source_id)
);


