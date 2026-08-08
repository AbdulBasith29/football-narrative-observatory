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
    backfill_status VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS FACT_API_REQUEST (
    api_request_id VARCHAR(36) NOT NULL,
    ingestion_run_id VARCHAR(36) NOT NULL,
    endpoint VARCHAR(128) NOT NULL,
    requested_at TIMESTAMP_NTZ NOT NULL,
    completed_at TIMESTAMP_NTZ,
    http_status NUMBER(38,0),
    page_token_used VARCHAR(128),
    next_page_token_returned VARCHAR(128),
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
    continuation_token_hint VARCHAR(128),
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
    continuation_token_hint VARCHAR(128),
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
    assessment_id VARCHAR(36) NOT NULL,
    channel_key VARCHAR(36) NOT NULL,
    assessed_at TIMESTAMP_NTZ NOT NULL,
    channel_type_value VARCHAR(128),
    player_focus_value VARCHAR(128),
    channel_type_confidence VARCHAR(64),
    player_focus_confidence VARCHAR(64),
    eligible_video_count NUMBER(38,0)
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
    override_id VARCHAR(36) NOT NULL,
    target_table VARCHAR(128) NOT NULL,
    target_key VARCHAR(128) NOT NULL,
    override_reason VARCHAR(512),
    reviewer_identity VARCHAR(128),
    override_timestamp TIMESTAMP_NTZ NOT NULL
);
