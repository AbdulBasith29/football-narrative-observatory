-- V007__add_metadata_resolution_and_snapshot_lineage.sql
-- 1. Create durable operational state table for video metadata resolution in OPS schema
-- 2. Add api_request_id and raw_response_id to FACT_VIDEO_SNAPSHOT for replay idempotency

USE DATABASE FOOTBALL_NARRATIVE_DEV;

-- 1. Create OPS.VIDEO_METADATA_RESOLUTION_STATE
CREATE TABLE IF NOT EXISTS OPS.VIDEO_METADATA_RESOLUTION_STATE (
    resolution_state_key VARCHAR(36) NOT NULL PRIMARY KEY,
    source_system VARCHAR(128) NOT NULL,
    source_id VARCHAR(128) NOT NULL,
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

-- 2. Add request and raw lineage to CORE.FACT_VIDEO_SNAPSHOT
ALTER TABLE CORE.FACT_VIDEO_SNAPSHOT ADD COLUMN IF NOT EXISTS api_request_id VARCHAR(36);
ALTER TABLE CORE.FACT_VIDEO_SNAPSHOT ADD COLUMN IF NOT EXISTS raw_response_id VARCHAR(36);
