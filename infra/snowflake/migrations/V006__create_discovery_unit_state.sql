USE DATABASE FOOTBALL_NARRATIVE_DEV;
USE SCHEMA OPS;

-- 1. Create table if not exists
CREATE TABLE IF NOT EXISTS OPS.DISCOVERY_UNIT_STATE (
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

-- 2. Upgrade existing table in-place without deleting state
ALTER TABLE OPS.DISCOVERY_UNIT_STATE MODIFY COLUMN next_page_token VARCHAR(2048);
ALTER TABLE OPS.DISCOVERY_UNIT_STATE ALTER COLUMN started_at DROP NOT NULL;
ALTER TABLE OPS.DISCOVERY_UNIT_STATE ADD COLUMN IF NOT EXISTS last_error_code VARCHAR(128);
ALTER TABLE OPS.DISCOVERY_UNIT_STATE ADD COLUMN IF NOT EXISTS last_error_message VARCHAR(1024);
