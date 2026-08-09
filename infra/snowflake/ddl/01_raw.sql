-- 01_raw.sql
-- Bootstraps the RAW schema tables.

USE DATABASE FOOTBALL_NARRATIVE_DEV;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS YOUTUBE_API_RESPONSE (
    raw_response_id VARCHAR(36) NOT NULL,
    api_request_id VARCHAR(36) NOT NULL,
    ingestion_run_id VARCHAR(36) NOT NULL,

    source_system VARCHAR(128) NOT NULL,
    endpoint VARCHAR(128) NOT NULL,

    resource_scope_type VARCHAR(128) NOT NULL,
    resource_scope_id VARCHAR(128) NOT NULL,

    request_parameters VARIANT NOT NULL,
    http_status NUMBER(3,0),

    page_token_used VARCHAR(1024),
    next_page_token_returned VARCHAR(1024),

    retrieved_at TIMESTAMP_NTZ NOT NULL,

    raw_json_payload VARIANT NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,

    parser_version VARCHAR(128),
    source_schema_version VARCHAR(128),

    CONSTRAINT pk_youtube_api_response
        PRIMARY KEY (raw_response_id),

    CONSTRAINT uq_youtube_api_response_request
        UNIQUE (api_request_id)
);
