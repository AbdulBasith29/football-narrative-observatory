-- 04_core_facts.sql
-- Bootstraps the CORE schema fact tables.

USE DATABASE FOOTBALL_NARRATIVE_DEV;
USE SCHEMA CORE;

CREATE TABLE IF NOT EXISTS FACT_COMMENT (
    comment_key VARCHAR(36) NOT NULL PRIMARY KEY,
    source_system VARCHAR(128) NOT NULL,
    source_comment_id VARCHAR(128) NOT NULL,
    video_key VARCHAR(36) NOT NULL,
    parent_comment_key VARCHAR(36),
    author_key VARCHAR(36) NOT NULL,
    published_at TIMESTAMP_NTZ NOT NULL,
    first_observed_at TIMESTAMP_NTZ NOT NULL,
    is_reply BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS FACT_COMMENT_AVAILABILITY_SNAPSHOT (
    availability_snapshot_key VARCHAR(36) NOT NULL PRIMARY KEY,
    comment_key VARCHAR(36) NOT NULL,
    observed_at TIMESTAMP_NTZ NOT NULL,
    availability_status VARCHAR(64) NOT NULL,
    source_error_reason VARCHAR(256),
    ingestion_run_id VARCHAR(36) NOT NULL
);

CREATE TABLE IF NOT EXISTS FACT_COMMENT_TEXT_VERSION (
    text_version_key VARCHAR(36) NOT NULL PRIMARY KEY,
    comment_key VARCHAR(36) NOT NULL,
    text_hash VARCHAR(64) NOT NULL,
    text_content VARCHAR(10000),
    source_updated_at TIMESTAMP_NTZ,
    observed_at TIMESTAMP_NTZ NOT NULL,
    is_latest_version BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS FACT_COMMENT_ENGAGEMENT_SNAPSHOT (
    engagement_snapshot_key VARCHAR(36) NOT NULL PRIMARY KEY,
    comment_key VARCHAR(36) NOT NULL,
    observed_at TIMESTAMP_NTZ NOT NULL,
    likes NUMBER(38,0),
    reply_count NUMBER(38,0),
    ingestion_run_id VARCHAR(36) NOT NULL
);

CREATE TABLE IF NOT EXISTS FACT_VIDEO_SNAPSHOT (
    video_snapshot_key VARCHAR(36) NOT NULL PRIMARY KEY,
    video_key VARCHAR(36) NOT NULL,
    observed_at TIMESTAMP_NTZ NOT NULL,
    title VARCHAR(512),
    description VARCHAR(5000),
    views NUMBER(38,0),
    likes NUMBER(38,0),
    ingestion_run_id VARCHAR(36) NOT NULL
);
