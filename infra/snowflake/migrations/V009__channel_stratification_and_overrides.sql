-- V009__channel_stratification_and_overrides.sql
-- Implements Phase 1E Channel Stratification, Evidence Manifest, Acquisition State, and Overrides.

USE DATABASE FOOTBALL_NARRATIVE_DEV;

-- 1. Seed 30 Composite Strata in CORE.DIM_CHANNEL_STRATUM
USE SCHEMA CORE;

MERGE INTO CORE.DIM_CHANNEL_STRATUM AS target
USING (
    WITH types AS (
        SELECT column1 AS channel_type_value FROM (VALUES
            ('BROAD_REACH_PUBLISHER'),
            ('CLUB_MEDIA'),
            ('ANALYSIS_PUBLISHER'),
            ('INDEPENDENT_CREATOR'),
            ('OTHER'),
            ('UNCLASSIFIED')
        )
    ),
    foci AS (
        SELECT column1 AS player_focus_value FROM (VALUES
            ('MESSI_FOCUSED'),
            ('RONALDO_FOCUSED'),
            ('MIXED_FOCUS'),
            ('NO_STRONG_DOMINANT_PLAYER_FOCUS'),
            ('UNCLASSIFIED')
        )
    )
    SELECT
        -- Deterministic surrogate key formatted as standard UUID
        LOWER(
            SUBSTR(MD5(channel_type_value || ':' || player_focus_value), 1, 8) || '-' ||
            SUBSTR(MD5(channel_type_value || ':' || player_focus_value), 9, 4) || '-' ||
            SUBSTR(MD5(channel_type_value || ':' || player_focus_value), 13, 4) || '-' ||
            SUBSTR(MD5(channel_type_value || ':' || player_focus_value), 17, 4) || '-' ||
            SUBSTR(MD5(channel_type_value || ':' || player_focus_value), 21, 12)
        ) AS stratum_key,
        channel_type_value,
        player_focus_value,
        channel_type_value || ' x ' || player_focus_value AS stratum_name,
        CASE
            WHEN channel_type_value = 'UNCLASSIFIED' OR player_focus_value = 'UNCLASSIFIED' THEN FALSE
            ELSE TRUE
        END AS analytical_eligibility
    FROM types
    CROSS JOIN foci
) AS src
ON target.channel_type_value = src.channel_type_value 
   AND target.player_focus_value = src.player_focus_value
WHEN MATCHED THEN
    UPDATE SET 
        target.stratum_name = src.stratum_name,
        target.analytical_eligibility = src.analytical_eligibility
WHEN NOT MATCHED THEN
    INSERT (stratum_key, channel_type_value, player_focus_value, stratum_name, analytical_eligibility)
    VALUES (src.stratum_key, src.channel_type_value, src.player_focus_value, src.stratum_name, src.analytical_eligibility);

-- 2. Alter CORE.BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT
ALTER TABLE CORE.BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT ADD COLUMN IF NOT EXISTS headline_decomposition_eligible BOOLEAN;
EXECUTE IMMEDIATE '
BEGIN
    ALTER TABLE CORE.BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT 
    ADD CONSTRAINT uq_event_channel_stratum_snapshot 
    UNIQUE (event_version_key, channel_key, classification_protocol_version);
EXCEPTION
    WHEN OTHER THEN
        NULL;
END;
';
ALTER TABLE CORE.BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT MODIFY COLUMN focus_ratio NUMBER(10,5);

-- 3. Operations schema tables
USE SCHEMA OPS;

-- Enhanced Assessment Telemetry
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS assessment_trigger VARCHAR(64);
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS event_version_key VARCHAR(36);
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS classification_protocol_version VARCHAR(64);
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS rule_config_version VARCHAR(64);
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS rule_config_hash VARCHAR(64);
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS evidence_manifest_hash VARCHAR(64);
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS reference_period_start TIMESTAMP_NTZ;
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS reference_period_end TIMESTAMP_NTZ;
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS effective_window_days NUMBER(38,0);
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS messi_video_count NUMBER(38,0);
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS ronaldo_video_count NUMBER(38,0);
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS messi_prevalence NUMBER(6,5);
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS ronaldo_prevalence NUMBER(6,5);
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS focus_ratio NUMBER(10,5);
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT MODIFY COLUMN focus_ratio NUMBER(10,5);
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS fallback_used BOOLEAN;
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS deduplication_rule_version VARCHAR(64);
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS channel_type_evidence VARIANT;
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS player_focus_evidence VARIANT;
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS git_commit_sha VARCHAR(40);
ALTER TABLE OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT ADD COLUMN IF NOT EXISTS ingestion_run_id VARCHAR(36);

-- Immutable Evidence Manifest Table
CREATE TABLE IF NOT EXISTS OPS.FACT_CHANNEL_ASSESSMENT_EVIDENCE_ITEM (
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

-- Evidence Acquisition State Machine Table
CREATE TABLE IF NOT EXISTS OPS.CHANNEL_EVIDENCE_ACQUISITION_STATE (
    acquisition_state_key VARCHAR(36) NOT NULL PRIMARY KEY,
    frame_version_key VARCHAR(36) NOT NULL,
    channel_key VARCHAR(36) NOT NULL,
    source_channel_id VARCHAR(128) NOT NULL,
    uploads_playlist_id VARCHAR(128) NOT NULL,
    scan_scope VARCHAR(32) NOT NULL, -- '90D_PRIMARY' or '180D_FALLBACK'
    window_start_at TIMESTAMP_NTZ NOT NULL,
    window_end_at TIMESTAMP_NTZ NOT NULL,
    status VARCHAR(32) NOT NULL, -- 'PENDING', 'IN_PROGRESS', 'PARTIAL_QUOTA_LIMIT', 'PARTIAL_ERROR', 'COMPLETED'
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

-- Structured Manual Overrides
ALTER TABLE OPS.MANUAL_OVERRIDE ADD COLUMN IF NOT EXISTS classification_axis_overridden VARCHAR(64);
ALTER TABLE OPS.MANUAL_OVERRIDE ADD COLUMN IF NOT EXISTS original_axis_value VARCHAR(128);
ALTER TABLE OPS.MANUAL_OVERRIDE ADD COLUMN IF NOT EXISTS proposed_axis_value VARCHAR(128);
ALTER TABLE OPS.MANUAL_OVERRIDE ADD COLUMN IF NOT EXISTS supporting_evidence VARCHAR(1024);
ALTER TABLE OPS.MANUAL_OVERRIDE ADD COLUMN IF NOT EXISTS review_status VARCHAR(64);
ALTER TABLE OPS.MANUAL_OVERRIDE ADD COLUMN IF NOT EXISTS approved_by_reviewer_identity VARCHAR(128);
ALTER TABLE OPS.MANUAL_OVERRIDE ADD COLUMN IF NOT EXISTS approved_at TIMESTAMP_NTZ;
