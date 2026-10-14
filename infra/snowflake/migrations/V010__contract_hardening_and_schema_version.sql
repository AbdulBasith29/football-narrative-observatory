-- V010__contract_hardening_and_schema_version.sql
-- Implements Phase 2A Migration Ledger, Concurrency Lock, and Core Contract Hardening.

USE DATABASE FOOTBALL_NARRATIVE_DEV;

-- =============================================================================
-- 1. OPS Schema: Migration Ledger & Concurrency Lock
-- =============================================================================
USE SCHEMA OPS;

CREATE TABLE IF NOT EXISTS OPS.SCHEMA_VERSION (
    installed_rank      NUMBER(38,0) NOT NULL,
    version             VARCHAR(32) NOT NULL,
    description         VARCHAR(256) NOT NULL,
    script_name         VARCHAR(256) NOT NULL,
    checksum            VARCHAR(64) NOT NULL,
    execution_type      VARCHAR(32) NOT NULL, -- 'MIGRATION', 'BASELINE_ADOPTION', 'REPLAY'
    installed_by        VARCHAR(128) NOT NULL,
    installed_on        TIMESTAMP_NTZ NOT NULL,
    execution_time_ms   NUMBER(38,0) NOT NULL,
    status              VARCHAR(32) NOT NULL, -- 'SUCCESS', 'FAILED', 'IN_PROGRESS'
    error_message       VARCHAR(2048),
    CONSTRAINT pk_schema_version PRIMARY KEY (version),
    CONSTRAINT uq_schema_version_rank UNIQUE (installed_rank)
);

CREATE TABLE IF NOT EXISTS OPS.SCHEMA_LOCK (
    lock_id             VARCHAR(32) NOT NULL,
    locked_at           TIMESTAMP_NTZ,
    locked_by           VARCHAR(128),
    lock_token          VARCHAR(36),
    lock_timeout_sec    NUMBER(38,0) NOT NULL DEFAULT 900,
    CONSTRAINT pk_schema_lock PRIMARY KEY (lock_id)
);

MERGE INTO OPS.SCHEMA_LOCK AS target
USING (SELECT 'MIGRATION_RUNNER_LOCK' AS lock_id) AS src
ON target.lock_id = src.lock_id
WHEN NOT MATCHED THEN
    INSERT (lock_id, locked_at, locked_by, lock_token, lock_timeout_sec)
    VALUES (src.lock_id, NULL, NULL, NULL, 900);

-- =============================================================================
-- 2. OPS Schema: Primary Key Constraint Hardening
-- Adds explicit informational PK metadata to unconstrained operational tables.
-- =============================================================================

EXECUTE IMMEDIATE '
BEGIN
    ALTER TABLE OPS.FACT_INGESTION_RUN ADD CONSTRAINT pk_fact_ingestion_run PRIMARY KEY (ingestion_run_id);
EXCEPTION
    WHEN OTHER THEN
        IF (SQLSTATE = ''42710'' OR SQLCODE = 2002 OR SQLERRM ILIKE ''%already exists%'') THEN
            NULL;
        ELSE
            RAISE;
        END IF;
END;
';

EXECUTE IMMEDIATE '
BEGIN
    ALTER TABLE OPS.FACT_API_REQUEST ADD CONSTRAINT pk_fact_api_request PRIMARY KEY (api_request_id);
EXCEPTION
    WHEN OTHER THEN
        IF (SQLSTATE = ''42710'' OR SQLCODE = 2002 OR SQLERRM ILIKE ''%already exists%'') THEN
            NULL;
        ELSE
            RAISE;
        END IF;
END;
';

EXECUTE IMMEDIATE '
BEGIN
    ALTER TABLE OPS.CURRENT_CHECKPOINT ADD CONSTRAINT pk_current_checkpoint PRIMARY KEY (checkpoint_id);
EXCEPTION
    WHEN OTHER THEN
        IF (SQLSTATE = ''42710'' OR SQLCODE = 2002 OR SQLERRM ILIKE ''%already exists%'') THEN
            NULL;
        ELSE
            RAISE;
        END IF;
END;
';

EXECUTE IMMEDIATE '
BEGIN
    ALTER TABLE OPS.CHECKPOINT_HISTORY ADD CONSTRAINT pk_checkpoint_history PRIMARY KEY (checkpoint_history_id);
EXCEPTION
    WHEN OTHER THEN
        IF (SQLSTATE = ''42710'' OR SQLCODE = 2002 OR SQLERRM ILIKE ''%already exists%'') THEN
            NULL;
        ELSE
            RAISE;
        END IF;
END;
';

EXECUTE IMMEDIATE '
BEGIN
    ALTER TABLE OPS.FACT_BACKFILL_JOB ADD CONSTRAINT pk_fact_backfill_job PRIMARY KEY (backfill_run_id);
EXCEPTION
    WHEN OTHER THEN
        IF (SQLSTATE = ''42710'' OR SQLCODE = 2002 OR SQLERRM ILIKE ''%already exists%'') THEN
            NULL;
        ELSE
            RAISE;
        END IF;
END;
';

EXECUTE IMMEDIATE '
BEGIN
    ALTER TABLE OPS.DEAD_LETTER_RECORD ADD CONSTRAINT pk_dead_letter_record PRIMARY KEY (dead_letter_id);
EXCEPTION
    WHEN OTHER THEN
        IF (SQLSTATE = ''42710'' OR SQLCODE = 2002 OR SQLERRM ILIKE ''%already exists%'') THEN
            NULL;
        ELSE
            RAISE;
        END IF;
END;
';

-- =============================================================================
-- 3. CORE Schema: Composite Natural-Key Unique Metadata
-- Hardens CORE.DIM_CHANNEL_STRATUM with unique constraint on (type, focus).
-- =============================================================================
USE SCHEMA CORE;

EXECUTE IMMEDIATE '
BEGIN
    ALTER TABLE CORE.DIM_CHANNEL_STRATUM 
    ADD CONSTRAINT uq_dim_channel_stratum 
    UNIQUE (channel_type_value, player_focus_value);
EXCEPTION
    WHEN OTHER THEN
        IF (SQLSTATE = ''42710'' OR SQLCODE = 2002 OR SQLERRM ILIKE ''%already exists%'') THEN
            NULL;
        ELSE
            RAISE;
        END IF;
END;
';
