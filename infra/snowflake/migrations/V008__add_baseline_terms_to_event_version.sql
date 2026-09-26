-- V008__add_baseline_terms_to_event_version.sql
-- Adds pre-registered baseline_terms VARIANT column to CORE.DIM_EVENT_VERSION
-- for frozen methodology baseline cohort relevance (competition / baseline topics).

USE DATABASE FOOTBALL_NARRATIVE_DEV;
USE SCHEMA CORE;

ALTER TABLE CORE.DIM_EVENT_VERSION ADD COLUMN IF NOT EXISTS baseline_terms VARIANT;
