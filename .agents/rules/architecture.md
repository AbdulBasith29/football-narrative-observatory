---
trigger: model_decision
description: Observatory architecture, layer boundaries, and schema responsibilities across RAW, OPS, CORE, ML, and MARTS schemas.
---

# Observatory Architecture & Schema Boundaries

The warehouse adheres to the five-layer architecture defined in `docs/11-data-warehouse-design.md`. Code must preserve layer boundaries, write exclusively to appropriate schemas, and respect the documented grain of every table.

## 1. Schema Responsibilities

| Schema | Purpose | Immutability | Access / Modification Rules |
| :--- | :--- | :--- | :--- |
| **`RAW`** | Immutable API payload landing zone (`raw.youtube_api_response`) | Append-only, immutable | Verbatim JSON payloads preserved before parsing. Audit lineage for every downstream entity. |
| **`OPS`** | Telemetry for ingestion, quota, checkpoints, and backfills | Mutable operational state | Records ingestion run metadata, discovery unit checkpoints, watermarks, and pipeline errors. |
| **`CORE`** | Conformed dimensions, immutable root facts, and resolution bridges | Managed SCD / Fact / Bridge | Canonical entities (`DIM_*`), measurement events (`FACT_*`), and relationships (`BRIDGE_*`). |
| **`ML`** | Model inference runs, predictions, and evidence spans | Versioned model assets | Stores text embeddings, sentiment scores, classification tags, and model evaluation metrics. |
| **`MARTS`** | Pre-aggregated analytical objects directly mapped to the V1.0 metric dictionary | Derived / Rebuildable | Dimensional aggregates optimized for analytical queries, Streamlit dashboards, and research exports. |

## 2. Table Grain Invariants
- **`OPS.FACT_INGESTION_RUN`**: Grain is one API execution session attempt (`ingestion_run_id`). Must record pages requested/succeeded, quota consumed, and run outcome.
- **`OPS.DISCOVERY_UNIT_STATE`**: Grain is `(frame_version_key, channel_key, window_key, discovery_policy_version, query_hash)`. Never aggregate or update across discovery units.
- **`CORE.DIM_VIDEO`**: Grain is one unique video (`video_key`, source `videoId`).
- **`CORE.FACT_VIDEO_SNAPSHOT`**: Grain is one observation of a video at a point in time (`video_snapshot_key`).
- **`CORE.BRIDGE_VIDEO_EVENT`**: Grain is one association between a video, an event version, and a sampling policy (`video_key, event_version_key, sampling_policy_version_key`). Provenance must record discovery queries.
- **`CORE.BRIDGE_FRAME_CHANNEL`**: Grain is one evaluated channel within a channel frame version (`frame_version_key, channel_key`).

## 3. Directional Flow
Data flows strictly forward:
`RAW` (verbatim payload) $\rightarrow$ `OPS` (telemetry/checkpoint) $\rightarrow$ `CORE` (conformed dimensions/facts/bridges) $\rightarrow$ `ML` (enrichment) $\rightarrow$ `MARTS` (reporting).
Never write raw unparsed payloads directly into `CORE` or bypass `RAW` auditability.
