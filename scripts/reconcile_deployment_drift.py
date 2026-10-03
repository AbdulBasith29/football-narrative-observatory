"""Observatory Deployment Reconciliation Tool.

Performs read-only comparison of live Snowflake information_schema against
merged repository definitions (Phase 1E baseline: infra/snowflake/ddl + V001-V009).

Checks:
- Table presence across RAW, OPS, CORE, ML, MARTS
- Column existence, physical data types, character lengths, precision, scale, and nullability
- Primary key and unique constraint column composition
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from typing import Any, Dict, List, Optional


@dataclasses.dataclass(frozen=True)
class ColumnSpec:
    column_name: str
    data_type: str
    is_nullable: str = "YES"
    character_maximum_length: Optional[int] = None
    numeric_precision: Optional[int] = None
    numeric_scale: Optional[int] = None


@dataclasses.dataclass(frozen=True)
class ConstraintSpec:
    constraint_name: str
    constraint_type: str  # 'PRIMARY KEY' or 'UNIQUE'
    column_names: List[str]


@dataclasses.dataclass(frozen=True)
class TableSpec:
    table_schema: str
    table_name: str
    columns: Dict[str, ColumnSpec]
    constraints: Dict[str, ConstraintSpec] = dataclasses.field(default_factory=dict)


def get_canonical_phase_1e_specs() -> Dict[str, TableSpec]:
    """Returns canonical TableSpecs for all expected Phase 1E warehouse tables."""
    specs: Dict[str, TableSpec] = {}

    def add_table(schema: str, name: str, cols: List[ColumnSpec], cons: Optional[List[ConstraintSpec]] = None):
        key = f"{schema.upper()}.{name.upper()}"
        col_map = {c.column_name.upper(): c for c in cols}
        con_map = {cn.constraint_name.upper(): cn for cn in (cons or [])}
        specs[key] = TableSpec(table_schema=schema.upper(), table_name=name.upper(), columns=col_map, constraints=con_map)

    # -------------------------------------------------------------------------
    # RAW SCHEMA
    # -------------------------------------------------------------------------
    add_table(
        "RAW", "YOUTUBE_API_RESPONSE",
        [
            ColumnSpec("RAW_RESPONSE_ID", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("API_REQUEST_ID", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("INGESTION_RUN_ID", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("SOURCE_SYSTEM", "VARCHAR", "NO", character_maximum_length=128),
            ColumnSpec("ENDPOINT", "VARCHAR", "NO", character_maximum_length=128),
            ColumnSpec("RESOURCE_SCOPE_TYPE", "VARCHAR", "NO", character_maximum_length=128),
            ColumnSpec("RESOURCE_SCOPE_ID", "VARCHAR", "NO", character_maximum_length=128),
            ColumnSpec("REQUEST_PARAMETERS", "VARIANT", "NO"),
            ColumnSpec("HTTP_STATUS", "NUMBER", "YES", numeric_precision=3, numeric_scale=0),
            ColumnSpec("PAGE_TOKEN_USED", "VARCHAR", "YES", character_maximum_length=1024),
            ColumnSpec("NEXT_PAGE_TOKEN_RETURNED", "VARCHAR", "YES", character_maximum_length=1024),
            ColumnSpec("RETRIEVED_AT", "TIMESTAMP_NTZ", "NO"),
            ColumnSpec("RAW_JSON_PAYLOAD", "VARIANT", "NO"),
            ColumnSpec("PAYLOAD_HASH", "VARCHAR", "NO", character_maximum_length=64),
            ColumnSpec("PARSER_VERSION", "VARCHAR", "YES", character_maximum_length=128),
            ColumnSpec("SOURCE_SCHEMA_VERSION", "VARCHAR", "YES", character_maximum_length=128),
        ],
        [
            ConstraintSpec("PK_YOUTUBE_API_RESPONSE", "PRIMARY KEY", ["RAW_RESPONSE_ID"]),
            ConstraintSpec("UQ_API_REQUEST_ID", "UNIQUE", ["API_REQUEST_ID"]),
        ]
    )

    # -------------------------------------------------------------------------
    # OPS SCHEMA
    # -------------------------------------------------------------------------
    add_table(
        "OPS", "FACT_INGESTION_RUN",
        [
            ColumnSpec("INGESTION_RUN_ID", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("RESOURCE_SCOPE_ID", "VARCHAR", "NO", character_maximum_length=128),
            ColumnSpec("SOURCE_SYSTEM", "VARCHAR", "NO", character_maximum_length=128),
            ColumnSpec("ENDPOINT", "VARCHAR", "NO", character_maximum_length=128),
            ColumnSpec("RESOURCE_SCOPE_TYPE", "VARCHAR", "NO", character_maximum_length=128),
            ColumnSpec("STARTED_AT", "TIMESTAMP_NTZ", "NO"),
            ColumnSpec("COMPLETED_AT", "TIMESTAMP_NTZ", "YES"),
            ColumnSpec("PAGES_REQUESTED", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("PAGES_SUCCEEDED", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("RECORDS_OBSERVED", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("RECORDS_INSERTED", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("DUPLICATE_RECORDS_OBSERVED", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("ERROR_CODE", "VARCHAR", "YES", character_maximum_length=128),
            ColumnSpec("RUN_OUTCOME", "VARCHAR", "YES", character_maximum_length=64),
            ColumnSpec("CONTINUITY_STATUS", "VARCHAR", "YES", character_maximum_length=64),
            ColumnSpec("RUN_PURPOSE", "VARCHAR", "YES", character_maximum_length=64),
        ]
    )

    add_table(
        "OPS", "DISCOVERY_UNIT_STATE",
        [
            ColumnSpec("DISCOVERY_UNIT_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("FRAME_VERSION_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("CHANNEL_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("WINDOW_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("DISCOVERY_POLICY_VERSION", "VARCHAR", "NO", character_maximum_length=64),
            ColumnSpec("QUERY_HASH", "VARCHAR", "NO", character_maximum_length=64),
            ColumnSpec("QUERY_BATCH_NUMBER", "NUMBER", "NO", numeric_precision=38, numeric_scale=0),
            ColumnSpec("SEARCH_QUERY", "VARCHAR", "NO", character_maximum_length=1024),
            ColumnSpec("STATUS", "VARCHAR", "NO", character_maximum_length=32),
            ColumnSpec("PAGES_COMPLETED", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("NEXT_PAGE_TOKEN", "VARCHAR", "YES", character_maximum_length=2048),
            ColumnSpec("ITEMS_OBSERVED", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("UNIQUE_VIDEO_IDS_OBSERVED", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("SEARCH_CALLS_CONSUMED", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("STARTED_AT", "TIMESTAMP_NTZ", "YES"),
            ColumnSpec("UPDATED_AT", "TIMESTAMP_NTZ", "NO"),
            ColumnSpec("COMPLETED_AT", "TIMESTAMP_NTZ", "YES"),
            ColumnSpec("LAST_ERROR_CODE", "VARCHAR", "YES", character_maximum_length=128),
            ColumnSpec("LAST_ERROR_MESSAGE", "VARCHAR", "YES", character_maximum_length=1024),
            ColumnSpec("FIRST_INGESTION_RUN_ID", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("LATEST_INGESTION_RUN_ID", "VARCHAR", "NO", character_maximum_length=36),
        ],
        [
            ConstraintSpec("PK_DISCOVERY_UNIT_STATE", "PRIMARY KEY", ["DISCOVERY_UNIT_KEY"]),
            ConstraintSpec(
                "UQ_DISCOVERY_UNIT_STATE", "UNIQUE",
                ["FRAME_VERSION_KEY", "CHANNEL_KEY", "WINDOW_KEY", "DISCOVERY_POLICY_VERSION", "QUERY_HASH"]
            ),
        ]
    )

    add_table(
        "OPS", "FACT_CHANNEL_CLASSIFICATION_ASSESSMENT",
        [
            ColumnSpec("ASSESSMENT_ID", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("CHANNEL_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("ASSESSED_AT", "TIMESTAMP_NTZ", "NO"),
            ColumnSpec("ASSESSMENT_TRIGGER", "VARCHAR", "YES", character_maximum_length=64),
            ColumnSpec("EVENT_VERSION_KEY", "VARCHAR", "YES", character_maximum_length=36),
            ColumnSpec("CLASSIFICATION_PROTOCOL_VERSION", "VARCHAR", "YES", character_maximum_length=64),
            ColumnSpec("RULE_CONFIG_VERSION", "VARCHAR", "YES", character_maximum_length=64),
            ColumnSpec("RULE_CONFIG_HASH", "VARCHAR", "YES", character_maximum_length=64),
            ColumnSpec("EVIDENCE_MANIFEST_HASH", "VARCHAR", "YES", character_maximum_length=64),
            ColumnSpec("CHANNEL_TYPE_VALUE", "VARCHAR", "YES", character_maximum_length=64),
            ColumnSpec("PLAYER_FOCUS_VALUE", "VARCHAR", "YES", character_maximum_length=64),
            ColumnSpec("CHANNEL_TYPE_CONFIDENCE", "VARCHAR", "YES", character_maximum_length=32),
            ColumnSpec("PLAYER_FOCUS_CONFIDENCE", "VARCHAR", "YES", character_maximum_length=32),
            ColumnSpec("REFERENCE_PERIOD_START", "TIMESTAMP_NTZ", "YES"),
            ColumnSpec("REFERENCE_PERIOD_END", "TIMESTAMP_NTZ", "YES"),
            ColumnSpec("EFFECTIVE_WINDOW_DAYS", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("ELIGIBLE_VIDEO_COUNT", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("MESSI_VIDEO_COUNT", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("RONALDO_VIDEO_COUNT", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("MESSI_PREVALENCE", "NUMBER", "YES", numeric_precision=6, numeric_scale=5),
            ColumnSpec("RONALDO_PREVALENCE", "NUMBER", "YES", numeric_precision=6, numeric_scale=5),
            ColumnSpec("FOCUS_RATIO", "NUMBER", "YES", numeric_precision=10, numeric_scale=5),
            ColumnSpec("FALLBACK_USED", "BOOLEAN", "YES"),
            ColumnSpec("DEDUPLICATION_RULE_VERSION", "VARCHAR", "YES", character_maximum_length=64),
            ColumnSpec("CHANNEL_TYPE_EVIDENCE", "VARIANT", "YES"),
            ColumnSpec("PLAYER_FOCUS_EVIDENCE", "VARIANT", "YES"),
            ColumnSpec("GIT_COMMIT_SHA", "VARCHAR", "YES", character_maximum_length=40),
            ColumnSpec("INGESTION_RUN_ID", "VARCHAR", "YES", character_maximum_length=36),
        ],
        [ConstraintSpec("PK_FACT_CHANNEL_CLASSIFICATION_ASSESSMENT", "PRIMARY KEY", ["ASSESSMENT_ID"])]
    )

    add_table(
        "OPS", "FACT_CHANNEL_ASSESSMENT_EVIDENCE_ITEM",
        [
            ColumnSpec("ASSESSMENT_EVIDENCE_ITEM_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("ASSESSMENT_ID", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("CHANNEL_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("VIDEO_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("SOURCE_VIDEO_ID", "VARCHAR", "NO", character_maximum_length=128),
            ColumnSpec("PUBLISHED_AT", "TIMESTAMP_NTZ", "NO"),
            ColumnSpec("DURATION_SECONDS", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("IS_SHORT", "BOOLEAN", "YES"),
            ColumnSpec("RAW_RESPONSE_ID", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("API_REQUEST_ID", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("MESSI_ALIAS_MATCHED", "BOOLEAN", "NO"),
            ColumnSpec("RONALDO_ALIAS_MATCHED", "BOOLEAN", "NO"),
            ColumnSpec("MATCHED_PLAYER_ALIASES", "VARIANT", "YES"),
            ColumnSpec("MATCHED_CLUB_ENTITIES", "VARIANT", "YES"),
            ColumnSpec("MATCHED_ANALYSIS_TOPICS", "VARIANT", "YES"),
            ColumnSpec("IS_DEDUPLICATED_DUPLICATE", "BOOLEAN", "NO"),
            ColumnSpec("DEDUPLICATION_CLUSTER_ID", "VARCHAR", "YES", character_maximum_length=64),
            ColumnSpec("INCLUDED_IN_PREVALENCE", "BOOLEAN", "NO"),
        ],
        [
            ConstraintSpec("PK_ASSESSMENT_EVIDENCE_ITEM", "PRIMARY KEY", ["ASSESSMENT_EVIDENCE_ITEM_KEY"]),
            ConstraintSpec("UQ_ASSESSMENT_VIDEO_EVIDENCE", "UNIQUE", ["ASSESSMENT_ID", "VIDEO_KEY"]),
        ]
    )

    add_table(
        "OPS", "CHANNEL_EVIDENCE_ACQUISITION_STATE",
        [
            ColumnSpec("ACQUISITION_STATE_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("FRAME_VERSION_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("CHANNEL_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("SOURCE_CHANNEL_ID", "VARCHAR", "NO", character_maximum_length=128),
            ColumnSpec("UPLOADS_PLAYLIST_ID", "VARCHAR", "NO", character_maximum_length=128),
            ColumnSpec("SCAN_SCOPE", "VARCHAR", "NO", character_maximum_length=32),
            ColumnSpec("WINDOW_START_AT", "TIMESTAMP_NTZ", "NO"),
            ColumnSpec("WINDOW_END_AT", "TIMESTAMP_NTZ", "NO"),
            ColumnSpec("STATUS", "VARCHAR", "NO", character_maximum_length=32),
            ColumnSpec("NEXT_PAGE_TOKEN", "VARCHAR", "YES", character_maximum_length=2048),
            ColumnSpec("PAGES_COMPLETED", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("OLDEST_OBSERVED_PUBLISHED_AT", "TIMESTAMP_NTZ", "YES"),
            ColumnSpec("ITEMS_OBSERVED", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("ELIGIBLE_VIDEOS_OBSERVED", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("API_CALLS_CONSUMED", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("FIRST_INGESTION_RUN_ID", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("LATEST_INGESTION_RUN_ID", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("LAST_API_REQUEST_ID", "VARCHAR", "YES", character_maximum_length=36),
            ColumnSpec("RETRY_COUNT", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("LAST_ERROR_CODE", "VARCHAR", "YES", character_maximum_length=128),
            ColumnSpec("LAST_ERROR_MESSAGE", "VARCHAR", "YES", character_maximum_length=1024),
            ColumnSpec("STARTED_AT", "TIMESTAMP_NTZ", "YES"),
            ColumnSpec("UPDATED_AT", "TIMESTAMP_NTZ", "NO"),
            ColumnSpec("COMPLETED_AT", "TIMESTAMP_NTZ", "YES"),
        ],
        [
            ConstraintSpec("PK_CHANNEL_EVIDENCE_ACQUISITION_STATE", "PRIMARY KEY", ["ACQUISITION_STATE_KEY"]),
            ConstraintSpec("UQ_CHANNEL_EVIDENCE_SCOPE", "UNIQUE", ["FRAME_VERSION_KEY", "CHANNEL_KEY", "WINDOW_END_AT", "SCAN_SCOPE"]),
        ]
    )

    add_table(
        "OPS", "VIDEO_METADATA_RESOLUTION_STATE",
        [
            ColumnSpec("RESOLUTION_STATE_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("SOURCE_SYSTEM", "VARCHAR", "NO", character_maximum_length=128),
            ColumnSpec("SOURCE_ID", "VARCHAR", "NO", character_maximum_length=128),
            ColumnSpec("RESOLUTION_STATUS", "VARCHAR", "NO", character_maximum_length=64),
            ColumnSpec("FIRST_DISCOVERED_AT", "TIMESTAMP_NTZ", "NO"),
            ColumnSpec("RESOLVED_AT", "TIMESTAMP_NTZ", "YES"),
            ColumnSpec("UPDATED_AT", "TIMESTAMP_NTZ", "NO"),
            ColumnSpec("API_REQUEST_ID", "VARCHAR", "YES", character_maximum_length=36),
            ColumnSpec("RAW_RESPONSE_ID", "VARCHAR", "YES", character_maximum_length=36),
            ColumnSpec("FIRST_INGESTION_RUN_ID", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("LATEST_INGESTION_RUN_ID", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("LAST_ERROR_CODE", "VARCHAR", "YES", character_maximum_length=128),
            ColumnSpec("LAST_ERROR_MESSAGE", "VARCHAR", "YES", character_maximum_length=1024),
            ColumnSpec("RETRY_COUNT", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
        ],
        [
            ConstraintSpec("PK_VIDEO_METADATA_RESOLUTION_STATE", "PRIMARY KEY", ["RESOLUTION_STATE_KEY"]),
            ConstraintSpec("UQ_VIDEO_METADATA_RESOLUTION_STATE", "UNIQUE", ["SOURCE_SYSTEM", "SOURCE_ID"]),
        ]
    )

    # -------------------------------------------------------------------------
    # CORE SCHEMA (Selected high-audit tables)
    # -------------------------------------------------------------------------
    add_table(
        "CORE", "DIM_CHANNEL_STRATUM",
        [
            ColumnSpec("STRATUM_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("CHANNEL_TYPE_VALUE", "VARCHAR", "NO", character_maximum_length=128),
            ColumnSpec("PLAYER_FOCUS_VALUE", "VARCHAR", "NO", character_maximum_length=128),
            ColumnSpec("STRATUM_NAME", "VARCHAR", "YES", character_maximum_length=256),
            ColumnSpec("ANALYTICAL_ELIGIBILITY", "BOOLEAN", "YES"),
        ],
        [
            ConstraintSpec("PK_DIM_CHANNEL_STRATUM", "PRIMARY KEY", ["STRATUM_KEY"]),
            ConstraintSpec("UQ_DIM_CHANNEL_STRATUM", "UNIQUE", ["CHANNEL_TYPE_VALUE", "PLAYER_FOCUS_VALUE"]),
        ]
    )

    add_table(
        "CORE", "BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT",
        [
            ColumnSpec("EVENT_CHANNEL_STRATUM_SNAPSHOT_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("EVENT_VERSION_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("CHANNEL_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("CLASSIFICATION_PROTOCOL_VERSION", "VARCHAR", "NO", character_maximum_length=64),
            ColumnSpec("CHANNEL_TYPE_VALUE", "VARCHAR", "NO", character_maximum_length=64),
            ColumnSpec("PLAYER_FOCUS_VALUE", "VARCHAR", "NO", character_maximum_length=64),
            ColumnSpec("CHANNEL_TYPE_CONFIDENCE", "VARCHAR", "NO", character_maximum_length=32),
            ColumnSpec("PLAYER_FOCUS_CONFIDENCE", "VARCHAR", "NO", character_maximum_length=32),
            ColumnSpec("REFERENCE_PERIOD_START", "TIMESTAMP_NTZ", "NO"),
            ColumnSpec("REFERENCE_PERIOD_END", "TIMESTAMP_NTZ", "NO"),
            ColumnSpec("EFFECTIVE_WINDOW_DAYS", "NUMBER", "NO", numeric_precision=38, numeric_scale=0),
            ColumnSpec("ELIGIBLE_VIDEO_COUNT", "NUMBER", "NO", numeric_precision=38, numeric_scale=0),
            ColumnSpec("MESSI_VIDEO_COUNT", "NUMBER", "NO", numeric_precision=38, numeric_scale=0),
            ColumnSpec("RONALDO_VIDEO_COUNT", "NUMBER", "NO", numeric_precision=38, numeric_scale=0),
            ColumnSpec("MESSI_PREVALENCE", "NUMBER", "NO", numeric_precision=6, numeric_scale=5),
            ColumnSpec("RONALDO_PREVALENCE", "NUMBER", "NO", numeric_precision=6, numeric_scale=5),
            ColumnSpec("FOCUS_RATIO", "NUMBER", "NO", numeric_precision=10, numeric_scale=5),
            ColumnSpec("FALLBACK_USED", "BOOLEAN", "NO"),
            ColumnSpec("HEADLINE_DECOMPOSITION_ELIGIBLE", "BOOLEAN", "YES"),
            ColumnSpec("SNAPSHOT_CREATED_AT", "TIMESTAMP_NTZ", "NO"),
            ColumnSpec("ASSESSMENT_ID", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("OVERRIDE_ID", "VARCHAR", "YES", character_maximum_length=36),
        ],
        [
            ConstraintSpec("PK_EVENT_CHANNEL_STRATUM_SNAPSHOT", "PRIMARY KEY", ["EVENT_CHANNEL_STRATUM_SNAPSHOT_KEY"]),
            ConstraintSpec("UQ_EVENT_CHANNEL_STRATUM_SNAPSHOT", "UNIQUE", ["EVENT_VERSION_KEY", "CHANNEL_KEY", "CLASSIFICATION_PROTOCOL_VERSION"]),
        ]
    )

    add_table(
        "CORE", "BRIDGE_VIDEO_EVENT",
        [
            ColumnSpec("VIDEO_EVENT_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("VIDEO_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("EVENT_VERSION_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("SAMPLING_POLICY_VERSION_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("COHORT_TYPE", "VARCHAR", "NO", character_maximum_length=32),
            ColumnSpec("SELECTION_RANK", "NUMBER", "YES", numeric_precision=38, numeric_scale=0),
            ColumnSpec("INCLUSION_STATUS", "VARCHAR", "NO", character_maximum_length=32),
            ColumnSpec("INCLUSION_RATIONALE", "VARCHAR", "YES", character_maximum_length=512),
            ColumnSpec("PRIMARY_EXCLUSION_REASON", "VARCHAR", "YES", character_maximum_length=128),
            ColumnSpec("DISCOVERY_METHOD", "VARCHAR", "NO", character_maximum_length=64),
            ColumnSpec("DISCOVERY_PROVENANCE", "VARIANT", "YES"),
            ColumnSpec("CANDIDATE_LIST_LINEAGE", "VARIANT", "YES"),
            ColumnSpec("OVERRIDE_ID", "VARCHAR", "YES", character_maximum_length=36),
            ColumnSpec("EVALUATED_AT", "TIMESTAMP_NTZ", "NO"),
        ],
        [ConstraintSpec("PK_BRIDGE_VIDEO_EVENT", "PRIMARY KEY", ["VIDEO_EVENT_KEY"])]
    )

    add_table(
        "CORE", "BRIDGE_FRAME_CHANNEL",
        [
            ColumnSpec("FRAME_CHANNEL_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("FRAME_VERSION_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("CHANNEL_KEY", "VARCHAR", "NO", character_maximum_length=36),
            ColumnSpec("EVALUATED_AT", "TIMESTAMP_NTZ", "NO"),
            ColumnSpec("ELIGIBILITY_RULE_VERSION", "VARCHAR", "NO", character_maximum_length=64),
            ColumnSpec("FRAME_INCLUSION_STATUS", "VARCHAR", "NO", character_maximum_length=64),
            ColumnSpec("GENERAL_EXCLUSION_REASON", "VARCHAR", "YES", character_maximum_length=128),
        ],
        [
            ConstraintSpec("PK_BRIDGE_FRAME_CHANNEL", "PRIMARY KEY", ["FRAME_CHANNEL_KEY"]),
            ConstraintSpec("UQ_BRIDGE_FRAME_CHANNEL", "UNIQUE", ["FRAME_VERSION_KEY", "CHANNEL_KEY"]),
        ]
    )

    return specs


class DeploymentReconciler:
    """Read-only comparison of live Snowflake schema metadata against canonical specifications."""

    def __init__(self, canonical_specs: Optional[Dict[str, TableSpec]] = None):
        self.canonical_specs = canonical_specs or get_canonical_phase_1e_specs()

    def reconcile_from_metadata(
        self,
        tables_meta: List[dict],
        columns_meta: List[dict],
        constraints_meta: Optional[List[dict]] = None,
    ) -> dict:
        """Compares metadata dictionaries against canonical specs in memory."""
        # Index live tables: KEY -> table_type
        live_tables: Dict[str, str] = {
            f"{r['table_schema'].upper()}.{r['table_name'].upper()}": r.get("table_type", "BASE TABLE")
            for r in tables_meta
        }

        # Index live columns: TABLE_KEY -> COL_NAME -> dict
        live_columns: Dict[str, Dict[str, dict]] = {}
        for r in columns_meta:
            tkey = f"{r['table_schema'].upper()}.{r['table_name'].upper()}"
            if tkey not in live_columns:
                live_columns[tkey] = {}
            cname = r["column_name"].upper()
            live_columns[tkey][cname] = {
                "data_type": r.get("data_type", "").upper(),
                "is_nullable": r.get("is_nullable", "YES").upper(),
                "character_maximum_length": r.get("character_maximum_length"),
                "numeric_precision": r.get("numeric_precision"),
                "numeric_scale": r.get("numeric_scale"),
            }

        # Index live constraints: TABLE_KEY -> list of {name, type, columns}
        live_constraints: Dict[str, List[dict]] = {}
        if constraints_meta:
            # Group by constraint_name
            by_cname: Dict[str, dict] = {}
            for r in constraints_meta:
                cname = r["constraint_name"].upper()
                tkey = f"{r['table_schema'].upper()}.{r['table_name'].upper()}"
                if cname not in by_cname:
                    by_cname[cname] = {
                        "table_key": tkey,
                        "constraint_name": cname,
                        "constraint_type": r.get("constraint_type", "").upper(),
                        "columns": [],
                    }
                by_cname[cname]["columns"].append(r["column_name"].upper())

            for c_info in by_cname.values():
                tkey = c_info["table_key"]
                if tkey not in live_constraints:
                    live_constraints[tkey] = []
                live_constraints[tkey].append({
                    "constraint_name": c_info["constraint_name"],
                    "constraint_type": c_info["constraint_type"],
                    "columns": sorted(c_info["columns"]),
                })

        # Perform reconciliation
        drift_report = {
            "has_drift": False,
            "matching_tables": [],
            "missing_tables": [],
            "unexpected_tables": [],
            "column_mismatches": [],
            "constraint_mismatches": [],
        }

        # 1. Table existence
        for tkey, expected_spec in self.canonical_specs.items():
            if tkey not in live_tables:
                drift_report["missing_tables"].append(tkey)
                drift_report["has_drift"] = True
                continue

            drift_report["matching_tables"].append(tkey)
            live_cols = live_columns.get(tkey, {})

            # 2. Columns comparison
            for col_name, exp_col in expected_spec.columns.items():
                if col_name not in live_cols:
                    drift_report["column_mismatches"].append({
                        "table": tkey,
                        "column": col_name,
                        "issue": "MISSING_COLUMN",
                        "expected": dataclasses.asdict(exp_col),
                        "actual": None,
                    })
                    drift_report["has_drift"] = True
                    continue

                act_col = live_cols[col_name]
                # Compare data type
                if exp_col.data_type != act_col["data_type"]:
                    drift_report["column_mismatches"].append({
                        "table": tkey,
                        "column": col_name,
                        "issue": "DATA_TYPE_MISMATCH",
                        "expected": exp_col.data_type,
                        "actual": act_col["data_type"],
                    })
                    drift_report["has_drift"] = True

                # Compare length if expected
                if exp_col.character_maximum_length is not None and act_col["character_maximum_length"] is not None:
                    if exp_col.character_maximum_length != act_col["character_maximum_length"]:
                        drift_report["column_mismatches"].append({
                            "table": tkey,
                            "column": col_name,
                            "issue": "LENGTH_MISMATCH",
                            "expected": exp_col.character_maximum_length,
                            "actual": act_col["character_maximum_length"],
                        })
                        drift_report["has_drift"] = True

                # Compare precision & scale if expected
                if exp_col.numeric_precision is not None and act_col["numeric_precision"] is not None:
                    if (exp_col.numeric_precision, exp_col.numeric_scale) != (act_col["numeric_precision"], act_col["numeric_scale"]):
                        drift_report["column_mismatches"].append({
                            "table": tkey,
                            "column": col_name,
                            "issue": "PRECISION_SCALE_MISMATCH",
                            "expected": f"({exp_col.numeric_precision},{exp_col.numeric_scale})",
                            "actual": f"({act_col['numeric_precision']},{act_col['numeric_scale']})",
                        })
                        drift_report["has_drift"] = True

                # Compare nullability
                if exp_col.is_nullable != act_col["is_nullable"]:
                    drift_report["column_mismatches"].append({
                        "table": tkey,
                        "column": col_name,
                        "issue": "NULLABILITY_MISMATCH",
                        "expected": exp_col.is_nullable,
                        "actual": act_col["is_nullable"],
                    })
                    drift_report["has_drift"] = True

            # 3. Constraints comparison
            if constraints_meta is not None:
                t_live_cons = live_constraints.get(tkey, [])
                for con_name, exp_con in expected_spec.constraints.items():
                    exp_cols_sorted = sorted([c.upper() for c in exp_con.column_names])
                    matching_live = [
                        lc for lc in t_live_cons
                        if lc["columns"] == exp_cols_sorted and (
                            exp_con.constraint_type in lc["constraint_type"] or lc["constraint_type"] in exp_con.constraint_type
                        )
                    ]
                    if not matching_live:
                        drift_report["constraint_mismatches"].append({
                            "table": tkey,
                            "constraint": con_name,
                            "expected_type": exp_con.constraint_type,
                            "expected_columns": exp_cols_sorted,
                            "issue": "CONSTRAINT_NOT_FOUND_OR_COLUMNS_MISMATCH",
                        })
                        drift_report["has_drift"] = True

        return drift_report

    def reconcile_live(self, conn: Any, target_db: str = "FOOTBALL_NARRATIVE_DEV") -> dict:
        """Strictly read-only query of information_schema on target Snowflake connection."""
        cur = conn.cursor()
        try:
            # 1. Query tables
            cur.execute(
                f"SELECT table_schema, table_name, table_type "
                f"FROM {target_db}.information_schema.tables "
                f"WHERE table_schema IN ('RAW', 'OPS', 'CORE', 'ML', 'MARTS')"
            )
            tables_meta = [
                {"table_schema": r[0], "table_name": r[1], "table_type": r[2]}
                for r in cur.fetchall()
            ]

            # 2. Query columns
            cur.execute(
                f"SELECT table_schema, table_name, column_name, data_type, is_nullable, "
                f"character_maximum_length, numeric_precision, numeric_scale "
                f"FROM {target_db}.information_schema.columns "
                f"WHERE table_schema IN ('RAW', 'OPS', 'CORE', 'ML', 'MARTS')"
            )
            columns_meta = [
                {
                    "table_schema": r[0],
                    "table_name": r[1],
                    "column_name": r[2],
                    "data_type": r[3],
                    "is_nullable": r[4],
                    "character_maximum_length": r[5],
                    "numeric_precision": r[6],
                    "numeric_scale": r[7],
                }
                for r in cur.fetchall()
            ]

            # 3. Query constraints
            constraints_meta = []
            try:
                cur.execute(
                    f"SELECT tc.table_schema, tc.table_name, tc.constraint_name, tc.constraint_type, kcu.column_name "
                    f"FROM {target_db}.information_schema.table_constraints tc "
                    f"JOIN {target_db}.information_schema.key_column_usage kcu "
                    f"  ON tc.constraint_name = kcu.constraint_name "
                    f"  AND tc.table_schema = kcu.table_schema "
                    f"WHERE tc.table_schema IN ('RAW', 'OPS', 'CORE', 'ML', 'MARTS')"
                )
                constraints_meta = [
                    {
                        "table_schema": r[0],
                        "table_name": r[1],
                        "constraint_name": r[2],
                        "constraint_type": r[3],
                        "column_name": r[4],
                    }
                    for r in cur.fetchall()
                ]
            except Exception:
                # Information schema constraint joins may vary by permissions
                constraints_meta = []

            return self.reconcile_from_metadata(tables_meta, columns_meta, constraints_meta)
        finally:
            cur.close()


def main():
    parser = argparse.ArgumentParser(description="Read-only deployment reconciliation for Football Narrative Observatory")
    parser.add_argument("--target-db", default="FOOTBALL_NARRATIVE_DEV", help="Target Snowflake database")
    parser.add_argument("--json", action="store_true", help="Output machine-readable JSON")
    args = parser.parse_args()

    reconciler = DeploymentReconciler()

    # Attempt connection if module available
    try:
        from ingestion_snowflake import get_snowflake_connection
        conn = get_snowflake_connection(target_db=args.target_db)
        report = reconciler.reconcile_live(conn, target_db=args.target_db)
        conn.close()
    except Exception as ex:
        print(f"[ERROR] Could not connect to Snowflake: {ex}", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print("\n=======================================================")
        print(f"DEPLOYMENT RECONCILIATION REPORT: {args.target_db}")
        print("=======================================================")
        status_str = 'DRIFT DETECTED' if report['has_drift'] else 'MATCHES BASELINE'
        print(f"Drift Status: {status_str}")
        print(f"Matching Tables: {len(report['matching_tables'])}")
        print(f"Missing Tables:  {len(report['missing_tables'])}")
        if report['missing_tables']:
            for t in report['missing_tables']:
                print(f"  - {t}")
        print(f"Column Mismatches: {len(report['column_mismatches'])}")
        for cm in report['column_mismatches']:
            print(f"  - {cm['table']}.{cm['column']}: {cm['issue']} (Expected: {cm.get('expected')}, Actual: {cm.get('actual')})")
        print(f"Constraint Mismatches: {len(report['constraint_mismatches'])}")
        for csm in report['constraint_mismatches']:
            print(f"  - {csm['table']} constraint {csm['constraint']}: {csm['issue']}")


if __name__ == "__main__":
    main()
