"""Offline unit tests for DeploymentReconciler.

Verifies detection of:
- Exact schema matches (zero drift)
- Missing tables
- Missing columns
- Character length mismatches
- Precision & scale mismatches (e.g. NUMBER(6,5) vs NUMBER(10,5))
- Nullability mismatches
- Constraint column composition mismatches
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from reconcile_deployment_drift import (
        DeploymentReconciler,
        get_canonical_phase_1e_specs,
    )
except ImportError:
    from scripts.reconcile_deployment_drift import (
        DeploymentReconciler,
        get_canonical_phase_1e_specs,
    )


def build_perfect_metadata(specs):
    """Builds mock tables_meta, columns_meta, constraints_meta matching specs exactly."""
    tables_meta = []
    columns_meta = []
    constraints_meta = []

    for tkey, tspec in specs.items():
        tables_meta.append({
            "table_schema": tspec.table_schema,
            "table_name": tspec.table_name,
            "table_type": "BASE TABLE"
        })
        for cname, cspec in tspec.columns.items():
            columns_meta.append({
                "table_schema": tspec.table_schema,
                "table_name": tspec.table_name,
                "column_name": cname,
                "data_type": cspec.data_type,
                "is_nullable": cspec.is_nullable,
                "character_maximum_length": cspec.character_maximum_length,
                "numeric_precision": cspec.numeric_precision,
                "numeric_scale": cspec.numeric_scale,
            })
        for con_name, con_spec in tspec.constraints.items():
            for col in con_spec.column_names:
                constraints_meta.append({
                    "table_schema": tspec.table_schema,
                    "table_name": tspec.table_name,
                    "constraint_name": con_name,
                    "constraint_type": con_spec.constraint_type,
                    "column_name": col,
                })

    return tables_meta, columns_meta, constraints_meta


def test_perfect_baseline_matches_with_zero_drift():
    specs = get_canonical_phase_1e_specs()
    tables_meta, columns_meta, constraints_meta = build_perfect_metadata(specs)

    reconciler = DeploymentReconciler(canonical_specs=specs)
    report = reconciler.reconcile_from_metadata(tables_meta, columns_meta, constraints_meta)

    assert report["has_drift"] is False
    assert len(report["missing_tables"]) == 0
    assert len(report["column_mismatches"]) == 0
    assert len(report["constraint_mismatches"]) == 0
    assert len(report["matching_tables"]) == len(specs)


def test_missing_table_detected():
    specs = get_canonical_phase_1e_specs()
    tables_meta, columns_meta, constraints_meta = build_perfect_metadata(specs)

    # Remove DISCOVERY_UNIT_STATE from tables_meta
    tables_meta = [t for t in tables_meta if t["table_name"] != "DISCOVERY_UNIT_STATE"]

    reconciler = DeploymentReconciler(canonical_specs=specs)
    report = reconciler.reconcile_from_metadata(tables_meta, columns_meta, constraints_meta)

    assert report["has_drift"] is True
    assert "OPS.DISCOVERY_UNIT_STATE" in report["missing_tables"]


def test_missing_column_detected():
    specs = get_canonical_phase_1e_specs()
    tables_meta, columns_meta, constraints_meta = build_perfect_metadata(specs)

    # Remove HEADLINE_DECOMPOSITION_ELIGIBLE from BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT
    columns_meta = [
        c for c in columns_meta
        if not (c["table_name"] == "BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT" and c["column_name"] == "HEADLINE_DECOMPOSITION_ELIGIBLE")
    ]

    reconciler = DeploymentReconciler(canonical_specs=specs)
    report = reconciler.reconcile_from_metadata(tables_meta, columns_meta, constraints_meta)

    assert report["has_drift"] is True
    mismatches = [m for m in report["column_mismatches"] if m["column"] == "HEADLINE_DECOMPOSITION_ELIGIBLE"]
    assert len(mismatches) == 1
    assert mismatches[0]["issue"] == "MISSING_COLUMN"


def test_character_length_mismatch_detected():
    specs = get_canonical_phase_1e_specs()
    tables_meta, columns_meta, constraints_meta = build_perfect_metadata(specs)

    # Drift NEXT_PAGE_TOKEN on DISCOVERY_UNIT_STATE from 2048 back to 1024
    for c in columns_meta:
        if c["table_name"] == "DISCOVERY_UNIT_STATE" and c["column_name"] == "NEXT_PAGE_TOKEN":
            c["character_maximum_length"] = 1024

    reconciler = DeploymentReconciler(canonical_specs=specs)
    report = reconciler.reconcile_from_metadata(tables_meta, columns_meta, constraints_meta)

    assert report["has_drift"] is True
    mismatches = [m for m in report["column_mismatches"] if m["column"] == "NEXT_PAGE_TOKEN"]
    assert len(mismatches) == 1
    assert mismatches[0]["issue"] == "LENGTH_MISMATCH"
    assert mismatches[0]["expected"] == 2048
    assert mismatches[0]["actual"] == 1024


def test_precision_and_scale_mismatch_detected():
    specs = get_canonical_phase_1e_specs()
    tables_meta, columns_meta, constraints_meta = build_perfect_metadata(specs)

    # Drift FOCUS_RATIO on FACT_CHANNEL_CLASSIFICATION_ASSESSMENT to NUMBER(6,5) instead of NUMBER(10,5)
    for c in columns_meta:
        if c["table_name"] == "FACT_CHANNEL_CLASSIFICATION_ASSESSMENT" and c["column_name"] == "FOCUS_RATIO":
            c["numeric_precision"] = 6
            c["numeric_scale"] = 5

    reconciler = DeploymentReconciler(canonical_specs=specs)
    report = reconciler.reconcile_from_metadata(tables_meta, columns_meta, constraints_meta)

    assert report["has_drift"] is True
    mismatches = [m for m in report["column_mismatches"] if m["column"] == "FOCUS_RATIO"]
    assert len(mismatches) == 1
    assert mismatches[0]["issue"] == "PRECISION_SCALE_MISMATCH"
    assert mismatches[0]["expected"] == "(10,5)"
    assert mismatches[0]["actual"] == "(6,5)"


def test_nullability_mismatch_detected():
    specs = get_canonical_phase_1e_specs()
    tables_meta, columns_meta, constraints_meta = build_perfect_metadata(specs)

    # Drift STARTED_AT on DISCOVERY_UNIT_STATE from nullable to NOT NULL
    for c in columns_meta:
        if c["table_name"] == "DISCOVERY_UNIT_STATE" and c["column_name"] == "STARTED_AT":
            c["is_nullable"] = "NO"

    reconciler = DeploymentReconciler(canonical_specs=specs)
    report = reconciler.reconcile_from_metadata(tables_meta, columns_meta, constraints_meta)

    assert report["has_drift"] is True
    mismatches = [m for m in report["column_mismatches"] if m["column"] == "STARTED_AT"]
    assert len(mismatches) == 1
    assert mismatches[0]["issue"] == "NULLABILITY_MISMATCH"
    assert mismatches[0]["expected"] == "YES"
    assert mismatches[0]["actual"] == "NO"


def test_constraint_column_composition_mismatch_detected():
    specs = get_canonical_phase_1e_specs()
    tables_meta, columns_meta, constraints_meta = build_perfect_metadata(specs)

    # Omit one column from uq_discovery_unit_state constraint
    constraints_meta = [
        c for c in constraints_meta
        if not (c["constraint_name"] == "UQ_DISCOVERY_UNIT_STATE" and c["column_name"] == "QUERY_HASH")
    ]

    reconciler = DeploymentReconciler(canonical_specs=specs)
    report = reconciler.reconcile_from_metadata(tables_meta, columns_meta, constraints_meta)

    assert report["has_drift"] is True
    mismatches = [m for m in report["constraint_mismatches"] if m["constraint"] == "UQ_DISCOVERY_UNIT_STATE"]
    assert len(mismatches) == 1
    assert mismatches[0]["issue"] == "CONSTRAINT_NOT_FOUND_OR_COLUMNS_MISMATCH"
