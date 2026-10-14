"""Observatory Deployment Reconciliation Tool.

Performs read-only comparison of live Snowflake metadata against
canonical repository definitions (infra/snowflake/ddl + V010 hardening).

Checks:
- Table presence across RAW, OPS, CORE, ML, MARTS
- Column existence, physical data types, character lengths, precision, scale, and nullability
- Primary key and unique constraint column composition
"""

from __future__ import annotations

import argparse
import dataclasses
import glob
import json
import os
import re
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


def split_table_elements(body: str) -> List[str]:
    """Splits a CREATE TABLE definition body by top-level commas."""
    elements: List[str] = []
    current: List[str] = []
    depth = 0
    in_quote = False
    for ch in body:
        if ch == "'" and not in_quote:
            in_quote = True
        elif ch == "'" and in_quote:
            in_quote = False
        elif not in_quote:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                elements.append("".join(current).strip())
                current = []
                continue
        current.append(ch)
    if current:
        elements.append("".join(current).strip())
    return [e for e in elements if e]


def parse_column_and_constraints(elem: str, table_spec: TableSpec) -> None:
    """Parses a single element (column or constraint) into TableSpec."""
    # Table-level named PRIMARY KEY
    m_con_pk = re.match(r"CONSTRAINT\s+([A-Za-z0-9_]+)\s+PRIMARY\s+KEY\s*\((.*?)\)", elem, re.IGNORECASE)
    if m_con_pk:
        con_name = m_con_pk.group(1).upper()
        cols = [c.strip().upper() for c in m_con_pk.group(2).split(",")]
        table_spec.constraints[con_name] = ConstraintSpec(con_name, "PRIMARY KEY", cols)
        return

    # Table-level named UNIQUE
    m_con_uq = re.match(r"CONSTRAINT\s+([A-Za-z0-9_]+)\s+UNIQUE\s*\((.*?)\)", elem, re.IGNORECASE)
    if m_con_uq:
        con_name = m_con_uq.group(1).upper()
        cols = [c.strip().upper() for c in m_con_uq.group(2).split(",")]
        table_spec.constraints[con_name] = ConstraintSpec(con_name, "UNIQUE", cols)
        return

    # Table-level unnamed PRIMARY KEY
    m_pk = re.match(r"PRIMARY\s+KEY\s*\((.*?)\)", elem, re.IGNORECASE)
    if m_pk:
        con_name = f"PK_{table_spec.table_name}"
        cols = [c.strip().upper() for c in m_pk.group(1).split(",")]
        table_spec.constraints[con_name] = ConstraintSpec(con_name, "PRIMARY KEY", cols)
        return

    # Table-level unnamed UNIQUE
    m_uq = re.match(r"UNIQUE\s*\((.*?)\)", elem, re.IGNORECASE)
    if m_uq:
        con_name = f"UQ_{table_spec.table_name}"
        cols = [c.strip().upper() for c in m_uq.group(1).split(",")]
        table_spec.constraints[con_name] = ConstraintSpec(con_name, "UNIQUE", cols)
        return

    # Column definition: col_name type[(params)] [NOT NULL] [PRIMARY KEY] [DEFAULT ...]
    tokens = elem.split()
    if not tokens:
        return
    col_name = tokens[0].upper()
    col_def_upper = elem.upper()

    type_part = tokens[1].upper() if len(tokens) > 1 else "VARCHAR"
    type_match = re.match(r"^([A-Z0-9_]+)(?:\((.*?)\))?", type_part)
    if not type_match:
        data_type = type_part
        length = None
        prec = None
        scale = None
    else:
        data_type = type_match.group(1)
        params = type_match.group(2)
        length = None
        prec = None
        scale = None
        if params:
            parts = [p.strip() for p in params.split(",")]
            if len(parts) == 1:
                if data_type in ("VARCHAR", "CHAR", "STRING", "TEXT"):
                    length = int(parts[0])
                elif data_type in ("NUMBER", "NUMERIC", "DECIMAL", "INT", "INTEGER"):
                    prec = int(parts[0])
                    scale = 0
            elif len(parts) == 2:
                prec = int(parts[0])
                scale = int(parts[1])

    is_not_null = "NOT NULL" in col_def_upper or "PRIMARY KEY" in col_def_upper
    is_nullable = "NO" if is_not_null else "YES"

    if data_type in ("NUMBER", "NUMERIC", "DECIMAL", "INT", "INTEGER", "BIGINT") and prec is None:
        prec = 38
        scale = 0

    col_spec = ColumnSpec(col_name, data_type, is_nullable, length, prec, scale)
    table_spec.columns[col_name] = col_spec

    # Inline PRIMARY KEY
    if "PRIMARY KEY" in col_def_upper:
        con_name = f"PK_{table_spec.table_name}"
        if con_name not in table_spec.constraints:
            table_spec.constraints[con_name] = ConstraintSpec(con_name, "PRIMARY KEY", [col_name])


def parse_ddl_content(content: str, default_schema: str, specs: Dict[str, TableSpec]) -> None:
    """Parses SQL DDL text into TableSpecs, registering tables and ALTER TABLE constraints."""
    lines = []
    for line in content.splitlines():
        l_strip = line.strip()
        if l_strip.startswith("--"):
            continue
        if "--" in line:
            line = line.split("--")[0]
        lines.append(line)
    text = "\n".join(lines)

    current_schema = default_schema
    schema_match = re.search(r"USE\s+SCHEMA\s+([A-Za-z0-9_]+)", text, re.IGNORECASE)
    if schema_match:
        current_schema = schema_match.group(1).upper()

    # Match CREATE TABLE statements
    pattern = re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z0-9_.]+)\s*\((.*?)\);",
        re.DOTALL | re.IGNORECASE,
    )
    for tbl_name_raw, body in pattern.findall(text):
        if "." in tbl_name_raw:
            parts = tbl_name_raw.split(".")
            s = parts[-2].upper()
            t = parts[-1].upper()
        else:
            s, t = current_schema, tbl_name_raw.upper()

        col_dict: Dict[str, ColumnSpec] = {}
        con_dict: Dict[str, ConstraintSpec] = {}
        table_spec = TableSpec(table_schema=s, table_name=t, columns=col_dict, constraints=con_dict)
        for elem in split_table_elements(body):
            parse_column_and_constraints(elem, table_spec)
        tkey = f"{s}.{t}"
        specs[tkey] = table_spec

    # Match ALTER TABLE ADD CONSTRAINT statements
    con_pattern = re.compile(
        r"ALTER\s+TABLE\s+([A-Za-z0-9_.]+)\s+ADD\s+CONSTRAINT\s+([A-Za-z0-9_]+)\s+(PRIMARY\s+KEY|UNIQUE)\s*\((.*?)\)",
        re.IGNORECASE,
    )
    for tbl_name_raw, con_name, con_type, cols_raw in con_pattern.findall(text):
        if "." in tbl_name_raw:
            parts = tbl_name_raw.split(".")
            s = parts[-2].upper()
            t = parts[-1].upper()
        else:
            s, t = current_schema, tbl_name_raw.upper()
        tkey = f"{s}.{t}"
        if tkey in specs:
            cols = [c.strip().upper() for c in cols_raw.split(",")]
            specs[tkey].constraints[con_name.upper()] = ConstraintSpec(
                constraint_name=con_name.upper(),
                constraint_type=con_type.upper(),
                column_names=cols,
            )


def get_canonical_phase_1e_specs(base_dir: Optional[str] = None) -> Dict[str, TableSpec]:
    """Returns canonical TableSpecs parsed from repository DDL files and V010 migration.

    Covers all 54 warehouse tables across RAW, OPS, and CORE without hardcoded drift.
    """
    if base_dir is None:
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    specs: Dict[str, TableSpec] = {}
    ddl_dir = os.path.join(base_dir, "infra", "snowflake", "ddl")

    # 1. Parse repository DDL files in ordered sequence
    for f in sorted(glob.glob(os.path.join(ddl_dir, "*.sql"))):
        if os.path.basename(f).startswith("00_"):
            continue
        with open(f, "r", encoding="utf-8") as fh:
            parse_ddl_content(fh.read(), "PUBLIC", specs)

    # 2. Parse V010 hardening migration (adds OPS.SCHEMA_VERSION, OPS.SCHEMA_LOCK, and PK/UQ constraints)
    v010_path = os.path.join(
        base_dir,
        "infra",
        "snowflake",
        "migrations",
        "V010__contract_hardening_and_schema_version.sql",
    )
    if os.path.exists(v010_path):
        with open(v010_path, "r", encoding="utf-8") as fh:
            parse_ddl_content(fh.read(), "OPS", specs)

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
        drift_report: Dict[str, Any] = {
            "has_drift": False,
            "matching_tables": [],
            "missing_tables": [],
            "unexpected_tables": [],
            "column_mismatches": [],
            "constraint_mismatches": [],
        }

        # Check for missing tables
        for tkey, expected_spec in self.canonical_specs.items():
            if tkey not in live_tables:
                drift_report["missing_tables"].append(tkey)
                drift_report["has_drift"] = True
                continue

            drift_report["matching_tables"].append(tkey)
            t_live_cols = live_columns.get(tkey, {})

            # 2. Columns comparison
            for col_name, exp_col in expected_spec.columns.items():
                if col_name not in t_live_cols:
                    drift_report["column_mismatches"].append({
                        "table": tkey,
                        "column": col_name,
                        "issue": "MISSING_COLUMN",
                    })
                    drift_report["has_drift"] = True
                    continue

                act_col = t_live_cols[col_name]

                # Compare data type
                if exp_col.data_type != act_col["data_type"]:
                    # Equivalence for TEXT / VARCHAR
                    text_types = {"VARCHAR", "TEXT", "STRING", "CHAR"}
                    if not (exp_col.data_type in text_types and act_col["data_type"] in text_types):
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
                    if (exp_col.numeric_precision, exp_col.numeric_scale) != (
                        act_col["numeric_precision"],
                        act_col["numeric_scale"],
                    ):
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
                            exp_con.constraint_type in lc["constraint_type"]
                            or lc["constraint_type"] in exp_con.constraint_type
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
        """Strictly read-only query of information_schema and SHOW constraints on target Snowflake connection."""
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

            # 3. Query constraints using supported native Snowflake SHOW commands
            constraints_meta: List[dict] = []
            for cmd, con_type in [
                (f"SHOW PRIMARY KEYS IN DATABASE {target_db}", "PRIMARY KEY"),
                (f"SHOW UNIQUE KEYS IN DATABASE {target_db}", "UNIQUE"),
            ]:
                try:
                    cur.execute(cmd)
                    desc = [d[0].lower() for d in cur.description] if cur.description else []
                    for row in cur.fetchall():
                        r_dict = dict(zip(desc, row))
                        schema_name = (
                            r_dict.get("schema_name") or r_dict.get("table_schema") or ""
                        ).upper()
                        table_name = (r_dict.get("table_name") or "").upper()
                        col_name = (r_dict.get("column_name") or "").upper()
                        con_name = (r_dict.get("constraint_name") or "").upper()
                        if schema_name in ("RAW", "OPS", "CORE", "ML", "MARTS"):
                            constraints_meta.append({
                                "table_schema": schema_name,
                                "table_name": table_name,
                                "constraint_name": con_name or f"{con_type[:2]}_{table_name}",
                                "constraint_type": con_type,
                                "column_name": col_name,
                            })
                except Exception as ex:
                    raise RuntimeError(
                        f"Failed to query {con_type} constraints via Snowflake command '{cmd}': {ex}"
                    ) from ex

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
        status_str = "DRIFT DETECTED" if report["has_drift"] else "MATCHES BASELINE"
        print(f"Drift Status: {status_str}")
        print(f"Matching Tables: {len(report['matching_tables'])}")
        print(f"Missing Tables:  {len(report['missing_tables'])}")
        if report["missing_tables"]:
            for t in report["missing_tables"]:
                print(f"  - {t}")
        print(f"Column Mismatches: {len(report['column_mismatches'])}")
        for cm in report["column_mismatches"]:
            print(f"  - {cm['table']}.{cm['column']}: {cm['issue']} (Expected: {cm.get('expected')}, Actual: {cm.get('actual')})")
        print(f"Constraint Mismatches: {len(report['constraint_mismatches'])}")
        for csm in report["constraint_mismatches"]:
            print(f"  - {csm['table']} constraint {csm['constraint']}: {csm['issue']}")


if __name__ == "__main__":
    main()
