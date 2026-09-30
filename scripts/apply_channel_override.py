import os
import uuid
import argparse
from datetime import datetime, timezone
from ingestion_snowflake import get_snowflake_connection

def apply_channel_override(
    conn,
    channel_key: str,
    axis_to_override: str,
    original_value: str,
    proposed_value: str,
    override_reason: str,
    supporting_evidence: str,
    submitted_by: str,
    approved_by: str = None
):
    """
    Submits and processes a structured channel classification manual override into OPS.MANUAL_OVERRIDE.
    Per protocol Section 9:
    - Overrides produce a discrete review_status (e.g. APPROVED_OVERRIDE).
    - An approved override creates a new audited Type-2 SCD row; it does NOT mutate the previous row.
    - An override does not silently elevate algorithmic confidence to HIGH.
    """
    if axis_to_override not in {"CHANNEL_TYPE", "PLAYER_FOCUS"}:
        raise ValueError("axis_to_override must be 'CHANNEL_TYPE' or 'PLAYER_FOCUS'.")

    cursor = conn.cursor()
    override_id = str(uuid.uuid4())
    now_iso = datetime.now(timezone.utc).isoformat()
    review_status = "APPROVED_OVERRIDE" if approved_by else "PENDING"
    approved_at_iso = now_iso if approved_by else None

    cursor.execute('''
        INSERT INTO OPS.MANUAL_OVERRIDE (
            override_id, target_table, target_key, override_reason, reviewer_identity,
            override_timestamp, classification_axis_overridden, original_axis_value,
            proposed_axis_value, supporting_evidence, review_status,
            approved_by_reviewer_identity, approved_at
        ) VALUES (%s, 'CORE.DIM_CHANNEL_STRATUM_VERSION', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', (
        override_id, channel_key, override_reason, submitted_by, now_iso,
        axis_to_override, original_value, proposed_value, supporting_evidence,
        review_status, approved_by, approved_at_iso
    ))

    if review_status == "APPROVED_OVERRIDE":
        # Retrieve active stratum version
        cursor.execute('''
            SELECT v.stratum_version_key, s.channel_type_value, s.player_focus_value
            FROM CORE.DIM_CHANNEL_STRATUM_VERSION v
            JOIN CORE.DIM_CHANNEL_STRATUM s ON v.stratum_key = s.stratum_key
            WHERE v.channel_key = %s AND v.is_current = TRUE
        ''', (channel_key,))
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"Channel {channel_key} has no active DIM_CHANNEL_STRATUM_VERSION.")
            
        old_ver_key, old_type, old_focus = row
        new_type = proposed_value if axis_to_override == "CHANNEL_TYPE" else old_type
        new_focus = proposed_value if axis_to_override == "PLAYER_FOCUS" else old_focus

        cursor.execute('''
            SELECT stratum_key FROM CORE.DIM_CHANNEL_STRATUM
            WHERE channel_type_value = %s AND player_focus_value = %s
        ''', (new_type, new_focus))
        st_row = cursor.fetchone()
        if not st_row:
            raise ValueError(f"Stratum combination {new_type} x {new_focus} does not exist in DIM_CHANNEL_STRATUM.")
        new_stratum_key = st_row[0]

        # Close old version
        cursor.execute('''
            UPDATE CORE.DIM_CHANNEL_STRATUM_VERSION
            SET is_current = FALSE, valid_to = %s
            WHERE stratum_version_key = %s
        ''', (now_iso, old_ver_key))

        # Insert new audited version
        new_ver_key = str(uuid.uuid4())
        cursor.execute('''
            INSERT INTO CORE.DIM_CHANNEL_STRATUM_VERSION (
                stratum_version_key, channel_key, stratum_key, valid_from, valid_to, is_current
            ) VALUES (%s, %s, %s, %s, NULL, TRUE)
        ''', (new_ver_key, channel_key, new_stratum_key, now_iso))

    conn.commit()
    return {
        "override_id": override_id,
        "review_status": review_status,
        "channel_key": channel_key,
        "axis_overridden": axis_to_override,
        "proposed_value": proposed_value
    }


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description="Submit and apply a channel classification override.")
    parser.add_argument("--channel-key", required=True)
    parser.add_argument("--axis", required=True, choices=["CHANNEL_TYPE", "PLAYER_FOCUS"])
    parser.add_argument("--original-value", required=True)
    parser.add_argument("--proposed-value", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--submitted-by", required=True)
    parser.add_argument("--approved-by", default=None)
    parser.add_argument("--target-db", default=os.getenv("TARGET_DATABASE", "FOOTBALL_NARRATIVE_DEV"))
    args = parser.parse_args()

    conn = get_snowflake_connection(target_db=args.target_db)
    res = apply_channel_override(
        conn, args.channel_key, args.axis, args.original_value, args.proposed_value,
        args.reason, args.evidence, args.submitted_by, args.approved_by
    )
    conn.close()
    print(f"Override recorded: {res}")
