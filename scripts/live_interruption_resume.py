import sys
import os
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
from ingestion_snowflake import get_snowflake_connection
from discovery_youtube import discover_videos

def run_live_test():
    load_dotenv()
    target_db = 'FOOTBALL_NARRATIVE_TEST'
    
    # Dynamically resolve the latest PIPELINE_PILOT frame
    conn = get_snowflake_connection(target_db=target_db)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT frame_version_key 
        FROM CORE.DIM_CHANNEL_FRAME_VERSION 
        WHERE frame_purpose = 'PIPELINE_PILOT' 
        ORDER BY constructed_at DESC 
        LIMIT 1
    """)
    row = cursor.fetchone()
    if not row:
        cursor.close()
        conn.close()
        raise RuntimeError("No PIPELINE_PILOT frame found in CORE.DIM_CHANNEL_FRAME_VERSION.")
    frame_key = row[0]
    cursor.close()
    conn.close()

    print("=" * 70)
    print("STEP 1: Executing Run 1 (Interruption Test) with run_search_call_budget = 1")
    print("=" * 70)

    res1 = discover_videos(
        target_db=target_db,
        run_purpose='INTEGRATION_TEST',
        frame_version_key=frame_key,
        use_search_fallback=True,
        run_search_call_budget=1
    )

    print(f"\nRun 1 Result: {json.dumps(res1, indent=2)}")

    conn = get_snowflake_connection(target_db=target_db)
    cursor = conn.cursor()

    print("\n--- OPS.FACT_INGESTION_RUN (Run 1) ---")
    cursor.execute('''
        SELECT ingestion_run_id, endpoint, resource_scope_id, started_at, completed_at, 
               pages_requested, pages_succeeded, estimated_quota_consumed, run_outcome, continuity_status, run_purpose
        FROM OPS.FACT_INGESTION_RUN
        WHERE ingestion_run_id = %s
    ''', (res1['ingestion_run_id'],))
    col_names = [desc[0] for desc in cursor.description]
    row = cursor.fetchone()
    if row:
        for col, val in zip(col_names, row):
            print(f"  {col}: {val}")

    print("\n--- OPS.DISCOVERY_UNIT_STATE (After Run 1) ---")
    cursor.execute('''
        SELECT discovery_unit_key, query_batch_number, query_hash, status, pages_completed, 
               next_page_token, items_observed, unique_video_ids_observed, search_calls_consumed,
               first_ingestion_run_id, latest_ingestion_run_id
        FROM OPS.DISCOVERY_UNIT_STATE
        WHERE frame_version_key = %s
        ORDER BY query_batch_number
    ''', (frame_key,))
    unit_cols = [desc[0] for desc in cursor.description]
    units_r1 = cursor.fetchall()
    for u in units_r1:
        print("  Unit State:")
        for col, val in zip(unit_cols, u):
            print(f"    {col}: {val}")

    print("\n" + "=" * 70)
    print("STEP 2: Executing Run 2 (Resume Test) with run_search_call_budget = 10")
    print("=" * 70)

    res2 = discover_videos(
        target_db=target_db,
        run_purpose='INTEGRATION_TEST',
        frame_version_key=frame_key,
        use_search_fallback=True,
        run_search_call_budget=10
    )

    print(f"\nRun 2 Result: {json.dumps(res2, indent=2)}")

    print("\n--- OPS.FACT_INGESTION_RUN (Run 2) ---")
    cursor.execute('''
        SELECT ingestion_run_id, endpoint, resource_scope_id, started_at, completed_at, 
               pages_requested, pages_succeeded, estimated_quota_consumed, run_outcome, continuity_status, run_purpose
        FROM OPS.FACT_INGESTION_RUN
        WHERE ingestion_run_id = %s
    ''', (res2['ingestion_run_id'],))
    row2 = cursor.fetchone()
    if row2:
        for col, val in zip(col_names, row2):
            print(f"  {col}: {val}")

    print("\n--- OPS.DISCOVERY_UNIT_STATE (After Run 2) ---")
    cursor.execute('''
        SELECT discovery_unit_key, query_batch_number, query_hash, status, pages_completed, 
               next_page_token, items_observed, unique_video_ids_observed, search_calls_consumed,
               first_ingestion_run_id, latest_ingestion_run_id
        FROM OPS.DISCOVERY_UNIT_STATE
        WHERE frame_version_key = %s
        ORDER BY query_batch_number
    ''', (frame_key,))
    units_r2 = cursor.fetchall()
    for u in units_r2:
        print("  Unit State:")
        for col, val in zip(unit_cols, u):
            print(f"    {col}: {val}")

    print("\n--- Summary of Core Tables ---")
    cursor.execute("SELECT COUNT(*) FROM CORE.DIM_VIDEO")
    print(f"Total DIM_VIDEO count: {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM CORE.FACT_VIDEO_SNAPSHOT")
    print(f"Total FACT_VIDEO_SNAPSHOT count: {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM CORE.BRIDGE_VIDEO_EVENT")
    print(f"Total BRIDGE_VIDEO_EVENT count: {cursor.fetchone()[0]}")

    conn.close()
    print("\nLive Interruption + Resume Test Complete!")

if __name__ == "__main__":
    run_live_test()
