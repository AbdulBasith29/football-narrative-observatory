import sys
import os
sys.path.insert(0, 'scripts')
from dotenv import load_dotenv
from ingestion_snowflake import get_snowflake_connection
from discovery_youtube import discover_videos

load_dotenv()

target_db = 'FOOTBALL_NARRATIVE_TEST'
conn = get_snowflake_connection(target_db=target_db)
cursor = conn.cursor()

# Get the latest frame_version_key synced for the TEST database
cursor.execute("SELECT frame_version_key FROM CORE.DIM_CHANNEL_FRAME_VERSION ORDER BY constructed_at DESC LIMIT 1")
row = cursor.fetchone()
if not row:
    print("No frame found. Did you run sync_config.py?")
    sys.exit(1)

frame_key = row[0]
conn.close()

print(f"Starting discovery on {target_db} with frame {frame_key}...")
discover_videos(target_db=target_db, run_purpose='INTEGRATION_TEST', frame_version_key=frame_key, use_search_fallback=True)
print("Discovery complete!")
