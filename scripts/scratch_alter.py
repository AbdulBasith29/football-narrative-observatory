import os
from dotenv import load_dotenv
from ingestion_snowflake import get_snowflake_connection

load_dotenv()

# Alter TEST DB
conn = get_snowflake_connection(target_db="FOOTBALL_NARRATIVE_TEST")
cursor = conn.cursor()
cursor.execute("ALTER TABLE CORE.DIM_EVENT_VERSION ADD COLUMN IF NOT EXISTS event_terms VARIANT;")
conn.commit()
conn.close()
print("Successfully applied V005 to FOOTBALL_NARRATIVE_TEST")
