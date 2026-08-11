import sys
sys.path.insert(0, "scripts")

from dotenv import load_dotenv
from ingestion_snowflake import get_snowflake_connection

load_dotenv()

conn = get_snowflake_connection(
    target_db="FOOTBALL_NARRATIVE_TEST"
)

cursor = conn.cursor()

cursor.execute("""
SELECT
    CURRENT_DATABASE(),
    CURRENT_SCHEMA(),
    CURRENT_WAREHOUSE(),
    CURRENT_ROLE()
""")

print(cursor.fetchone())

conn.close()