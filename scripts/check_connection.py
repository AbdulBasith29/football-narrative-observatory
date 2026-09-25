import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
from ingestion_snowflake import get_snowflake_connection

def main():
    load_dotenv()
    target_db = os.getenv("TARGET_DATABASE", "FOOTBALL_NARRATIVE_DEV")
    conn = get_snowflake_connection(target_db=target_db)
    cursor = conn.cursor()
    cursor.execute("""
    SELECT
        CURRENT_DATABASE(),
        CURRENT_SCHEMA(),
        CURRENT_WAREHOUSE(),
        CURRENT_ROLE()
    """)
    print("Snowflake Connection Status:", cursor.fetchone())
    conn.close()

if __name__ == "__main__":
    main()