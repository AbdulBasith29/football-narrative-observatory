from dotenv import load_dotenv

from ingestion_snowflake import (
    get_snowflake_connection,
    setup_snowflake_ddl,
)

load_dotenv()

conn = get_snowflake_connection(bootstrap=True)

setup_snowflake_ddl(
    conn,
    target_db="FOOTBALL_NARRATIVE_TEST",
)

conn.close()

print("FOOTBALL_NARRATIVE_TEST bootstrap complete.")