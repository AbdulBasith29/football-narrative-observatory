import os
import sys
from dotenv import load_dotenv

# Ensure scripts dir is on sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ingestion_snowflake import get_snowflake_connection, setup_snowflake_ddl
from sync_config import sync_aliases, sync_events, sync_channels

def bootstrap_database(conn, db_name: str):
    print(f"\n==================================================")
    print(f"Bootstrapping Snowflake Database: {db_name}")
    print(f"==================================================")
    
    # 1. Run base DDL and incremental migrations
    print("[1/3] Applying DDL & migrations...")
    setup_snowflake_ddl(conn, target_db=db_name)
    print(f"      DDL & migrations successfully applied to {db_name}.")
        
    # 2. Synchronize configuration datasets into CORE dimension tables
    print("[2/3] Synchronizing configuration datasets into CORE dimension tables...")
    cursor = conn.cursor()
    cursor.execute(f"USE DATABASE {db_name}")
    cursor.execute("USE SCHEMA CORE")
    
    sync_aliases(conn, "config/player_aliases.yml")
    print("      Player aliases synced.")
    sync_events(conn, "config/event_windows.yml")
    print("      Event windows synced.")
    sync_channels(conn, "config/tracked_channels.yml")
    print("      Tracked channels & frame versions synced.")
    conn.commit()
        
    # 3. Verify created schemas and tables
    print("[3/3] Verifying database structure...")
    cursor.execute("""
        SELECT table_schema, count(*) 
        FROM information_schema.tables 
        WHERE table_schema IN ('RAW', 'OPS', 'CORE', 'ML', 'MARTS')
        GROUP BY table_schema
        ORDER BY table_schema
    """)
    counts = cursor.fetchall()
    for schema, count in counts:
        print(f"      Schema {schema}: {count} tables/views")

    print(f"Successfully bootstrapped and initialized {db_name}!\n")

def main():
    load_dotenv()
    
    # Check for CLI arguments or environment variables for TOTP passcode
    # Usage: python scripts/setup_snowflake.py [optional_passcode]
    passcode = None
    if len(sys.argv) > 1 and sys.argv[1].isdigit() and len(sys.argv[1]) == 6:
        passcode = sys.argv[1]
        databases = ["FOOTBALL_NARRATIVE_DEV", "FOOTBALL_NARRATIVE_TEST"]
    elif len(sys.argv) > 1:
        databases = [sys.argv[1]]
    else:
        databases = ["FOOTBALL_NARRATIVE_DEV", "FOOTBALL_NARRATIVE_TEST"]
        
    if passcode:
        os.environ["SNOWFLAKE_PASSCODE"] = passcode
    elif not os.getenv("SNOWFLAKE_PASSCODE"):
        # Check if stdin is interactive
        if sys.stdin and sys.stdin.isatty():
            code = input("Enter Snowflake 6-digit TOTP code from your authenticator app: ").strip()
            if code:
                os.environ["SNOWFLAKE_PASSCODE"] = code

    print("Connecting to Snowflake (authenticating once for session)...")
    conn = get_snowflake_connection(bootstrap=True)
    
    try:
        for db in databases:
            bootstrap_database(conn, db)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
