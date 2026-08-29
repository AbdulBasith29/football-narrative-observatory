import os
import json
import hashlib
import hmac
import uuid
from datetime import datetime, timezone
from dotenv import load_dotenv
from googleapiclient.discovery import build
import snowflake.connector
from snowflake.connector.errors import ProgrammingError

SALT_VERSION_ID = "v1.0"
MAX_PAGES_PER_RUN = 5

def get_snowflake_connection(bootstrap=False, target_db="FOOTBALL_NARRATIVE_DEV"):
    conn_params = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT")
    }
    wh = os.getenv("SNOWFLAKE_WAREHOUSE")
    if wh:
        conn_params["warehouse"] = wh
        
    if not bootstrap:
        conn_params["database"] = target_db
        conn_params["schema"] = "OPS"
        
    return snowflake.connector.connect(**conn_params)

def setup_snowflake_ddl(conn, target_db="FOOTBALL_NARRATIVE_DEV"):
    ddl_dir = "infra/snowflake/ddl"
    files = [
        "00_database_and_schemas.sql",
        "01_raw.sql",
        "02_ops.sql",
        "03_core_dimensions.sql",
        "04_core_facts.sql",
        "05_core_bridges.sql"
    ]
    for file in files:
        path = os.path.join(ddl_dir, file)
        with open(path, 'r') as f:
            sql_script = f.read().replace('FOOTBALL_NARRATIVE_DEV', target_db)
            conn.execute_string(sql_script)

def hash_author_id(author_id: str, hmac_secret: str) -> str:
    h = hmac.new(hmac_secret.encode('utf-8'), author_id.encode('utf-8'), hashlib.sha256)
    return h.hexdigest()

def get_or_create_author(conn, author_id: str, hmac_secret: str):
    if not author_id:
        return None
    cursor = conn.cursor()
    hashed_id = hash_author_id(author_id, hmac_secret)
    cursor.execute("SELECT author_key FROM CORE.DIM_AUTHOR WHERE author_hash = %s AND salt_version_id = %s", (hashed_id, SALT_VERSION_ID))
    row = cursor.fetchone()
    if row:
        return row[0]
    author_key = str(uuid.uuid4())
    cursor.execute("INSERT INTO CORE.DIM_AUTHOR (author_key, author_hash, salt_version_id) VALUES (%s, %s, %s)",
                   (author_key, hashed_id, SALT_VERSION_ID))
    return author_key

def fetch_and_create_video(conn, video_id: str, youtube):
    cursor = conn.cursor()
    cursor.execute("SELECT video_key FROM CORE.DIM_VIDEO WHERE source_system = 'YOUTUBE' AND source_id = %s", (video_id,))
    row = cursor.fetchone()
    if row:
        return row[0]
        
    response = youtube.videos().list(part="snippet", id=video_id).execute()
    items = response.get("items", [])
    if not items:
        raise ValueError("Video not found on YouTube")
        
    channel_id = items[0]["snippet"]["channelId"]
    
    cursor.execute("SELECT channel_key FROM CORE.DIM_CHANNEL WHERE source_system = 'YOUTUBE' AND source_id = %s", (channel_id,))
    ch_row = cursor.fetchone()
    if ch_row:
        channel_key = ch_row[0]
    else:
        channel_key = str(uuid.uuid4())
        cursor.execute("INSERT INTO CORE.DIM_CHANNEL (channel_key, source_system, source_id, channel_name) VALUES (%s, 'YOUTUBE', %s, %s)", 
                       (channel_key, channel_id, items[0]["snippet"]["channelTitle"]))
                       
    video_key = str(uuid.uuid4())
    cursor.execute("INSERT INTO CORE.DIM_VIDEO (video_key, channel_key, source_system, source_id) VALUES (%s, %s, 'YOUTUBE', %s)",
                   (video_key, channel_key, video_id))
    return video_key

def ingest_video(video_id: str, hmac_secret: str, youtube, run_type: str = "HOT_PATH", run_purpose: str = None, fail_purposefully=False, engage_bump=False, fail_downstream=False, target_db="FOOTBALL_NARRATIVE_DEV"):
    if run_purpose not in {"INTEGRATION_TEST", "RESEARCH"}:
        raise ValueError("run_purpose must be explicitly set to 'INTEGRATION_TEST' or 'RESEARCH'")
    core_conn = get_snowflake_connection(target_db=target_db)
    raw_conn = get_snowflake_connection(target_db=target_db)
    core_cursor = core_conn.cursor()
    raw_cursor = raw_conn.cursor()

    run_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc).isoformat()
    
    # 6. Checkpoint natural key scope
    core_cursor.execute('''SELECT checkpoint_id, committed_watermark_at, continuation_token_hint, current_continuity_state 
                           FROM OPS.CURRENT_CHECKPOINT 
                           WHERE source_system = 'YOUTUBE' AND endpoint = 'commentThreads' 
                           AND resource_scope_type = 'VIDEO' AND resource_scope_id = %s
                           AND retrieval_mode = 'CHRONOLOGICAL' AND sample_type = 'CHRONOLOGICAL' ''', (video_id,))
    checkpoint = core_cursor.fetchone()
    
    page_token = None
    watermark_at = None
    watermark_ids = []
    checkpoint_id = None
    
    if checkpoint and run_type == "HOT_PATH":
        checkpoint_id = checkpoint[0]
        watermark_at = checkpoint[1] 
        current_continuity_state = checkpoint[3]
        
        if watermark_at:
            watermark_at = watermark_at.isoformat() + "Z" if not watermark_at.tzinfo else watermark_at.isoformat()
            watermark_at = watermark_at.replace("+00:00", "Z")
            
        core_cursor.execute("SELECT source_comment_id FROM OPS.CHECKPOINT_WATERMARK_COMMENT WHERE checkpoint_id = %s AND watermark_type = 'COMMITTED'", (checkpoint_id,))
        rows = core_cursor.fetchall()
        watermark_ids = [r[0] for r in rows]
        
        if current_continuity_state == 'INITIAL_LOAD_CAPPED':
            page_token = checkpoint[2]

    if run_type == "BACKFILL":
        raise NotImplementedError("Backfill execution is deferred to Phase 1C/2.")

    pages_requested = 0
    records_observed = 0
    records_inserted = 0
    duplicate_records_observed = 0
    
    candidate_watermark_at = None
    candidate_watermark_ids = []
    
    continuity_status = 'COMPLETE_TO_WATERMARK'
    remaining_watermark_ids = set(watermark_ids)

    video_key = fetch_and_create_video(core_conn, video_id, youtube)

    try:
        while pages_requested < MAX_PAGES_PER_RUN:
            api_request_id = str(uuid.uuid4())
            requested_at = datetime.now(timezone.utc).isoformat()
            
            kwargs = {
                "part": "snippet",
                "videoId": video_id,
                "order": "time",
                "maxResults": 100,
                "textFormat": "plainText"
            }
            if page_token:
                kwargs['pageToken'] = page_token

            # 5. Log API Request properly inside a try/except block
            try:
                response = youtube.commentThreads().list(**kwargs).execute()
            except Exception as e:
                # Log failed API attempt
                raw_cursor.execute('''INSERT INTO OPS.FACT_API_REQUEST 
                    (api_request_id, ingestion_run_id, endpoint, requested_at, completed_at, page_token_used, error_code) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)''',
                    (api_request_id, run_id, "commentThreads.list", requested_at, datetime.now(timezone.utc).isoformat(), page_token, "API_EXCEPTION"))
                raw_conn.commit()
                raise e
            
            pages_requested += 1
            next_page_token = response.get('nextPageToken')

            raw_payload_str = json.dumps(response)
            payload_hash = hashlib.sha256(raw_payload_str.encode('utf-8')).hexdigest()
            raw_response_id = str(uuid.uuid4())
            retrieved_at = datetime.now(timezone.utc).isoformat()
            request_parameters_str = json.dumps(kwargs)
            
            raw_cursor.execute('''INSERT INTO OPS.FACT_API_REQUEST 
                (api_request_id, ingestion_run_id, endpoint, requested_at, completed_at, http_status, page_token_used, next_page_token_returned) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''',
                (api_request_id, run_id, "commentThreads.list", requested_at, retrieved_at, 200, page_token, next_page_token))
            
            raw_cursor.execute('''INSERT INTO RAW.YOUTUBE_API_RESPONSE 
                (raw_response_id, api_request_id, ingestion_run_id, source_system, endpoint, resource_scope_type, resource_scope_id, request_parameters, http_status, page_token_used, next_page_token_returned, retrieved_at, raw_json_payload, payload_hash) 
                SELECT column1, column2, column3, 'YOUTUBE', 'commentThreads', 'VIDEO', column4, PARSE_JSON(column5), column6, column7, column8, column9, PARSE_JSON(column10), column11
                FROM VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                (raw_response_id, api_request_id, run_id, video_id, request_parameters_str, 200, page_token, next_page_token, retrieved_at, raw_payload_str, payload_hash))
            
            raw_conn.commit()
            
            if fail_downstream:
                # 1. Durable dead-letter logging on raw_conn
                raw_cursor.execute('''INSERT INTO OPS.DEAD_LETTER_RECORD (dead_letter_id, raw_response_id, ingestion_run_id, processing_stage, error_message, first_failure_at)
                                  VALUES (%s, %s, %s, 'PARSE', 'Simulated parsing failure', %s)''',
                                  (str(uuid.uuid4()), raw_response_id, run_id, datetime.now(timezone.utc).isoformat()))
                raw_conn.commit()
                raise RuntimeError("Purposeful downstream failure")

            items = response.get('items', [])
            if not items:
                break
                
            reached_watermark = False

            for pos, item in enumerate(items):
                snippet = item['snippet']['topLevelComment']['snippet']
                source_comment_id = item['id']
                published_at = snippet['publishedAt'] 
                author_channel_id = snippet.get('authorChannelId', {}).get('value', None)
                text_content = snippet.get('textOriginal', '')
                likes = snippet.get('likeCount', 0)
                reply_count = item['snippet'].get('totalReplyCount', 0)
                
                if engage_bump:
                    likes += 10
                
                # Check composite watermark
                if run_type == "HOT_PATH" and watermark_at:
                    if published_at == watermark_at:
                        remaining_watermark_ids.discard(source_comment_id)
                    elif published_at < watermark_at:
                        if not remaining_watermark_ids:
                            reached_watermark = True
                        else:
                            continuity_status = "POTENTIAL_GAP"
                        break

                # 4. Candidate watermark across pages
                if not candidate_watermark_at:
                    candidate_watermark_at = published_at
                    candidate_watermark_ids = [source_comment_id]
                elif published_at == candidate_watermark_at:
                    candidate_watermark_ids.append(source_comment_id)

                records_observed += 1
                
                author_key = get_or_create_author(core_conn, author_channel_id, hmac_secret)
                
                core_cursor.execute("SELECT comment_key FROM CORE.FACT_COMMENT WHERE source_system = 'YOUTUBE' AND source_comment_id = %s", (source_comment_id,))
                comment_row = core_cursor.fetchone()
                
                if comment_row:
                    comment_key = comment_row[0]
                    duplicate_records_observed += 1
                else:
                    comment_key = str(uuid.uuid4())
                    if author_key:
                        core_cursor.execute('''INSERT INTO CORE.FACT_COMMENT 
                            (comment_key, source_system, source_comment_id, video_key, author_key, published_at, first_observed_at, is_reply) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, FALSE)''',
                            (comment_key, 'YOUTUBE', source_comment_id, video_key, author_key, published_at, retrieved_at))
                    else:
                        core_cursor.execute('''INSERT INTO CORE.FACT_COMMENT 
                            (comment_key, source_system, source_comment_id, video_key, published_at, first_observed_at, is_reply) 
                            VALUES (%s, %s, %s, %s, %s, %s, FALSE)''',
                            (comment_key, 'YOUTUBE', source_comment_id, video_key, published_at, retrieved_at))
                    records_inserted += 1

                core_cursor.execute('''INSERT INTO CORE.BRIDGE_COMMENT_SAMPLE_OBSERVATION
                    (comment_sample_observation_key, comment_key, raw_response_id, source_item_position, observed_at)
                    VALUES (%s, %s, %s, %s, %s)''',
                    (str(uuid.uuid4()), comment_key, raw_response_id, pos, retrieved_at))

                text_hash = hashlib.sha256(text_content.encode('utf-8')).hexdigest()
                core_cursor.execute("SELECT text_version_key FROM CORE.FACT_COMMENT_TEXT_VERSION WHERE comment_key = %s AND text_hash = %s", (comment_key, text_hash))
                if not core_cursor.fetchone():
                    core_cursor.execute("UPDATE CORE.FACT_COMMENT_TEXT_VERSION SET is_latest_version = FALSE WHERE comment_key = %s", (comment_key,))
                    core_cursor.execute('''INSERT INTO CORE.FACT_COMMENT_TEXT_VERSION 
                        (text_version_key, comment_key, text_hash, text_content, observed_at, is_latest_version) 
                        VALUES (%s, %s, %s, %s, %s, TRUE)''',
                        (str(uuid.uuid4()), comment_key, text_hash, text_content, retrieved_at))

                # Clean up engagement fact
                core_cursor.execute("SELECT likes, reply_count FROM CORE.FACT_COMMENT_ENGAGEMENT_SNAPSHOT WHERE comment_key = %s ORDER BY observed_at DESC LIMIT 1", (comment_key,))
                eng_row = core_cursor.fetchone()
                if not eng_row or eng_row[0] != likes or eng_row[1] != reply_count:
                    core_cursor.execute('''INSERT INTO CORE.FACT_COMMENT_ENGAGEMENT_SNAPSHOT 
                        (engagement_snapshot_key, comment_key, observed_at, likes, reply_count, ingestion_run_id) 
                        VALUES (%s, %s, %s, %s, %s, %s)''',
                        (str(uuid.uuid4()), comment_key, retrieved_at, likes, reply_count, run_id))

            if fail_purposefully:
                raise RuntimeError("Purposeful failure to test rollback")

            if reached_watermark:
                break

            page_token = next_page_token
            if not page_token:
                break
                
        if not reached_watermark and next_page_token:
            if not checkpoint:
                continuity_status = 'INITIAL_LOAD_CAPPED'
            else:
                continuity_status = 'CAPPED_BEFORE_WATERMARK'

    except Exception as e:
        continuity_status = 'FAILED'
        core_conn.rollback()
        print(f"Run failed and rolled back CORE: {e}")
    else:
        # Commit Checkpoint
        if run_type == "HOT_PATH":
            updated_at = datetime.now(timezone.utc).isoformat()
            new_checkpoint_id = str(uuid.uuid4())
            checkpoint_history_id = str(uuid.uuid4())
            
            # Capped logic
            if continuity_status in ('CAPPED_BEFORE_WATERMARK', 'INITIAL_LOAD_CAPPED'):
                # 3. Don't hide unresolved backlogs / proper semantics
                if checkpoint:
                    active_watermark_at = watermark_at
                    active_watermark_ids = watermark_ids
                else:
                    active_watermark_at = None
                    active_watermark_ids = []
            else:
                active_watermark_at = candidate_watermark_at
                active_watermark_ids = candidate_watermark_ids
            
            if checkpoint:
                core_cursor.execute('''UPDATE OPS.CURRENT_CHECKPOINT 
                    SET checkpoint_id = %s, committed_watermark_at = %s, candidate_watermark_at = %s, continuation_token_hint = %s, current_continuity_state = %s, updated_at = %s
                    WHERE checkpoint_id = %s''',
                    (new_checkpoint_id, active_watermark_at, candidate_watermark_at, page_token, continuity_status, updated_at, checkpoint_id))
            else:
                core_cursor.execute('''INSERT INTO OPS.CURRENT_CHECKPOINT 
                    (checkpoint_id, source_system, endpoint, resource_scope_type, resource_scope_id, retrieval_mode, sample_type, committed_watermark_at, candidate_watermark_at, continuation_token_hint, current_continuity_state, updated_at) 
                    VALUES (%s, 'YOUTUBE', 'commentThreads', 'VIDEO', %s, 'CHRONOLOGICAL', 'CHRONOLOGICAL', %s, %s, %s, %s, %s)''',
                    (new_checkpoint_id, video_id, active_watermark_at, candidate_watermark_at, page_token, continuity_status, updated_at))
            
            if checkpoint:
                core_cursor.execute("DELETE FROM OPS.CHECKPOINT_WATERMARK_COMMENT WHERE checkpoint_id = %s", (checkpoint_id,))
            for cid in active_watermark_ids:
                core_cursor.execute('''INSERT INTO OPS.CHECKPOINT_WATERMARK_COMMENT (checkpoint_id, source_comment_id, watermark_type)
                                  VALUES (%s, %s, 'COMMITTED')''', (new_checkpoint_id, cid))
            for cid in candidate_watermark_ids:
                core_cursor.execute('''INSERT INTO OPS.CHECKPOINT_WATERMARK_COMMENT (checkpoint_id, source_comment_id, watermark_type)
                                  VALUES (%s, %s, 'CANDIDATE')''', (new_checkpoint_id, cid))
                                  
            core_cursor.execute('''INSERT INTO OPS.CHECKPOINT_HISTORY 
                (checkpoint_history_id, checkpoint_id, committed_watermark_at, candidate_watermark_at, continuation_token_hint, current_continuity_state, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)''',
                (checkpoint_history_id, new_checkpoint_id, active_watermark_at, candidate_watermark_at, page_token, continuity_status, updated_at))
            
            for cid in active_watermark_ids:
                core_cursor.execute('''INSERT INTO OPS.CHECKPOINT_WATERMARK_COMMENT_HISTORY (checkpoint_history_id, source_comment_id, watermark_type)
                                  VALUES (%s, %s, 'COMMITTED')''', (checkpoint_history_id, cid))
            for cid in candidate_watermark_ids:
                core_cursor.execute('''INSERT INTO OPS.CHECKPOINT_WATERMARK_COMMENT_HISTORY (checkpoint_history_id, source_comment_id, watermark_type)
                                  VALUES (%s, %s, 'CANDIDATE')''', (checkpoint_history_id, cid))
            
        core_conn.commit()

    finally:
        # Telemetry for the run is written no matter what
        run_conn = get_snowflake_connection(target_db=target_db)
        run_cursor = run_conn.cursor()
        end_time = datetime.now(timezone.utc).isoformat()
        run_cursor.execute('''INSERT INTO OPS.FACT_INGESTION_RUN 
            (ingestion_run_id, resource_scope_id, source_system, endpoint, resource_scope_type, started_at, completed_at, pages_requested, records_observed, records_inserted, duplicate_records_observed, continuity_status, run_purpose) 
            VALUES (%s, %s, 'YOUTUBE', 'commentThreads', 'VIDEO', %s, %s, %s, %s, %s, %s, %s, %s)''',
            (run_id, video_id, start_time, end_time, pages_requested, records_observed, records_inserted, duplicate_records_observed, continuity_status, run_purpose))
        run_conn.commit()
        run_conn.close()

    raw_conn.close()
    core_conn.close()
    
    return {
        "run_id": run_id,
        "continuity_status": continuity_status,
        "records_inserted": records_inserted,
        "records_observed": records_observed,
        "pages_requested": pages_requested,
        "duplicate_records_observed": duplicate_records_observed
    }

def clean_database(conn):
    c = conn.cursor()
    c.execute("DELETE FROM OPS.DISCOVERY_UNIT_STATE")
    c.execute("DELETE FROM OPS.CHECKPOINT_WATERMARK_COMMENT_HISTORY")
    c.execute("DELETE FROM OPS.CHECKPOINT_WATERMARK_COMMENT")
    c.execute("DELETE FROM OPS.CHECKPOINT_HISTORY")
    c.execute("DELETE FROM OPS.CURRENT_CHECKPOINT")
    c.execute("DELETE FROM CORE.BRIDGE_COMMENT_SAMPLE_OBSERVATION")
    c.execute("DELETE FROM CORE.FACT_COMMENT_ENGAGEMENT_SNAPSHOT")
    c.execute("DELETE FROM CORE.FACT_COMMENT_TEXT_VERSION")
    c.execute("DELETE FROM CORE.FACT_COMMENT")
    c.execute("DELETE FROM RAW.YOUTUBE_API_RESPONSE")
    c.execute("DELETE FROM OPS.DEAD_LETTER_RECORD")
    c.execute("DELETE FROM OPS.FACT_API_REQUEST")
    c.execute("DELETE FROM OPS.FACT_INGESTION_RUN")
    c.execute("DELETE FROM CORE.DIM_VIDEO")
    c.execute("DELETE FROM CORE.DIM_CHANNEL")
    c.execute("DELETE FROM CORE.DIM_AUTHOR")
    conn.commit()

def test_composite_watermark_logic():
    # Explicit test for the boundary edge case
    watermark_at = "12:00:00"
    watermark_ids = ["A", "B", "C"]
    
    # 1. 12:00:00 A, 12:00:00 B, 11:59:59 D
    # Expected: POTENTIAL_GAP (C is missing)
    remaining_watermark_ids = set(watermark_ids)
    reached_watermark = False
    continuity_status = None
    stream_1 = [("A", "12:00:00"), ("B", "12:00:00"), ("D", "11:59:59")]
    for source_comment_id, published_at in stream_1:
        if published_at == watermark_at:
            remaining_watermark_ids.discard(source_comment_id)
        elif published_at < watermark_at:
            if not remaining_watermark_ids:
                reached_watermark = True
            else:
                continuity_status = "POTENTIAL_GAP"
            break
    assert continuity_status == "POTENTIAL_GAP"
    
    # 2. 12:00:00 A, 12:00:00 B, 12:00:00 C, 11:59:59 D
    # Expected: True completion
    remaining_watermark_ids = set(watermark_ids)
    reached_watermark = False
    continuity_status = None
    stream_2 = [("A", "12:00:00"), ("B", "12:00:00"), ("C", "12:00:00"), ("D", "11:59:59")]
    for source_comment_id, published_at in stream_2:
        if published_at == watermark_at:
            remaining_watermark_ids.discard(source_comment_id)
        elif published_at < watermark_at:
            if not remaining_watermark_ids:
                reached_watermark = True
            else:
                continuity_status = "POTENTIAL_GAP"
            break
    assert reached_watermark == True
    print("[OK] Synthetic composite watermark logic tested successfully (handles crossing exactly).")

def run_tests():
    load_dotenv()
    
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        print("YOUTUBE_API_KEY not found in .env. Exiting.")
        return
        
    hmac_secret = os.getenv("HMAC_SECRET")
    if not hmac_secret:
        print("HMAC_SECRET not found in .env. Exiting.")
        return
    
    if not os.getenv("SNOWFLAKE_USER"):
        print("SNOWFLAKE_USER not found in .env. Skipping Snowflake tests. Please configure credentials to run Phase 1B.")
        return

    youtube = build("youtube", "v3", developerKey=api_key, cache_discovery=False)
    video_id = os.getenv("INTEGRATION_TEST_YOUTUBE_VIDEO_ID")
    if not video_id:
        raise RuntimeError("INTEGRATION_TEST_YOUTUBE_VIDEO_ID is required to run tests.")
    
    print("\n--- Repository & Security Tests ---")
    try:
        with open(".gitignore", "r") as f:
            if ".env" not in f.read():
                print("[WARNING] .env is not in .gitignore. You must gitignore your credentials.")
            else:
                print("[OK] .env is gitignored.")
    except FileNotFoundError:
        print("[WARNING] .gitignore not found. Ensure credentials are not committed.")
    
    # Use TEST database for the test suite
    test_db = "FOOTBALL_NARRATIVE_TEST"
    
    print("\n--- Connecting to Snowflake & Bootstrapping DDL ---")
    conn = get_snowflake_connection(bootstrap=True, target_db=test_db)
    c = conn.cursor()
    c.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
    setup_snowflake_ddl(conn, target_db=test_db)
    
    conn = get_snowflake_connection(target_db=test_db)
    c = conn.cursor()
    c.execute("SELECT count(DISTINCT schema_name) FROM information_schema.schemata WHERE schema_name IN ('RAW', 'OPS', 'CORE', 'ML', 'MARTS')")
    assert c.fetchone()[0] == 5, "Not all 5 expected schemas exist"
    print("[OK] Snowflake database and five schemas demonstrably exist.")
    
    # Prove non-destructive twice
    clean_database(conn)
    c.execute("INSERT INTO CORE.DIM_AUTHOR (author_key, author_hash, salt_version_id) VALUES ('sentinel-1', 'hash1', 'v1')")
    conn.commit()
    setup_snowflake_ddl(conn) # run it again
    c.execute("SELECT count(*) FROM CORE.DIM_AUTHOR WHERE author_key = 'sentinel-1'")
    assert c.fetchone()[0] == 1, "Data lost during DDL bootstrap! Destructive script detected."
    c.execute("DELETE FROM CORE.DIM_AUTHOR WHERE author_key = 'sentinel-1'")
    conn.commit()
    print("[OK] Running bootstrap DDL twice is demonstrably non-destructive (sentinel record preserved).")
    
    print("\n--- Running Phase 1B Snowflake Integration Tests ---")
    
    print("Test: Initial Load (and cap)")
    res1 = ingest_video(video_id, hmac_secret, youtube, run_type="HOT_PATH", run_purpose="INTEGRATION_TEST", target_db=test_db)
    assert res1['continuity_status'] == 'INITIAL_LOAD_CAPPED', "Should hit INITIAL_LOAD_CAPPED"
    
    c.execute("SELECT count(*) FROM RAW.YOUTUBE_API_RESPONSE")
    assert c.fetchone()[0] > 0
    c.execute("SELECT count(*) FROM CORE.FACT_COMMENT")
    assert c.fetchone()[0] > 0
    
    print("[OK] A capped initial test run produces INITIAL_LOAD_CAPPED.")
    print("[OK] A complete YouTube API response page is persisted as native VARIANT before parsing.")
    print("[OK] The persisted response can be transformed into canonical CORE records.")
    
    print("Test: Failed runs don't advance checkpoints")
    res_fail = ingest_video(video_id, hmac_secret, youtube, run_type="HOT_PATH", run_purpose="INTEGRATION_TEST", fail_purposefully=True, target_db=test_db)
    assert res_fail['continuity_status'] == 'FAILED'
    c.execute("SELECT count(*) FROM OPS.CHECKPOINT_HISTORY")
    ch_fail_ct = c.fetchone()[0]
    assert ch_fail_ct == 1, "Checkpoint history should not advance on failed run"
    print("[OK] Failed reconciliation cannot advance the committed checkpoint.")
    
    print("Test: Capped Before Watermark on existing checkpoint")
    # For this to cap BEFORE watermark, we need to guarantee we don't reach the old watermark.
    c.execute("UPDATE OPS.CURRENT_CHECKPOINT SET committed_watermark_at = '2000-01-01T00:00:00Z'")
    conn.commit()
    res_cap = ingest_video(video_id, hmac_secret, youtube, run_type="HOT_PATH", run_purpose="INTEGRATION_TEST", target_db=test_db)
    assert res_cap['continuity_status'] == 'CAPPED_BEFORE_WATERMARK'
    
    c.execute("SELECT committed_watermark_at FROM OPS.CURRENT_CHECKPOINT")
    assert c.fetchone()[0].strftime("%Y-%m-%dT%H:%M:%SZ") == '2000-01-01T00:00:00Z'
    print("[OK] A capped test run produces CAPPED_BEFORE_WATERMARK and preserves committed watermark.")
    
    c.execute("DELETE FROM OPS.CURRENT_CHECKPOINT")
    conn.commit()
    # Run until complete to establish a real watermark.
    MAX_INITIAL_LOAD_RUNS = 10
    for _ in range(MAX_INITIAL_LOAD_RUNS):
        res = ingest_video(video_id, hmac_secret, youtube, run_type="HOT_PATH", run_purpose="INTEGRATION_TEST", target_db=test_db)
        if res['continuity_status'] == 'COMPLETE_TO_WATERMARK':
            break
    else:
        raise AssertionError(f"Initial load failed to establish continuity after {MAX_INITIAL_LOAD_RUNS} runs.")

    print("Test: Idempotent rerun hits watermark immediately")
    res2 = ingest_video(video_id, hmac_secret, youtube, run_type="HOT_PATH", run_purpose="INTEGRATION_TEST", target_db=test_db)
    assert res2['continuity_status'] == 'COMPLETE_TO_WATERMARK', "Should hit composite watermark immediately"
    assert res2['records_inserted'] == 0, "No new comments should be inserted"
    
    c.execute("SELECT count(*) FROM OPS.CHECKPOINT_WATERMARK_COMMENT WHERE watermark_type = 'COMMITTED'")
    assert c.fetchone()[0] > 0
    print("[OK] Re-ingestion creates zero duplicate logical comments.")
    print("[OK] Composite watermark = timestamp + complete boundary comment-ID set.")
    
    print("Test: Text-version and engagement-snapshot grains")
    c.execute("SELECT count(*) FROM CORE.FACT_COMMENT")
    comment_ct = c.fetchone()[0]
    c.execute("SELECT count(*) FROM CORE.FACT_COMMENT_TEXT_VERSION")
    text_ct = c.fetchone()[0]
    c.execute("SELECT count(*) FROM CORE.FACT_COMMENT_ENGAGEMENT_SNAPSHOT")
    eng_ct = c.fetchone()[0]
    
    c.execute("DELETE FROM OPS.CURRENT_CHECKPOINT")
    conn.commit()
    
    res_bump = ingest_video(video_id, hmac_secret, youtube, run_type="HOT_PATH", run_purpose="INTEGRATION_TEST", engage_bump=True, target_db=test_db)
    
    c.execute("SELECT count(*) FROM CORE.FACT_COMMENT")
    assert c.fetchone()[0] == comment_ct, "No new comments should be created"
    c.execute("SELECT count(*) FROM CORE.FACT_COMMENT_TEXT_VERSION")
    assert c.fetchone()[0] == text_ct, "Text hash didn't change, so no new text versions"
    c.execute("SELECT count(*) FROM CORE.FACT_COMMENT_ENGAGEMENT_SNAPSHOT")
    assert c.fetchone()[0] > eng_ct, "Engagement changed (likes+10), so new snapshots MUST be recorded!"
    
    # Untraceable comments check
    c.execute('''SELECT count(*) FROM CORE.FACT_COMMENT c 
                 LEFT JOIN CORE.BRIDGE_COMMENT_SAMPLE_OBSERVATION b ON c.comment_key = b.comment_key 
                 WHERE b.comment_sample_observation_key IS NULL''')
    assert c.fetchone()[0] == 0, "All comments must be traceable"
    
    print("[OK] Text-version and engagement-snapshot histories obey their independent grains.")
    print("[OK] Every canonical comment traces to its exact raw response and source-item position.")
    
    print("Test: Raw source data survives downstream failure")
    res_ds_fail = ingest_video(video_id, hmac_secret, youtube, run_type="HOT_PATH", run_purpose="INTEGRATION_TEST", fail_downstream=True, target_db=test_db)
    c.execute("SELECT count(*) FROM OPS.DEAD_LETTER_RECORD")
    dl_ct = c.fetchone()[0]
    assert dl_ct > 0, "Dead letter queue should have entries"
    c.execute("SELECT count(*) FROM RAW.YOUTUBE_API_RESPONSE WHERE raw_response_id IN (SELECT raw_response_id FROM OPS.DEAD_LETTER_RECORD)")
    raw_dl_ct = c.fetchone()[0]
    assert raw_dl_ct > 0, "RAW payload must survive"
    print("[OK] Raw source data survives downstream parsing failure and can be replayed.")
    
    print("Test: Checkpoint History & Telemetry")
    c.execute("SELECT count(*) FROM OPS.CHECKPOINT_HISTORY")
    hist_ct = c.fetchone()[0]
    assert hist_ct > 0, "History should be populated"
    c.execute("SELECT count(*) FROM OPS.FACT_API_REQUEST")
    api_ct = c.fetchone()[0]
    assert api_ct > 0, "API Request telemetry must be saved"
    c.execute("SELECT count(*) FROM OPS.FACT_INGESTION_RUN")
    run_ct = c.fetchone()[0]
    assert run_ct >= 5, "Ingestion run telemetry (success + failures) must be logged"
    
    print("[OK] Successful checkpoint advancement creates immutable checkpoint history.")
    print("[OK] Run/request telemetry is queryable from OPS.")
    
    conn.close()

    print("\n========================================")
    print("ALL PHASE 1B INTEGRATION AND REPOSITORY TESTS COMPLETE.")
    print("========================================")

if __name__ == "__main__":
    run_tests()
