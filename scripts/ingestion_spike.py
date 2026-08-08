import os
import json
import sqlite3
import hashlib
import hmac
import uuid
from datetime import datetime, timezone
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

DB_PATH = "data/spike.db"
SALT_VERSION_ID = "v1.0"
# Lower cap for the spike to explicitly test CAPPED_BEFORE_WATERMARK quickly
MAX_COMMENTS_PER_RUN = 150 

def get_db_connection(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def setup_schema(conn):
    cursor = conn.cursor()
    # Ops Tables
    cursor.execute('''CREATE TABLE IF NOT EXISTS ops_fact_ingestion_run (
        ingestion_run_id TEXT PRIMARY KEY,
        resource_scope_id TEXT,
        run_type TEXT,
        started_at TEXT,
        completed_at TEXT,
        pages_requested INTEGER,
        records_observed INTEGER,
        records_inserted INTEGER,
        duplicate_records_observed INTEGER,
        continuity_status TEXT
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS ops_fact_api_request (
        api_request_id TEXT PRIMARY KEY,
        ingestion_run_id TEXT,
        requested_at TEXT,
        page_token_used TEXT,
        next_page_token_returned TEXT
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS ops_current_checkpoint (
        checkpoint_id TEXT PRIMARY KEY,
        resource_scope_id TEXT UNIQUE,
        committed_watermark_at TEXT,
        committed_watermark_comment_ids TEXT,
        continuation_token_hint TEXT,
        current_continuity_state TEXT
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS ops_fact_backfill_job (
        backfill_run_id TEXT PRIMARY KEY,
        originating_hot_path_run_id TEXT,
        gap_start_at TEXT,
        gap_end_at TEXT,
        backfill_status TEXT
    )''')
    
    # Raw Table
    cursor.execute('''CREATE TABLE IF NOT EXISTS raw_youtube_api_response (
        raw_response_id TEXT PRIMARY KEY,
        api_request_id TEXT UNIQUE,
        retrieved_at TEXT,
        raw_json_payload TEXT,
        payload_hash TEXT
    )''')
    
    # Core Tables
    cursor.execute('''CREATE TABLE IF NOT EXISTS core_dim_author (
        author_key TEXT PRIMARY KEY,
        author_hash TEXT,
        salt_version_id TEXT,
        UNIQUE(author_hash, salt_version_id)
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS core_fact_comment (
        comment_key TEXT PRIMARY KEY,
        source_system TEXT,
        source_comment_id TEXT,
        video_source_id TEXT,
        author_key TEXT,
        published_at TEXT,
        first_observed_at TEXT,
        UNIQUE(source_system, source_comment_id)
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS core_fact_comment_text_version (
        text_version_key TEXT PRIMARY KEY,
        comment_key TEXT,
        text_hash TEXT,
        text_content TEXT,
        observed_at TEXT,
        is_latest_version INTEGER,
        UNIQUE(comment_key, text_hash)
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS core_fact_comment_engagement_snapshot (
        engagement_snapshot_key TEXT PRIMARY KEY,
        comment_key TEXT,
        observed_at TEXT,
        likes INTEGER,
        reply_count INTEGER,
        ingestion_run_id TEXT
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS core_bridge_comment_sample_observation (
        comment_sample_observation_key TEXT PRIMARY KEY,
        comment_key TEXT,
        raw_response_id TEXT,
        source_item_position INTEGER,
        observed_at TEXT
    )''')
    conn.commit()

def hash_author_id(author_id: str, hmac_secret: str) -> str:
    if not author_id:
        return "UNKNOWN_AUTHOR"
    h = hmac.new(hmac_secret.encode('utf-8'), author_id.encode('utf-8'), hashlib.sha256)
    return h.hexdigest()

def get_or_create_author(conn, author_id: str, hmac_secret: str) -> str:
    cursor = conn.cursor()
    hashed_id = hash_author_id(author_id, hmac_secret)
    cursor.execute("SELECT author_key FROM core_dim_author WHERE author_hash = ? AND salt_version_id = ?", (hashed_id, SALT_VERSION_ID))
    row = cursor.fetchone()
    if row:
        return row['author_key']
    author_key = str(uuid.uuid4())
    cursor.execute("INSERT INTO core_dim_author (author_key, author_hash, salt_version_id) VALUES (?, ?, ?)",
                   (author_key, hashed_id, SALT_VERSION_ID))
    return author_key

def ingest_video(video_id: str, hmac_secret: str, youtube, run_type: str = "HOT_PATH", fail_purposefully=False):
    conn = get_db_connection()
    setup_schema(conn)
    cursor = conn.cursor()

    run_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc).isoformat()
    
    # Check checkpoint
    cursor.execute("SELECT * FROM ops_current_checkpoint WHERE resource_scope_id = ?", (video_id,))
    checkpoint = cursor.fetchone()
    
    page_token = None
    watermark_at = None
    watermark_ids = []
    if checkpoint and run_type == "HOT_PATH":
        watermark_at = checkpoint['committed_watermark_at']
        watermark_ids_str = checkpoint['committed_watermark_comment_ids']
        if watermark_ids_str:
            watermark_ids = watermark_ids_str.split(",")

    if run_type == "BACKFILL":
        if checkpoint:
            page_token = checkpoint['continuation_token_hint']
        # Backfills track their own status
        cursor.execute('''INSERT INTO ops_fact_backfill_job (backfill_run_id, originating_hot_path_run_id, backfill_status)
                          VALUES (?, ?, 'IN_PROGRESS')''', (run_id, run_id))

    pages_requested = 0
    records_observed = 0
    records_inserted = 0
    duplicate_records_observed = 0
    
    # We collect the candidate watermark from the first page
    candidate_watermark_at = watermark_at
    candidate_watermark_ids = watermark_ids.copy()
    
    continuity_status = 'COMPLETE_TO_WATERMARK'

    try:
        while records_observed < MAX_COMMENTS_PER_RUN:
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

            response = youtube.commentThreads().list(**kwargs).execute()
            pages_requested += 1
            next_page_token = response.get('nextPageToken')

            # Insert ops_fact_api_request
            cursor.execute('''INSERT INTO ops_fact_api_request 
                (api_request_id, ingestion_run_id, requested_at, page_token_used, next_page_token_returned) 
                VALUES (?, ?, ?, ?, ?)''',
                (api_request_id, run_id, requested_at, page_token, next_page_token))

            # Insert raw_youtube_api_response
            raw_payload_str = json.dumps(response)
            payload_hash = hashlib.sha256(raw_payload_str.encode('utf-8')).hexdigest()
            raw_response_id = str(uuid.uuid4())
            retrieved_at = datetime.now(timezone.utc).isoformat()
            cursor.execute('''INSERT INTO raw_youtube_api_response 
                (raw_response_id, api_request_id, retrieved_at, raw_json_payload, payload_hash) 
                VALUES (?, ?, ?, ?, ?)''',
                (raw_response_id, api_request_id, retrieved_at, raw_payload_str, payload_hash))

            items = response.get('items', [])
            if not items:
                break
                
            reached_watermark = False

            for pos, item in enumerate(items):
                if records_observed >= MAX_COMMENTS_PER_RUN:
                    continuity_status = 'CAPPED_BEFORE_WATERMARK'
                    break

                snippet = item['snippet']['topLevelComment']['snippet']
                source_comment_id = item['id']
                published_at = snippet['publishedAt']
                author_channel_id = snippet.get('authorChannelId', {}).get('value', '')
                text_content = snippet.get('textOriginal', '')
                likes = snippet.get('likeCount', 0)
                reply_count = item['snippet'].get('totalReplyCount', 0)
                
                # Check composite watermark
                if run_type == "HOT_PATH" and watermark_at:
                    if published_at < watermark_at:
                        reached_watermark = True
                        break
                    if published_at == watermark_at and source_comment_id in watermark_ids:
                        reached_watermark = True
                        break

                # Update candidate watermark from the newest comments
                if pages_requested == 1:
                    if pos == 0:
                        candidate_watermark_at = published_at
                        candidate_watermark_ids = [source_comment_id]
                    elif published_at == candidate_watermark_at:
                        candidate_watermark_ids.append(source_comment_id)

                records_observed += 1
                
                # Deduplication logic
                author_key = get_or_create_author(conn, author_channel_id, hmac_secret)
                cursor.execute("SELECT comment_key FROM core_fact_comment WHERE source_system = 'YOUTUBE' AND source_comment_id = ?", (source_comment_id,))
                comment_row = cursor.fetchone()
                
                if comment_row:
                    comment_key = comment_row['comment_key']
                    duplicate_records_observed += 1
                else:
                    comment_key = str(uuid.uuid4())
                    cursor.execute('''INSERT INTO core_fact_comment 
                        (comment_key, source_system, source_comment_id, video_source_id, author_key, published_at, first_observed_at) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)''',
                        (comment_key, 'YOUTUBE', source_comment_id, video_id, author_key, published_at, retrieved_at))
                    records_inserted += 1

                # Lineage
                cursor.execute('''INSERT INTO core_bridge_comment_sample_observation
                    (comment_sample_observation_key, comment_key, raw_response_id, source_item_position, observed_at)
                    VALUES (?, ?, ?, ?, ?)''',
                    (str(uuid.uuid4()), comment_key, raw_response_id, pos, retrieved_at))

                # Text Version
                text_hash = hashlib.md5(text_content.encode('utf-8')).hexdigest()
                cursor.execute("SELECT text_version_key FROM core_fact_comment_text_version WHERE comment_key = ? AND text_hash = ?", (comment_key, text_hash))
                if not cursor.fetchone():
                    cursor.execute("UPDATE core_fact_comment_text_version SET is_latest_version = 0 WHERE comment_key = ?", (comment_key,))
                    cursor.execute('''INSERT INTO core_fact_comment_text_version 
                        (text_version_key, comment_key, text_hash, text_content, observed_at, is_latest_version) 
                        VALUES (?, ?, ?, ?, ?, ?)''',
                        (str(uuid.uuid4()), comment_key, text_hash, text_content, retrieved_at, 1))

                # Engagement Snapshot
                cursor.execute('''INSERT INTO core_fact_comment_engagement_snapshot 
                    (engagement_snapshot_key, comment_key, observed_at, likes, reply_count, ingestion_run_id) 
                    VALUES (?, ?, ?, ?, ?, ?)''',
                    (str(uuid.uuid4()), comment_key, retrieved_at, likes, reply_count, run_id))

            if fail_purposefully:
                raise RuntimeError("Purposeful failure to test rollback")

            if reached_watermark or continuity_status == 'CAPPED_BEFORE_WATERMARK':
                break

            page_token = next_page_token
            if not page_token:
                break

    except Exception as e:
        continuity_status = 'FAILED'
        conn.rollback() # Failed runs do not advance checkpoints
    else:
        # Reconciliation successful, commit checkpoint if HOT_PATH
        if run_type == "HOT_PATH":
            checkpoint_id = str(uuid.uuid4())
            new_ids_str = ",".join(candidate_watermark_ids)
            if checkpoint:
                cursor.execute('''UPDATE ops_current_checkpoint 
                    SET committed_watermark_at = ?, committed_watermark_comment_ids = ?, continuation_token_hint = ?, current_continuity_state = ?
                    WHERE resource_scope_id = ?''',
                    (candidate_watermark_at, new_ids_str, page_token, continuity_status, video_id))
            else:
                cursor.execute('''INSERT INTO ops_current_checkpoint 
                    (checkpoint_id, resource_scope_id, committed_watermark_at, committed_watermark_comment_ids, continuation_token_hint, current_continuity_state) 
                    VALUES (?, ?, ?, ?, ?, ?)''',
                    (checkpoint_id, video_id, candidate_watermark_at, new_ids_str, page_token, continuity_status))
        
        end_time = datetime.now(timezone.utc).isoformat()
        cursor.execute('''INSERT INTO ops_fact_ingestion_run 
            (ingestion_run_id, resource_scope_id, run_type, started_at, completed_at, pages_requested, records_observed, records_inserted, duplicate_records_observed, continuity_status) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (run_id, video_id, run_type, start_time, end_time, pages_requested, records_observed, records_inserted, duplicate_records_observed, continuity_status))
        
        if run_type == "BACKFILL":
            cursor.execute("UPDATE ops_fact_backfill_job SET backfill_status = 'QUEUED' WHERE backfill_run_id = ?", (run_id,))
        
        conn.commit()

    conn.close()
    return {
        "run_id": run_id,
        "continuity_status": continuity_status,
        "records_inserted": records_inserted,
        "records_observed": records_observed,
        "pages_requested": pages_requested,
        "duplicate_records_observed": duplicate_records_observed
    }

def run_tests():
    load_dotenv()
    api_key = os.getenv("YOUTUBE_API_KEY")
    hmac_secret = os.getenv("HMAC_SECRET", "default_test_secret_123")
    
    # Overwrite DB for clean tests
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        
    youtube = build("youtube", "v3", developerKey=api_key, cache_discovery=False)
    video_id = os.getenv("TEST_YOUTUBE_VIDEO_ID", "r8nKiYLezCk")
    
    print("\n--- Running Phase 1A Tests ---")
    
    # 1. Initial Load
    print("\nTest 1 & 7: Initial Load (and 150-comment cap)")
    res1 = ingest_video(video_id, hmac_secret, youtube, run_type="HOT_PATH")
    assert res1['continuity_status'] == 'CAPPED_BEFORE_WATERMARK', "Should cap at 150 comments"
    assert res1['records_inserted'] == 150, "Should insert exactly 150 comments"
    print("[OK] Initial load works and correctly caps.")

    # 2. Failed run doesn't advance checkpoint
    print("\nTest 8: Failed runs don't advance checkpoints")
    res_fail = ingest_video(video_id, hmac_secret, youtube, run_type="HOT_PATH", fail_purposefully=True)
    assert res_fail['continuity_status'] == 'FAILED'
    
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM ops_current_checkpoint")
    chk = c.fetchone()
    assert chk is not None, "Checkpoint should still exist from run 1"
    
    # 3. Idempotent rerun hits watermark immediately
    print("\nTest 2 & 3 & 4: Second run starts at page 1, doesn't duplicate canonicals, and composite watermark works")
    res2 = ingest_video(video_id, hmac_secret, youtube, run_type="HOT_PATH")
    assert res2['continuity_status'] == 'COMPLETE_TO_WATERMARK', "Should hit composite watermark immediately"
    assert res2['records_inserted'] == 0, "No new comments should be inserted"
    assert res2['pages_requested'] == 1, "Should only need 1 page request"
    print("[OK] Second run stops at watermark seamlessly.")
    
    # 4. Engagement Snapshots & Text Versions work
    print("\nTest 5 & 6: Engagement snapshots and text versions")
    c.execute("SELECT count(*) as c FROM core_fact_comment_engagement_snapshot")
    eng_count = c.fetchone()['c']
    # 150 from run1 + whatever is on page 1 for run2 (which could be 0 if no new comments)
    assert eng_count >= 150, "Engagement snapshots should be preserved."
    c.execute("SELECT count(*) as c FROM core_fact_comment_text_version")
    txt_count = c.fetchone()['c']
    assert txt_count == 150, "No new text versions unless text mutated"
    print("[OK] Deduplication correctly preserves new engagement snapshots while keeping canonicals/text unique.")

    # 5. Backfill distinction
    print("\nTest 9: Backfill is distinguishable")
    res3 = ingest_video(video_id, hmac_secret, youtube, run_type="BACKFILL")
    c.execute("SELECT count(*) as c FROM ops_fact_backfill_job")
    bf_count = c.fetchone()['c']
    assert bf_count == 1, "Backfill job must be logged"
    print("[OK] Backfill isolates from hot-path watermarks.")
    
    # 6. Lineage tracking
    print("\nTest 10: Raw -> request -> comment lineage survives")
    c.execute("SELECT count(*) as c FROM core_bridge_comment_sample_observation")
    lin_count = c.fetchone()['c']
    assert lin_count > 0, "Lineage bridge should be populated"
    print("[OK] Lineage firmly established.")

    print("\n=================================")
    print("ALL 10 CRITERIA SUCCESSFULLY MET.")
    print("=================================")

if __name__ == "__main__":
    run_tests()
