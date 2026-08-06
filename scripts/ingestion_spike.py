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
MAX_COMMENTS_PER_RUN = 500

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def setup_schema(conn):
    cursor = conn.cursor()
    # 1. Ops Tables
    cursor.execute('''CREATE TABLE IF NOT EXISTS ops_fact_ingestion_run (
        ingestion_run_id TEXT PRIMARY KEY,
        resource_scope_id TEXT,
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
        continuation_token_hint TEXT,
        current_continuity_state TEXT
    )''')
    # 2. Raw Table
    cursor.execute('''CREATE TABLE IF NOT EXISTS raw_youtube_api_response (
        raw_response_id TEXT PRIMARY KEY,
        api_request_id TEXT UNIQUE,
        retrieved_at TEXT,
        raw_json_payload TEXT,
        payload_hash TEXT
    )''')
    # 3. Core Tables
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

def ingest_video(video_id: str, hmac_secret: str, youtube):
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
    if checkpoint:
        print(f"Resuming from checkpoint for video {video_id}.")
        page_token = checkpoint['continuation_token_hint']
        watermark_at = checkpoint['committed_watermark_at']

    pages_requested = 0
    records_observed = 0
    records_inserted = 0
    duplicate_records_observed = 0
    new_watermark = watermark_at
    continuity_status = 'COMPLETE'

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

            print(f"Requesting page {pages_requested + 1}...")
            response = youtube.commentThreads().list(**kwargs).execute()
            pages_requested += 1
            next_page_token = response.get('nextPageToken')

            # 1. Insert ops_fact_api_request
            cursor.execute('''INSERT INTO ops_fact_api_request 
                (api_request_id, ingestion_run_id, requested_at, page_token_used, next_page_token_returned) 
                VALUES (?, ?, ?, ?, ?)''',
                (api_request_id, run_id, requested_at, page_token, next_page_token))

            # 2. Insert raw_youtube_api_response
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
                    continuity_status = 'CAPPED'
                    break

                snippet = item['snippet']['topLevelComment']['snippet']
                source_comment_id = item['id']
                published_at = snippet['publishedAt']
                author_channel_id = snippet.get('authorChannelId', {}).get('value', '')
                text_content = snippet.get('textOriginal', '')
                likes = snippet.get('likeCount', 0)
                reply_count = item['snippet'].get('totalReplyCount', 0)
                
                # Check watermark
                if watermark_at and published_at <= watermark_at:
                    reached_watermark = True
                    break

                # Update new watermark if this is the first (latest) item of the first page
                if pages_requested == 1 and pos == 0:
                    new_watermark = published_at

                records_observed += 1
                
                # Author
                author_key = get_or_create_author(conn, author_channel_id, hmac_secret)
                
                # Comment
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
                    # Unset previous latest
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

            if reached_watermark or continuity_status == 'CAPPED':
                break

            page_token = next_page_token
            if not page_token:
                break

    except Exception as e:
        print(f"Error during ingestion: {e}")
        continuity_status = 'FAILED'
        conn.rollback()
    else:
        # Reconciliation successful, commit checkpoint
        checkpoint_id = str(uuid.uuid4())
        if checkpoint:
            cursor.execute('''UPDATE ops_current_checkpoint 
                SET committed_watermark_at = ?, continuation_token_hint = ?, current_continuity_state = ?
                WHERE resource_scope_id = ?''',
                (new_watermark, page_token, continuity_status, video_id))
        else:
            cursor.execute('''INSERT INTO ops_current_checkpoint 
                (checkpoint_id, resource_scope_id, committed_watermark_at, continuation_token_hint, current_continuity_state) 
                VALUES (?, ?, ?, ?, ?)''',
                (checkpoint_id, video_id, new_watermark, page_token, continuity_status))
        
        end_time = datetime.now(timezone.utc).isoformat()
        cursor.execute('''INSERT INTO ops_fact_ingestion_run 
            (ingestion_run_id, resource_scope_id, started_at, completed_at, pages_requested, records_observed, records_inserted, duplicate_records_observed, continuity_status) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (run_id, video_id, start_time, end_time, pages_requested, records_observed, records_inserted, duplicate_records_observed, continuity_status))
        conn.commit()

    print("="*50)
    print("INGESTION RUN SUMMARY")
    print(f"Video ID: {video_id}")
    print(f"Pages Requested: {pages_requested}")
    print(f"Records Observed: {records_observed}")
    print(f"Records Inserted (New): {records_inserted}")
    print(f"Duplicates Observed: {duplicate_records_observed}")
    print(f"Continuity Status: {continuity_status}")
    print("="*50)

    conn.close()


def main():
    load_dotenv()
    api_key = os.getenv("YOUTUBE_API_KEY")
    hmac_secret = os.getenv("HMAC_SECRET", "default_test_secret_123")
    
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY is missing from .env")

    youtube = build("youtube", "v3", developerKey=api_key, cache_discovery=False)

    test_videos = [
        os.getenv("TEST_YOUTUBE_VIDEO_ID", "r8nKiYLezCk"),  # from env
    ]
    
    for video_id in test_videos:
        print(f"\n--- Testing Ingestion for Video {video_id} ---")
        ingest_video(video_id, hmac_secret, youtube)
        print(f"\n--- Running idempotent test for Video {video_id} ---")
        ingest_video(video_id, hmac_secret, youtube)


if __name__ == "__main__":
    main()
