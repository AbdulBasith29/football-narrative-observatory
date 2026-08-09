import os
import uuid
import json
from datetime import datetime, timezone
from googleapiclient.discovery import build
from ingestion_snowflake import get_snowflake_connection
import re

def get_aliases(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT alias_text FROM CORE.DIM_TARGET_ENTITY_ALIAS WHERE is_active = TRUE")
    return [row[0].lower() for row in cursor.fetchall()]

def get_event_windows_and_terms(conn):
    cursor = conn.cursor()
    # Need to fetch event_terms. Wait, we didn't add event_terms to DIM_EVENT_VERSION in sync_config, 
    # but we can fetch it via the script or just pass it in. For simplicity since we didn't update DDL for event_terms,
    # let's assume we can fetch them or we use a basic config loader for terms.
    # To keep it isolated, I will load event_terms directly from the config here for the pilot.
    import yaml
    with open("config/event_windows.yml", 'r') as f:
        data = yaml.safe_load(f)
    terms_map = {}
    for ev in data.get('events', []):
        terms_map[ev['external_event_id']] = [t.lower() for t in ev.get('event_terms', [])]

    cursor.execute('''
        SELECT ew.window_key, ev.event_version_key, ew.absolute_start_at, ew.absolute_end_at, e.external_event_id
        FROM CORE.DIM_EVENT_WINDOW ew
        JOIN CORE.DIM_EVENT_VERSION ev ON ew.event_version_key = ev.event_version_key
        JOIN CORE.DIM_EVENT e ON ev.event_key = e.event_key
        WHERE ev.is_current = TRUE
    ''')
    windows = []
    for row in cursor.fetchall():
        start = row[2].replace(tzinfo=timezone.utc) if row[2] else None
        end = row[3].replace(tzinfo=timezone.utc) if row[3] else None
        windows.append({
            'window_key': row[0],
            'event_version_key': row[1],
            'start': start,
            'end': end,
            'terms': terms_map.get(row[4], [])
        })
    return windows

def check_eligibility(video, aliases, windows):
    title = video['snippet']['title'].lower()
    desc = video['snippet'].get('description', '').lower()
    
    pub_at_str = video['snippet']['publishedAt']
    pub_at = datetime.fromisoformat(pub_at_str.replace('Z', '+00:00'))
    
    # 1. Window Match
    matched_window = None
    for w in windows:
        if w['start'] <= pub_at <= w['end']:
            matched_window = w
            break
            
    if not matched_window:
        return 'INELIGIBLE', 'OUT_OF_WINDOW', None

    # 2. Relevance Match (Alias OR Event Term)
    is_relevant = False
    
    for alias in aliases:
        if re.search(r'\b' + re.escape(alias) + r'\b', title) or re.search(r'\b' + re.escape(alias) + r'\b', desc):
            is_relevant = True
            break
            
    if not is_relevant:
        for term in matched_window['terms']:
            if re.search(r'\b' + re.escape(term) + r'\b', title) or re.search(r'\b' + re.escape(term) + r'\b', desc):
                is_relevant = True
                break

    if not is_relevant:
        return 'INELIGIBLE', 'NO_TARGET_RELEVANCE', matched_window['event_version_key']
        
    return 'ELIGIBLE', None, matched_window['event_version_key']

def discover_videos(target_db="FOOTBALL_NARRATIVE_DEV", run_purpose="RESEARCH", use_search_fallback=False):
    if run_purpose not in {"INTEGRATION_TEST", "RESEARCH"}:
        raise ValueError("run_purpose must be 'INTEGRATION_TEST' or 'RESEARCH'")
        
    api_key = os.getenv("YOUTUBE_API_KEY")
    youtube = build("youtube", "v3", developerKey=api_key, cache_discovery=False)
    conn = get_snowflake_connection(target_db=target_db)
    cursor = conn.cursor()
    
    aliases = get_aliases(conn)
    windows = get_event_windows_and_terms(conn)
    
    cursor.execute("SELECT channel_key, source_id FROM CORE.DIM_CHANNEL WHERE source_system = 'YOUTUBE'")
    channels = cursor.fetchall()
    
    methodology_version = 'v1.1'
    
    for ch_key, ch_id in channels:
        print(f"Discovering for channel: {ch_id}")
        
        # Determine method based on fallback toggle
        # (For the pilot, we might invoke this with use_search_fallback=True for historical tests)
        if use_search_fallback:
            discovery_method = 'search_list_fallback'
            # Find the earliest start and latest end across all windows
            min_date = min([w['start'] for w in windows]).isoformat().replace("+00:00", "Z")
            max_date = max([w['end'] for w in windows]).isoformat().replace("+00:00", "Z")
            
            search_query = " | ".join([f'"{a}"' for a in aliases[:3]]) # basic query for quota
            req = youtube.search().list(
                part="snippet",
                channelId=ch_id,
                q=search_query,
                publishedAfter=min_date,
                publishedBefore=max_date,
                type="video",
                maxResults=50
            )
            resp = req.execute()
            items = resp.get("items", [])
            # Search returns videoId nested differently
            for i in items:
                i['snippet']['resourceId'] = {'videoId': i['id']['videoId']}
        else:
            discovery_method = 'uploads_playlist'
            ch_resp = youtube.channels().list(part="contentDetails", id=ch_id).execute()
            if not ch_resp.get("items"):
                continue
            uploads_id = ch_resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
            
            req = youtube.playlistItems().list(
                part="snippet",
                playlistId=uploads_id,
                maxResults=50
            )
            resp = req.execute()
            items = resp.get("items", [])
            
        provenance = json.dumps({
            "method": discovery_method,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        
        for item in items:
            v_id = item["snippet"]["resourceId"]["videoId"]
            pub_at = item["snippet"]["publishedAt"]
            title = item["snippet"]["title"]
            desc = item["snippet"].get("description", "")
            
            cursor.execute("SELECT video_key FROM CORE.DIM_VIDEO WHERE source_id = %s", (v_id,))
            row = cursor.fetchone()
            if row:
                v_key = row[0]
            else:
                v_key = str(uuid.uuid4())
                cursor.execute('''INSERT INTO CORE.DIM_VIDEO (video_key, channel_key, source_system, source_id, published_at, first_observed_at) 
                                  VALUES (%s, %s, 'YOUTUBE', %s, %s, %s)''', 
                               (v_key, ch_key, v_id, pub_at, datetime.now(timezone.utc).isoformat()))
            
            # Insert to FACT_VIDEO_SNAPSHOT
            run_id = str(uuid.uuid4())
            cursor.execute('''INSERT INTO CORE.FACT_VIDEO_SNAPSHOT (video_snapshot_key, video_key, observed_at, title, description, ingestion_run_id)
                              VALUES (%s, %s, %s, %s, %s, %s)''',
                           (str(uuid.uuid4()), v_key, datetime.now(timezone.utc).isoformat(), title, desc, run_id))
            
            status, reason, ev_key = check_eligibility(item, aliases, windows)
            
            if ev_key:
                # Idempotency check at video x event x methodology version grain
                cursor.execute('''SELECT video_event_key FROM CORE.BRIDGE_VIDEO_EVENT 
                                  WHERE video_key = %s AND event_version_key = %s AND sampling_policy_version_key = %s''', 
                               (v_key, ev_key, methodology_version))
                if cursor.fetchone():
                    continue # Already bridged for this methodology
                    
                cursor.execute('''INSERT INTO CORE.BRIDGE_VIDEO_EVENT 
                                  (video_event_key, video_key, event_version_key, sampling_policy_version_key, inclusion_status, primary_exclusion_reason, discovery_method, discovery_provenance)
                                  VALUES (%s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s))''',
                               (str(uuid.uuid4()), v_key, ev_key, methodology_version, status, reason, discovery_method, provenance))
                               
    conn.commit()
    conn.close()

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    discover_videos(target_db=os.getenv("TARGET_DATABASE", "FOOTBALL_NARRATIVE_DEV"))
