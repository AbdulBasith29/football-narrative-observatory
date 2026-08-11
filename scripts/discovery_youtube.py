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
    cursor.execute('''
        SELECT ew.window_key, ev.event_version_key, ew.absolute_start_at, ew.absolute_end_at, ev.event_terms
        FROM CORE.DIM_EVENT_WINDOW ew
        JOIN CORE.DIM_EVENT_VERSION ev ON ew.event_version_key = ev.event_version_key
        WHERE ev.is_current = TRUE
    ''')
    windows = []
    for row in cursor.fetchall():
        start = row[2].replace(tzinfo=timezone.utc) if row[2] else None
        end = row[3].replace(tzinfo=timezone.utc) if row[3] else None
        # event_terms is stored as VARIANT (JSON), parse it if it's string or use directly
        terms_raw = row[4]
        if isinstance(terms_raw, str):
            terms = json.loads(terms_raw)
        else:
            terms = terms_raw or []
        terms = [str(t).lower() for t in terms]
            
        windows.append({
            'window_key': row[0],
            'event_version_key': row[1],
            'start': start,
            'end': end,
            'terms': terms
        })
    return windows

def fetch_playlist_items(youtube, playlist_id):
    items = []
    page_token = None
    while True:
        req = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=page_token
        )
        resp = req.execute()
        items.extend(resp.get("items", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return items

def fetch_search_items(youtube, channel_id, query, min_date, max_date):
    items = []
    page_token = None
    while True:
        req = youtube.search().list(
            part="snippet",
            channelId=channel_id,
            q=query,
            publishedAfter=min_date,
            publishedBefore=max_date,
            type="video",
            maxResults=50,
            pageToken=page_token
        )
        resp = req.execute()
        new_items = resp.get("items", [])
        for i in new_items:
            # Normalize schema to match playlistItems
            i['snippet']['resourceId'] = {'videoId': i['id']['videoId']}
        items.extend(new_items)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return items

def evaluate_video_against_events(video, aliases, event_to_windows):
    results = []
    title = video["snippet"]["title"]
    desc = video["snippet"].get("description", "")
    pub_at_str = video["snippet"]["publishedAt"]
    pub_at = datetime.fromisoformat(pub_at_str.replace('Z', '+00:00'))
    
    title_lower = title.lower()
    desc_lower = desc.lower()
    
    for ev_key, ev_windows in event_to_windows.items():
        matched_window = None
        for w in ev_windows:
            if w['start'] <= pub_at <= w['end']:
                matched_window = w
                break
                
        if not matched_window:
            results.append((ev_key, 'INELIGIBLE', 'OUT_OF_WINDOW'))
        else:
            is_relevant = False
            for alias in aliases:
                if re.search(r'\b' + re.escape(alias) + r'\b', title_lower) or re.search(r'\b' + re.escape(alias) + r'\b', desc_lower):
                    is_relevant = True
                    break
            if not is_relevant:
                for term in matched_window['terms']:
                    if re.search(r'\b' + re.escape(term) + r'\b', title_lower) or re.search(r'\b' + re.escape(term) + r'\b', desc_lower):
                        is_relevant = True
                        break
            if not is_relevant:
                results.append((ev_key, 'INELIGIBLE', 'NO_TARGET_RELEVANCE'))
            else:
                results.append((ev_key, 'ELIGIBLE', None))
    return results

def discover_videos(target_db="FOOTBALL_NARRATIVE_DEV", run_purpose="RESEARCH", frame_version_key=None, use_search_fallback=False):
    if not frame_version_key:
        raise ValueError("frame_version_key must be explicitly provided.")
    if run_purpose not in {"INTEGRATION_TEST", "RESEARCH"}:
        raise ValueError("run_purpose must be 'INTEGRATION_TEST' or 'RESEARCH'")
        
    conn = get_snowflake_connection(target_db=target_db)
    cursor = conn.cursor()
    
    # 1. Reject PIPELINE_PILOT if RESEARCH
    if run_purpose == "RESEARCH":
        cursor.execute("SELECT frame_purpose FROM CORE.DIM_CHANNEL_FRAME_VERSION WHERE frame_version_key = %s", (frame_version_key,))
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"frame_version_key {frame_version_key} not found.")
        if row[0] == "PIPELINE_PILOT":
            raise ValueError("RESEARCH discovery structurally rejects PIPELINE_PILOT frames.")

    # 2. Get eligible channels via BRIDGE_FRAME_CHANNEL
    cursor.execute('''
        SELECT c.channel_key, c.source_id 
        FROM CORE.DIM_CHANNEL c
        JOIN CORE.BRIDGE_FRAME_CHANNEL b ON c.channel_key = b.channel_key
        WHERE b.frame_version_key = %s AND b.frame_inclusion_status = 'ELIGIBLE'
    ''', (frame_version_key,))
    channels = cursor.fetchall()

    if not channels:
        print("No eligible channels found for the given frame_version_key.")
        return

    api_key = os.getenv("YOUTUBE_API_KEY")
    youtube = build("youtube", "v3", developerKey=api_key, cache_discovery=False)
    
    aliases = get_aliases(conn)
    windows = get_event_windows_and_terms(conn)
    
    event_to_windows = {}
    for w in windows:
        event_to_windows.setdefault(w['event_version_key'], []).append(w)
    
    # Fetch actual sampling policy version key instead of conflating string
    methodology_version = '1.2'
    cursor.execute("SELECT sampling_policy_version_key FROM CORE.DIM_SAMPLING_POLICY_VERSION WHERE policy_name=%s", (f"Methodology v{methodology_version}",))
    sp_row = cursor.fetchone()
    if sp_row:
        sampling_policy_version_key = sp_row[0]
    else:
        raise ValueError(f"Sampling policy 'Methodology v{methodology_version}' not found in DIM_SAMPLING_POLICY_VERSION. Sync config first.")
    
    for ch_key, ch_id in channels:
        print(f"Discovering for channel: {ch_id}")
        
        if use_search_fallback:
            discovery_method = 'search_list_fallback'
            min_date = min([w['start'] for w in windows]).isoformat().replace("+00:00", "Z")
            max_date = max([w['end'] for w in windows]).isoformat().replace("+00:00", "Z")
            search_query = " | ".join([f'"{a}"' for a in aliases[:3]])
            items = fetch_search_items(youtube, ch_id, search_query, min_date, max_date)
        else:
            discovery_method = 'uploads_playlist'
            ch_resp = youtube.channels().list(part="contentDetails", id=ch_id).execute()
            if not ch_resp.get("items"):
                continue
            uploads_id = ch_resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
            items = fetch_playlist_items(youtube, uploads_id)
            
        provenance = json.dumps({
            "method": discovery_method,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        
        for item in items:
            v_id = item["snippet"]["resourceId"]["videoId"]
            pub_at_str = item["snippet"]["publishedAt"]
            pub_at = datetime.fromisoformat(pub_at_str.replace('Z', '+00:00'))
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
                               (v_key, ch_key, v_id, pub_at_str, datetime.now(timezone.utc).isoformat()))
            
            # Record video snapshot
            run_id = str(uuid.uuid4())
            cursor.execute('''INSERT INTO CORE.FACT_VIDEO_SNAPSHOT (video_snapshot_key, video_key, observed_at, title, description, ingestion_run_id)
                              VALUES (%s, %s, %s, %s, %s, %s)''',
                           (str(uuid.uuid4()), v_key, datetime.now(timezone.utc).isoformat(), title, desc, run_id))
            
            # Evaluate against ALL events for full candidate universe tracking
            results = evaluate_video_against_events(item, aliases, event_to_windows)
            
            for ev_key, status, reason in results:
                # Idempotency check + Insert
                cursor.execute('''SELECT video_event_key FROM CORE.BRIDGE_VIDEO_EVENT 
                                  WHERE video_key = %s AND event_version_key = %s AND sampling_policy_version_key = %s''', 
                               (v_key, ev_key, sampling_policy_version_key))
                if cursor.fetchone():
                    continue
                    
                cursor.execute('''INSERT INTO CORE.BRIDGE_VIDEO_EVENT 
                                  (video_event_key, video_key, event_version_key, sampling_policy_version_key, inclusion_status, primary_exclusion_reason, discovery_method, discovery_provenance)
                                  SELECT %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)''',
                               (str(uuid.uuid4()), v_key, ev_key, sampling_policy_version_key, status, reason, discovery_method, provenance))
                               
    conn.commit()
    conn.close()

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    # Note: Requires frame_version_key in practice.
    pass
