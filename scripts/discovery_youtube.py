import os
import uuid
import json
import hashlib
import re
from datetime import datetime, timezone
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from ingestion_snowflake import get_snowflake_connection

def compute_query_hash(search_query: str) -> str:
    return hashlib.sha256(search_query.encode('utf-8')).hexdigest()

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

def fetch_playlist_items(youtube, playlist_id, min_date, max_date):
    items = []
    page_token = None
    last_pub_at = None
    is_monotonic = True
    
    while True:
        req = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=page_token
        )
        resp = req.execute()
        new_items = resp.get("items", [])
        if not new_items:
            break
            
        for i in new_items:
            pub_at = datetime.fromisoformat(i["snippet"]["publishedAt"].replace('Z', '+00:00'))
            
            if last_pub_at and pub_at > last_pub_at:
                is_monotonic = False
            last_pub_at = pub_at
            
            if pub_at > max_date:
                continue
            if pub_at < min_date:
                if is_monotonic:
                    return items # Early stop optimization
                else:
                    continue # Keep paginating if invariant broken
            items.append(i)
            
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return items

def generate_discovery_units(frame_version_key, channels, windows, aliases, sampling_policy_version_key, discovery_policy_version="1.0", search_query_character_budget=500):
    """
    Deterministically generates expected discovery units for (Frame x Channel x Window x DiscoveryPolicy x QueryHash).
    """
    sorted_aliases = sorted(aliases)
    units = []
    
    for ch_key, ch_id in channels:
        for w in windows:
            sorted_terms = sorted(w['terms'])
            all_terms = sorted_aliases + sorted_terms
            query_parts = [f'"{t}"' for t in all_terms]
            
            batches = []
            current_batch = []
            current_len = 0
            
            for part in query_parts:
                add_len = len(part) + (3 if current_batch else 0)
                if current_len + add_len > search_query_character_budget and current_batch:
                    batches.append(" | ".join(current_batch))
                    current_batch = [part]
                    current_len = len(part)
                else:
                    current_batch.append(part)
                    current_len += add_len
                    
            if current_batch:
                batches.append(" | ".join(current_batch))
                
            for batch_num, search_query in enumerate(batches, 1):
                q_hash = compute_query_hash(search_query)
                units.append({
                    "frame_version_key": frame_version_key,
                    "channel_key": ch_key,
                    "channel_id": ch_id,
                    "window_key": w["window_key"],
                    "event_version_key": w["event_version_key"],
                    "start": w["start"],
                    "end": w["end"],
                    "sampling_policy_version_key": sampling_policy_version_key,
                    "discovery_policy_version": discovery_policy_version,
                    "query_batch_number": batch_num,
                    "query_hash": q_hash,
                    "search_query": search_query
                })
    return units

def get_discovery_unit_states(conn, frame_version_key, discovery_policy_version="1.0"):
    cursor = conn.cursor()
    try:
        cursor.execute('''
            SELECT discovery_unit_key, frame_version_key, channel_key, window_key, 
                   sampling_policy_version_key, discovery_policy_version, query_batch_number, 
                   query_hash, search_query, status, pages_completed, next_page_token, 
                   items_observed, unique_video_ids_observed, search_calls_consumed, 
                   started_at, updated_at, completed_at, last_error_code, last_error_message, 
                   first_ingestion_run_id, latest_ingestion_run_id
            FROM OPS.DISCOVERY_UNIT_STATE
            WHERE frame_version_key = %s AND discovery_policy_version = %s
        ''', (frame_version_key, discovery_policy_version))
        rows = cursor.fetchall()
    except Exception:
        return {}
        
    states = {}
    for r in rows:
        key = (r[1], r[2], r[3], r[5], r[7]) # (frame_version_key, channel_key, window_key, discovery_policy_version, query_hash)
        states[key] = {
            "discovery_unit_key": r[0],
            "frame_version_key": r[1],
            "channel_key": r[2],
            "window_key": r[3],
            "sampling_policy_version_key": r[4],
            "discovery_policy_version": r[5],
            "query_batch_number": r[6],
            "query_hash": r[7],
            "search_query": r[8],
            "status": r[9],
            "pages_completed": r[10] or 0,
            "next_page_token": r[11],
            "items_observed": r[12] or 0,
            "unique_video_ids_observed": r[13] or 0,
            "search_calls_consumed": r[14] or 0,
            "started_at": r[15],
            "updated_at": r[16],
            "completed_at": r[17],
            "last_error_code": r[18],
            "last_error_message": r[19],
            "first_ingestion_run_id": r[20],
            "latest_ingestion_run_id": r[21]
        }
    return states

def upsert_discovery_unit_state(conn, state):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT discovery_unit_key, started_at FROM OPS.DISCOVERY_UNIT_STATE 
        WHERE frame_version_key = %s AND channel_key = %s AND window_key = %s 
          AND discovery_policy_version = %s AND query_hash = %s
    ''', (state['frame_version_key'], state['channel_key'], state['window_key'], state['discovery_policy_version'], state['query_hash']))
    row = cursor.fetchone()
    now_iso = datetime.now(timezone.utc).isoformat()
    
    last_error_code = state.get('last_error_code')
    last_error_message = state.get('last_error_message')
    if state['status'] == 'COMPLETED':
        last_error_code = None
        last_error_message = None
        
    if row:
        unit_key = row[0]
        existing_started_at = row[1]
        started_at = existing_started_at or state.get('started_at')
        cursor.execute('''
            UPDATE OPS.DISCOVERY_UNIT_STATE 
            SET status = %s, pages_completed = %s, next_page_token = %s, 
                items_observed = %s, unique_video_ids_observed = %s, search_calls_consumed = %s, 
                started_at = %s, updated_at = %s, completed_at = %s, 
                last_error_code = %s, last_error_message = %s, latest_ingestion_run_id = %s
            WHERE discovery_unit_key = %s
        ''', (state['status'], state['pages_completed'], state.get('next_page_token'),
              state['items_observed'], state['unique_video_ids_observed'], state['search_calls_consumed'],
              started_at, now_iso, state.get('completed_at'),
              last_error_code, last_error_message, state.get('ingestion_run_id'), unit_key))
    else:
        unit_key = str(uuid.uuid4())
        started_at = state.get('started_at')
        cursor.execute('''
            INSERT INTO OPS.DISCOVERY_UNIT_STATE (
                discovery_unit_key, frame_version_key, channel_key, window_key, 
                sampling_policy_version_key, discovery_policy_version, query_batch_number, 
                query_hash, search_query, status, pages_completed, next_page_token, 
                items_observed, unique_video_ids_observed, search_calls_consumed, 
                started_at, updated_at, completed_at, last_error_code, last_error_message, 
                first_ingestion_run_id, latest_ingestion_run_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            unit_key, state['frame_version_key'], state['channel_key'], state['window_key'],
            state['sampling_policy_version_key'], state['discovery_policy_version'], state['query_batch_number'],
            state['query_hash'], state['search_query'], state['status'], state['pages_completed'],
            state.get('next_page_token'), state['items_observed'], state['unique_video_ids_observed'],
            state['search_calls_consumed'], started_at, now_iso, state.get('completed_at'),
            last_error_code, last_error_message,
            state.get('ingestion_run_id'), state.get('ingestion_run_id')
        ))
    conn.commit()
    return unit_key

def estimate_search_calls(expected_units, existing_states):
    total_expected = len(expected_units)
    completed_count = 0
    unfinished_units = []
    
    for u in expected_units:
        key = (u['frame_version_key'], u['channel_key'], u['window_key'], u['discovery_policy_version'], u['query_hash'])
        st = existing_states.get(key)
        if st and st['status'] == 'COMPLETED':
            completed_count += 1
        else:
            unfinished_units.append(u)
            
    return {
        "total_expected_units": total_expected,
        "completed_units_count": completed_count,
        "remaining_units_count": len(unfinished_units),
        "minimum_required_calls": len(unfinished_units)
    }

def fetch_search_items_for_windows(youtube, channel_id, aliases, windows, search_query_character_budget=500, run_search_call_budget=100):
    """
    Backwards-compatible helper for search item fetching.
    """
    dummy_channel_key = f"ch_{channel_id}"
    dummy_frame_key = "dummy_frame"
    dummy_sp_key = "dummy_sp"
    
    channels = [(dummy_channel_key, channel_id)]
    units = generate_discovery_units(dummy_frame_key, channels, windows, aliases, dummy_sp_key, search_query_character_budget=search_query_character_budget)
    
    all_items = {}
    provenance_queries = []
    actual_search_calls = 0
    
    for u in units:
        provenance_queries.append({
            "window_key": u["window_key"],
            "batch_number": u["query_batch_number"],
            "query": u["search_query"],
            "character_budget": search_query_character_budget,
            "query_hash": u["query_hash"]
        })
        
        min_date_str = u['start'].isoformat().replace("+00:00", "Z")
        max_date_str = u['end'].isoformat().replace("+00:00", "Z")
        page_token = None
        
        while True:
            if actual_search_calls >= run_search_call_budget:
                print(f"Reached search call budget ({run_search_call_budget}). Stopping search calls.")
                break
                
            actual_search_calls += 1
            try:
                req = youtube.search().list(
                    part="snippet",
                    channelId=channel_id,
                    q=u['search_query'],
                    publishedAfter=min_date_str,
                    publishedBefore=max_date_str,
                    type="video",
                    maxResults=50,
                    pageToken=page_token
                )
                resp = req.execute()
            except Exception as err:
                err_str = str(err).lower()
                if "429" in err_str or "quotaexceeded" in err_str or "ratelimitexceeded" in err_str or (hasattr(err, 'resp') and getattr(err.resp, 'status', None) in (429, 403)):
                    print("API Quota exceeded / 429 encountered in fetch_search_items_for_windows.")
                    break
                raise err
                
            new_items = resp.get("items", [])
            for i in new_items:
                if 'videoId' in i['id']:
                    i['snippet']['resourceId'] = {'videoId': i['id']['videoId']}
                    all_items[i['id']['videoId']] = i
                    
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
                
        if actual_search_calls >= run_search_call_budget:
            break
            
    return list(all_items.values()), provenance_queries

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

def discover_videos(
    target_db="FOOTBALL_NARRATIVE_DEV",
    run_purpose="RESEARCH",
    frame_version_key=None,
    use_search_fallback=False,
    run_search_call_budget=100,
    search_query_character_budget=500,
    discovery_policy_version="1.0"
):
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
        conn.close()
        return {"run_status": "COMPLETE", "actual_search_calls": 0, "minimum_required_calls": 0}

    api_key = os.getenv("YOUTUBE_API_KEY")
    youtube = build("youtube", "v3", developerKey=api_key, cache_discovery=False)
    
    aliases = get_aliases(conn)
    windows = get_event_windows_and_terms(conn)
    
    event_to_windows = {}
    for w in windows:
        event_to_windows.setdefault(w['event_version_key'], []).append(w)
    
    methodology_version = '1.2'
    cursor.execute("SELECT sampling_policy_version_key FROM CORE.DIM_SAMPLING_POLICY_VERSION WHERE policy_name=%s", (f"Methodology v{methodology_version}",))
    sp_row = cursor.fetchone()
    if sp_row:
        sampling_policy_version_key = sp_row[0]
    else:
        raise ValueError(f"Sampling policy 'Methodology v{methodology_version}' not found in DIM_SAMPLING_POLICY_VERSION. Sync config first.")
        
    ingestion_run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    actual_search_calls = 0
    run_outcome = "COMPLETE"
    
    if use_search_fallback:
        discovery_method = 'search_list_fallback'
        expected_units = generate_discovery_units(
            frame_version_key, channels, windows, aliases, 
            sampling_policy_version_key, discovery_policy_version=discovery_policy_version,
            search_query_character_budget=search_query_character_budget
        )
        
        existing_states = get_discovery_unit_states(conn, frame_version_key, discovery_policy_version=discovery_policy_version)
        pre_run_metrics = estimate_search_calls(expected_units, existing_states)
        
        print(f"Pre-Run Search Call Estimation: Total Units={pre_run_metrics['total_expected_units']}, "
              f"Completed={pre_run_metrics['completed_units_count']}, "
              f"Remaining={pre_run_metrics['remaining_units_count']}, "
              f"Minimum Required Calls={pre_run_metrics['minimum_required_calls']}")
              
        stopped_early = False
        encountered_non_quota_error = False
        
        for unit in expected_units:
            unit_key = (unit['frame_version_key'], unit['channel_key'], unit['window_key'], unit['discovery_policy_version'], unit['query_hash'])
            curr_state = existing_states.get(unit_key)
            
            if curr_state and curr_state['status'] == 'COMPLETED':
                continue
                
            if stopped_early or encountered_non_quota_error:
                break
                
            page_token = curr_state['next_page_token'] if curr_state else None
            pages_completed = curr_state['pages_completed'] if curr_state else 0
            items_observed = curr_state['items_observed'] if curr_state else 0
            calls_this_unit = curr_state['search_calls_consumed'] if curr_state else 0
            started_at = (curr_state.get('started_at') if curr_state else None) or datetime.now(timezone.utc).isoformat()
            
            unit_items = {}
            min_date_str = unit['start'].isoformat().replace("+00:00", "Z")
            max_date_str = unit['end'].isoformat().replace("+00:00", "Z")
            
            unit_status = 'IN_PROGRESS'
            last_error_code = None
            last_error_message = None
            hit_limit = False
            
            while True:
                if actual_search_calls >= run_search_call_budget:
                    print(f"Hit process search call budget ({run_search_call_budget}). Halting clean.")
                    unit_status = 'PARTIAL_QUOTA_LIMIT'
                    hit_limit = True
                    stopped_early = True
                    break
                    
                actual_search_calls += 1
                calls_this_unit += 1
                pages_completed += 1
                
                try:
                    req = youtube.search().list(
                        part="snippet",
                        channelId=unit['channel_id'],
                        q=unit['search_query'],
                        publishedAfter=min_date_str,
                        publishedBefore=max_date_str,
                        type="video",
                        maxResults=50,
                        pageToken=page_token
                    )
                    resp = req.execute()
                except Exception as err:
                    err_str = str(err).lower()
                    status_code = getattr(getattr(err, 'resp', None), 'status', None)
                    if status_code in (429, 403) or "429" in err_str or "quotaexceeded" in err_str or "ratelimitexceeded" in err_str:
                        print("API Quota Limit (429) hit during search list. Halting clean and persisting state.")
                        unit_status = 'PARTIAL_QUOTA_LIMIT'
                        hit_limit = True
                        stopped_early = True
                        break
                    else:
                        print(f"Non-quota error hit during search list: {err}")
                        unit_status = 'PARTIAL_ERROR'
                        encountered_non_quota_error = True
                        last_error_code = f"HTTP_{status_code}" if status_code else "API_ERROR"
                        last_error_message = str(err)[:1024]
                        break
                        
                new_items = resp.get("items", [])
                items_observed += len(new_items)
                
                for i in new_items:
                    if 'videoId' in i['id']:
                        v_id = i['id']['videoId']
                        i['snippet']['resourceId'] = {'videoId': v_id}
                        unit_items[v_id] = i
                        
                page_token = resp.get("nextPageToken")
                if not page_token:
                    unit_status = 'COMPLETED'
                    break
            
            # Persist Discovery Unit State
            st_record = {
                "frame_version_key": unit['frame_version_key'],
                "channel_key": unit['channel_key'],
                "window_key": unit['window_key'],
                "sampling_policy_version_key": unit['sampling_policy_version_key'],
                "discovery_policy_version": unit['discovery_policy_version'],
                "query_batch_number": unit['query_batch_number'],
                "query_hash": unit['query_hash'],
                "search_query": unit['search_query'],
                "status": unit_status,
                "pages_completed": pages_completed if unit_status == 'COMPLETED' else max(0, pages_completed - 1),
                "next_page_token": page_token if hit_limit else None,
                "items_observed": items_observed,
                "unique_video_ids_observed": len(unit_items),
                "search_calls_consumed": calls_this_unit,
                "started_at": started_at,
                "completed_at": datetime.now(timezone.utc).isoformat() if unit_status == 'COMPLETED' else None,
                "last_error_code": last_error_code,
                "last_error_message": last_error_message,
                "ingestion_run_id": ingestion_run_id
            }
            upsert_discovery_unit_state(conn, st_record)
            
            # Save discovered videos to DB
            search_provenance = [{
                "window_key": unit["window_key"],
                "batch_number": unit["query_batch_number"],
                "query": unit["search_query"],
                "query_hash": unit["query_hash"],
                "unit_status": unit_status
            }]
            provenance = json.dumps({
                "method": discovery_method,
                "queries": search_provenance,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            
            for v_id, item in unit_items.items():
                pub_at_str = item["snippet"]["publishedAt"]
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
                                   (v_key, unit['channel_key'], v_id, pub_at_str, datetime.now(timezone.utc).isoformat()))
                
                cursor.execute('''INSERT INTO CORE.FACT_VIDEO_SNAPSHOT (video_snapshot_key, video_key, observed_at, title, description, ingestion_run_id)
                                  VALUES (%s, %s, %s, %s, %s, %s)''',
                               (str(uuid.uuid4()), v_key, datetime.now(timezone.utc).isoformat(), title, desc, ingestion_run_id))
                
                results = evaluate_video_against_events(item, aliases, event_to_windows)
                for ev_key, status, reason in results:
                    cursor.execute('''SELECT video_event_key FROM CORE.BRIDGE_VIDEO_EVENT 
                                      WHERE video_key = %s AND event_version_key = %s AND sampling_policy_version_key = %s''', 
                                   (v_key, ev_key, sampling_policy_version_key))
                    if cursor.fetchone():
                        continue
                        
                    cursor.execute('''INSERT INTO CORE.BRIDGE_VIDEO_EVENT 
                                      (video_event_key, video_key, event_version_key, sampling_policy_version_key, inclusion_status, primary_exclusion_reason, discovery_method, discovery_provenance)
                                      SELECT %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)''',
                                   (str(uuid.uuid4()), v_key, ev_key, sampling_policy_version_key, status, reason, discovery_method, provenance))
                                   
        # Check completeness audit rule
        post_states = get_discovery_unit_states(conn, frame_version_key, discovery_policy_version=discovery_policy_version)
        expected_keys = {
            (u['frame_version_key'], u['channel_key'], u['window_key'], u['discovery_policy_version'], u['query_hash'])
            for u in expected_units
        }
        completed_keys = {
            k for k, st in post_states.items() if st['status'] == 'COMPLETED'
        }
        remaining_keys = expected_keys - completed_keys
        
        if len(remaining_keys) == 0:
            run_outcome = "COMPLETE"
        elif stopped_early:
            run_outcome = "PARTIAL_QUOTA_LIMIT"
        elif encountered_non_quota_error:
            run_outcome = "PARTIAL_ERROR"
        else:
            run_outcome = "FAILED"
            
    else:
        # Uploads playlist path
        discovery_method = 'uploads_playlist'
        for ch_key, ch_id in channels:
            min_date = min([w['start'] for w in windows])
            max_date = max([w['end'] for w in windows])
            ch_resp = youtube.channels().list(part="contentDetails", id=ch_id).execute()
            if not ch_resp.get("items"):
                continue
            uploads_id = ch_resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
            items = fetch_playlist_items(youtube, uploads_id, min_date, max_date)
            provenance = json.dumps({
                "method": discovery_method,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            
            for item in items:
                v_id = item["snippet"]["resourceId"]["videoId"]
                pub_at_str = item["snippet"]["publishedAt"]
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
                
                cursor.execute('''INSERT INTO CORE.FACT_VIDEO_SNAPSHOT (video_snapshot_key, video_key, observed_at, title, description, ingestion_run_id)
                                  VALUES (%s, %s, %s, %s, %s, %s)''',
                               (str(uuid.uuid4()), v_key, datetime.now(timezone.utc).isoformat(), title, desc, ingestion_run_id))
                
                results = evaluate_video_against_events(item, aliases, event_to_windows)
                for ev_key, status, reason in results:
                    cursor.execute('''SELECT video_event_key FROM CORE.BRIDGE_VIDEO_EVENT 
                                      WHERE video_key = %s AND event_version_key = %s AND sampling_policy_version_key = %s''', 
                                   (v_key, ev_key, sampling_policy_version_key))
                    if cursor.fetchone():
                        continue
                        
                    cursor.execute('''INSERT INTO CORE.BRIDGE_VIDEO_EVENT 
                                      (video_event_key, video_key, event_version_key, sampling_policy_version_key, inclusion_status, primary_exclusion_reason, discovery_method, discovery_provenance)
                                      SELECT %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)''',
                                   (str(uuid.uuid4()), v_key, ev_key, sampling_policy_version_key, status, reason, discovery_method, provenance))
        run_outcome = "COMPLETE"

    completed_at = datetime.now(timezone.utc).isoformat()
    cursor.execute('''
        INSERT INTO OPS.FACT_INGESTION_RUN (
            ingestion_run_id, source_system, endpoint, resource_scope_type, resource_scope_id,
            started_at, completed_at, pages_requested, pages_succeeded, estimated_quota_consumed,
            run_outcome, continuity_status, run_purpose
        ) VALUES (%s, 'YOUTUBE', %s, 'FRAME', %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', (
        ingestion_run_id,
        'search.list' if use_search_fallback else 'channels.list',
        frame_version_key,
        started_at,
        completed_at,
        actual_search_calls,
        actual_search_calls,
        actual_search_calls,
        run_outcome,
        'DISCOVERY_COMPLETE' if run_outcome == 'COMPLETE' else 'DISCOVERY_PARTIAL',
        run_purpose
    ))
    
    conn.commit()
    conn.close()
    
    return {
        "ingestion_run_id": ingestion_run_id,
        "run_status": run_outcome,
        "actual_search_calls": actual_search_calls
    }

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    pass
