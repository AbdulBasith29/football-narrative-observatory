import os
import uuid
import json
import hashlib
import re
from datetime import datetime, timezone
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from ingestion_snowflake import get_snowflake_connection

def parse_iso8601_duration(duration_str: str) -> int:
    """
    Parses an ISO 8601 duration string (e.g. 'PT4M13S', 'PT58S', 'PT1H2M10S') into integer seconds.
    Returns 0 if duration_str is empty or unparseable.
    """
    if not duration_str or not isinstance(duration_str, str):
        return 0
    pattern = re.compile(
        r'P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?'
    )
    match = pattern.match(duration_str)
    if not match:
        return 0
    parts = match.groupdict()
    days = int(parts.get('days') or 0)
    hours = int(parts.get('hours') or 0)
    minutes = int(parts.get('minutes') or 0)
    seconds = int(parts.get('seconds') or 0)
    return days * 86400 + hours * 3600 + minutes * 60 + seconds

def chunk_video_ids(video_ids, chunk_size: int = 50):
    """
    Deterministically sorts and chunks video IDs into conservative batches of up to 50 IDs.
    50 IDs is the project's conservative batching convention for videos.list.
    """
    unique_sorted = sorted(list(set(video_ids)))
    for i in range(0, len(unique_sorted), chunk_size):
        yield unique_sorted[i:i + chunk_size]

def is_quota_or_rate_limit_error(err) -> bool:
    """
    Distinguishes documented quota/rate-limit reasons from ordinary HTTP 403 forbidden/auth errors.
    """
    status_code = getattr(getattr(err, 'resp', None), 'status', None)
    err_str = str(err).lower()
    if status_code == 429 or "429" in err_str:
        return True
    quota_reasons = {
        "quotaexceeded",
        "ratelimitexceeded",
        "userratelimitexceeded",
        "dailylimitexceeded"
    }
    if hasattr(err, 'error_details'):
        for detail in err.error_details:
            if isinstance(detail, dict):
                reason = detail.get('reason', '').lower()
                if reason in quota_reasons:
                    return True
    content = getattr(err, 'content', b'')
    if isinstance(content, bytes):
        content_str = content.decode('utf-8', errors='ignore').lower()
    else:
        content_str = str(content).lower()
    for qr in quota_reasons:
        if qr in err_str or qr in content_str:
            return True
    return False

def persist_raw_video_response(conn, raw_payload: dict, batch_ids: list, ingestion_run_id: str,
                               api_request_id: str, requested_at: str, completed_at: str, http_status: int = 200):
    """
    Persists verbatim JSON API response into RAW.YOUTUBE_API_RESPONSE and logs OPS.FACT_API_REQUEST
    in an independent transaction BEFORE downstream normalization (raw-before-parse).
    """
    raw_payload_str = json.dumps(raw_payload)
    payload_hash = hashlib.sha256(raw_payload_str.encode('utf-8')).hexdigest()
    raw_response_id = str(uuid.uuid4())
    batch_hash = hashlib.sha256(",".join(sorted(batch_ids)).encode('utf-8')).hexdigest()
    request_params = json.dumps({"part": "snippet,contentDetails,statistics,status", "id_count": len(batch_ids)})
    
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO OPS.FACT_API_REQUEST (
            api_request_id, ingestion_run_id, endpoint, requested_at, completed_at,
            http_status, page_token_used, next_page_token_returned, retry_number,
            error_code, estimated_quota_cost
        ) VALUES (%s, %s, 'videos.list', %s, %s, %s, NULL, NULL, 0, NULL, 1)
    ''', (api_request_id, ingestion_run_id, requested_at, completed_at, http_status))
    
    cursor.execute('''
        INSERT INTO RAW.YOUTUBE_API_RESPONSE (
            raw_response_id, api_request_id, ingestion_run_id, source_system, endpoint,
            resource_scope_type, resource_scope_id, request_parameters, http_status,
            page_token_used, next_page_token_returned, retrieved_at, raw_json_payload,
            payload_hash, parser_version
        ) SELECT %s, %s, %s, 'YOUTUBE', 'videos.list', 'VIDEO_BATCH', %s,
                 PARSE_JSON(%s), %s, NULL, NULL, %s, PARSE_JSON(%s), %s, '1.0'
    ''', (
        raw_response_id, api_request_id, ingestion_run_id, batch_hash,
        request_params, http_status, completed_at, raw_payload_str, payload_hash
    ))
    conn.commit()
    return raw_response_id

def process_video_metadata_payload(conn, raw_payload: dict, requested_batch_ids: list,
                                   ingestion_run_id: str, api_request_id: str, raw_response_id: str,
                                   now_iso: str = None):
    """
    Parses items from raw payload, enriches CORE.DIM_VIDEO, inserts snapshots into CORE.FACT_VIDEO_SNAPSHOT
    with request lineage, and updates OPS.VIDEO_METADATA_RESOLUTION_STATE.
    Replay idempotent: does NOT insert duplicate snapshots for existing (video_key, api_request_id).
    """
    if now_iso is None:
        now_iso = datetime.now(timezone.utc).isoformat()
    cursor = conn.cursor()
    items = raw_payload.get("items", [])
    returned_by_id = {item["id"]: item for item in items if "id" in item}
    requested_set = set(requested_batch_ids)
    
    # 1. Process returned videos
    for v_id, item in returned_by_id.items():
        snippet = item.get("snippet", {})
        content_details = item.get("contentDetails", {})
        statistics = item.get("statistics", {})
        
        pub_at_str = snippet.get("publishedAt")
        title = snippet.get("title", "")
        desc = snippet.get("description", "")
        if len(desc) > 4900:
            desc = desc[:4900]
            
        # Parse duration
        duration_raw = content_details.get("duration", "")
        duration_seconds = parse_iso8601_duration(duration_raw)
        
        # Shorts rule under Decision A.1: Do not infer from duration alone.
        # Preserve is_short = None (UNKNOWN) and fail-closed during cohort selection.
        is_short = None
        short_rule_version = "frozen_v1.2_unresolved_shorts"
        
        # Language handling under Decision A.3:
        # Check defaultAudioLanguage, then defaultLanguage; if missing, UNKNOWN.
        lang = snippet.get("defaultAudioLanguage") or snippet.get("defaultLanguage")
        if lang:
            primary_language_code = lang.strip().lower()[:16]
        else:
            primary_language_code = "UNKNOWN"
            
        views = statistics.get("viewCount")
        likes = statistics.get("likeCount")
        try:
            views = int(views) if views is not None else None
        except (ValueError, TypeError):
            views = None
        try:
            likes = int(likes) if likes is not None else None
        except (ValueError, TypeError):
            likes = None
            
        # Enrich CORE.DIM_VIDEO in-place (Managed Lifecycle Enrichment)
        cursor.execute("SELECT video_key, first_observed_at FROM CORE.DIM_VIDEO WHERE source_system = 'YOUTUBE' AND source_id = %s", (v_id,))
        v_row = cursor.fetchone()
        if v_row:
            v_key = v_row[0]
            cursor.execute('''
                UPDATE CORE.DIM_VIDEO
                SET published_at = COALESCE(published_at, %s),
                    duration_seconds = %s,
                    is_short = %s,
                    short_classification_rule_version = %s,
                    primary_language_code = %s
                WHERE video_key = %s
            ''', (pub_at_str, duration_seconds, is_short, short_rule_version, primary_language_code, v_key))
        else:
            v_key = str(uuid.uuid4())
            cursor.execute('''
                INSERT INTO CORE.DIM_VIDEO (
                    video_key, channel_key, source_system, source_id, published_at,
                    duration_seconds, is_short, short_classification_rule_version,
                    primary_language_code, first_observed_at
                ) VALUES (%s, %s, 'YOUTUBE', %s, %s, %s, %s, %s, %s, %s)
            ''', (
                v_key, snippet.get("channelId", "UNKNOWN"), v_id, pub_at_str,
                duration_seconds, is_short, short_rule_version, primary_language_code, now_iso
            ))
            
        # Idempotent write to CORE.FACT_VIDEO_SNAPSHOT:
        # Replaying the same RAW observation / api_request_id will NOT insert duplicate rows
        snapshot_key = str(uuid.uuid4())
        cursor.execute('''
            INSERT INTO CORE.FACT_VIDEO_SNAPSHOT (
                video_snapshot_key, video_key, observed_at, title, description,
                views, likes, ingestion_run_id, api_request_id, raw_response_id
            )
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM CORE.FACT_VIDEO_SNAPSHOT 
                WHERE video_key = %s AND api_request_id = %s
            )
        ''', (
            snapshot_key, v_key, now_iso, title, desc, views, likes,
            ingestion_run_id, api_request_id, raw_response_id,
            v_key, api_request_id
        ))
        
        # Upsert OPS.VIDEO_METADATA_RESOLUTION_STATE to RESOLVED
        cursor.execute('''
            SELECT resolution_state_key FROM OPS.VIDEO_METADATA_RESOLUTION_STATE
            WHERE source_system = 'YOUTUBE' AND source_id = %s
        ''', (v_id,))
        res_row = cursor.fetchone()
        if res_row:
            cursor.execute('''
                UPDATE OPS.VIDEO_METADATA_RESOLUTION_STATE
                SET resolution_status = 'RESOLVED',
                    resolved_at = %s,
                    updated_at = %s,
                    api_request_id = %s,
                    raw_response_id = %s,
                    latest_ingestion_run_id = %s,
                    last_error_code = NULL,
                    last_error_message = NULL
                WHERE resolution_state_key = %s
            ''', (now_iso, now_iso, api_request_id, raw_response_id, ingestion_run_id, res_row[0]))
        else:
            cursor.execute('''
                INSERT INTO OPS.VIDEO_METADATA_RESOLUTION_STATE (
                    resolution_state_key, source_system, source_id, resolution_status,
                    first_discovered_at, resolved_at, updated_at, api_request_id,
                    raw_response_id, first_ingestion_run_id, latest_ingestion_run_id
                ) VALUES (%s, 'YOUTUBE', %s, 'RESOLVED', %s, %s, %s, %s, %s, %s, %s)
            ''', (
                str(uuid.uuid4()), v_id, now_iso, now_iso, now_iso,
                api_request_id, raw_response_id, ingestion_run_id, ingestion_run_id
            ))
            
    # 2. Process missing / unavailable candidates
    missing_ids = requested_set - set(returned_by_id.keys())
    neutral_rationale = "Video ID was not returned by the videos.list request; underlying cause was not observable from this response."
    
    for m_id in missing_ids:
        # In OPS.VIDEO_METADATA_RESOLUTION_STATE: mark UNAVAILABLE
        cursor.execute('''
            SELECT resolution_state_key FROM OPS.VIDEO_METADATA_RESOLUTION_STATE
            WHERE source_system = 'YOUTUBE' AND source_id = %s
        ''', (m_id,))
        res_row = cursor.fetchone()
        if res_row:
            cursor.execute('''
                UPDATE OPS.VIDEO_METADATA_RESOLUTION_STATE
                SET resolution_status = 'UNAVAILABLE',
                    resolved_at = %s,
                    updated_at = %s,
                    api_request_id = %s,
                    raw_response_id = %s,
                    latest_ingestion_run_id = %s
                WHERE resolution_state_key = %s
            ''', (now_iso, now_iso, api_request_id, raw_response_id, ingestion_run_id, res_row[0]))
        else:
            cursor.execute('''
                INSERT INTO OPS.VIDEO_METADATA_RESOLUTION_STATE (
                    resolution_state_key, source_system, source_id, resolution_status,
                    first_discovered_at, resolved_at, updated_at, api_request_id,
                    raw_response_id, first_ingestion_run_id, latest_ingestion_run_id
                ) VALUES (%s, 'YOUTUBE', %s, 'UNAVAILABLE', %s, %s, %s, %s, %s, %s, %s)
            ''', (
                str(uuid.uuid4()), m_id, now_iso, now_iso, now_iso,
                api_request_id, raw_response_id, ingestion_run_id, ingestion_run_id
            ))
            
        # In CORE.BRIDGE_VIDEO_EVENT: mark INELIGIBLE with VIDEO_UNAVAILABLE
        cursor.execute('''
            UPDATE CORE.BRIDGE_VIDEO_EVENT
            SET inclusion_status = 'INELIGIBLE',
                primary_exclusion_reason = 'VIDEO_UNAVAILABLE',
                inclusion_rationale = %s
            WHERE video_key IN (
                SELECT video_key FROM CORE.DIM_VIDEO WHERE source_system = 'YOUTUBE' AND source_id = %s
            )
        ''', (neutral_rationale, m_id))
        
    conn.commit()
    return len(returned_by_id), len(missing_ids)

def replay_raw_video_responses(conn, ingestion_run_id: str = None, raw_response_id: str = None):
    """
    Offline re-processing of already-persisted RAW.YOUTUBE_API_RESPONSE pages without making
    any external YouTube API calls. Replay preserves snapshot idempotency.
    """
    cursor = conn.cursor()
    query = '''
        SELECT raw_response_id, api_request_id, ingestion_run_id, raw_json_payload, request_parameters
        FROM RAW.YOUTUBE_API_RESPONSE
        WHERE endpoint = 'videos.list'
    '''
    params = []
    if raw_response_id:
        query += " AND raw_response_id = %s"
        params.append(raw_response_id)
    elif ingestion_run_id:
        query += " AND ingestion_run_id = %s"
        params.append(ingestion_run_id)
        
    cursor.execute(query, tuple(params))
    rows = cursor.fetchall()
    
    total_replayed = 0
    for r_id, req_id, run_id, payload_raw, req_params_raw in rows:
        payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
        items = payload.get("items", [])
        requested_ids = [it["id"] for it in items if "id" in it]
        process_video_metadata_payload(conn, payload, requested_ids, run_id, req_id, r_id)
        total_replayed += len(items)
        
    return total_replayed

def ingest_video_metadata(
    target_db="FOOTBALL_NARRATIVE_DEV",
    run_purpose="RESEARCH",
    frame_version_key=None,
    run_video_call_budget=100,
    youtube_client=None
):
    """
    Orchestrates Phase 1D Video Metadata Ingestion for candidate videos scoped to frame_version_key.
    Enforces raw-before-parse, durable OPS resolution states, and conservative 50 ID batching.
    """
    if not frame_version_key:
        raise ValueError("frame_version_key must be explicitly provided.")
    if run_purpose not in {"INTEGRATION_TEST", "RESEARCH"}:
        raise ValueError("run_purpose must be 'INTEGRATION_TEST' or 'RESEARCH'")
        
    conn = get_snowflake_connection(target_db=target_db)
    cursor = conn.cursor()
    
    # 1. Structural Isolation: Reject PIPELINE_PILOT frames for RESEARCH runs
    if run_purpose == "RESEARCH":
        cursor.execute("SELECT frame_purpose FROM CORE.DIM_CHANNEL_FRAME_VERSION WHERE frame_version_key = %s", (frame_version_key,))
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"frame_version_key {frame_version_key} not found.")
        if row[0] == "PIPELINE_PILOT":
            raise ValueError("RESEARCH runs structurally reject PIPELINE_PILOT frames.")
            
    # 2. Query unresolved candidate video IDs requiring metadata retrieval
    # Must have explicit frame lineage in search provenance or repaired uploads discovery
    cursor.execute('''
        SELECT DISTINCT v.source_id
        FROM CORE.DIM_VIDEO v
        JOIN CORE.BRIDGE_VIDEO_EVENT b ON v.video_key = b.video_key
        JOIN CORE.BRIDGE_FRAME_CHANNEL fc ON fc.channel_key = v.channel_key
        LEFT JOIN OPS.VIDEO_METADATA_RESOLUTION_STATE r 
               ON r.source_system = 'YOUTUBE' AND r.source_id = v.source_id
        WHERE fc.frame_version_key = %s
          AND fc.frame_inclusion_status = 'ELIGIBLE'
          AND (
              EXISTS (
                  SELECT 1 FROM TABLE(FLATTEN(input => b.discovery_provenance:queries)) q
                  WHERE q.value:frame_version_key::STRING = %s
              )
              OR (b.discovery_provenance:frame_version_key::STRING = %s)
          )
          AND (r.resolution_status IS NULL OR r.resolution_status IN ('PENDING', 'RETRYABLE_ERROR'))
        ORDER BY v.source_id ASC
    ''', (frame_version_key, frame_version_key, frame_version_key))
    
    candidate_ids = [r[0] for r in cursor.fetchall()]
    if not candidate_ids:
        conn.close()
        return {"run_status": "COMPLETE", "resolved_count": 0, "unavailable_count": 0}
        
    ingestion_run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    
    if youtube_client is None:
        api_key = os.getenv("YOUTUBE_API_KEY")
        youtube_client = build("youtube", "v3", developerKey=api_key, cache_discovery=False)
        
    batches = list(chunk_video_ids(candidate_ids, chunk_size=50))
    calls_executed = 0
    total_resolved = 0
    total_unavailable = 0
    run_outcome = "COMPLETE"
    
    for batch in batches:
        if calls_executed >= run_video_call_budget:
            run_outcome = "PARTIAL_QUOTA_LIMIT"
            break
            
        api_request_id = str(uuid.uuid4())
        req_start = datetime.now(timezone.utc).isoformat()
        calls_executed += 1
        
        try:
            req = youtube_client.videos().list(
                part="snippet,contentDetails,statistics,status",
                id=",".join(batch)
            )
            resp = req.execute()
            req_end = datetime.now(timezone.utc).isoformat()
        except Exception as err:
            req_end = datetime.now(timezone.utc).isoformat()
            if is_quota_or_rate_limit_error(err):
                run_outcome = "PARTIAL_QUOTA_LIMIT"
                break
            else:
                run_outcome = "PARTIAL_ERROR"
                break
                
        # 1. Raw-before-parse: write RAW response in separate transaction
        raw_resp_id = persist_raw_video_response(
            conn, resp, batch, ingestion_run_id, api_request_id, req_start, req_end, http_status=200
        )
        
        # 2. Downstream normalization & resolution state update
        try:
            res_cnt, unavail_cnt = process_video_metadata_payload(
                conn, resp, batch, ingestion_run_id, api_request_id, raw_resp_id, req_end
            )
            total_resolved += res_cnt
            total_unavailable += unavail_cnt
        except Exception as parse_err:
            cursor.execute('''
                INSERT INTO OPS.DEAD_LETTER_RECORD (
                    dead_letter_id, raw_response_id, ingestion_run_id, processing_stage,
                    error_message, first_failure_at
                ) VALUES (%s, %s, %s, 'PARSE', %s, %s)
            ''', (str(uuid.uuid4()), raw_resp_id, ingestion_run_id, str(parse_err)[:500], req_end))
            conn.commit()
            run_outcome = "PARTIAL_ERROR"
            break
            
    completed_at = datetime.now(timezone.utc).isoformat()
    cursor.execute('''
        INSERT INTO OPS.FACT_INGESTION_RUN (
            ingestion_run_id, source_system, endpoint, resource_scope_type, resource_scope_id,
            started_at, completed_at, pages_requested, pages_succeeded, estimated_quota_consumed,
            run_outcome, continuity_status, run_purpose
        ) VALUES (%s, 'YOUTUBE', 'videos.list', 'FRAME', %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', (
        ingestion_run_id, frame_version_key, started_at, completed_at,
        calls_executed, calls_executed, calls_executed,
        run_outcome, 'METADATA_COMPLETE' if run_outcome == 'COMPLETE' else 'METADATA_PARTIAL',
        run_purpose
    ))
    conn.commit()
    conn.close()
    
    return {
        "ingestion_run_id": ingestion_run_id,
        "run_status": run_outcome,
        "calls_executed": calls_executed,
        "resolved_count": total_resolved,
        "unavailable_count": total_unavailable
    }
