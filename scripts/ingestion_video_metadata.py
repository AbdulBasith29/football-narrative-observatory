import os
import uuid
import json
import hashlib
import re
from datetime import datetime, timezone
from googleapiclient.discovery import build
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

def log_fact_api_request(conn, api_request_id: str, ingestion_run_id: str, endpoint: str,
                         requested_at: str, completed_at: str, http_status: int = None,
                         retry_number: int = 0, error_code: str = None, estimated_quota_cost: int = 1):
    """
    Persists FACT_API_REQUEST telemetry for an attempted API call (whether successful or failed).
    """
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO OPS.FACT_API_REQUEST (
            api_request_id, ingestion_run_id, endpoint, requested_at, completed_at,
            http_status, page_token_used, next_page_token_returned, retry_number,
            error_code, estimated_quota_cost
        ) VALUES (%s, %s, %s, %s, %s, %s, NULL, NULL, %s, %s, %s)
    ''', (api_request_id, ingestion_run_id, endpoint, requested_at, completed_at, http_status, retry_number, error_code, estimated_quota_cost))
    conn.commit()

def update_batch_resolution_state(conn, batch_ids: list, status: str, api_request_id: str,
                                  raw_response_id: str, ingestion_run_id: str, error_code: str,
                                  error_message: str, retry_count: int = 0):
    """
    Updates or inserts OPS.VIDEO_METADATA_RESOLUTION_STATE records for a batch of video IDs.
    """
    cursor = conn.cursor()
    now_iso = datetime.now(timezone.utc).isoformat()
    for v_id in batch_ids:
        cursor.execute('''
            SELECT resolution_state_key, retry_count FROM OPS.VIDEO_METADATA_RESOLUTION_STATE
            WHERE source_system = 'YOUTUBE' AND source_id = %s
        ''', (v_id,))
        row = cursor.fetchone()
        if row:
            curr_retries = (row[1] or 0) + retry_count
            cursor.execute('''
                UPDATE OPS.VIDEO_METADATA_RESOLUTION_STATE
                SET resolution_status = %s,
                    updated_at = %s,
                    api_request_id = %s,
                    raw_response_id = %s,
                    latest_ingestion_run_id = %s,
                    last_error_code = %s,
                    last_error_message = %s,
                    retry_count = %s
                WHERE resolution_state_key = %s
            ''', (status, now_iso, api_request_id, raw_response_id, ingestion_run_id, error_code, error_message, curr_retries, row[0]))
        else:
            cursor.execute('''
                INSERT INTO OPS.VIDEO_METADATA_RESOLUTION_STATE (
                    resolution_state_key, source_system, source_id, resolution_status,
                    first_discovered_at, updated_at, api_request_id, raw_response_id,
                    first_ingestion_run_id, latest_ingestion_run_id, last_error_code,
                    last_error_message, retry_count
                ) VALUES (%s, 'YOUTUBE', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                str(uuid.uuid4()), v_id, status, now_iso, now_iso, api_request_id,
                raw_response_id, ingestion_run_id, ingestion_run_id, error_code,
                error_message, retry_count
            ))
    conn.commit()

def persist_raw_video_response(conn, raw_payload: dict, batch_ids: list, ingestion_run_id: str,
                               api_request_id: str, requested_at: str, completed_at: str,
                               http_status: int = 200, retry_number: int = 0):
    """
    Persists verbatim JSON API response into RAW.YOUTUBE_API_RESPONSE and logs OPS.FACT_API_REQUEST
    in an independent transaction BEFORE downstream normalization (raw-before-parse).
    Preserves exact deterministic ordered requested video IDs in request_parameters.
    """
    raw_payload_str = json.dumps(raw_payload)
    payload_hash = hashlib.sha256(raw_payload_str.encode('utf-8')).hexdigest()
    raw_response_id = str(uuid.uuid4())
    batch_hash = hashlib.sha256(",".join(sorted(batch_ids)).encode('utf-8')).hexdigest()
    request_params = json.dumps({
        "part": "snippet,contentDetails,statistics,status",
        "requested_video_ids": list(batch_ids),
        "id_count": len(batch_ids)
    })

    log_fact_api_request(
        conn, api_request_id, ingestion_run_id, 'videos.list',
        requested_at, completed_at, http_status=http_status,
        retry_number=retry_number, error_code=None, estimated_quota_cost=1
    )

    cursor = conn.cursor()
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
    Parses items from raw payload, enriches CORE.DIM_VIDEO with freeze semantics (preserving existing
    resolved attributes like is_short), inserts snapshots into CORE.FACT_VIDEO_SNAPSHOT with request lineage,
    and updates OPS.VIDEO_METADATA_RESOLUTION_STATE.
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

        # Managed Lifecycle Enrichment with Freeze Semantics:
        # Normal metadata refresh must NEVER overwrite previously resolved stable attributes
        # (e.g. existing is_short=True/False must not be erased by UNKNOWN/None).
        cursor.execute(
            "SELECT video_key, first_observed_at, is_short, short_classification_rule_version, "
            "primary_language_code, duration_seconds FROM CORE.DIM_VIDEO "
            "WHERE source_system = 'YOUTUBE' AND source_id = %s",
            (v_id,)
        )
        v_row = cursor.fetchone()
        if v_row:
            v_key = v_row[0]
            existing_is_short = v_row[2]
            existing_rule = v_row[3]
            existing_lang = v_row[4]
            existing_duration = v_row[5]

            # Coalesce / freeze attributes:
            final_is_short = existing_is_short if existing_is_short is not None else is_short
            final_rule = existing_rule if existing_is_short is not None else (
                short_rule_version if is_short is not None else existing_rule
            )
            final_lang = existing_lang if (existing_lang and existing_lang != "UNKNOWN") else primary_language_code
            final_duration = existing_duration if existing_duration is not None else duration_seconds

            cursor.execute('''
                UPDATE CORE.DIM_VIDEO
                SET published_at = COALESCE(published_at, %s),
                    duration_seconds = %s,
                    is_short = %s,
                    short_classification_rule_version = %s,
                    primary_language_code = %s
                WHERE video_key = %s
            ''', (pub_at_str, final_duration, final_is_short, final_rule, final_lang, v_key))
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
    neutral_rationale = (
        "Video ID was not returned by the videos.list request; underlying cause was not observable from this response."
    )

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
    any external YouTube API calls. Reconstructs requested_batch_ids from preserved request_parameters.
    Replay preserves snapshot idempotency and reconciles unavailable videos correctly.
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
        req_params = json.loads(req_params_raw) if isinstance(req_params_raw, str) else (req_params_raw or {})
        requested_ids = req_params.get("requested_video_ids")
        if requested_ids is None:
            # Fallback for legacy records without requested_video_ids
            items = payload.get("items", [])
            requested_ids = [it["id"] for it in items if "id" in it]

        process_video_metadata_payload(conn, payload, requested_ids, run_id, req_id, r_id)
        total_replayed += len(requested_ids)

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
    Enforces test database isolation, raw-before-parse, durable OPS resolution states,
    bounded retries for retryable errors, and conservative 50 ID batching.
    """
    # Test Database Isolation Enforcement
    if run_purpose == "INTEGRATION_TEST" and target_db != "FOOTBALL_NARRATIVE_TEST":
        raise ValueError(
            f"run_purpose 'INTEGRATION_TEST' requires target_db 'FOOTBALL_NARRATIVE_TEST' exclusively; got '{target_db}'."
        )
    if not frame_version_key:
        raise ValueError("frame_version_key must be explicitly provided.")
    if run_purpose not in {"INTEGRATION_TEST", "RESEARCH"}:
        raise ValueError("run_purpose must be 'INTEGRATION_TEST' or 'RESEARCH'")

    conn = get_snowflake_connection(target_db=target_db)
    cursor = conn.cursor()

    # 1. Structural Isolation: Reject PIPELINE_PILOT frames for RESEARCH runs
    if run_purpose == "RESEARCH":
        cursor.execute(
            "SELECT frame_purpose FROM CORE.DIM_CHANNEL_FRAME_VERSION WHERE frame_version_key = %s",
            (frame_version_key,)
        )
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"frame_version_key {frame_version_key} not found.")
        if row[0] == "PIPELINE_PILOT":
            raise ValueError("RESEARCH runs structurally reject PIPELINE_PILOT frames.")

    # 2. Query unresolved candidate video IDs requiring metadata retrieval
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
    calls_attempted = 0
    calls_succeeded = 0
    total_resolved = 0
    total_unavailable = 0
    run_outcome = "COMPLETE"

    for batch in batches:
        if calls_attempted >= run_video_call_budget:
            run_outcome = "PARTIAL_QUOTA_LIMIT"
            break

        batch_success = False
        max_retries = 3
        retry_num = 0
        resp = None
        req_start = None
        req_end = None

        while retry_num < max_retries:
            if calls_attempted >= run_video_call_budget:
                run_outcome = "PARTIAL_QUOTA_LIMIT"
                break

            api_request_id = str(uuid.uuid4())
            req_start = datetime.now(timezone.utc).isoformat()
            calls_attempted += 1

            try:
                req = youtube_client.videos().list(
                    part="snippet,contentDetails,statistics,status",
                    id=",".join(batch)
                )
                resp = req.execute()
                req_end = datetime.now(timezone.utc).isoformat()
                calls_succeeded += 1
                batch_success = True
                break
            except Exception as err:
                req_end = datetime.now(timezone.utc).isoformat()
                status_code = getattr(getattr(err, 'resp', None), 'status', None)

                if is_quota_or_rate_limit_error(err):
                    err_code = "QUOTA_OR_RATE_LIMIT_EXCEEDED"
                    log_fact_api_request(
                        conn, api_request_id, ingestion_run_id, 'videos.list',
                        req_start, req_end, http_status=status_code or 429,
                        retry_number=retry_num, error_code=err_code, estimated_quota_cost=1
                    )
                    run_outcome = "PARTIAL_QUOTA_LIMIT"
                    update_batch_resolution_state(
                        conn, batch, "RETRYABLE_ERROR", api_request_id, None,
                        ingestion_run_id, err_code, str(err)[:500], retry_num + 1
                    )
                    break
                elif status_code in (401, 403):
                    err_code = "AUTH_OR_CONFIG_ERROR"
                    log_fact_api_request(
                        conn, api_request_id, ingestion_run_id, 'videos.list',
                        req_start, req_end, http_status=status_code,
                        retry_number=retry_num, error_code=err_code, estimated_quota_cost=1
                    )
                    run_outcome = "PARTIAL_ERROR"
                    update_batch_resolution_state(
                        conn, batch, "FATAL_ERROR", api_request_id, None,
                        ingestion_run_id, err_code, str(err)[:500], retry_num + 1
                    )
                    break
                else:
                    err_code = f"HTTP_{status_code}" if status_code else "TRANSIENT_NETWORK_ERROR"
                    log_fact_api_request(
                        conn, api_request_id, ingestion_run_id, 'videos.list',
                        req_start, req_end, http_status=status_code or 500,
                        retry_number=retry_num, error_code=err_code, estimated_quota_cost=1
                    )
                    retry_num += 1
                    if calls_attempted >= run_video_call_budget:
                        run_outcome = "PARTIAL_QUOTA_LIMIT"
                        update_batch_resolution_state(
                            conn, batch, "RETRYABLE_ERROR", api_request_id, None,
                            ingestion_run_id, err_code, str(err)[:500], retry_num
                        )
                        break
                    if retry_num >= max_retries:
                        run_outcome = "PARTIAL_ERROR"
                        update_batch_resolution_state(
                            conn, batch, "RETRYABLE_ERROR", api_request_id, None,
                            ingestion_run_id, err_code, str(err)[:500], retry_num
                        )
                        break

        if not batch_success:
            break

        # 1. Raw-before-parse: write RAW response in separate transaction
        raw_resp_id = persist_raw_video_response(
            conn, resp, batch, ingestion_run_id, api_request_id, req_start, req_end,
            http_status=200, retry_number=retry_num
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
                    dead_letter_id, raw_response_id, source_record_id, ingestion_run_id, processing_stage,
                    error_code, error_message, first_failure_at, retry_count, resolution_status
                ) VALUES (%s, %s, NULL, %s, %s, %s, %s, %s, %s, %s)
            ''', (str(uuid.uuid4()), raw_resp_id, ingestion_run_id, 'PARSE', 'PARSE_ERROR', str(parse_err)[:500], req_end, 0, 'PARSE_ERROR'))

            update_batch_resolution_state(
                conn, batch, "PARSE_ERROR", api_request_id, raw_resp_id,
                ingestion_run_id, "PARSE_ERROR", str(parse_err)[:500], 0
            )
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
        calls_attempted, calls_succeeded, calls_attempted,
        run_outcome, 'METADATA_COMPLETE' if run_outcome == 'COMPLETE' else 'METADATA_PARTIAL',
        run_purpose
    ))
    conn.commit()
    conn.close()

    return {
        "ingestion_run_id": ingestion_run_id,
        "run_status": run_outcome,
        "calls_attempted": calls_attempted,
        "calls_succeeded": calls_succeeded,
        "calls_executed": calls_attempted,
        "resolved_count": total_resolved,
        "unavailable_count": total_unavailable
    }
