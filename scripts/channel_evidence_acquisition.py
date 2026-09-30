import os
import uuid
import json
import hashlib
from datetime import datetime, timezone, timedelta
from ingestion_snowflake import get_snowflake_connection
from ingestion_video_metadata import (
    is_quota_or_rate_limit_error,
    log_fact_api_request,
    ingest_video_metadata
)

def persist_raw_youtube_response(
    conn,
    raw_payload: dict,
    endpoint: str,
    resource_scope_type: str,
    resource_scope_id: str,
    request_parameters: dict,
    ingestion_run_id: str,
    api_request_id: str,
    retrieved_at: str,
    http_status: int = 200,
    page_token_used: str = None,
    next_page_token_returned: str = None,
    parser_version: str = "1.0",
    source_schema_version: str = "v3"
) -> str:
    """
    Persists verbatim JSON API response into RAW.YOUTUBE_API_RESPONSE following the
    canonical RAW schema exactly, preserving complete request, lineage, and pagination fields.
    Execution must occur BEFORE parsing downstream (raw-before-parse).
    """
    raw_payload_str = json.dumps(raw_payload)
    payload_hash = hashlib.sha256(raw_payload_str.encode("utf-8")).hexdigest()
    raw_response_id = str(uuid.uuid4())
    req_params_str = json.dumps(request_parameters)

    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO RAW.YOUTUBE_API_RESPONSE (
            raw_response_id, api_request_id, ingestion_run_id, source_system, endpoint,
            resource_scope_type, resource_scope_id, request_parameters, http_status,
            page_token_used, next_page_token_returned, retrieved_at, raw_json_payload,
            payload_hash, parser_version, source_schema_version
        ) SELECT %s, %s, %s, 'YOUTUBE', %s, %s, %s, PARSE_JSON(%s), %s, %s, %s, %s,
                 PARSE_JSON(%s), %s, %s, %s
    ''', (
        raw_response_id, api_request_id, ingestion_run_id, endpoint,
        resource_scope_type, resource_scope_id, req_params_str, http_status,
        page_token_used, next_page_token_returned, retrieved_at, raw_payload_str,
        payload_hash, parser_version, source_schema_version
    ))
    conn.commit()
    return raw_response_id


def acquire_channel_identity_and_uploads(
    youtube_client,
    channel_source_id: str,
    conn=None,
    ingestion_run_id: str = None
) -> tuple:
    """
    Fetches channel snippet and contentDetails via channels.list.
    Returns (uploads_playlist_id, about_description, raw_response_id).
    Persists raw response into RAW.YOUTUBE_API_RESPONSE before parsing.
    """
    default_uploads = "UU" + channel_source_id[2:] if channel_source_id.startswith("UC") else "UU" + channel_source_id
    if not youtube_client:
        return default_uploads, "", None

    api_request_id = str(uuid.uuid4())
    req_at = datetime.now(timezone.utc).isoformat()
    req_params = {"part": "snippet,contentDetails", "id": channel_source_id}
    try:
        req = youtube_client.channels().list(part="snippet,contentDetails", id=channel_source_id)
        resp = req.execute()
        comp_at = datetime.now(timezone.utc).isoformat()

        raw_id = None
        if conn and ingestion_run_id:
            log_fact_api_request(
                conn, api_request_id, ingestion_run_id, "channels.list",
                req_at, comp_at, http_status=200, retry_number=0, error_code=None, estimated_quota_cost=1
            )
            raw_id = persist_raw_youtube_response(
                conn=conn,
                raw_payload=resp,
                endpoint="channels.list",
                resource_scope_type="CHANNEL",
                resource_scope_id=channel_source_id,
                request_parameters=req_params,
                ingestion_run_id=ingestion_run_id,
                api_request_id=api_request_id,
                retrieved_at=comp_at,
                http_status=200
            )

        items = resp.get("items", [])
        if items:
            uploads_id = items[0].get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads", default_uploads)
            about_desc = items[0].get("snippet", {}).get("description", "")
            return uploads_id, about_desc, raw_id
    except Exception as e:
        if conn and ingestion_run_id:
            comp_at = datetime.now(timezone.utc).isoformat()
            log_fact_api_request(
                conn, api_request_id, ingestion_run_id, "channels.list",
                req_at, comp_at, http_status=500, retry_number=0, error_code=str(e)[:128], estimated_quota_cost=1
            )

    return default_uploads, "", None


def get_channel_uploads_playlist_id(youtube_client, channel_source_id: str, conn=None, ingestion_run_id: str = None) -> str:
    """
    Resolves the uploads playlist ID for a channel.
    Standard YouTube convention derives uploads playlist ID by replacing 'UC' prefix with 'UU'.
    If client is provided, queries channels.list acquiring both identity and playlist ID.
    """
    uploads_id, _, _ = acquire_channel_identity_and_uploads(youtube_client, channel_source_id, conn=conn, ingestion_run_id=ingestion_run_id)
    return uploads_id


def persist_raw_playlist_response(conn, raw_payload: dict, playlist_id: str, ingestion_run_id: str, api_request_id: str, page_token_used: str = None, next_page_token_returned: str = None) -> str:
    """
    Persists raw playlistItems.list API responses in RAW.YOUTUBE_API_RESPONSE prior to parsing.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    req_params = {
        "part": "snippet,contentDetails",
        "playlistId": playlist_id,
        "maxResults": 50,
        "pageToken": page_token_used
    }
    return persist_raw_youtube_response(
        conn=conn,
        raw_payload=raw_payload,
        endpoint="playlistItems.list",
        resource_scope_type="PLAYLIST",
        resource_scope_id=playlist_id,
        request_parameters=req_params,
        ingestion_run_id=ingestion_run_id,
        api_request_id=api_request_id,
        retrieved_at=now_iso,
        http_status=200,
        page_token_used=page_token_used,
        next_page_token_returned=next_page_token_returned
    )


def get_or_create_acquisition_state(
    conn, frame_version_key: str, channel_key: str, source_channel_id: str,
    uploads_playlist_id: str, scan_scope: str, window_start_at: datetime,
    window_end_at: datetime, ingestion_run_id: str
):
    """
    Retrieves or initializes the operational state record for evidence acquisition.
    Grain: (frame_version_key, channel_key, window_end_at, scan_scope)
    """
    cursor = conn.cursor()
    w_end_str = window_end_at.isoformat()
    w_start_str = window_start_at.isoformat()
    
    cursor.execute('''
        SELECT acquisition_state_key, status, next_page_token, pages_completed,
               oldest_observed_published_at, items_observed, eligible_videos_observed,
               api_calls_consumed, retry_count
        FROM OPS.CHANNEL_EVIDENCE_ACQUISITION_STATE
        WHERE frame_version_key = %s
          AND channel_key = %s
          AND window_end_at = %s
          AND scan_scope = %s
    ''', (frame_version_key, channel_key, w_end_str, scan_scope))
    row = cursor.fetchone()
    if row:
        return {
            "acquisition_state_key": row[0],
            "status": row[1],
            "next_page_token": row[2],
            "pages_completed": row[3] or 0,
            "oldest_observed_published_at": row[4],
            "items_observed": row[5] or 0,
            "eligible_videos_observed": row[6] or 0,
            "api_calls_consumed": row[7] or 0,
            "retry_count": row[8] or 0,
            "is_new": False
        }
        
    state_key = str(uuid.uuid4())
    now_iso = datetime.now(timezone.utc).isoformat()
    cursor.execute('''
        INSERT INTO OPS.CHANNEL_EVIDENCE_ACQUISITION_STATE (
            acquisition_state_key, frame_version_key, channel_key, source_channel_id,
            uploads_playlist_id, scan_scope, window_start_at, window_end_at, status,
            next_page_token, pages_completed, items_observed, eligible_videos_observed,
            api_calls_consumed, first_ingestion_run_id, latest_ingestion_run_id,
            retry_count, started_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'PENDING', NULL, 0, 0, 0, 0, %s, %s, 0, %s, %s)
    ''', (state_key, frame_version_key, channel_key, source_channel_id, uploads_playlist_id,
          scan_scope, w_start_str, w_end_str, ingestion_run_id, ingestion_run_id, now_iso, now_iso))
    conn.commit()
    
    return {
        "acquisition_state_key": state_key,
        "status": "PENDING",
        "next_page_token": None,
        "pages_completed": 0,
        "oldest_observed_published_at": None,
        "items_observed": 0,
        "eligible_videos_observed": 0,
        "api_calls_consumed": 0,
        "retry_count": 0,
        "is_new": True
    }


def execute_channel_evidence_harvest(
    conn, youtube_client, frame_version_key: str, channel_key: str,
    source_channel_id: str, reference_period_end: datetime,
    scan_scope: str = "90D_PRIMARY", run_purpose: str = "RESEARCH",
    max_pages: int = 50, ingestion_run_id: str = None
):
    """
    Executes resumable, idempotent evidence acquisition for a channel over a 90d or 180d window.
    
    Lifecycle states:
    PENDING -> IN_PROGRESS -> PARTIAL_QUOTA_LIMIT | PARTIAL_ERROR | COMPLETED
    """
    cursor = conn.cursor()
    if not ingestion_run_id:
        ingestion_run_id = str(uuid.uuid4())
        now_iso = datetime.now(timezone.utc).isoformat()
        cursor.execute('''
            INSERT INTO OPS.FACT_INGESTION_RUN (
                ingestion_run_id, source_system, endpoint, resource_scope_type,
                resource_scope_id, started_at, run_purpose, run_outcome, continuity_status
            ) VALUES (%s, 'YOUTUBE', 'playlistItems.list', 'CHANNEL_EVIDENCE', %s, %s, %s, 'IN_PROGRESS', 'RUNNING')
        ''', (ingestion_run_id, channel_key, now_iso, run_purpose))
        conn.commit()

    if scan_scope == "90D_PRIMARY":
        window_days = 90
    elif scan_scope == "180D_FALLBACK":
        window_days = 180
    else:
        raise ValueError(f"Unknown scan_scope: {scan_scope}. Must be '90D_PRIMARY' or '180D_FALLBACK'.")

    window_end_at = reference_period_end
    if window_end_at.tzinfo is None:
        window_end_at = window_end_at.replace(tzinfo=timezone.utc)
    window_start_at = window_end_at - timedelta(days=window_days)

    uploads_playlist_id, about_description, channel_raw_id = acquire_channel_identity_and_uploads(
        youtube_client, source_channel_id, conn=conn, ingestion_run_id=ingestion_run_id
    )

    state = get_or_create_acquisition_state(
        conn, frame_version_key, channel_key, source_channel_id,
        uploads_playlist_id, scan_scope, window_start_at, window_end_at, ingestion_run_id
    )

    if state["status"] == "COMPLETED":
        return {
            "status": "ALREADY_COMPLETED",
            "acquisition_state_key": state["acquisition_state_key"],
            "eligible_videos_observed": state["eligible_videos_observed"],
            "fallback_initialized": False,
            "fallback_state_key": None,
            "about_description": about_description
        }

    # Transition to IN_PROGRESS
    cursor.execute('''
        UPDATE OPS.CHANNEL_EVIDENCE_ACQUISITION_STATE
        SET status = 'IN_PROGRESS',
            latest_ingestion_run_id = %s,
            updated_at = %s
        WHERE acquisition_state_key = %s
    ''', (ingestion_run_id, datetime.now(timezone.utc).isoformat(), state["acquisition_state_key"]))
    conn.commit()

    next_page_token = state["next_page_token"]
    pages_completed = state["pages_completed"]
    items_observed = state["items_observed"]
    api_calls_consumed = state["api_calls_consumed"]
    oldest_observed_dt = state["oldest_observed_published_at"]
    if isinstance(oldest_observed_dt, str):
        oldest_observed_dt = datetime.fromisoformat(oldest_observed_dt.replace("Z", "+00:00"))
    if oldest_observed_dt and oldest_observed_dt.tzinfo is None:
        oldest_observed_dt = oldest_observed_dt.replace(tzinfo=timezone.utc)

    observed_video_ids = []
    final_status = "IN_PROGRESS"
    last_err_code = None
    last_err_msg = None

    for _ in range(max_pages):
        api_request_id = str(uuid.uuid4())
        req_at = datetime.now(timezone.utc).isoformat()
        try:
            req = youtube_client.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=uploads_playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            resp = req.execute()
            comp_at = datetime.now(timezone.utc).isoformat()
            api_calls_consumed += 1

            log_fact_api_request(
                conn, api_request_id, ingestion_run_id, "playlistItems.list",
                req_at, comp_at, http_status=200, retry_number=0, error_code=None, estimated_quota_cost=1
            )
            returned_next_token = resp.get("nextPageToken")
            persist_raw_playlist_response(
                conn=conn,
                raw_payload=resp,
                playlist_id=uploads_playlist_id,
                ingestion_run_id=ingestion_run_id,
                api_request_id=api_request_id,
                page_token_used=next_page_token,
                next_page_token_returned=returned_next_token
            )

            items = resp.get("items", [])
            items_observed += len(items)
            pages_completed += 1

            reached_window_start = False
            for item in items:
                snippet = item.get("snippet", {})
                pub_str = snippet.get("publishedAt")
                v_id = item.get("contentDetails", {}).get("videoId") or snippet.get("resourceId", {}).get("videoId")
                if not v_id or not pub_str:
                    continue

                pub_dt = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                if oldest_observed_dt is None or pub_dt < oldest_observed_dt:
                    oldest_observed_dt = pub_dt

                # We strictly evaluate evidence within [window_start_at, window_end_at)
                if window_start_at <= pub_dt < window_end_at:
                    observed_video_ids.append(v_id)
                elif pub_dt < window_start_at:
                    reached_window_start = True

            next_page_token = returned_next_token

            # Check completion conditions
            if reached_window_start or not next_page_token:
                final_status = "COMPLETED"
                break

        except Exception as err:
            comp_at = datetime.now(timezone.utc).isoformat()
            is_quota = is_quota_or_rate_limit_error(err)
            err_code = "QUOTA_EXCEEDED" if is_quota else str(getattr(err, 'resp', {}).get('status', 'ERROR'))
            err_msg = str(err)[:1024]
            last_err_code = err_code
            last_err_msg = err_msg

            log_fact_api_request(
                conn, api_request_id, ingestion_run_id, "playlistItems.list",
                req_at, comp_at, http_status=429 if is_quota else 500, retry_number=1,
                error_code=err_code, estimated_quota_cost=1
            )

            final_status = "PARTIAL_QUOTA_LIMIT" if is_quota else "PARTIAL_ERROR"
            break

    # Persist and resolve video metadata for newly observed videos
    unique_video_ids = sorted(list(set(observed_video_ids)))
    if unique_video_ids and youtube_client:
        # Ingest video metadata via batched videos.list
        ingest_video_metadata(
            conn, youtube_client, unique_video_ids,
            run_purpose=run_purpose, ingestion_run_id=ingestion_run_id
        )

    # Count eligible videos in window
    cursor.execute('''
        SELECT COUNT(DISTINCT v.video_key)
        FROM CORE.DIM_VIDEO v
        LEFT JOIN OPS.VIDEO_METADATA_RESOLUTION_STATE r
            ON r.source_system = 'YOUTUBE' AND r.source_id = v.source_id
        WHERE v.channel_key = %s
          AND v.published_at >= %s
          AND v.published_at < %s
          AND (v.is_short = FALSE)
          AND (r.resolution_status = 'RESOLVED' OR r.resolution_status IS NULL)
    ''', (channel_key, window_start_at.isoformat(), window_end_at.isoformat()))
    eligible_count = cursor.fetchone()[0]

    # Update state record
    now_iso = datetime.now(timezone.utc).isoformat()
    oldest_str = oldest_observed_dt.isoformat() if oldest_observed_dt else None
    cursor.execute('''
        UPDATE OPS.CHANNEL_EVIDENCE_ACQUISITION_STATE
        SET status = %s,
            next_page_token = %s,
            pages_completed = %s,
            oldest_observed_published_at = %s,
            items_observed = %s,
            eligible_videos_observed = %s,
            api_calls_consumed = %s,
            latest_ingestion_run_id = %s,
            last_error_code = %s,
            last_error_message = %s,
            updated_at = %s,
            completed_at = CASE WHEN %s = 'COMPLETED' THEN %s ELSE NULL END
        WHERE acquisition_state_key = %s
    ''', (
        final_status, next_page_token, pages_completed, oldest_str, items_observed,
        eligible_count, api_calls_consumed, ingestion_run_id, last_err_code, last_err_msg,
        now_iso, final_status, now_iso, state["acquisition_state_key"]
    ))

    # 90D -> 180D Fallback Progression
    # When a 90D_PRIMARY evidence-acquisition unit reaches COMPLETED with fewer than 10 eligible videos,
    # deterministically initialize the corresponding 180D_FALLBACK acquisition state.
    # Incomplete/quota-limited/error 90D units are NOT eligible for fallback progression.
    fallback_initialized = False
    fallback_state_key = None
    if scan_scope == "90D_PRIMARY" and final_status == "COMPLETED" and eligible_count < 10:
        w180_start = window_end_at - timedelta(days=180)
        fallback_state = get_or_create_acquisition_state(
            conn, frame_version_key, channel_key, source_channel_id,
            uploads_playlist_id, "180D_FALLBACK", w180_start, window_end_at, ingestion_run_id
        )
        fallback_initialized = True
        fallback_state_key = fallback_state["acquisition_state_key"]

    # Close fact_ingestion_run
    cursor.execute('''
        UPDATE OPS.FACT_INGESTION_RUN
        SET completed_at = %s,
            pages_requested = %s,
            pages_succeeded = %s,
            records_observed = %s,
            estimated_quota_consumed = %s,
            run_outcome = %s,
            continuity_status = %s
        WHERE ingestion_run_id = %s
    ''', (
        now_iso, pages_completed, pages_completed, items_observed, api_calls_consumed,
        "COMPLETE" if final_status == "COMPLETED" else "PARTIAL",
        final_status, ingestion_run_id
    ))
    conn.commit()

    return {
        "status": final_status,
        "acquisition_state_key": state["acquisition_state_key"],
        "pages_completed": pages_completed,
        "eligible_videos_observed": eligible_count,
        "oldest_observed_published_at": oldest_str,
        "next_page_token": next_page_token,
        "fallback_initialized": fallback_initialized,
        "fallback_state_key": fallback_state_key,
        "about_description": about_description
    }
