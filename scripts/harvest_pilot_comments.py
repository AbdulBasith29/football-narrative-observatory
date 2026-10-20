#!/usr/bin/env python3
"""scripts/harvest_pilot_comments.py
Bounded top-level comment harvesting for the PIPELINE_PILOT frame.

Governing Invariants:
1. Zero Snowflake Writes: All output is local under reports/ (gitignored). Nothing is loaded
   into RAW/CORE by this script.
2. Versioned Manifest: Harvesting reads a collection manifest built from a verified discovery
   report. The manifest records the source report hash, the selected videos and the bounded
   scope; its content hash is re-checked before every run so edited manifests fail closed.
3. Explicit Scope Acceptance: Live harvesting refuses to run unless `--scope-accepted-by` names
   the person who accepted the manifest scope. The acceptance is recorded in the run record.
4. Raw-Before-Parse: Every HTTP response body (success or error) is written verbatim to disk,
   with a sidecar of request/retrieval metadata, before it is parsed.
5. Distinct Outcomes: Collected, empty, comments-disabled, video-not-found, quota-limited,
   partial and other errors are recorded as distinct per-video outcomes. HTTP 403 is
   discriminated by reason (quota vs commentsDisabled vs other).
6. Identity vs Observation Grain: The local store mirrors docs/11-data-warehouse-design.md:
   one comment identity per source comment ID, text versions keyed by text hash, engagement
   snapshots and sample observations keyed by observation. Reruns add new comments and new
   versions without duplicating comment IDs.
7. Author Privacy: Author channel IDs are HMAC-hashed with the existing ingestion salt version;
   parsed output never carries author IDs or display names (raw payloads stay local).
8. Epistemic Scope: Comments are those retrievable at harvest time on historical videos. They are
   not reconstructed 2022 reactions; published_at is retained so later analysis can bound windows.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import hmac
import json
import os
import sys
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from googleapiclient.errors import HttpError
except ImportError:
    HttpError = None  # type: ignore

try:
    from discover_pilot_coverage import OFFICIAL_QUOTA_COSTS, build_youtube_client
except ImportError:
    from scripts.discover_pilot_coverage import OFFICIAL_QUOTA_COSTS, build_youtube_client

# Must match scripts/ingestion_snowflake.py so local hashes join to CORE.DIM_AUTHOR later.
SALT_VERSION_ID = "v1.0"

MANIFEST_SCHEMA_VERSION = "pilot-comment-manifest/1"      # NEWEST_N scope (pilot-comments-v1)
MANIFEST_SCHEMA_VERSION_V2 = "pilot-comment-manifest/2"   # carries an explicit scope mode
SCOPE_NEWEST_N = "NEWEST_N"
SCOPE_EARLY_WINDOW = "EARLY_WINDOW"
RUN_SCHEMA_VERSION = "pilot-comment-run/1"
STORE_SCHEMA_VERSION = "pilot-comment-store/1"

DEFAULT_MAX_COMMENTS_PER_VIDEO = 100

# The API returns some comments with updatedAt exactly 1s before publishedAt (observed in the first
# pilot run, both values from the same item). Gaps up to this tolerance are reported as warnings with
# raw values unchanged; larger gaps still fail verification.
TIMESTAMP_SKEW_TOLERANCE_SECONDS = 1
API_MAX_RESULTS = 100  # commentThreads.list maxResults upper bound

QUOTA_REASONS = {"quotaExceeded", "rateLimitExceeded", "userRateLimitExceeded", "dailyLimitExceeded"}

# Per-video outcomes
OUTCOME_COLLECTED = "COLLECTED"
OUTCOME_EMPTY = "EMPTY"
OUTCOME_COMMENTS_DISABLED = "COMMENTS_DISABLED"
OUTCOME_VIDEO_NOT_FOUND = "VIDEO_NOT_FOUND"
OUTCOME_QUOTA_LIMIT = "QUOTA_LIMIT"
OUTCOME_PARTIAL_QUOTA_LIMIT = "PARTIAL_QUOTA_LIMIT"
OUTCOME_ERROR = "ERROR"
OUTCOME_PARTIAL_ERROR = "PARTIAL_ERROR"
OUTCOME_NOT_ATTEMPTED = "NOT_ATTEMPTED"

# Pagination availability when a video's collection stopped
PAGINATION_EXHAUSTED = "EXHAUSTED"
PAGINATION_MORE_AVAILABLE = "MORE_AVAILABLE_CAP_REACHED"
PAGINATION_REQUEST_CAP = "MORE_AVAILABLE_REQUEST_CAP_REACHED"
PAGINATION_UNKNOWN = "UNKNOWN"

# Early-window coverage: whether the window's comment count is complete or budget-truncated.
# Only COMPLETE_* statuses may be read as the full set of surviving top-level comments in the window.
COVERAGE_COMPLETE_ALL_PAGES = "COMPLETE_ALL_PAGES"
COVERAGE_COMPLETE_WINDOW_PASSED = "COMPLETE_WINDOW_PASSED"
COVERAGE_TRUNCATED_BEFORE_WINDOW = "BUDGET_TRUNCATED_BEFORE_WINDOW"
COVERAGE_TRUNCATED_WITHIN_WINDOW = "BUDGET_TRUNCATED_WITHIN_WINDOW"
COVERAGE_INCOMPLETE_ERROR = "INCOMPLETE_ERROR"
COVERAGE_UNAVAILABLE = "UNAVAILABLE"
COVERAGE_NOT_ATTEMPTED = "NOT_ATTEMPTED"
COMPLETE_COVERAGE = {COVERAGE_COMPLETE_ALL_PAGES, COVERAGE_COMPLETE_WINDOW_PASSED}
RESUMABLE_COVERAGE = {COVERAGE_TRUNCATED_BEFORE_WINDOW, COVERAGE_TRUNCATED_WITHIN_WINDOW}
OUTCOME_SKIPPED_COMPLETE = "SKIPPED_COMPLETE_IN_PRIOR_RUN"

Fetcher = Callable[[Dict[str, Any]], Tuple[Optional[int], bytes]]


def utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_json_hash(obj: Any) -> str:
    return sha256_bytes(json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8"))


def hash_author_id(author_id: str, hmac_secret: str) -> str:
    """Same construction as scripts/ingestion_snowflake.hash_author_id."""
    return hmac.new(hmac_secret.encode("utf-8"), author_id.encode("utf-8"), hashlib.sha256).hexdigest()


def parse_api_timestamp(value: Optional[str]) -> Optional[datetime.datetime]:
    if not value:
        return None
    try:
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


# =============================================================================
# 1. Manifest
# =============================================================================

def build_manifest(
    report: Dict[str, Any],
    source_report_path: str,
    source_report_sha256: str,
    manifest_version: str,
    max_comments_per_video: int = DEFAULT_MAX_COMMENTS_PER_VIDEO,
    relevance_tier: str = "PLAYER_MATCH",
) -> Dict[str, Any]:
    """Selects ELIGIBLE videos of one relevance tier from a discovery report into a manifest.

    Selection is mechanical (status + tier) so no video is handpicked. Exact video IDs are
    deduplicated defensively; the discovery report should already be unique per video_id.
    """
    meta = report.get("report_metadata", {})
    frame = meta.get("pipeline_frame")
    if frame != "PIPELINE_PILOT":
        raise ValueError(f"Source report frame is {frame!r}; comment pilot manifests require PIPELINE_PILOT")
    if meta.get("execution_mode") != "LIVE_YOUTUBE_API":
        raise ValueError(
            f"Source report execution_mode is {meta.get('execution_mode')!r}; "
            "manifests must be built from a live (or live-replayed) report, never synthetic mock data"
        )
    if not 1 <= max_comments_per_video <= 10000:
        raise ValueError("max_comments_per_video must be between 1 and 10000")

    seen = set()
    videos = []
    for cand in report.get("evaluated_candidates", []):
        if cand.get("status") != "ELIGIBLE" or cand.get("relevance_tier") != relevance_tier:
            continue
        vid = cand.get("video_id")
        if not vid or vid in seen:
            continue
        seen.add(vid)
        videos.append({
            "video_id": vid,
            "channel_id": cand.get("channel_id"),
            "channel_title": cand.get("channel_title"),
            "title": cand.get("title"),
            "video_published_at": cand.get("published_at"),
            "player_category": cand.get("player_category") or cand.get("player_relevance") or "UNKNOWN",
            "relevance_tier": cand.get("relevance_tier"),
        })
    videos.sort(key=lambda v: v["video_id"])

    pages_per_video = -(-max_comments_per_video // API_MAX_RESULTS)
    body = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "manifest_version": manifest_version,
        "frame_purpose": "PIPELINE_PILOT",
        "created_at": utc_now_iso(),
        "source_report": {
            "path": source_report_path.replace("\\", "/"),
            "sha256": source_report_sha256,
            "report_generated_at": meta.get("report_generated_at"),
            "window_start": meta.get("window_start"),
            "window_end": meta.get("window_end"),
        },
        "selection_rule": {"status": "ELIGIBLE", "relevance_tier": relevance_tier},
        "scope": {
            "endpoint": "commentThreads.list",
            "part": "snippet",
            "comment_level": "TOP_LEVEL_ONLY",
            "order": "time",
            "text_format": "plainText",
            "max_comments_per_video": max_comments_per_video,
            "interpretation": (
                "Newest top-level comments retrievable at harvest time on historical videos; "
                "not reconstructed reactions at the time of the event."
            ),
        },
        "budget": {
            "videos": len(videos),
            "max_requests_per_video": pages_per_video,
            "max_total_requests": pages_per_video * len(videos),
            "quota_units_per_request": OFFICIAL_QUOTA_COSTS["commentThreads.list"],
            "max_quota_units": pages_per_video * len(videos) * OFFICIAL_QUOTA_COSTS["commentThreads.list"],
            "quota_rate_note": "Verify commentThreads.list cost against current official docs/project allocation.",
        },
        "videos": videos,
    }
    body["manifest_sha256"] = canonical_json_hash(body)
    return body


def _select_eligible(report: Dict[str, Any], relevance_tier: str) -> List[Dict[str, Any]]:
    seen = set()
    selected = []
    for cand in report.get("evaluated_candidates", []):
        if cand.get("status") != "ELIGIBLE" or cand.get("relevance_tier") != relevance_tier:
            continue
        vid = cand.get("video_id")
        if not vid or vid in seen:
            continue
        seen.add(vid)
        selected.append(cand)
    return sorted(selected, key=lambda c: c["video_id"])


def _parse_count(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def estimate_early_window_cost(videos: List[Dict[str, Any]], max_requests_per_video: int) -> Dict[str, Any]:
    """Estimates commentThreads.list requests for early-window retrieval from discovery-time counts.

    commentThreads.list returns newest first, so reaching a video's earliest comments means paging
    through every newer one: full retrieval costs about ceil(top-level comments / 100) requests.
    commentCount includes replies, so ceil(commentCount / 100) is an upper bound. Videos without a
    count (hidden or disabled) are assumed to use the full cap.
    """
    per_video = []
    for v in videos:
        count = _parse_count(v.get("comment_count_at_discovery"))
        full = max(1, -(-count // API_MAX_RESULTS)) if count is not None else None
        planned = min(full, max_requests_per_video) if full is not None else max_requests_per_video
        per_video.append({
            "video_id": v["video_id"],
            "comment_count_at_discovery": count,
            "requests_for_full_retrieval_upper_bound": full,
            "planned_max_requests": planned,
            "expected_budget_truncation": full is None or full > max_requests_per_video,
        })
    known = [p for p in per_video if p["requests_for_full_retrieval_upper_bound"] is not None]
    return {
        "max_requests_per_video": max_requests_per_video,
        "planned_max_requests": sum(p["planned_max_requests"] for p in per_video),
        "full_retrieval_requests_upper_bound_known_counts": sum(p["requests_for_full_retrieval_upper_bound"] for p in known),
        "videos_with_unknown_comment_count": len(per_video) - len(known),
        "videos_expected_budget_truncated": sum(1 for p in per_video if p["expected_budget_truncation"]),
        "quota_units_per_request": OFFICIAL_QUOTA_COSTS["commentThreads.list"],
        "basis": "statistics.commentCount at discovery time (includes replies; upper bound on top-level pages)",
        "per_video": per_video,
    }


def build_early_window_manifest(
    report: Dict[str, Any],
    source_report_path: str,
    source_report_sha256: str,
    manifest_version: str,
    window_hours: int,
    max_requests_per_video: int,
    relevance_tier: str = "PLAYER_MATCH",
) -> Dict[str, Any]:
    """Builds an EARLY_WINDOW manifest: for each video, top-level comments published within
    window_hours of that video's own publication timestamp, under an explicit request budget."""
    meta = report.get("report_metadata", {})
    if meta.get("pipeline_frame") != "PIPELINE_PILOT":
        raise ValueError(f"Source report frame is {meta.get('pipeline_frame')!r}; pilot manifests require PIPELINE_PILOT")
    if meta.get("execution_mode") != "LIVE_YOUTUBE_API":
        raise ValueError(f"Source report execution_mode is {meta.get('execution_mode')!r}; never build from synthetic data")
    if not isinstance(window_hours, int) or not 1 <= window_hours <= 24 * 365:
        raise ValueError("window_hours must be a positive integer number of hours")
    if not isinstance(max_requests_per_video, int) or not 1 <= max_requests_per_video <= 10000:
        raise ValueError("max_requests_per_video must be between 1 and 10000")

    videos = []
    for cand in _select_eligible(report, relevance_tier):
        if parse_api_timestamp(cand.get("published_at")) is None:
            raise ValueError(f"Video {cand['video_id']} has no parseable published_at; the window cannot be anchored")
        videos.append({
            "video_id": cand["video_id"],
            "channel_id": cand.get("channel_id"),
            "channel_title": cand.get("channel_title"),
            "title": cand.get("title"),
            "video_published_at": cand.get("published_at"),
            "player_category": cand.get("player_category") or cand.get("player_relevance") or "UNKNOWN",
            "relevance_tier": cand.get("relevance_tier"),
            "comment_count_at_discovery": _parse_count(cand.get("comment_count")),
        })

    body = {
        "schema_version": MANIFEST_SCHEMA_VERSION_V2,
        "manifest_version": manifest_version,
        "frame_purpose": "PIPELINE_PILOT",
        "event_id": meta.get("event_id"),
        "created_at": utc_now_iso(),
        "source_report": {
            "path": source_report_path.replace("\\", "/"),
            "sha256": source_report_sha256,
            "report_generated_at": meta.get("report_generated_at"),
            "window_start": meta.get("window_start"),
            "window_end": meta.get("window_end"),
        },
        "selection_rule": {"status": "ELIGIBLE", "relevance_tier": relevance_tier},
        "scope": {
            "mode": SCOPE_EARLY_WINDOW,
            "endpoint": "commentThreads.list",
            "part": "snippet",
            "comment_level": "TOP_LEVEL_ONLY",
            "order": "time",
            "text_format": "plainText",
            "window_anchor": "VIDEO_PUBLISHED_AT",
            "window_hours": window_hours,
            "max_requests_per_video": max_requests_per_video,
            "interpretation": (
                "Top-level comments still retrievable at harvest time that were published within "
                f"{window_hours}h of each video's publication. Counts are complete only for videos whose "
                "coverage is COMPLETE_*; budget-truncated videos are partial and are not total discussion volume."
            ),
        },
        "budget": {
            "videos": len(videos),
            "max_requests_per_video": max_requests_per_video,
            "max_total_requests": max_requests_per_video * len(videos),
            "quota_units_per_request": OFFICIAL_QUOTA_COSTS["commentThreads.list"],
            "max_quota_units": max_requests_per_video * len(videos) * OFFICIAL_QUOTA_COSTS["commentThreads.list"],
            "quota_rate_note": "Verify commentThreads.list cost against current official docs/project allocation.",
        },
        "cost_estimate": estimate_early_window_cost(videos, max_requests_per_video),
        "videos": videos,
    }
    body["manifest_sha256"] = canonical_json_hash(body)
    return body


def manifest_scope_mode(manifest: Dict[str, Any]) -> str:
    return manifest.get("scope", {}).get("mode", SCOPE_NEWEST_N)


def verify_manifest_integrity(manifest: Dict[str, Any]) -> None:
    if manifest.get("schema_version") not in (MANIFEST_SCHEMA_VERSION, MANIFEST_SCHEMA_VERSION_V2):
        raise ValueError(f"Unsupported manifest schema_version {manifest.get('schema_version')!r}")
    mode = manifest_scope_mode(manifest)
    if (manifest["schema_version"] == MANIFEST_SCHEMA_VERSION) != (mode == SCOPE_NEWEST_N) or \
            mode not in (SCOPE_NEWEST_N, SCOPE_EARLY_WINDOW):
        raise ValueError(f"Manifest schema_version and scope mode {mode!r} do not match")
    if manifest.get("frame_purpose") != "PIPELINE_PILOT":
        raise ValueError("Manifest frame_purpose must be PIPELINE_PILOT")
    recorded = manifest.get("manifest_sha256")
    body = {k: v for k, v in manifest.items() if k != "manifest_sha256"}
    if not recorded or canonical_json_hash(body) != recorded:
        raise ValueError("Manifest content hash mismatch: the manifest was edited after generation. Rebuild it.")
    budget = manifest.get("budget", {})
    per_video = budget.get("max_requests_per_video")
    if mode == SCOPE_EARLY_WINDOW:
        expected_per_video = manifest["scope"].get("max_requests_per_video")
        if not isinstance(manifest["scope"].get("window_hours"), int) or manifest["scope"]["window_hours"] < 1:
            raise ValueError("Early-window manifest needs a positive integer window_hours")
    else:
        expected_per_video = -(-manifest["scope"]["max_comments_per_video"] // API_MAX_RESULTS)
    if (not isinstance(per_video, int) or per_video < 1 or per_video != expected_per_video
            or budget.get("max_total_requests") != per_video * len(manifest.get("videos", []))):
        raise ValueError("Manifest request budget is inconsistent with its scope and video count.")


# =============================================================================
# 2. Response classification & parsing
# =============================================================================

def extract_error_reasons(body: bytes) -> List[str]:
    try:
        payload = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return []
    err = payload.get("error", {}) if isinstance(payload, dict) else {}
    reasons = [e.get("reason") for e in err.get("errors", []) if isinstance(e, dict) and e.get("reason")]
    for detail in err.get("details", []) or []:
        if isinstance(detail, dict) and detail.get("reason"):
            reasons.append(detail["reason"])
    return reasons


def classify_error_response(http_status: Optional[int], body: bytes, pages_succeeded: int) -> Tuple[str, str]:
    """Maps an error response to (outcome, error_code)."""
    reasons = extract_error_reasons(body)
    if http_status == 403 and "commentsDisabled" in reasons:
        return OUTCOME_COMMENTS_DISABLED, "COMMENTS_DISABLED"
    if http_status == 404 and ("videoNotFound" in reasons or not reasons):
        return OUTCOME_VIDEO_NOT_FOUND, "VIDEO_NOT_FOUND"
    if http_status in (403, 429) and QUOTA_REASONS.intersection(reasons):
        code = sorted(QUOTA_REASONS.intersection(reasons))[0]
        return (OUTCOME_PARTIAL_QUOTA_LIMIT if pages_succeeded else OUTCOME_QUOTA_LIMIT), code
    code = f"HTTP_{http_status}" if http_status is not None else "TRANSPORT_ERROR"
    return (OUTCOME_PARTIAL_ERROR if pages_succeeded else OUTCOME_ERROR), code


def parse_comment_thread(item: Dict[str, Any], hmac_secret: str) -> Dict[str, Any]:
    thread_snippet = item.get("snippet", {})
    top = thread_snippet.get("topLevelComment", {})
    snippet = top.get("snippet", {})
    author_channel_id = (snippet.get("authorChannelId") or {}).get("value")
    text = snippet.get("textOriginal")
    if text is None:
        text = snippet.get("textDisplay", "")
    return {
        "comment_id": top.get("id") or item.get("id"),
        "comment_thread_id": item.get("id"),
        "video_id": snippet.get("videoId") or thread_snippet.get("videoId"),
        "published_at": snippet.get("publishedAt"),
        "updated_at": snippet.get("updatedAt"),
        "text": text,
        "text_hash": sha256_bytes(text.encode("utf-8")),
        "like_count": snippet.get("likeCount"),
        "total_reply_count": thread_snippet.get("totalReplyCount"),
        "author_hash": hash_author_id(author_channel_id, hmac_secret) if author_channel_id else None,
        "salt_version_id": SALT_VERSION_ID if author_channel_id else None,
    }


# =============================================================================
# 3. Live fetcher (raw bytes)
# =============================================================================

def make_live_fetcher(youtube) -> Fetcher:
    """Returns a fetcher yielding (http_status, verbatim response bytes) for commentThreads.list."""

    def fetch(params: Dict[str, Any]) -> Tuple[Optional[int], bytes]:
        request = youtube.commentThreads().list(**params)
        # Keep the verbatim body instead of googleapiclient's parsed dict.
        request.postproc = lambda resp, content: (int(resp.status), content)
        try:
            status, content = request.execute()
        except Exception as exc:  # HttpError carries resp/content; anything else is transport-level
            if HttpError is not None and isinstance(exc, HttpError):
                content = exc.content if isinstance(exc.content, bytes) else str(exc.content).encode("utf-8")
                return int(exc.resp.status), content
            return None, json.dumps({"transport_error": str(exc)[:500]}).encode("utf-8")
        if isinstance(content, str):
            content = content.encode("utf-8")
        return status, content

    return fetch


# =============================================================================
# 4. Harvest
# =============================================================================

def _write_bytes(path: str, data: bytes) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)


def _write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


def request_page(
    fetcher: Fetcher,
    params: Dict[str, Any],
    run_dir: str,
    run_id: str,
    video_id: str,
    page_number: int,
    pages_succeeded: int,
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], Optional[str], Optional[str]]:
    """Issues one commentThreads.list request, writing the verbatim body and a metadata sidecar before
    parsing. Returns (page_record, payload, error_outcome, error_code); payload is None on error."""
    requested_at = utc_now_iso()
    status, body = fetcher(params)
    retrieved_at = utc_now_iso()

    raw_rel = f"raw/{video_id}/page_{page_number:03d}.json"
    meta_path = os.path.join(run_dir, f"raw/{video_id}/page_{page_number:03d}.meta.json")
    _write_bytes(os.path.join(run_dir, raw_rel), body)
    page_record = {
        "run_id": run_id,
        "video_id": video_id,
        "page_number": page_number,
        "endpoint": "commentThreads.list",
        "request_parameters": params,
        "requested_at": requested_at,
        "retrieved_at": retrieved_at,
        "http_status": status,
        "raw_file": raw_rel,
        "payload_sha256": sha256_bytes(body),
        "payload_bytes": len(body),
    }

    if status != 200:
        outcome, error_code = classify_error_response(status, body, pages_succeeded)
        page_record["error_code"] = error_code
        page_record["error_reasons"] = extract_error_reasons(body)
        _write_json(meta_path, page_record)
        return page_record, None, outcome, error_code

    try:
        payload = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        page_record["error_code"] = "UNPARSEABLE_RESPONSE"
        _write_json(meta_path, page_record)
        return page_record, None, (OUTCOME_PARTIAL_ERROR if pages_succeeded else OUTCOME_ERROR), "UNPARSEABLE_RESPONSE"

    page_record["item_count"] = len(payload.get("items", []) or [])
    page_record["next_page_token_returned"] = payload.get("nextPageToken")
    _write_json(meta_path, page_record)
    return page_record, payload, None, None


def harvest_video(
    video: Dict[str, Any],
    fetcher: Fetcher,
    run_dir: str,
    run_id: str,
    max_comments: int,
    hmac_secret: str,
    max_requests: int,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Collects up to max_comments newest top-level comments for one video, issuing at most max_requests
    requests. The request cap is enforced independently of the comment count, so short pages cannot
    push a video past its budget.

    Returns (video_outcome, page_records, parsed_observations). Raw bytes are written before parsing.
    """
    video_id = video["video_id"]
    pages: List[Dict[str, Any]] = []
    observations: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    collected = 0
    outcome: Optional[str] = None
    error_code: Optional[str] = None
    pagination = PAGINATION_UNKNOWN
    pages_succeeded = 0
    if max_requests < 1:
        raise ValueError("max_requests must be at least 1")

    while True:
        page_number = len(pages) + 1
        params: Dict[str, Any] = {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": min(API_MAX_RESULTS, max_comments - collected),
            "order": "time",
            "textFormat": "plainText",
        }
        if page_token:
            params["pageToken"] = page_token

        page_record, payload, err_outcome, err_code = request_page(
            fetcher, params, run_dir, run_id, video_id, page_number, pages_succeeded)
        pages.append(page_record)
        if payload is None:
            outcome, error_code = err_outcome, err_code
            break
        raw_rel = page_record["raw_file"]
        retrieved_at = page_record["retrieved_at"]
        items = payload.get("items", []) or []
        next_token = payload.get("nextPageToken")
        pages_succeeded += 1

        for position, item in enumerate(items):
            if collected >= max_comments:
                break
            parsed = parse_comment_thread(item, hmac_secret)
            parsed.update({
                "manifest_video_id": video_id,
                "run_id": run_id,
                "raw_file": raw_rel,
                "payload_sha256": page_record["payload_sha256"],
                "page_number": page_number,
                "source_item_position": position,
                "observed_at": retrieved_at,
            })
            observations.append(parsed)
            collected += 1

        if not next_token:
            pagination = PAGINATION_EXHAUSTED
            break
        if collected >= max_comments:
            pagination = PAGINATION_MORE_AVAILABLE
            break
        if not items:
            # A token with an empty page: stop rather than loop; availability is unknown.
            pagination = PAGINATION_UNKNOWN
            break
        if len(pages) >= max_requests:
            pagination = PAGINATION_REQUEST_CAP
            break
        page_token = next_token

    if outcome is None:
        outcome = OUTCOME_COLLECTED if collected else OUTCOME_EMPTY

    video_outcome = {
        "video_id": video_id,
        "outcome": outcome,
        "error_code": error_code,
        "pagination_availability": pagination if outcome in (OUTCOME_COLLECTED, OUTCOME_EMPTY) else PAGINATION_UNKNOWN,
        "pages_requested": len(pages),
        "pages_succeeded": pages_succeeded,
        "comments_collected": collected,
    }
    return video_outcome, pages, observations


def early_window_bounds(video: Dict[str, Any], window_hours: int) -> Tuple[datetime.datetime, datetime.datetime]:
    start = parse_api_timestamp(video.get("video_published_at"))
    if start is None:
        raise ValueError(f"Video {video.get('video_id')} has no parseable video_published_at")
    return start, start + datetime.timedelta(hours=window_hours)


def _item_published_at(item: Dict[str, Any]) -> Optional[str]:
    return item.get("snippet", {}).get("topLevelComment", {}).get("snippet", {}).get("publishedAt")


def in_early_window(published_at: Optional[str], start: datetime.datetime, end: datetime.datetime) -> bool:
    ts = parse_api_timestamp(published_at)
    return ts is not None and start <= ts < end


def harvest_video_early_window(
    video: Dict[str, Any],
    fetcher: Fetcher,
    run_dir: str,
    run_id: str,
    hmac_secret: str,
    window_hours: int,
    max_requests: int,
    resume: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Pages a video's top-level comments (newest first) until its early window is fully covered, the
    pages run out, or the request budget is spent.

    Every retrieved comment is kept and flagged in_early_window; raw pages are written before parsing,
    and a checkpoint (next page token + frontier) is written after every successful page so a
    budget-truncated video can be resumed. `resume` is a prior run's checkpoint for this video.
    """
    if max_requests < 1:
        raise ValueError("max_requests must be at least 1")
    video_id = video["video_id"]
    window_start, window_end = early_window_bounds(video, window_hours)
    checkpoint_path = os.path.join(run_dir, "checkpoints", f"{video_id}.json")

    pages: List[Dict[str, Any]] = []
    observations: List[Dict[str, Any]] = []
    page_token = resume.get("next_page_token") if resume else None
    frontier = parse_api_timestamp(resume.get("frontier_published_at")) if resume else None
    pages_succeeded = 0
    outcome: Optional[str] = None
    error_code: Optional[str] = None
    coverage: Optional[str] = None
    next_token: Optional[str] = page_token

    def write_checkpoint(status: str) -> Dict[str, Any]:
        cp = {
            "video_id": video_id,
            "run_id": run_id,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "pages_succeeded_this_run": pages_succeeded,
            "last_page_number": len(pages),
            "next_page_token": next_token,
            "frontier_published_at": frontier.isoformat() if frontier else None,
            "coverage_status": status,
            "resumed_from_run_id": resume.get("run_id") if resume else None,
            "updated_at": utc_now_iso(),
        }
        _write_json(checkpoint_path, cp)
        return cp

    while True:
        page_number = len(pages) + 1
        params: Dict[str, Any] = {"part": "snippet", "videoId": video_id, "maxResults": API_MAX_RESULTS,
                                  "order": "time", "textFormat": "plainText"}
        if page_token:
            params["pageToken"] = page_token
        page_record, payload, err_outcome, err_code = request_page(
            fetcher, params, run_dir, run_id, video_id, page_number, pages_succeeded)
        pages.append(page_record)
        if payload is None:
            outcome, error_code = err_outcome, err_code
            break
        pages_succeeded += 1
        items = payload.get("items", []) or []
        next_token = payload.get("nextPageToken")

        # A first-page item older than the one after it is likely pinned; it must not move the frontier.
        pinned_position = None
        if page_number == 1 and not resume and len(items) >= 2:
            first, second = (parse_api_timestamp(_item_published_at(items[i])) for i in (0, 1))
            if first and second and first < second:
                pinned_position = 0

        for position, item in enumerate(items):
            parsed = parse_comment_thread(item, hmac_secret)
            parsed.update({
                "manifest_video_id": video_id,
                "run_id": run_id,
                "raw_file": page_record["raw_file"],
                "payload_sha256": page_record["payload_sha256"],
                "page_number": page_number,
                "source_item_position": position,
                "observed_at": page_record["retrieved_at"],
                "in_early_window": in_early_window(parsed["published_at"], window_start, window_end),
                "likely_pinned": position == pinned_position,
            })
            observations.append(parsed)
            ts = parse_api_timestamp(parsed["published_at"])
            if ts is not None and position != pinned_position and (frontier is None or ts < frontier):
                frontier = ts

        if not next_token:
            coverage = COVERAGE_COMPLETE_ALL_PAGES
            break
        if frontier is not None and frontier < window_start:
            coverage = COVERAGE_COMPLETE_WINDOW_PASSED
            break
        if not items:
            outcome, error_code = (OUTCOME_PARTIAL_ERROR if pages_succeeded > 1 else OUTCOME_ERROR), "EMPTY_PAGE_WITH_TOKEN"
            break
        write_checkpoint("IN_PROGRESS")
        if len(pages) >= max_requests:
            coverage = (COVERAGE_TRUNCATED_WITHIN_WINDOW if frontier is not None and frontier < window_end
                        else COVERAGE_TRUNCATED_BEFORE_WINDOW)
            break
        page_token = next_token

    if outcome in (OUTCOME_COMMENTS_DISABLED, OUTCOME_VIDEO_NOT_FOUND):
        coverage = COVERAGE_UNAVAILABLE
    elif outcome is not None:
        coverage = COVERAGE_INCOMPLETE_ERROR
    else:
        outcome = OUTCOME_COLLECTED if observations or resume else OUTCOME_EMPTY
    checkpoint = write_checkpoint(coverage)

    in_window = sum(1 for o in observations if o["in_early_window"])
    video_outcome = {
        "video_id": video_id,
        "outcome": outcome,
        "error_code": error_code,
        "coverage_status": coverage,
        "window_count_is_complete": coverage in COMPLETE_COVERAGE,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "frontier_published_at": checkpoint["frontier_published_at"],
        "window_covered_from": (max(frontier, window_start).isoformat()
                                if coverage == COVERAGE_TRUNCATED_WITHIN_WINDOW and frontier else None),
        "next_page_token_saved": bool(checkpoint["next_page_token"]) and coverage in RESUMABLE_COVERAGE,
        "resumed_from_run_id": checkpoint["resumed_from_run_id"],
        "pages_requested": len(pages),
        "pages_succeeded": pages_succeeded,
        "comments_collected": len(observations),
        "in_window_comments_observed": in_window,
        "out_of_window_comments_observed": len(observations) - in_window,
        "pagination_availability": PAGINATION_EXHAUSTED if coverage == COVERAGE_COMPLETE_ALL_PAGES else (
            PAGINATION_REQUEST_CAP if coverage in RESUMABLE_COVERAGE else PAGINATION_UNKNOWN),
    }
    return video_outcome, pages, observations


def merge_into_store(store: Dict[str, Any], observations: List[Dict[str, Any]], run_id: str) -> Dict[str, int]:
    """Merges parsed observations into the local store at warehouse-aligned grains.

    - comments: one identity per comment_id (never duplicated across reruns)
    - text_versions: one per (comment_id, text_hash)
    - engagement_snapshots / sample_observations: one per observation
    """
    comments = store.setdefault("comments", {})
    stats = {"new_comments": 0, "existing_comments": 0, "new_text_versions": 0, "text_reverts": 0,
             "updated_at_changes": 0, "duplicate_ids_within_run": 0}
    seen_in_run = set()

    for obs in observations:
        cid = obs["comment_id"]
        repeat_in_run = cid in seen_in_run
        if repeat_in_run:
            stats["duplicate_ids_within_run"] += 1
        seen_in_run.add(cid)

        entry = comments.get(cid)
        if entry is None:
            entry = {
                "comment_id": cid,
                "comment_thread_id": obs["comment_thread_id"],
                "video_id": obs["video_id"],
                "manifest_video_id": obs["manifest_video_id"],
                "published_at": obs["published_at"],
                "author_hash": obs["author_hash"],
                "salt_version_id": obs["salt_version_id"],
                "is_reply": False,
                "in_early_window": obs.get("in_early_window"),
                "likely_pinned": bool(obs.get("likely_pinned")),
                "first_observed_at": obs["observed_at"],
                "first_observed_run_id": run_id,
                "text_versions": [],
                "engagement_snapshots": [],
                "sample_observations": [],
            }
            comments[cid] = entry
            stats["new_comments"] += 1
        elif not repeat_in_run and entry["first_observed_run_id"] != run_id:
            stats["existing_comments"] += 1

        # One version per distinct text hash; the latest flag follows the most recent observation, so a
        # revert (A -> B -> A) re-flags the existing A version instead of leaving B marked latest.
        version = next((v for v in entry["text_versions"] if v["text_hash"] == obs["text_hash"]), None)
        if version is None:
            version = {
                "text_hash": obs["text_hash"],
                "text": obs["text"],
                "updated_at": obs["updated_at"],
                "observed_at": obs["observed_at"],
                "run_id": run_id,
                "is_latest_version": False,
            }
            entry["text_versions"].append(version)
            stats["new_text_versions"] += 1
        elif not version["is_latest_version"]:
            stats["text_reverts"] += 1
        for v in entry["text_versions"]:
            v["is_latest_version"] = v is version
        version["last_observed_at"] = obs["observed_at"]
        version["last_observed_run_id"] = run_id
        entry["latest_text_hash"] = obs["text_hash"]

        last_updated = entry.get("latest_updated_at")
        if last_updated is not None and obs["updated_at"] != last_updated:
            stats["updated_at_changes"] += 1
        entry["latest_updated_at"] = obs["updated_at"]
        entry["last_observed_at"] = obs["observed_at"]
        entry["last_observed_run_id"] = run_id

        entry["engagement_snapshots"].append({
            "observed_at": obs["observed_at"],
            "run_id": run_id,
            "like_count": obs["like_count"],
            "total_reply_count": obs["total_reply_count"],
        })
        entry["sample_observations"].append({
            "run_id": run_id,
            "raw_file": obs["raw_file"],
            "payload_sha256": obs["payload_sha256"],
            "page_number": obs["page_number"],
            "source_item_position": obs["source_item_position"],
            "observed_at": obs["observed_at"],
            "text_hash": obs["text_hash"],
        })

    return stats


def load_store(store_path: str, must_exist: bool = False) -> Dict[str, Any]:
    if must_exist and not os.path.exists(store_path):
        raise FileNotFoundError(f"Comment store not found: {store_path}")
    if os.path.exists(store_path):
        with open(store_path, "r", encoding="utf-8") as f:
            store = json.load(f)
        if store.get("schema_version") != STORE_SCHEMA_VERSION:
            raise ValueError(f"Unsupported store schema_version {store.get('schema_version')!r}")
        return store
    return {"schema_version": STORE_SCHEMA_VERSION, "manifest_sha256": None, "runs": [], "comments": {}}


def run_harvest(
    manifest: Dict[str, Any],
    fetcher: Fetcher,
    output_root: str,
    hmac_secret: str,
    scope_accepted_by: str,
    run_id: Optional[str] = None,
    video_limit: Optional[int] = None,
    resume_from_run: Optional[str] = None,
) -> Dict[str, Any]:
    """Executes one bounded harvest run over the manifest. No Snowflake access.

    For EARLY_WINDOW manifests, `resume_from_run` continues budget-truncated videos from that run's
    saved page tokens and skips videos it already covered completely.
    """
    verify_manifest_integrity(manifest)
    mode = manifest_scope_mode(manifest)
    if resume_from_run and mode != SCOPE_EARLY_WINDOW:
        raise ValueError("--resume-from-run is only supported for EARLY_WINDOW manifests")
    if not hmac_secret:
        raise RuntimeError("HMAC_SECRET is required so author IDs are hashed before anything is parsed.")
    if not scope_accepted_by or not scope_accepted_by.strip():
        raise RuntimeError("Scope acceptance is required: pass --scope-accepted-by with the accepting person's name.")

    run_id = run_id or (datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8])
    run_dir = os.path.join(output_root, "runs", run_id)
    if os.path.exists(run_dir):
        raise FileExistsError(f"Run directory already exists: {run_dir}")
    os.makedirs(run_dir)

    store_path = os.path.join(output_root, "comment_store.json")
    store = load_store(store_path)
    if store.get("manifest_sha256") not in (None, manifest["manifest_sha256"]):
        raise ValueError(
            "The local comment store was built from a different manifest. Use a separate --output-root "
            "per manifest version so lineage is not mixed."
        )
    store["manifest_sha256"] = manifest["manifest_sha256"]

    prior_checkpoints: Dict[str, Dict[str, Any]] = {}
    if resume_from_run:
        prior_dir = os.path.join(output_root, "runs", resume_from_run)
        if resume_from_run not in [r.get("run_id") for r in store.get("runs", [])] or \
                not os.path.exists(os.path.join(prior_dir, "run.json")):
            raise ValueError(f"Cannot resume: run {resume_from_run!r} is not recorded in this output root")
        with open(os.path.join(prior_dir, "run.json"), "r", encoding="utf-8") as f:
            if json.load(f).get("manifest_sha256") != manifest["manifest_sha256"]:
                raise ValueError("Cannot resume: the prior run used a different manifest")
        cp_dir = os.path.join(prior_dir, "checkpoints")
        for name in (os.listdir(cp_dir) if os.path.isdir(cp_dir) else []):
            with open(os.path.join(cp_dir, name), "r", encoding="utf-8") as f:
                cp = json.load(f)
            prior_checkpoints[cp["video_id"]] = cp

    max_comments = manifest["scope"].get("max_comments_per_video")
    window_hours = manifest["scope"].get("window_hours")
    max_requests_per_video = manifest["budget"]["max_requests_per_video"]
    max_total_requests = manifest["budget"]["max_total_requests"]
    videos = manifest["videos"][:video_limit] if video_limit else manifest["videos"]
    started_at = utc_now_iso()

    video_outcomes: List[Dict[str, Any]] = []
    all_pages: List[Dict[str, Any]] = []
    all_obs: List[Dict[str, Any]] = []
    quota_stop = False
    budget_stop = False
    for video in videos:
        remaining_requests = max_total_requests - len(all_pages)
        if remaining_requests < 1:
            budget_stop = True
        prior = prior_checkpoints.get(video["video_id"])
        if mode == SCOPE_EARLY_WINDOW and prior and prior.get("coverage_status") in COMPLETE_COVERAGE:
            video_outcomes.append({
                "video_id": video["video_id"], "outcome": OUTCOME_SKIPPED_COMPLETE, "error_code": None,
                "coverage_status": prior["coverage_status"], "window_count_is_complete": True,
                "skipped_because_run_id": prior.get("run_id"), "pagination_availability": PAGINATION_UNKNOWN,
                "pages_requested": 0, "pages_succeeded": 0, "comments_collected": 0,
            })
            continue
        if quota_stop or budget_stop:
            not_attempted = {
                "video_id": video["video_id"], "outcome": OUTCOME_NOT_ATTEMPTED,
                "error_code": "STOPPED_AFTER_QUOTA_LIMIT" if quota_stop else "RUN_REQUEST_BUDGET_EXHAUSTED",
                "pagination_availability": PAGINATION_UNKNOWN,
                "pages_requested": 0, "pages_succeeded": 0, "comments_collected": 0,
            }
            if mode == SCOPE_EARLY_WINDOW:
                not_attempted.update({"coverage_status": COVERAGE_NOT_ATTEMPTED, "window_count_is_complete": False})
            video_outcomes.append(not_attempted)
            continue
        request_cap = min(max_requests_per_video, remaining_requests)
        if mode == SCOPE_EARLY_WINDOW:
            resumable = prior and prior.get("coverage_status") in RESUMABLE_COVERAGE and prior.get("next_page_token")
            resume = prior if resumable else None
            outcome, pages, obs = harvest_video_early_window(video, fetcher, run_dir, run_id, hmac_secret,
                                                             window_hours, request_cap, resume)
            store.setdefault("video_coverage", {})[video["video_id"]] = {
                k: outcome[k] for k in ("coverage_status", "window_count_is_complete", "window_start", "window_end",
                                        "frontier_published_at", "window_covered_from", "resumed_from_run_id")}
            store["video_coverage"][video["video_id"]]["run_id"] = run_id
        else:
            outcome, pages, obs = harvest_video(video, fetcher, run_dir, run_id, max_comments, hmac_secret, request_cap)
        video_outcomes.append(outcome)
        all_pages.extend(pages)
        all_obs.extend(obs)
        if outcome["outcome"] in (OUTCOME_QUOTA_LIMIT, OUTCOME_PARTIAL_QUOTA_LIMIT):
            quota_stop = True

    # Parsed observations for this run (lineage-bearing, no author IDs/display names).
    with open(os.path.join(run_dir, "parsed_comments.jsonl"), "w", encoding="utf-8") as f:
        for obs in all_obs:
            f.write(json.dumps(obs, ensure_ascii=False) + "\n")

    merge_stats = merge_into_store(store, all_obs, run_id)

    outcome_counts: Dict[str, int] = {}
    for vo in video_outcomes:
        outcome_counts[vo["outcome"]] = outcome_counts.get(vo["outcome"], 0) + 1

    requests = len(all_pages)
    coverage_counts: Dict[str, int] = {}
    for vo in video_outcomes:
        if "coverage_status" in vo:
            coverage_counts[vo["coverage_status"]] = coverage_counts.get(vo["coverage_status"], 0) + 1
    ok_outcomes = {OUTCOME_COLLECTED, OUTCOME_EMPTY, OUTCOME_COMMENTS_DISABLED, OUTCOME_VIDEO_NOT_FOUND,
                   OUTCOME_SKIPPED_COMPLETE}
    if quota_stop:
        run_status = "STOPPED_QUOTA_LIMIT"
    elif budget_stop:
        run_status = "STOPPED_REQUEST_BUDGET"
    elif not all(vo["outcome"] in ok_outcomes for vo in video_outcomes):
        run_status = "COMPLETED_WITH_ERRORS"
    elif any(vo.get("coverage_status") in RESUMABLE_COVERAGE for vo in video_outcomes):
        run_status = "COMPLETED_BUDGET_TRUNCATED"
    else:
        run_status = "COMPLETED"
    run_record = {
        "schema_version": RUN_SCHEMA_VERSION,
        "run_id": run_id,
        "run_purpose": "PIPELINE_PILOT",
        "snowflake_writes": False,
        "manifest_version": manifest["manifest_version"],
        "manifest_sha256": manifest["manifest_sha256"],
        "scope": manifest["scope"],
        "scope_accepted_by": scope_accepted_by.strip(),
        "started_at": started_at,
        "completed_at": utc_now_iso(),
        "scope_mode": mode,
        "resumed_from_run_id": resume_from_run,
        "run_status": run_status,
        "videos_in_manifest": len(manifest["videos"]),
        "videos_in_run": len(videos),
        "telemetry": {
            "requests_by_endpoint": {"commentThreads.list": requests},
            "quota_units_consumed_estimate": requests * OFFICIAL_QUOTA_COSTS["commentThreads.list"],
            "budget_max_quota_units": manifest["budget"]["max_quota_units"],
            "budget_max_total_requests": max_total_requests,
            "budget_max_requests_per_video": max_requests_per_video,
        },
        "outcome_counts": outcome_counts,
        "coverage_counts": coverage_counts,
        "comments_observed": len(all_obs),
        "in_window_comments_observed": (sum(1 for o in all_obs if o.get("in_early_window"))
                                        if mode == SCOPE_EARLY_WINDOW else None),
        "store_merge": merge_stats,
        "video_outcomes": video_outcomes,
        "pages": all_pages,
    }
    _write_json(os.path.join(run_dir, "run.json"), run_record)

    store["runs"].append({
        "run_id": run_id, "started_at": started_at, "completed_at": run_record["completed_at"],
        "run_status": run_record["run_status"], "comments_observed": len(all_obs), **merge_stats,
    })
    _write_json(store_path, store)
    return run_record


# =============================================================================
# 5. Verification (timestamps, duplicates, lineage)
# =============================================================================

def verify_store(output_root: str, manifest: Dict[str, Any]) -> Dict[str, Any]:
    """Offline checks the user can run after a harvest. Returns a dict of check results.

    Fails (passed=False) when the store is missing, records no runs, was built from a different
    manifest, or references runs whose records are missing on disk.
    """
    store_path = os.path.join(output_root, "comment_store.json")
    if not os.path.exists(store_path):
        return {"comments": 0, "duplicate_sample_observations": 0, "sample_observations": 0, "runs": 0,
                "problems": {"store_missing": [store_path]}, "problem_counts": {"store_missing": 1},
                "warnings": {}, "warning_counts": {}, "passed": False}
    store = load_store(store_path, must_exist=True)
    video_pub = {v["video_id"]: parse_api_timestamp(v.get("video_published_at")) for v in manifest["videos"]}
    problems: Dict[str, List[Any]] = {
        "unparseable_timestamps": [],
        "updated_before_published": [],
        "comment_before_video_published": [],
        "video_not_in_manifest": [],
        "video_id_mismatch": [],
        "lineage_file_missing": [],
        "lineage_hash_mismatch": [],
        "lineage_item_not_found": [],
        "multiple_latest_text_versions": [],
        "store_key_mismatch": [],
        "latest_text_not_last_observed": [],
        "store_manifest_mismatch": [],
        "no_runs_recorded": [],
        "run_record_missing": [],
        "run_parsed_file_missing": [],
        "run_manifest_mismatch": [],
        "run_not_in_store": [],
        "observation_run_not_in_store": [],
        "window_flag_mismatch": [],
        "coverage_video_not_in_manifest": [],
        "coverage_checkpoint_missing": [],
        "coverage_checkpoint_mismatch": [],
    }
    if store.get("manifest_sha256") != manifest.get("manifest_sha256"):
        problems["store_manifest_mismatch"].append(
            {"store": store.get("manifest_sha256"), "manifest": manifest.get("manifest_sha256")})
    store_run_ids = [r.get("run_id") for r in store.get("runs", [])]
    if not store_run_ids:
        problems["no_runs_recorded"].append(store_path)
    for rid in store_run_ids:
        run_dir = os.path.join(output_root, "runs", rid)
        run_json = os.path.join(run_dir, "run.json")
        if not os.path.exists(run_json):
            problems["run_record_missing"].append(rid)
        else:
            with open(run_json, "r", encoding="utf-8") as f:
                if json.load(f).get("manifest_sha256") != manifest.get("manifest_sha256"):
                    problems["run_manifest_mismatch"].append(rid)
        if not os.path.exists(os.path.join(run_dir, "parsed_comments.jsonl")):
            problems["run_parsed_file_missing"].append(rid)
    runs_dir = os.path.join(output_root, "runs")
    if os.path.isdir(runs_dir):
        problems["run_not_in_store"] = sorted(set(os.listdir(runs_dir)) - set(store_run_ids))
    warnings: Dict[str, List[Any]] = {"updated_before_published_within_tolerance": []}
    raw_cache: Dict[str, Tuple[str, Optional[Dict[str, Any]]]] = {}
    observation_count = 0
    sample_keys = set()
    duplicate_sample_observations = 0

    for cid, c in store.get("comments", {}).items():
        if c.get("comment_id") != cid:
            problems["store_key_mismatch"].append(cid)
        pub = parse_api_timestamp(c.get("published_at"))
        upd = parse_api_timestamp(c.get("latest_updated_at"))
        if pub is None or (c.get("latest_updated_at") and upd is None):
            problems["unparseable_timestamps"].append(cid)
        elif upd is not None and upd < pub:
            gap = (pub - upd).total_seconds()
            if gap <= TIMESTAMP_SKEW_TOLERANCE_SECONDS:
                warnings["updated_before_published_within_tolerance"].append([cid, gap])
            else:
                problems["updated_before_published"].append(cid)
        vp = video_pub.get(c.get("manifest_video_id"))
        if c.get("manifest_video_id") not in video_pub:
            problems["video_not_in_manifest"].append(cid)
        elif pub is not None and vp is not None and pub < vp:
            problems["comment_before_video_published"].append(cid)
        if c.get("video_id") != c.get("manifest_video_id"):
            problems["video_id_mismatch"].append(cid)
        latest = [v for v in c.get("text_versions", []) if v.get("is_latest_version")]
        if len(latest) != 1:
            problems["multiple_latest_text_versions"].append(cid)
        observations = c.get("sample_observations", [])
        if latest and observations and len(latest) == 1:
            last = max(observations, key=lambda so: (so.get("observed_at") or "", so.get("page_number", 0),
                                                     so.get("source_item_position", 0)))
            if last.get("text_hash") is not None and latest[0]["text_hash"] != last["text_hash"]:
                problems["latest_text_not_last_observed"].append(cid)

        for so in c.get("sample_observations", []):
            observation_count += 1
            if so["run_id"] not in store_run_ids:
                problems["observation_run_not_in_store"].append([cid, so["run_id"]])
            key = (so["run_id"], so["raw_file"], so["source_item_position"])
            if key in sample_keys:
                duplicate_sample_observations += 1
            sample_keys.add(key)
            path = os.path.join(output_root, "runs", so["run_id"], so["raw_file"])
            if path not in raw_cache:
                if not os.path.exists(path):
                    raw_cache[path] = ("", None)
                else:
                    with open(path, "rb") as f:
                        data = f.read()
                    try:
                        raw_cache[path] = (sha256_bytes(data), json.loads(data.decode("utf-8")))
                    except (ValueError, UnicodeDecodeError):
                        raw_cache[path] = (sha256_bytes(data), None)
            digest, payload = raw_cache[path]
            if not digest:
                problems["lineage_file_missing"].append([cid, so["raw_file"]])
                continue
            if digest != so["payload_sha256"]:
                problems["lineage_hash_mismatch"].append([cid, so["raw_file"]])
                continue
            items = (payload or {}).get("items", [])
            pos = so["source_item_position"]
            if pos >= len(items) or ((items[pos].get("snippet", {}).get("topLevelComment", {}).get("id")
                                      or items[pos].get("id")) != cid):
                problems["lineage_item_not_found"].append([cid, so["raw_file"], pos])

    if manifest_scope_mode(manifest) == SCOPE_EARLY_WINDOW:
        hours = manifest["scope"]["window_hours"]
        bounds = {v["video_id"]: early_window_bounds(v, hours) for v in manifest["videos"]}
        for cid, c in store.get("comments", {}).items():
            b = bounds.get(c.get("manifest_video_id"))
            if b is not None and c.get("in_early_window") != in_early_window(c.get("published_at"), *b):
                problems["window_flag_mismatch"].append(cid)
        for vid, cov in store.get("video_coverage", {}).items():
            if vid not in bounds:
                problems["coverage_video_not_in_manifest"].append(vid)
                continue
            cp_path = os.path.join(output_root, "runs", cov.get("run_id", ""), "checkpoints", f"{vid}.json")
            if not os.path.exists(cp_path):
                problems["coverage_checkpoint_missing"].append(vid)
                continue
            with open(cp_path, "r", encoding="utf-8") as f:
                cp = json.load(f)
            status = cov.get("coverage_status")
            if (cp.get("coverage_status") != status
                    or cov.get("window_count_is_complete") != (status in COMPLETE_COVERAGE)
                    or (status in RESUMABLE_COVERAGE and not cp.get("next_page_token"))):
                problems["coverage_checkpoint_mismatch"].append(vid)

    total_problems = sum(len(v) for v in problems.values()) + duplicate_sample_observations
    return {
        "comments": len(store.get("comments", {})),
        "duplicate_sample_observations": duplicate_sample_observations,
        "sample_observations": observation_count,
        "runs": len(store.get("runs", [])),
        "problems": {k: v for k, v in problems.items()},
        "problem_counts": {k: len(v) for k, v in problems.items()},
        "warnings": warnings,
        "warning_counts": {k: len(v) for k, v in warnings.items()},
        "timestamp_skew_tolerance_seconds": TIMESTAMP_SKEW_TOLERANCE_SECONDS,
        "passed": total_problems == 0,
    }


def scan_run_duplicates(output_root: str) -> Dict[str, int]:
    """Counts duplicate comment IDs per run in parsed_comments.jsonl (independent of the keyed store)."""
    results: Dict[str, int] = {}
    runs_dir = os.path.join(output_root, "runs")
    if not os.path.isdir(runs_dir):
        return results
    for run_id in sorted(os.listdir(runs_dir)):
        path = os.path.join(runs_dir, run_id, "parsed_comments.jsonl")
        if not os.path.exists(path):
            continue
        ids = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    ids.append(json.loads(line)["comment_id"])
        results[run_id] = len(ids) - len(set(ids))
    return results


# =============================================================================
# 6. CLI
# =============================================================================

def parse_args(argv: Optional[List[str]] = None):
    parser = argparse.ArgumentParser(description="Bounded top-level comment harvesting for the PIPELINE_PILOT frame")
    sub = parser.add_subparsers(dest="command", required=True)

    m = sub.add_parser("manifest", help="Build a versioned collection manifest from a verified discovery report")
    m.add_argument("--report", required=True, help="Verified discovery report JSON")
    m.add_argument("--manifest-version", required=True, help="Version label, e.g. pilot-comments-v1")
    m.add_argument("--mode", choices=["newest", "early-window"], default="newest",
                   help="newest: newest N comments per video; early-window: comments within --window-hours of upload")
    m.add_argument("--max-comments-per-video", type=int, default=DEFAULT_MAX_COMMENTS_PER_VIDEO,
                   help="newest mode only")
    m.add_argument("--window-hours", type=int, default=None, help="early-window mode: hours after each video's upload")
    m.add_argument("--max-requests-per-video", type=int, default=None, help="early-window mode: request cap per video")
    m.add_argument("--output", required=True, help="Manifest JSON output path (refuses to overwrite)")

    e = sub.add_parser("estimate", help="Offline early-window cost estimate from a discovery report (no API calls)")
    e.add_argument("--report", required=True)
    e.add_argument("--max-requests-per-video", type=int, nargs="+", required=True,
                   help="One or more candidate caps to compare, e.g. 5 10 20 50")

    h = sub.add_parser("harvest", help="Run a bounded harvest (live YouTube API; local output only)")
    h.add_argument("--manifest", required=True)
    h.add_argument("--output-root", required=True, help="Local directory for raw pages, run records and the store")
    h.add_argument("--scope-accepted-by", required=True, help="Name of the person who accepted the manifest scope")
    h.add_argument("--video-limit", type=int, default=None, help="Harvest only the first N manifest videos (smoke test)")
    h.add_argument("--api-key", default=None, help="Override YOUTUBE_API_KEY")
    h.add_argument("--resume-from-run", default=None,
                   help="early-window only: continue budget-truncated videos from this run's saved page tokens")

    v = sub.add_parser("verify", help="Offline timestamp/duplicate/lineage checks on a harvest output root")
    v.add_argument("--manifest", required=True)
    v.add_argument("--output-root", required=True)
    return parser.parse_args(argv)


def _count_by(items: List[Dict[str, Any]], key: Callable[[Dict[str, Any]], Any]) -> Dict[Any, int]:
    counts: Dict[Any, int] = {}
    for item in items:
        k = key(item)
        counts[k] = counts.get(k, 0) + 1
    return counts


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main(argv: Optional[List[str]] = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    args = parse_args(argv)

    if args.command == "estimate":
        report = _load_json(args.report)
        meta = report.get("report_metadata", {})
        cands = _select_eligible(report, "PLAYER_MATCH")
        videos = [{"video_id": c["video_id"], "comment_count_at_discovery": c.get("comment_count")} for c in cands]
        players = _count_by(cands, lambda c: c.get("player_category") or c.get("player_relevance") or "UNKNOWN")
        channels = _count_by(cands, lambda c: c.get("channel_title") or "UNKNOWN")
        print(f"event_id={meta.get('event_id')} eligible PLAYER_MATCH videos={len(videos)}")
        print(f"players={dict(sorted(players.items()))} channels={dict(sorted(channels.items()))}")
        print("cap/video  planned_max_requests  full_retrieval_upper_bound  expected_truncated  unknown_counts")
        for cap in args.max_requests_per_video:
            est = estimate_early_window_cost(videos, cap)
            print(f"{cap:>9}  {est['planned_max_requests']:>20}  {est['full_retrieval_requests_upper_bound_known_counts']:>26}  "
                  f"{est['videos_expected_budget_truncated']:>18}  {est['videos_with_unknown_comment_count']:>14}")
        print("Basis: commentCount at discovery (includes replies), so these are upper bounds on top-level pages.")
        return 0

    if args.command == "manifest":
        if os.path.exists(args.output):
            print(f"Refusing to overwrite existing manifest: {args.output}", file=sys.stderr)
            return 1
        with open(args.report, "rb") as f:
            report_bytes = f.read()
        report = json.loads(report_bytes.decode("utf-8"))
        if args.mode == "early-window":
            if args.window_hours is None or args.max_requests_per_video is None:
                print("early-window mode needs --window-hours and --max-requests-per-video", file=sys.stderr)
                return 1
            manifest = build_early_window_manifest(report, args.report, sha256_bytes(report_bytes),
                                                   args.manifest_version, args.window_hours, args.max_requests_per_video)
        else:
            manifest = build_manifest(report, args.report, sha256_bytes(report_bytes),
                                      args.manifest_version, args.max_comments_per_video)
        _write_json(args.output, manifest)
        b = manifest["budget"]
        print(f"[OK] Manifest {manifest['manifest_version']} written to {args.output}")
        print(f"     mode={manifest_scope_mode(manifest)} videos={b['videos']} max_requests={b['max_total_requests']} "
              f"max_quota_units={b['max_quota_units']}")
        if "cost_estimate" in manifest:
            est = manifest["cost_estimate"]
            print(f"     estimated_requests<={est['planned_max_requests']} expected_budget_truncated_videos="
                  f"{est['videos_expected_budget_truncated']} unknown_comment_counts={est['videos_with_unknown_comment_count']}")
        print(f"     manifest_sha256={manifest['manifest_sha256']}")
        return 0

    manifest = _load_json(args.manifest)
    verify_manifest_integrity(manifest)

    if args.command == "harvest":
        fetcher = make_live_fetcher(build_youtube_client(args.api_key))
        record = run_harvest(
            manifest, fetcher, args.output_root, os.getenv("HMAC_SECRET", ""),
            args.scope_accepted_by, video_limit=args.video_limit, resume_from_run=args.resume_from_run,
        )
        print(f"[OK] Run {record['run_id']} status={record['run_status']}")
        print(f"     requests={record['telemetry']['requests_by_endpoint']['commentThreads.list']} "
              f"comments_observed={record['comments_observed']}")
        print(f"     outcomes={json.dumps(record['outcome_counts'])}")
        print(f"     store_merge={json.dumps(record['store_merge'])}")
        if record["coverage_counts"]:
            print(f"     coverage={json.dumps(record['coverage_counts'])} "
                  f"in_window_comments_observed={record['in_window_comments_observed']}")
            if record["run_status"] == "COMPLETED_BUDGET_TRUNCATED":
                print("     [NOTE] Some videos are budget-truncated: their window counts are partial, not total volume.")
        return 0 if record["run_status"] in ("COMPLETED", "COMPLETED_BUDGET_TRUNCATED") else 2

    result = verify_store(args.output_root, manifest)
    run_dups = scan_run_duplicates(args.output_root)
    print(json.dumps({k: v for k, v in result.items() if k not in ("problems", "warnings")}, indent=2))
    print("duplicate comment IDs within each run:", json.dumps(run_dups))
    for name, items in result["warnings"].items():
        if items:
            print(f"[WARN] {name}: {items[:10]}{' ...' if len(items) > 10 else ''}")
    for name, items in result["problems"].items():
        if items:
            print(f"[FAIL] {name}: {items[:10]}{' ...' if len(items) > 10 else ''}")
    passed = result["passed"] and not any(run_dups.values())
    print("[OK] All checks passed" if passed else "[FAIL] Verification found problems")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
