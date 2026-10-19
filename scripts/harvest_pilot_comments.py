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

MANIFEST_SCHEMA_VERSION = "pilot-comment-manifest/1"
RUN_SCHEMA_VERSION = "pilot-comment-run/1"
STORE_SCHEMA_VERSION = "pilot-comment-store/1"

DEFAULT_MAX_COMMENTS_PER_VIDEO = 100
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
PAGINATION_UNKNOWN = "UNKNOWN"

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


def verify_manifest_integrity(manifest: Dict[str, Any]) -> None:
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise ValueError(f"Unsupported manifest schema_version {manifest.get('schema_version')!r}")
    if manifest.get("frame_purpose") != "PIPELINE_PILOT":
        raise ValueError("Manifest frame_purpose must be PIPELINE_PILOT")
    recorded = manifest.get("manifest_sha256")
    body = {k: v for k, v in manifest.items() if k != "manifest_sha256"}
    if not recorded or canonical_json_hash(body) != recorded:
        raise ValueError("Manifest content hash mismatch: the manifest was edited after generation. Rebuild it.")


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


def harvest_video(
    video: Dict[str, Any],
    fetcher: Fetcher,
    run_dir: str,
    run_id: str,
    max_comments: int,
    hmac_secret: str,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Collects up to max_comments newest top-level comments for one video.

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

        requested_at = utc_now_iso()
        status, body = fetcher(params)
        retrieved_at = utc_now_iso()

        raw_rel = f"raw/{video_id}/page_{page_number:03d}.json"
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
            _write_json(os.path.join(run_dir, f"raw/{video_id}/page_{page_number:03d}.meta.json"), page_record)
            pages.append(page_record)
            break

        try:
            payload = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            outcome = OUTCOME_PARTIAL_ERROR if pages_succeeded else OUTCOME_ERROR
            error_code = "UNPARSEABLE_RESPONSE"
            page_record["error_code"] = error_code
            _write_json(os.path.join(run_dir, f"raw/{video_id}/page_{page_number:03d}.meta.json"), page_record)
            pages.append(page_record)
            break

        items = payload.get("items", []) or []
        next_token = payload.get("nextPageToken")
        page_record["item_count"] = len(items)
        page_record["next_page_token_returned"] = next_token
        _write_json(os.path.join(run_dir, f"raw/{video_id}/page_{page_number:03d}.meta.json"), page_record)
        pages.append(page_record)
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


def merge_into_store(store: Dict[str, Any], observations: List[Dict[str, Any]], run_id: str) -> Dict[str, int]:
    """Merges parsed observations into the local store at warehouse-aligned grains.

    - comments: one identity per comment_id (never duplicated across reruns)
    - text_versions: one per (comment_id, text_hash)
    - engagement_snapshots / sample_observations: one per observation
    """
    comments = store.setdefault("comments", {})
    stats = {"new_comments": 0, "existing_comments": 0, "new_text_versions": 0,
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

        if not any(v["text_hash"] == obs["text_hash"] for v in entry["text_versions"]):
            for v in entry["text_versions"]:
                v["is_latest_version"] = False
            entry["text_versions"].append({
                "text_hash": obs["text_hash"],
                "text": obs["text"],
                "updated_at": obs["updated_at"],
                "observed_at": obs["observed_at"],
                "run_id": run_id,
                "is_latest_version": True,
            })
            stats["new_text_versions"] += 1

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
        })

    return stats


def load_store(store_path: str) -> Dict[str, Any]:
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
) -> Dict[str, Any]:
    """Executes one bounded harvest run over the manifest. No Snowflake access."""
    verify_manifest_integrity(manifest)
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

    max_comments = manifest["scope"]["max_comments_per_video"]
    videos = manifest["videos"][:video_limit] if video_limit else manifest["videos"]
    started_at = utc_now_iso()

    video_outcomes: List[Dict[str, Any]] = []
    all_pages: List[Dict[str, Any]] = []
    all_obs: List[Dict[str, Any]] = []
    quota_stop = False
    for video in videos:
        if quota_stop:
            video_outcomes.append({
                "video_id": video["video_id"], "outcome": OUTCOME_NOT_ATTEMPTED,
                "error_code": "STOPPED_AFTER_QUOTA_LIMIT", "pagination_availability": PAGINATION_UNKNOWN,
                "pages_requested": 0, "pages_succeeded": 0, "comments_collected": 0,
            })
            continue
        outcome, pages, obs = harvest_video(video, fetcher, run_dir, run_id, max_comments, hmac_secret)
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
        "run_status": "STOPPED_QUOTA_LIMIT" if quota_stop else (
            "COMPLETED" if all(vo["outcome"] in (OUTCOME_COLLECTED, OUTCOME_EMPTY, OUTCOME_COMMENTS_DISABLED,
                                                 OUTCOME_VIDEO_NOT_FOUND) for vo in video_outcomes)
            else "COMPLETED_WITH_ERRORS"),
        "videos_in_manifest": len(manifest["videos"]),
        "videos_in_run": len(videos),
        "telemetry": {
            "requests_by_endpoint": {"commentThreads.list": requests},
            "quota_units_consumed_estimate": requests * OFFICIAL_QUOTA_COSTS["commentThreads.list"],
            "budget_max_quota_units": manifest["budget"]["max_quota_units"],
        },
        "outcome_counts": outcome_counts,
        "comments_observed": len(all_obs),
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
    """Offline checks the user can run after a harvest. Returns a dict of check results."""
    store = load_store(os.path.join(output_root, "comment_store.json"))
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
    }
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
            problems["updated_before_published"].append(cid)
        vp = video_pub.get(c.get("manifest_video_id"))
        if c.get("manifest_video_id") not in video_pub:
            problems["video_not_in_manifest"].append(cid)
        elif pub is not None and vp is not None and pub < vp:
            problems["comment_before_video_published"].append(cid)
        if c.get("video_id") != c.get("manifest_video_id"):
            problems["video_id_mismatch"].append(cid)
        if sum(1 for v in c.get("text_versions", []) if v.get("is_latest_version")) != 1:
            problems["multiple_latest_text_versions"].append(cid)

        for so in c.get("sample_observations", []):
            observation_count += 1
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

    total_problems = sum(len(v) for v in problems.values()) + duplicate_sample_observations
    return {
        "comments": len(store.get("comments", {})),
        "duplicate_sample_observations": duplicate_sample_observations,
        "sample_observations": observation_count,
        "runs": len(store.get("runs", [])),
        "problems": {k: v for k, v in problems.items()},
        "problem_counts": {k: len(v) for k, v in problems.items()},
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
    m.add_argument("--max-comments-per-video", type=int, default=DEFAULT_MAX_COMMENTS_PER_VIDEO)
    m.add_argument("--output", required=True, help="Manifest JSON output path (refuses to overwrite)")

    h = sub.add_parser("harvest", help="Run a bounded harvest (live YouTube API; local output only)")
    h.add_argument("--manifest", required=True)
    h.add_argument("--output-root", required=True, help="Local directory for raw pages, run records and the store")
    h.add_argument("--scope-accepted-by", required=True, help="Name of the person who accepted the manifest scope")
    h.add_argument("--video-limit", type=int, default=None, help="Harvest only the first N manifest videos (smoke test)")
    h.add_argument("--api-key", default=None, help="Override YOUTUBE_API_KEY")

    v = sub.add_parser("verify", help="Offline timestamp/duplicate/lineage checks on a harvest output root")
    v.add_argument("--manifest", required=True)
    v.add_argument("--output-root", required=True)
    return parser.parse_args(argv)


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main(argv: Optional[List[str]] = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    args = parse_args(argv)

    if args.command == "manifest":
        if os.path.exists(args.output):
            print(f"Refusing to overwrite existing manifest: {args.output}", file=sys.stderr)
            return 1
        with open(args.report, "rb") as f:
            report_bytes = f.read()
        manifest = build_manifest(
            json.loads(report_bytes.decode("utf-8")), args.report, sha256_bytes(report_bytes),
            args.manifest_version, args.max_comments_per_video,
        )
        _write_json(args.output, manifest)
        b = manifest["budget"]
        print(f"[OK] Manifest {manifest['manifest_version']} written to {args.output}")
        print(f"     videos={b['videos']} max_requests={b['max_total_requests']} max_quota_units={b['max_quota_units']}")
        print(f"     manifest_sha256={manifest['manifest_sha256']}")
        return 0

    manifest = _load_json(args.manifest)
    verify_manifest_integrity(manifest)

    if args.command == "harvest":
        fetcher = make_live_fetcher(build_youtube_client(args.api_key))
        record = run_harvest(
            manifest, fetcher, args.output_root, os.getenv("HMAC_SECRET", ""),
            args.scope_accepted_by, video_limit=args.video_limit,
        )
        print(f"[OK] Run {record['run_id']} status={record['run_status']}")
        print(f"     requests={record['telemetry']['requests_by_endpoint']['commentThreads.list']} "
              f"comments_observed={record['comments_observed']}")
        print(f"     outcomes={json.dumps(record['outcome_counts'])}")
        print(f"     store_merge={json.dumps(record['store_merge'])}")
        return 0 if record["run_status"] == "COMPLETED" else 2

    result = verify_store(args.output_root, manifest)
    run_dups = scan_run_duplicates(args.output_root)
    print(json.dumps({k: v for k, v in result.items() if k != "problems"}, indent=2))
    print("duplicate comment IDs within each run:", json.dumps(run_dups))
    for name, items in result["problems"].items():
        if items:
            print(f"[FAIL] {name}: {items[:10]}{' ...' if len(items) > 10 else ''}")
    passed = result["passed"] and not any(run_dups.values())
    print("[OK] All checks passed" if passed else "[FAIL] Verification found problems")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
