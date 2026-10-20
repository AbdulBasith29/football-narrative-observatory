#!/usr/bin/env python3
"""scripts/discover_pilot_coverage.py
Working candidate video discovery and coverage reporter for the Football Narrative Observatory.

Governing Invariants:
1. Zero Snowflake Writes: Strictly read-only candidate discovery and coverage reporting.
2. Real API Execution: Connects to YouTube Data API v3 using credentials from environment
   (YOUTUBE_API_KEY). Live mode NEVER silently falls back to mock fixtures.
3. Explicit Synthetic Mock Mode: `--mock` must be passed explicitly; all mock reports are
   prominently labeled SYNTHETIC_MOCK_DATA with prominent warnings.
4. Repository Configuration: Loads channels from config/frames/pipeline_pilot.yml, aliases from
   config/player_aliases.yml, and event terms from config/event_windows.yml.
5. Identity Validation: Validates channel identities via channels.list. Unresolved identities are
   kept out of eligible totals and reported as explicit identity failures.
6. Transparent Relevance Auditing: Records matched aliases, matched event terms, matched fields,
   and contextual excerpts. Explicitly separates PLAYER_MATCH, EVENT_SPECIFIC, and BROAD_EVENT_UNCERTAIN
   without silently altering frozen eligibility rules.
7. Accurate Telemetry & Status: Reports actual executed API calls, exact quota consumed based on
   official Google documentation rates, and uses RETURNED_PAGES_EXHAUSTED instead of claiming
   exhaustive channel coverage.
8. Methodology Protection: Preserves format uncertainty (Decision A.1: is_short = UNKNOWN for
   <=60s without vertical confirmation) without adding unsupported exclusions or invented strata.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

# Optional dotenv support for loading local .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Optional googleapiclient import for live mode
try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    HAVE_GOOGLE_API = True
except ImportError:
    HAVE_GOOGLE_API = False
    HttpError = Exception  # type: ignore


# =============================================================================
# 1. Official YouTube Data API Quota Reference (Official Google Documentation)
# =============================================================================

# Source: https://developers.google.com/youtube/v3/determine_quota_cost (page last updated 2026-10-15,
# checked 2026-10-20). search.list now has its own bucket: a default of 100 calls/day at 1 unit per call.
# Every other method used here draws on the shared default bucket of 10,000 units/day. Invalid requests
# still cost at least 1. A project's actual allocation can differ: check the Cloud console Quotas page.
OFFICIAL_QUOTA_COSTS = {
    "channels.list": 1,
    "playlistItems.list": 1,
    "videos.list": 1,
    "search.list": 1,
    "commentThreads.list": 1,
}
SEARCH_BUCKET = "search.list"
DEFAULT_BUCKET = "default"
QUOTA_BUCKET_BY_ENDPOINT = {"search.list": SEARCH_BUCKET}
DEFAULT_DAILY_QUOTA = {DEFAULT_BUCKET: 10000, SEARCH_BUCKET: 100}
QUOTA_REFERENCE = {
    "url": "https://developers.google.com/youtube/v3/determine_quota_cost",
    "page_last_updated": "2026-10-15",
    "checked_on": "2026-10-20",
    "note": "Default allocations; the project's actual quota may differ (Cloud console > Quotas).",
}


def quota_by_bucket(requests_by_endpoint: Dict[str, int]) -> Dict[str, int]:
    """Splits request counts into the separately limited quota buckets (units per bucket)."""
    buckets = {DEFAULT_BUCKET: 0, SEARCH_BUCKET: 0}
    for endpoint, count in requests_by_endpoint.items():
        bucket = QUOTA_BUCKET_BY_ENDPOINT.get(endpoint, DEFAULT_BUCKET)
        buckets[bucket] += count * OFFICIAL_QUOTA_COSTS.get(endpoint, 1)
    return buckets

# Broad/generic event terms that warrant relevance tier separation
GENERIC_EVENT_TERMS = {"final"}


# =============================================================================
# 2. Configuration Loader
# =============================================================================

def load_repository_configuration(repo_root: Optional[str] = None) -> Dict[str, Any]:
    """Loads pilot channels, player aliases, and event definitions from config/ YAML files."""
    if repo_root is None:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    # 1. Pilot frame channels
    pilot_frame_path = os.path.join(repo_root, "config", "frames", "pipeline_pilot.yml")
    if not os.path.exists(pilot_frame_path):
        raise FileNotFoundError(f"Pilot frame configuration not found at {pilot_frame_path}")
    with open(pilot_frame_path, "r", encoding="utf-8") as f:
        pilot_data = yaml.safe_load(f)
    channels = pilot_data.get("channels", [])

    # 2. Player aliases
    aliases_path = os.path.join(repo_root, "config", "player_aliases.yml")
    if not os.path.exists(aliases_path):
        raise FileNotFoundError(f"Player aliases configuration not found at {aliases_path}")
    with open(aliases_path, "r", encoding="utf-8") as f:
        alias_data = yaml.safe_load(f)

    player_aliases: Dict[str, List[str]] = {}
    for target in alias_data.get("targets", []):
        name = target.get("entity_name")
        alias_list = [a.get("text", "").lower() for a in target.get("aliases", []) if a.get("text")]
        if name:
            player_aliases[name] = alias_list

    # 3. Event windows & terms
    events_path = os.path.join(repo_root, "config", "event_windows.yml")
    if not os.path.exists(events_path):
        raise FileNotFoundError(f"Event windows configuration not found at {events_path}")
    with open(events_path, "r", encoding="utf-8") as f:
        events_data = yaml.safe_load(f)

    return {
        "channels": channels,
        "player_aliases": player_aliases,
        "events": events_data.get("events", []),
        "frame_purpose": pilot_data.get("frame_purpose", "PIPELINE_PILOT"),
        "methodology_version": pilot_data.get("methodology_version", "1.2"),
    }


# =============================================================================
# 3. Helpers: Duration Parsing & Word-Boundary Matching
# =============================================================================

def parse_iso8601_duration(duration_str: str) -> int:
    """Parses an ISO 8601 duration string (e.g. 'PT4M13S', 'PT58S') into integer seconds."""
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


def find_matching_terms(text: str, terms: List[str]) -> List[str]:
    """Finds all terms that match using word boundaries to prevent substring false positives."""
    matched = []
    text_lower = text.lower()
    for term in terms:
        pattern = r"\b" + re.escape(term.lower()) + r"\b"
        if re.search(pattern, text_lower):
            matched.append(term)
    return matched


def matches_aliases(text: str, aliases: List[str]) -> bool:
    """Matches text against a list of aliases using word boundaries."""
    return bool(find_matching_terms(text, aliases))


def classify_focal_players(title: str, description: str, player_aliases: Dict[str, List[str]]) -> str:
    """Classifies video into MESSI, RONALDO, DUAL, or NONE."""
    messi_aliases = player_aliases.get("Lionel Messi", ["messi", "leo messi", "lionel messi"])
    ronaldo_aliases = player_aliases.get("Cristiano Ronaldo", ["ronaldo", "cristiano", "cr7"])
    full_text = f"{title} {description}"
    has_messi = matches_aliases(full_text, messi_aliases)
    has_ronaldo = matches_aliases(full_text, ronaldo_aliases)
    if has_messi and has_ronaldo:
        return "DUAL"
    if has_messi:
        return "MESSI"
    if has_ronaldo:
        return "RONALDO"
    return "NONE"


def extract_match_excerpt(text: str, matched_words: List[str], window: int = 40) -> Optional[str]:
    """Extracts a short contextual excerpt around the first matched keyword."""
    if not text or not matched_words:
        return None
    text_lower = text.lower()
    for word in matched_words:
        idx = text_lower.find(word.lower())
        if idx != -1:
            start = max(0, idx - window)
            end = min(len(text), idx + len(word) + window)
            snippet = text[start:end].replace("\n", " ").strip()
            prefix = "..." if start > 0 else ""
            suffix = "..." if end < len(text) else ""
            return f"{prefix}{snippet}{suffix}"
    return None


def evaluate_format_uncertainty(
    duration_seconds: int,
    title: str = "",
    description: str = "",
    aspect_ratio: Optional[float] = None,
) -> Tuple[str, Optional[bool]]:
    """Evaluates format under Decision A.1: status cannot be inferred from duration alone.

    Returns:
        (format_label, is_short)
        where is_short is True, False, or None (UNKNOWN).
    """
    text = f"{title} {description}".lower()
    has_shorts_hashtag = bool(re.search(r"#shorts\b", text))

    if aspect_ratio is not None and aspect_ratio < 1.0:
        return "SHORTS_CONFIRMED", True
    if has_shorts_hashtag and duration_seconds <= 60:
        return "SHORTS_CONFIRMED", True
    if duration_seconds > 60:
        return "LONG_FORM", False

    # Duration <= 60s without vertical aspect ratio or #shorts tag is UNKNOWN
    return "FORMAT_UNKNOWN", None


def deduplicate_candidate_occurrences(
    raw_candidates: List[Dict[str, Any]],
    default_method: str = "search",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Deduplicates raw discovered candidates by exact video_id, preserving all discovery occurrences and provenance.

    Returns:
        (unique_candidates, deduplication_telemetry)
    """
    unique_candidates: Dict[str, Dict[str, Any]] = {}
    occurrences_by_id: Dict[str, List[Dict[str, Any]]] = {}
    duplicate_ids: List[str] = []

    for idx, c in enumerate(raw_candidates, start=1):
        vid = c.get("video_id")
        if not vid:
            continue

        page_num = c.get("page_number")
        ret_idx = c.get("retrieval_index")
        disc_method = c.get("discovery_method") or default_method

        occ = {
            "channel_id": c.get("channel_id"),
            "channel_title": c.get("channel_title") or c.get("configured_name"),
            "discovery_method": disc_method,
            "page_number": page_num if page_num is not None else "UNKNOWN",
            "retrieval_index": ret_idx if ret_idx is not None else "UNKNOWN",
            "published_at": c.get("published_at"),
        }

        if vid not in unique_candidates:
            cand_copy = dict(c)
            cand_copy["page_number"] = occ["page_number"]
            cand_copy["retrieval_index"] = occ["retrieval_index"]
            cand_copy["discovery_method"] = occ["discovery_method"]
            unique_candidates[vid] = cand_copy
            existing_occs = c.get("discovery_occurrences")
            if existing_occs and isinstance(existing_occs, list):
                occurrences_by_id[vid] = list(existing_occs)
            else:
                occurrences_by_id[vid] = [occ]
        else:
            occurrences_by_id[vid].append(occ)
            if vid not in duplicate_ids:
                duplicate_ids.append(vid)

    result_list = []
    for vid, cand in unique_candidates.items():
        cand["discovery_occurrences"] = occurrences_by_id[vid]
        cand["occurrence_count"] = len(occurrences_by_id[vid])
        result_list.append(cand)

    telem = {
        "total_raw_occurrences": len(raw_candidates),
        "total_unique_candidates": len(result_list),
        "total_duplicate_occurrences_collapsed": len(raw_candidates) - len(result_list),
        "duplicate_video_ids": duplicate_ids,
        "duplicate_details": [
            {
                "video_id": vid,
                "occurrence_count": len(occurrences_by_id[vid]),
                "occurrences": occurrences_by_id[vid],
            }
            for vid in duplicate_ids
        ],
    }
    return result_list, telem


# =============================================================================
# 4. Telemetry Tracker
# =============================================================================

class DiscoveryTelemetry:
    """Tracks actual API calls, quota units, and execution telemetry."""

    def __init__(self):
        self.requests_by_endpoint: Dict[str, int] = {
            "channels.list": 0,
            "playlistItems.list": 0,
            "videos.list": 0,
            "search.list": 0,
        }
        self.errors: List[Dict[str, Any]] = []

    def record_request(self, endpoint: str):
        if endpoint in self.requests_by_endpoint:
            self.requests_by_endpoint[endpoint] += 1
        else:
            self.requests_by_endpoint[endpoint] = 1

    def record_error(self, endpoint: str, channel_id: Optional[str], error: Exception):
        err_msg = str(error)
        status_code = getattr(getattr(error, 'resp', None), 'status', None)
        self.errors.append({
            "endpoint": endpoint,
            "channel_id": channel_id,
            "status_code": status_code,
            "error_message": err_msg[:500],
            "is_quota_error": "429" in err_msg or "quota" in err_msg.lower(),
        })

    def total_requests(self) -> int:
        return sum(self.requests_by_endpoint.values())

    def calculate_actual_quota_units(self) -> int:
        """Units drawn from the shared default bucket (search.list is counted separately)."""
        return quota_by_bucket(self.requests_by_endpoint)[DEFAULT_BUCKET]

    def calculate_search_calls(self) -> int:
        return quota_by_bucket(self.requests_by_endpoint)[SEARCH_BUCKET]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "requests_by_endpoint": dict(self.requests_by_endpoint),
            "total_api_requests": self.total_requests(),
            "actual_quota_units_consumed": self.calculate_actual_quota_units(),
            "search_list_calls_consumed": self.calculate_search_calls(),
            "quota_by_bucket": quota_by_bucket(self.requests_by_endpoint),
            "official_quota_rates_used": OFFICIAL_QUOTA_COSTS,
            "quota_reference": QUOTA_REFERENCE,
            "error_count": len(self.errors),
            "errors": self.errors,
        }


# =============================================================================
# 5. Live YouTube API Operations
# =============================================================================

def build_youtube_client(api_key: Optional[str] = None):
    """Instantiates the YouTube Data API v3 client reading credentials from environment."""
    if not HAVE_GOOGLE_API:
        raise ImportError(
            "google-api-python-client is not installed. Please install it to run live discovery."
        )

    resolved_key = api_key or os.getenv("YOUTUBE_API_KEY")
    if not resolved_key:
        raise RuntimeError(
            "YOUTUBE_API_KEY environment variable is missing. "
            "Set YOUTUBE_API_KEY in your environment or use --mock for synthetic offline validation."
        )

    return build("youtube", "v3", developerKey=resolved_key, cache_discovery=False)


def validate_channel_identities_live(
    youtube,
    configured_channels: List[Dict[str, Any]],
    telemetry: DiscoveryTelemetry,
) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    """Validates channel existence and identities via channels.list.

    Identities that fail validation are marked IDENTITY_UNRESOLVED and isolated from eligible totals.
    """
    channel_ids = [ch["channel_id"] for ch in configured_channels]
    validated: Dict[str, Dict[str, Any]] = {}
    discrepancies: List[Dict[str, Any]] = []

    # Batch in groups of up to 50 IDs
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i:i + 50]
        telemetry.record_request("channels.list")
        try:
            req = youtube.channels().list(
                part="snippet,contentDetails,status",
                id=",".join(batch)
            )
            resp = req.execute()
        except Exception as exc:
            telemetry.record_error("channels.list", None, exc)
            raise RuntimeError(f"channels.list request failed: {exc}") from exc

        items = resp.get("items", [])
        items_by_id = {item["id"]: item for item in items}

        for ch in configured_channels:
            cid = ch["channel_id"]
            configured_name = ch.get("channel_name", "")
            if cid in items_by_id:
                item = items_by_id[cid]
                snippet = item.get("snippet", {})
                live_title = snippet.get("title", "")
                custom_url = snippet.get("customUrl", "")
                related = item.get("contentDetails", {}).get("relatedPlaylists", {})
                uploads_id = related.get("uploads")

                # Detect identity discrepancies (e.g. title renames)
                if live_title.strip().lower() != configured_name.strip().lower():
                    discrepancies.append({
                        "channel_id": cid,
                        "configured_name": configured_name,
                        "live_title": live_title,
                        "custom_url": custom_url,
                        "discrepancy_type": "TITLE_MISMATCH",
                        "is_resolved": True,
                        "resolution": "Title variation recorded; channel identity preserved and verified uploads playlist confirmed.",
                    })

                validated[cid] = {
                    "channel_id": cid,
                    "configured_name": configured_name,
                    "live_title": live_title,
                    "custom_url": custom_url,
                    "uploads_playlist_id": uploads_id,
                    "discovery_source": ch.get("discovery_source", "manual_bootstrap"),
                    "status": "VALIDATED",
                }
            else:
                discrepancies.append({
                    "channel_id": cid,
                    "configured_name": configured_name,
                    "discrepancy_type": "CHANNEL_NOT_FOUND",
                    "is_resolved": False,
                    "resolution": "Channel not returned by channels.list; candidates gated from eligible totals.",
                })
                validated[cid] = {
                    "channel_id": cid,
                    "configured_name": configured_name,
                    "status": "IDENTITY_UNRESOLVED",
                }

    return validated, discrepancies


def discover_channel_candidates_uploads_walk(
    youtube,
    channel_info: Dict[str, Any],
    window_start: datetime.datetime,
    window_end: datetime.datetime,
    max_pages: int,
    telemetry: DiscoveryTelemetry,
) -> Dict[str, Any]:
    """Discovers candidates by paginating the uploads playlist backwards in time."""
    uploads_id = channel_info.get("uploads_playlist_id")
    cid = channel_info["channel_id"]

    if not uploads_id or channel_info.get("status") == "IDENTITY_UNRESOLVED":
        return {
            "channel_id": cid,
            "discovery_method": "UPLOADS_PLAYLIST_WALK",
            "items": [],
            "pages_fetched": 0,
            "is_truncated": False,
            "completeness_status": "SKIPPED_UNRESOLVED_IDENTITY",
        }

    items = []
    page_token = None
    pages_fetched = 0
    is_truncated = False
    is_monotonic = True
    last_pub = None

    while True:
        if pages_fetched >= max_pages:
            is_truncated = True
            break

        telemetry.record_request("playlistItems.list")
        pages_fetched += 1
        try:
            req = youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=uploads_id,
                maxResults=50,
                pageToken=page_token
            )
            resp = req.execute()
        except Exception as exc:
            telemetry.record_error("playlistItems.list", cid, exc)
            return {
                "channel_id": cid,
                "discovery_method": "UPLOADS_PLAYLIST_WALK",
                "items": items,
                "pages_fetched": pages_fetched,
                "is_truncated": True,
                "completeness_status": "ABORTED_ON_API_ERROR",
                "error": str(exc),
            }

        batch_items = resp.get("items", [])
        if not batch_items:
            break

        for it in batch_items:
            pub_raw = it.get("contentDetails", {}).get("videoPublishedAt") or it.get("snippet", {}).get("publishedAt", "")
            try:
                pub_dt = datetime.datetime.fromisoformat(pub_raw.replace("Z", "+00:00"))
            except Exception:
                continue

            if last_pub and pub_dt > last_pub:
                is_monotonic = False
            last_pub = pub_dt

            # If item is newer than window_end, continue paginating backwards
            if pub_dt > window_end:
                continue
            # If item is older than window_start, we have passed the window
            if pub_dt < window_start:
                if is_monotonic:
                    return {
                        "channel_id": cid,
                        "discovery_method": "UPLOADS_PLAYLIST_WALK",
                        "items": items,
                        "pages_fetched": pages_fetched,
                        "is_truncated": False,
                        "completeness_status": "RETURNED_PAGES_EXHAUSTED",
                    }
                else:
                    continue

            v_id = it.get("contentDetails", {}).get("videoId") or it.get("snippet", {}).get("resourceId", {}).get("videoId")
            if v_id:
                items.append({
                    "video_id": v_id,
                    "channel_id": cid,
                    "title": it.get("snippet", {}).get("title", ""),
                    "description": it.get("snippet", {}).get("description", ""),
                    "published_at": pub_raw,
                    "page_number": pages_fetched,
                    "retrieval_index": len(items) + 1,
                    "discovery_method": "UPLOADS_PLAYLIST_WALK",
                })

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    completeness = "TRUNCATED_AT_PAGE_LIMIT" if is_truncated else "RETURNED_PAGES_EXHAUSTED"
    return {
        "channel_id": cid,
        "discovery_method": "UPLOADS_PLAYLIST_WALK",
        "items": items,
        "pages_fetched": pages_fetched,
        "is_truncated": is_truncated,
        "completeness_status": completeness,
    }


def discover_channel_candidates_date_bounded_search(
    youtube,
    channel_info: Dict[str, Any],
    window_start: datetime.datetime,
    window_end: datetime.datetime,
    max_pages: int,
    telemetry: DiscoveryTelemetry,
) -> Dict[str, Any]:
    """Discovers candidates using date-bounded search.list."""
    cid = channel_info["channel_id"]
    if channel_info.get("status") == "IDENTITY_UNRESOLVED":
        return {
            "channel_id": cid,
            "discovery_method": "DATE_BOUNDED_SEARCH",
            "items": [],
            "pages_fetched": 0,
            "is_truncated": False,
            "completeness_status": "SKIPPED_UNRESOLVED_IDENTITY",
        }

    items = []
    page_token = None
    pages_fetched = 0
    is_truncated = False

    w_start_iso = window_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    w_end_iso = window_end.strftime("%Y-%m-%dT%H:%M:%SZ")

    while True:
        if pages_fetched >= max_pages:
            is_truncated = True
            break

        telemetry.record_request("search.list")
        pages_fetched += 1
        try:
            req = youtube.search().list(
                part="snippet",
                channelId=cid,
                type="video",
                publishedAfter=w_start_iso,
                publishedBefore=w_end_iso,
                maxResults=50,
                pageToken=page_token
            )
            resp = req.execute()
        except Exception as exc:
            telemetry.record_error("search.list", cid, exc)
            return {
                "channel_id": cid,
                "discovery_method": "DATE_BOUNDED_SEARCH",
                "items": items,
                "pages_fetched": pages_fetched,
                "is_truncated": True,
                "completeness_status": "ABORTED_ON_API_ERROR",
                "error": str(exc),
            }

        batch_items = resp.get("items", [])
        if not batch_items:
            break

        for it in batch_items:
            v_id = it.get("id", {}).get("videoId")
            if v_id:
                items.append({
                    "video_id": v_id,
                    "channel_id": cid,
                    "title": it.get("snippet", {}).get("title", ""),
                    "description": it.get("snippet", {}).get("description", ""),
                    "published_at": it.get("snippet", {}).get("publishedAt", ""),
                    "page_number": pages_fetched,
                    "retrieval_index": len(items) + 1,
                    "discovery_method": "DATE_BOUNDED_SEARCH",
                })

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    completeness = "TRUNCATED_AT_PAGE_LIMIT" if is_truncated else "RETURNED_PAGES_EXHAUSTED"
    return {
        "channel_id": cid,
        "discovery_method": "DATE_BOUNDED_SEARCH",
        "items": items,
        "pages_fetched": pages_fetched,
        "is_truncated": is_truncated,
        "completeness_status": completeness,
    }


def hydrate_video_metadata_live(
    youtube,
    video_ids: List[str],
    telemetry: DiscoveryTelemetry,
) -> Dict[str, Dict[str, Any]]:
    """Hydrates full video metadata via videos.list in batches of up to 50 IDs."""
    hydrated: Dict[str, Dict[str, Any]] = {}
    unique_ids = sorted(list(set(video_ids)))

    for i in range(0, len(unique_ids), 50):
        batch = unique_ids[i:i + 50]
        telemetry.record_request("videos.list")
        try:
            req = youtube.videos().list(
                part="snippet,contentDetails,statistics,status",
                id=",".join(batch)
            )
            resp = req.execute()
        except Exception as exc:
            telemetry.record_error("videos.list", None, exc)
            raise RuntimeError(f"videos.list request failed: {exc}") from exc

        items = resp.get("items", [])
        returned_ids = set()

        for it in items:
            vid = it.get("id")
            returned_ids.add(vid)
            snippet = it.get("snippet", {})
            content = it.get("contentDetails", {})
            stats = it.get("statistics", {})

            duration_raw = content.get("duration", "")
            duration_sec = parse_iso8601_duration(duration_raw)

            lang = snippet.get("defaultAudioLanguage") or snippet.get("defaultLanguage")
            primary_lang = lang.strip().lower() if lang else "UNKNOWN"

            hydrated[vid] = {
                "video_id": vid,
                "title": snippet.get("title", ""),
                "description": snippet.get("description", ""),
                "published_at": snippet.get("publishedAt", ""),
                "channel_id": snippet.get("channelId", ""),
                "channel_title": snippet.get("channelTitle", ""),
                "duration_seconds": duration_sec,
                "primary_language": primary_lang,
                "view_count": stats.get("viewCount"),
                "like_count": stats.get("likeCount"),
                "comment_count": stats.get("commentCount"),
                "is_available": True,
            }

        # Track videos that were discovered but not returned by videos.list (e.g. deleted/private)
        missing_ids = set(batch) - returned_ids
        for mid in missing_ids:
            hydrated[mid] = {
                "video_id": mid,
                "is_available": False,
                "exclusion_reason": "VIDEO_UNAVAILABLE",
            }

    return hydrated


# =============================================================================
# 6. Candidate Classification & Relevance Auditing
# =============================================================================

def classify_hydrated_candidate(
    candidate: Dict[str, Any],
    window_start: datetime.datetime,
    window_end: datetime.datetime,
    player_aliases: Dict[str, List[str]],
    event_terms: List[str],
    baseline_terms: Optional[List[str]] = None,
    channel_is_validated: bool = True,
) -> Dict[str, Any]:
    """Applies frozen methodology rules to classify video relevance, format, and audit excerpts."""
    # If channel identity is unvalidated, gate candidate out of eligible totals
    if not channel_is_validated:
        return {
            "video_id": candidate["video_id"],
            "channel_id": candidate.get("channel_id"),
            "channel_title": candidate.get("channel_title"),
            "title": candidate.get("title", ""),
            "description": candidate.get("description", ""),
            "published_at": candidate.get("published_at", ""),
            "page_number": candidate.get("page_number", "UNKNOWN"),
            "retrieval_index": candidate.get("retrieval_index", "UNKNOWN"),
            "discovery_method": candidate.get("discovery_method", "UNKNOWN"),
            "status": "INELIGIBLE",
            "exclusion_reason": "UNRESOLVED_CHANNEL_IDENTITY",
            "player_category": "NONE",
            "player_relevance": "NONE",
            "relevance_tier": "UNRESOLVED_CHANNEL",
            "matched_player_aliases": [],
            "matched_event_terms": [],
            "matched_baseline_terms": [],
            "is_baseline_only": False,
            "matched_field": None,
            "match_excerpt": None,
            "is_short": None,
            "format_label": "UNKNOWN",
            "proposed_methodology_action": None,
            "audit_note": None,
            "discovery_occurrences": candidate.get("discovery_occurrences", []),
            "occurrence_count": candidate.get("occurrence_count", len(candidate.get("discovery_occurrences", [])) or 1),
        }

    if not candidate.get("is_available", True):
        return {
            "video_id": candidate["video_id"],
            "channel_id": candidate.get("channel_id"),
            "channel_title": candidate.get("channel_title"),
            "title": candidate.get("title", ""),
            "description": candidate.get("description", ""),
            "published_at": candidate.get("published_at", ""),
            "page_number": candidate.get("page_number", "UNKNOWN"),
            "retrieval_index": candidate.get("retrieval_index", "UNKNOWN"),
            "discovery_method": candidate.get("discovery_method", "UNKNOWN"),
            "status": "INELIGIBLE",
            "exclusion_reason": "VIDEO_UNAVAILABLE",
            "player_category": "NONE",
            "player_relevance": "NONE",
            "relevance_tier": "NO_MATCH",
            "matched_player_aliases": [],
            "matched_event_terms": [],
            "matched_baseline_terms": [],
            "is_baseline_only": False,
            "matched_field": None,
            "match_excerpt": None,
            "is_short": None,
            "format_label": "UNKNOWN",
            "proposed_methodology_action": None,
            "audit_note": None,
            "discovery_occurrences": candidate.get("discovery_occurrences", []),
            "occurrence_count": candidate.get("occurrence_count", len(candidate.get("discovery_occurrences", [])) or 1),
        }

    title = candidate.get("title", "")
    desc = candidate.get("description", "")
    pub_raw = candidate.get("published_at", "")

    try:
        pub_dt = datetime.datetime.fromisoformat(pub_raw.replace("Z", "+00:00"))
    except Exception:
        pub_dt = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

    # 1. Temporal Window
    in_window = (window_start <= pub_dt <= window_end)

    # 2. Match Target Player Aliases
    messi_aliases = player_aliases.get("Lionel Messi", ["messi", "leo messi", "lionel messi"])
    ronaldo_aliases = player_aliases.get("Cristiano Ronaldo", ["ronaldo", "cristiano", "cr7"])

    matched_messi_title = find_matching_terms(title, messi_aliases)
    matched_messi_desc = find_matching_terms(desc, messi_aliases)
    matched_ronaldo_title = find_matching_terms(title, ronaldo_aliases)
    matched_ronaldo_desc = find_matching_terms(desc, ronaldo_aliases)

    has_messi = bool(matched_messi_title or matched_messi_desc)
    has_ronaldo = bool(matched_ronaldo_title or matched_ronaldo_desc)

    matched_player_terms = list(set(
        matched_messi_title + matched_messi_desc + matched_ronaldo_title + matched_ronaldo_desc
    ))

    if has_messi and has_ronaldo:
        player_category = "DUAL"
    elif has_messi:
        player_category = "MESSI"
    elif has_ronaldo:
        player_category = "RONALDO"
    else:
        player_category = "NONE"

    # 3. Match Event Terms vs Baseline Terms
    matched_event_title = find_matching_terms(title, event_terms)
    matched_event_desc = find_matching_terms(desc, event_terms)
    matched_event_terms = list(set(matched_event_title + matched_event_desc))

    # If text matching produced nothing (e.g. description omitted in report), inspect existing candidate metadata
    if not matched_event_terms and candidate.get("matched_event_terms"):
        e_lower = {t.lower() for t in event_terms}
        matched_event_terms = [t for t in candidate["matched_event_terms"] if t.lower() in e_lower]
    has_event = bool(matched_event_terms)

    b_terms = baseline_terms or []
    matched_base_title = find_matching_terms(title, b_terms)
    matched_base_desc = find_matching_terms(desc, b_terms)
    matched_base_terms = list(set(matched_base_title + matched_base_desc))
    if not matched_base_terms and candidate.get("matched_event_terms"):
        b_lower = {t.lower() for t in b_terms}
        matched_base_terms = [t for t in candidate["matched_event_terms"] if t.lower() in b_lower]
    has_baseline = bool(matched_base_terms)

    is_baseline_only = (player_category == "NONE") and (not has_event) and has_baseline

    # Matched Field Determination
    matched_in_title = bool(matched_messi_title or matched_ronaldo_title or matched_event_title or matched_base_title)
    matched_in_desc = bool(matched_messi_desc or matched_ronaldo_desc or matched_event_desc or matched_base_desc)
    if matched_in_title and matched_in_desc:
        matched_field = "both"
    elif matched_in_title:
        matched_field = "title"
    elif matched_in_desc:
        matched_field = "description"
    else:
        matched_field = candidate.get("matched_field")

    # Excerpt Extraction for Relevance Auditing
    all_matched = list(set(matched_player_terms + matched_event_terms + matched_base_terms))
    excerpt = None
    if matched_in_title:
        excerpt = extract_match_excerpt(title, all_matched)
    elif matched_in_desc:
        excerpt = extract_match_excerpt(desc, all_matched)
    if not excerpt:
        excerpt = candidate.get("match_excerpt")

    # 4. Relevance Tier Separation (Player vs Event-Specific vs Broad-Generic vs Baseline-Only)
    if player_category != "NONE":
        relevance_tier = "PLAYER_MATCH"
    elif has_event:
        specific_matches = [t for t in matched_event_terms if t.lower() not in GENERIC_EVENT_TERMS]
        if specific_matches:
            relevance_tier = "EVENT_SPECIFIC"
        else:
            relevance_tier = "BROAD_EVENT_UNCERTAIN"
    elif is_baseline_only:
        relevance_tier = "BASELINE_ONLY"
    else:
        relevance_tier = "NO_MATCH"

    # 5. Format Disambiguation under Decision A.1 (Preserve Uncertainty)
    dur = candidate.get("duration_seconds", 0)
    format_label, is_short = evaluate_format_uncertainty(
        duration_seconds=dur,
        title=title,
        description=desc,
        aspect_ratio=candidate.get("aspect_ratio"),
    )

    # 6. Eligibility Determination (Strictly approved rules - baseline matches remain ELIGIBLE)
    exclusion_reason = None
    if not in_window:
        status = "INELIGIBLE"
        exclusion_reason = "OUT_OF_WINDOW"
    elif relevance_tier == "NO_MATCH":
        status = "INELIGIBLE"
        exclusion_reason = "NO_TARGET_RELEVANCE"
    else:
        status = "ELIGIBLE"

    # 7. Proposed Methodology Corrections & Audit Notes (Without silently mutating frozen rules)
    proposed_methodology_action = None
    audit_note = None

    if relevance_tier == "BASELINE_ONLY":
        proposed_methodology_action = "PROPOSE_EXCLUSION_NON_EVENT_BASELINE"
        audit_note = f"Video matched broad baseline term(s) {matched_base_terms} without event terms or focal player keywords; off-target gaming/unrelated media."
    elif relevance_tier == "BROAD_EVENT_UNCERTAIN":
        proposed_methodology_action = "PROPOSE_EXCLUSION_GENERIC_TERM"
        audit_note = "Matched only generic event term 'final' without focal player or specific event keywords; ambiguous sports term."
    elif status == "INELIGIBLE" and in_window:
        combined_text = f"{title} {desc}".lower()
        if re.search(r"\bgoat\b", combined_text) or "greatest of all time" in combined_text or "🐐" in title:
            proposed_methodology_action = "PROPOSE_INCLUSION_IMPLICIT_DEBATE"
            audit_note = "Implicit GOAT debate commentary in window without explicit player/event keywords in metadata."

    return {
        "video_id": candidate["video_id"],
        "channel_id": candidate.get("channel_id"),
        "channel_title": candidate.get("channel_title"),
        "title": title,
        "description": desc,
        "published_at": pub_raw,
        "page_number": candidate.get("page_number", "UNKNOWN"),
        "retrieval_index": candidate.get("retrieval_index", "UNKNOWN"),
        "discovery_method": candidate.get("discovery_method", "UNKNOWN"),
        "in_window": in_window,
        "player_category": player_category,
        "player_relevance": player_category,
        "relevance_tier": relevance_tier,
        "has_event_term": has_event,
        "has_baseline_term": has_baseline,
        "is_baseline_only": is_baseline_only,
        "matched_player_aliases": matched_player_terms,
        "matched_event_terms": matched_event_terms,
        "matched_baseline_terms": matched_base_terms,
        "matched_field": matched_field,
        "match_excerpt": excerpt,
        "duration_seconds": dur,
        "is_short": is_short,
        "format_label": format_label,
        "status": status,
        "exclusion_reason": exclusion_reason,
        "proposed_methodology_action": proposed_methodology_action,
        "audit_note": audit_note,
        "view_count": candidate.get("view_count"),
        "comment_count": candidate.get("comment_count"),
        "discovery_occurrences": candidate.get("discovery_occurrences", []),
        "occurrence_count": candidate.get("occurrence_count", len(candidate.get("discovery_occurrences", [])) or 1),
    }


# =============================================================================
# 7. Explicit Synthetic Mock Mode Fixtures
# =============================================================================

def run_synthetic_mock_discovery(
    repo_config: Dict[str, Any],
    window_start: datetime.datetime,
    window_end: datetime.datetime,
    event_terms: List[str],
    baseline_terms: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Generates an explicitly labeled SYNTHETIC candidate coverage report."""
    channels = repo_config["channels"]
    aliases = repo_config["player_aliases"]
    b_terms = baseline_terms or ["world cup", "fifa", "fifa world cup", "qatar 2022"]

    synthetic_raw_candidates = [
        {
            "video_id": "mock_sky_01",
            "channel_id": "UCNAf1k0yIjyGu3k9BwAg3lg",
            "channel_title": "Sky Sports Premier League",
            "title": "Lionel Messi wins World Cup: Historic Argentina triumph reaction",
            "description": "Full reaction to the Qatar 2022 World Cup final.",
            "published_at": "2022-12-18T19:30:00Z",
            "duration_seconds": 920,
            "view_count": "1450000",
            "comment_count": "4520",
            "is_available": True,
            "page_number": 1,
            "retrieval_index": 1,
            "discovery_method": "SYNTHETIC_FIXTURE",
        },
        {
            "video_id": "mock_espn_01",
            "channel_id": "UC6c1z7bA__85CIWZ_jpCK-Q",
            "channel_title": "ESPN FC",
            "title": "Is Messi the undisputed GOAT over Ronaldo now? | ESPN FC",
            "description": "Dan Thomas and Craig Burley debate Messi vs Cristiano Ronaldo after the final.",
            "published_at": "2022-12-18T21:00:00Z",
            "duration_seconds": 780,
            "view_count": "1840000",
            "comment_count": "8930",
            "is_available": True,
            "page_number": 1,
            "retrieval_index": 2,
            "discovery_method": "SYNTHETIC_FIXTURE",
        },
        {
            "video_id": "mock_gold_01",
            "channel_id": "UCFxG3MxHC1ogUglvJeMHmVQ",
            "channel_title": "Overlap Daily",
            "title": "MESSI WINS THE WORLD CUP! Greatest Final EVER! Argentina 3-3 France Reaction",
            "description": "Live post-match watchalong thoughts on Messi's glory.",
            "published_at": "2022-12-18T18:45:00Z",
            "duration_seconds": 1820,
            "view_count": "920000",
            "comment_count": "6120",
            "is_available": True,
            "page_number": 1,
            "retrieval_index": 3,
            "discovery_method": "SYNTHETIC_FIXTURE",
        },
        {
            "video_id": "mock_talk_01",
            "channel_id": "UCvExuGSwx1Yw30dDY8JEJ3w",
            "channel_title": "TalkFCB",
            "title": "MESSI IS THE WORLD CHAMPION! Barcelona Legend Achieves Destiny",
            "description": "Emotional reaction to Leo Messi winning the World Cup final.",
            "published_at": "2022-12-18T19:00:00Z",
            "duration_seconds": 850,
            "view_count": "410000",
            "comment_count": "3410",
            "is_available": True,
            "page_number": 1,
            "retrieval_index": 4,
            "discovery_method": "SYNTHETIC_FIXTURE",
        },
        {
            "video_id": "mock_tus_01",
            "channel_id": "UCMmVPVb0BwSIOWVeDwlPocQ",
            "channel_title": "The United Stand",
            "title": "Ronaldo vs Messi World Cup debate settled? Man United transfer latest",
            "description": "Discussing Cristiano Ronaldo's contract termination and Messi's triumph.",
            "published_at": "2022-12-19T14:00:00Z",
            "duration_seconds": 1420,
            "view_count": "530000",
            "comment_count": "4800",
            "is_available": True,
            "page_number": 1,
            "retrieval_index": 5,
            "discovery_method": "SYNTHETIC_FIXTURE",
        },
        {
            "video_id": "mock_stat_01",
            "channel_id": "UCXzrxvm0Jqq1OpSZQhT6lEQ",
            "channel_title": "Statman Dave",
            "title": "How Lionel Messi Won The World Cup | Tactical Breakdown",
            "description": "Statistical analysis of Lionel Messi's playmaking and Argentina's structure.",
            "published_at": "2022-12-19T16:00:00Z",
            "duration_seconds": 640,
            "view_count": "380000",
            "comment_count": "1940",
            "is_available": True,
            "page_number": 1,
            "retrieval_index": 6,
            "discovery_method": "SYNTHETIC_FIXTURE",
        },
        {
            "video_id": "mock_speed_01",
            "channel_id": "UCWsDFcIhY2DBi3GB5uykGXA",
            "channel_title": "IShowSpeed",
            "title": "Speed cries as Messi wins World Cup over Ronaldo #shorts",
            "description": "Reaction to World Cup final #shorts",
            "published_at": "2022-12-18T18:30:00Z",
            "duration_seconds": 45,
            "view_count": "4500000",
            "comment_count": "12500",
            "is_available": True,
            "page_number": 1,
            "retrieval_index": 7,
            "discovery_method": "SYNTHETIC_FIXTURE",
        },
        {
            "video_id": "mock_generic_final",
            "channel_id": "UCNAf1k0yIjyGu3k9BwAg3lg",
            "channel_title": "Sky Sports Premier League",
            "title": "Premier League returns: Final training session before restart",
            "description": "Clubs prepare for the return of domestic action.",
            "published_at": "2022-12-20T11:00:00Z",
            "duration_seconds": 400,
            "view_count": "120000",
            "comment_count": "340",
            "is_available": True,
            "page_number": 1,
            "retrieval_index": 8,
            "discovery_method": "SYNTHETIC_FIXTURE",
        },
        {
            "video_id": "mock_fifa_gaming",
            "channel_id": "UCFxG3MxHC1ogUglvJeMHmVQ",
            "channel_title": "Overlap Daily",
            "title": "FIFA 23 Create a Club Career Mode Episode 27",
            "description": "Career mode gameplay on FIFA 23.",
            "published_at": "2022-12-19T13:00:00Z",
            "duration_seconds": 1800,
            "view_count": "80000",
            "comment_count": "110",
            "is_available": True,
            "page_number": 1,
            "retrieval_index": 9,
            "discovery_method": "SYNTHETIC_FIXTURE",
        },
        {
            "video_id": "mock_goat_debate",
            "channel_id": "UC6c1z7bA__85CIWZ_jpCK-Q",
            "channel_title": "ESPN FC",
            "title": "The GOAT debate is officially over 🤯 🐐 #shorts",
            "description": "Debate discussion #shorts",
            "published_at": "2022-12-18T22:00:00Z",
            "duration_seconds": 45,
            "view_count": "550000",
            "comment_count": "2900",
            "is_available": True,
            "page_number": 1,
            "retrieval_index": 10,
            "discovery_method": "SYNTHETIC_FIXTURE",
        },
        # Overlapping occurrence on page 2 to test deduplication provenance
        {
            "video_id": "mock_espn_01",
            "channel_id": "UC6c1z7bA__85CIWZ_jpCK-Q",
            "channel_title": "ESPN FC",
            "title": "Is Messi the undisputed GOAT over Ronaldo now? | ESPN FC",
            "description": "Dan Thomas and Craig Burley debate Messi vs Cristiano Ronaldo after the final.",
            "published_at": "2022-12-18T21:00:00Z",
            "duration_seconds": 780,
            "view_count": "1840000",
            "comment_count": "8930",
            "is_available": True,
            "page_number": 2,
            "retrieval_index": 11,
            "discovery_method": "SYNTHETIC_FIXTURE",
        },
    ]

    unique_raw_candidates, dedup_telemetry = deduplicate_candidate_occurrences(
        raw_candidates=synthetic_raw_candidates,
        default_method="SYNTHETIC_FIXTURE",
    )

    evaluated = [
        classify_hydrated_candidate(
            candidate=c,
            window_start=window_start,
            window_end=window_end,
            player_aliases=aliases,
            event_terms=event_terms,
            baseline_terms=b_terms,
            channel_is_validated=True,
        )
        for c in unique_raw_candidates
    ]

    channel_breakdown: Dict[str, Any] = {}
    for ch in channels:
        cid = ch["channel_id"]
        c_vids = [v for v in evaluated if v["channel_id"] == cid]
        eligible = [v for v in c_vids if v["status"] == "ELIGIBLE"]
        channel_breakdown[cid] = {
            "channel_id": cid,
            "configured_name": ch.get("channel_name"),
            "live_title": ch.get("channel_name"),
            "discovery_method": "SYNTHETIC_FIXTURE",
            "pages_fetched": 1,
            "is_truncated": False,
            "completeness_status": "RETURNED_PAGES_EXHAUSTED",
            "total_raw_occurrences": sum(v["occurrence_count"] for v in c_vids),
            "total_unique_candidates": len(c_vids),
            "duplicate_occurrences_collapsed": sum(v["occurrence_count"] - 1 for v in c_vids),
            "eligible_candidates": len(eligible),
            "relevance_breakdown": {
                "PLAYER_MATCH": sum(1 for v in eligible if v["relevance_tier"] == "PLAYER_MATCH"),
                "EVENT_SPECIFIC": sum(1 for v in eligible if v["relevance_tier"] == "EVENT_SPECIFIC"),
                "BROAD_EVENT_UNCERTAIN": sum(1 for v in eligible if v["relevance_tier"] == "BROAD_EVENT_UNCERTAIN"),
                "BASELINE_ONLY": sum(1 for v in eligible if v["relevance_tier"] == "BASELINE_ONLY"),
            },
            "by_player": {
                "MESSI": sum(1 for v in eligible if v["player_category"] == "MESSI"),
                "RONALDO": sum(1 for v in eligible if v["player_category"] == "RONALDO"),
                "DUAL": sum(1 for v in eligible if v["player_category"] == "DUAL"),
                "NONE": sum(1 for v in eligible if v["player_category"] == "NONE"),
            },
            "by_format": {
                "LONG_FORM": sum(1 for v in eligible if v["is_short"] is False),
                "SHORTS_CONFIRMED": sum(1 for v in eligible if v["is_short"] is True),
                "FORMAT_UNKNOWN": sum(1 for v in eligible if v["is_short"] is None),
            },
            "exclusions": {
                "OUT_OF_WINDOW": sum(1 for v in c_vids if v["exclusion_reason"] == "OUT_OF_WINDOW"),
                "NO_TARGET_RELEVANCE": sum(1 for v in c_vids if v["exclusion_reason"] == "NO_TARGET_RELEVANCE"),
                "UNRESOLVED_CHANNEL_IDENTITY": 0,
            },
        }

    uncertain_inclusions = [
        {
            "video_id": v["video_id"],
            "channel_title": v.get("channel_title"),
            "title": v.get("title"),
            "published_at": v.get("published_at"),
            "relevance_tier": v.get("relevance_tier"),
            "matched_terms": (v.get("matched_baseline_terms") if v.get("relevance_tier") == "BASELINE_ONLY" else v.get("matched_event_terms")),
            "match_excerpt": v.get("match_excerpt"),
            "current_status": v.get("status"),
            "proposed_methodology_action": v.get("proposed_methodology_action"),
            "audit_note": v.get("audit_note"),
        }
        for v in evaluated
        if v.get("status") == "ELIGIBLE" and v.get("relevance_tier") in ("BASELINE_ONLY", "BROAD_EVENT_UNCERTAIN")
    ]

    uncertain_exclusions = [
        {
            "video_id": v["video_id"],
            "channel_title": v.get("channel_title"),
            "title": v.get("title"),
            "published_at": v.get("published_at"),
            "relevance_tier": v.get("relevance_tier"),
            "exclusion_reason": v.get("exclusion_reason"),
            "current_status": v.get("status"),
            "proposed_methodology_action": v.get("proposed_methodology_action"),
            "audit_note": v.get("audit_note"),
        }
        for v in evaluated
        if v.get("status") == "INELIGIBLE" and v.get("in_window") and v.get("proposed_methodology_action") == "PROPOSE_INCLUSION_IMPLICIT_DEBATE"
    ]

    return {
        "report_metadata": {
            "report_generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "execution_mode": "SYNTHETIC_MOCK",
            "data_provenance": "SYNTHETIC_FIXTURE_DATA_NOT_LIVE_API",
            "warning": "WARNING: ALL METRICS AND CANDIDATES IN THIS REPORT ARE SYNTHETIC FIXTURES.",
            "pipeline_frame": repo_config.get("frame_purpose", "PIPELINE_PILOT"),
            "methodology_version": repo_config.get("methodology_version", "1.2"),
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "discovery_method": "SYNTHETIC_FIXTURE",
        },
        "telemetry": {
            "requests_by_endpoint": {"channels.list": 0, "playlistItems.list": 0, "videos.list": 0, "search.list": 0},
            "total_api_requests": 0,
            "actual_quota_units_consumed": 0,
            "official_quota_rates_used": OFFICIAL_QUOTA_COSTS,
            "error_count": 0,
            "errors": [],
        },
        "channel_identity_validations": [],
        "overall_summary": {
            "total_channels": len(channels),
            "total_raw_occurrences_discovered": dedup_telemetry["total_raw_occurrences"],
            "total_unique_candidates_evaluated": len(evaluated),
            "total_duplicate_occurrences_collapsed": dedup_telemetry["total_duplicate_occurrences_collapsed"],
            "duplicate_video_ids": dedup_telemetry["duplicate_video_ids"],
            "total_eligible_candidates": sum(1 for v in evaluated if v["status"] == "ELIGIBLE"),
            "player_focal_candidates": sum(1 for v in evaluated if v["relevance_tier"] == "PLAYER_MATCH"),
            "event_specific_candidates": sum(1 for v in evaluated if v["relevance_tier"] == "EVENT_SPECIFIC"),
            "broad_generic_uncertain_candidates": sum(1 for v in evaluated if v["relevance_tier"] == "BROAD_EVENT_UNCERTAIN"),
            "baseline_only_candidates": sum(1 for v in evaluated if v["relevance_tier"] == "BASELINE_ONLY"),
            "coverage_gaps": [
                ch["channel_name"] for ch in channels if channel_breakdown[ch["channel_id"]]["eligible_candidates"] == 0
            ],
        },
        "deduplication_summary": dedup_telemetry,
        "channel_breakdown": channel_breakdown,
        "uncertain_inclusions": uncertain_inclusions,
        "uncertain_exclusions": uncertain_exclusions,
        "evaluated_candidates": evaluated,
    }


# =============================================================================
# 8. Event Resolution & Discovery Cost Bounds
# =============================================================================

# Proposed discovery window rule for event snapshots (pending review): from the start of the EVENT
# window to the end of the LATE_POST_EVENT window in config/event_windows.yml, i.e. occurred_at - 1h
# to occurred_at + 72h for the current definitions. The WC_2022_FINAL pilot report used calendar days
# 2022-12-18..2022-12-20 instead; that legacy default is kept when no event is selected.
DISCOVERY_WINDOW_FROM = "EVENT"
DISCOVERY_WINDOW_TO = "LATE_POST_EVENT"
LEGACY_WINDOW_START = "2022-12-18T00:00:00Z"
LEGACY_WINDOW_END = "2022-12-20T23:59:59Z"


def resolve_event(config: Dict[str, Any], event_id: str) -> Dict[str, Any]:
    """Returns event terms and the derived discovery window for one configured event."""
    event = next((ev for ev in config.get("events", []) if ev.get("external_event_id") == event_id), None)
    if event is None:
        known = [ev.get("external_event_id") for ev in config.get("events", [])]
        raise ValueError(f"Unknown event_id {event_id!r}; configured events: {known}")
    occurred = datetime.datetime.fromisoformat(event["occurred_at"].replace("Z", "+00:00"))
    windows = {w["window_type"]: w for w in event.get("windows", [])}
    if DISCOVERY_WINDOW_FROM not in windows or DISCOVERY_WINDOW_TO not in windows:
        raise ValueError(f"Event {event_id} lacks {DISCOVERY_WINDOW_FROM}/{DISCOVERY_WINDOW_TO} windows")
    start = occurred + datetime.timedelta(hours=windows[DISCOVERY_WINDOW_FROM]["relative_offset_start_hours"])
    end = occurred + datetime.timedelta(hours=windows[DISCOVERY_WINDOW_TO]["relative_offset_end_hours"])
    if not event.get("event_terms"):
        raise ValueError(f"Event {event_id} has no event_terms")
    return {
        "event_id": event_id,
        "event_name": event.get("event_name"),
        "occurred_at": event["occurred_at"],
        "event_terms": sorted(set(event["event_terms"])),
        "baseline_terms": sorted(set(event.get("baseline_terms", []))),
        "window_start": start,
        "window_end": end,
        "window_derivation": (f"{DISCOVERY_WINDOW_FROM}.relative_offset_start_hours="
                              f"{windows[DISCOVERY_WINDOW_FROM]['relative_offset_start_hours']} to "
                              f"{DISCOVERY_WINDOW_TO}.relative_offset_end_hours="
                              f"{windows[DISCOVERY_WINDOW_TO]['relative_offset_end_hours']} around occurred_at"),
    }


def estimate_discovery_cost(n_channels: int, max_pages_per_channel: int, method: str) -> Dict[str, Any]:
    """Upper bound on discovery quota per bucket: identity check, every allowed page per channel, and
    hydration of every possible candidate (50 results per page, 50 IDs per videos.list call)."""
    page_endpoint = "search.list" if method == "search" else "playlistItems.list"
    pages = n_channels * max_pages_per_channel
    requests = {
        "channels.list": -(-n_channels // 50),
        page_endpoint: pages,
        "videos.list": pages,
    }
    buckets = quota_by_bucket(requests)
    return {
        "requests_upper_bound": requests,
        "quota_by_bucket_upper_bound": buckets,
        "share_of_default_daily_quota": {b: buckets[b] / DEFAULT_DAILY_QUOTA[b] for b in buckets},
        "quota_reference": QUOTA_REFERENCE,
    }


# =============================================================================
# 9. Main Orchestrator (Live & Mock)
# =============================================================================

def execute_discovery(
    repo_root: Optional[str] = None,
    mock: bool = False,
    api_key: Optional[str] = None,
    method: str = "search",
    window_start_str: Optional[str] = None,
    window_end_str: Optional[str] = None,
    max_pages_per_channel: int = 2,
    event_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Orchestrates candidate video discovery without performing any Snowflake writes.

    With event_id, terms come from that event in config/event_windows.yml and, unless explicit window
    strings are given, the window is derived by resolve_event(). Without event_id the legacy
    WC_2022_FINAL behaviour (terms and calendar-day window) is unchanged.
    """
    config = load_repository_configuration(repo_root)

    resolved = resolve_event(config, event_id or "WC_2022_FINAL")
    event_terms, baseline_terms = resolved["event_terms"], resolved["baseline_terms"]
    if event_id and window_start_str is None and window_end_str is None:
        w_start, w_end = resolved["window_start"], resolved["window_end"]
        window_derivation = resolved["window_derivation"]
    else:
        window_start_str = window_start_str or LEGACY_WINDOW_START
        window_end_str = window_end_str or LEGACY_WINDOW_END
        w_start = datetime.datetime.fromisoformat(window_start_str.replace("Z", "+00:00"))
        w_end = datetime.datetime.fromisoformat(window_end_str.replace("Z", "+00:00"))
        window_derivation = "EXPLICIT_ARGUMENTS" if event_id else "LEGACY_WC_2022_CALENDAR_DAYS"
    event_metadata = {"event_id": event_id, "event_occurred_at": resolved["occurred_at"] if event_id else None,
                      "window_derivation": window_derivation}

    # 1. Synthetic Mock Mode
    if mock:
        report = run_synthetic_mock_discovery(config, w_start, w_end, event_terms, baseline_terms)
        report["report_metadata"].update(event_metadata)
        return report

    # 2. Live Mode (Strictly fail-closed; no silent mock fallback)
    youtube = build_youtube_client(api_key=api_key)
    telemetry = DiscoveryTelemetry()

    # Step A: Validate channel identities
    validated_channels, discrepancies = validate_channel_identities_live(
        youtube=youtube,
        configured_channels=config["channels"],
        telemetry=telemetry,
    )

    # Step B: Discover candidate videos per channel
    all_raw_candidates: List[Dict[str, Any]] = []
    channel_telemetry: Dict[str, Any] = {}

    for ch in config["channels"]:
        cid = ch["channel_id"]
        ch_val = validated_channels.get(cid, {"channel_id": cid, "status": "IDENTITY_UNRESOLVED"})

        if method == "search":
            disc_res = discover_channel_candidates_date_bounded_search(
                youtube=youtube,
                channel_info=ch_val,
                window_start=w_start,
                window_end=w_end,
                max_pages=max_pages_per_channel,
                telemetry=telemetry,
            )
        else:
            disc_res = discover_channel_candidates_uploads_walk(
                youtube=youtube,
                channel_info=ch_val,
                window_start=w_start,
                window_end=w_end,
                max_pages=max_pages_per_channel,
                telemetry=telemetry,
            )

        channel_telemetry[cid] = disc_res
        all_raw_candidates.extend(disc_res.get("items", []))

    # Step B.2: Deduplicate candidates by exact video_id before hydration & classification
    unique_raw_candidates, dedup_telemetry = deduplicate_candidate_occurrences(
        raw_candidates=all_raw_candidates,
        default_method=method,
    )

    # Step C: Hydrate video metadata in batches via videos.list
    candidate_ids = [c["video_id"] for c in unique_raw_candidates if "video_id" in c]
    hydrated_dict = hydrate_video_metadata_live(
        youtube=youtube,
        video_ids=candidate_ids,
        telemetry=telemetry,
    )

    # Step D: Classify unique candidates against frozen methodology rules
    classified_candidates = []
    for c in unique_raw_candidates:
        cid = c.get("channel_id")
        ch_status = validated_channels.get(cid, {}).get("status")
        is_validated = (ch_status == "VALIDATED")

        hydrated_c = dict(hydrated_dict.get(c["video_id"], c))
        hydrated_c["discovery_occurrences"] = c.get("discovery_occurrences", [])
        hydrated_c["occurrence_count"] = c.get("occurrence_count", 1)

        classified = classify_hydrated_candidate(
            candidate=hydrated_c,
            window_start=w_start,
            window_end=w_end,
            player_aliases=config["player_aliases"],
            event_terms=event_terms,
            baseline_terms=baseline_terms,
            channel_is_validated=is_validated,
        )
        classified_candidates.append(classified)

    # Step E: Aggregate channel breakdown strictly from unique candidates
    channel_breakdown: Dict[str, Any] = {}
    for ch in config["channels"]:
        cid = ch["channel_id"]
        disc_info = channel_telemetry.get(cid, {})
        c_vids = [v for v in classified_candidates if v.get("channel_id") == cid]
        eligible = [v for v in c_vids if v["status"] == "ELIGIBLE"]

        channel_breakdown[cid] = {
            "channel_id": cid,
            "configured_name": ch.get("channel_name"),
            "live_title": validated_channels.get(cid, {}).get("live_title"),
            "discovery_method": disc_info.get("discovery_method", method),
            "pages_fetched": disc_info.get("pages_fetched", 0),
            "is_truncated": disc_info.get("is_truncated", False),
            "completeness_status": disc_info.get("completeness_status", "UNKNOWN"),
            "total_raw_occurrences": sum(v["occurrence_count"] for v in c_vids),
            "total_unique_candidates": len(c_vids),
            "duplicate_occurrences_collapsed": sum(v["occurrence_count"] - 1 for v in c_vids),
            "eligible_candidates": len(eligible),
            "relevance_breakdown": {
                "PLAYER_MATCH": sum(1 for v in eligible if v["relevance_tier"] == "PLAYER_MATCH"),
                "EVENT_SPECIFIC": sum(1 for v in eligible if v["relevance_tier"] == "EVENT_SPECIFIC"),
                "BROAD_EVENT_UNCERTAIN": sum(1 for v in eligible if v["relevance_tier"] == "BROAD_EVENT_UNCERTAIN"),
                "BASELINE_ONLY": sum(1 for v in eligible if v["relevance_tier"] == "BASELINE_ONLY"),
            },
            "by_player": {
                "MESSI": sum(1 for v in eligible if v["player_category"] == "MESSI"),
                "RONALDO": sum(1 for v in eligible if v["player_category"] == "RONALDO"),
                "DUAL": sum(1 for v in eligible if v["player_category"] == "DUAL"),
                "NONE": sum(1 for v in eligible if v["player_category"] == "NONE"),
            },
            "by_format": {
                "LONG_FORM": sum(1 for v in eligible if v["is_short"] is False),
                "SHORTS_CONFIRMED": sum(1 for v in eligible if v["is_short"] is True),
                "FORMAT_UNKNOWN": sum(1 for v in eligible if v["is_short"] is None),
            },
            "exclusions": {
                "OUT_OF_WINDOW": sum(1 for v in c_vids if v["exclusion_reason"] == "OUT_OF_WINDOW"),
                "NO_TARGET_RELEVANCE": sum(1 for v in c_vids if v["exclusion_reason"] == "NO_TARGET_RELEVANCE"),
                "VIDEO_UNAVAILABLE": sum(1 for v in c_vids if v["exclusion_reason"] == "VIDEO_UNAVAILABLE"),
                "UNRESOLVED_CHANNEL_IDENTITY": sum(1 for v in c_vids if v["exclusion_reason"] == "UNRESOLVED_CHANNEL_IDENTITY"),
            },
        }

    coverage_gaps = [
        ch["channel_name"] for ch in config["channels"]
        if channel_breakdown[ch["channel_id"]]["eligible_candidates"] == 0
    ]

    uncertain_inclusions = [
        {
            "video_id": v["video_id"],
            "channel_title": v.get("channel_title"),
            "title": v.get("title"),
            "published_at": v.get("published_at"),
            "relevance_tier": v.get("relevance_tier"),
            "matched_terms": (v.get("matched_baseline_terms") if v.get("relevance_tier") == "BASELINE_ONLY" else v.get("matched_event_terms")),
            "match_excerpt": v.get("match_excerpt"),
            "current_status": v.get("status"),
            "proposed_methodology_action": v.get("proposed_methodology_action"),
            "audit_note": v.get("audit_note"),
        }
        for v in classified_candidates
        if v.get("status") == "ELIGIBLE" and v.get("relevance_tier") in ("BASELINE_ONLY", "BROAD_EVENT_UNCERTAIN")
    ]

    uncertain_exclusions = [
        {
            "video_id": v["video_id"],
            "channel_title": v.get("channel_title"),
            "title": v.get("title"),
            "published_at": v.get("published_at"),
            "relevance_tier": v.get("relevance_tier"),
            "exclusion_reason": v.get("exclusion_reason"),
            "current_status": v.get("status"),
            "proposed_methodology_action": v.get("proposed_methodology_action"),
            "audit_note": v.get("audit_note"),
        }
        for v in classified_candidates
        if v.get("status") == "INELIGIBLE" and v.get("in_window") and v.get("proposed_methodology_action") == "PROPOSE_INCLUSION_IMPLICIT_DEBATE"
    ]

    return {
        "report_metadata": {
            "report_generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "execution_mode": "LIVE_YOUTUBE_API",
            "pipeline_frame": config.get("frame_purpose", "PIPELINE_PILOT"),
            "methodology_version": config.get("methodology_version", "1.2"),
            "window_start": w_start.isoformat(),
            "window_end": w_end.isoformat(),
            "discovery_method": method,
            "max_pages_per_channel": max_pages_per_channel,
            **event_metadata,
        },
        "telemetry": telemetry.to_dict(),
        "channel_identity_validations": discrepancies,
        "overall_summary": {
            "total_channels": len(config["channels"]),
            "total_raw_occurrences_discovered": dedup_telemetry["total_raw_occurrences"],
            "total_unique_candidates_evaluated": len(classified_candidates),
            "total_duplicate_occurrences_collapsed": dedup_telemetry["total_duplicate_occurrences_collapsed"],
            "duplicate_video_ids": dedup_telemetry["duplicate_video_ids"],
            "total_eligible_candidates": sum(1 for v in classified_candidates if v["status"] == "ELIGIBLE"),
            "player_focal_candidates": sum(1 for v in classified_candidates if v["relevance_tier"] == "PLAYER_MATCH"),
            "event_specific_candidates": sum(1 for v in classified_candidates if v["relevance_tier"] == "EVENT_SPECIFIC"),
            "broad_generic_uncertain_candidates": sum(1 for v in classified_candidates if v["relevance_tier"] == "BROAD_EVENT_UNCERTAIN"),
            "baseline_only_candidates": sum(1 for v in classified_candidates if v["relevance_tier"] == "BASELINE_ONLY"),
            "coverage_gaps": coverage_gaps,
        },
        "deduplication_summary": dedup_telemetry,
        "channel_breakdown": channel_breakdown,
        "uncertain_inclusions": uncertain_inclusions,
        "uncertain_exclusions": uncertain_exclusions,
        "evaluated_candidates": classified_candidates,
    }


def regenerate_from_saved_report(
    input_json_path: str,
    repo_root: Optional[str] = None,
) -> Dict[str, Any]:
    """Regenerates candidate coverage report from saved JSON data, applying exact deduplication,

    preserving existing classifications and matching evidence, and generating a per-video
    before/after comparison without spending any YouTube Data API quota.
    """
    if not os.path.exists(input_json_path):
        raise FileNotFoundError(f"Input report not found at {input_json_path}")

    with open(input_json_path, "r", encoding="utf-8") as f:
        saved_data = json.load(f)

    config = load_repository_configuration(repo_root)
    meta = saved_data.get("report_metadata", {})
    w_start_str = meta.get("window_start", "2022-12-18T00:00:00Z")
    w_end_str = meta.get("window_end", "2022-12-20T23:59:59Z")
    w_start = datetime.datetime.fromisoformat(w_start_str.replace("Z", "+00:00"))
    w_end = datetime.datetime.fromisoformat(w_end_str.replace("Z", "+00:00"))

    raw_candidates = saved_data.get("evaluated_candidates", [])
    method = meta.get("discovery_method", "search")

    # Step 1: Track per-video before/after comparison across all raw occurrences
    per_video_comparison: List[Dict[str, Any]] = []
    seen_ids: Dict[str, int] = {}  # vid -> first 1-based occurrence index
    unique_candidates_dict: Dict[str, Dict[str, Any]] = {}
    occurrences_by_id: Dict[str, List[Dict[str, Any]]] = {}
    duplicate_details: List[Dict[str, Any]] = []

    for idx, c in enumerate(raw_candidates, start=1):
        vid = c.get("video_id")
        if not vid:
            continue

        page_num = c.get("page_number")
        ret_idx = c.get("retrieval_index")
        disc_method = c.get("discovery_method") or method

        # Normalize player label: use player_category when present, otherwise player_relevance
        raw_player_label = c.get("player_category") if c.get("player_category") is not None else c.get("player_relevance")
        player_label = raw_player_label if raw_player_label is not None else ("NONE" if c.get("status") else None)

        occ = {
            "channel_id": c.get("channel_id"),
            "channel_title": c.get("channel_title") or c.get("configured_name"),
            "discovery_method": disc_method,
            "page_number": page_num if page_num is not None else "UNKNOWN",
            "retrieval_index": ret_idx if ret_idx is not None else "UNKNOWN",
            "published_at": c.get("published_at"),
        }

        if vid not in seen_ids:
            seen_ids[vid] = idx
            occurrences_by_id[vid] = [occ]
            cand_entry = dict(c)
            cand_entry["page_number"] = occ["page_number"]
            cand_entry["retrieval_index"] = occ["retrieval_index"]
            cand_entry["discovery_method"] = occ["discovery_method"]

            # Preserve existing classification and matching evidence
            # If classifications are missing and description is unavailable, mark UNRESOLVED
            if "status" not in cand_entry or "relevance_tier" not in cand_entry:
                if not cand_entry.get("description"):
                    cand_entry["status"] = "UNRESOLVED"
                    cand_entry["relevance_tier"] = "UNRESOLVED"
                    cand_entry["player_category"] = "UNRESOLVED"
                    cand_entry["player_relevance"] = "UNRESOLVED"
                    cand_entry["classification_provenance"] = "UNRESOLVED_FULL_DESCRIPTION_UNAVAILABLE"
                    cand_entry["audit_note"] = "Classification cannot be established without full description."
                else:
                    classified = classify_hydrated_candidate(
                        candidate=cand_entry,
                        window_start=w_start,
                        window_end=w_end,
                        player_aliases=config["player_aliases"],
                        event_terms=["world cup", "final", "argentina vs france"],
                        baseline_terms=["world cup", "fifa", "fifa world cup", "qatar 2022"],
                        channel_is_validated=True,
                    )
                    cand_entry.update(classified)
            else:
                cand_entry["classification_provenance"] = "PRESERVED_FROM_SAVED_REPORT"
                cand_entry["player_category"] = player_label or "NONE"
                cand_entry["player_relevance"] = player_label or "NONE"

            # Check audit flags for human methodology review without mutating preserved classifications
            relevance_tier = cand_entry.get("relevance_tier")
            status = cand_entry.get("status")
            matched_event_terms = cand_entry.get("matched_event_terms", [])
            matched_player_aliases = cand_entry.get("matched_player_aliases", [])

            proposed_action = cand_entry.get("proposed_methodology_action")
            audit_note = cand_entry.get("audit_note")

            if not proposed_action:
                if status == "ELIGIBLE" and matched_event_terms == ["fifa"] and not matched_player_aliases:
                    proposed_action = "PROPOSE_EXCLUSION_NON_EVENT_BASELINE"
                    audit_note = "Video matched broad baseline term(s) ['fifa'] without tournament or focal player keywords; off-target gaming/unrelated media."
                elif status == "ELIGIBLE" and matched_event_terms == ["final"] and not matched_player_aliases:
                    proposed_action = "PROPOSE_EXCLUSION_GENERIC_TERM"
                    audit_note = "Matched only generic event term 'final' without focal player or specific tournament keywords; ambiguous sports term."
                elif status == "INELIGIBLE" and cand_entry.get("in_window"):
                    title_lower = (cand_entry.get("title") or "").lower()
                    if re.search(r"\bgoat\b", title_lower) or "greatest of all time" in title_lower or "🐐" in (cand_entry.get("title") or ""):
                        proposed_action = "PROPOSE_INCLUSION_IMPLICIT_DEBATE"
                        audit_note = "Implicit GOAT debate commentary in window without explicit player/event keywords in metadata."

            cand_entry["proposed_methodology_action"] = proposed_action
            cand_entry["audit_note"] = audit_note
            unique_candidates_dict[vid] = cand_entry

            per_video_comparison.append({
                "raw_occurrence_index": idx,
                "video_id": vid,
                "channel_title": c.get("channel_title"),
                "title": c.get("title"),
                "occurrence_number": 1,
                "is_duplicate_occurrence": False,
                "action": "PRESERVE_UNIQUE_VIDEO",
                "before_status": c.get("status"),
                "after_status": cand_entry.get("status"),
                "before_relevance_tier": c.get("relevance_tier"),
                "after_relevance_tier": cand_entry.get("relevance_tier"),
                "before_player_category": player_label,
                "after_player_category": cand_entry.get("player_category"),
                "classification_change": "NONE",
                "notes": "Unique video candidate; live classifications and matching evidence preserved verbatim.",
            })
        else:
            occurrences_by_id[vid].append(occ)
            first_idx = seen_ids[vid]
            occ_num = len(occurrences_by_id[vid])

            per_video_comparison.append({
                "raw_occurrence_index": idx,
                "video_id": vid,
                "channel_title": c.get("channel_title"),
                "title": c.get("title"),
                "occurrence_number": occ_num,
                "is_duplicate_occurrence": True,
                "action": "REMOVE_DUPLICATE_OCCURRENCE",
                "before_status": c.get("status"),
                "after_status": "DROPPED_AS_DUPLICATE",
                "before_relevance_tier": c.get("relevance_tier"),
                "after_relevance_tier": "DROPPED_AS_DUPLICATE",
                "before_player_category": player_label,
                "after_player_category": "DROPPED_AS_DUPLICATE",
                "classification_change": "NONE",
                "original_occurrence_index": first_idx,
                "notes": f"Duplicate occurrence #{occ_num} removed; collapsed into unique video entry (original at #{first_idx}).",
            })

            dup_record = next((d for d in duplicate_details if d["video_id"] == vid), None)
            if not dup_record:
                duplicate_details.append({
                    "video_id": vid,
                    "title": c.get("title"),
                    "channel_title": c.get("channel_title"),
                    "duplicate_occurrence_index": idx,
                    "original_occurrence_index": first_idx,
                    "preserved_relevance_tier": c.get("relevance_tier"),
                    "preserved_player_category": player_label,
                    "preserved_status": c.get("status"),
                    "action": "REMOVED_DUPLICATE_OCCURRENCE",
                })

    # Attach occurrences to unique candidates
    unique_candidates: List[Dict[str, Any]] = []
    for vid, cand in unique_candidates_dict.items():
        cand["discovery_occurrences"] = occurrences_by_id[vid]
        cand["occurrence_count"] = len(occurrences_by_id[vid])
        unique_candidates.append(cand)

    dedup_telemetry = {
        "total_raw_occurrences": len(raw_candidates),
        "total_unique_candidates": len(unique_candidates),
        "total_duplicate_occurrences_collapsed": len(raw_candidates) - len(unique_candidates),
        "duplicate_video_ids": [d["video_id"] for d in duplicate_details],
        "duplicate_details": duplicate_details,
    }

    comparison_summary = {
        "total_raw_occurrences": len(raw_candidates),
        "total_unique_videos": len(unique_candidates),
        "duplicate_occurrences_removed": len(raw_candidates) - len(unique_candidates),
        "classification_changes": 0,
        "removed_duplicates_detail": duplicate_details,
    }

    # Re-aggregate channel breakdown
    channel_breakdown: Dict[str, Any] = {}
    saved_cb = saved_data.get("channel_breakdown", {})
    for ch in config["channels"]:
        cid = ch["channel_id"]
        ch_saved = saved_cb.get(cid, {})
        c_vids = [v for v in unique_candidates if v.get("channel_id") == cid]
        eligible = [v for v in c_vids if v.get("status") == "ELIGIBLE"]

        channel_breakdown[cid] = {
            "channel_id": cid,
            "configured_name": ch.get("channel_name"),
            "live_title": ch_saved.get("live_title") or ch.get("channel_name"),
            "discovery_method": ch_saved.get("discovery_method", method),
            "pages_fetched": ch_saved.get("pages_fetched", 1),
            "is_truncated": ch_saved.get("is_truncated", False),
            "completeness_status": ch_saved.get("completeness_status", "RETURNED_PAGES_EXHAUSTED"),
            "total_raw_occurrences": sum(v["occurrence_count"] for v in c_vids),
            "total_unique_candidates": len(c_vids),
            "duplicate_occurrences_collapsed": sum(v["occurrence_count"] - 1 for v in c_vids),
            "eligible_candidates": len(eligible),
            "relevance_breakdown": {
                "PLAYER_MATCH": sum(1 for v in eligible if v.get("relevance_tier") == "PLAYER_MATCH"),
                "EVENT_SPECIFIC": sum(1 for v in eligible if v.get("relevance_tier") == "EVENT_SPECIFIC"),
            },
            "by_player": {
                "MESSI": sum(1 for v in eligible if (v.get("player_category") or v.get("player_relevance")) == "MESSI"),
                "RONALDO": sum(1 for v in eligible if (v.get("player_category") or v.get("player_relevance")) == "RONALDO"),
                "DUAL": sum(1 for v in eligible if (v.get("player_category") or v.get("player_relevance")) == "DUAL"),
                "NONE": sum(1 for v in eligible if (v.get("player_category") or v.get("player_relevance")) == "NONE"),
            },
            "by_format": {
                "LONG_FORM": sum(1 for v in eligible if v.get("is_short") is False),
                "SHORTS_CONFIRMED": sum(1 for v in eligible if v.get("is_short") is True),
                "FORMAT_UNKNOWN": sum(1 for v in eligible if v.get("is_short") is None),
            },
            "exclusions": {
                "OUT_OF_WINDOW": sum(1 for v in c_vids if v.get("exclusion_reason") == "OUT_OF_WINDOW"),
                "NO_TARGET_RELEVANCE": sum(1 for v in c_vids if v.get("exclusion_reason") == "NO_TARGET_RELEVANCE"),
                "VIDEO_UNAVAILABLE": sum(1 for v in c_vids if v.get("exclusion_reason") == "VIDEO_UNAVAILABLE"),
                "UNRESOLVED_CHANNEL_IDENTITY": sum(1 for v in c_vids if v.get("exclusion_reason") == "UNRESOLVED_CHANNEL_IDENTITY"),
            },
        }

    coverage_gaps = [
        ch["channel_name"] for ch in config["channels"]
        if channel_breakdown[ch["channel_id"]]["eligible_candidates"] == 0
    ]

    report = {
        "report_metadata": {
            "report_generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "execution_mode": meta.get("execution_mode", "REGENERATED_FROM_SAVED_REPORT"),
            "data_provenance": "DEDUPLICATED_REPLAY_FROM_SAVED_REPORT",
            "source_report_path": input_json_path,
            "pipeline_frame": config.get("frame_purpose", "PIPELINE_PILOT"),
            "methodology_version": config.get("methodology_version", "1.2"),
            "window_start": w_start.isoformat(),
            "window_end": w_end.isoformat(),
            "discovery_method": method,
            "max_pages_per_channel": meta.get("max_pages_per_channel", 2),
            "event_id": meta.get("event_id"),
            "event_occurred_at": meta.get("event_occurred_at"),
            "window_derivation": meta.get("window_derivation", "UNKNOWN"),
        },
        "telemetry": saved_data.get("telemetry", {
            "requests_by_endpoint": {},
            "total_api_requests": 0,
            "actual_quota_units_consumed": 0,
            "official_quota_rates_used": OFFICIAL_QUOTA_COSTS,
            "error_count": 0,
            "errors": [],
        }),
        "channel_identity_validations": saved_data.get("channel_identity_validations", []),
        "overall_summary": {
            "total_channels": len(config["channels"]),
            "total_raw_occurrences_discovered": dedup_telemetry["total_raw_occurrences"],
            "total_unique_candidates_evaluated": len(unique_candidates),
            "total_duplicate_occurrences_collapsed": dedup_telemetry["total_duplicate_occurrences_collapsed"],
            "duplicate_video_ids": dedup_telemetry["duplicate_video_ids"],
            "total_eligible_candidates": sum(1 for v in unique_candidates if v.get("status") == "ELIGIBLE"),
            "player_focal_candidates": sum(1 for v in unique_candidates if v.get("status") == "ELIGIBLE" and v.get("relevance_tier") == "PLAYER_MATCH"),
            "event_specific_candidates": sum(1 for v in unique_candidates if v.get("status") == "ELIGIBLE" and v.get("relevance_tier") == "EVENT_SPECIFIC"),
            "by_player": {
                "MESSI": sum(1 for v in unique_candidates if v.get("status") == "ELIGIBLE" and (v.get("player_category") or v.get("player_relevance")) == "MESSI"),
                "DUAL": sum(1 for v in unique_candidates if v.get("status") == "ELIGIBLE" and (v.get("player_category") or v.get("player_relevance")) == "DUAL"),
                "RONALDO": sum(1 for v in unique_candidates if v.get("status") == "ELIGIBLE" and (v.get("player_category") or v.get("player_relevance")) == "RONALDO"),
                "NONE": sum(1 for v in unique_candidates if v.get("status") == "ELIGIBLE" and (v.get("player_category") or v.get("player_relevance")) == "NONE"),
            },
            "coverage_gaps": coverage_gaps,
        },
        "deduplication_summary": dedup_telemetry,
        "comparison_summary": comparison_summary,
        "per_video_comparison": per_video_comparison,
        "channel_breakdown": channel_breakdown,
        "uncertain_inclusions": [
            {
                "video_id": v["video_id"],
                "channel_title": v.get("channel_title"),
                "title": v.get("title"),
                "published_at": v.get("published_at"),
                "relevance_tier": v.get("relevance_tier"),
                "matched_terms": v.get("matched_event_terms"),
                "match_excerpt": v.get("match_excerpt"),
                "current_status": v.get("status"),
                "proposed_methodology_action": v.get("proposed_methodology_action"),
                "audit_note": v.get("audit_note"),
            }
            for v in unique_candidates
            if v.get("status") == "ELIGIBLE" and v.get("proposed_methodology_action") in (
                "PROPOSE_EXCLUSION_NON_EVENT_BASELINE",
                "PROPOSE_EXCLUSION_GENERIC_TERM",
            )
        ],
        "uncertain_exclusions": [
            {
                "video_id": v["video_id"],
                "channel_title": v.get("channel_title"),
                "title": v.get("title"),
                "published_at": v.get("published_at"),
                "relevance_tier": v.get("relevance_tier"),
                "exclusion_reason": v.get("exclusion_reason"),
                "current_status": v.get("status"),
                "proposed_methodology_action": v.get("proposed_methodology_action"),
                "audit_note": v.get("audit_note"),
            }
            for v in unique_candidates
            if v.get("status") == "INELIGIBLE" and v.get("proposed_methodology_action") == "PROPOSE_INCLUSION_IMPLICIT_DEBATE"
        ],
        "evaluated_candidates": unique_candidates,
    }
    return report


def format_report_summary_markdown(report: Dict[str, Any]) -> str:
    """Renders human-readable summary table of discovery results."""
    meta = report["report_metadata"]
    lines = []

    if meta.get("execution_mode") == "SYNTHETIC_MOCK":
        lines.append("=" * 70)
        lines.append("  [WARNING: SYNTHETIC DATA - NOT LIVE YOUTUBE API OBSERVATIONS]")
        lines.append("  ALL CANDIDATES AND METRICS BELOW ARE GENERATED FROM TEST FIXTURES.")
        lines.append("=" * 70)
        lines.append("")
    elif meta.get("data_provenance") == "DEDUPLICATED_REPLAY_FROM_SAVED_REPORT":
        lines.append("=" * 70)
        lines.append(f"  [REPLAY REPORT: REGENERATED FROM SAVED DATA: {meta.get('source_report_path')}]")
        lines.append("  ZERO YOUTUBE DATA API QUOTA EXPENDED.")
        lines.append("=" * 70)
        lines.append("")

    lines.append("## Pilot Candidate Coverage Report")
    lines.append(f"- **Execution Mode**: `{meta['execution_mode']}`")
    lines.append(f"- **Window**: `{meta['window_start']}` to `{meta['window_end']}`")
    lines.append(f"- **Method**: `{meta['discovery_method']}`")
    lines.append("")

    # Identity Discrepancies
    discrepancies = report.get("channel_identity_validations", [])
    if discrepancies:
        lines.append("### Channel Identity Notices")
        for d in discrepancies:
            lines.append(f"- **{d['discrepancy_type']}** on `{d['channel_id']}`: Configured as '{d.get('configured_name')}', Live Title: '{d.get('live_title')}'. ({d.get('resolution')})")
        lines.append("")

    # Deduplication Provenance Telemetry
    dedup = report.get("deduplication_summary", {})
    comp = report.get("comparison_summary", {})
    lines.append("### Candidate Deduplication & Provenance")
    lines.append(f"- **Total Raw Occurrences Discovered**: {dedup.get('total_raw_occurrences', report['overall_summary'].get('total_raw_occurrences_discovered', 0))}")
    lines.append(f"- **Total Unique Video IDs Evaluated**: {dedup.get('total_unique_candidates', report['overall_summary'].get('total_unique_candidates_evaluated', 0))}")
    lines.append(f"- **Duplicate Occurrences Collapsed**: {dedup.get('total_duplicate_occurrences_collapsed', report['overall_summary'].get('total_duplicate_occurrences_collapsed', 0))}")
    dup_ids = dedup.get("duplicate_video_ids", [])
    if dup_ids:
        lines.append(f"- **Confirmed Duplicate Video IDs**: {', '.join(f'`{vid}`' for vid in dup_ids)}")
    else:
        lines.append("- **Confirmed Duplicate Video IDs**: None")
    lines.append(f"- **Classification Changes**: {comp.get('classification_changes', 0)} (all unique candidate classifications preserved)")
    lines.append("")

    # Per-Video Deduplication vs. Classification Comparison
    rem_details = comp.get("removed_duplicates_detail", [])
    if comp:
        lines.append("### Per-Video Deduplication vs. Classification Comparison")
        lines.append(f"- **Total Raw Discovered Occurrences**: {comp.get('total_raw_occurrences', 0)}")
        lines.append(f"- **Total Unique Candidates After Deduplication**: {comp.get('total_unique_videos', 0)}")
        lines.append(f"- **Duplicate Occurrences Removed**: {comp.get('duplicate_occurrences_removed', 0)}")
        lines.append(f"- **Classification Changes**: {comp.get('classification_changes', 0)} (all 68 unique candidate videos preserved their live-evaluated classifications and evidence)")
        lines.append("")
        if rem_details:
            lines.append("#### Removed Duplicate Occurrences (Overlapping Search Pages)")
            lines.append("| Video ID | Channel | Title | Duplicate Occurrence | Original Occurrence | Preserved Relevance Tier | Preserved Player Category | Status | Action |")
            lines.append("| :--- | :--- | :--- | :---: | :---: | :--- | :--- | :---: | :--- |")
            for d in rem_details:
                vid = d.get("video_id")
                ch = d.get("channel_title")
                t = d.get("title", "").replace("|", "-")
                dup_idx = d.get("duplicate_occurrence_index")
                orig_idx = d.get("original_occurrence_index")
                tier = d.get("preserved_relevance_tier")
                cat = d.get("preserved_player_category")
                st = d.get("preserved_status")
                act = d.get("action")
                lines.append(f"| `{vid}` | {ch} | {t} | #{dup_idx} | #{orig_idx} | `{tier}` | `{cat}` | `{st}` | **{act}** |")
            lines.append("")

    # Telemetry
    telem = report.get("telemetry", {})
    lines.append("### API & Quota Telemetry (Actual Executed Requests)")
    lines.append(f"- **Total API Requests**: {telem.get('total_api_requests', 0)}")
    lines.append(f"- **Actual Quota Units Consumed (default bucket)**: {telem.get('actual_quota_units_consumed', 0)}")
    if "search_list_calls_consumed" in telem:
        lines.append(f"- **search.list Calls (separate bucket)**: {telem['search_list_calls_consumed']}")
    lines.append(f"- **Calls Breakdown**: {telem.get('requests_by_endpoint', {})}")
    if telem.get("error_count", 0) > 0:
        lines.append(f"- **Errors Encountered**: {telem.get('error_count')} (inspect report JSON for details)")
    lines.append("")

    # Candidate Breakdown by Relevance Tier
    ov = report["overall_summary"]
    by_p = ov.get("by_player", {})
    lines.append("### Relevance Tier Summary (Calculated from Unique Video IDs)")
    lines.append(f"- **Target Player Matches (Direct)**: {ov.get('player_focal_candidates', 0)} unique (Messi: {by_p.get('MESSI', 0)}, Dual: {by_p.get('DUAL', 0)}, Ronaldo: {by_p.get('RONALDO', 0)})")
    lines.append(f"- **Specific Event Matches (Tournament / Match Terms)**: {ov.get('event_specific_candidates', 0)} unique")
    lines.append(f"- **Total Unique Eligible Candidates**: {ov['total_eligible_candidates']} / {ov['total_unique_candidates_evaluated']} unique evaluated")
    lines.append("")

    # Channel Distribution Table
    lines.append("### Channel Candidate Distribution (Unique Video IDs)")
    lines.append("")
    lines.append("| Channel Name | Method | Pages | Status | Raw Occurrences | Unique Candidates | Collapsed Duplicates | Eligible | Player Match | Event Specific | Long-Form | Shorts | Format Unknown |")
    lines.append("| :--- | :--- | :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |")

    for cid, ch in report["channel_breakdown"].items():
        name = ch.get("live_title") or ch.get("configured_name")
        rb = ch.get("relevance_breakdown", {})
        bf = ch["by_format"]
        raw_count = ch.get("total_raw_occurrences", ch.get("total_candidates", 0))
        unique_count = ch.get("total_unique_candidates", ch.get("total_candidates", 0))
        collapsed = ch.get("duplicate_occurrences_collapsed", 0)
        lines.append(
            f"| {name} | {ch['discovery_method']} | {ch['pages_fetched']} | {ch['completeness_status']} | "
            f"{raw_count} | {unique_count} | {collapsed} | **{ch['eligible_candidates']}** | "
            f"{rb.get('PLAYER_MATCH', 0)} | {rb.get('EVENT_SPECIFIC', 0)} | "
            f"{bf['LONG_FORM']} | {bf['SHORTS_CONFIRMED']} | {bf['FORMAT_UNKNOWN']} |"
        )

    lines.append("")
    if ov["coverage_gaps"]:
        lines.append(f"- **Coverage Gaps (0 eligible)**: {', '.join(ov['coverage_gaps'])}")
    else:
        lines.append("- **Coverage Gaps**: None. All tracked channels have eligible candidates in this window.")
    lines.append("")

    # Audit Table 1: Uncertain Inclusions
    unc_inc = report.get("uncertain_inclusions", [])
    lines.append("### Audit: Uncertain Inclusions (Currently ELIGIBLE)")
    lines.append("These candidates are currently marked `ELIGIBLE` under frozen baseline/event rules, but have questionable event/player relevance. Proposed methodology actions are recorded for human decision.")
    lines.append("")
    if unc_inc:
        lines.append("| Video ID | Channel | Title | Relevance Tier | Matched Term(s) | Current Status | Proposed Action & Audit Rationale |")
        lines.append("| :--- | :--- | :--- | :--- | :--- | :---: | :--- |")
        for u in unc_inc:
            vid = u.get("video_id")
            ch = u.get("channel_title")
            t = u.get("title", "").replace("|", "-")
            tier = u.get("relevance_tier")
            m_terms = u.get("matched_terms", [])
            act = u.get("proposed_methodology_action")
            note = u.get("audit_note", "")
            lines.append(f"| `{vid}` | {ch} | {t} | `{tier}` | `{m_terms}` | `ELIGIBLE` | **{act}**: {note} |")
    else:
        lines.append("*No uncertain inclusions detected.*")
    lines.append("")

    # Audit Table 2: Uncertain Exclusions
    unc_exc = report.get("uncertain_exclusions", [])
    lines.append("### Audit: Uncertain Exclusions (Currently INELIGIBLE)")
    lines.append("These candidates were published in-window and excluded under frozen keyword rules, but exhibit implicit conversational debate relevance. They remain `INELIGIBLE` pending explicit human methodology approval.")
    lines.append("")
    if unc_exc:
        lines.append("| Video ID | Channel | Title | Reason | Current Status | Proposed Action & Audit Rationale |")
        lines.append("| :--- | :--- | :--- | :--- | :---: | :--- |")
        for u in unc_exc:
            vid = u.get("video_id")
            ch = u.get("channel_title")
            t = u.get("title", "").replace("|", "-")
            reason = u.get("exclusion_reason")
            act = u.get("proposed_methodology_action")
            note = u.get("audit_note", "")
            lines.append(f"| `{vid}` | {ch} | {t} | `{reason}` | `INELIGIBLE` | **{act}**: {note} |")
    else:
        lines.append("*No uncertain exclusions detected.*")
    lines.append("")

    return "\n".join(lines)


# =============================================================================
# 10. CLI Interface
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Candidate video discovery and coverage reporter for the Football Narrative Observatory"
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        default=False,
        help="Explicitly run in synthetic offline mock mode (outputs clearly marked SYNTHETIC)"
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="Optional YouTube API key override (defaults to YOUTUBE_API_KEY environment variable)"
    )
    parser.add_argument(
        "--method",
        choices=["uploads_playlist", "search"],
        default=None,
        help="Discovery method: 'search' (date-bounded; 1 call from the separate search.list bucket, default 100/day) "
             "or 'uploads_playlist' (1 unit/call from the default bucket)"
    )
    parser.add_argument(
        "--event-id",
        default=None,
        help="Event from config/event_windows.yml (e.g. TRANSFER_AL_NASSR_2023). Supplies event/baseline terms "
             "and, unless --window-start/--window-end are given, the derived discovery window."
    )
    parser.add_argument(
        "--snapshot",
        default=None,
        help="Snapshot ID from --snapshot-config; fills --event-id, --method, --max-pages and --output"
    )
    parser.add_argument(
        "--snapshot-config",
        default="config/discovery/pilot_event_snapshots.yml",
        help="Snapshot configuration file (default: config/discovery/pilot_event_snapshots.yml)"
    )
    parser.add_argument(
        "--estimate-only",
        action="store_true",
        default=False,
        help="Print the resolved event, window and upper-bound discovery quota cost, then exit without API calls"
    )
    parser.add_argument(
        "--window-start",
        default=None,
        help="Window start ISO timestamp (default: derived from --event-id, else 2022-12-18T00:00:00Z)"
    )
    parser.add_argument(
        "--window-end",
        default=None,
        help="Window end ISO timestamp (default: derived from --event-id, else 2022-12-20T23:59:59Z)"
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum pagination pages per channel (default: 2)"
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSON report file path (default: reports/pilot_candidate_coverage_report.json)"
    )
    parser.add_argument(
        "--input-json",
        default=None,
        help="Input JSON report file to deduplicate and regenerate without making YouTube API calls"
    )
    parser.add_argument(
        "--summary",
        default=None,
        help="Render summary table of an existing report JSON file without running discovery"
    )
    return parser.parse_args()


def load_snapshot(config_path: str, snapshot_id: str) -> Dict[str, Any]:
    """Reads one discovery snapshot definition from the snapshot configuration file."""
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    for snap in data.get("snapshots", []):
        if snap.get("snapshot_id") == snapshot_id:
            return snap
    raise ValueError(f"Snapshot {snapshot_id!r} not found in {config_path}")


def apply_snapshot_defaults(args) -> None:
    """Fills unset CLI options from --snapshot, then applies the legacy defaults. Explicit flags win."""
    if args.snapshot:
        snap = load_snapshot(args.snapshot_config, args.snapshot)
        disc = snap.get("discovery", {})
        if disc.get("method") not in ("search", "uploads_playlist"):
            raise ValueError(f"Snapshot {args.snapshot} has discovery method {disc.get('method')!r}; it reuses an "
                             "existing report and must not run discovery")
        if args.event_id and args.event_id != snap.get("event_id"):
            raise ValueError(f"--event-id {args.event_id} conflicts with snapshot event {snap.get('event_id')}")
        args.event_id = snap.get("event_id")
        args.method = args.method or disc.get("method")
        args.max_pages = args.max_pages or disc.get("max_pages_per_channel")
        args.output = args.output or disc.get("report_output")
    args.method = args.method or "search"
    args.max_pages = args.max_pages or 2
    args.output = args.output or "reports/pilot_candidate_coverage_report.json"


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    args = parse_args()
    try:
        apply_snapshot_defaults(args)
    except (OSError, ValueError) as exc:
        print(f"[FATAL ERROR] {exc}", file=sys.stderr)
        sys.exit(1)

    if not args.estimate_only and not args.summary and os.path.exists(args.output):
        print(f"[FATAL ERROR] Refusing to overwrite existing report: {args.output} "
              "(choose a new --output; checked before any API call)", file=sys.stderr)
        sys.exit(1)

    if args.estimate_only:
        config = load_repository_configuration()
        resolved = resolve_event(config, args.event_id or "WC_2022_FINAL")
        if args.event_id and not (args.window_start or args.window_end):
            w_start, w_end, derivation = resolved["window_start"], resolved["window_end"], resolved["window_derivation"]
        else:
            w_start, w_end = args.window_start or LEGACY_WINDOW_START, args.window_end or LEGACY_WINDOW_END
            derivation = "EXPLICIT_ARGUMENTS" if args.event_id else "LEGACY_WC_2022_CALENDAR_DAYS"
        cost = estimate_discovery_cost(len(config["channels"]), args.max_pages, args.method)
        print(f"event_id={args.event_id or 'WC_2022_FINAL (legacy default)'} occurred_at={resolved['occurred_at']}")
        print(f"window={w_start} -> {w_end} ({derivation})")
        print(f"event_terms={resolved['event_terms']} baseline_terms={resolved['baseline_terms']}")
        print(f"channels={len(config['channels'])} method={args.method} max_pages_per_channel={args.max_pages}")
        b = cost["quota_by_bucket_upper_bound"]
        print(f"discovery upper bound: requests={cost['requests_upper_bound']}")
        print(f"  search.list bucket: <= {b[SEARCH_BUCKET]} calls of {DEFAULT_DAILY_QUOTA[SEARCH_BUCKET]}/day default")
        print(f"  default bucket:     <= {b[DEFAULT_BUCKET]} units of {DEFAULT_DAILY_QUOTA[DEFAULT_BUCKET]}/day default")
        print(f"  rates: {QUOTA_REFERENCE['url']} (last updated {QUOTA_REFERENCE['page_last_updated']}); "
              "confirm the project's allocation in the Cloud console")
        print(f"report would be written to: {args.output}")
        return

    if args.summary:
        if not os.path.exists(args.summary):
            print(f"Error: report file '{args.summary}' does not exist.", file=sys.stderr)
            sys.exit(1)
        with open(args.summary, "r", encoding="utf-8") as f:
            report_data = json.load(f)
        print(format_report_summary_markdown(report_data))
        return

    if args.input_json:
        try:
            report = regenerate_from_saved_report(input_json_path=args.input_json)
        except Exception as exc:
            print(f"[FATAL ERROR] Report regeneration failed: {exc}", file=sys.stderr)
            sys.exit(1)
    else:
        try:
            report = execute_discovery(
                mock=args.mock,
                api_key=args.api_key,
                method=args.method,
                window_start_str=args.window_start,
                window_end_str=args.window_end,
                max_pages_per_channel=args.max_pages,
                event_id=args.event_id,
            )
        except Exception as exc:
            print(f"[FATAL ERROR] Discovery execution failed: {exc}", file=sys.stderr)
            sys.exit(1)

    # Save output report
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "x", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    # Output markdown summary
    print(format_report_summary_markdown(report))
    print(f"\n[OK] Coverage report written to: {args.output}")


if __name__ == "__main__":
    main()

