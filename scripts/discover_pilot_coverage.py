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

OFFICIAL_QUOTA_COSTS = {
    "channels.list": 1,
    "playlistItems.list": 1,
    "videos.list": 1,
    "search.list": 100,
    "commentThreads.list": 1,
}

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
        total = 0
        for ep, count in self.requests_by_endpoint.items():
            cost = OFFICIAL_QUOTA_COSTS.get(ep, 1)
            total += count * cost
        return total

    def to_dict(self) -> Dict[str, Any]:
        return {
            "requests_by_endpoint": dict(self.requests_by_endpoint),
            "total_api_requests": self.total_requests(),
            "actual_quota_units_consumed": self.calculate_actual_quota_units(),
            "official_quota_rates_used": OFFICIAL_QUOTA_COSTS,
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
            "published_at": candidate.get("published_at", ""),
            "status": "INELIGIBLE",
            "exclusion_reason": "UNRESOLVED_CHANNEL_IDENTITY",
            "player_category": "NONE",
            "player_relevance": "NONE",
            "relevance_tier": "UNRESOLVED_CHANNEL",
            "matched_player_aliases": [],
            "matched_event_terms": [],
            "matched_field": None,
            "match_excerpt": None,
            "is_short": None,
            "format_label": "UNKNOWN",
        }

    if not candidate.get("is_available", True):
        return {
            "video_id": candidate["video_id"],
            "channel_id": candidate.get("channel_id"),
            "channel_title": candidate.get("channel_title"),
            "title": candidate.get("title", ""),
            "published_at": candidate.get("published_at", ""),
            "status": "INELIGIBLE",
            "exclusion_reason": "VIDEO_UNAVAILABLE",
            "player_category": "NONE",
            "player_relevance": "NONE",
            "relevance_tier": "NO_MATCH",
            "matched_player_aliases": [],
            "matched_event_terms": [],
            "matched_field": None,
            "match_excerpt": None,
            "is_short": None,
            "format_label": "UNKNOWN",
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

    # 3. Match Event Terms
    matched_event_title = find_matching_terms(title, event_terms)
    matched_event_desc = find_matching_terms(desc, event_terms)
    matched_event_terms = list(set(matched_event_title + matched_event_desc))
    has_event = bool(matched_event_terms)

    # Matched Field Determination
    matched_in_title = bool(matched_messi_title or matched_ronaldo_title or matched_event_title)
    matched_in_desc = bool(matched_messi_desc or matched_ronaldo_desc or matched_event_desc)
    if matched_in_title and matched_in_desc:
        matched_field = "both"
    elif matched_in_title:
        matched_field = "title"
    elif matched_in_desc:
        matched_field = "description"
    else:
        matched_field = None

    # Excerpt Extraction for Relevance Auditing
    all_matched = matched_player_terms + matched_event_terms
    excerpt = None
    if matched_in_title:
        excerpt = extract_match_excerpt(title, all_matched)
    elif matched_in_desc:
        excerpt = extract_match_excerpt(desc, all_matched)

    # 4. Relevance Tier Separation (Player vs Event-Specific vs Broad-Generic)
    if player_category != "NONE":
        relevance_tier = "PLAYER_MATCH"
    elif has_event:
        specific_matches = [t for t in matched_event_terms if t.lower() not in GENERIC_EVENT_TERMS]
        if specific_matches:
            relevance_tier = "EVENT_SPECIFIC"
        else:
            relevance_tier = "BROAD_EVENT_UNCERTAIN"
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

    # 6. Eligibility Determination (Strictly approved rules)
    exclusion_reason = None
    if not in_window:
        status = "INELIGIBLE"
        exclusion_reason = "OUT_OF_WINDOW"
    elif relevance_tier == "NO_MATCH":
        status = "INELIGIBLE"
        exclusion_reason = "NO_TARGET_RELEVANCE"
    else:
        status = "ELIGIBLE"

    return {
        "video_id": candidate["video_id"],
        "channel_id": candidate.get("channel_id"),
        "channel_title": candidate.get("channel_title"),
        "title": title,
        "published_at": pub_raw,
        "in_window": in_window,
        "player_category": player_category,
        "player_relevance": player_category,
        "relevance_tier": relevance_tier,
        "has_event_term": has_event,
        "matched_player_aliases": matched_player_terms,
        "matched_event_terms": matched_event_terms,
        "matched_field": matched_field,
        "match_excerpt": excerpt,
        "duration_seconds": dur,
        "is_short": is_short,
        "format_label": format_label,
        "status": status,
        "exclusion_reason": exclusion_reason,
        "view_count": candidate.get("view_count"),
        "comment_count": candidate.get("comment_count"),
    }


# =============================================================================
# 7. Explicit Synthetic Mock Mode Fixtures
# =============================================================================

def run_synthetic_mock_discovery(
    repo_config: Dict[str, Any],
    window_start: datetime.datetime,
    window_end: datetime.datetime,
    event_terms: List[str],
) -> Dict[str, Any]:
    """Generates an explicitly labeled SYNTHETIC candidate coverage report."""
    channels = repo_config["channels"]
    aliases = repo_config["player_aliases"]

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
        },
    ]

    evaluated = [
        classify_hydrated_candidate(
            candidate=c,
            window_start=window_start,
            window_end=window_end,
            player_aliases=aliases,
            event_terms=event_terms,
            channel_is_validated=True,
        )
        for c in synthetic_raw_candidates
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
            "total_candidates": len(c_vids),
            "eligible_candidates": len(eligible),
            "relevance_breakdown": {
                "PLAYER_MATCH": sum(1 for v in eligible if v["relevance_tier"] == "PLAYER_MATCH"),
                "EVENT_SPECIFIC": sum(1 for v in eligible if v["relevance_tier"] == "EVENT_SPECIFIC"),
                "BROAD_EVENT_UNCERTAIN": sum(1 for v in eligible if v["relevance_tier"] == "BROAD_EVENT_UNCERTAIN"),
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
            "total_candidates_evaluated": len(evaluated),
            "total_eligible_candidates": sum(1 for v in evaluated if v["status"] == "ELIGIBLE"),
            "player_focal_candidates": sum(1 for v in evaluated if v["relevance_tier"] == "PLAYER_MATCH"),
            "event_specific_candidates": sum(1 for v in evaluated if v["relevance_tier"] == "EVENT_SPECIFIC"),
            "broad_generic_uncertain_candidates": sum(1 for v in evaluated if v["relevance_tier"] == "BROAD_EVENT_UNCERTAIN"),
            "coverage_gaps": [
                ch["channel_name"] for ch in channels if channel_breakdown[ch["channel_id"]]["eligible_candidates"] == 0
            ],
        },
        "channel_breakdown": channel_breakdown,
        "evaluated_candidates": evaluated,
    }


# =============================================================================
# 8. Main Orchestrator (Live & Mock)
# =============================================================================

def execute_discovery(
    repo_root: Optional[str] = None,
    mock: bool = False,
    api_key: Optional[str] = None,
    method: str = "search",
    window_start_str: str = "2022-12-18T00:00:00Z",
    window_end_str: str = "2022-12-20T23:59:59Z",
    max_pages_per_channel: int = 2,
) -> Dict[str, Any]:
    """Orchestrates candidate video discovery without performing any Snowflake writes."""
    config = load_repository_configuration(repo_root)

    w_start = datetime.datetime.fromisoformat(window_start_str.replace("Z", "+00:00"))
    w_end = datetime.datetime.fromisoformat(window_end_str.replace("Z", "+00:00"))

    # Select event terms from first matching pilot event (WC_2022_FINAL)
    event_terms = ["world cup", "final", "argentina vs france", "fifa world cup", "qatar 2022"]
    for ev in config.get("events", []):
        if ev.get("external_event_id") == "WC_2022_FINAL":
            terms = ev.get("event_terms", []) + ev.get("baseline_terms", [])
            if terms:
                event_terms = sorted(list(set(terms)))
            break

    # 1. Synthetic Mock Mode
    if mock:
        return run_synthetic_mock_discovery(config, w_start, w_end, event_terms)

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

    # Step C: Hydrate video metadata in batches via videos.list
    candidate_ids = [c["video_id"] for c in all_raw_candidates if "video_id" in c]
    hydrated_dict = hydrate_video_metadata_live(
        youtube=youtube,
        video_ids=candidate_ids,
        telemetry=telemetry,
    )

    # Step D: Classify candidates against frozen methodology rules
    classified_candidates = []
    for c in all_raw_candidates:
        cid = c.get("channel_id")
        ch_status = validated_channels.get(cid, {}).get("status")
        is_validated = (ch_status == "VALIDATED")

        classified = classify_hydrated_candidate(
            candidate=hydrated_dict.get(c["video_id"], c),
            window_start=w_start,
            window_end=w_end,
            player_aliases=config["player_aliases"],
            event_terms=event_terms,
            channel_is_validated=is_validated,
        )
        classified_candidates.append(classified)

    # Step E: Aggregate channel breakdown
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
            "total_candidates": len(c_vids),
            "eligible_candidates": len(eligible),
            "relevance_breakdown": {
                "PLAYER_MATCH": sum(1 for v in eligible if v["relevance_tier"] == "PLAYER_MATCH"),
                "EVENT_SPECIFIC": sum(1 for v in eligible if v["relevance_tier"] == "EVENT_SPECIFIC"),
                "BROAD_EVENT_UNCERTAIN": sum(1 for v in eligible if v["relevance_tier"] == "BROAD_EVENT_UNCERTAIN"),
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
        },
        "telemetry": telemetry.to_dict(),
        "channel_identity_validations": discrepancies,
        "overall_summary": {
            "total_channels": len(config["channels"]),
            "total_candidates_evaluated": len(classified_candidates),
            "total_eligible_candidates": sum(1 for v in classified_candidates if v["status"] == "ELIGIBLE"),
            "player_focal_candidates": sum(1 for v in classified_candidates if v["relevance_tier"] == "PLAYER_MATCH"),
            "event_specific_candidates": sum(1 for v in classified_candidates if v["relevance_tier"] == "EVENT_SPECIFIC"),
            "broad_generic_uncertain_candidates": sum(1 for v in classified_candidates if v["relevance_tier"] == "BROAD_EVENT_UNCERTAIN"),
            "coverage_gaps": coverage_gaps,
        },
        "channel_breakdown": channel_breakdown,
        "evaluated_candidates": classified_candidates,
    }


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

    # Telemetry
    telem = report.get("telemetry", {})
    lines.append("### API & Quota Telemetry (Actual Executed Requests)")
    lines.append(f"- **Total API Requests**: {telem.get('total_api_requests', 0)}")
    lines.append(f"- **Actual Quota Units Consumed**: {telem.get('actual_quota_units_consumed', 0)}")
    lines.append(f"- **Calls Breakdown**: {telem.get('requests_by_endpoint', {})}")
    if telem.get("error_count", 0) > 0:
        lines.append(f"- **Errors Encountered**: {telem.get('error_count')} (inspect report JSON for details)")
    lines.append("")

    # Candidate Breakdown by Relevance Tier
    ov = report["overall_summary"]
    lines.append("### Relevance Tier Summary")
    lines.append(f"- **Target Player Matches (Messi / Ronaldo / Dual)**: {ov.get('player_focal_candidates', 0)}")
    lines.append(f"- **Specific Event Matches (World Cup / Tournament Terms)**: {ov.get('event_specific_candidates', 0)}")
    lines.append(f"- **Broad Generic Uncertain (Matched only 'final')**: {ov.get('broad_generic_uncertain_candidates', 0)}")
    lines.append(f"- **Total Eligible Candidates**: {ov['total_eligible_candidates']} / {ov['total_candidates_evaluated']} evaluated")
    lines.append("")

    # Table
    lines.append("### Channel Candidate Distribution")
    lines.append("")
    lines.append("| Channel Name | Method | Pages | Status | Total | Eligible | Player Match | Event Specific | Broad Uncertain | Long-Form | Shorts | Format Unknown |")
    lines.append("| :--- | :--- | :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |")

    for cid, ch in report["channel_breakdown"].items():
        name = ch.get("live_title") or ch.get("configured_name")
        rb = ch.get("relevance_breakdown", {})
        bf = ch["by_format"]
        lines.append(
            f"| {name} | {ch['discovery_method']} | {ch['pages_fetched']} | {ch['completeness_status']} | "
            f"{ch['total_candidates']} | **{ch['eligible_candidates']}** | {rb.get('PLAYER_MATCH', 0)} | {rb.get('EVENT_SPECIFIC', 0)} | {rb.get('BROAD_EVENT_UNCERTAIN', 0)} | "
            f"{bf['LONG_FORM']} | {bf['SHORTS_CONFIRMED']} | {bf['FORMAT_UNKNOWN']} |"
        )

    lines.append("")
    if ov["coverage_gaps"]:
        lines.append(f"- **Coverage Gaps (0 eligible)**: {', '.join(ov['coverage_gaps'])}")
    else:
        lines.append("- **Coverage Gaps**: None. All tracked channels have eligible candidates in this window.")

    return "\n".join(lines)


# =============================================================================
# 9. CLI Interface
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
        default="search",
        help="Discovery method: 'search' (date-bounded; 100 quota units/call) or 'uploads_playlist' (1 quota unit/call)"
    )
    parser.add_argument(
        "--window-start",
        default="2022-12-18T00:00:00Z",
        help="Event window start ISO timestamp (default: 2022-12-18T00:00:00Z)"
    )
    parser.add_argument(
        "--window-end",
        default="2022-12-20T23:59:59Z",
        help="Event window end ISO timestamp (default: 2022-12-20T23:59:59Z)"
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=2,
        help="Maximum pagination pages per channel (default: 2)"
    )
    parser.add_argument(
        "--output",
        default="reports/pilot_candidate_coverage_report.json",
        help="Output JSON report file path"
    )
    parser.add_argument(
        "--summary",
        default=None,
        help="Render summary table of an existing report JSON file without running discovery"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.summary:
        if not os.path.exists(args.summary):
            print(f"Error: report file '{args.summary}' does not exist.", file=sys.stderr)
            sys.exit(1)
        with open(args.summary, "r", encoding="utf-8") as f:
            report_data = json.load(f)
        print(format_report_summary_markdown(report_data))
        return

    try:
        report = execute_discovery(
            mock=args.mock,
            api_key=args.api_key,
            method=args.method,
            window_start_str=args.window_start,
            window_end_str=args.window_end,
            max_pages_per_channel=args.max_pages,
        )
    except Exception as exc:
        print(f"[FATAL ERROR] Discovery execution failed: {exc}", file=sys.stderr)
        sys.exit(1)

    # Save output report
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    # Output markdown summary
    print(format_report_summary_markdown(report))
    print(f"\n[OK] Coverage report written to: {args.output}")


if __name__ == "__main__":
    main()
