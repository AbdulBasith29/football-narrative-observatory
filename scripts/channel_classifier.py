import os
import uuid
import json
import hashlib
import re
import yaml
import subprocess
from datetime import datetime, timezone, timedelta, date, time
from dateutil import parser as date_parser
from ingestion_snowflake import get_snowflake_connection

class ClassificationError(Exception):
    """Base exception for classification engine."""
    pass

class DeduplicationRuleMissingError(ClassificationError):
    """Raised when cross-video deduplication is required but no approved rule exists."""
    pass

class UnapprovedParameterError(ClassificationError):
    """Raised when a classification rule accesses an unapproved configuration parameter."""
    pass

class SnapshotMutationViolationError(ClassificationError):
    """Raised when an attempt is made to mutate or overwrite an existing event snapshot."""
    pass


DEDUPLICATION_RULE_REGISTRY = {}


def register_deduplication_rule(rule_version: str):
    """
    Registers an approved cross-video deduplication rule implementation.
    The rule function accepts a list of evidence item dicts and returns
    the modified list with is_deduplicated_duplicate, deduplication_cluster_id,
    and included_in_prevalence updated.
    """
    def decorator(fn):
        DEDUPLICATION_RULE_REGISTRY[rule_version] = fn
        return fn
    return decorator


def normalize_ntz_timestamp(val) -> str:
    """
    Normalizes a timestamp (datetime, date, ISO string, etc.) to a naive UTC ISO string
    (YYYY-MM-DDTHH:MM:SS or YYYY-MM-DDTHH:MM:SS.ffffff) matching Snowflake TIMESTAMP_NTZ
    semantics while preserving fractional-second precision and converting all timezones to UTC.
    """
    if val is None:
        return ""
    if isinstance(val, str):
        val_str = val.strip()
        if not val_str:
            return ""
        if val_str.endswith("z") or val_str.endswith("Z"):
            val_str = val_str[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(val_str)
        except ValueError:
            dt = date_parser.parse(val_str)
    elif isinstance(val, datetime):
        dt = val
    elif isinstance(val, date):
        dt = datetime.combine(val, time.min)
    else:
        val_str = str(val).strip()
        if not val_str:
            return ""
        if val_str.endswith("z") or val_str.endswith("Z"):
            val_str = val_str[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(val_str)
        except Exception:
            dt = date_parser.parse(val_str)

    if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    else:
        dt = dt.replace(tzinfo=None)

    return dt.isoformat()


def load_stratification_config(config_path="config/channel_stratification.yml"):
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    config_json = json.dumps(data, sort_keys=True)
    config_hash = hashlib.sha256(config_json.encode("utf-8")).hexdigest()
    return data, config_hash


def load_player_aliases(config_path="config/player_aliases.yml"):
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    messi_aliases = []
    ronaldo_aliases = []
    for target in data.get("targets", []):
        name = target.get("entity_name", "").lower()
        aliases = [a["text"].lower() for a in target.get("aliases", [])]
        if "messi" in name:
            messi_aliases.extend(aliases)
        elif "ronaldo" in name:
            ronaldo_aliases.extend(aliases)
    return sorted(list(set(messi_aliases))), sorted(list(set(ronaldo_aliases)))


def compute_canonical_manifest_hash(evidence_items: list) -> str:
    """
    Computes a deterministic SHA-256 hash across sorted canonical evidence items.
    """
    clean_items = []
    for it in evidence_items:
        clean_item = {k: v for k, v in it.items() if k != "published_at_dt"}
        clean_items.append(clean_item)
    sorted_items = sorted(
        clean_items,
        key=lambda x: (x["source_video_id"], x["published_at"])
    )
    serialized = json.dumps(sorted_items, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def evaluate_video_mentions(title: str, description: str, messi_aliases: list, ronaldo_aliases: list):
    """
    Evaluates exact alias matches for Messi and Ronaldo across title and description.
    """
    combined = f"{title or ''} {description or ''}".lower()
    m_matched = [a for a in messi_aliases if re.search(r'\b' + re.escape(a) + r'\b', combined)]
    r_matched = [a for a in ronaldo_aliases if re.search(r'\b' + re.escape(a) + r'\b', combined)]
    return {
        "messi_matched": len(m_matched) > 0,
        "ronaldo_matched": len(r_matched) > 0,
        "matched_messi_aliases": m_matched,
        "matched_ronaldo_aliases": r_matched
    }


def classify_channel_type(
    channel_id: str,
    channel_name: str,
    about_description: str,
    eligible_videos: list,
    strat_cfg: dict
):
    """
    Evaluates Channel Type hierarchically per frozen protocol Section 6:
    1. Minimum evidence check: reliable identity signal AND >= 10 eligible videos.
       Reliable identity signal requires an About description or an approved versioned third-party categorisation.
       channel_name alone does NOT satisfy the reliable identity requirement.
    2. Curated Broadcaster Registry match -> BROAD_REACH_PUBLISHER
    3. Club Media Proportion (P_club > 0.70 and winning margin) -> CLUB_MEDIA
    4. Curated Analysis Registry match or tag share -> ANALYSIS_PUBLISHER
    5. Fallback -> INDEPENDENT_CREATOR
    Fails closed if an unapproved threshold/registry is required.
    """
    num_videos = len(eligible_videos)
    has_about = bool(about_description and about_description.strip())
    third_party_reg = strat_cfg.get("third_party_categorisation_registry") or {}
    has_third_party = bool(third_party_reg.get(channel_id))
    identity_present = has_about or has_third_party
    
    # Minimum evidence gating: channel_name alone is NOT sufficient.
    if not identity_present or num_videos < 10:
        reason = "MISSING_RELIABLE_IDENTITY_SIGNAL" if not identity_present else "LESS_THAN_10_VIDEOS"
        return {
            "channel_type": "UNCLASSIFIED",
            "confidence": "LOW",
            "evidence": {
                "identity_present": identity_present,
                "has_about_description": has_about,
                "has_third_party_categorisation": has_third_party,
                "about_snippet_preview": (about_description[:100] if about_description else None),
                "video_count": num_videos,
                "reason": reason
            }
        }

    # Step 1: Broadcaster Check
    broadcaster_reg_ver = strat_cfg.get("broadcaster_registry_version")
    broadcaster_allowlist = strat_cfg.get("broadcasters_allowlist") or []
    if broadcaster_reg_ver and channel_id in broadcaster_allowlist:
        return {
            "channel_type": "BROAD_REACH_PUBLISHER",
            "confidence": "HIGH",
            "evidence": {
                "broadcaster_registry_version": broadcaster_reg_ver,
                "registry_matched": True
            }
        }

    # Step 2: Club Media Check
    club_dominance_threshold = strat_cfg.get("frozen_parameters", {}).get("club_dominance_threshold", 0.70)
    club_margin = strat_cfg.get("club_dominance_winning_margin")
    
    # Tally club mentions if clubs configured
    club_registry = strat_cfg.get("club_registry") or {}
    club_counts = {}
    for v in eligible_videos:
        text = f"{v.get('title', '')} {v.get('description', '')}".lower()
        for club_name, aliases in club_registry.items():
            if any(re.search(r'\b' + re.escape(a.lower()) + r'\b', text) for a in aliases):
                club_counts[club_name] = club_counts.get(club_name, 0) + 1
                
    if club_counts and num_videos > 0:
        sorted_clubs = sorted(club_counts.items(), key=lambda x: x[1], reverse=True)
        top_club, top_count = sorted_clubs[0]
        top_share = top_count / num_videos
        runner_up_share = (sorted_clubs[1][1] / num_videos) if len(sorted_clubs) > 1 else 0.0
        
        if top_share > club_dominance_threshold:
            if club_margin is None:
                raise UnapprovedParameterError(
                    f"Club dominance threshold exceeded ({top_share:.2f} > {club_dominance_threshold}) "
                    "but 'club_dominance_winning_margin' is unapproved (null). Fails closed."
                )
            if (top_share - runner_up_share) >= club_margin:
                confidence = "HIGH" if top_share > 0.90 else "MEDIUM"
                return {
                    "channel_type": "CLUB_MEDIA",
                    "confidence": confidence,
                    "evidence": {
                        "winning_club": top_club,
                        "winning_share": top_share,
                        "runner_up_share": runner_up_share,
                        "margin": top_share - runner_up_share,
                        "margin_threshold": club_margin
                    }
                }

    # Step 3: Analysis Publisher Check
    analysis_reg_ver = strat_cfg.get("analysis_registry_version")
    analysis_allowlist = strat_cfg.get("analysis_publishers_allowlist") or []
    if analysis_reg_ver and channel_id in analysis_allowlist:
        return {
            "channel_type": "ANALYSIS_PUBLISHER",
            "confidence": "HIGH",
            "evidence": {
                "analysis_registry_version": analysis_reg_ver,
                "registry_matched": True
            }
        }
        
    analysis_tag_threshold = strat_cfg.get("analysis_topic_share_threshold")
    analysis_keywords = strat_cfg.get("analysis_keywords") or []
    if analysis_keywords and num_videos > 0:
        analysis_count = sum(
            1 for v in eligible_videos
            if any(re.search(r'\b' + re.escape(k.lower()) + r'\b', f"{v.get('title', '')} {v.get('description', '')}".lower())
                   for k in analysis_keywords)
        )
        analysis_share = analysis_count / num_videos
        if analysis_tag_threshold is not None and analysis_share >= analysis_tag_threshold:
            return {
                "channel_type": "ANALYSIS_PUBLISHER",
                "confidence": "MEDIUM",
                "evidence": {
                    "analysis_share": analysis_share,
                    "analysis_threshold": analysis_tag_threshold
                }
            }

    # Step 4: Fallback Default
    return {
        "channel_type": "INDEPENDENT_CREATOR",
        "confidence": "LOW",
        "evidence": {
            "identity_present": identity_present,
            "reason": "DEFAULT_FALLBACK_WITH_IDENTITY"
        }
    }


def classify_player_focus(
    eligible_videos: list,
    reference_period_end: datetime,
    effective_window_days: int,
    strat_cfg: dict
):
    """
    Evaluates Player Focus per frozen protocol Section 6:
    1. Prevalence floor: P_M > 0.15, P_R > 0.15
    2. Smoothed ratios: R_M:R = (V_M + 1) / (V_R + 1), R_R:M = (V_R + 1) / (V_M + 1)
    3. Dominance ratio: > 4.0
    4. Mixed focus: dual prevalence AND (P_M + P_R >= combined_focus_threshold)
    5. Temporal stability across 3x30d subwindows.
    Fails closed if combined_focus_threshold is unapproved.
    """
    num_videos = len(eligible_videos)
    alpha = strat_cfg.get("frozen_parameters", {}).get("smoothing_alpha", 1.0)
    ratio_thresh = strat_cfg.get("frozen_parameters", {}).get("player_focus_ratio_threshold", 4.0)
    prev_floor = strat_cfg.get("frozen_parameters", {}).get("player_prevalence_floor", 0.15)
    combined_focus_threshold = strat_cfg.get("combined_focus_threshold")

    v_m = sum(1 for v in eligible_videos if v.get("messi_matched"))
    v_r = sum(1 for v in eligible_videos if v.get("ronaldo_matched"))
    
    p_m = (v_m / num_videos) if num_videos > 0 else 0.0
    p_r = (v_r / num_videos) if num_videos > 0 else 0.0
    
    r_m_r = (v_m + alpha) / (v_r + alpha)
    r_r_m = (v_r + alpha) / (v_m + alpha)
    primary_ratio = r_m_r if v_m >= v_r else r_r_m

    if num_videos < 10:
        return {
            "player_focus": "UNCLASSIFIED",
            "confidence": "LOW",
            "messi_count": v_m,
            "ronaldo_count": v_r,
            "messi_prevalence": p_m,
            "ronaldo_prevalence": p_r,
            "focus_ratio": primary_ratio,
            "evidence": {
                "video_count": num_videos,
                "reason": "LESS_THAN_10_VIDEOS",
                "messi_count": v_m,
                "ronaldo_count": v_r,
                "messi_prevalence": p_m,
                "ronaldo_prevalence": p_r,
                "focus_ratio": primary_ratio
            }
        }

    # Decision tree
    if r_m_r > ratio_thresh and p_m > prev_floor:
        assigned_focus = "MESSI_FOCUSED"
    elif r_r_m > ratio_thresh and p_r > prev_floor:
        assigned_focus = "RONALDO_FOCUSED"
    elif p_m > prev_floor and p_r > prev_floor:
        if combined_focus_threshold is None:
            raise UnapprovedParameterError(
                f"Dual prevalence satisfied (P_M={p_m:.3f}, P_R={p_r:.3f}) but "
                "'combined_focus_threshold' is unapproved (null). Fails closed."
            )
        if (p_m + p_r) >= combined_focus_threshold:
            assigned_focus = "MIXED_FOCUS"
        else:
            assigned_focus = "NO_STRONG_DOMINANT_PLAYER_FOCUS"
    else:
        assigned_focus = "NO_STRONG_DOMINANT_PLAYER_FOCUS"

    # Temporal stability across 3x30d subwindows (for 90d window)
    subwindow_results = []
    if effective_window_days == 90:
        for sw_idx in range(3):
            sw_end = reference_period_end - timedelta(days=sw_idx * 30)
            sw_start = reference_period_end - timedelta(days=(sw_idx + 1) * 30)
            sw_vids = [
                v for v in eligible_videos
                if sw_start <= v["published_at_dt"] < sw_end
            ]
            sw_n = len(sw_vids)
            if sw_n >= 3:
                sw_vm = sum(1 for v in sw_vids if v.get("messi_matched"))
                sw_vr = sum(1 for v in sw_vids if v.get("ronaldo_matched"))
                sw_rm = (sw_vm + alpha) / (sw_vr + alpha)
                sw_rr = (sw_vr + alpha) / (sw_vm + alpha)
                sw_pm = sw_vm / sw_n
                sw_pr = sw_vr / sw_n
                if sw_rm > ratio_thresh and sw_pm > prev_floor:
                    sw_focus = "MESSI_FOCUSED"
                elif sw_rr > ratio_thresh and sw_pr > prev_floor:
                    sw_focus = "RONALDO_FOCUSED"
                elif sw_pm > prev_floor and sw_pr > prev_floor:
                    sw_focus = "MIXED_FOCUS"
                else:
                    sw_focus = "NO_STRONG_DOMINANT_PLAYER_FOCUS"
                subwindow_results.append(sw_focus)

    # Stability confidence scoring
    if effective_window_days == 180:
        confidence = "LOW"
    else:
        if len(subwindow_results) == 3 and all(sw == assigned_focus for sw in subwindow_results):
            confidence = "MEDIUM" # Defaults to MEDIUM deterministically since large-margin rules are unapproved
        elif len(subwindow_results) >= 2 and subwindow_results.count(assigned_focus) >= 2:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"

    return {
        "player_focus": assigned_focus,
        "confidence": confidence,
        "messi_count": v_m,
        "ronaldo_count": v_r,
        "messi_prevalence": p_m,
        "ronaldo_prevalence": p_r,
        "focus_ratio": primary_ratio,
        "evidence": {
            "video_count": num_videos,
            "subwindow_evaluations": subwindow_results,
            "effective_window_days": effective_window_days
        }
    }


def classify_channel_assessment(
    conn,
    frame_version_key: str,
    channel_key: str,
    reference_period_end: datetime,
    assessment_trigger: str = "EVENT_PREPARATION",
    event_version_key: str = None,
    config_path: str = "config/channel_stratification.yml",
    alias_config_path: str = "config/player_aliases.yml",
    ingestion_run_id: str = None,
    about_description: str = None
):
    """
    Main deterministic classification routine for a single channel assessment.
    Maintains Type-2 SCD in CORE.DIM_CHANNEL_STRATUM_VERSION and persists
    the exact evidence manifest in OPS.FACT_CHANNEL_ASSESSMENT_EVIDENCE_ITEM.
    """
    strat_cfg, config_hash = load_stratification_config(config_path)
    messi_aliases, ronaldo_aliases = load_player_aliases(alias_config_path)
    
    protocol_version = strat_cfg.get("classification_protocol_version", "1.1")
    dedup_rule_ver = strat_cfg.get("deduplication_rule_version")
    dedup_status = strat_cfg.get("deduplication_rule_status")
    
    # Cross-video-ID deduplication fail-closed gate
    # Frozen methodology requires deduplication before prevalence calculation.
    # When no approved rule exists, raise DeduplicationRuleMissingError on the relevant classification path.
    # Only proceed when:
    # - an approved versioned deduplication rule exists (dedup_status == "APPROVED"); OR
    # - human decision has explicitly approved EXPLICIT_NONE (dedup_status in ("APPROVED", "APPROVED_EXPLICIT_NONE")).
    is_explicit_none = (
        (dedup_rule_ver == "EXPLICIT_NONE" and dedup_status in ("APPROVED", "APPROVED_EXPLICIT_NONE"))
        or dedup_status == "APPROVED_EXPLICIT_NONE"
    )
    is_rule_approved = (
        dedup_status == "APPROVED"
        and bool(dedup_rule_ver)
        and dedup_rule_ver not in ("HUMAN_APPROVAL_REQUIRED", "EXPLICIT_NONE")
    )

    if not (is_rule_approved or is_explicit_none):
        raise DeduplicationRuleMissingError(
            "Cross-video deduplication methodology is required before prevalence calculation, "
            f"but deduplication_rule_version is '{dedup_rule_ver}' and status is '{dedup_status}'. "
            "Prevalence calculation cannot proceed with raw distinct video IDs without explicit approval. "
            "Either an approved versioned deduplication rule with status 'APPROVED' "
            "or explicit human approval of 'APPROVED_EXPLICIT_NONE' is required."
        )

    cursor = conn.cursor()
    if not ingestion_run_id:
        ingestion_run_id = str(uuid.uuid4())
        
    try:
        git_sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], stderr=subprocess.STDOUT).decode('utf-8').strip()
    except Exception:
        git_sha = "unknown_sha"

    # Fetch channel identity
    cursor.execute("SELECT source_id, channel_name FROM CORE.DIM_CHANNEL WHERE channel_key = %s", (channel_key,))
    ch_row = cursor.fetchone()
    if not ch_row:
        raise ValueError(f"Channel {channel_key} not found in CORE.DIM_CHANNEL.")
    source_channel_id, channel_name = ch_row

    # Resolve reliable channel-identity signal (About description) from RAW if not explicitly supplied
    about_raw_response_id = None
    if about_description is None:
        cursor.execute('''
            SELECT raw_json_payload, raw_response_id
            FROM RAW.YOUTUBE_API_RESPONSE
            WHERE endpoint = 'channels.list' AND resource_scope_id = %s
            ORDER BY retrieved_at DESC
        ''', (source_channel_id,))
        about_row = cursor.fetchone()
        if about_row:
            try:
                payload = json.loads(about_row[0]) if isinstance(about_row[0], str) else about_row[0]
                items = payload.get("items", [])
                if items:
                    about_description = items[0].get("snippet", {}).get("description", "")
                    about_raw_response_id = about_row[1]
            except Exception:
                about_description = ""
        else:
            about_description = ""

    # Define 90d and 180d boundaries
    if reference_period_end.tzinfo is None:
        reference_period_end = reference_period_end.replace(tzinfo=timezone.utc)
    w90_start = reference_period_end - timedelta(days=90)
    w180_start = reference_period_end - timedelta(days=180)

    # Fetch evaluated pre-event videos strictly within 180d
    cursor.execute('''
        SELECT v.video_key, v.source_id, v.published_at, v.duration_seconds, v.is_short,
               s.title, s.description, s.raw_response_id, s.api_request_id
        FROM CORE.DIM_VIDEO v
        LEFT JOIN (
            SELECT video_key, title, description, raw_response_id, api_request_id,
                   ROW_NUMBER() OVER (PARTITION BY video_key ORDER BY observed_at DESC) as rn
            FROM CORE.FACT_VIDEO_SNAPSHOT
        ) s ON s.video_key = v.video_key AND s.rn = 1
        WHERE v.channel_key = %s
          AND v.published_at >= %s
          AND v.published_at < %s
          AND (v.is_short = FALSE OR v.is_short IS NULL)
        ORDER BY v.published_at ASC, v.source_id ASC
    ''', (channel_key, w180_start.isoformat(), reference_period_end.isoformat()))
    raw_video_rows = cursor.fetchall()

    # Exact source_video_id deduplication (deterministic identity)
    seen_source_ids = set()
    videos_180 = []
    videos_90 = []
    manifest_evidence_items = []
    
    for r in raw_video_rows:
        v_key, src_id, pub_at, duration, is_short, title, desc, raw_id, api_req_id = r
        pub_dt = pub_at.replace(tzinfo=timezone.utc) if pub_at and pub_at.tzinfo is None else pub_at
        
        is_exact_dup = (src_id in seen_source_ids)
        seen_source_ids.add(src_id)
        
        # Check alias matches
        mention_res = evaluate_video_mentions(title, desc, messi_aliases, ronaldo_aliases)
        
        v_dict = {
            "video_key": v_key,
            "source_video_id": src_id,
            "published_at": pub_dt.isoformat() if pub_dt else "",
            "published_at_dt": pub_dt,
            "duration_seconds": duration,
            "is_short": is_short,
            "title": title or "",
            "description": desc or "",
            "raw_response_id": raw_id or str(uuid.uuid4()),
            "api_request_id": api_req_id or str(uuid.uuid4()),
            "messi_matched": mention_res["messi_matched"],
            "ronaldo_matched": mention_res["ronaldo_matched"],
            "matched_messi_aliases": mention_res["matched_messi_aliases"],
            "matched_ronaldo_aliases": mention_res["matched_ronaldo_aliases"],
            "is_deduplicated_duplicate": is_exact_dup,
            "deduplication_cluster_id": src_id if is_exact_dup else None,
            "included_in_prevalence": (not is_exact_dup and is_short is False)
        }
        
        manifest_evidence_items.append(v_dict)

    # Apply cross-video deduplication rule if an approved versioned rule is configured
    if is_rule_approved:
        if dedup_rule_ver not in DEDUPLICATION_RULE_REGISTRY:
            raise DeduplicationRuleMissingError(
                f"Approved deduplication rule '{dedup_rule_ver}' has no registered execution logic. Fails closed."
            )
        rule_fn = DEDUPLICATION_RULE_REGISTRY[dedup_rule_ver]
        manifest_evidence_items = rule_fn(manifest_evidence_items)

    for v_dict in manifest_evidence_items:
        if v_dict.get("included_in_prevalence"):
            videos_180.append(v_dict)
            if v_dict["published_at_dt"] >= w90_start:
                videos_90.append(v_dict)

    # Minimum evidence resolution: 90d vs 180d fallback
    if len(videos_90) >= 10:
        effective_videos = videos_90
        effective_days = 90
        fallback_used = False
        ref_start = w90_start
    elif len(videos_180) >= 10:
        effective_videos = videos_180
        effective_days = 180
        fallback_used = True
        ref_start = w180_start
    else:
        effective_videos = videos_90
        effective_days = 90
        fallback_used = False
        ref_start = w90_start

    # Evaluate classifiers independently
    type_res = classify_channel_type(
        channel_id=source_channel_id,
        channel_name=channel_name,
        about_description=about_description,
        eligible_videos=effective_videos,
        strat_cfg=strat_cfg
    )
    if about_raw_response_id and "evidence" in type_res:
        type_res["evidence"]["about_raw_response_id"] = about_raw_response_id

    focus_res = classify_player_focus(
        eligible_videos=effective_videos,
        reference_period_end=reference_period_end,
        effective_window_days=effective_days,
        strat_cfg=strat_cfg
    )

    channel_type_val = type_res["channel_type"]
    player_focus_val = focus_res["player_focus"]
    type_conf = type_res["confidence"]
    focus_conf = focus_res["confidence"]

    # Compute manifest hash
    manifest_hash = compute_canonical_manifest_hash(manifest_evidence_items)
    assessment_id = str(uuid.uuid4())
    assessed_at_iso = datetime.now(timezone.utc).isoformat()

    # Insert into OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT
    cursor.execute('''
        INSERT INTO OPS.FACT_CHANNEL_CLASSIFICATION_ASSESSMENT (
            assessment_id, channel_key, assessed_at, assessment_trigger, event_version_key,
            classification_protocol_version, rule_config_version, rule_config_hash,
            evidence_manifest_hash, channel_type_value, player_focus_value,
            channel_type_confidence, player_focus_confidence, reference_period_start,
            reference_period_end, effective_window_days, eligible_video_count,
            messi_video_count, ronaldo_video_count, messi_prevalence, ronaldo_prevalence,
            focus_ratio, fallback_used, deduplication_rule_version, channel_type_evidence,
            player_focus_evidence, git_commit_sha, ingestion_run_id
        ) SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                 PARSE_JSON(%s), PARSE_JSON(%s), %s, %s
    ''', (
        assessment_id, channel_key, assessed_at_iso, assessment_trigger, event_version_key,
        protocol_version, "v1.1", config_hash, manifest_hash, channel_type_val, player_focus_val,
        type_conf, focus_conf, ref_start.isoformat(), reference_period_end.isoformat(),
        effective_days, len(effective_videos), focus_res["messi_count"], focus_res["ronaldo_count"],
        focus_res["messi_prevalence"], focus_res["ronaldo_prevalence"], focus_res["focus_ratio"],
        fallback_used, dedup_rule_ver, json.dumps(type_res["evidence"]), json.dumps(focus_res["evidence"]),
        git_sha, ingestion_run_id
    ))

    # Insert manifest items into OPS.FACT_CHANNEL_ASSESSMENT_EVIDENCE_ITEM
    for item in manifest_evidence_items:
        cursor.execute('''
            INSERT INTO OPS.FACT_CHANNEL_ASSESSMENT_EVIDENCE_ITEM (
                assessment_evidence_item_key, assessment_id, channel_key, video_key,
                source_video_id, published_at, duration_seconds, is_short, raw_response_id,
                api_request_id, messi_alias_matched, ronaldo_alias_matched,
                matched_player_aliases, matched_club_entities, matched_analysis_topics,
                is_deduplicated_duplicate, deduplication_cluster_id, included_in_prevalence
            ) SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), NULL, NULL, %s, %s, %s
        ''', (
            str(uuid.uuid4()), assessment_id, channel_key, item["video_key"],
            item["source_video_id"], item["published_at"], item["duration_seconds"],
            item["is_short"], item["raw_response_id"], item["api_request_id"],
            item["messi_matched"], item["ronaldo_matched"],
            json.dumps(item["matched_messi_aliases"] + item["matched_ronaldo_aliases"]),
            item["is_deduplicated_duplicate"], item.get("deduplication_cluster_id"), item["included_in_prevalence"]
        ))

    # Resolve composite stratum key from CORE.DIM_CHANNEL_STRATUM
    cursor.execute('''
        SELECT stratum_key FROM CORE.DIM_CHANNEL_STRATUM
        WHERE channel_type_value = %s AND player_focus_value = %s
    ''', (channel_type_val, player_focus_val))
    st_row = cursor.fetchone()
    if not st_row:
        raise ValueError(f"Composite stratum {channel_type_val} x {player_focus_val} not found in CORE.DIM_CHANNEL_STRATUM.")
    new_stratum_key = st_row[0]

    # Type-2 SCD Maintenance in CORE.DIM_CHANNEL_STRATUM_VERSION (Grain: channel x validity interval)
    cursor.execute('''
        SELECT stratum_version_key, stratum_key
        FROM CORE.DIM_CHANNEL_STRATUM_VERSION
        WHERE channel_key = %s AND is_current = TRUE
    ''', (channel_key,))
    curr_v = cursor.fetchone()
    
    if curr_v:
        curr_ver_key, curr_st_key = curr_v
        if curr_st_key != new_stratum_key:
            # Close old version
            cursor.execute('''
                UPDATE CORE.DIM_CHANNEL_STRATUM_VERSION
                SET is_current = FALSE, valid_to = %s
                WHERE stratum_version_key = %s
            ''', (assessed_at_iso, curr_ver_key))
            # Insert new version
            cursor.execute('''
                INSERT INTO CORE.DIM_CHANNEL_STRATUM_VERSION (
                    stratum_version_key, channel_key, stratum_key, valid_from, valid_to, is_current
                ) VALUES (%s, %s, %s, %s, NULL, TRUE)
            ''', (str(uuid.uuid4()), channel_key, new_stratum_key, assessed_at_iso))
    else:
        # Initial version
        cursor.execute('''
            INSERT INTO CORE.DIM_CHANNEL_STRATUM_VERSION (
                stratum_version_key, channel_key, stratum_key, valid_from, valid_to, is_current
            ) VALUES (%s, %s, %s, %s, NULL, TRUE)
        ''', (str(uuid.uuid4()), channel_key, new_stratum_key, assessed_at_iso))

    conn.commit()

    return {
        "assessment_id": assessment_id,
        "channel_type_value": channel_type_val,
        "player_focus_value": player_focus_val,
        "channel_type_confidence": type_conf,
        "player_focus_confidence": focus_conf,
        "reference_period_start": ref_start,
        "reference_period_end": reference_period_end,
        "effective_window_days": effective_days,
        "eligible_video_count": len(effective_videos),
        "messi_video_count": focus_res["messi_count"],
        "ronaldo_video_count": focus_res["ronaldo_count"],
        "messi_prevalence": focus_res["messi_prevalence"],
        "ronaldo_prevalence": focus_res["ronaldo_prevalence"],
        "focus_ratio": focus_res["focus_ratio"],
        "fallback_used": fallback_used,
        "evidence_manifest_hash": manifest_hash,
        "stratum_key": new_stratum_key,
        "about_description": about_description
    }


def create_or_verify_event_channel_snapshot(
    conn,
    event_version_key: str,
    channel_key: str,
    classification_protocol_version: str,
    assessment_result: dict,
    reference_period_start: datetime,
    reference_period_end: datetime,
    override_id: str = None
):
    """
    Pins the event-specific channel classification snapshot into
    CORE.BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT.
    
    Idempotency & Immutability Protocol:
    - If snapshot does not exist -> INSERT complete canonical payload.
    - If snapshot exists and complete payload matches -> Idempotent NO-OP.
    - If snapshot exists and any field differs -> Fail closed with SnapshotMutationViolationError!
    """
    cursor = conn.cursor()
    
    # Validate required statistical fields - do not silently default missing values
    required_stat_keys = [
        "channel_type_value", "player_focus_value", "channel_type_confidence",
        "player_focus_confidence", "effective_window_days", "eligible_video_count",
        "messi_video_count", "ronaldo_video_count", "messi_prevalence",
        "ronaldo_prevalence", "focus_ratio", "fallback_used"
    ]
    for k in required_stat_keys:
        if k not in assessment_result or assessment_result[k] is None:
            raise ValueError(f"Missing required assessment statistic '{k}' for snapshot creation.")

    # Rule: headline_decomposition_eligible = fully_resolved_axes AND fallback_used = FALSE
    ch_type = assessment_result["channel_type_value"]
    pl_focus = assessment_result["player_focus_value"]
    fallback = assessment_result["fallback_used"]
    headline_eligible = (ch_type != "UNCLASSIFIED" and pl_focus != "UNCLASSIFIED" and not fallback)

    candidate_payload = {
        "channel_type_value": ch_type,
        "player_focus_value": pl_focus,
        "channel_type_confidence": assessment_result["channel_type_confidence"],
        "player_focus_confidence": assessment_result["player_focus_confidence"],
        "reference_period_start": normalize_ntz_timestamp(reference_period_start),
        "reference_period_end": normalize_ntz_timestamp(reference_period_end),
        "effective_window_days": int(assessment_result["effective_window_days"]),
        "eligible_video_count": int(assessment_result["eligible_video_count"]),
        "messi_video_count": int(assessment_result["messi_video_count"]),
        "ronaldo_video_count": int(assessment_result["ronaldo_video_count"]),
        "messi_prevalence": round(float(assessment_result["messi_prevalence"]), 5),
        "ronaldo_prevalence": round(float(assessment_result["ronaldo_prevalence"]), 5),
        "focus_ratio": round(float(assessment_result["focus_ratio"]), 5),
        "fallback_used": bool(fallback),
        "headline_decomposition_eligible": bool(headline_eligible),
        "override_id": override_id
    }

    cursor.execute('''
        SELECT event_channel_stratum_snapshot_key, channel_type_value, player_focus_value,
               channel_type_confidence, player_focus_confidence, reference_period_start,
               reference_period_end, effective_window_days, eligible_video_count,
               messi_video_count, ronaldo_video_count, messi_prevalence, ronaldo_prevalence,
               focus_ratio, fallback_used, headline_decomposition_eligible, override_id
            FROM CORE.BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT
        WHERE event_version_key = %s
          AND channel_key = %s
          AND classification_protocol_version = %s
    ''', (event_version_key, channel_key, classification_protocol_version))
    existing = cursor.fetchone()

    if existing:
        ref_start_existing = normalize_ntz_timestamp(existing[5])
        ref_end_existing = normalize_ntz_timestamp(existing[6])
        existing_payload = {
            "channel_type_value": existing[1],
            "player_focus_value": existing[2],
            "channel_type_confidence": existing[3],
            "player_focus_confidence": existing[4],
            "reference_period_start": ref_start_existing,
            "reference_period_end": ref_end_existing,
            "effective_window_days": int(existing[7]) if existing[7] is not None else None,
            "eligible_video_count": int(existing[8]) if existing[8] is not None else None,
            "messi_video_count": int(existing[9]) if existing[9] is not None else None,
            "ronaldo_video_count": int(existing[10]) if existing[10] is not None else None,
            "messi_prevalence": round(float(existing[11]), 5) if existing[11] is not None else None,
            "ronaldo_prevalence": round(float(existing[12]), 5) if existing[12] is not None else None,
            "focus_ratio": round(float(existing[13]), 5) if existing[13] is not None else None,
            "fallback_used": bool(existing[14]),
            "headline_decomposition_eligible": bool(existing[15]),
            "override_id": existing[16]
        }
        if existing_payload == candidate_payload:
            return {
                "status": "IDEMPOTENT_NOOP",
                "snapshot_key": existing[0],
                "payload": existing_payload
            }
        else:
            raise SnapshotMutationViolationError(
                f"Cannot mutate existing event snapshot for event {event_version_key}, channel {channel_key}. "
                f"Existing: {existing_payload}, Attempted: {candidate_payload}"
            )

    snapshot_key = str(uuid.uuid4())
    cursor.execute('''
        INSERT INTO CORE.BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT (
            event_channel_stratum_snapshot_key, event_version_key, channel_key,
            classification_protocol_version, channel_type_value, player_focus_value,
            channel_type_confidence, player_focus_confidence, reference_period_start,
            reference_period_end, effective_window_days, eligible_video_count,
            messi_video_count, ronaldo_video_count, messi_prevalence, ronaldo_prevalence,
            focus_ratio, fallback_used, headline_decomposition_eligible, override_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', (
        snapshot_key, event_version_key, channel_key, classification_protocol_version,
        ch_type, pl_focus, assessment_result["channel_type_confidence"],
        assessment_result["player_focus_confidence"], normalize_ntz_timestamp(reference_period_start),
        normalize_ntz_timestamp(reference_period_end), assessment_result["effective_window_days"],
        assessment_result["eligible_video_count"], assessment_result["messi_video_count"],
        assessment_result["ronaldo_video_count"], round(float(assessment_result["messi_prevalence"]), 5),
        round(float(assessment_result["ronaldo_prevalence"]), 5), round(float(assessment_result["focus_ratio"]), 5),
        fallback, headline_eligible, override_id
    ))
    conn.commit()

    return {
        "status": "INSERTED",
        "snapshot_key": snapshot_key,
        "payload": candidate_payload
    }
