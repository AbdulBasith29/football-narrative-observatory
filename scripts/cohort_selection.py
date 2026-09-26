import os
import uuid
import json
import hashlib
import re
import yaml
from datetime import datetime, timezone
from ingestion_snowflake import get_snowflake_connection

def repair_legacy_uploads_frame_lineage(conn, target_frame_version_key: str):
    """
    Deterministically repairs legacy uploads_playlist records in CORE.BRIDGE_VIDEO_EVENT
    where frame_version_key was omitted during Phase 1C.
    
    Lineage trace:
    BRIDGE_VIDEO_EVENT.video_key
        -> FACT_VIDEO_SNAPSHOT.video_key
        -> FACT_VIDEO_SNAPSHOT.ingestion_run_id
        -> OPS.FACT_INGESTION_RUN.ingestion_run_id
    Where:
        FACT_INGESTION_RUN.resource_scope_type = 'FRAME'
        FACT_INGESTION_RUN.endpoint = 'channels.list'
    The run's resource_scope_id supplies the candidate frame_version_key.
    
    Backfills explicit frame provenance ONLY where exactly one originating frame
    can be established safely. If 0 or >1 frames remain plausible, does not guess.
    """
    cursor = conn.cursor()
    cursor.execute('''
        SELECT b.video_event_key, b.video_key, b.discovery_provenance
        FROM CORE.BRIDGE_VIDEO_EVENT b
        WHERE b.discovery_method = 'uploads_playlist'
          AND (
              b.discovery_provenance:frame_version_key IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM TABLE(FLATTEN(input => b.discovery_provenance:queries)) q
                  WHERE q.value:frame_version_key IS NOT NULL
              )
          )
    ''')
    unrepaired_rows = cursor.fetchall()
    
    repaired_count = 0
    unresolved_count = 0
    
    for ve_key, v_key, prov_raw in unrepaired_rows:
        prov = json.loads(prov_raw) if isinstance(prov_raw, str) else (prov_raw or {})
        
        # Trace originating run through snapshots
        cursor.execute('''
            SELECT DISTINCT r.resource_scope_id
            FROM CORE.FACT_VIDEO_SNAPSHOT s
            JOIN OPS.FACT_INGESTION_RUN r ON s.ingestion_run_id = r.ingestion_run_id
            WHERE s.video_key = %s
              AND r.resource_scope_type = 'FRAME'
              AND r.endpoint = 'channels.list'
        ''', (v_key,))
        frame_candidates = cursor.fetchall()
        
        # Exactly one plausible originating frame required
        if len(frame_candidates) == 1 and frame_candidates[0][0] == target_frame_version_key:
            prov["frame_version_key"] = target_frame_version_key
            cursor.execute('''
                UPDATE CORE.BRIDGE_VIDEO_EVENT
                SET discovery_provenance = PARSE_JSON(%s)
                WHERE video_event_key = %s
            ''', (json.dumps(prov), ve_key))
            repaired_count += 1
        else:
            unresolved_count += 1
            
    conn.commit()
    return {"repaired": repaired_count, "unresolved": unresolved_count}

def evaluate_video_eligibility(video, aliases, event_terms, event_occurred_at, windows):
    """
    Evaluates video eligibility strictly against frozen criteria:
    - Availability: resolution_status == 'RESOLVED'
    - Shorts: Decision A.1: is_short == False passes. is_short is None fails closed as SHORTS_UNRESOLVED.
    - Language: Decision A.3: UNKNOWN passes at metadata stage. Explicit non-English fails.
    - Publication Window: Baseline vs Event windows.
    - Relevance: Binary match on event_terms and target aliases.
    """
    res_status = video.get("resolution_status")
    if res_status == "UNAVAILABLE":
        return {
            "is_eligible": False,
            "cohort_type": None,
            "reason": "VIDEO_UNAVAILABLE",
            "proximity_seconds": None,
            "event_matched": False,
            "player_matched": False
        }
        
    is_short = video.get("is_short")
    if is_short is True:
        return {
            "is_eligible": False,
            "cohort_type": None,
            "reason": "YOUTUBE_SHORT",
            "proximity_seconds": None,
            "event_matched": False,
            "player_matched": False
        }
    if is_short is None:
        # Decision A.1 Fail-closed semantics
        return {
            "is_eligible": False,
            "cohort_type": None,
            "reason": "SHORTS_UNRESOLVED",
            "proximity_seconds": None,
            "event_matched": False,
            "player_matched": False
        }
        
    lang = (video.get("primary_language_code") or "UNKNOWN").lower()
    # Explicit non-English disqualified; UNKNOWN or English permitted
    if lang != "unknown" and not lang.startswith("en"):
        return {
            "is_eligible": False,
            "cohort_type": None,
            "reason": "NON_ENGLISH_METADATA",
            "proximity_seconds": None,
            "event_matched": False,
            "player_matched": False
        }
        
    pub_at = video.get("published_at")
    if isinstance(pub_at, str):
        pub_dt = datetime.fromisoformat(pub_at.replace("Z", "+00:00"))
    else:
        pub_dt = pub_at
        
    if not pub_dt or not event_occurred_at:
        return {
            "is_eligible": False,
            "cohort_type": None,
            "reason": "OUT_OF_WINDOW",
            "proximity_seconds": None,
            "event_matched": False,
            "player_matched": False
        }
        
    delta_seconds = abs((pub_dt - event_occurred_at).total_seconds())
    
    # Check publication window
    matched_window_type = None
    for w in windows:
        if w["start"] <= pub_dt <= w["end"]:
            matched_window_type = w["window_type"]
            break
            
    if not matched_window_type:
        return {
            "is_eligible": False,
            "cohort_type": None,
            "reason": "OUT_OF_WINDOW",
            "proximity_seconds": delta_seconds,
            "event_matched": False,
            "player_matched": False
        }
        
    cohort_type = "BASELINE" if matched_window_type == "PRE_EVENT" else "EVENT"
    
    # Binary relevance matching: title and description evaluated equally (no title-over-description precedence)
    title = (video.get("title") or "").lower()
    desc = (video.get("description") or "").lower()
    combined_text = f"{title} {desc}"
    
    player_matched = any(re.search(r'\b' + re.escape(a.lower()) + r'\b', combined_text) for a in aliases)
    event_matched = any(re.search(r'\b' + re.escape(t.lower()) + r'\b', combined_text) for t in event_terms)
    
    if not (player_matched and event_matched):
        return {
            "is_eligible": False,
            "cohort_type": cohort_type,
            "reason": "NO_TARGET_RELEVANCE",
            "proximity_seconds": delta_seconds,
            "event_matched": event_matched,
            "player_matched": player_matched
        }
        
    return {
        "is_eligible": True,
        "cohort_type": cohort_type,
        "reason": None,
        "proximity_seconds": delta_seconds,
        "event_matched": True,
        "player_matched": True
    }

def rank_and_select_cohorts(
    target_db="FOOTBALL_NARRATIVE_DEV",
    run_purpose="RESEARCH",
    frame_version_key=None,
    event_version_key=None,
    sampling_policy_path="config/sampling_policy.yml",
    classification_protocol_version="1.2"
):
    """
    Deterministically ranks eligible videos and selects up to K videos per channel stratum.
    
    CRITICAL GATES:
    1. Fails closed if K is not explicitly approved in sampling_policy.yml.
    2. Fails closed if evaluated channels lack an event snapshot in BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT.
    """
    if not frame_version_key or not event_version_key:
        raise ValueError("frame_version_key and event_version_key must be explicitly provided.")
    if run_purpose not in {"INTEGRATION_TEST", "RESEARCH"}:
        raise ValueError("run_purpose must be 'INTEGRATION_TEST' or 'RESEARCH'")
        
    # Gate 1: Check K approval
    with open(sampling_policy_path, "r", encoding="utf-8") as f:
        policy_cfg = yaml.safe_load(f) or {}
        
    k_capacity = policy_cfg.get("k_capacity")
    k_status = policy_cfg.get("k_capacity_status")
    if k_capacity is None or k_status != "APPROVED":
        raise ValueError(
            "Cohort selection is GATED: sampling_policy.yml contains unapproved K capacity "
            f"(k_capacity={k_capacity}, status={k_status}). Human approval required before cohort execution."
        )
        
    conn = get_snowflake_connection(target_db=target_db)
    cursor = conn.cursor()
    
    # Gate 2: Structural isolation
    if run_purpose == "RESEARCH":
        cursor.execute("SELECT frame_purpose FROM CORE.DIM_CHANNEL_FRAME_VERSION WHERE frame_version_key = %s", (frame_version_key,))
        row = cursor.fetchone()
        if not row or row[0] == "PIPELINE_PILOT":
            raise ValueError("RESEARCH runs structurally reject PIPELINE_PILOT frames.")
            
    # Fetch event occurred_at and terms
    cursor.execute('''
        SELECT occurred_at, event_terms 
        FROM CORE.DIM_EVENT_VERSION 
        WHERE event_version_key = %s
    ''', (event_version_key,))
    ev_row = cursor.fetchone()
    if not ev_row:
        raise ValueError(f"Event version {event_version_key} not found.")
    occurred_at = ev_row[0].replace(tzinfo=timezone.utc) if ev_row[0] else None
    terms_raw = ev_row[1]
    event_terms = json.loads(terms_raw) if isinstance(terms_raw, str) else (terms_raw or [])
    
    # Fetch aliases
    cursor.execute("SELECT alias_text FROM CORE.DIM_TARGET_ENTITY_ALIAS WHERE is_active = TRUE")
    aliases = [r[0].lower() for r in cursor.fetchall()]
    
    # Fetch windows
    cursor.execute('''
        SELECT window_key, window_type, absolute_start_at, absolute_end_at
        FROM CORE.DIM_EVENT_WINDOW
        WHERE event_version_key = %s
    ''', (event_version_key,))
    windows = []
    for r in cursor.fetchall():
        windows.append({
            "window_key": r[0],
            "window_type": r[1],
            "start": r[2].replace(tzinfo=timezone.utc) if r[2] else None,
            "end": r[3].replace(tzinfo=timezone.utc) if r[3] else None
        })
        
    # Fetch candidate videos with explicit frame lineage
    cursor.execute('''
        SELECT DISTINCT
            v.video_key, v.source_id, v.channel_key, v.published_at, v.duration_seconds,
            v.is_short, v.primary_language_code, b.video_event_key, b.sampling_policy_version_key,
            r.resolution_status, s.title, s.description
        FROM CORE.DIM_VIDEO v
        JOIN CORE.BRIDGE_VIDEO_EVENT b ON v.video_key = b.video_key
        JOIN CORE.BRIDGE_FRAME_CHANNEL fc ON fc.channel_key = v.channel_key
        LEFT JOIN OPS.VIDEO_METADATA_RESOLUTION_STATE r ON r.source_system = 'YOUTUBE' AND r.source_id = v.source_id
        LEFT JOIN (
            SELECT video_key, title, description,
                   ROW_NUMBER() OVER (PARTITION BY video_key ORDER BY observed_at DESC) as rn
            FROM CORE.FACT_VIDEO_SNAPSHOT
        ) s ON s.video_key = v.video_key AND s.rn = 1
        WHERE b.event_version_key = %s
          AND fc.frame_version_key = %s
          AND fc.frame_inclusion_status = 'ELIGIBLE'
          AND (
              EXISTS (
                  SELECT 1 FROM TABLE(FLATTEN(input => b.discovery_provenance:queries)) q
                  WHERE q.value:frame_version_key::STRING = %s
              )
              OR (b.discovery_provenance:frame_version_key::STRING = %s)
          )
    ''', (event_version_key, frame_version_key, frame_version_key, frame_version_key))
    
    candidates = cursor.fetchall()
    
    # Check Gate 3: Event-Channel Stratum Snapshot Prerequisite
    channel_keys = {c[2] for c in candidates}
    channel_to_stratum = {}
    
    for ch_key in channel_keys:
        cursor.execute('''
            SELECT event_channel_stratum_snapshot_key, channel_type_value, player_focus_value
            FROM CORE.BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT
            WHERE event_version_key = %s
              AND channel_key = %s
              AND classification_protocol_version = %s
        ''', (event_version_key, ch_key, classification_protocol_version))
        st_row = cursor.fetchone()
        if not st_row:
            conn.close()
            raise ValueError(
                f"Cohort selection is GATED: Channel {ch_key} lacks active "
                f"BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT for event {event_version_key}. "
                "Event-relative channel classification must exist prior to cohort selection."
            )
        channel_to_stratum[ch_key] = {
            "snapshot_key": st_row[0],
            "stratum_name": f"{st_row[1]}_x_{st_row[2]}"
        }
        
    # Group eligible candidates by stratum x cohort
    strata_cohort_buckets = {}
    
    for c in candidates:
        v_key, source_id, ch_key, pub_at, duration, is_short, lang, ve_key, sp_key, res_st, title, desc = c
        v_dict = {
            "source_id": source_id,
            "published_at": pub_at.replace(tzinfo=timezone.utc) if pub_at else None,
            "duration_seconds": duration,
            "is_short": is_short,
            "primary_language_code": lang,
            "resolution_status": res_st,
            "title": title,
            "description": desc
        }
        
        eval_res = evaluate_video_eligibility(v_dict, aliases, event_terms, occurred_at, windows)
        stratum_info = channel_to_stratum[ch_key]
        bucket_key = (stratum_info["snapshot_key"], stratum_info["stratum_name"], eval_res["cohort_type"])
        
        if not eval_res["is_eligible"]:
            # Update as INELIGIBLE
            cursor.execute('''
                UPDATE CORE.BRIDGE_VIDEO_EVENT
                SET inclusion_status = 'INELIGIBLE',
                    primary_exclusion_reason = %s,
                    cohort_type = %s
                WHERE video_event_key = %s
            ''', (eval_res["reason"], eval_res["cohort_type"], ve_key))
        else:
            strata_cohort_buckets.setdefault(bucket_key, []).append({
                "video_event_key": ve_key,
                "video_key": v_key,
                "video_id": source_id,
                "published_at": v_dict["published_at"].isoformat() if v_dict["published_at"] else "",
                "proximity_seconds": eval_res["proximity_seconds"],
                "event_matched": eval_res["event_matched"],
                "player_matched": eval_res["player_matched"]
            })
            
    # Deterministic 5-step ranking within stratum x cohort
    for (st_snapshot_key, st_name, cohort), cand_list in strata_cohort_buckets.items():
        # Frozen 5-step ranking:
        # 1. Event relevance (binary True/False)
        # 2. Player relevance (binary True/False)
        # 3. Proximity seconds (ascending - closer is better)
        # 4. Channel eligibility (all in frame are eligible)
        # 5. Tie-break: published_at ASC, source_id ASC
        sorted_candidates = sorted(
            cand_list,
            key=lambda x: (
                0 if x["event_matched"] else 1,
                0 if x["player_matched"] else 1,
                x["proximity_seconds"] or 999999999,
                x["published_at"] or "",
                x["video_id"]
            )
        )
        
        # Canonical serialization for candidate_list_lineage
        canonical_audit = {
            "frame_version_key": frame_version_key,
            "event_version_key": event_version_key,
            "event_channel_stratum_snapshot_key": st_snapshot_key,
            "cohort_type": cohort,
            "sampling_policy_version": policy_cfg.get("sampling_policy_version", "1.2"),
            "k_capacity": k_capacity,
            "ranking_algorithm_version": "frozen_v1.2_binary_relevance_five_criteria",
            "ordered_candidates": [
                {
                    "rank": idx + 1,
                    "video_id": c["video_id"],
                    "published_at": c["published_at"],
                    "proximity_seconds": c["proximity_seconds"]
                }
                for idx, c in enumerate(sorted_candidates)
            ]
        }
        canonical_json = json.dumps(canonical_audit, sort_keys=True)
        lineage_hash = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
        
        for idx, cand in enumerate(sorted_candidates):
            rank = idx + 1
            if rank <= k_capacity:
                status = "SELECTED"
                reason = None
                rationale = f"Selected: Rank {rank} in stratum {st_name} cohort {cohort}"
            else:
                status = "ELIGIBLE_UNSELECTED"
                reason = "EXCEEDS_STRATUM_CAP_K"
                rationale = f"Unselected: Rank {rank} exceeds stratum cap K={k_capacity}"
                
            cursor.execute('''
                UPDATE CORE.BRIDGE_VIDEO_EVENT
                SET cohort_type = %s,
                    selection_rank = %s,
                    inclusion_status = %s,
                    primary_exclusion_reason = %s,
                    inclusion_rationale = %s,
                    candidate_list_lineage = %s
                WHERE video_event_key = %s
            ''', (cohort, rank, status, reason, rationale, lineage_hash, cand["video_event_key"]))
            
    conn.commit()
    conn.close()
    return {
        "status": "COMPLETE",
        "strata_processed": len(strata_cohort_buckets)
    }
