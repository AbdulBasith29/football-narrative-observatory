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
        
        cursor.execute('''
            SELECT DISTINCT r.resource_scope_id
            FROM CORE.FACT_VIDEO_SNAPSHOT s
            JOIN OPS.FACT_INGESTION_RUN r ON s.ingestion_run_id = r.ingestion_run_id
            WHERE s.video_key = %s
              AND r.resource_scope_type = 'FRAME'
              AND r.endpoint = 'channels.list'
        ''', (v_key,))
        frame_candidates = cursor.fetchall()
        
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

def evaluate_video_eligibility(
    video, aliases, event_terms, event_occurred_at, windows=None, baseline_terms=None
):
    """
    Evaluates video eligibility strictly against frozen criteria:
    - Availability: resolution_status == 'RESOLVED'
    - Shorts: Decision A.1: is_short == False passes. is_short is None fails closed as SHORTS_UNRESOLVED.
    - Language: Decision A.3: UNKNOWN passes at metadata stage. Explicit non-English fails.
    - Temporal Windows & Overlap Precedence (docs/04-sampling-methodology.md Section 7.1):
      - Pure Baseline: [T-14d, T-24h) -> BASELINE if baseline relevance satisfied.
      - Overlap: [T-24h, T-1h] -> EVENT if event relevance satisfied; else BASELINE if baseline relevance satisfied; else ineligible.
      - Pure Event: (T-1h, T+72h] -> EVENT if event relevance satisfied.
      - Outside [T-14d, T+72h] -> OUT_OF_WINDOW.
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

    if pub_dt.tzinfo is None:
        pub_dt = pub_dt.replace(tzinfo=timezone.utc)
    if event_occurred_at.tzinfo is None:
        event_occurred_at = event_occurred_at.replace(tzinfo=timezone.utc)

    delta_seconds = (pub_dt - event_occurred_at).total_seconds()
    proximity_seconds = abs(delta_seconds)

    # Text relevance matching
    title = (video.get("title") or "").lower()
    desc = (video.get("description") or "").lower()
    combined_text = f"{title} {desc}"

    player_matched = any(re.search(r'\b' + re.escape(a.lower()) + r'\b', combined_text) for a in (aliases or []))
    baseline_term_matched = any(re.search(r'\b' + re.escape(b.lower()) + r'\b', combined_text) for b in (baseline_terms or []))
    baseline_relevance = player_matched or baseline_term_matched

    event_matched = any(re.search(r'\b' + re.escape(t.lower()) + r'\b', combined_text) for t in (event_terms or []))

    # Temporal boundary definitions in seconds:
    # T-14d = -14 * 86400 = -1,209,600
    # T-24h = -24 * 3600  = -86,400
    # T-1h  = -1 * 3600   = -3,600
    # T+72h = 72 * 3600   = 259,200
    SEC_14D = 14 * 86400
    SEC_24H = 24 * 3600
    SEC_1H = 1 * 3600
    SEC_72H = 72 * 3600

    # 1. Out of overall sampling window [T-14d, T+72h]
    if delta_seconds < -SEC_14D or delta_seconds > SEC_72H:
        return {
            "is_eligible": False,
            "cohort_type": None,
            "reason": "OUT_OF_WINDOW",
            "proximity_seconds": proximity_seconds,
            "event_matched": event_matched,
            "player_matched": player_matched
        }

    # 2. Pure Baseline interval: [T-14d, T-24h)
    if delta_seconds < -SEC_24H:
        if baseline_relevance:
            return {
                "is_eligible": True,
                "cohort_type": "BASELINE",
                "reason": None,
                "proximity_seconds": proximity_seconds,
                "event_matched": event_matched,
                "player_matched": player_matched
            }
        else:
            return {
                "is_eligible": False,
                "cohort_type": "BASELINE",
                "reason": "NO_TARGET_RELEVANCE",
                "proximity_seconds": proximity_seconds,
                "event_matched": event_matched,
                "player_matched": player_matched
            }

    # 3. Overlapping interval: [T-24h, T-1h]
    # Approved Rule: Mutually exclusive assignment.
    # If explicit EVENT relevance is satisfied -> EVENT.
    # Else if BASELINE relevance is satisfied -> BASELINE.
    # Else -> ineligible.
    if delta_seconds <= -SEC_1H:
        if event_matched:
            return {
                "is_eligible": True,
                "cohort_type": "EVENT",
                "reason": None,
                "proximity_seconds": proximity_seconds,
                "event_matched": True,
                "player_matched": player_matched
            }
        elif baseline_relevance:
            return {
                "is_eligible": True,
                "cohort_type": "BASELINE",
                "reason": None,
                "proximity_seconds": proximity_seconds,
                "event_matched": False,
                "player_matched": player_matched
            }
        else:
            return {
                "is_eligible": False,
                "cohort_type": None,
                "reason": "NO_TARGET_RELEVANCE",
                "proximity_seconds": proximity_seconds,
                "event_matched": False,
                "player_matched": player_matched
            }

    # 4. Pure Event interval: (T-1h, T+72h]
    if event_matched:
        return {
            "is_eligible": True,
            "cohort_type": "EVENT",
            "reason": None,
            "proximity_seconds": proximity_seconds,
            "event_matched": True,
            "player_matched": player_matched
        }
    else:
        return {
            "is_eligible": False,
            "cohort_type": "EVENT",
            "reason": "NO_TARGET_RELEVANCE",
            "proximity_seconds": proximity_seconds,
            "event_matched": False,
            "player_matched": player_matched
        }

def rank_and_select_cohorts(
    target_db="FOOTBALL_NARRATIVE_DEV",
    run_purpose="RESEARCH",
    frame_version_key=None,
    event_version_key=None,
    sampling_policy_version_key=None,
    sampling_policy_path="config/sampling_policy.yml",
    classification_protocol_version="1.2"
):
    """
    Deterministically ranks eligible videos and selects up to K videos per channel stratum.
    
    CRITICAL GATES:
    1. Test database isolation: INTEGRATION_TEST requires FOOTBALL_NARRATIVE_TEST exclusively.
    2. Fails closed if K is not explicitly approved in sampling_policy.yml.
    3. Fails closed if sampling_policy_version_key cannot be resolved uniquely.
    4. Scopes target entity aliases to this event via BRIDGE_EVENT_ENTITY.
    5. Fails closed if evaluated channels lack an event snapshot in BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT.
    """
    # Gate 1: Test Database Isolation
    if run_purpose == "INTEGRATION_TEST" and target_db != "FOOTBALL_NARRATIVE_TEST":
        raise ValueError(
            f"run_purpose 'INTEGRATION_TEST' requires target_db 'FOOTBALL_NARRATIVE_TEST' exclusively; got '{target_db}'."
        )
    if not frame_version_key or not event_version_key:
        raise ValueError("frame_version_key and event_version_key must be explicitly provided.")
    if run_purpose not in {"INTEGRATION_TEST", "RESEARCH"}:
        raise ValueError("run_purpose must be 'INTEGRATION_TEST' or 'RESEARCH'")
        
    # Gate 2: Check K approval
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
    
    # Gate 3: Structural isolation for RESEARCH
    if run_purpose == "RESEARCH":
        cursor.execute("SELECT frame_purpose FROM CORE.DIM_CHANNEL_FRAME_VERSION WHERE frame_version_key = %s", (frame_version_key,))
        row = cursor.fetchone()
        if not row or row[0] == "PIPELINE_PILOT":
            conn.close()
            raise ValueError("RESEARCH runs structurally reject PIPELINE_PILOT frames.")

    # Gate 4: Resolve and pin sampling_policy_version_key
    if not sampling_policy_version_key:
        cfg_sp_key = policy_cfg.get("sampling_policy_version_key")
        if cfg_sp_key:
            sampling_policy_version_key = cfg_sp_key
        else:
            pol_name = policy_cfg.get("policy_name") or f"Methodology v{policy_cfg.get('sampling_policy_version', '1.2')}"
            cursor.execute(
                "SELECT sampling_policy_version_key FROM CORE.DIM_SAMPLING_POLICY_VERSION WHERE policy_name = %s",
                (pol_name,)
            )
            sp_rows = cursor.fetchall()
            if len(sp_rows) == 1:
                sampling_policy_version_key = sp_rows[0][0]
            else:
                conn.close()
                raise ValueError(
                    f"Could not resolve unique sampling_policy_version_key for policy '{pol_name}' "
                    f"(found {len(sp_rows)} matches). Explicit sampling_policy_version_key is required."
                )
            
    # Fetch event occurred_at and terms
    cursor.execute('''
        SELECT occurred_at, event_terms 
        FROM CORE.DIM_EVENT_VERSION 
        WHERE event_version_key = %s
    ''', (event_version_key,))
    ev_row = cursor.fetchone()
    if not ev_row:
        conn.close()
        raise ValueError(f"Event version {event_version_key} not found.")
    occurred_at = ev_row[0].replace(tzinfo=timezone.utc) if ev_row[0] else None
    terms_raw = ev_row[1]
    event_terms = json.loads(terms_raw) if isinstance(terms_raw, str) else (terms_raw or [])
    
    # Fetch aliases scoped to THIS event via BRIDGE_EVENT_ENTITY
    cursor.execute('''
        SELECT a.alias_text
        FROM CORE.DIM_TARGET_ENTITY_ALIAS a
        JOIN CORE.BRIDGE_EVENT_ENTITY bee ON a.target_entity_key = bee.target_entity_key
        WHERE bee.event_version_key = %s
          AND a.is_active = TRUE
    ''', (event_version_key,))
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
        
    # Fetch candidate videos with explicit frame lineage AND pinned sampling_policy_version_key
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
          AND b.sampling_policy_version_key = %s
          AND fc.frame_version_key = %s
          AND fc.frame_inclusion_status = 'ELIGIBLE'
          AND (
              EXISTS (
                  SELECT 1 FROM TABLE(FLATTEN(input => b.discovery_provenance:queries)) q
                  WHERE q.value:frame_version_key::STRING = %s
              )
              OR (b.discovery_provenance:frame_version_key::STRING = %s)
          )
    ''', (event_version_key, sampling_policy_version_key, frame_version_key, frame_version_key, frame_version_key))
    
    candidates = cursor.fetchall()
    
    # Gate 5: Event-Channel Stratum Snapshot Prerequisite
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
        # 2. Target-player relevance (binary True/False)
        # 3. Proximity seconds (ascending - closer is better)
        # 4. Tracked channel eligibility (all in frame are eligible)
        # 5. Deterministic tie-break: published_at ASC, source_id ASC
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
        
        canonical_audit = {
            "frame_version_key": frame_version_key,
            "event_version_key": event_version_key,
            "event_channel_stratum_snapshot_key": st_snapshot_key,
            "cohort_type": cohort,
            "sampling_policy_version": policy_cfg.get("sampling_policy_version", "1.2"),
            "sampling_policy_version_key": sampling_policy_version_key,
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
