import os
import argparse
import json
import statistics
from ingestion_snowflake import get_snowflake_connection

def inspect_candidate_strata(conn, event_version_key: str, classification_protocol_version: str = "1.1"):
    """
    Descriptive-only candidate distribution reporter.
    Reports both aggregate stratum candidate totals and per-channel candidate distributions
    per (event_version_key x composite stratum x cohort_type).
    
    Zero recommendations. Zero premature capping assumptions.
    Strictly pre-comment scope.
    """
    cursor = conn.cursor()
    
    # Query candidate videos joined with event-channel stratum snapshot
    cursor.execute('''
        SELECT 
            b.cohort_type,
            COALESCE(s.channel_type_value || ' x ' || s.player_focus_value, 'UNCLASSIFIED') AS stratum_name,
            c.channel_key,
            COALESCE(c.channel_name, c.source_id) AS channel_display_name,
            COUNT(DISTINCT b.video_key) AS video_count
        FROM CORE.BRIDGE_VIDEO_EVENT b
        JOIN CORE.DIM_VIDEO v ON b.video_key = v.video_key
        JOIN CORE.DIM_CHANNEL c ON v.channel_key = c.channel_key
        LEFT JOIN CORE.BRIDGE_EVENT_CHANNEL_STRATUM_SNAPSHOT s
            ON s.event_version_key = b.event_version_key
           AND s.channel_key = c.channel_key
           AND s.classification_protocol_version = %s
        WHERE b.event_version_key = %s
          AND b.inclusion_status IN ('SELECTED', 'ELIGIBLE_UNSELECTED', 'ELIGIBLE')
        GROUP BY b.cohort_type, stratum_name, c.channel_key, channel_display_name
        ORDER BY b.cohort_type, stratum_name, channel_display_name
    ''', (classification_protocol_version, event_version_key))
    
    rows = cursor.fetchall()
    
    # Group by (stratum_name, cohort_type)
    strata_buckets = {}
    for cohort_type, stratum_name, ch_key, ch_name, v_count in rows:
        key = (stratum_name, cohort_type or "UNASSIGNED")
        strata_buckets.setdefault(key, []).append({
            "channel_key": ch_key,
            "channel_name": ch_name,
            "count": v_count
        })
        
    report = []
    for (st_name, cohort), channels in sorted(strata_buckets.items()):
        counts = [c["count"] for c in channels]
        total_candidates = sum(counts)
        num_channels = len(channels)
        
        # Calculate percentiles descriptively
        counts_sorted = sorted(counts)
        min_v = counts_sorted[0]
        max_v = counts_sorted[-1]
        med_v = statistics.median(counts_sorted)
        
        # Simple quartile estimation
        def get_percentile(data, p):
            k = (len(data) - 1) * p
            f = int(k)
            c = f + 1 if f + 1 < len(data) else f
            d = k - f
            return data[f] + (data[c] - data[f]) * d

        p25 = get_percentile(counts_sorted, 0.25)
        p75 = get_percentile(counts_sorted, 0.75)
        
        report.append({
            "event_version_key": event_version_key,
            "stratum_name": st_name,
            "cohort_type": cohort,
            "participating_channel_count": num_channels,
            "aggregate_candidate_count": total_candidates,
            "per_channel_distribution": {
                "min": min_v,
                "p25": p25,
                "median": med_v,
                "p75": p75,
                "max": max_v
            },
            "channel_breakdown": [
                {"channel_name": c["channel_name"], "candidate_count": c["count"]}
                for c in channels
            ]
        })
        
    return report


def print_descriptive_report(report: list):
    """
    Renders human-readable descriptive tables of candidate distributions.
    """
    if not report:
        print("No eligible candidates found for the specified event version.")
        return

    print("=" * 100)
    print("DESCRIPTIVE CANDIDATE STRATA REPORT (Grain: event x stratum x cohort_type)")
    print("=" * 100)
    print(f"{'Stratum Name':<45} | {'Cohort':<10} | {'Channels':<8} | {'Total':<6} | {'Min':<4} | {'p25':<5} | {'Med':<5} | {'p75':<5} | {'Max':<4}")
    print("-" * 100)
    for r in report:
        dist = r["per_channel_distribution"]
        print(f"{r['stratum_name']:<45} | {r['cohort_type']:<10} | {r['participating_channel_count']:<8} | "
              f"{r['aggregate_candidate_count']:<6} | {dist['min']:<4} | {dist['p25']:<5.1f} | {dist['median']:<5.1f} | {dist['p75']:<5.1f} | {dist['max']:<4}")
    print("=" * 100)
    print("NOTE: The above distributions are purely descriptive. K selection remains an explicit human decision.")
    print("=" * 100)


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description="Inspect candidate strata distributions descriptively.")
    parser.add_argument("--event-version-key", required=True, help="Target event_version_key in Snowflake")
    parser.add_argument("--target-db", default=os.getenv("TARGET_DATABASE", "FOOTBALL_NARRATIVE_DEV"))
    parser.add_argument("--protocol-version", default="1.1")
    parser.add_argument("--json", action="store_true", help="Output raw JSON instead of table")
    args = parser.parse_args()

    conn = get_snowflake_connection(target_db=args.target_db)
    report_data = inspect_candidate_strata(conn, args.event_version_key, args.protocol_version)
    conn.close()

    if args.json:
        print(json.dumps(report_data, indent=2))
    else:
        print_descriptive_report(report_data)
