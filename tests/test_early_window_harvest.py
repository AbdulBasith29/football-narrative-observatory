"""tests/test_early_window_harvest.py
Offline tests for EARLY_WINDOW comment collection (scripts/harvest_pilot_comments.py).

Validates:
1. Early-window manifests: v2 schema, window anchored on each video's published_at, explicit request
   budget, cost estimate from discovery-time commentCount, and backward compatibility with v1 manifests.
2. Coverage classification: complete retrieval vs budget truncation (before / within the window),
   errors and unavailable videos; window counts are flagged complete only when coverage is COMPLETE_*.
3. Likely-pinned first items do not move the frontier.
4. Raw pages and per-page checkpoints are preserved; resuming continues from the saved page token and
   skips videos already covered completely.
5. Request budgets per video and per run are enforced.
6. Verification checks window flags and coverage/checkpoint consistency.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts")))

import harvest_pilot_comments as hpc  # noqa: E402

SECRET = "test-secret"
VIDEO_PUB = "2023-01-03T16:00:00Z"   # window 72h -> [2023-01-03T16:00Z, 2023-01-06T16:00Z)


def _candidate(vid, published=VIDEO_PUB, comment_count="150", player="RONALDO"):
    return {"video_id": vid, "channel_id": "UC1", "channel_title": "Chan", "title": f"Title {vid}",
            "published_at": published, "status": "ELIGIBLE", "relevance_tier": "PLAYER_MATCH",
            "player_category": player, "comment_count": comment_count}


def _report(cands, event_id="TRANSFER_AL_NASSR_2023"):
    return {"report_metadata": {"pipeline_frame": "PIPELINE_PILOT", "execution_mode": "LIVE_YOUTUBE_API",
                                "event_id": event_id, "report_generated_at": "2026-10-20T00:00:00Z",
                                "window_start": "2023-01-03T15:00:00+00:00", "window_end": "2023-01-06T16:00:00+00:00"},
            "evaluated_candidates": cands}


def _manifest(vids=("v1",), window_hours=72, cap=5, **kw):
    return hpc.build_early_window_manifest(_report([_candidate(v, **kw) for v in vids]), "reports/r.json", "abc",
                                           "al-nassr-early-test", window_hours, cap)


def _thread(cid, vid, published, text="t", likes=0):
    snippet = {"videoId": vid, "textOriginal": text, "textDisplay": text, "publishedAt": published,
               "updatedAt": published, "likeCount": likes, "authorChannelId": {"value": "UCa"}}
    return {"id": cid, "snippet": {"videoId": vid, "totalReplyCount": 0,
                                   "topLevelComment": {"id": cid, "snippet": snippet}}}


def _page(items, next_token=None):
    body = {"items": items}
    if next_token:
        body["nextPageToken"] = next_token
    return 200, json.dumps(body).encode("utf-8")


def _error(status, reason):
    return status, json.dumps({"error": {"code": status, "errors": [{"reason": reason}]}}).encode("utf-8")


class ScriptedFetcher:
    def __init__(self, script: Dict[Tuple[str, Optional[str]], Tuple[Optional[int], bytes]]):
        self.script = script
        self.calls: List[Dict[str, Any]] = []

    def __call__(self, params):
        self.calls.append(dict(params))
        return self.script[(params["videoId"], params.get("pageToken"))]


LATE = [_thread(f"late{i}", "v1", f"2025-0{3 - i}-01T00:00:00Z") for i in range(3)]          # newest first, after window
IN_WINDOW = [_thread("w2", "v1", "2023-01-05T10:00:00Z"), _thread("w1", "v1", "2023-01-03T16:30:00Z")]
POST_WINDOW = [_thread("p1", "v1", "2023-01-10T00:00:00Z")]


# =============================================================================
# 1. Manifest
# =============================================================================

def test_early_window_manifest_shape_and_estimate():
    report = _report([_candidate("a", comment_count="250"), _candidate("b", comment_count="40"),
                      _candidate("c", comment_count=None)])
    m = hpc.build_early_window_manifest(report, "r.json", "h", "v1", 72, 2)
    assert m["schema_version"] == hpc.MANIFEST_SCHEMA_VERSION_V2
    assert m["event_id"] == "TRANSFER_AL_NASSR_2023"
    assert m["scope"]["mode"] == hpc.SCOPE_EARLY_WINDOW
    assert m["scope"]["window_anchor"] == "VIDEO_PUBLISHED_AT" and m["scope"]["window_hours"] == 72
    assert m["budget"]["max_requests_per_video"] == 2 and m["budget"]["max_total_requests"] == 6
    est = m["cost_estimate"]
    by_vid = {p["video_id"]: p for p in est["per_video"]}
    assert by_vid["a"]["requests_for_full_retrieval_upper_bound"] == 3 and by_vid["a"]["planned_max_requests"] == 2
    assert by_vid["b"]["requests_for_full_retrieval_upper_bound"] == 1 and not by_vid["b"]["expected_budget_truncation"]
    assert by_vid["c"]["planned_max_requests"] == 2 and by_vid["c"]["expected_budget_truncation"]
    assert est["planned_max_requests"] == 5
    assert est["videos_expected_budget_truncated"] == 2 and est["videos_with_unknown_comment_count"] == 1
    hpc.verify_manifest_integrity(m)
    assert "not total discussion volume" in m["scope"]["interpretation"]


def test_v1_newest_manifest_still_verifies():
    report = _report([_candidate("a")])
    m = hpc.build_manifest(report, "r.json", "h", "pilot-comments-v1", 100)
    assert m["schema_version"] == hpc.MANIFEST_SCHEMA_VERSION and "mode" not in m["scope"]
    hpc.verify_manifest_integrity(m)
    assert hpc.manifest_scope_mode(m) == hpc.SCOPE_NEWEST_N


def test_manifest_schema_mode_mismatch_rejected():
    m = _manifest()
    m["schema_version"] = hpc.MANIFEST_SCHEMA_VERSION
    m["manifest_sha256"] = hpc.canonical_json_hash({k: v for k, v in m.items() if k != "manifest_sha256"})
    with pytest.raises(ValueError, match="do not match"):
        hpc.verify_manifest_integrity(m)


@pytest.mark.parametrize("hours,cap", [(0, 5), (72, 0)])
def test_manifest_rejects_invalid_window_or_cap(hours, cap):
    with pytest.raises(ValueError):
        hpc.build_early_window_manifest(_report([_candidate("a")]), "r", "h", "v", hours, cap)


def test_manifest_requires_parseable_video_published_at():
    with pytest.raises(ValueError, match="published_at"):
        hpc.build_early_window_manifest(_report([_candidate("a", published="")]), "r", "h", "v", 72, 5)


# =============================================================================
# 2. Coverage classification
# =============================================================================

def test_complete_retrieval_counts_window_comments(tmp_path):
    m = _manifest(cap=5)
    f = ScriptedFetcher({("v1", None): _page(LATE + POST_WINDOW, "T2"), ("v1", "T2"): _page(IN_WINDOW)})
    rec = hpc.run_harvest(m, f, str(tmp_path), SECRET, "Abdul", run_id="r1")
    o = rec["video_outcomes"][0]
    assert o["coverage_status"] == hpc.COVERAGE_COMPLETE_ALL_PAGES and o["window_count_is_complete"]
    assert o["in_window_comments_observed"] == 2 and o["out_of_window_comments_observed"] == 4
    assert all(c["maxResults"] == 100 for c in f.calls)
    assert rec["run_status"] == "COMPLETED" and rec["in_window_comments_observed"] == 2
    store = json.loads((tmp_path / "comment_store.json").read_text())
    assert store["comments"]["w1"]["in_early_window"] is True and store["comments"]["p1"]["in_early_window"] is False
    assert store["video_coverage"]["v1"]["coverage_status"] == hpc.COVERAGE_COMPLETE_ALL_PAGES
    assert hpc.verify_store(str(tmp_path), m)["passed"]


def test_window_boundaries_start_inclusive_end_exclusive():
    start, end = hpc.early_window_bounds({"video_id": "v", "video_published_at": VIDEO_PUB}, 72)
    assert hpc.in_early_window("2023-01-03T16:00:00Z", start, end)
    assert not hpc.in_early_window("2023-01-06T16:00:00Z", start, end)
    assert not hpc.in_early_window("2023-01-03T15:59:59Z", start, end)


def test_budget_truncated_before_window(tmp_path):
    m = _manifest(cap=1)
    f = ScriptedFetcher({("v1", None): _page(LATE, "T2")})
    rec = hpc.run_harvest(m, f, str(tmp_path), SECRET, "Abdul", run_id="r1")
    o = rec["video_outcomes"][0]
    assert o["coverage_status"] == hpc.COVERAGE_TRUNCATED_BEFORE_WINDOW
    assert o["window_count_is_complete"] is False and o["in_window_comments_observed"] == 0
    assert o["next_page_token_saved"] and o["window_covered_from"] is None
    assert rec["run_status"] == "COMPLETED_BUDGET_TRUNCATED" and len(f.calls) == 1
    cp = json.loads((tmp_path / "runs" / "r1" / "checkpoints" / "v1.json").read_text())
    assert cp["next_page_token"] == "T2" and cp["coverage_status"] == hpc.COVERAGE_TRUNCATED_BEFORE_WINDOW
    assert cp["frontier_published_at"] == "2025-01-01T00:00:00+00:00"
    assert hpc.verify_store(str(tmp_path), m)["passed"]


def test_budget_truncated_within_window(tmp_path):
    m = _manifest(cap=2)
    f = ScriptedFetcher({("v1", None): _page(LATE, "T2"), ("v1", "T2"): _page(IN_WINDOW[:1], "T3")})
    rec = hpc.run_harvest(m, f, str(tmp_path), SECRET, "Abdul", run_id="r1")
    o = rec["video_outcomes"][0]
    assert o["coverage_status"] == hpc.COVERAGE_TRUNCATED_WITHIN_WINDOW
    assert o["window_covered_from"] == "2023-01-05T10:00:00+00:00"
    assert o["in_window_comments_observed"] == 1 and not o["window_count_is_complete"]
    assert len(f.calls) == 2


def test_frontier_before_window_start_completes_early(tmp_path):
    m = _manifest(cap=5)
    early = [_thread("pre", "v1", "2023-01-03T15:00:00Z")]  # before upload (bad data) passes the window start
    f = ScriptedFetcher({("v1", None): _page(IN_WINDOW + early, "T2")})
    rec = hpc.run_harvest(m, f, str(tmp_path), SECRET, "Abdul", run_id="r1")
    assert rec["video_outcomes"][0]["coverage_status"] == hpc.COVERAGE_COMPLETE_WINDOW_PASSED
    assert len(f.calls) == 1


def test_likely_pinned_item_does_not_move_frontier(tmp_path):
    m = _manifest(cap=1)
    pinned = _thread("pin", "v1", "2023-01-03T17:00:00Z", likes=900)
    f = ScriptedFetcher({("v1", None): _page([pinned] + LATE, "T2")})
    rec = hpc.run_harvest(m, f, str(tmp_path), SECRET, "Abdul", run_id="r1")
    o = rec["video_outcomes"][0]
    assert o["coverage_status"] == hpc.COVERAGE_TRUNCATED_BEFORE_WINDOW   # not WITHIN: pinned item ignored
    assert o["in_window_comments_observed"] == 1
    store = json.loads((tmp_path / "comment_store.json").read_text())
    assert store["comments"]["pin"]["likely_pinned"] is True and store["comments"]["pin"]["in_early_window"] is True
    assert store["comments"]["late0"]["likely_pinned"] is False


@pytest.mark.parametrize("response,coverage,outcome", [
    (_error(403, "commentsDisabled"), hpc.COVERAGE_UNAVAILABLE, hpc.OUTCOME_COMMENTS_DISABLED),
    (_error(404, "videoNotFound"), hpc.COVERAGE_UNAVAILABLE, hpc.OUTCOME_VIDEO_NOT_FOUND),
    (_error(403, "forbidden"), hpc.COVERAGE_INCOMPLETE_ERROR, hpc.OUTCOME_ERROR),
])
def test_unavailable_and_error_coverage(tmp_path, response, coverage, outcome):
    m = _manifest(cap=3)
    rec = hpc.run_harvest(m, ScriptedFetcher({("v1", None): response}), str(tmp_path), SECRET, "Abdul", run_id="r1")
    o = rec["video_outcomes"][0]
    assert o["coverage_status"] == coverage and o["outcome"] == outcome and not o["window_count_is_complete"]


def test_error_after_page_is_incomplete_not_truncated(tmp_path):
    m = _manifest(cap=5)
    f = ScriptedFetcher({("v1", None): _page(LATE, "T2"), ("v1", "T2"): _error(500, "backendError")})
    rec = hpc.run_harvest(m, f, str(tmp_path), SECRET, "Abdul", run_id="r1")
    o = rec["video_outcomes"][0]
    assert o["outcome"] == hpc.OUTCOME_PARTIAL_ERROR and o["coverage_status"] == hpc.COVERAGE_INCOMPLETE_ERROR
    assert o["comments_collected"] == 3 and rec["run_status"] == "COMPLETED_WITH_ERRORS"


def test_quota_stop_marks_remaining_not_attempted(tmp_path):
    m = _manifest(vids=("v1", "v2"), cap=3)
    f = ScriptedFetcher({("v1", None): _error(403, "quotaExceeded")})
    rec = hpc.run_harvest(m, f, str(tmp_path), SECRET, "Abdul", run_id="r1")
    assert [o["coverage_status"] for o in rec["video_outcomes"]] == [hpc.COVERAGE_INCOMPLETE_ERROR, hpc.COVERAGE_NOT_ATTEMPTED]
    assert rec["run_status"] == "STOPPED_QUOTA_LIMIT" and len(f.calls) == 1


def test_run_budget_never_exceeded(tmp_path):
    m = _manifest(vids=("v1", "v2"), cap=2)
    script = {}
    for vid in ("v1", "v2"):
        script[(vid, None)] = _page([_thread(f"{vid}a", vid, "2025-01-01T00:00:00Z")], "T1")
        script[(vid, "T1")] = _page([_thread(f"{vid}b", vid, "2024-01-01T00:00:00Z")], "T2")
        script[(vid, "T2")] = _page([_thread(f"{vid}c", vid, "2023-06-01T00:00:00Z")], "T3")
    f = ScriptedFetcher(script)
    rec = hpc.run_harvest(m, f, str(tmp_path), SECRET, "Abdul", run_id="r1")
    assert len(f.calls) == 4 == m["budget"]["max_total_requests"]
    assert all(o["pages_requested"] == 2 for o in rec["video_outcomes"])


# =============================================================================
# 3. Resume from checkpoints
# =============================================================================

def test_resume_continues_from_saved_token_and_skips_complete(tmp_path):
    m = _manifest(vids=("v1", "v2"), cap=1)
    run1 = ScriptedFetcher({("v1", None): _page(LATE, "T2"),
                            ("v2", None): _page([_thread("v2a", "v2", "2023-01-04T00:00:00Z")])})
    rec1 = hpc.run_harvest(m, run1, str(tmp_path), SECRET, "Abdul", run_id="r1")
    assert [o["coverage_status"] for o in rec1["video_outcomes"]] == [
        hpc.COVERAGE_TRUNCATED_BEFORE_WINDOW, hpc.COVERAGE_COMPLETE_ALL_PAGES]

    run2 = ScriptedFetcher({("v1", "T2"): _page(IN_WINDOW)})
    rec2 = hpc.run_harvest(m, run2, str(tmp_path), SECRET, "Abdul", run_id="r2", resume_from_run="r1")
    assert run2.calls == [{"part": "snippet", "videoId": "v1", "maxResults": 100, "order": "time",
                           "textFormat": "plainText", "pageToken": "T2"}]
    by_vid = {o["video_id"]: o for o in rec2["video_outcomes"]}
    assert by_vid["v1"]["coverage_status"] == hpc.COVERAGE_COMPLETE_ALL_PAGES
    assert by_vid["v1"]["resumed_from_run_id"] == "r1"
    assert by_vid["v2"]["outcome"] == hpc.OUTCOME_SKIPPED_COMPLETE and by_vid["v2"]["pages_requested"] == 0
    assert rec2["resumed_from_run_id"] == "r1" and rec2["run_status"] == "COMPLETED"

    store = json.loads((tmp_path / "comment_store.json").read_text())
    assert store["video_coverage"]["v1"] == {**store["video_coverage"]["v1"], "run_id": "r2",
                                             "coverage_status": hpc.COVERAGE_COMPLETE_ALL_PAGES,
                                             "resumed_from_run_id": "r1"}
    assert store["video_coverage"]["v2"]["run_id"] == "r1"
    assert sum(1 for c in store["comments"].values() if c["in_early_window"]) == 3
    result = hpc.verify_store(str(tmp_path), m)
    assert result["passed"], result["problem_counts"]


def test_resume_carries_prior_frontier(tmp_path):
    m = _manifest(cap=1)
    hpc.run_harvest(m, ScriptedFetcher({("v1", None): _page(IN_WINDOW, "T2")}), str(tmp_path), SECRET, "A", run_id="r1")
    rec = hpc.run_harvest(m, ScriptedFetcher({("v1", "T2"): _page(IN_WINDOW[1:], "T3")}),
                          str(tmp_path), SECRET, "A", run_id="r2", resume_from_run="r1")
    o = rec["video_outcomes"][0]
    assert o["coverage_status"] == hpc.COVERAGE_TRUNCATED_WITHIN_WINDOW
    assert o["frontier_published_at"] == "2023-01-03T16:30:00+00:00"


def test_resume_validation(tmp_path):
    m = _manifest()
    with pytest.raises(ValueError, match="not recorded"):
        hpc.run_harvest(m, ScriptedFetcher({}), str(tmp_path), SECRET, "A", run_id="r1", resume_from_run="ghost")
    newest = hpc.build_manifest(_report([_candidate("v1")]), "r", "h", "n", 100)
    with pytest.raises(ValueError, match="EARLY_WINDOW"):
        hpc.run_harvest(newest, ScriptedFetcher({}), str(tmp_path / "n"), SECRET, "A", resume_from_run="r1")


def test_checkpoint_written_after_each_page(tmp_path):
    m = _manifest(cap=3)
    seen = []

    class Recording(ScriptedFetcher):
        def __call__(self, params):
            cp = tmp_path / "runs" / "r1" / "checkpoints" / "v1.json"
            seen.append(json.loads(cp.read_text())["next_page_token"] if cp.exists() else None)
            return super().__call__(params)

    f = Recording({("v1", None): _page(LATE, "T2"), ("v1", "T2"): _page(POST_WINDOW, "T3"), ("v1", "T3"): _page(IN_WINDOW)})
    hpc.run_harvest(m, f, str(tmp_path), SECRET, "A", run_id="r1")
    assert seen == [None, "T2", "T3"]


# =============================================================================
# 4. Verification
# =============================================================================

def test_verify_detects_window_flag_and_checkpoint_problems(tmp_path):
    m = _manifest(cap=1)
    hpc.run_harvest(m, ScriptedFetcher({("v1", None): _page(LATE, "T2")}), str(tmp_path), SECRET, "A", run_id="r1")
    store_path = tmp_path / "comment_store.json"
    store = json.loads(store_path.read_text())
    store["comments"]["late0"]["in_early_window"] = True
    store["video_coverage"]["v1"]["coverage_status"] = hpc.COVERAGE_COMPLETE_ALL_PAGES
    store_path.write_text(json.dumps(store))
    result = hpc.verify_store(str(tmp_path), m)
    assert result["problems"]["window_flag_mismatch"] == ["late0"]
    assert result["problems"]["coverage_checkpoint_mismatch"] == ["v1"]
    (tmp_path / "runs" / "r1" / "checkpoints" / "v1.json").unlink()
    assert hpc.verify_store(str(tmp_path), m)["problems"]["coverage_checkpoint_missing"] == ["v1"]


# =============================================================================
# 5. CLI
# =============================================================================

def test_cli_estimate_and_manifest(tmp_path, capsys):
    report = tmp_path / "report.json"
    report.write_text(json.dumps(_report([_candidate("a", comment_count="250"), _candidate("b", comment_count="40")])))
    assert hpc.main(["estimate", "--report", str(report), "--max-requests-per-video", "1", "5"]) == 0
    out = capsys.readouterr().out
    assert "event_id=TRANSFER_AL_NASSR_2023 eligible PLAYER_MATCH videos=2" in out
    assert "upper bounds" in out

    out_path = tmp_path / "m.json"
    assert hpc.main(["manifest", "--report", str(report), "--manifest-version", "v", "--mode", "early-window",
                     "--output", str(out_path)]) == 1  # missing window/cap
    assert not out_path.exists()
    assert hpc.main(["manifest", "--report", str(report), "--manifest-version", "v", "--mode", "early-window",
                     "--window-hours", "72", "--max-requests-per-video", "5", "--output", str(out_path)]) == 0
    m = json.loads(out_path.read_text())
    assert m["scope"]["mode"] == hpc.SCOPE_EARLY_WINDOW and m["cost_estimate"]["planned_max_requests"] == 4
