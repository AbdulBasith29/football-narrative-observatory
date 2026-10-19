"""tests/test_comment_harvest.py
Offline tests for bounded pilot comment harvesting (scripts/harvest_pilot_comments.py).

Validates:
1. Manifest selection is mechanical (ELIGIBLE + PLAYER_MATCH), deduplicated, hashed, and fails closed on
   edits, non-pilot frames and synthetic reports.
2. Raw response bytes are written before parsing, with request/retrieval metadata sidecars.
3. Distinct outcomes: collected, empty, comments disabled, video not found, quota (stops the run),
   partial error, other HTTP 403.
4. Per-video caps and pagination availability.
5. Author IDs are HMAC-hashed identically to scripts/ingestion_snowflake.py; parsed output carries no raw IDs.
6. Reruns add new comments and text versions without duplicating comment IDs.
7. Verification detects timestamp, duplicate and lineage problems.
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


# =============================================================================
# Fixtures
# =============================================================================

def _candidate(vid, status="ELIGIBLE", tier="PLAYER_MATCH", player="MESSI", published="2022-12-18T20:00:00Z"):
    return {
        "video_id": vid, "channel_id": "UC1", "channel_title": "Chan", "title": f"Title {vid}",
        "published_at": published, "status": status, "relevance_tier": tier, "player_category": player,
    }


def _report(candidates, frame="PIPELINE_PILOT", mode="LIVE_YOUTUBE_API"):
    return {
        "report_metadata": {"pipeline_frame": frame, "execution_mode": mode,
                            "report_generated_at": "2026-10-01T00:00:00Z",
                            "window_start": "2022-12-18T00:00:00+00:00", "window_end": "2022-12-20T23:59:59+00:00"},
        "evaluated_candidates": candidates,
    }


def _manifest(vids=("vidA", "vidB"), max_comments=100):
    return hpc.build_manifest(_report([_candidate(v) for v in vids]), "reports/verified.json", "abc",
                              "pilot-comments-test", max_comments)


def _thread(cid, vid, text="hello", published="2023-01-01T00:00:00Z", updated=None, author="UCauthor", likes=1):
    snippet = {
        "videoId": vid, "textOriginal": text, "textDisplay": text, "publishedAt": published,
        "updatedAt": updated or published, "likeCount": likes, "authorDisplayName": "Real Name",
    }
    if author:
        snippet["authorChannelId"] = {"value": author}
    return {"kind": "youtube#commentThread", "id": cid,
            "snippet": {"videoId": vid, "totalReplyCount": 0,
                        "topLevelComment": {"kind": "youtube#comment", "id": cid, "snippet": snippet}}}


def _page(items, next_token=None):
    body = {"kind": "youtube#commentThreadListResponse", "items": items}
    if next_token:
        body["nextPageToken"] = next_token
    return 200, json.dumps(body).encode("utf-8")


def _error(status, reason):
    return status, json.dumps({"error": {"code": status, "message": reason,
                                         "errors": [{"reason": reason, "domain": "youtube"}]}}).encode("utf-8")


class ScriptedFetcher:
    """Returns scripted responses keyed by (videoId, pageToken)."""

    def __init__(self, script: Dict[Tuple[str, Optional[str]], Tuple[Optional[int], bytes]]):
        self.script = script
        self.calls: List[Dict[str, Any]] = []

    def __call__(self, params):
        self.calls.append(dict(params))
        return self.script[(params["videoId"], params.get("pageToken"))]


# =============================================================================
# 1. Manifest
# =============================================================================

def test_manifest_selects_only_eligible_player_match_and_dedupes():
    report = _report([
        _candidate("v2"), _candidate("v1"), _candidate("v1"),
        _candidate("v3", tier="EVENT_SPECIFIC"),
        _candidate("v4", status="INELIGIBLE"),
        {**_candidate("v5"), "player_category": None, "player_relevance": "DUAL"},
    ])
    m = hpc.build_manifest(report, "r.json", "deadbeef", "v1", 100)
    assert [v["video_id"] for v in m["videos"]] == ["v1", "v2", "v5"]
    assert m["videos"][2]["player_category"] == "DUAL"
    assert m["budget"] == {**m["budget"], "videos": 3, "max_requests_per_video": 1,
                           "max_total_requests": 3, "max_quota_units": 3}
    assert m["scope"]["comment_level"] == "TOP_LEVEL_ONLY"
    assert m["source_report"]["sha256"] == "deadbeef"
    hpc.verify_manifest_integrity(m)


def test_manifest_budget_rounds_pages_up():
    m = _manifest(vids=("a",), max_comments=150)
    assert m["budget"]["max_requests_per_video"] == 2


@pytest.mark.parametrize("frame,mode", [("RESEARCH", "LIVE_YOUTUBE_API"), ("PIPELINE_PILOT", "SYNTHETIC_MOCK_DATA")])
def test_manifest_rejects_non_pilot_or_synthetic_reports(frame, mode):
    with pytest.raises(ValueError):
        hpc.build_manifest(_report([_candidate("v1")], frame=frame, mode=mode), "r", "h", "v1")


def test_edited_manifest_fails_integrity_check():
    m = _manifest()
    m["scope"]["max_comments_per_video"] = 5000
    with pytest.raises(ValueError, match="hash mismatch"):
        hpc.verify_manifest_integrity(m)


# =============================================================================
# 2. Harvest outcomes, raw-before-parse, lineage
# =============================================================================

def test_harvest_requires_scope_acceptance_and_secret(tmp_path):
    m = _manifest()
    f = ScriptedFetcher({})
    with pytest.raises(RuntimeError, match="Scope acceptance"):
        hpc.run_harvest(m, f, str(tmp_path), SECRET, "  ")
    with pytest.raises(RuntimeError, match="HMAC_SECRET"):
        hpc.run_harvest(m, f, str(tmp_path), "", "Abdul")
    assert f.calls == []


def test_harvest_writes_raw_bytes_and_lineage(tmp_path):
    m = _manifest(vids=("vidA",))
    raw_status, raw_body = _page([_thread("c1", "vidA"), _thread("c2", "vidA", author=None)])
    f = ScriptedFetcher({("vidA", None): (raw_status, raw_body)})
    rec = hpc.run_harvest(m, f, str(tmp_path), SECRET, "Abdul", run_id="r1")

    raw_path = tmp_path / "runs" / "r1" / "raw" / "vidA" / "page_001.json"
    assert raw_path.read_bytes() == raw_body
    meta = json.loads((tmp_path / "runs" / "r1" / "raw" / "vidA" / "page_001.meta.json").read_text())
    assert meta["request_parameters"] == {"part": "snippet", "videoId": "vidA", "maxResults": 100,
                                          "order": "time", "textFormat": "plainText"}
    assert meta["http_status"] == 200 and meta["item_count"] == 2
    assert meta["payload_sha256"] == hpc.sha256_bytes(raw_body)

    assert rec["run_status"] == "COMPLETED"
    assert rec["scope_accepted_by"] == "Abdul" and rec["snowflake_writes"] is False
    assert rec["video_outcomes"][0]["outcome"] == hpc.OUTCOME_COLLECTED
    assert rec["video_outcomes"][0]["pagination_availability"] == hpc.PAGINATION_EXHAUSTED

    parsed = [json.loads(line) for line in (tmp_path / "runs" / "r1" / "parsed_comments.jsonl").read_text().splitlines()]
    assert [p["comment_id"] for p in parsed] == ["c1", "c2"]
    assert parsed[0]["source_item_position"] == 0 and parsed[1]["source_item_position"] == 1
    assert parsed[0]["raw_file"] == "raw/vidA/page_001.json"
    assert parsed[0]["author_hash"] == hpc.hash_author_id("UCauthor", SECRET)
    assert parsed[1]["author_hash"] is None and parsed[1]["salt_version_id"] is None
    blob = (tmp_path / "runs" / "r1" / "parsed_comments.jsonl").read_text() + (tmp_path / "comment_store.json").read_text()
    assert "UCauthor" not in blob and "Real Name" not in blob


def test_author_hash_matches_existing_ingestion_construction():
    snowflake = pytest.importorskip("snowflake.connector")  # noqa: F841
    import ingestion_snowflake
    assert hpc.SALT_VERSION_ID == ingestion_snowflake.SALT_VERSION_ID
    assert hpc.hash_author_id("UCx", SECRET) == ingestion_snowflake.hash_author_id("UCx", SECRET)


def test_distinct_outcomes_and_403_discrimination(tmp_path):
    m = _manifest(vids=("vCollected", "vDisabled", "vEmpty", "vForbidden", "vMissing"))
    f = ScriptedFetcher({
        ("vCollected", None): _page([_thread("c1", "vCollected")]),
        ("vDisabled", None): _error(403, "commentsDisabled"),
        ("vEmpty", None): _page([]),
        ("vForbidden", None): _error(403, "forbidden"),
        ("vMissing", None): _error(404, "videoNotFound"),
    })
    rec = hpc.run_harvest(m, f, str(tmp_path), SECRET, "Abdul", run_id="r1")
    by_vid = {o["video_id"]: o for o in rec["video_outcomes"]}
    assert by_vid["vCollected"]["outcome"] == hpc.OUTCOME_COLLECTED
    assert by_vid["vDisabled"]["outcome"] == hpc.OUTCOME_COMMENTS_DISABLED
    assert by_vid["vEmpty"]["outcome"] == hpc.OUTCOME_EMPTY
    assert by_vid["vEmpty"]["pagination_availability"] == hpc.PAGINATION_EXHAUSTED
    assert by_vid["vForbidden"]["outcome"] == hpc.OUTCOME_ERROR
    assert by_vid["vForbidden"]["error_code"] == "HTTP_403"
    assert by_vid["vMissing"]["outcome"] == hpc.OUTCOME_VIDEO_NOT_FOUND
    assert rec["run_status"] == "COMPLETED_WITH_ERRORS"
    # Error bodies are preserved verbatim too.
    assert (tmp_path / "runs" / "r1" / "raw" / "vDisabled" / "page_001.json").read_bytes() == _error(403, "commentsDisabled")[1]


def test_quota_limit_stops_run_and_marks_remaining_not_attempted(tmp_path):
    m = _manifest(vids=("v1", "v2", "v3"))
    f = ScriptedFetcher({
        ("v1", None): _page([_thread("c1", "v1")]),
        ("v2", None): _error(403, "quotaExceeded"),
    })
    rec = hpc.run_harvest(m, f, str(tmp_path), SECRET, "Abdul", run_id="r1")
    outcomes = [o["outcome"] for o in rec["video_outcomes"]]
    assert outcomes == [hpc.OUTCOME_COLLECTED, hpc.OUTCOME_QUOTA_LIMIT, hpc.OUTCOME_NOT_ATTEMPTED]
    assert rec["run_status"] == "STOPPED_QUOTA_LIMIT"
    assert len(f.calls) == 2
    assert rec["telemetry"]["requests_by_endpoint"]["commentThreads.list"] == 2


def test_cap_and_pagination_across_pages(tmp_path):
    m = _manifest(vids=("v1",), max_comments=150)
    page1 = [_thread(f"a{i}", "v1") for i in range(100)]
    page2 = [_thread(f"b{i}", "v1") for i in range(100)]
    f = ScriptedFetcher({("v1", None): _page(page1, "T2"), ("v1", "T2"): _page(page2, "T3")})
    rec = hpc.run_harvest(m, f, str(tmp_path), SECRET, "Abdul", run_id="r1")
    assert [c["maxResults"] for c in f.calls] == [100, 50]
    o = rec["video_outcomes"][0]
    assert o["comments_collected"] == 150 and o["pages_succeeded"] == 2
    assert o["pagination_availability"] == hpc.PAGINATION_MORE_AVAILABLE


def test_error_after_successful_page_is_partial(tmp_path):
    m = _manifest(vids=("v1",), max_comments=200)
    f = ScriptedFetcher({("v1", None): _page([_thread("c1", "v1")], "T2"), ("v1", "T2"): _error(500, "backendError")})
    rec = hpc.run_harvest(m, f, str(tmp_path), SECRET, "Abdul", run_id="r1")
    o = rec["video_outcomes"][0]
    assert o["outcome"] == hpc.OUTCOME_PARTIAL_ERROR and o["error_code"] == "HTTP_500"
    assert o["comments_collected"] == 1
    assert rec["store_merge"]["new_comments"] == 1


def test_existing_run_directory_is_never_overwritten(tmp_path):
    m = _manifest(vids=("v1",))
    f = ScriptedFetcher({("v1", None): _page([_thread("c1", "v1")])})
    hpc.run_harvest(m, f, str(tmp_path), SECRET, "Abdul", run_id="r1")
    with pytest.raises(FileExistsError):
        hpc.run_harvest(m, f, str(tmp_path), SECRET, "Abdul", run_id="r1")


def test_store_refuses_mixing_manifests(tmp_path):
    f = ScriptedFetcher({("v1", None): _page([]), ("v9", None): _page([])})
    hpc.run_harvest(_manifest(vids=("v1",)), f, str(tmp_path), SECRET, "Abdul", run_id="r1")
    with pytest.raises(ValueError, match="different manifest"):
        hpc.run_harvest(_manifest(vids=("v9",)), f, str(tmp_path), SECRET, "Abdul", run_id="r2")


# =============================================================================
# 3. Reruns: new/updated comments without duplicate IDs
# =============================================================================

def test_rerun_adds_new_and_updated_comments_without_duplicate_ids(tmp_path):
    m = _manifest(vids=("v1",))
    run1 = ScriptedFetcher({("v1", None): _page([_thread("c2", "v1", text="second"), _thread("c1", "v1", text="first")])})
    rec1 = hpc.run_harvest(m, run1, str(tmp_path), SECRET, "Abdul", run_id="r1")
    assert rec1["store_merge"]["new_comments"] == 2

    run2 = ScriptedFetcher({("v1", None): _page([
        _thread("c3", "v1", text="third", published="2023-02-01T00:00:00Z"),
        _thread("c2", "v1", text="second (edited)", updated="2023-03-01T00:00:00Z", likes=5),
        _thread("c1", "v1", text="first"),
    ])})
    rec2 = hpc.run_harvest(m, run2, str(tmp_path), SECRET, "Abdul", run_id="r2")
    assert rec2["store_merge"] == {"new_comments": 1, "existing_comments": 2, "new_text_versions": 2,
                                   "updated_at_changes": 1, "duplicate_ids_within_run": 0}

    store = json.loads((tmp_path / "comment_store.json").read_text())
    assert sorted(store["comments"]) == ["c1", "c2", "c3"]
    c2 = store["comments"]["c2"]
    assert c2["first_observed_run_id"] == "r1" and c2["last_observed_run_id"] == "r2"
    assert [v["text"] for v in c2["text_versions"]] == ["second", "second (edited)"]
    assert [v["is_latest_version"] for v in c2["text_versions"]] == [False, True]
    assert [s["like_count"] for s in c2["engagement_snapshots"]] == [1, 5]
    assert len(store["comments"]["c1"]["text_versions"]) == 1
    assert len(store["comments"]["c1"]["sample_observations"]) == 2
    assert [r["run_id"] for r in store["runs"]] == ["r1", "r2"]

    result = hpc.verify_store(str(tmp_path), m)
    assert result["passed"], result["problem_counts"]
    assert result["comments"] == 3 and result["sample_observations"] == 5
    assert hpc.scan_run_duplicates(str(tmp_path)) == {"r1": 0, "r2": 0}


# =============================================================================
# 4. Verification detects problems
# =============================================================================

def test_verify_detects_timestamp_and_lineage_problems(tmp_path):
    m = _manifest(vids=("v1",))
    f = ScriptedFetcher({("v1", None): _page([
        _thread("early", "v1", published="2022-01-01T00:00:00Z"),           # before video published
        _thread("backwards", "v1", published="2023-05-01T00:00:00Z", updated="2023-04-01T00:00:00Z"),
        _thread("ok", "v1"),
    ])})
    hpc.run_harvest(m, f, str(tmp_path), SECRET, "Abdul", run_id="r1")
    result = hpc.verify_store(str(tmp_path), m)
    assert result["problems"]["comment_before_video_published"] == ["early"]
    assert result["problems"]["updated_before_published"] == ["backwards"]
    assert not result["passed"]

    # Tamper with the raw page: every lineage reference must now fail the hash check.
    raw = tmp_path / "runs" / "r1" / "raw" / "v1" / "page_001.json"
    raw.write_bytes(raw.read_bytes() + b" ")
    result = hpc.verify_store(str(tmp_path), m)
    assert result["problem_counts"]["lineage_hash_mismatch"] == 3
    raw.unlink()
    assert hpc.verify_store(str(tmp_path), m)["problem_counts"]["lineage_file_missing"] == 3


def test_scan_run_duplicates_counts_repeated_ids(tmp_path):
    m = _manifest(vids=("v1",), max_comments=200)
    f = ScriptedFetcher({("v1", None): _page([_thread("c1", "v1")], "T2"), ("v1", "T2"): _page([_thread("c1", "v1")])})
    rec = hpc.run_harvest(m, f, str(tmp_path), SECRET, "Abdul", run_id="r1")
    assert rec["store_merge"]["duplicate_ids_within_run"] == 1
    assert hpc.scan_run_duplicates(str(tmp_path)) == {"r1": 1}
    store = json.loads((tmp_path / "comment_store.json").read_text())
    assert list(store["comments"]) == ["c1"]


# =============================================================================
# 5. Live fetcher keeps verbatim bytes
# =============================================================================

def test_live_fetcher_returns_raw_bytes_and_http_errors():
    class FakeResp:
        def __init__(self, status):
            self.status = status
            self.reason = "Forbidden"

    class FakeRequest:
        def __init__(self, outcome):
            self.outcome = outcome
            self.postproc = None

        def execute(self):
            if isinstance(self.outcome, Exception):
                raise self.outcome
            return self.postproc(FakeResp(200), self.outcome)

    class FakeYoutube:
        def __init__(self, outcome):
            self.outcome = outcome
            self.params = None

        def commentThreads(self):
            return self

        def list(self, **params):
            self.params = params
            return FakeRequest(self.outcome)

    body = b'{"items": []}'
    yt = FakeYoutube(body)
    assert hpc.make_live_fetcher(yt)({"videoId": "v1", "part": "snippet"}) == (200, body)
    assert yt.params == {"videoId": "v1", "part": "snippet"}

    if hpc.HttpError is not None:
        err_body = _error(403, "commentsDisabled")[1]
        exc = hpc.HttpError(FakeResp(403), err_body)
        assert hpc.make_live_fetcher(FakeYoutube(exc))({"videoId": "v1"}) == (403, err_body)

    status, payload = hpc.make_live_fetcher(FakeYoutube(OSError("reset")))({"videoId": "v1"})
    assert status is None and b"transport_error" in payload
    assert hpc.classify_error_response(status, payload, 0) == (hpc.OUTCOME_ERROR, "TRANSPORT_ERROR")


def test_cli_manifest_refuses_overwrite(tmp_path):
    report = tmp_path / "report.json"
    report.write_text(json.dumps(_report([_candidate("v1")])))
    out = tmp_path / "manifest.json"
    assert hpc.main(["manifest", "--report", str(report), "--manifest-version", "v1", "--output", str(out)]) == 0
    first = out.read_text()
    assert hpc.main(["manifest", "--report", str(report), "--manifest-version", "v2", "--output", str(out)]) == 1
    assert out.read_text() == first
    assert json.loads(first)["source_report"]["sha256"] == hpc.sha256_bytes(report.read_bytes())


def test_live_fetcher_with_real_client_library_keeps_verbatim_bytes():
    """Uses googleapiclient's bundled discovery document and HttpMockSequence (fully offline)."""
    discovery = pytest.importorskip("googleapiclient.discovery")
    http_mod = pytest.importorskip("googleapiclient.http")
    ok = b'{"kind": "youtube#commentThreadListResponse",   "items": []}'
    err = _error(403, "commentsDisabled")[1]
    http = http_mod.HttpMockSequence([({"status": "200"}, ok), ({"status": "403"}, err)])
    yt = discovery.build("youtube", "v3", developerKey="offline", http=http, static_discovery=True)
    fetch = hpc.make_live_fetcher(yt)
    assert fetch({"part": "snippet", "videoId": "v1", "maxResults": 100}) == (200, ok)
    assert fetch({"part": "snippet", "videoId": "v1"}) == (403, err)
