"""tests/test_occurrences_and_prepublication.py
Regression tests for two cases observed in the World Cup early-window pilot.

1. Cross-page repetition: the same comment returned on two consecutive pages within one run. Every raw
   occurrence is preserved with its lineage and recorded as a duplicate occurrence (identical or changed);
   analytical counts stay unique by comment ID; verification records it instead of failing.
2. Pre-publication comments: 71 comments whose published_at precedes the video's publishedAt (for example
   livestream-era comments; not verified). They stay flagged, timestamps are never rewritten, the window anchor
   stays VIDEO_PUBLISHED_AT, and coverage of pre-publication discussion is reported separately from
   completion of the configured window.
The fixtures reproduce the observed shapes; the actual comment IDs are not in this repository.
"""

from __future__ import annotations

import datetime
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts")))

import explore_comment_pilot as ecp  # noqa: E402
import harvest_pilot_comments as hpc  # noqa: E402

SECRET = "s"


def _cand(vid, published):
    return {"video_id": vid, "channel_id": "UC1", "channel_title": "ESPN FC", "title": f"LIVE {vid}",
            "published_at": published, "status": "ELIGIBLE", "relevance_tier": "PLAYER_MATCH",
            "player_category": "MESSI", "comment_count": "500"}


def _report(cands):
    return {"report_metadata": {"pipeline_frame": "PIPELINE_PILOT", "execution_mode": "LIVE_YOUTUBE_API"},
            "evaluated_candidates": cands}


def _early(vid="v1", published="2022-12-18T20:00:00Z", cap=5):
    return hpc.build_early_window_manifest(_report([_cand(vid, published)]), "r.json", "h", "wc-2022-early-v1",
                                           72, cap, event_id="WC_2022_FINAL")


def _thread(cid, vid, published, likes=0, text="t"):
    snippet = {"videoId": vid, "textOriginal": text, "textDisplay": text, "publishedAt": published,
               "updatedAt": published, "likeCount": likes, "authorChannelId": {"value": "UCa"}}
    return {"kind": "youtube#commentThread", "id": cid,
            "snippet": {"videoId": vid, "totalReplyCount": 0, "topLevelComment": {"id": cid, "snippet": snippet}}}


def _page(items, token=None):
    body = {"items": items}
    if token:
        body["nextPageToken"] = token
    return 200, json.dumps(body).encode("utf-8")


class Fetcher:
    def __init__(self, script):
        self.script, self.calls = script, []

    def __call__(self, params):
        self.calls.append(dict(params))
        return self.script[(params["videoId"], params.get("pageToken"))]


def _store(root):
    return json.loads((root / "comment_store.json").read_text())


def _jsonl(root, run_id):
    return [json.loads(line) for line in (root / "runs" / run_id / "parsed_comments.jsonl").read_text().splitlines()]


REPEATED = _thread("Ugrepeated", "v1", "2023-01-01T00:00:00Z", likes=3)
PAGE_1 = [_thread("Ugn3", "v1", "2023-03-01T00:00:00Z"), _thread("Ugn2", "v1", "2023-02-01T00:00:00Z"), REPEATED]
PAGE_2_IDENTICAL = [REPEATED, _thread("Ugw1", "v1", "2022-12-19T10:00:00Z")]


# =============================================================================
# 1. Cross-page repetition
# =============================================================================

def test_identical_cross_page_repeat_recorded_and_counted_once(tmp_path):
    m = _early()
    f = Fetcher({("v1", None): _page(PAGE_1, "T2"), ("v1", "T2"): _page(PAGE_2_IDENTICAL)})
    rec = hpc.run_harvest(m, f, str(tmp_path), SECRET, "A", run_id="r1")

    o = rec["video_outcomes"][0]
    assert (o["comments_collected"], o["raw_occurrences"], o["duplicate_occurrences"]) == (4, 5, 1)
    assert o["in_window_comments_observed"] == 1 and o["out_of_window_comments_observed"] == 3
    assert rec["raw_occurrences"] == 5 and rec["unique_comments_observed"] == 4
    assert rec["occurrence_counts"] == {"FIRST": 4, "DUPLICATE_IDENTICAL": 1, "DUPLICATE_CHANGED": 0}
    assert rec["store_merge"]["duplicate_occurrences_identical"] == 1 and rec["store_merge"]["new_comments"] == 4

    # Every raw occurrence is preserved: both raw pages, both parsed lines, both lineage records.
    lines = [line for line in _jsonl(tmp_path, "r1") if line["comment_id"] == "Ugrepeated"]
    assert [(line["raw_file"], line["source_item_position"], line["occurrence_kind"]) for line in lines] == [
        ("raw/v1/page_001.json", 2, "FIRST"), ("raw/v1/page_002.json", 0, "DUPLICATE_IDENTICAL")]
    entry = _store(tmp_path)["comments"]["Ugrepeated"]
    assert [(s["raw_file"], s["occurrence_kind"]) for s in entry["sample_observations"]] == [
        ("raw/v1/page_001.json", "FIRST"), ("raw/v1/page_002.json", "DUPLICATE_IDENTICAL")]
    assert len(entry["engagement_snapshots"]) == 1 and len(entry["text_versions"]) == 1
    assert len(_store(tmp_path)["comments"]) == 4

    result = hpc.verify_store(str(tmp_path), m)
    assert result["passed"], {k: v for k, v in result["problem_counts"].items() if v}
    assert result["warnings"]["duplicate_occurrences_identical"] == [{
        "run_id": "r1", "comment_id": "Ugrepeated",
        "first": {"raw_file": "raw/v1/page_001.json", "page_number": 1, "position": 2},
        "repeat": {"raw_file": "raw/v1/page_002.json", "page_number": 2, "position": 0}}]
    assert result["occurrences_by_run"]["r1"] == {"FIRST": 4, "DUPLICATE_IDENTICAL": 1, "DUPLICATE_CHANGED": 0}


def test_changed_cross_page_repeat_keeps_new_engagement(tmp_path):
    m = _early()
    changed = _thread("Ugrepeated", "v1", "2023-01-01T00:00:00Z", likes=4)
    f = Fetcher({("v1", None): _page(PAGE_1, "T2"), ("v1", "T2"): _page([changed, PAGE_2_IDENTICAL[1]])})
    rec = hpc.run_harvest(m, f, str(tmp_path), SECRET, "A", run_id="r1")
    assert rec["occurrence_counts"]["DUPLICATE_CHANGED"] == 1
    entry = _store(tmp_path)["comments"]["Ugrepeated"]
    assert [s["like_count"] for s in entry["engagement_snapshots"]] == [3, 4]
    result = hpc.verify_store(str(tmp_path), m)
    assert result["passed"] and len(result["warnings"]["duplicate_occurrences_changed"]) == 1
    assert result["warnings"]["duplicate_occurrences_identical"] == []


def test_newest_mode_repeat_counted_once(tmp_path):
    m = hpc.build_manifest(_report([_cand("v1", "2022-12-18T20:00:00Z")]), "r", "h", "n", 200)
    f = Fetcher({("v1", None): _page(PAGE_1, "T2"), ("v1", "T2"): _page(PAGE_2_IDENTICAL)})
    rec = hpc.run_harvest(m, f, str(tmp_path), SECRET, "A", run_id="r1")
    o = rec["video_outcomes"][0]
    assert (o["comments_collected"], o["raw_occurrences"], o["duplicate_occurrences"]) == (4, 5, 1)
    assert hpc.verify_store(str(tmp_path), m)["passed"]


def test_cli_verify_passes_with_recorded_repeats(tmp_path, capsys):
    m = _early()
    hpc.run_harvest(m, Fetcher({("v1", None): _page(PAGE_1, "T2"), ("v1", "T2"): _page(PAGE_2_IDENTICAL)}),
                    str(tmp_path), SECRET, "A", run_id="r1")
    path = tmp_path.parent / f"{tmp_path.name}-m.json"
    path.write_text(json.dumps(m))
    assert hpc.main(["verify", "--manifest", str(path), "--output-root", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "[WARN] duplicate_occurrences_identical" in out and "[OK] All checks passed" in out
    assert "recorded occurrences, not failures" in out


def test_legacy_run_without_occurrence_fields_is_classified_from_raw(tmp_path):
    """Runs written before occurrence kinds existed (the user's current data) verify from raw lineage."""
    m = _early()
    hpc.run_harvest(m, Fetcher({("v1", None): _page(PAGE_1, "T2"), ("v1", "T2"): _page(PAGE_2_IDENTICAL)}),
                    str(tmp_path), SECRET, "A", run_id="r1")
    store = _store(tmp_path)
    for c in store["comments"].values():
        for so in c["sample_observations"]:
            so.pop("occurrence_kind"), so.pop("item_sha256")
    (tmp_path / "comment_store.json").write_text(json.dumps(store))
    lines = _jsonl(tmp_path, "r1")
    for line in lines:
        line.pop("occurrence_kind"), line.pop("item_sha256")
    (tmp_path / "runs" / "r1" / "parsed_comments.jsonl").write_text("".join(json.dumps(x) + "\n" for x in lines))

    result = hpc.verify_store(str(tmp_path), m)
    assert result["passed"], {k: v for k, v in result["problem_counts"].items() if v}
    assert len(result["warnings"]["duplicate_occurrences_identical"]) == 1


def test_occurrence_integrity_problems(tmp_path):
    m = _early()
    hpc.run_harvest(m, Fetcher({("v1", None): _page(PAGE_1, "T2"), ("v1", "T2"): _page(PAGE_2_IDENTICAL)}),
                    str(tmp_path), SECRET, "A", run_id="r1")
    store = _store(tmp_path)
    obs = store["comments"]["Ugrepeated"]["sample_observations"]
    obs[1]["occurrence_kind"] = "FIRST"
    (tmp_path / "comment_store.json").write_text(json.dumps(store))
    assert hpc.verify_store(str(tmp_path), m)["problems"]["occurrence_kind_mismatch"] == [
        ["r1", "Ugrepeated", "raw/v1/page_002.json", 0]]

    store["comments"]["Ugrepeated"]["sample_observations"] = obs[:1]   # drop the repeat's lineage record
    store["comments"]["Ugrepeated"]["sample_observations"][0]["occurrence_kind"] = "FIRST"
    (tmp_path / "comment_store.json").write_text(json.dumps(store))
    result = hpc.verify_store(str(tmp_path), m)
    assert result["problems"]["occurrence_missing_from_store"] == [["r1", "Ugrepeated", "raw/v1/page_002.json", 0]]
    assert not result["passed"]

    lines = [x for x in _jsonl(tmp_path, "r1") if not (x["comment_id"] == "Ugn3")]
    (tmp_path / "runs" / "r1" / "parsed_comments.jsonl").write_text("".join(json.dumps(x) + "\n" for x in lines))
    assert hpc.verify_store(str(tmp_path), m)["problems"]["store_observation_missing_from_run_file"] == [
        ["r1", "Ugn3", "raw/v1/page_001.json", 0]]


# =============================================================================
# 2. Pre-publication comments (71 observed)
# =============================================================================

LIVESTREAM_PUBLISHED = "2022-12-18T21:00:00Z"   # publishedAt after a stream that ran through the match
START = datetime.datetime(2022, 12, 18, 14, 0, tzinfo=datetime.timezone.utc)
PRE_PUB = [_thread(f"Ugpre{i:02d}", "v1", (START + datetime.timedelta(minutes=5 * (70 - i))).strftime("%Y-%m-%dT%H:%M:%SZ"))
           for i in range(71)]                    # newest first, 14:00..19:50, all before publishedAt
IN_WINDOW = [_thread(f"Ugwin{i:02d}", "v1", f"2022-12-{21 - i // 10:02d}T{20 - i % 10:02d}:00:00Z")
             for i in range(30)]          # newest first, 19-21 Dec 11:00-20:00, inside [18 Dec 21:00, 21 Dec 21:00)


def test_71_pre_publication_comments_flagged_and_window_complete(tmp_path):
    m = _early(published=LIVESTREAM_PUBLISHED)
    assert m["scope"]["window_anchor"] == "VIDEO_PUBLISHED_AT"
    f = Fetcher({("v1", None): _page(IN_WINDOW, "T2"), ("v1", "T2"): _page(PRE_PUB, "T3")})
    rec = hpc.run_harvest(m, f, str(tmp_path), SECRET, "A", run_id="r1")
    o = rec["video_outcomes"][0]

    # The configured window [publishedAt, +72h) is complete: paging passed its start.
    assert o["window_start"] == "2022-12-18T21:00:00+00:00"
    assert o["coverage_status"] == hpc.COVERAGE_COMPLETE_WINDOW_PASSED and o["window_count_is_complete"]
    assert o["in_window_comments_observed"] == 30
    # Pre-publication discussion is reported separately and is only partially covered.
    assert o["pre_publication_comments_observed"] == 71
    assert o["pre_publication_coverage"] == hpc.PREPUB_PARTIAL_STOPPED
    assert len(f.calls) == 2                      # T3 was not requested: the anchor was not moved to chase it
    cov = _store(tmp_path)["video_coverage"]["v1"]
    assert cov["pre_publication_coverage"] == hpc.PREPUB_PARTIAL_STOPPED and cov["window_count_is_complete"]

    store = _store(tmp_path)
    pre = [c for c in store["comments"].values() if c["published_before_video"]]
    assert len(pre) == 71 and all(c["in_early_window"] is False for c in pre)
    assert store["comments"]["Ugpre70"]["published_at"] == "2022-12-18T14:00:00Z"   # exactly as returned

    result = hpc.verify_store(str(tmp_path), m)
    assert result["passed"], {k: v for k, v in result["problem_counts"].items() if v}
    flagged = result["warnings"]["comment_before_video_published"]
    assert len(flagged) == 71 and {row[1] for row in flagged} == {"v1"}
    assert max(row[2] for row in flagged) == 7 * 3600     # 14:00 vs 21:00


@pytest.mark.parametrize("coverage,count,expected", [
    (hpc.COVERAGE_COMPLETE_ALL_PAGES, 71, hpc.PREPUB_COMPLETE),
    (hpc.COVERAGE_COMPLETE_ALL_PAGES, 0, hpc.PREPUB_NONE_RETRIEVABLE),
    (hpc.COVERAGE_COMPLETE_WINDOW_PASSED, 71, hpc.PREPUB_PARTIAL_STOPPED),
    (hpc.COVERAGE_TRUNCATED_WITHIN_WINDOW, 0, hpc.PREPUB_NOT_REACHED),
    (hpc.COVERAGE_TRUNCATED_BEFORE_WINDOW, 1, hpc.PREPUB_PARTIAL),
    (hpc.COVERAGE_INCOMPLETE_ERROR, 0, hpc.PREPUB_NOT_REACHED),
    (hpc.COVERAGE_UNAVAILABLE, 0, hpc.PREPUB_UNAVAILABLE),
])
def test_pre_publication_coverage_mapping(coverage, count, expected):
    assert hpc.pre_publication_coverage(coverage, count) == expected


def test_all_pages_retrieved_covers_pre_publication_completely(tmp_path):
    m = _early(published=LIVESTREAM_PUBLISHED)
    rec = hpc.run_harvest(m, Fetcher({("v1", None): _page(IN_WINDOW + PRE_PUB[:5])}), str(tmp_path), SECRET, "A",
                          run_id="r1")
    o = rec["video_outcomes"][0]
    assert o["coverage_status"] == hpc.COVERAGE_COMPLETE_ALL_PAGES
    assert (o["pre_publication_comments_observed"], o["pre_publication_coverage"]) == (5, hpc.PREPUB_COMPLETE)


def test_pinned_pre_publication_comment_with_budget_truncation_is_partial(tmp_path):
    m = _early(published=LIVESTREAM_PUBLISHED, cap=1)
    pinned = _thread("Ugpinned", "v1", "2022-12-18T15:30:00Z", likes=900)
    later = [_thread("Uglate", "v1", "2024-01-01T00:00:00Z"), _thread("Uglate2", "v1", "2023-06-01T00:00:00Z")]
    rec = hpc.run_harvest(m, Fetcher({("v1", None): _page([pinned] + later, "T2")}), str(tmp_path), SECRET, "A",
                          run_id="r1")
    o = rec["video_outcomes"][0]
    assert o["coverage_status"] == hpc.COVERAGE_TRUNCATED_BEFORE_WINDOW and not o["window_count_is_complete"]
    assert (o["pre_publication_comments_observed"], o["pre_publication_coverage"]) == (1, hpc.PREPUB_PARTIAL)


def test_explorer_separates_window_completion_from_pre_publication_coverage(tmp_path):
    m = _early(published=LIVESTREAM_PUBLISHED)
    hpc.run_harvest(m, Fetcher({("v1", None): _page(IN_WINDOW, "T2"), ("v1", "T2"): _page(PRE_PUB, "T3")}),
                    str(tmp_path), SECRET, "A", run_id="r1")
    data = ecp.build_explorer_data(m, str(tmp_path))
    v = data["videos"][0]
    assert v["coverage_label"] == "Complete (window passed)" and v["coverage_complete"]
    assert v["pre_publication"] == 71 and v["pre_publication_status"] == hpc.PREPUB_PARTIAL_STOPPED
    assert v["pre_publication_earliest"] == "2022-12-18T14:00:00Z"
    page = ecp.render_html(data)
    assert "Flagged: comments published before the video's publishedAt" in page
    assert "71 comment(s)" in page and "partial: paging stopped at the window start" in page
    assert "Configured window coverage (from upload)" in page and "Before publishedAt (flagged)" in page
    sampled_pre = [c for c in v["sample"] if c["comment_id"].startswith("Ugpre")]
    assert sampled_pre, "fixture expects the deterministic sample to include a pre-publication comment"
    audit = page.split("<h2>Audit sample</h2>", 1)[1].split("<h2>Runs</h2>", 1)[0]
    assert audit.count("published before video (flagged)") == len(sampled_pre)
    # Event phases count unique comments; pre-publication comments fall in PRE_EVENT / EVENT by their timestamps.
    total = sum(sum(v) for v in data["phase_counts"].values())
    assert total == data["total_comments"] == 101


def test_explorer_lists_repeats_and_counts_unique(tmp_path):
    m = _early()
    hpc.run_harvest(m, Fetcher({("v1", None): _page(PAGE_1, "T2"), ("v1", "T2"): _page(PAGE_2_IDENTICAL)}),
                    str(tmp_path), SECRET, "A", run_id="r1")
    data = ecp.build_explorer_data(m, str(tmp_path))
    assert data["videos"][0]["retrieved"] == 4 and data["total_comments"] == 4
    page = ecp.render_html(data)
    assert "Repeated occurrences within runs" in page
    assert "raw/v1/page_001.json #2" in page and "raw/v1/page_002.json #0" in page
    assert "<td>identical</td>" in page


def test_legacy_early_store_without_pre_publication_fields(tmp_path):
    """Existing early-window stores lack the new coverage fields; the explorer derives them, data untouched."""
    m = _early(published=LIVESTREAM_PUBLISHED)
    hpc.run_harvest(m, Fetcher({("v1", None): _page(IN_WINDOW, "T2"), ("v1", "T2"): _page(PRE_PUB, "T3")}),
                    str(tmp_path), SECRET, "A", run_id="r1")
    store = _store(tmp_path)
    for key in ("pre_publication_comments_observed", "pre_publication_coverage"):
        store["video_coverage"]["v1"].pop(key)
    for c in store["comments"].values():
        c.pop("published_before_video")
    (tmp_path / "comment_store.json").write_text(json.dumps(store))
    before = (tmp_path / "comment_store.json").read_bytes()
    v = ecp.build_explorer_data(m, str(tmp_path))["videos"][0]
    assert v["pre_publication"] == 71 and v["pre_publication_status"] == hpc.PREPUB_PARTIAL_STOPPED
    assert (tmp_path / "comment_store.json").read_bytes() == before
