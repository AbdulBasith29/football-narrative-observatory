"""tests/test_comment_explorer.py
Offline tests for the local comment explorer (scripts/explore_comment_pilot.py).

Validates counts, coverage labelling (newest-N and early-window), timestamp warnings, HTML escaping,
absence of author identifiers, deterministic audit samples, and failure banners.
"""

from __future__ import annotations

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts")))

import explore_comment_pilot as ecp  # noqa: E402
import harvest_pilot_comments as hpc  # noqa: E402

SECRET = "s"


def _cand(vid, channel="ESPN FC", published="2022-12-18T20:00:00Z", count="100"):
    return {"video_id": vid, "channel_id": "UC1", "channel_title": channel, "title": f"<b>{vid}</b> & co",
            "published_at": published, "status": "ELIGIBLE", "relevance_tier": "PLAYER_MATCH",
            "player_category": "MESSI", "comment_count": count}


def _report(cands, event_id=None):
    meta = {"pipeline_frame": "PIPELINE_PILOT", "execution_mode": "LIVE_YOUTUBE_API"}
    if event_id:
        meta["event_id"] = event_id
    return {"report_metadata": meta, "evaluated_candidates": cands}


def _thread(cid, vid, published, updated=None, text="hi", author="UCsecretAuthor"):
    snippet = {"videoId": vid, "textOriginal": text, "publishedAt": published, "updatedAt": updated or published,
               "likeCount": 0, "authorDisplayName": "Secret Name", "authorChannelId": {"value": author}}
    return {"id": cid, "snippet": {"videoId": vid, "totalReplyCount": 0,
                                   "topLevelComment": {"id": cid, "snippet": snippet}}}


def _page(items, token=None):
    body = {"items": items}
    if token:
        body["nextPageToken"] = token
    return 200, json.dumps(body).encode("utf-8")


class Fetcher:
    def __init__(self, script):
        self.script = script

    def __call__(self, params):
        return self.script[(params["videoId"], params.get("pageToken"))]


@pytest.fixture
def newest_root(tmp_path):
    m = hpc.build_manifest(_report([_cand("vA"), _cand("vB", channel="TalkFCB")]), "r.json", "h", "pilot-comments-v1", 2)
    script = {
        ("vA", None): _page([_thread("a2", "vA", "2023-09-03T16:13:14Z", "2023-09-03T16:13:13Z"),
                             _thread("a1", "vA", "2022-12-19T10:00:00Z", text="<script>alert(1)</script>")], "MORE"),
        ("vB", None): _page([_thread("b1", "vB", "2022-12-18T21:00:00Z")]),
    }
    hpc.run_harvest(m, Fetcher(script), str(tmp_path), SECRET, "Abdul", run_id="r1")
    return m, tmp_path


def test_newest_mode_counts_and_coverage(newest_root):
    m, root = newest_root
    data = ecp.build_explorer_data(m, str(root))
    assert data["total_comments"] == 3
    by_vid = {v["video_id"]: v for v in data["videos"]}
    assert by_vid["vA"]["retrieved"] == 2 and not by_vid["vA"]["coverage_complete"]
    assert by_vid["vA"]["coverage_label"].startswith("Newest only")
    assert by_vid["vB"]["coverage_complete"] and by_vid["vB"]["coverage_label"] == "All retrieved (pages exhausted)"
    assert by_vid["vA"]["within_72h"] == 1 and by_vid["vB"]["within_72h"] == 1
    assert data["monthly"] == {"2022-12": [1, 1], "2023-09": [0, 1]}
    assert data["age_counts"]["< 1 day"] == [1, 1] and data["age_counts"]["> 1 year"] == [0, 0]
    assert data["channels"]["ESPN FC"] == {"videos": 1, "retrieved": 2, "complete": 0}
    assert data["verification"]["passed"]


def test_html_labels_escapes_and_hides_authors(newest_root):
    m, root = newest_root
    page = ecp.render_html(ecp.build_explorer_data(m, str(root)))
    assert "retrieved comments, not total discussion volume" in page
    assert "1 timestamp warning(s)" in page and "a2" in page
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in page and "<script>" not in page
    assert "&lt;b&gt;vA&lt;/b&gt; &amp; co" in page
    assert "UCsecretAuthor" not in page and "Secret Name" not in page
    assert "Offline verification passed." in page
    assert "http" not in page.replace("https://www.youtube.com/watch?v=", "")  # no external resources


def test_cli_writes_file(newest_root, capsys):
    m, root = newest_root
    manifest_path = root / "m.json"
    manifest_path.write_text(json.dumps(m))
    assert ecp.main(["--manifest", str(manifest_path), "--output-root", str(root)]) == 0
    assert (root / "explorer.html").exists()
    assert "timestamp_warnings=1" in capsys.readouterr().out


def test_failed_verification_banner(newest_root):
    m, root = newest_root
    (root / "runs" / "r1" / "raw" / "vB" / "page_001.json").write_bytes(b"tampered")
    page = ecp.render_html(ecp.build_explorer_data(m, str(root)))
    assert "Verification failed" in page and "lineage_hash_mismatch" in page


def test_sample_is_deterministic_and_bounded():
    comments = [{"comment_id": f"c{i}"} for i in range(30)]
    s1 = ecp.deterministic_sample(comments, 10)
    s2 = ecp.deterministic_sample(list(reversed(comments)), 10)
    assert s1 == s2 and len(s1) == 10


def test_early_window_mode_labels(tmp_path):
    report = _report([_cand("vE", published="2023-01-03T16:00:00Z", count="900")], event_id="TRANSFER_AL_NASSR_2023")
    m = hpc.build_early_window_manifest(report, "r.json", "h", "al-nassr-early", 72, 1)
    script = {("vE", None): _page([_thread("e2", "vE", "2025-01-01T00:00:00Z"),
                                   _thread("e1", "vE", "2023-01-04T00:00:00Z")], "T2")}
    hpc.run_harvest(m, Fetcher(script), str(tmp_path), SECRET, "Abdul", run_id="r1")
    data = ecp.build_explorer_data(m, str(tmp_path))
    v = data["videos"][0]
    assert v["coverage_label"] == "Budget-truncated within window (partial)" and not v["coverage_complete"]
    assert v["in_window"] == 1
    page = ecp.render_html(data)
    assert "TRANSFER_AL_NASSR_2023" in page and "In window" in page and "budget-truncated videos are partial" in page


def test_uncollected_videos_are_not_complete(tmp_path):
    m = hpc.build_manifest(_report([_cand("vA"), _cand("vB")]), "r.json", "h", "v", 100)
    hpc.run_harvest(m, Fetcher({("vA", None): _page([_thread("a1", "vA", "2022-12-19T00:00:00Z")])}),
                    str(tmp_path), SECRET, "Abdul", run_id="r1", video_limit=1)
    by_vid = {v["video_id"]: v for v in ecp.build_explorer_data(m, str(tmp_path))["videos"]}
    assert by_vid["vB"]["coverage_label"] == "Not collected" and not by_vid["vB"]["coverage_complete"]


def test_month_tick_labels_do_not_crowd():
    long_range = ecp._month_range("2022-12", "2026-09")
    ticks = ecp.month_tick_labels(long_range)
    assert ticks[0] == "2022-12" and ticks[13] == "2024" and 1 not in ticks  # Jan 2023 too close to the first label
    assert sorted(ticks.values())[:1] == ["2022-12"] and len(ticks) == 4
    assert ecp.month_tick_labels(["2023-01", "2023-02"]) == {0: "2023-01", 1: "2023-02"}
