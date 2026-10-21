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


# =============================================================================
# Event-relative phases
# =============================================================================

import datetime  # noqa: E402
import hashlib  # noqa: E402

UTC = datetime.timezone.utc


def _dt(*args):
    return datetime.datetime(*args, tzinfo=UTC)


def test_wc_phase_boundaries_from_config():
    ev = ecp.load_event_phases("WC_2022_FINAL")
    assert ev["occurred_at"] == _dt(2022, 12, 18, 15) and ev["anchor"] == "kickoff"
    bounds = {p["window_type"]: (p["start"], p["end"]) for p in ev["phases"]}
    assert bounds == {
        "PRE_EVENT": (_dt(2022, 12, 4, 15), _dt(2022, 12, 18, 14)),
        "EVENT": (_dt(2022, 12, 18, 14), _dt(2022, 12, 18, 21)),
        "EARLY_POST_EVENT": (_dt(2022, 12, 18, 21), _dt(2022, 12, 19, 15)),
        "LATE_POST_EVENT": (_dt(2022, 12, 19, 15), _dt(2022, 12, 21, 15)),
        "RECOVERY_WINDOW": (_dt(2022, 12, 21, 15), _dt(2023, 1, 1, 15)),
    }
    assert ev["overlapping_windows"] == [] and ev["gaps_between_windows"] == []
    assert ev["first_start"] == _dt(2022, 12, 4, 15) and ev["last_end"] == _dt(2023, 1, 1, 15)


def test_phase_descriptions_follow_offsets_only():
    ev = ecp.load_event_phases("WC_2022_FINAL")
    desc = {p["window_type"]: p["description"] for p in ev["phases"]}
    assert desc["PRE_EVENT"] == "-336h (-14d) to -1h relative to kickoff; before kickoff"
    assert desc["EVENT"] == "-1h to +6h relative to kickoff; spans kickoff"
    assert desc["EARLY_POST_EVENT"] == "+6h to +24h relative to kickoff; after kickoff"
    for text in desc.values():
        lowered = text.lower()
        assert "match" not in lowered and "during" not in lowered and "post" not in lowered


def test_non_kickoff_event_uses_occurred_at_anchor():
    ev = ecp.load_event_phases("TRANSFER_AL_NASSR_2023")
    assert ev["anchor"] == "event start (occurred_at)"
    assert all("kickoff" not in p["description"] for p in ev["phases"])
    event_phase = next(p for p in ev["phases"] if p["window_type"] == "EVENT")
    assert event_phase["start"] == _dt(2023, 1, 3, 15) and event_phase["end"] == _dt(2023, 1, 3, 22)


def test_unknown_event_raises():
    with pytest.raises(ValueError, match="not found"):
        ecp.load_event_phases("NOPE")


@pytest.mark.parametrize("ts,expected", [
    ("2022-12-18T14:00:00Z", ("EVENT", None)),              # start inclusive
    ("2022-12-18T13:59:59Z", ("PRE_EVENT", None)),          # end exclusive
    ("2022-12-18T21:00:00Z", ("EARLY_POST_EVENT", None)),
    ("2022-12-04T14:59:59Z", (ecp.OUTSIDE_PHASE, "before")),
    ("2023-01-01T15:00:00Z", (ecp.OUTSIDE_PHASE, "after")),
    ("2026-07-19T00:00:00Z", (ecp.OUTSIDE_PHASE, "after")),
    ("garbage", (ecp.UNPARSEABLE_PHASE, None)),
])
def test_assign_phase_boundaries(ts, expected):
    assert ecp.assign_phase(ts, ecp.load_event_phases("WC_2022_FINAL")) == expected


def test_assign_phase_reports_gaps_as_between(tmp_path):
    cfg = tmp_path / "events.yml"
    cfg.write_text(json.dumps({"events": [{"external_event_id": "X", "occurred_at": "2023-01-01T00:00:00Z", "windows": [
        {"window_type": "A", "relative_offset_start_hours": -2, "relative_offset_end_hours": -1},
        {"window_type": "B", "relative_offset_start_hours": 1, "relative_offset_end_hours": 2}]}]}))
    ev = ecp.load_event_phases("X", str(cfg))
    assert ev["gaps_between_windows"] == [("A", "B")]
    assert ecp.assign_phase("2023-01-01T00:00:00Z", ev) == (ecp.OUTSIDE_PHASE, "between")


@pytest.fixture
def wc_root(tmp_path):
    m = hpc.build_manifest(_report([_cand("vA"), _cand("vB")]), "r.json", "h", "pilot-comments-v1", 2)
    script = {
        ("vA", None): _page([_thread("a3", "vA", "2025-01-01T00:00:00Z"),   # partial video, outside (after)
                             _thread("a2", "vA", "2022-12-18T16:00:00Z")], "MORE"),  # partial video, EVENT
        ("vB", None): _page([_thread("b2", "vB", "2022-12-19T10:00:00Z"),   # complete video, EARLY_POST_EVENT
                             _thread("b1", "vB", "2022-12-18T09:00:00Z")]),  # complete video, PRE_EVENT
    }
    hpc.run_harvest(m, Fetcher(script), str(tmp_path), SECRET, "Abdul", run_id="r1")
    return m, tmp_path


def test_phase_counts_split_complete_and_partial(wc_root):
    m, root = wc_root
    data = ecp.build_explorer_data(m, str(root), event_id="WC_2022_FINAL")
    pc = data["phase_counts"]
    assert pc["PRE_EVENT"] == [1, 0] and pc["EARLY_POST_EVENT"] == [1, 0]
    assert pc["EVENT"] == [0, 1] and pc[ecp.OUTSIDE_PHASE] == [0, 1]
    assert pc["LATE_POST_EVENT"] == [0, 0] and pc[ecp.UNPARSEABLE_PHASE] == [0, 0]
    assert data["outside_sides"]["after"] == [0, 1]
    assert data["comment_phase"]["a3"] == f"{ecp.OUTSIDE_PHASE} (after)" and data["comment_phase"]["b1"] == "PRE_EVENT"


def test_phase_section_html(wc_root):
    m, root = wc_root
    page = ecp.render_html(ecp.build_explorer_data(m, str(root), event_id="WC_2022_FINAL"))
    assert "Event-relative phases" in page
    assert "2022-12-04 15:00:00 UTC" in page and "2022-12-18 14:00:00 UTC" in page and "2023-01-01 15:00:00 UTC" in page
    assert "From complete-collection videos" in page and "From partial-collection videos" in page
    assert ecp.OUTSIDE_PHASE in page and "after: 0 complete / 1 partial" in page
    assert "Video-relative: time from each video's upload to comment" in page
    assert "First 72h after upload (video-relative)" in page
    assert "phase PRE_EVENT" in page                     # audit sample carries the phase
    assert "from --event-id; not recorded in v1 manifest" in page
    body = page.split("Event-relative phases", 1)[1].split("Video-relative", 1)[0].lower()
    assert "during the match" in body and "“post-match”" in body   # only in the disclaimer sentence
    assert body.count("match") == 2


def test_no_event_selected_shows_notice(wc_root):
    m, root = wc_root
    data = ecp.build_explorer_data(m, str(root))
    assert data["event_phases"] is None and data["phase_counts"] == {}
    assert "No event selected" in ecp.render_html(data)


def test_event_id_conflict_with_manifest(tmp_path):
    report = _report([_cand("vE", published="2023-01-03T16:00:00Z")], event_id="TRANSFER_AL_NASSR_2023")
    m = hpc.build_early_window_manifest(report, "r.json", "h", "al", 72, 1)
    with pytest.raises(ValueError, match="conflicts"):
        ecp.resolve_event_id(m, "WC_2022_FINAL")
    assert ecp.resolve_event_id(m, None) == "TRANSFER_AL_NASSR_2023"


def test_explorer_leaves_collected_data_unchanged(wc_root):
    m, root = wc_root

    def snapshot():
        digests = {}
        for dirpath, _, files in os.walk(root):
            for name in files:
                path = os.path.join(dirpath, name)
                with open(path, "rb") as f:
                    digests[os.path.relpath(path, root)] = hashlib.sha256(f.read()).hexdigest()
        return digests

    before = snapshot()
    manifest_path = root.parent / f"{root.name}-manifest.json"
    manifest_path.write_text(json.dumps(m))
    assert ecp.main(["--manifest", str(manifest_path), "--output-root", str(root), "--event-id", "WC_2022_FINAL",
                     "--out", str(root.parent / f"{root.name}-explorer.html")]) == 0
    assert snapshot() == before


def test_cli_event_conflict_returns_error(tmp_path, capsys):
    report = _report([_cand("vE", published="2023-01-03T16:00:00Z")], event_id="TRANSFER_AL_NASSR_2023")
    m = hpc.build_early_window_manifest(report, "r.json", "h", "al", 72, 1)
    hpc.run_harvest(m, Fetcher({("vE", None): _page([])}), str(tmp_path), SECRET, "A", run_id="r1")
    path = tmp_path.parent / f"{tmp_path.name}-m.json"
    path.write_text(json.dumps(m))
    assert ecp.main(["--manifest", str(path), "--output-root", str(tmp_path), "--event-id", "WC_2022_FINAL"]) == 1
    assert "conflicts" in capsys.readouterr().err
