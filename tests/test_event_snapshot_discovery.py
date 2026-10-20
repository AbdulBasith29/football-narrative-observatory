"""tests/test_event_snapshot_discovery.py
Offline tests for event-parameterised discovery and pilot snapshot configuration.

Validates:
1. Event terms and discovery windows resolve from config/event_windows.yml for the Al Nassr and
   2023 Ballon d'Or events; unknown events fail closed.
2. Legacy behaviour (no event) keeps the WC_2022_FINAL terms and calendar-day window.
3. Report metadata records event_id, occurred_at and how the window was derived (mock mode, offline).
4. Discovery cost upper bounds use official per-endpoint rates.
5. The snapshot configuration parses, references configured events, and leaves collection
   parameters unset until reviewed.
6. The CLI refuses to overwrite an existing report before any API call, and --estimate-only makes none.
"""

from __future__ import annotations

import datetime
import json
import os
import sys
from unittest.mock import patch

import pytest
import yaml

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts")))

import discover_pilot_coverage as dpc  # noqa: E402

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SNAPSHOT_CONFIG = os.path.join(REPO, "config", "discovery", "pilot_event_snapshots.yml")
UTC = datetime.timezone.utc


@pytest.fixture(scope="module")
def config():
    return dpc.load_repository_configuration()


def test_resolve_al_nassr_event(config):
    ev = dpc.resolve_event(config, "TRANSFER_AL_NASSR_2023")
    assert ev["event_terms"] == ["al nassr", "transfer", "unveiling"]
    assert ev["baseline_terms"] == ["al nassr", "saudi football", "saudi pro league"]
    assert ev["window_start"] == datetime.datetime(2023, 1, 3, 15, 0, tzinfo=UTC)
    assert ev["window_end"] == datetime.datetime(2023, 1, 6, 16, 0, tzinfo=UTC)
    assert "EVENT" in ev["window_derivation"] and "LATE_POST_EVENT" in ev["window_derivation"]


def test_resolve_ballon_dor_event(config):
    ev = dpc.resolve_event(config, "BALLON_DOR_2023")
    assert "ballon d'or" in ev["event_terms"]
    assert ev["window_start"] == datetime.datetime(2023, 10, 30, 18, 45, tzinfo=UTC)
    assert ev["window_end"] == datetime.datetime(2023, 11, 2, 19, 45, tzinfo=UTC)


def test_unknown_event_fails_closed(config):
    with pytest.raises(ValueError, match="Unknown event_id"):
        dpc.resolve_event(config, "NOT_AN_EVENT")


def test_mock_discovery_records_event_metadata():
    report = dpc.execute_discovery(mock=True, event_id="TRANSFER_AL_NASSR_2023")
    meta = report["report_metadata"]
    assert meta["event_id"] == "TRANSFER_AL_NASSR_2023"
    assert meta["event_occurred_at"] == "2023-01-03T16:00:00Z"
    assert meta["window_start"].startswith("2023-01-03T15:00:00")
    assert meta["window_end"].startswith("2023-01-06T16:00:00")
    assert "LATE_POST_EVENT" in meta["window_derivation"]
    assert meta["execution_mode"] == "SYNTHETIC_MOCK"


def test_explicit_window_overrides_event_window():
    report = dpc.execute_discovery(mock=True, event_id="BALLON_DOR_2023",
                                   window_start_str="2023-10-30T00:00:00Z", window_end_str="2023-10-31T00:00:00Z")
    meta = report["report_metadata"]
    assert meta["window_derivation"] == "EXPLICIT_ARGUMENTS" and meta["window_start"].startswith("2023-10-30T00:00:00")


def test_legacy_default_unchanged():
    report = dpc.execute_discovery(mock=True)
    meta = report["report_metadata"]
    assert meta["event_id"] is None and meta["window_derivation"] == "LEGACY_WC_2022_CALENDAR_DAYS"
    assert meta["window_start"].startswith("2022-12-18T00:00:00") and meta["window_end"].startswith("2022-12-20T23:59:59")


def test_live_discovery_passes_event_terms_and_window():
    captured = {}

    def fake_classify(candidate, window_start, window_end, player_aliases, event_terms, baseline_terms=None,
                      channel_is_validated=True):
        captured.update(window_start=window_start, window_end=window_end, event_terms=event_terms,
                        baseline_terms=baseline_terms)
        return {**candidate, "status": "ELIGIBLE", "relevance_tier": "PLAYER_MATCH", "player_category": "RONALDO",
                "exclusion_reason": None, "is_short": False, "in_window": True}

    search_result = {"channel_id": "x", "items": [{"video_id": "v1", "channel_id": "UC6c1z7bA__85CIWZ_jpCK-Q"}],
                     "pages_fetched": 1, "discovery_method": "DATE_BOUNDED_SEARCH"}
    with patch.object(dpc, "build_youtube_client", return_value=object()), \
         patch.object(dpc, "validate_channel_identities_live", return_value=({}, [])), \
         patch.object(dpc, "discover_channel_candidates_date_bounded_search", return_value=search_result) as search, \
         patch.object(dpc, "hydrate_video_metadata_live", return_value={"v1": {"video_id": "v1", "comment_count": "10"}}), \
         patch.object(dpc, "classify_hydrated_candidate", side_effect=fake_classify):
        report = dpc.execute_discovery(event_id="TRANSFER_AL_NASSR_2023", max_pages_per_channel=2)
    assert search.call_args.kwargs["window_start"] == datetime.datetime(2023, 1, 3, 15, 0, tzinfo=UTC)
    assert captured["event_terms"] == ["al nassr", "transfer", "unveiling"]
    assert captured["window_end"] == datetime.datetime(2023, 1, 6, 16, 0, tzinfo=UTC)
    assert report["report_metadata"]["event_id"] == "TRANSFER_AL_NASSR_2023"
    assert report["report_metadata"]["execution_mode"] == "LIVE_YOUTUBE_API"


def test_estimate_discovery_cost():
    search = dpc.estimate_discovery_cost(7, 2, "search")
    assert search["requests_upper_bound"] == {"channels.list": 1, "search.list": 14, "videos.list": 14}
    assert search["quota_units_upper_bound"] == 1 + 1400 + 14
    uploads = dpc.estimate_discovery_cost(7, 2, "uploads_playlist")
    assert uploads["quota_units_upper_bound"] == 1 + 14 + 14


def test_snapshot_config_is_consistent(config):
    with open(SNAPSHOT_CONFIG, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    snaps = {s["snapshot_id"]: s for s in data["snapshots"]}
    assert set(snaps) == {"WC_2022_FINAL_PILOT_V1", "AL_NASSR_2023_PILOT_V1", "BALLON_DOR_2023_PILOT_V1"}
    configured = {ev["external_event_id"] for ev in config["events"]}
    for snap in snaps.values():
        assert snap["event_id"] in configured
        assert snap["discovery"]["method"] == "search"
    for sid in ("AL_NASSR_2023_PILOT_V1", "BALLON_DOR_2023_PILOT_V1"):
        coll = snaps[sid]["collection"]
        assert snaps[sid]["status"] == "PROPOSED" and coll["mode"] == "EARLY_WINDOW"
        assert coll["window_hours"] is None and coll["max_requests_per_video"] is None  # set only after review
    outputs = [s["discovery"]["report_output"] for s in snaps.values()]
    assert len(outputs) == len(set(outputs))


def test_snapshot_loader_and_defaults():
    snap = dpc.load_snapshot(SNAPSHOT_CONFIG, "BALLON_DOR_2023_PILOT_V1")
    assert snap["event_id"] == "BALLON_DOR_2023"
    with pytest.raises(ValueError):
        dpc.load_snapshot(SNAPSHOT_CONFIG, "NOPE")

    class Args:
        snapshot, snapshot_config, event_id, method, max_pages, output = (
            "AL_NASSR_2023_PILOT_V1", SNAPSHOT_CONFIG, None, None, None, None)
    args = Args()
    dpc.apply_snapshot_defaults(args)
    assert (args.event_id, args.method, args.max_pages) == ("TRANSFER_AL_NASSR_2023", "search", 2)
    assert args.output.endswith("AL_NASSR_2023_PILOT_V1/discovery_report.json")

    args = Args()
    args.event_id = "BALLON_DOR_2023"
    with pytest.raises(ValueError, match="conflicts"):
        dpc.apply_snapshot_defaults(args)


def test_cli_estimate_only_makes_no_api_calls(capsys):
    with patch.object(sys, "argv", ["discover", "--snapshot", "AL_NASSR_2023_PILOT_V1", "--snapshot-config",
                                    SNAPSHOT_CONFIG, "--estimate-only"]), \
         patch.object(dpc, "build_youtube_client", side_effect=AssertionError("no API client in estimate mode")):
        dpc.main()
    out = capsys.readouterr().out
    assert "TRANSFER_AL_NASSR_2023" in out and "quota_units<=1415" in out


def test_cli_refuses_overwrite_before_api_calls(tmp_path):
    existing = tmp_path / "report.json"
    existing.write_text("{}")
    with patch.object(sys, "argv", ["discover", "--event-id", "BALLON_DOR_2023", "--output", str(existing)]), \
         patch.object(dpc, "build_youtube_client", side_effect=AssertionError("must not reach the API")):
        with pytest.raises(SystemExit) as exc:
            dpc.main()
    assert exc.value.code == 1 and existing.read_text() == "{}"


def test_replay_preserves_event_metadata(tmp_path):
    report = dpc.execute_discovery(mock=True, event_id="BALLON_DOR_2023")
    src = tmp_path / "src.json"
    src.write_text(json.dumps(report))
    replayed = dpc.regenerate_from_saved_report(str(src))
    assert replayed["report_metadata"]["event_id"] == "BALLON_DOR_2023"
    assert "LATE_POST_EVENT" in replayed["report_metadata"]["window_derivation"]
