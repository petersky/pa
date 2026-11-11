"""Fleet overview cache: legacy single-file migration and write amplification."""

from __future__ import annotations

import json
from unittest.mock import patch

from pa.core.io import atomic_write_json
from pa.fleet.overview import FleetOverviewCache, field


def _legacy(tmp_path, **instances):
    path = tmp_path / "fleet_overview_cache.json"
    path.write_text(json.dumps({"revision": 5, "instances": instances}))
    return path


def test_legacy_cache_is_not_read_once_per_dimension_files_exist(tmp_path):
    legacy = _legacy(
        tmp_path,
        old={"sync": field("fresh", {"head": "frozen"}, observed_at="2026-09-01T00:00:00+00:00")},
        live={"sync": field("fresh", {"head": "stale-legacy"}, observed_at="2026-09-01T00:00:00+00:00")},
    )
    seeded = FleetOverviewCache(tmp_path)
    # No per-dimension files yet: the legacy file is still the baseline and stays.
    assert seeded.get("old", "sync")["value"] == {"head": "frozen"}
    assert legacy.exists()
    seeded.put("live", "sync", field("fresh", {"head": "current"}, observed_at="2026-11-10T00:00:00+00:00"))

    reopened = FleetOverviewCache(tmp_path)

    assert reopened.get("live", "sync")["value"] == {"head": "current"}
    # The frozen single-file entries no longer resurface...
    assert reopened.get("old", "sync") is None
    # ...and the migration removed the file it confirmed was superseded.
    assert not legacy.exists()
    assert (tmp_path / "fleet_overview_cache.d").is_dir()


def test_legacy_cache_survives_when_per_dimension_cache_is_unusable(tmp_path):
    legacy = _legacy(tmp_path, old={"sync": field("fresh", {"head": "frozen"})})
    directory = tmp_path / "fleet_overview_cache.d"
    directory.mkdir()
    (directory / "broken.json").write_text("{not json")

    cache = FleetOverviewCache(tmp_path)

    # The only per-dimension file is unreadable, so nothing confirms the new
    # cache is present: the legacy record stays both on disk and as baseline.
    assert legacy.exists()
    assert cache.get("old", "sync")["value"] == {"head": "frozen"}


def test_empty_data_dir_has_nothing_to_migrate(tmp_path):
    cache = FleetOverviewCache(tmp_path)
    assert cache.get("any", "sync") is None
    assert not (tmp_path / "fleet_overview_cache.json").exists()


class _Writes:
    def __init__(self):
        self.paths: list[str] = []

    def __call__(self, path, payload):
        self.paths.append(path.name)
        atomic_write_json(path, payload)


def _fresh(head, observed_at, duration_ms=5):
    return field("fresh", {"head": head}, observed_at=observed_at, duration_ms=duration_ms)


def test_clock_only_refresh_updates_memory_but_not_disk(tmp_path):
    cache = FleetOverviewCache(tmp_path)
    writes = _Writes()
    with patch("pa.fleet.overview.atomic_write_json", side_effect=writes):
        assert cache.put("a", "sync", _fresh("h1", "2026-11-10T00:00:00+00:00"))
        assert len(writes.paths) == 1
        for second in range(1, 6):
            assert cache.put(
                "a",
                "sync",
                _fresh("h1", f"2026-11-10T00:00:0{second}+00:00", duration_ms=5 + second),
            )
        assert len(writes.paths) == 1, "only clocks changed; no rewrite expected"
        # In-memory readers still see the freshest observation.
        assert cache.get("a", "sync")["observed_at"] == "2026-11-10T00:00:05+00:00"
        revision = cache.revision
        # A real content change is written immediately.
        cache.put("a", "sync", _fresh("h2", "2026-11-10T00:00:07+00:00"))
        assert len(writes.paths) == 2
        assert cache.revision > revision
    reopened = FleetOverviewCache(tmp_path)
    assert reopened.get("a", "sync")["value"] == {"head": "h2"}


def test_unchanged_dimension_is_still_rewritten_after_the_refresh_bound(tmp_path):
    now = [1000.0]
    cache = FleetOverviewCache(tmp_path, persist_refresh_seconds=60)
    cache._clock = lambda: now[0]
    writes = _Writes()
    with patch("pa.fleet.overview.atomic_write_json", side_effect=writes):
        cache.put("a", "sync", _fresh("h1", "2026-11-10T00:00:00+00:00"))
        now[0] += 59
        cache.put("a", "sync", _fresh("h1", "2026-11-10T00:00:59+00:00"))
        assert len(writes.paths) == 1
        now[0] += 2
        cache.put("a", "sync", _fresh("h1", "2026-11-10T00:01:01+00:00"))
        assert len(writes.paths) == 2
    on_disk = FleetOverviewCache(tmp_path).get("a", "sync")
    assert on_disk["observed_at"] == "2026-11-10T00:01:01+00:00"


def test_failed_probe_and_recovery_are_persisted_not_mistaken_for_clock_noise(tmp_path):
    cache = FleetOverviewCache(tmp_path)
    writes = _Writes()
    with patch("pa.fleet.overview.atomic_write_json", side_effect=writes):
        cache.put("a", "sync", _fresh("h1", "2026-11-10T00:00:00+00:00"), attempt_id=1)
        cache.put("a", "sync", field("timeout", None, error="deadline"), attempt_id=2)
        cache.put("a", "sync", _fresh("h1", "2026-11-10T00:00:09+00:00"), attempt_id=3)
    assert len(writes.paths) == 3


def test_reopened_cache_does_not_rewrite_unchanged_dimensions(tmp_path):
    first = FleetOverviewCache(tmp_path)
    first.put("a", "sync", _fresh("h1", "2026-11-10T00:00:00+00:00"))
    reopened = FleetOverviewCache(tmp_path)
    writes = _Writes()
    with patch("pa.fleet.overview.atomic_write_json", side_effect=writes):
        reopened.put("a", "sync", _fresh("h1", "2026-11-10T00:00:30+00:00"))
    assert writes.paths == []
