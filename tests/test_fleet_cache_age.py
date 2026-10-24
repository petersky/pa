"""Fleet cache age must not turn historical success into current readiness."""
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from pa.domain.models import FleetInstance
from pa.fleet.overview import (
    DIMENSIONS, FleetOverviewCache, _cached_or_default, field, required_readiness,
)

NOW = datetime(2026, 10, 24, 15, 0, tzinfo=UTC)
INSTANCE = FleetInstance(instance_id="remote", name="remote", url="http://remote:8080")


@pytest.mark.parametrize("dimension", DIMENSIONS)
def test_cached_success_expires_without_replacing_attempt_or_persisting(tmp_path, dimension):
    cache = FleetOverviewCache(tmp_path)
    value = field("fresh", {"kept": ["complete"]},
                  observed_at=(NOW - timedelta(days=1)).isoformat(), duration_ms=17)
    cache.put("remote", dimension, value, attempt_id=1)
    original = cache.get("remote", dimension)
    revision = cache.revision
    with patch("pa.fleet.overview.datetime") as clock:
        clock.now.return_value = NOW
        clock.fromisoformat.side_effect = datetime.fromisoformat
        aged = _cached_or_default(cache, INSTANCE, dimension)
    assert aged["state"] == "stale"
    assert {k: v for k, v in aged.items() if k != "state"} == {
        k: v for k, v in original.items() if k != "state"
    }
    assert aged["last_attempt_state"] == "fresh"
    assert cache.revision == revision
    assert cache.get("remote", dimension) == original
    assert FleetOverviewCache(tmp_path).get("remote", dimension) == original


@pytest.mark.parametrize("dimension,ttl", [("status", 3), ("providers", 30),
                                         ("update", 30), ("mcp_bootstrap", 30)])
@pytest.mark.parametrize("offset,expected", [(-0.001, "fresh"), (0, "stale"), (0.001, "stale")])
def test_cached_age_uses_existing_probe_ttl(tmp_path, dimension, ttl, offset, expected):
    cache = FleetOverviewCache(tmp_path)
    cache.put("remote", dimension, field("fresh", {},
              observed_at=(NOW - timedelta(seconds=ttl + offset)).isoformat()))
    with patch("pa.fleet.overview.datetime") as clock:
        clock.now.return_value = NOW
        clock.fromisoformat.side_effect = datetime.fromisoformat
        assert _cached_or_default(cache, INSTANCE, dimension)["state"] == expected


@pytest.mark.parametrize("observed", [None, "invalid", "2026-10-24T15:00:00", "2026-10-25T15:00:00+00:00"])
def test_unverifiable_timestamp_does_not_claim_fresh(tmp_path, observed):
    cache = FleetOverviewCache(tmp_path)
    cache.put("remote", "providers", field("fresh", [], observed_at=observed))
    with patch("pa.fleet.overview.datetime") as clock:
        clock.now.return_value = NOW
        clock.fromisoformat.side_effect = datetime.fromisoformat
        assert _cached_or_default(cache, INSTANCE, "providers")["state"] == "stale"


def test_failed_refresh_diagnostics_are_unchanged(tmp_path):
    cache = FleetOverviewCache(tmp_path)
    cache.put("remote", "sync", field("fresh", {"consistent": False}, observed_at=NOW.isoformat()), attempt_id=1)
    cache.put("remote", "sync", field("timeout", None, error="deadline", duration_ms=4000), attempt_id=2)
    original = cache.get("remote", "sync")
    assert _cached_or_default(cache, INSTANCE, "sync") == original
    assert original["last_attempt_state"] == "timeout"
    assert original["value"] == {"consistent": False}


def test_expired_success_is_stale_and_mismatch_is_historical():
    dimensions = {name: field("fresh", {"health": "up"} if name == "reachability" else {}, observed_at=NOW.isoformat())
                  for name in ("reachability", "status", "sync")}
    dimensions["sync"] = {**field("fresh", {"consistent": False}, observed_at=NOW.isoformat()), "state": "stale"}
    assert required_readiness(dimensions) == "stale"
    dimensions["status"] = field("timeout", None, error="deadline")
    assert required_readiness(dimensions) == "timeout"
    dimensions["sync"]["state"] = "fresh"
    assert required_readiness(dimensions) == "error"
