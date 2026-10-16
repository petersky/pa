"""Control-plane counts must not decode retained execution/observation proofs."""
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest

from pa.config import Settings
from pa.core.performance import OperationObservation, current_observation
from pa.pr_supervisor.models import GITHUB_TERMINAL_PR_WATCH_STATUSES, PRWatch, PRWatchStatus
from pa.pr_supervisor.service import PRSupervisor
from pa.pr_supervisor.store import PRSupervisorStore


def catalog(tmp_path):
    store = PRSupervisorStore(tmp_path / "supervisor.db")
    for index, status in enumerate(PRWatchStatus):
        for archived in (False, True):
            store.upsert_watch(PRWatch(
                repository="example/repo", pr_number=index * 2 + int(archived) + 1,
                pr_url="https://github.com/example/repo/pull/1", status=status,
                owner_instance_id="local" if index % 2 else "peer", fence_token=index + 7,
                retired_at=datetime.now(UTC) if archived else None,
                state={"retained_proof": ["fixture " * 1000] * 8},
            ), preserve_lease=False)
    return store


def expected_inventory(watches, owner):
    active = [watch for watch in watches if watch.actionable]
    return {
        "active_watches": len(active), "historical_watches": len(watches) - len(active),
        "archived_watches": sum(watch.retired_at is not None for watch in watches),
        "terminal_retirement_backlog": sum(watch.status in GITHUB_TERMINAL_PR_WATCH_STATUSES and watch.retired_at is None for watch in watches),
        "locally_owned_watches": sum(watch.owner_instance_id == owner for watch in active),
        "max_fence_token": max((watch.fence_token for watch in active), default=0),
    }


def test_aggregate_matches_every_lifecycle_and_preserves_metric_counters(tmp_path):
    store = catalog(tmp_path)
    watches = store.list_watches(include_retired=True)
    store.increment_metric("polls", 5)
    with patch.object(store, "_row_to_watch", side_effect=AssertionError("proof hydration")):
        for owner in ("local", "peer", "missing"):
            assert store.watch_inventory(owner_instance_id=owner) == expected_inventory(watches, owner)
        observation = OperationObservation()
        token = current_observation.set(observation)
        try:
            metrics = store.metrics()
        finally:
            current_observation.reset(token)
    expected = expected_inventory(watches, "local")
    assert metrics == {"polls": 5, **{name: expected[name] for name in (
        "active_watches", "historical_watches", "archived_watches", "terminal_retirement_backlog")}}
    assert observation.sqlite_rows == 2  # One metric row and one aggregate row.


def test_empty_inventory_uses_zero_counts(tmp_path):
    store = PRSupervisorStore(tmp_path / "empty.db")
    assert set(store.watch_inventory(owner_instance_id="local").values()) == {0}


@pytest.mark.parametrize("fence", [-4, 3])
def test_maximum_ignores_inactive_fences_even_for_legacy_negative_values(tmp_path, fence):
    store = PRSupervisorStore(tmp_path / "fences.db")
    for number, status, token in ((1, "active", fence), (2, "closed", 999)):
        store.upsert_watch(PRWatch(repository="example/repo", pr_number=number,
            pr_url=f"https://github.com/example/repo/pull/{number}", status=status,
            fence_token=token), preserve_lease=False)
    assert store.watch_inventory()["max_fence_token"] == fence


@pytest.mark.asyncio
async def test_authority_health_uses_counts_without_decoding_proofs(tmp_path):
    store = catalog(tmp_path)
    expected = expected_inventory(store.list_watches(include_retired=True), "local")
    settings = Settings(data_dir=tmp_path, instance_id="local")
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(500))) as client:
        service = PRSupervisor(settings, MagicMock(), supervisor_store=store, http_client=client)
        with patch.object(store, "list_watches", side_effect=AssertionError("full catalog read")), patch.object(store, "_row_to_watch", side_effect=AssertionError("proof hydration")):
            health = service.authority_health()
    assert {key: health[key] for key in expected} == expected
