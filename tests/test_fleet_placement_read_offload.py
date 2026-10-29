"""Placement's synchronous planning/policy readers must not stall its event loop."""
import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
import pytest_asyncio
from fastapi import HTTPException

from pa.config import Settings
from pa.core.async_runtime import AsyncRuntime
from pa.domain.models import Project
from pa.domain.projection import CardProjection
from pa.execution.dispatch import DispatchRecord
from pa.fleet.placement import PlacementCandidate, PlacementError
from pa.fleet.policy import FleetPolicyService
from pa.modules import fleet

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def placement(tmp_path):
    store = CardProjection(tmp_path / "fixture.db")
    policy = FleetPolicyService(store)
    candidates = [PlacementCandidate(instance_id="local", name="Local")]
    runtime = AsyncRuntime(max_workers=1)
    services = {"async_runtime": runtime, "fleet_policy": policy, "fleet_registry": SimpleNamespace(list_instances=lambda: []),
                "placement_service": SimpleNamespace(resolve=Mock(side_effect=PlacementError("fixture_stop", "Read-only retry fixture")))}
    ctx = SimpleNamespace(store=store, services=services,
                          settings=Settings(data_dir=tmp_path / "data", workspace_root=tmp_path / "workspaces",
                                            instance_id="local", agent_enabled=False, auth_required=False),
                          require_service=services.__getitem__)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)),
                              headers={"Idempotency-Key": "fixture-retry"})
    selection = SimpleNamespace(joint=Mock(return_value=SimpleNamespace(
        execution_selection={"selected": {"harness": "codex", "model": "fixture-balanced", "options": {}}},
        eligible_candidates=[{"instance_id": "local", "provider_id": "codex"}],
        rejected_candidates=[], chosen_instance_id="local")))
    with (patch.object(fleet, "_placement_candidates", AsyncMock(return_value=candidates)),
          patch.object(fleet, "selection_service_for", return_value=selection),
          patch.object(fleet, "get_principal_id", return_value="user:fixture")):
        try:
            yield SimpleNamespace(store=store, policy=policy, candidates=candidates, request=request,
                                  ctx=ctx, selection=selection, services=services)
        finally:
            await runtime.close()


def body(**kwargs):
    return fleet.FleetDispatchBody(execution_contract={
        "version": 1, "profile": "research", "confirmed": True, "requirements": {},
    }, **kwargs)


async def resolve(env, **kwargs):
    return await fleet._resolve_policy_placement(env.request, body(**kwargs), card=None,
                                                project=Project(id="p", title="Fixture"), project_id="p")


@pytest.mark.parametrize("method", ["list_project_repositories", "list_placement_defaults", "get_instance_participation_policy"])
async def test_placement_native_readers_run_off_loop(placement, method):
    reader = getattr(placement.store, method)
    threads = []

    def read(*args, **kwargs):
        threads.append(threading.get_ident())
        return reader(*args, **kwargs)

    with patch.object(placement.store, method, side_effect=read):
        decision, plan = await resolve(placement)
    assert decision.resolved_provider == "codex" and plan.profile.value == "research"
    assert threads and all(t != threading.get_ident() for t in threads)


async def test_retry_policy_readers_run_off_loop_before_any_retry_effect(placement):
    record = DispatchRecord(dispatch_id="retry", mutation_id="retry-mutation", realm_id="default",
                            authority_instance_id="local", authority_url="http://fixture.invalid",
                            state="failed", recoverable=True, target_instance_id="local", principal_id="user:fixture")
    ledger = SimpleNamespace(get=lambda _: record, retry_with_capacity=Mock())
    reader = placement.store.get_instance_participation_policy
    threads = []
    with (patch.object(fleet, "require_user", return_value=SimpleNamespace(role="user")),
          patch.object(fleet, "_require_dispatch_access"),
          patch.object(fleet, "_dispatch_store", return_value=ledger),
          patch.object(placement.store, "get_instance_participation_policy",
                       side_effect=lambda *a, **k: threads.append(threading.get_ident()) or reader(*a, **k))):
        with pytest.raises(HTTPException) as caught:
            await fleet._retry_dispatch_api(placement.request, record.dispatch_id)
    assert caught.value.status_code == 409 and caught.value.detail["code"] == "fixture_stop"
    assert threads and all(t != threading.get_ident() for t in threads)
    ledger.retry_with_capacity.assert_not_called()


@pytest.mark.parametrize("method", ["list_project_repositories", "get_instance_participation_policy"])
async def test_cancelled_policy_read_cannot_continue_to_selection(placement, method):
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    reader = getattr(placement.store, method)

    def blocked(*args, **kwargs):
        entered.set()
        try:
            assert release.wait(5), "Fixture must release the read-only worker"
            return reader(*args, **kwargs)
        finally:
            finished.set()

    with patch.object(placement.store, method, side_effect=blocked):
        task = asyncio.create_task(resolve(placement))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            placement.selection.joint.assert_not_called()
        finally:
            release.set()
            assert await asyncio.to_thread(finished.wait, 2)
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    placement.selection.joint.assert_not_called()


async def test_missing_group_keeps_native_recoverable_error(placement):
    with pytest.raises(PlacementError) as caught:
        await resolve(placement, group_id="missing-fixture-group")
    assert caught.value.code == "configured_group_unavailable"
    assert caught.value.message == "The requested or configured worker group is missing or archived."
    assert caught.value.recoverable is True
    placement.selection.joint.assert_not_called()


@pytest.mark.parametrize("method", ["list_instance_participation_policies", "get_instance_participation_policy"])
async def test_non_group_storage_error_is_not_reclassified(placement, method):
    error = ValueError("fixture storage error")
    with patch.object(placement.store, method, side_effect=error):
        with pytest.raises(ValueError) as caught:
            await resolve(placement)
    assert caught.value is error
    placement.selection.joint.assert_not_called()


async def test_stale_preview_stops_before_any_planning_read(placement):
    with patch.object(placement.store, "list_project_repositories") as read:
        with pytest.raises(PlacementError) as caught:
            await resolve(placement, expected_card_version="2026-10-29T00:00:00Z")
    assert caught.value.code == "stale_card_preferences"
    read.assert_not_called()
    placement.selection.joint.assert_not_called()


async def test_each_selection_reads_fresh_native_policy(placement):
    from pa.fleet.policy import InstanceParticipationPolicy

    reader = placement.store.get_instance_participation_policy
    with patch.object(placement.store, "get_instance_participation_policy", wraps=reader) as read:
        await resolve(placement)
        assert not placement.candidates[0].participation_policy_explicit
        reads_before_mutation = read.call_count
        placement.store.set_instance_participation_policy(
            InstanceParticipationPolicy(instance_id="local", maintenance=True),
            principal_id="user:fixture", instance_id="local",
        )
        reads_after_mutation = read.call_count
        await resolve(placement)
    assert reads_before_mutation >= 1
    assert read.call_count > reads_after_mutation
    assert placement.candidates[0].participation_policy_explicit
    assert placement.candidates[0].participation_policy.maintenance


async def test_missing_native_runtime_uses_read_only_fallback(placement):
    runtime = placement.services.pop("async_runtime")
    threads = []
    reader = placement.store.list_project_repositories
    try:
        with patch.object(placement.store, "list_project_repositories",
                          side_effect=lambda *a, **k: threads.append(threading.get_ident()) or reader(*a, **k)):
            await resolve(placement)
    finally:
        placement.services["async_runtime"] = runtime
    assert threads and all(t != threading.get_ident() for t in threads)
