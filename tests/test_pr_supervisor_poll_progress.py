"""Active watches must not wait behind auxiliary startup/completion batches."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from pa.pr_supervisor.service import PRSupervisor


async def offload(operation, call, *args, **kwargs):
    return call(*args, **kwargs)


@pytest.mark.asyncio
async def test_first_active_poll_precedes_legacy_discovery_without_reprobing_credentials():
    service = object.__new__(PRSupervisor)
    capability = object()
    calls = []
    service._stopping = False
    service.refresh_capability = AsyncMock(return_value=capability)
    service._poll_due = AsyncMock(side_effect=lambda cap: calls.append(("poll",cap)))
    async def discovery():
        calls.append(("discovery",None))
        service._stopping = True
    service.migrate_discoverable_associations = discovery
    await service._run_loop()
    assert calls == [("poll",capability),("discovery",None)]
    service.refresh_capability.assert_awaited_once_with(force=True)


@pytest.mark.asyncio
async def test_active_watch_observed_before_blocked_completion_batch():
    service = object.__new__(PRSupervisor)
    watch = SimpleNamespace(id="ready-watch",repository="owner/repo",actionable=True)
    capability = SimpleNamespace(supports=lambda repository:True)
    service.refresh_capability = AsyncMock(return_value=capability)
    service._worker_heartbeat = Mock()
    service._prune_lease_state = AsyncMock()
    service._renew_due_local_leases = AsyncMock()
    service.store = SimpleNamespace(list_due=lambda:[watch],get_watch=lambda identity:watch)
    service._offload = offload
    service.eligibility_journal_hook = None
    service._acquire_lease = AsyncMock(return_value=SimpleNamespace(acquired=True))
    service._process_watch = AsyncMock()
    entered, release = asyncio.Event(), asyncio.Event()
    async def completion():
        entered.set()
        await release.wait()
    service._reconcile_merged_cards = completion
    task = asyncio.create_task(service.run_once())
    try:
        await asyncio.wait_for(entered.wait(),1)
        service._process_watch.assert_awaited_once_with(watch,service._acquire_lease.return_value)
        release.set()
        await task
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task,return_exceptions=True)


@pytest.mark.asyncio
async def test_completed_card_units_report_progress_but_failed_unit_does_not():
    service = object.__new__(PRSupervisor)
    service.store = SimpleNamespace(list_card_completion_due=lambda:["first","second","third"])
    service._offload = offload
    heartbeats = []
    completed = []
    service._worker_heartbeat = lambda:heartbeats.append(tuple(completed))
    async def complete(watch):
        if watch=="third":
            raise RuntimeError("blocked unit")
        completed.append(watch)
    service._complete_merged_card = complete
    with pytest.raises(RuntimeError):
        await service._reconcile_merged_cards()
    assert heartbeats == [("first",),("first","second")]


@pytest.mark.asyncio
async def test_initial_poll_failure_does_not_drop_legacy_discovery_or_swallow_cancellation():
    service = object.__new__(PRSupervisor)
    service._stopping = False
    service.refresh_capability = AsyncMock(return_value=object())
    service._poll_due = AsyncMock(side_effect=ValueError("transient poll failure"))
    service.store = SimpleNamespace(increment_metric=Mock())
    service._offload = AsyncMock()
    discovered = []
    async def discovery():
        discovered.append(True)
        service._stopping = True
    service.migrate_discoverable_associations = discovery
    await service._run_loop()
    assert discovered == [True]
    assert service._offload.await_args.args[-1] == "loop_errors"
    service._stopping = False
    service._poll_due = AsyncMock(side_effect=asyncio.CancelledError)
    with pytest.raises(asyncio.CancelledError):
        await service._run_loop()
    assert discovered == [True]
