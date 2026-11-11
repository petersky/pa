"""Authority-side liveness for remote dispatches that are running."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pa.execution.dispatch import CapacityAdmission, DispatchEvent, DispatchRecord, DispatchStore
from pa.execution.liveness import RunningLivenessSweeper, TargetStatus, last_activity
from pa.execution.reconciliation import CompletionReconciler
from pa.modules.fleet import (
    DispatchCompletionBody, _reoffer_lost_dispatch, complete_dispatch, dispatch_target_status,
)
from tests.test_dispatch_two_ledger_completion import (  # noqa: F401 - pytest fixture
    AUTHORITY, DISPATCH, SESSION, _deliver_initial_prompt, pair,
)

NOW = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)


def running(dispatch_id="r1", *, silent: timedelta, target="peer", **extra) -> DispatchRecord:
    stamp = NOW - silent
    return DispatchRecord(
        dispatch_id=dispatch_id, mutation_id=f"m-{dispatch_id}", authority_instance_id="local",
        authority_url="http://local.invalid", target_instance_id=target, session_id=f"s-{dispatch_id}",
        state="running", created_at=stamp, updated_at=stamp, request_payload={"message": "x"},
        events=[DispatchEvent(seq=1, state="running", message="Prompt accepted.", created_at=stamp)],
        **extra,
    )


@pytest.fixture
def ledger(tmp_path):
    store = DispatchStore(tmp_path)
    yield store
    store.close()


def sweeper(ledger, status: TargetStatus, **overrides):
    hooks = {
        "probe": AsyncMock(return_value=status),
        "adopt": AsyncMock(),
        "reoffer": AsyncMock(return_value=True),
        "notify": MagicMock(),
    }

    async def fail_lost(record, reason, code):
        return ledger.fail(record, reason, code=code, recoverable=True)

    hooks["fail_lost"] = AsyncMock(side_effect=fail_lost)
    hooks.update(overrides)
    return RunningLivenessSweeper(
        ledger, instance_id="local", stale_seconds=1800, lost_seconds=21600, **hooks
    ), hooks


@pytest.mark.asyncio
async def test_recent_activity_is_not_probed(ledger):
    ledger.put(running(silent=timedelta(minutes=5)))
    sw, hooks = sweeper(ledger, TargetStatus(reachable=True, known=True))
    assert await sw.sweep_once(NOW) == []
    hooks["probe"].assert_not_awaited()


@pytest.mark.asyncio
async def test_local_and_settled_dispatches_are_never_swept(ledger):
    ledger.put(running("local-target", silent=timedelta(days=9), target="local"))
    ledger.put(running("acked", silent=timedelta(days=9), acknowledged_at=NOW))
    sw, hooks = sweeper(ledger, TargetStatus(reachable=True, known=False))
    assert await sw.sweep_once(NOW) == []
    hooks["probe"].assert_not_awaited()


@pytest.mark.asyncio
async def test_target_completion_is_adopted(ledger):
    ledger.put(running(silent=timedelta(hours=1)))
    completion = {"mutation_id": "m-r1", "result": {}}
    sw, hooks = sweeper(ledger, TargetStatus(reachable=True, known=True, completion=completion))
    assert await sw.sweep_once(NOW) == [("r1", "adopted_completion")]
    hooks["adopt"].assert_awaited_once()
    hooks["fail_lost"].assert_not_awaited()


@pytest.mark.asyncio
async def test_confirmed_running_counts_as_activity_and_is_not_reprobed(ledger):
    ledger.put(running(silent=timedelta(hours=1)))
    sw, hooks = sweeper(ledger, TargetStatus(reachable=True, known=True, state="running"))
    assert await sw.sweep_once(NOW) == [("r1", "confirmed_running")]
    assert ledger.get("r1").state == "running"
    # The confirmation is stamped "now" (real clock), so a sweep at the real time sees fresh activity.
    assert last_activity(ledger.get("r1")) > NOW
    assert await sw.sweep_once(datetime.now(UTC)) == []
    assert hooks["probe"].await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status,code", [
    (TargetStatus(reachable=True, known=False), "remote_dispatch_unknown"),
    (TargetStatus(reachable=True, known=True, state="failed"), "remote_dispatch_dead"),
    (TargetStatus(reachable=True, known=True, state="running", session_status="recovery_blocked"),
     "remote_dispatch_dead"),
])
async def test_target_that_lost_the_dispatch_fails_it_notifies_and_reoffers(ledger, status, code):
    ledger.put(running(silent=timedelta(hours=1)))
    sw, hooks = sweeper(ledger, status)
    assert await sw.sweep_once(NOW) == [("r1", "failed_and_reoffered")]
    failed = ledger.get("r1")
    assert failed.state == "failed" and failed.recoverable is True and failed.error_code == code
    hooks["notify"].assert_called_once()
    hooks["reoffer"].assert_awaited_once()


@pytest.mark.asyncio
async def test_reoffer_can_be_disabled(ledger):
    ledger.put(running(silent=timedelta(hours=1)))
    sw, hooks = sweeper(ledger, TargetStatus(reachable=True, known=False), reoffer_enabled=False)
    assert await sw.sweep_once(NOW) == [("r1", "failed")]
    hooks["reoffer"].assert_not_awaited()
    hooks["notify"].assert_called_once()


@pytest.mark.asyncio
async def test_unreachable_target_is_failed_only_after_the_longer_threshold(ledger):
    ledger.put(running("brief", silent=timedelta(hours=2)))
    ledger.put(running("long", silent=timedelta(hours=7)))
    sw, hooks = sweeper(ledger, TargetStatus(reachable=False, error="timed out"))
    assert await sw.sweep_once(NOW) == [("long", "failed_and_reoffered")]
    assert ledger.get("brief").state == "running"
    assert ledger.get("long").error_code == "remote_dispatch_unreachable"


@pytest.mark.asyncio
async def test_peer_without_a_status_endpoint_is_treated_as_silent_not_lost(ledger):
    ledger.put(running(silent=timedelta(hours=2)))
    sw, _hooks = sweeper(ledger, TargetStatus(reachable=True, known=None, error="HTTP 404"))
    assert await sw.sweep_once(NOW) == []
    assert ledger.get("r1").state == "running"


@pytest.mark.asyncio
async def test_disabled_when_stale_seconds_is_zero(ledger):
    ledger.put(running(silent=timedelta(days=96)))
    sw, hooks = sweeper(ledger, TargetStatus(reachable=True, known=False))
    sw._stale = 0
    assert await sw.sweep_once(NOW) == []
    hooks["probe"].assert_not_awaited()


# --- two real ledgers: the target has the completion, the authority never got it ---------

@pytest.mark.asyncio
async def test_authority_adopts_a_completion_the_target_could_not_deliver(pair):
    await _deliver_initial_prompt(pair)
    pair.authority.transition(pair.authority.get(DISPATCH), "running", "Prompt accepted.")
    outbox = MagicMock()
    outbox.queue = lambda session_id, payload: pair.target.queue_completion_payload(session_id, payload)
    reconciler = CompletionReconciler(
        pair.target, SimpleNamespace(async_runtime=None, store=MagicMock(), get=lambda _s: pair.runtime),
        outbox, MagicMock(), lambda: None, retry_seconds=0.01,
    )
    assert await reconciler.handle_completion(SESSION, {
        "stop_reason": "end_turn",
        "card_disposition": {"contract": "pa.card-disposition/v1", "lane": "active", "outcome": "Done.",
                             "evidence": {"integration_required": False, "references": ["card:card-1"]}},
    })
    assert pair.target.get(DISPATCH).state == "completion_pending"   # outbox never delivers
    assert pair.authority.get(DISPATCH).state == "running"

    pair.target_request.headers = {"X-PA-Origin-Instance-ID": AUTHORITY}
    pair.target_request.app.state.ctx.store.get_session.return_value = SimpleNamespace(status="idle")

    async def probe(record):
        data = dispatch_target_status(pair.target_request, record.dispatch_id)
        return TargetStatus(reachable=True, known=data["known"], state=data["state"],
                            session_status=data["session_status"], completion=data["completion"])

    async def adopt(record, completion):
        shim = MagicMock()
        shim.headers = {"idempotency-key": record.mutation_id}
        shim.app = pair.authority_request.app
        complete_dispatch(shim, record.dispatch_id, DispatchCompletionBody.model_validate(completion))

    old = datetime.now(UTC) + timedelta(hours=2)  # evaluate "2 hours from now" so the record is stale
    sw = RunningLivenessSweeper(
        pair.authority, instance_id=AUTHORITY, probe=probe, adopt=adopt,
        fail_lost=AsyncMock(), stale_seconds=1800,
    )
    assert await sw.sweep_once(old) == [(DISPATCH, "adopted_completion")]
    delivered = pair.authority.get(DISPATCH)
    assert delivered.state == "completed" and delivered.acknowledged_at is not None


def test_status_endpoint_is_authority_only_and_reports_unknown(pair):
    pair.target_request.headers = {"X-PA-Origin-Instance-ID": "someone-else"}
    assert dispatch_target_status(pair.target_request, "never-heard-of-it") == {
        "dispatch_id": "never-heard-of-it", "known": False,
    }
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as denied:
        dispatch_target_status(pair.target_request, DISPATCH)
    assert denied.value.status_code == 403
    pair.target_request.headers = {"X-PA-Origin-Instance-ID": AUTHORITY}
    pair.target_request.app.state.ctx.store.get_session.return_value = SimpleNamespace(status="idle")
    status = dispatch_target_status(pair.target_request, DISPATCH)
    assert status["known"] is True and status["completion"] is None and status["state"] == "starting_session"


# --- re-offer ---------------------------------------------------------------------------

def reoffer_app(ledger, tmp_path):
    app = MagicMock()
    ctx = app.state.ctx
    ctx.settings = SimpleNamespace(instance_id="local", fleet_id="fleet")
    peer_a = SimpleNamespace(instance_id="dead", name="Dead")
    peer_b = SimpleNamespace(instance_id="alive", name="Alive")
    ctx.services = {"dispatch_store": ledger, "dispatch_worker": MagicMock()}
    ctx.require_service = ctx.services.__getitem__
    fleet = MagicMock()
    fleet.list_instances.return_value = [peer_a, peer_b]
    ctx.services["fleet_registry"] = fleet
    decision = MagicMock(chosen_instance_id="alive", chosen_instance_name="Alive", policy="best_match",
                         resolved_at=NOW)
    decision.model_dump.return_value = {
        "chosen_instance_id": "alive",
        "eligible_candidates": [{
            "instance_id": "alive", "capacity": 2, "capacity_detail": {"limit": 2, "source": "configured"},
            "active": 0, "queued": 0, "reserved": 0, "freshness": {},
        }],
    }
    placement = MagicMock()
    placement.resolve.return_value = decision
    ctx.services["placement_service"] = placement
    return app, placement


@pytest.mark.asyncio
async def test_lost_dispatch_is_requeued_on_another_instance_with_the_same_id(ledger, tmp_path):
    record = ledger.put(running(silent=timedelta(hours=1), target="dead", capacity_limit=1,
                                capacity_source="configured"))
    failed = ledger.fail(record, "lost", code="remote_dispatch_unknown", recoverable=True)
    app, placement = reoffer_app(ledger, tmp_path)
    with patch("pa.modules.fleet._placement_candidates", AsyncMock(return_value=[])), patch(
        "pa.modules.fleet._placement_policy_inputs", MagicMock(return_value=(None, False, [])),
    ), patch("pa.modules.fleet.notify_dispatch_reoffered") as notified:
        assert await _reoffer_lost_dispatch(app, failed) is True
    request = placement.resolve.call_args.args[0]
    assert request.instance_id is None and request.policy is not None
    again = ledger.get("r1")
    assert again.dispatch_id == "r1"
    assert again.target_instance_id == "alive"
    assert again.state in {"queued", "waiting_capacity"}
    assert again.session_id is None and again.materialization_plan is None
    notified.assert_called_once()


@pytest.mark.asyncio
async def test_governed_dispatches_keep_their_fixed_target(ledger, tmp_path):
    record = ledger.put(running(silent=timedelta(hours=1), target="dead"))
    failed = ledger.fail(record, "lost", code="remote_dispatch_unknown", recoverable=True)
    failed.materialization_plan = {"profile": "repository"}
    app, placement = reoffer_app(ledger, tmp_path)
    assert await _reoffer_lost_dispatch(app, failed) is False
    placement.resolve.assert_not_called()
    assert ledger.get("r1").target_instance_id == "dead"


@pytest.mark.asyncio
async def test_reoffer_is_bounded(ledger, tmp_path):
    record = ledger.put(running(silent=timedelta(hours=1), target="dead"))
    failed = ledger.fail(record, "lost", code="remote_dispatch_unknown", recoverable=True)
    failed.control_operations = {f"reoffer-lost:r1:t{i}": "retry" for i in range(3)}
    app, placement = reoffer_app(ledger, tmp_path)
    assert await _reoffer_lost_dispatch(app, failed) is False
