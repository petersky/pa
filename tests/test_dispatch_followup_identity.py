"""Dispatch follow-up receipts survive redaction, lost acknowledgements and retries."""
from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from pa.config import Settings
from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.followup import (PROMPT_IDENTITY_PROTOCOL, bind_followup_prompt, dispatch_prompt_identity)
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime
from pa.modules.agent_chat import (PromptBody, dispatch_prompt_capabilities, get_prompt_acceptance_status, session_prompt)
from pa.modules.fleet import DispatchFollowupBody, _process_remote_dispatch, prompt_dispatch_session
from pa.modules.items import operation_outcome_endpoint, operation_recovery_endpoint

MESSAGE = "Please review how Bearer credentials are handled."
KEY = "review-followup-v1"


def fingerprint(message=MESSAGE):
    return hashlib.sha256(json.dumps(
        {"message": message, "action": "append"}, sort_keys=True, separators=(",", ":"),
    ).encode()).hexdigest()


@pytest.fixture
def env(tmp_path):
    store = CardProjection(tmp_path / "pa.db")
    ledger = DispatchStore(tmp_path / "dispatch")
    record = ledger.put(DispatchRecord(
        dispatch_id="dispatch-identity", mutation_id="mutation-identity",
        session_id="session-identity", authority_instance_id="local",
        authority_url="http://local", target_instance_id="local", state="completed",
    ))
    settings = Settings(data_dir=tmp_path, instance_id="local", auth_required=False)
    session = store.save_session(AgentSession(
        id=record.session_id, agent_name="codex", dispatch_id=record.dispatch_id,
    ))
    manager = AgentSessionManager(settings, store, dispatch_store=ledger)
    manager._quiescing = True  # Exercise real queue admission without a provider.
    runtime = AgentSessionRuntime(manager, session)
    manager._runtimes[session.id] = runtime
    request = MagicMock()
    request.state.instance_authenticated = True
    request.state.user = None
    request.state.principal_id = "user:local"
    request.app.state.ctx.settings = settings
    request.app.state.ctx.services = {"dispatch_store": ledger, "instance_agent": manager}
    request.app.state.ctx.require_service = request.app.state.ctx.services.__getitem__
    with patch("pa.modules.agent_chat._runtime_or_404", return_value=runtime), patch(
        "pa.modules.agent_chat._record_web_intake", AsyncMock(return_value=None),
    ):
        yield SimpleNamespace(store=store, ledger=ledger, record=record,
                              request=request, runtime=runtime, manager=manager)


async def target(env, message=MESSAGE):
    return await session_prompt(env.request, env.record.session_id, PromptBody(
        message=message, dispatch_id=env.record.dispatch_id, idempotency_key=KEY,
        dispatch_prompt_id=dispatch_prompt_identity(env.record, KEY),
        dispatch_prompt_protocol=PROMPT_IDENTITY_PROTOCOL,
    ))


async def authority(env):
    return await prompt_dispatch_session(env.request, env.record.dispatch_id,
                                        DispatchFollowupBody(message=MESSAGE, idempotency_key=KEY))


def operation(env):
    return env.ledger.get(env.record.dispatch_id).followup_operations[KEY]


@pytest.mark.asyncio
async def test_redacted_acceptance_after_more_than_twenty_events_and_concurrent_retry(env):
    original = env.runtime.admit_dispatch_prompt

    async def interleaved(*args, **kwargs):
        bound = operation(env)
        assert bound["prompt_id"] == kwargs["prompt_id"]
        assert bound["admission_protocol"] == PROMPT_IDENTITY_PROTOCOL
        for i in range(30):
            env.runtime._append_transcript("agent_message", {"message": f"interleaved {i}"})
        return await original(*args, **kwargs)

    with patch.object(env.runtime, "admit_dispatch_prompt", AsyncMock(side_effect=interleaved)) as prompt:
        responses = await asyncio.gather(*(target(env) for _ in range(8)))
    assert prompt.await_count == 1
    assert len(env.runtime._queue) == 1
    assert all(r["accepted"] for r in responses)
    assert len({r["prompt_id"] for r in responses}) == 1
    accepted = env.store.get_prompt_acceptance(env.record.session_id, responses[0]["prompt_id"])
    assert accepted.seq > 20
    assert "[REDACTED_AUTH]" in accepted.payload["message"]
    assert "Bearer credentials" not in accepted.payload["message"]
    assert env.ledger.get(env.record.dispatch_id).state == "completed"
    assert len(env.ledger.get(env.record.dispatch_id).followup_turns) == 1


@pytest.mark.asyncio
async def test_enqueue_before_lost_ack_returns_exact_acceptance(env):
    original = env.runtime.admit_dispatch_prompt

    async def lost_ack(*args, **kwargs):
        await original(*args, **kwargs)
        raise TimeoutError("acknowledgement lost after enqueue")

    with patch.object(env.runtime, "admit_dispatch_prompt", AsyncMock(side_effect=lost_ack)) as prompt:
        first = await target(env)
        retry = await target(env)
    assert first["accepted"] and retry["duplicate"]
    assert first["prompt_id"] == retry["prompt_id"]
    assert prompt.await_count == len(env.runtime._queue) == 1


@pytest.mark.asyncio
async def test_late_persistence_and_restart_retry_never_readmit(env):
    with patch.object(env.store, "append_transcript_events", side_effect=OSError("disk busy")):
        with pytest.raises(HTTPException) as failed:
            await target(env)
        assert failed.value.detail["code"] == "prompt_not_persisted"
        prompt_id = failed.value.detail["prompt_id"]
        with pytest.raises(HTTPException):
            await target(env)
        assert len(env.runtime._queue) == 1
    env.runtime._flush_transcript()
    # Discard volatile queue evidence and reload the ledger from disk.
    env.runtime._queue.clear()
    env.ledger = DispatchStore(env.ledger.path.parent)
    env.request.app.state.ctx.services["dispatch_store"] = env.ledger
    with patch.object(env.runtime, "admit_dispatch_prompt", AsyncMock()) as prompt:
        receipt = await target(env)
    assert receipt["accepted"] and receipt["prompt_id"] == prompt_id
    prompt.assert_not_awaited()


@pytest.mark.asyncio
async def test_genuine_not_accepted_failure_can_retry_same_identity(env):
    original = env.runtime.admit_dispatch_prompt
    with patch.object(env.runtime, "admit_dispatch_prompt", AsyncMock(side_effect=RuntimeError("admission unavailable"))) as prompt:
        with pytest.raises(HTTPException) as failed:
            await target(env)
        assert failed.value.status_code == 503
        prompt_id = operation(env)["prompt_id"]
        assert not env.runtime._queue
        assert env.store.get_prompt_acceptance(env.record.session_id, prompt_id) is None
        prompt.assert_awaited_once()
    with patch.object(env.runtime, "admit_dispatch_prompt", AsyncMock(wraps=original)) as prompt:
        retry = await target(env)
    assert retry["accepted"] and retry["prompt_id"] == prompt_id
    assert prompt.await_count == len(env.runtime._queue) == 1


@pytest.mark.asyncio
async def test_legacy_ambiguous_without_identity_is_never_replayed(env):
    record = env.ledger.get(env.record.dispatch_id)
    record.followup_operations[KEY] = {"fingerprint": fingerprint(), "state": "delivery_ambiguous"}
    env.ledger.put(record)
    with patch.object(env.runtime, "admit_dispatch_prompt", AsyncMock()) as prompt, patch(
        "pa.modules.fleet._peer_agent_json", AsyncMock(),
    ) as peer:
        for submit in (target, authority):
            with pytest.raises(HTTPException) as failed:
                await submit(env)
            assert failed.value.detail["code"] == "legacy_followup_identity_unknown"
            assert failed.value.detail["recoverable"] is False
        prompt.assert_not_awaited()
        peer.assert_not_awaited()
    assert operation(env) == record.followup_operations[KEY]


@pytest.mark.asyncio
async def test_conflicting_same_key_cannot_replace_identity(env):
    first = await target(env)
    with pytest.raises(HTTPException) as failed:
        await target(env, "different request")
    assert failed.value.detail["code"] == "idempotency_conflict"
    assert operation(env)["prompt_id"] == first["prompt_id"]
    assert len(env.runtime._queue) == 1


@pytest.mark.asyncio
async def test_authority_transport_loss_and_completed_exact_prompt_outcome_reconcile(env, tmp_path):
    # Separate authority and target ledgers, as on a real remote dispatch.
    authority_ledger = DispatchStore(tmp_path / "authority")
    authority_ledger.put(env.record)
    authority_request = MagicMock()
    authority_request.state.instance_authenticated = True
    authority_request.app.state.ctx.settings = env.request.app.state.ctx.settings
    from pa.core.operation_status import OperationStatusService
    status_service = OperationStatusService(tmp_path)
    authority_request.app.state.ctx.services = {"dispatch_store": authority_ledger,
                                               "operation_status": status_service}
    authority_env = SimpleNamespace(**{**vars(env), "request": authority_request})
    hide_receipt = True
    posts = []

    async def peer(_request, _instance, method, path, **kwargs):
        nonlocal hide_receipt
        if path == "prompt-capabilities":
            return dispatch_prompt_capabilities()
        if method == "POST":
            posts.append(path)
            accepted = await target(env)
            assert accepted["prompt_id"] == authority_ledger.get(env.record.dispatch_id).followup_operations[KEY]["prompt_id"]
            raise TimeoutError("HTTP response lost")
        if hide_receipt:
            raise TimeoutError("receipt transport temporarily unavailable")
        return await get_prompt_acceptance_status(env.request, env.record.session_id, path.rsplit("/", 1)[1])

    with patch("pa.modules.fleet._peer_agent_json", AsyncMock(side_effect=peer)):
        with pytest.raises(TimeoutError):
            await authority(authority_env)
        ambiguous = authority_ledger.get(env.record.dispatch_id).followup_operations[KEY]
        assert ambiguous["state"] == "delivery_ambiguous"
        prompt_id = ambiguous["prompt_id"]
        env.runtime._append_transcript("turn_completed", {"queued_prompt_id": prompt_id, "stop_reason": "end_turn"})
        env.runtime._flush_transcript()
        hide_receipt = False
        with patch("pa.modules.items.get_store", return_value=env.store):
            outcome = await operation_outcome_endpoint(authority_request, KEY)
            assert outcome["status"] == "delivery_ambiguous"
            assert not status_service.tasks
            await operation_recovery_endpoint(authority_request, KEY)
            await asyncio.gather(*status_service.tasks.values())
            outcome = await operation_outcome_endpoint(authority_request, KEY)
        await status_service.close()
        assert outcome["status"] == "accepted"
        assert outcome["result"]["response"]["accepted"] is True
        assert outcome["result"]["prompt_id"] == prompt_id
        assert outcome["result"]["error"] is None
        retry = await authority(authority_env)
        assert retry["duplicate"] and retry["prompt_id"] == prompt_id
    assert len(posts) == len(env.runtime._queue) == 1
    assert (await get_prompt_acceptance_status(env.request, env.record.session_id, prompt_id))["status"] == "completed"


@pytest.mark.asyncio
async def test_concurrent_fingerprints_claim_one_identity(env):
    results = await asyncio.gather(*(
        asyncio.to_thread(bind_followup_prompt, env.ledger, env.record, KEY, fp,
                          )
        for fp in (fingerprint(), fingerprint("conflict"))
    ), return_exceptions=True)
    assert sum(isinstance(result, HTTPException) for result in results) == 1
    assert sum(isinstance(result, DispatchRecord) for result in results) == 1


@pytest.mark.asyncio
async def test_cold_exact_acceptance_and_completion_have_no_history_cutoff(env):
    from pa.domain.models import TranscriptEvent

    # More than a legacy lookup window before and after the exact admission.
    def unrelated(start):
        env.store.append_transcript_events([
            TranscriptEvent(session_id=env.record.session_id, seq=start + i,
                            event_type="queue_enqueued", payload={"id": f"other-{start + i}"})
            for i in range(1100)
        ])

    unrelated(1)
    env.runtime._seq = 1100
    message = MESSAGE + " Harmless review detail." * 200
    accepted = await target(env, message)
    prompt_id = accepted["prompt_id"]
    env.runtime._append_transcript("user_message", {"id": prompt_id, "message": message})
    env.runtime._append_transcript("turn_completed", {"queued_prompt_id": prompt_id})
    env.runtime._flush_transcript()
    unrelated(env.runtime._seq + 1)
    env.runtime._seq += 1100
    event = env.store.get_prompt_acceptance(env.record.session_id, prompt_id)
    assert event.seq == 1101 and event.event_type == "queue_enqueued"
    assert "[REDACTED_AUTH]" in event.payload["message"]
    assert env.store.get_prompt_lifecycle(env.record.session_id, prompt_id).event_type == "turn_completed"
    assert env.store.transcripts.find_prompt_completion(env.record.session_id, prompt_id).event_type == "turn_completed"
    assert env.store.get_prompt_acceptance(env.record.session_id, "never-accepted") is None
    record = env.ledger.get(env.record.dispatch_id)
    record.followup_operations[KEY].pop("response")
    record.followup_operations[KEY]["state"] = "delivery_ambiguous"
    env.ledger.put(record)
    with patch.object(env.runtime, "admit_dispatch_prompt", AsyncMock()) as prompt:
        retry = await target(env, message)
    prompt.assert_not_awaited()
    assert retry["queued"] == accepted["queued"]
    assert retry["accepted_event"] == "queue_enqueued"


@pytest.mark.asyncio
async def test_wrong_identity_receipt_never_acknowledges_bound_operation(env):
    async def peer(_request, _instance, _method, path, **kwargs):
        if path == "prompt-capabilities":
            return dispatch_prompt_capabilities()
        return {
            "accepted": True, "prompt_id": "another-prompt", "session_id": env.record.session_id,
            "accepted_event": "queue_enqueued", "accepted_action": "append",
        }
    with patch("pa.modules.fleet._peer_agent_json", AsyncMock(side_effect=peer)):
        with pytest.raises(HTTPException) as failed:
            await authority(env)
        assert failed.value.detail["code"] == "followup_not_acknowledged"
        with patch("pa.modules.items.get_store", return_value=env.store):
            outcome = await operation_outcome_endpoint(env.request, KEY)
    assert outcome["status"] == "delivery_ambiguous"
    assert not outcome["result"]["response"]
    assert outcome["result"]["prompt_id"] != "another-prompt"


@pytest.mark.asyncio
async def test_concurrent_authority_delivery_and_retry_admit_one_prompt(env):
    async def peer(_request, _instance, method, path, **kwargs):
        if path == "prompt-capabilities":
            return dispatch_prompt_capabilities()
        if method == "GET":
            return await get_prompt_acceptance_status(env.request, env.record.session_id, path.rsplit("/", 1)[1])
        return await target(env)

    with patch("pa.modules.fleet._peer_agent_json", AsyncMock(side_effect=peer)), patch.object(
        env.runtime, "admit_dispatch_prompt", wraps=env.runtime.admit_dispatch_prompt,
    ) as prompt:
        receipts = await asyncio.gather(*(authority(env) for _ in range(6)))
        retry = await authority(env)
    assert prompt.await_count == len(env.runtime._queue) == 1
    assert {r["prompt_id"] for r in receipts} == {retry["prompt_id"]}
    assert operation(env)["admission_protocol"] == PROMPT_IDENTITY_PROTOCOL
    assert operation(env)["state"] == "accepted"


@pytest.mark.asyncio
async def test_disconnected_waiter_and_same_key_retry_share_owned_admission(env):
    from pa.core.async_runtime import AsyncRuntime

    executor = AsyncRuntime()
    env.request.app.state.ctx.services["async_runtime"] = executor
    enqueued, release = asyncio.Event(), asyncio.Event()
    original = env.runtime.admit_dispatch_prompt

    async def slow_ack(*args, **kwargs):
        result = await original(*args, **kwargs)
        enqueued.set()
        await release.wait()
        return result

    try:
        with patch.object(env.runtime, "admit_dispatch_prompt", AsyncMock(side_effect=slow_ack)) as prompt:
            waiter = asyncio.create_task(target(env))
            await asyncio.wait_for(enqueued.wait(), 5)
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            retry = asyncio.create_task(target(env))
            release.set()
            receipt = await asyncio.wait_for(retry, 5)
            assert receipt["accepted"]
            assert receipt["prompt_id"] == operation(env)["prompt_id"]
            assert prompt.await_count == len(env.runtime._queue) == 1
    finally:
        release.set()
        await executor.close()


@pytest.mark.asyncio
async def test_crash_after_identity_binding_before_enqueue_is_recoverable(env):
    with patch.object(env.runtime, "admit_dispatch_prompt", AsyncMock(side_effect=SystemExit("crash after bind"))):
        with pytest.raises(SystemExit):
            await target(env)
    prompt_id = operation(env)["prompt_id"]
    assert not env.runtime._queue
    env.ledger = DispatchStore(env.ledger.path.parent)
    env.request.app.state.ctx.services["dispatch_store"] = env.ledger
    receipt = await target(env)
    assert receipt["accepted"] and receipt["prompt_id"] == prompt_id
    assert len(env.runtime._queue) == 1


@pytest.mark.asyncio
async def test_crash_after_persisted_queue_before_acceptance_restores_fence(env):
    with patch.object(env.runtime, "_finish_prompt_admission", AsyncMock(side_effect=SystemExit("crash before acceptance"))):
        with pytest.raises(SystemExit):
            await target(env)
    prompt_id = operation(env)["prompt_id"]
    assert env.store.get_prompt_acceptance(env.record.session_id, prompt_id) is None
    stored = env.store.get_session(env.record.session_id)
    snapshot = env.manager._snapshot_from_persisted(stored)
    assert len(snapshot.queued_prompts) == 1
    assert snapshot.queued_prompts[0].admission_pending
    runtime = AgentSessionRuntime(env.manager, stored)
    runtime._queue = snapshot.queued_prompts
    env.runtime = runtime
    env.manager._runtimes[stored.id] = runtime
    with patch("pa.modules.agent_chat._runtime_or_404", return_value=runtime):
        receipt = await target(env)
    assert receipt["prompt_id"] == prompt_id and receipt["accepted"]
    assert len(runtime._queue) == 1 and not runtime._queue[0].admission_pending


@pytest.mark.asyncio
async def test_provider_drain_cannot_execute_before_pending_admission_finishes(env):
    env.runtime.connection = SimpleNamespace(connected=True)
    env.manager._quiescing = False
    queued = env.runtime.enqueue(
        MESSAGE, source="dispatch:dispatch-identity", prompt_id="fenced-prompt",
        _durable_admission=True, _defer_drain=True,
    )
    with patch.object(env.runtime, "_run_prompt", AsyncMock()) as provider:
        with patch.object(env.runtime, "_finish_prompt_admission", AsyncMock(side_effect=OSError("storage unavailable"))):
            await env.runtime._drain_queue()
        provider.assert_not_awaited()
        assert queued.admission_pending and env.runtime._queue == [queued]
        assert env.store.get_prompt_acceptance(env.record.session_id, queued.id) is None
        await env.runtime._drain_queue()
        provider.assert_awaited_once_with(queued)
    assert env.store.get_prompt_acceptance(env.record.session_id, queued.id) is not None


@pytest.mark.asyncio
async def test_old_target_is_rejected_before_delivery_then_same_key_can_retry_after_upgrade(env):
    posts = []
    upgraded = False

    async def peer(_request, _instance, method, path, **kwargs):
        if path == "prompt-capabilities":
            if not upgraded:
                raise HTTPException(404, detail="Not Found")
            return dispatch_prompt_capabilities()
        if method == "POST":
            posts.append(path)
            assert path.endswith("/dispatch-prompts")
            return await session_prompt(env.request, env.record.session_id, PromptBody(**kwargs["body"]))
        raise AssertionError(path)

    with patch("pa.modules.fleet._peer_agent_json", AsyncMock(side_effect=peer)):
        with pytest.raises(HTTPException) as blocked:
            await authority(env)
        assert blocked.value.detail["code"] == "dispatch_prompt_protocol_unavailable"
        assert not posts and not env.runtime._queue
        assert KEY not in env.ledger.get(env.record.dispatch_id).followup_operations
        upgraded = True
        accepted = await authority(env)
        retry = await authority(env)
    assert accepted["accepted"] and accepted["prompt_id"] == retry["prompt_id"]
    assert len(posts) == len(env.runtime._queue) == 1


@pytest.mark.asyncio
async def test_operation_outcome_store_and_ledger_work_is_off_event_loop(env):
    import threading

    main_thread = threading.get_ident()
    original = env.store.read_operation_receipt
    lookup_threads = []

    def lookup(*args, **kwargs):
        lookup_threads.append(threading.get_ident())
        assert threading.get_ident() != main_thread
        return original(*args, **kwargs)

    with patch("pa.modules.items.get_store", return_value=env.store), patch.object(
        env.store, "read_operation_receipt", side_effect=lookup,
    ):
        result = await operation_outcome_endpoint(env.request, "absent-operation")
    assert result["status"] == "lookup_pending" and lookup_threads
    assert result["owner"] is None and result["accepted"] is None


@pytest.mark.asyncio
async def test_initial_dispatch_uses_shared_durable_identity_and_redacted_acceptance(env):
    body = PromptBody(
        message=MESSAGE, dispatch_id=env.record.dispatch_id,
        dispatch_prompt_id=dispatch_prompt_identity(env.record, None),
        dispatch_prompt_protocol=PROMPT_IDENTITY_PROTOCOL,
    )
    first, retry = await asyncio.gather(*(
        session_prompt(env.request, env.record.session_id, body) for _ in range(2)
    ))
    assert first["accepted"] and first["prompt_id"] == retry["prompt_id"]
    assert len(env.runtime._queue) == 1
    record = env.ledger.get(env.record.dispatch_id)
    assert record.initial_prompt_operation["prompt_id"] == first["prompt_id"]
    assert record.prompt_ack["prompt_id"] == first["prompt_id"]
    event = env.store.get_prompt_acceptance(env.record.session_id, first["prompt_id"])
    assert "[REDACTED_AUTH]" in event.payload["message"]


@pytest.mark.asyncio
async def test_lost_ack_after_user_message_keeps_same_queued_receipt(env):
    async def peer(_request, _instance, method, path, **kwargs):
        if path == "prompt-capabilities":
            return dispatch_prompt_capabilities()
        if method == "POST":
            accepted = await target(env)
            env.runtime._append_transcript("user_message", {"id": accepted["prompt_id"], "message": MESSAGE})
            env.runtime._flush_transcript()
            raise TimeoutError("ack lost after execution started")
        return await get_prompt_acceptance_status(env.request, env.record.session_id, path.rsplit("/", 1)[1])

    with patch("pa.modules.fleet._peer_agent_json", AsyncMock(side_effect=peer)):
        response = await authority(env)
    assert response["accepted_event"] == "queue_enqueued" and response["queued"]
    assert operation(env)["response"]["queued"]
    assert len(env.runtime._queue) == 1


@pytest.mark.asyncio
async def test_versioned_delivery_cannot_fall_back_to_old_random_id_admission(env):
    upgraded = False
    legacy_admissions = []

    async def peer(_request, _instance, method, path, **kwargs):
        if path == "prompt-capabilities":
            return dispatch_prompt_capabilities()  # Target downgrades after probe.
        if method == "POST":
            if path.endswith("/prompt"):
                legacy_admissions.append("random-legacy-id")
                return {"accepted": True, "prompt_id": "random-legacy-id"}
            if not upgraded:
                raise HTTPException(404, detail="Not Found")
            return await target(env)
        return {"accepted": False}

    with patch("pa.modules.fleet._peer_agent_json", AsyncMock(side_effect=peer)):
        with pytest.raises(HTTPException):
            await authority(env)
        assert operation(env)["state"] == "delivery_not_admitted"
        assert operation(env)["error"]["recoverable"]
        assert not legacy_admissions and not env.runtime._queue
        upgraded = True
        receipt = await authority(env)
    assert receipt["accepted"] and len(env.runtime._queue) == 1


@pytest.mark.asyncio
async def test_legacy_outcome_does_not_offer_an_unsupported_recovery(env):
    record = env.ledger.get(env.record.dispatch_id)
    record.followup_operations[KEY] = {
        "state": "delivery_ambiguous", "fingerprint": fingerprint(),
        "error": {"recoverable": True, "code": "old_prompt_not_persisted"},
    }
    env.ledger.put(record)
    with patch("pa.modules.items.get_store", return_value=env.store), patch(
        "pa.modules.fleet._peer_agent_json", AsyncMock(),
    ) as peer:
        outcome = await operation_outcome_endpoint(env.request, KEY)
    assert outcome["status"] == "legacy_delivery_ambiguous"
    assert outcome["recovery_state"] == "legacy_identity_unknown"
    assert not outcome["automatic_retry_safe"]
    peer.assert_not_awaited()
    assert operation(env) == record.followup_operations[KEY]  # Historical evidence untouched.


@pytest.mark.asyncio
async def test_legacy_initial_ambiguous_dispatch_cannot_allocate_new_prompt(env):
    record = env.ledger.get(env.record.dispatch_id)
    record.state = "failed"
    record.error_code = "prompt_not_persisted"
    env.ledger.put(record)
    body = PromptBody(
        message=MESSAGE, dispatch_id=env.record.dispatch_id,
        dispatch_prompt_id=dispatch_prompt_identity(env.record, None),
        dispatch_prompt_protocol=PROMPT_IDENTITY_PROTOCOL,
    )
    with pytest.raises(HTTPException) as blocked:
        await session_prompt(env.request, env.record.session_id, body)
    assert blocked.value.detail["code"] == "legacy_initial_prompt_identity_unknown"
    assert not blocked.value.detail["recoverable"]
    assert not env.runtime._queue
    assert not env.ledger.get(env.record.dispatch_id).initial_prompt_operation


@pytest.mark.asyncio
async def test_same_key_replay_resumes_accepted_queue_without_readmission(env):
    first = await target(env)
    with patch.object(env.runtime, "_start_drain") as schedule, patch.object(
        env.runtime, "admit_dispatch_prompt", AsyncMock(),
    ) as admit:
        replay = await target(env)
    schedule.assert_called_once()
    admit.assert_not_awaited()
    assert replay["prompt_id"] == first["prompt_id"] and len(env.runtime._queue) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("restore_process", [False, True])
@pytest.mark.parametrize("checkpoint_written", [False, True])
async def test_post_acceptance_checkpoint_failure_recovers_same_prompt_once(
    env, restore_process, checkpoint_written,
):
    checkpoint = env.runtime._checkpoint_runtime_async

    async def fail_handoff(*, lifecycle=None):
        assert lifecycle == "queued"
        item = env.runtime._queue[0]
        assert env.store.get_prompt_acceptance(env.record.session_id, item.id)
        assert item.admission_pending
        if checkpoint_written:
            await checkpoint(lifecycle=lifecycle)
        await asyncio.sleep(0)
        assert item.admission_pending  # No provider can bypass the awaited fence.
        raise OSError("queued checkpoint acknowledgement lost")

    with patch.object(env.runtime, "_checkpoint_runtime_async", side_effect=fail_handoff):
        receipt = await target(env)
        if env.runtime._drain_task:
            await env.runtime._drain_task
    assert receipt["accepted"]
    prompt_id = receipt["prompt_id"]
    assert len(env.runtime._queue) == 1
    assert env.runtime._queue[0].admission_pending
    accepted = env.store.get_prompt_acceptance(env.record.session_id, prompt_id)

    if restore_process:
        stored = env.store.get_session(env.record.session_id)
        snapshot = env.manager._snapshot_from_persisted(stored)
        runtime = AgentSessionRuntime(env.manager, stored)
        runtime._queue = snapshot.queued_prompts
        env.runtime = runtime
        env.manager._runtimes[stored.id] = runtime
        assert len(runtime._queue) == 1 and runtime._queue[0].admission_pending

    env.runtime.connection = SimpleNamespace(connected=True)
    env.manager._quiescing = False
    with patch("pa.modules.agent_chat._runtime_or_404", return_value=env.runtime), patch.object(
        env.runtime, "_run_prompt", AsyncMock(),
    ) as provider, patch.object(env.runtime, "admit_dispatch_prompt", AsyncMock()) as admit:
        replay = await target(env)  # Cached receipt must resume the pending handoff.
        await env.runtime._drain_task
        again = await target(env)
        provider.assert_awaited_once()
        assert provider.await_args.args[0].id == prompt_id
        assert not provider.await_args.args[0].admission_pending
        admit.assert_not_awaited()
    assert replay["prompt_id"] == again["prompt_id"] == prompt_id
    assert not env.runtime._queue
    assert env.store.get_prompt_acceptance(env.record.session_id, prompt_id).id == accepted.id


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_ack", [False, True])
@pytest.mark.parametrize("completed_before_ack", [False, True])
async def test_initial_authority_to_local_target_preserves_receipt_on_retry(
    env, lost_ack, completed_before_ack,
):
    record = env.ledger.get(env.record.dispatch_id)
    record.state = "queued"
    record.request_payload = {"message": MESSAGE}
    env.ledger.put(record)
    env.request.app.state.ctx.store = env.store
    env.request.app.state.ctx.services["fleet_registry"] = MagicMock()
    bodies = []
    responses = []
    target_acks = []

    async def peer(_request, _instance, method, path, **kwargs):
        if path == "sessions":
            return {"session": {"id": record.session_id}}
        if path == "prompt-capabilities":
            return dispatch_prompt_capabilities()
        if method == "POST":
            assert path == f"sessions/{record.session_id}/dispatch-prompts"
            body = PromptBody(**kwargs["body"])
            bodies.append(body)
            response = await session_prompt(env.request, record.session_id, body)
            responses.append(response)
            target_acks.append(env.ledger.get(record.dispatch_id).prompt_ack)
            if completed_before_ack and len(responses) == 1:
                def complete(current):
                    current.state = "completed"
                    current.acknowledged_at = datetime.now(UTC)
                env.ledger.mutate_current(record.dispatch_id, mutate=complete)
            if lost_ack and len(responses) == 1:
                raise TimeoutError("local target accepted before transport loss")
            return response
        return await get_prompt_acceptance_status(env.request, record.session_id, path.rsplit("/", 1)[1])

    with patch("pa.modules.fleet._peer_agent_json", AsyncMock(side_effect=peer)), patch(
        "pa.modules.fleet._peer_dispatch_json", AsyncMock(return_value={"resolvable": True}),
    ):
        await _process_remote_dispatch(env.request.app, record)
        committed = env.ledger.get(record.dispatch_id)
        assert committed.initial_prompt_operation["response"]["accepted_event"] == "queue_enqueued"
        assert committed.prompt_ack["event_id"] == target_acks[0]["event_id"]
        if completed_before_ack:
            assert committed.state == "completed" and committed.acknowledged_at
        with patch.object(env.runtime, "admit_dispatch_prompt", AsyncMock()) as admit:
            replay = await session_prompt(env.request, record.session_id, bodies[0])
            # A complete authority retry must also return the original receipt.
            await _process_remote_dispatch(env.request.app, committed)
            admit.assert_not_awaited()
    assert len(env.runtime._queue) == 1
    assert replay["duplicate"] and replay["accepted_event"] == "queue_enqueued"
    assert {r["prompt_id"] for r in [replay, *responses]} == {bodies[0].dispatch_prompt_id}
    assert env.ledger.get(record.dispatch_id).initial_prompt_operation["response"]["queued"]
