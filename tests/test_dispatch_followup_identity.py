"""Dispatch follow-up receipts survive redaction, lost acknowledgements and retries."""
from __future__ import annotations

import asyncio
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from pa.config import Settings
from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.followup import bind_followup_prompt
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime
from pa.modules.agent_chat import PromptBody, get_prompt_acceptance_status, session_prompt
from pa.modules.fleet import DispatchFollowupBody, prompt_dispatch_session
from pa.modules.items import operation_outcome_endpoint

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
    ))


async def authority(env):
    return await prompt_dispatch_session(env.request, env.record.dispatch_id,
                                        DispatchFollowupBody(message=MESSAGE, idempotency_key=KEY))


def operation(env):
    return env.ledger.get(env.record.dispatch_id).followup_operations[KEY]


@pytest.mark.asyncio
async def test_redacted_acceptance_after_more_than_twenty_events_and_concurrent_retry(env):
    original = env.runtime.prompt

    async def interleaved(*args, **kwargs):
        bound = operation(env)
        assert bound["prompt_id"] == kwargs["prompt_id"]
        assert bound["target_admission_started"] is True
        for i in range(30):
            env.runtime._append_transcript("agent_message", {"message": f"interleaved {i}"})
        return await original(*args, **kwargs)

    with patch.object(env.runtime, "prompt", AsyncMock(side_effect=interleaved)) as prompt:
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
    original = env.runtime.prompt

    async def lost_ack(*args, **kwargs):
        await original(*args, **kwargs)
        raise TimeoutError("acknowledgement lost after enqueue")

    with patch.object(env.runtime, "prompt", AsyncMock(side_effect=lost_ack)) as prompt:
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
    with patch.object(env.runtime, "prompt", AsyncMock()) as prompt:
        receipt = await target(env)
    assert receipt["accepted"] and receipt["prompt_id"] == prompt_id
    prompt.assert_not_awaited()


@pytest.mark.asyncio
async def test_genuine_not_accepted_and_retry_are_honest_and_nondoubling(env):
    with patch.object(env.runtime, "prompt", AsyncMock(side_effect=RuntimeError("admission unavailable"))) as prompt:
        for _ in range(2):
            with pytest.raises(HTTPException) as failed:
                await target(env)
            assert failed.value.status_code == 503
        assert prompt.await_count == 1
    assert not env.runtime._queue
    assert env.store.get_prompt_acceptance(env.record.session_id, operation(env)["prompt_id"]) is None
    assert not operation(env).get("response")


@pytest.mark.asyncio
async def test_legacy_ambiguous_without_identity_is_never_replayed(env):
    record = env.ledger.get(env.record.dispatch_id)
    record.followup_operations[KEY] = {"fingerprint": fingerprint(), "state": "delivery_ambiguous"}
    env.ledger.put(record)
    with patch.object(env.runtime, "prompt", AsyncMock()) as prompt, patch(
        "pa.modules.fleet._peer_agent_json", AsyncMock(),
    ) as peer:
        for submit in (target, authority):
            with pytest.raises(HTTPException) as failed:
                await submit(env)
            assert failed.value.detail["code"] == "legacy_followup_identity_unknown"
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
    authority_request.app.state.ctx.services = {"dispatch_store": authority_ledger}
    authority_env = SimpleNamespace(**{**vars(env), "request": authority_request})
    hide_receipt = True
    posts = []

    async def peer(_request, _instance, method, path, **kwargs):
        nonlocal hide_receipt
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
                          claim_admission=True)
        for fp in (fingerprint(), fingerprint("conflict"))
    ), return_exceptions=True)
    assert sum(isinstance(result, HTTPException) for result in results) == 1
    assert sum(isinstance(result, tuple) and result[1] for result in results) == 1


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
    with patch.object(env.runtime, "prompt", AsyncMock()) as prompt:
        retry = await target(env, message)
    prompt.assert_not_awaited()
    assert retry["queued"] == accepted["queued"]
    assert retry["accepted_event"] == "queue_enqueued"


@pytest.mark.asyncio
async def test_wrong_identity_receipt_never_acknowledges_bound_operation(env):
    with patch("pa.modules.fleet._peer_agent_json", AsyncMock(return_value={
        "accepted": True, "prompt_id": "another-prompt", "session_id": env.record.session_id,
        "accepted_event": "queue_enqueued", "accepted_action": "append",
    })):
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
        if method == "GET":
            return await get_prompt_acceptance_status(env.request, env.record.session_id, path.rsplit("/", 1)[1])
        return await target(env)

    with patch("pa.modules.fleet._peer_agent_json", AsyncMock(side_effect=peer)), patch.object(
        env.runtime, "prompt", wraps=env.runtime.prompt,
    ) as prompt:
        receipts = await asyncio.gather(*(authority(env) for _ in range(6)))
        retry = await authority(env)
    assert prompt.await_count == len(env.runtime._queue) == 1
    assert {r["prompt_id"] for r in receipts} == {retry["prompt_id"]}
    assert operation(env)["target_admission_started"] is True
    assert operation(env)["state"] == "accepted"


@pytest.mark.asyncio
async def test_disconnected_waiter_and_same_key_retry_share_owned_admission(env):
    from pa.core.async_runtime import AsyncRuntime

    executor = AsyncRuntime()
    env.request.app.state.ctx.services["async_runtime"] = executor
    enqueued, release = asyncio.Event(), asyncio.Event()
    original = env.runtime.prompt

    async def slow_ack(*args, **kwargs):
        result = await original(*args, **kwargs)
        enqueued.set()
        await release.wait()
        return result

    try:
        with patch.object(env.runtime, "prompt", AsyncMock(side_effect=slow_ack)) as prompt:
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
