"""A recorded response whose session was superseded by a linked run must be recoverable.

Regression for a notification stuck on "Delivery failed": the source session was
fenced by an explicit linked attempt, delivery to it can never succeed, yet it was
classed retryable (so "Retry delivery" looped forever), the explicit repair refused
the source as "not demonstrably unrecoverable", and nothing on the card said why.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from pa.domain.notifications import ContinuationTransferRequest, InteractionState
from pa.execution.selection import SelectionError
from pa.notifications import NotificationConflict
from pa.notifications.service import SUPERSEDED_SOURCE_ACTION
from tests.test_notification_continuation_transfer import recovery, respond, transfer  # noqa: F401

FENCED = "This source has a durable linked attempt; resume that target, not the superseded native context"


def fence(r, *, state="linked", target="successor", source_dispatch="dispatch-old",
          origin=True, origin_target="successor"):
    """Model what an explicit linked attempt records on both sessions."""
    boundary = {
        "contract": "pa.execution-context-boundary/v1", "id": "boundary-1",
        "source_session_id": "old", "source_dispatch_id": source_dispatch,
        "target_session_id": target, "state": state,
    }
    r.old.config_json = {**(r.old.config_json or {}), "execution_context_boundary": boundary}
    r.old.recovery_json = {}   # native context still exists: only the fence supersedes it
    r.store.save_session(r.old)
    if origin:
        r.successor.config_json = {**(r.successor.config_json or {}), "execution_context_boundary_from":
                                   {**boundary, "target_session_id": origin_target, "state": "reserved"}}
        r.store.save_session(r.successor)


def request_for(r, key):
    current = r.store.get_notification(r.notice.id)
    return ContinuationTransferRequest(**{**r.request.model_dump(), "idempotency_key": key,
                                           "expected_version": current.version})


def fail_delivery(r, error):
    r.manager.recover_session = AsyncMock(side_effect=error)


def stored(r):
    return r.store.get_notification(r.notice.id)


def test_fenced_source_can_be_repaired_to_its_exact_linked_run(recovery):
    r = recovery
    fence(r)
    moved = asyncio.run(transfer(r))
    assert moved.continuation_transfer.expected_session_id == "old"
    assert moved.continuation_transfer.successor_session_id == "successor"
    assert not r.runtime._queue   # routing is repaired; nothing is delivered by it


@pytest.mark.parametrize("case", ["unfenced", "wrong-target", "reserved-only", "one-sided",
                                  "other-dispatch", "origin-mismatch"])
def test_only_an_exact_mutual_fence_supersedes_a_recoverable_source(recovery, case):
    r = recovery
    if case == "unfenced":
        r.old.recovery_json = {}                       # recoverable, nothing supersedes it
        r.store.save_session(r.old)
    elif case == "wrong-target":
        fence(r, target="some-other-session")
    elif case == "reserved-only":
        fence(r, state="reserved")                    # the attempt never became a durable link
    elif case == "one-sided":
        fence(r, origin=False)                        # successor does not claim the fence
    elif case == "other-dispatch":
        fence(r, source_dispatch="dispatch-someone-else")
    elif case == "origin-mismatch":
        fence(r, origin_target="a-different-successor")
    with pytest.raises(NotificationConflict, match="not demonstrably unrecoverable"):
        asyncio.run(transfer(r))
    assert stored(r).continuation_transfer is None


def test_delivery_to_a_superseded_source_is_not_retryable_and_says_why(recovery):
    r = recovery
    fence(r)
    fail_delivery(r, SelectionError("context_boundary_fenced", FENCED))
    with pytest.raises(NotificationConflict) as raised:
        asyncio.run(respond(r))
    assert raised.value.code == "delivery_failed"
    interaction = stored(r).interaction
    assert interaction.state == InteractionState.FAILED
    assert interaction.delivery_retryable is False, "a fenced source can never accept the response"
    assert interaction.delivery_failure_code == "source_superseded"
    assert interaction.delivery_recovery_action == SUPERSEDED_SOURCE_ACTION
    assert FENCED in interaction.delivery_error
    # Retrying re-checks and fails the same way, without ever claiming it is retryable.
    with pytest.raises(NotificationConflict):
        asyncio.run(respond(r, key="retry-1", retry=True))
    again = stored(r).interaction
    assert again.delivery_attempts == 2 and again.delivery_retryable is False
    assert again.response == interaction.response, "the recorded answer is preserved"


def test_unclassified_delivery_failures_stay_retryable(recovery):
    r = recovery
    fail_delivery(r, RuntimeError("No space left on device"))
    with pytest.raises(NotificationConflict):
        asyncio.run(respond(r))
    interaction = stored(r).interaction
    assert interaction.delivery_retryable is True
    assert interaction.delivery_failure_code is None and interaction.delivery_recovery_action is None
    assert "No space left" in interaction.delivery_error


def test_failed_fenced_response_reaches_the_linked_run_exactly_once(recovery):
    r = recovery
    fence(r)
    fail_delivery(r, SelectionError("context_boundary_fenced", FENCED))
    with pytest.raises(NotificationConflict):
        asyncio.run(respond(r))
    assert stored(r).interaction.delivery_retryable is False

    async def exercise():
        moved = await transfer(r, request_for(r, "transfer-2"))
        # Repairing the route makes delivery possible again and clears the failure class.
        assert moved.interaction.delivery_retryable is True
        assert moved.interaction.delivery_failure_code is None
        delivered = await respond(r, key="retry-1", retry=True)
        assert delivered.interaction.state == InteractionState.DELIVERED
        assert delivered.interaction.delivered_at is not None
        assert delivered.interaction.delivery_error is None
        assert len(r.runtime._queue) == 1
        prompt = r.runtime._queue[0]
        assert r.store.get_prompt_acceptance("successor", prompt.id)
        assert r.store.get_prompt_acceptance("old", prompt.id) is None
        # Replaying the same retry cannot enqueue a second turn, and a new key is refused.
        assert (await respond(r, key="retry-1", retry=True)).version == delivered.version
        assert len(r.runtime._queue) == 1
        with pytest.raises(NotificationConflict, match="already delivered"):
            await respond(r, key="retry-2", retry=True)
        assert len(r.runtime._queue) == 1

    asyncio.run(exercise())


def test_cancelled_delivery_is_not_stranded_in_delivery_pending(recovery, monkeypatch):
    r = recovery

    async def exercise():
        started = asyncio.Event()

        async def hang(notification, response):
            started.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(r.service, "_deliver", hang)
        task = asyncio.create_task(respond(r))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        interaction = stored(r).interaction
        assert interaction.state == InteractionState.FAILED, "must not stay in delivery_pending"
        assert interaction.delivery_retryable is True
        assert interaction.delivery_failure_code == "interrupted"
        assert interaction.delivery_attempts == 1
        assert interaction.response is not None, "the recorded answer survives the interruption"
        # The same recorded response can then be retried and delivered.
        monkeypatch.setattr(r.service, "_deliver", AsyncMock(return_value=None))
        delivered = await respond(r, key="retry-1", retry=True)
        assert delivered.interaction.state == InteractionState.DELIVERED

    asyncio.run(exercise())


def api(r):
    app = r.kernel.build_app()
    app.state.ctx = r.kernel.ctx
    client = TestClient(app)
    client.get("/")
    return client, {"X-CSRF-Token": client.cookies.get("pa_csrf")}


def test_public_card_explains_the_failure_and_offers_the_linked_run(recovery):
    r = recovery
    fence(r)
    fail_delivery(r, SelectionError("context_boundary_fenced", FENCED))
    with pytest.raises(NotificationConflict):
        asyncio.run(respond(r))
    client, headers = api(r)
    public = client.get(f"/api/notifications/{r.notice.id}").json()
    presentation = public["presentation"]
    assert presentation["status"] == "Delivery failed"
    assert presentation["required_action"] == SUPERSEDED_SOURCE_ACTION      # not "Retry delivery"
    assert "cannot succeed" in presentation["next_effect"]
    assert FENCED in presentation["failure_reason"]
    assert presentation["recovery"] == {
        "kind": "transfer_to_linked_run", "label": "Send the recorded response to the linked run",
        "expected_session_id": "old", "expected_dispatch_id": "dispatch-old",
        "successor_session_id": "successor", "successor_dispatch_id": "dispatch-successor",
        "successor_label": presentation["recovery"]["successor_label"],
    }
    # The offered repair is exactly what the transfer endpoint accepts, and delivery then works.
    body = {key: value for key, value in presentation["recovery"].items() if key not in {"kind", "label", "successor_label"}}
    body.update(idempotency_key="ui-transfer", expected_version=public["version"], reason="Operator sent it to the linked run")
    assert client.post(f"/api/notifications/{r.notice.id}/transfer-continuation", json=body, headers=headers).status_code == 200
    retried = client.post(f"/api/notifications/{r.notice.id}/respond", json={"idempotency_key": "ui-retry", "retry": True}, headers=headers)
    assert retried.status_code == 200 and retried.json()["interaction"]["state"] == "delivered"
    assert "recovery" not in client.get(f"/api/notifications/{r.notice.id}").json()["presentation"]


def test_no_linked_run_is_offered_without_a_verifiable_fence(recovery):
    r = recovery
    fail_delivery(r, SelectionError("context_boundary_fenced", FENCED))
    with pytest.raises(NotificationConflict):
        asyncio.run(respond(r))
    client, _ = api(r)
    presentation = client.get(f"/api/notifications/{r.notice.id}").json()["presentation"]
    assert "recovery" not in presentation
    assert presentation["required_action"] == SUPERSEDED_SOURCE_ACTION


def test_retryable_failure_still_offers_retry_and_shows_its_reason(recovery):
    r = recovery
    fail_delivery(r, RuntimeError("No space left on device"))
    with pytest.raises(NotificationConflict):
        asyncio.run(respond(r))
    client, _ = api(r)
    presentation = client.get(f"/api/notifications/{r.notice.id}").json()["presentation"]
    assert presentation["required_action"] == "Retry delivery of the recorded response"
    assert "does not create a new reply" in presentation["next_effect"]
    assert "No space left" in presentation["failure_reason"]


def test_a_failed_undeliverable_response_can_be_retired_preserving_the_record(recovery):
    r = recovery
    fence(r)
    fail_delivery(r, SelectionError("context_boundary_fenced", FENCED))
    with pytest.raises(NotificationConflict):
        asyncio.run(respond(r))
    client, headers = api(r)
    resolved = client.post(f"/api/notifications/{r.notice.id}/resolve", json={"idempotency_key": "dismiss-1"}, headers=headers)
    assert resolved.status_code == 200 and resolved.json()["resolved_at"]
    public = client.get(f"/api/notifications/{r.notice.id}").json()
    assert public["presentation"]["status"] == "Request retired · resolved"
    assert "failure_reason" in public["presentation"] and public["presentation"]["failure_reason"] is None
    assert "recovery" not in public["presentation"]
    assert public["interaction"]["response"] is not None, "the saved response is preserved"
    with pytest.raises(NotificationConflict, match="resolved"):
        asyncio.run(respond(r, key="retry-after-dismiss", retry=True))


def test_record_classified_before_the_fix_still_gets_the_repair_from_the_fence(recovery):
    """The live notification was stored as retryable before delivery could classify it."""
    r = recovery
    fail_delivery(r, RuntimeError("Workspace recovery blocked"))   # stored: retryable, unclassified
    with pytest.raises(NotificationConflict):
        asyncio.run(respond(r))
    assert stored(r).interaction.delivery_retryable is True
    fence(r)                                                     # the linked run is created afterwards
    client, headers = api(r)
    presentation = client.get(f"/api/notifications/{r.notice.id}").json()["presentation"]
    assert presentation["recovery"]["kind"] == "transfer_to_linked_run"
    assert presentation["required_action"] == SUPERSEDED_SOURCE_ACTION, "not 'Retry delivery'"
    assert "cannot succeed" in presentation["next_effect"]
    body = {k: v for k, v in presentation["recovery"].items() if k not in {"kind", "label", "successor_label"}}
    body.update(idempotency_key="legacy-transfer", expected_version=client.get(f"/api/notifications/{r.notice.id}").json()["version"],
                reason="Operator sent it to the linked run")
    assert client.post(f"/api/notifications/{r.notice.id}/transfer-continuation", json=body, headers=headers).status_code == 200
    retried = client.post(f"/api/notifications/{r.notice.id}/respond", json={"idempotency_key": "legacy-retry", "retry": True}, headers=headers)
    assert retried.status_code == 200 and retried.json()["interaction"]["state"] == "delivered"
    assert len(r.runtime._queue) == 1
