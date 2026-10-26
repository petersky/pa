from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from tests.test_notifications import _kernel, _reset_singletons  # noqa: F401
from pa.domain.notifications import (
    InteractionKind, InteractionRequest, InteractionResponse, InteractionState, NotificationCreate,
    NotificationVersionConflict, OperatorInputWithdrawalRequest,
)
from pa.notifications import NotificationConflict


@pytest.fixture
def pending(tmp_path):
    kernel = _kernel(tmp_path)
    service = kernel.ctx.require_service("notifications")
    notice = service.create(NotificationCreate(
        title="Incorrect requester question", type="interaction", session_id="origin",
        principal_id="user:local", visibility="principal",
        interaction=InteractionRequest(
            kind="mcp_operator_input", prompt="An obsolete question", allow_freeform=True,
            request_id="original-key", protocol_request_id="original-key",
            protocol_method="pa/report_dispatch_progress.operator_input", continuation_mode="prompt",
        ),
    ), principal_id="user:local")
    request = OperatorInputWithdrawalRequest(
        idempotency_key="withdraw-key", expected_version=1, expected_session_id="origin",
        expected_request_id="original-key", reason="Requester corrected its mistaken premise",
    )
    return kernel, service, notice, request


def withdraw(pending, request=None, principal="user:local"):
    _, service, notice, original = pending
    return asyncio.run(service.withdraw_operator_input(
        notice, request or original, principal_id=principal,
    ))


def test_withdrawal_preserves_question_and_records_no_answer_or_delivery(pending):
    kernel, service, notice, request = pending
    with patch.object(service, "_deliver", create=True, side_effect=AssertionError("must not deliver")):
        result = withdraw(pending)
        assert withdraw(pending).version == result.version == 2
    before = notice.interaction.model_dump(exclude={"state"})
    assert result.interaction.model_dump(exclude={"state"}) == before
    assert result.interaction.state == InteractionState.SUPERSEDED
    assert result.resolved_at is not None
    assert result.body.startswith(notice.body)
    assert request.reason in result.body
    assert "Withdrawal receipt SHA256:" in result.body
    assert kernel.ctx.store.list_notification_audit(result.id)[0]["action"] == "interaction.superseded"
    with pytest.raises(NotificationConflict, match="already superseded"):
        asyncio.run(service.respond(notice, InteractionResponse(idempotency_key="answer", value="yes"), principal_id="user:local"))
    with pytest.raises(NotificationConflict) as conflict:
        withdraw(pending, request.model_copy(update={"reason": "different inputs"}))
    assert conflict.value.code == "idempotency_conflict"


@pytest.mark.parametrize("change", [
    {"owner_instance_id": "remote"}, {"source_instance_id": "remote"},
    {"principal_id": "user:other"}, {"session_id": "other"},
    {"interaction.kind": InteractionKind.ACP_ELICITATION}, {"interaction.request_id": "other"},
    {"interaction.protocol_request_id": "other"}, {"interaction.state": InteractionState.ANSWERED},
    {"interaction.response": {"value": "recorded"}}, {"interaction.delivery_attempts": 1},
    {"interaction.continuation_prompt_id": "already-prepared"},
])
def test_withdrawal_rejects_wrong_origin_native_requests_and_any_answer(pending, change):
    kernel, service, notice, _ = pending
    for path, value in change.items():
        target, attr = (notice.interaction, path.split(".")[1]) if "." in path else (notice, path)
        setattr(target, attr, value)
    kernel.ctx.store.save_notification(notice, principal_id="user:local", instance_id="local")
    with pytest.raises(NotificationConflict) as error:
        withdraw(pending)
    assert error.value.code == "operator_input_not_withdrawable"
    assert "Withdrawal receipt SHA256:" not in service.store.get_notification(notice.id).body


def test_withdrawal_rechecks_fresh_version_after_cas_conflict(pending):
    kernel, service, notice, _ = pending
    save = kernel.ctx.store.save_notification
    attempts = 0

    def concurrent_answer(item, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            answered = service.store.get_notification(notice.id)
            answered.version += 1
            answered.interaction.state = InteractionState.ANSWERED
            answered.interaction.response = {"value": "preserve this answer"}
            save(answered, principal_id="user:local", instance_id="local", expected_version=1)
            raise NotificationVersionConflict("concurrent answer")
        return save(item, **kwargs)

    with patch.object(kernel.ctx.store, "save_notification", side_effect=concurrent_answer):
        with pytest.raises(NotificationConflict) as error:
            withdraw(pending)
    assert error.value.code == "notification_version_conflict"
    current = service.store.get_notification(notice.id)
    assert current.interaction.response == {"value": "preserve this answer"}
    assert "Withdrawal receipt SHA256:" not in current.body


def test_withdrawal_route_authorization_and_receipt(pending):
    kernel, _, notice, request = pending
    with TestClient(kernel.build_app()) as client:
        client.get("/")
        csrf = {"X-CSRF-Token": client.cookies["pa_csrf"]}
        url = f"/api/notifications/{notice.id}/withdraw-operator-input"
        bad_bearer = client.post(url, json=request.model_dump(), headers={"Authorization": "Bearer invalid"})
        assert bad_bearer.status_code == 401
        denied = client.post(url, json={**request.model_dump(), "expected_version": 5}, headers=csrf)
        assert denied.status_code == 409
        result = client.post(url, json=request.model_dump(), headers=csrf)
        assert result.status_code == 200, result.text
        assert result.json()["interaction"]["state"] == "superseded"
        assert result.json()["interaction"]["response"] is None


def test_unmodified_notifications_do_not_emit_new_receipt_field(pending):
    assert "operator_input_withdrawal" not in pending[2].model_dump(mode="json")


def test_withdrawal_never_truncates_a_full_notice(pending):
    kernel, service, notice, _ = pending
    notice.body = "x" * 128_000
    kernel.ctx.store.save_notification(notice, principal_id="user:local", instance_id="local")
    with pytest.raises(NotificationConflict) as error:
        withdraw(pending)
    assert error.value.code == "withdrawal_audit_too_large"
    stored = service.store.get_notification(notice.id)
    assert stored.body == notice.body
    assert stored.interaction.state == InteractionState.OUTSTANDING


def test_withdrawal_serializes_with_an_inflight_answer(pending):
    _, service, notice, request = pending

    async def exercise():
        entered, release = asyncio.Event(), asyncio.Event()

        async def delivery(*args, **kwargs):
            entered.set()
            await release.wait()

        with patch.object(service, "_deliver", side_effect=delivery):
            answering = asyncio.create_task(service.respond(
                notice, InteractionResponse(idempotency_key="answer-inflight", value="retain"),
                principal_id="user:local",
            ))
            await asyncio.wait_for(entered.wait(), 2)
            withdrawing = asyncio.create_task(service.withdraw_operator_input(notice, request, principal_id="user:local"))
            await asyncio.sleep(0)
            assert not withdrawing.done()
            release.set()
            await answering
            with pytest.raises(NotificationConflict):
                await withdrawing
        assert service.store.get_notification(notice.id).interaction.response == "retain"

    asyncio.run(exercise())
