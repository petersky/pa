"""Completion/progress delivery to the authority keeps trying and follows the fleet."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from pa.execution.dispatch import CompletionOutbox, DispatchRecord, DispatchStore
from pa.execution.progress import ProgressService


@pytest.fixture
def ledger(tmp_path):
    store = DispatchStore(tmp_path)
    store.put(DispatchRecord(
        dispatch_id="d1", mutation_id="m1", authority_instance_id="authority",
        authority_url="http://frozen.invalid", target_instance_id="target",
        session_id="s1", state="running",
    ))
    yield store
    store.close()


def response(status: int, detail=None):
    out = MagicMock(status_code=status, text=str(detail))
    out.json.return_value = {"detail": detail or {}}
    return out


def outbox_for(ledger, post, **kwargs):
    outbox = CompletionOutbox(ledger, kwargs.pop("token", "tok"), retry_seconds=0.01,
                              peer_post=post, **kwargs)
    assert outbox.queue("s1", {"stop_reason": "end_turn"})
    return outbox


@pytest.mark.asyncio
async def test_transport_failure_is_retried_indefinitely_with_capped_backoff(ledger):
    post = AsyncMock(side_effect=httpx.ConnectError("offline"))
    outbox = outbox_for(ledger, post, max_attempts=0, retry_max_seconds=0.05)
    for _ in range(40):
        await outbox._send(ledger.get("d1"))
    record = ledger.get("d1")
    assert record.attempts == 40
    assert record.completion_delivery_class == "transport_retry"
    assert record.state == "completion_pending"
    assert record.completion_next_retry_at - datetime.now(UTC) <= timedelta(seconds=0.06)


@pytest.mark.asyncio
async def test_owner_is_notified_once_after_n_failures_while_retrying(ledger):
    notifier = MagicMock()
    post = AsyncMock(side_effect=httpx.ConnectError("offline"))
    outbox = outbox_for(ledger, post, max_attempts=0, notify_after_attempts=3, stall_notifier=notifier)
    for _ in range(6):
        await outbox._send(ledger.get("d1"))
    notifier.assert_called_once()
    assert ledger.get("d1").completion_delivery_class == "transport_retry"


@pytest.mark.asyncio
@pytest.mark.parametrize("status,detail", [
    (404, None), (401, None), (409, {"code": "unknown_dispatch", "recoverable": True}),
])
async def test_unknown_dispatch_and_auth_failures_are_retried_inside_the_grace_window(ledger, status, detail):
    outbox = outbox_for(ledger, AsyncMock(return_value=response(status, detail)),
                        unknown_dispatch_grace_seconds=3600)
    await outbox._send(ledger.get("d1"))
    record = ledger.get("d1")
    assert record.completion_delivery_class == "transport_retry"
    assert record.recoverable is True
    assert record.completion_next_retry_at is not None

    outbox._delivery_started_at = lambda _record: datetime.now(UTC) - timedelta(hours=2)
    await outbox._send(ledger.get("d1"))
    assert ledger.get("d1").completion_delivery_class == "permanent_failure"


@pytest.mark.asyncio
@pytest.mark.parametrize("status,detail", [
    (403, None), (422, None), (409, {"code": "completion_payload_conflict"}),
])
async def test_other_rejections_stay_permanent_immediately(ledger, status, detail):
    outbox = outbox_for(ledger, AsyncMock(return_value=response(status, detail)),
                        unknown_dispatch_grace_seconds=3600)
    await outbox._send(ledger.get("d1"))
    assert ledger.get("d1").completion_delivery_class == "permanent_failure"


@pytest.mark.asyncio
async def test_grace_zero_keeps_the_old_single_attempt_permanent_failure(ledger):
    outbox = outbox_for(ledger, AsyncMock(return_value=response(404)), unknown_dispatch_grace_seconds=0)
    await outbox._send(ledger.get("d1"))
    assert ledger.get("d1").completion_delivery_class == "permanent_failure"


@pytest.mark.asyncio
async def test_token_is_read_when_sending_not_captured_at_startup(ledger):
    settings = SimpleNamespace(sync_token="old-token")
    post = AsyncMock(return_value=response(500))
    outbox = outbox_for(ledger, post, token=lambda: settings.sync_token)
    settings.sync_token = "rotated-token"
    await outbox._send(ledger.get("d1"))
    assert post.await_args.kwargs["headers"]["Authorization"] == "Bearer rotated-token"


@pytest.mark.asyncio
async def test_delivery_goes_through_the_peer_router_not_the_frozen_url(ledger):
    post = AsyncMock(return_value=response(500))
    outbox = outbox_for(ledger, post)
    await outbox._send(ledger.get("d1"))
    record, path = post.await_args.args
    assert path == "api/fleet/dispatch/d1/complete"
    assert record.authority_instance_id == "authority"


@pytest.mark.asyncio
async def test_unroutable_authority_falls_back_to_the_recorded_url(ledger, monkeypatch):
    post = AsyncMock(return_value=None)  # authority not in the fleet roster
    outbox = outbox_for(ledger, post)
    client = MagicMock()
    client.post = AsyncMock(return_value=response(500))
    outbox._client = client
    await outbox._send(ledger.get("d1"))
    assert client.post.await_args.args[0] == "http://frozen.invalid/api/fleet/dispatch/d1/complete"


# --- progress ---------------------------------------------------------------------------

def progress_service(**kwargs):
    store = MagicMock()
    service = ProgressService(store, instance_id="target", token=kwargs.pop("token", "tok"),
                              retry_seconds=0.01, **kwargs)
    return service, store


def payload(key="k1"):
    return SimpleNamespace(idempotency_key=key, transport_dict=lambda: {"k": key})


@pytest.mark.asyncio
async def test_progress_backs_off_a_failing_authority_and_resets_on_success():
    post = AsyncMock(return_value=response(503))
    service, _store = progress_service(peer_post=post, retry_max_seconds=60)
    record = SimpleNamespace(authority_instance_id="authority", dispatch_id="d1", authority_url="http://x")
    await service._send(record, payload())
    assert service._backing_off("authority") is True
    assert service._backing_off("someone-else") is False
    service._authority_backoff.clear()
    post.return_value = response(200)
    await service._send(record, payload())
    assert service._backing_off("authority") is False


@pytest.mark.asyncio
async def test_progress_without_a_cap_keeps_the_flat_cadence():
    post = AsyncMock(return_value=response(503))
    service, _store = progress_service(peer_post=post)
    record = SimpleNamespace(authority_instance_id="authority", dispatch_id="d1", authority_url="http://x")
    await service._send(record, payload())
    assert service._backing_off("authority") is False


@pytest.mark.asyncio
async def test_progress_reads_the_token_at_send_time():
    settings = SimpleNamespace(sync_token="old")
    post = AsyncMock(return_value=response(200))
    service, _store = progress_service(peer_post=post, token=lambda: settings.sync_token)
    settings.sync_token = "rotated"
    record = SimpleNamespace(authority_instance_id="authority", dispatch_id="d1", authority_url="http://x")
    await service._send(record, payload())
    assert post.await_args.kwargs["headers"]["Authorization"] == "Bearer rotated"
