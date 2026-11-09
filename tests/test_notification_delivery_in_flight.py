"""A response being delivered right now must not offer "Retry delivery".

Regression: after the operator clicked an action, a live refresh rendered the
persisted ``delivery_pending`` state with a Retry button and a "delivery state"
line, although the delivery was running. Persisted state cannot distinguish that
from a delivery stranded by a restart, so the service now records what it is
actually delivering and the presentation exposes ``delivery_active``.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

from pa.domain.notifications import InteractionState
from pa.modules.notifications import _public_notice
from tests.test_notification_continuation_transfer import recovery, respond  # noqa: F401


def _public(r):
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=r.kernel.ctx)))
    return _public_notice(request, r.store.get_notification(r.notice.id), r.store.notification_context_labels([r.notice]))


def test_in_flight_delivery_is_active_then_cleared(recovery):
    r = recovery
    started, release = asyncio.Event(), asyncio.Event()
    seen = {}

    async def slow_handler(_response):
        started.set()
        await release.wait()

    r.service.register_delivery_handler(r.notice.id, slow_handler)

    async def scenario():
        task = asyncio.create_task(respond(r))
        await started.wait()
        stored = r.store.get_notification(r.notice.id)
        seen["state"] = stored.interaction.state
        seen["active"] = r.service.delivery_active(r.notice.id)
        seen["presentation"] = _public(r)["presentation"]
        release.set()
        await task

    asyncio.run(scenario())
    assert seen["state"] == InteractionState.DELIVERY_PENDING
    assert seen["active"] is True
    assert seen["presentation"]["delivery_active"] is True
    assert seen["presentation"]["response_status"]["delivery"] == "Delivering now"
    assert not r.service.delivery_active(r.notice.id)
    assert _public(r)["presentation"]["delivery_active"] is False


def test_failed_delivery_clears_the_marker(recovery):
    r = recovery

    async def broken(_response):
        raise RuntimeError("provider unavailable")

    r.service.register_delivery_handler(r.notice.id, broken)
    try:
        asyncio.run(respond(r))
    except Exception:
        pass
    assert not r.service.delivery_active(r.notice.id)
    assert r.store.get_notification(r.notice.id).interaction.state == InteractionState.FAILED


def test_stranded_pending_delivery_still_offers_retry_and_says_so(recovery):
    """Persisted as pending (e.g. a restart interrupted it) with nothing delivering."""
    r = recovery
    stored = r.store.get_notification(r.notice.id)
    stored.interaction.state = InteractionState.DELIVERY_PENDING
    stored.interaction.response = {"confirmation_id": "confirm-1", "scope": ["repo-1"]}
    stored.interaction.responded_at = stored.updated_at
    r.store.save_notification(stored, principal_id="system:test", instance_id="local")
    presentation = _public(r)["presentation"]
    assert presentation["delivery_active"] is False
    assert presentation["status"] == "Response recorded · delivery not confirmed"
    assert presentation["required_action"] == "Retry delivery of the recorded response"
    assert presentation["response_status"]["delivery"] == "Delivery not confirmed"


def test_stale_marker_expires(recovery):
    r = recovery
    r.service._deliveries_active[r.notice.id] = -10_000.0
    assert not r.service.delivery_active(r.notice.id)
    assert r.notice.id not in r.service._deliveries_active
