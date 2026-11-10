"""Telegram push of high-priority notifications and the owner-only reply path.

Everything runs against ``FakeTelegram``: no test contacts Telegram, and no token
or chat id here is real. A guard fixture fails the test if anything tries to reach
api.telegram.org anyway.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi.testclient import TestClient

from pa.auth.users import UserDirectory
from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel, reset_kernel
from pa.domain.notifications import (
    InteractionChoice,
    InteractionKind,
    InteractionRequest,
    InteractionResponse,
    InteractionState,
    Notification,
    NotificationCreate,
    NotificationPriority,
    NotificationType,
)
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent
from pa.intake import telegram_push_store as push_store
from pa.notifications.stall import StallKind, notify_stall

OWNER_CHAT = "1001"
OWNER_USER = "1001"
FAKE_TOKEN = "123456:FAKE-TOKEN-FOR-TESTS"


class FakeTelegram:
    """Stands in for ChannelTransport: records sends, never touches the network."""

    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.fail = False
        self._next = 500

    def close(self) -> None:
        pass

    def send_telegram(self, **kwargs) -> dict:
        if self.fail:
            raise RuntimeError("telegram is down")
        self._next += 1
        self.sent.append({**kwargs, "message_id": str(self._next)})
        return {"provider_message_id": str(self._next), "provider_delivery_id": None}

    @property
    def pushes(self) -> list[dict]:
        return [m for m in self.sent if not m.get("reply_to_message_id")]

    @property
    def acks(self) -> list[dict]:
        return [m for m in self.sent if m.get("reply_to_message_id")]


@pytest.fixture(autouse=True)
def _isolation():
    reset_kernel()
    reset_settings()
    reset_store()
    reset_instance_agent()

    real_send = httpx.Client.send

    def guarded(self, request, **kwargs):
        if "telegram.org" in str(request.url):
            raise AssertionError(f"Telegram network access attempted: {request.url}")
        return real_send(self, request, **kwargs)

    with patch.object(httpx.Client, "send", guarded):
        yield
    reset_instance_agent()
    reset_store()
    reset_settings()
    reset_kernel()


def boot(tmp_path: Path, **overrides) -> tuple[Kernel, FakeTelegram]:
    values = dict(
        data_dir=tmp_path,
        instance_id="local",
        instance_name="Local PA",
        instance_url="http://pa.test:8080",
        agent_enabled=False,
        subscribed_realms=["default"],
        primary_realm="default",
        peers=[],
        telegram_bot_token=FAKE_TOKEN,
        telegram_push_enabled=True,
        telegram_owner_chat_id=OWNER_CHAT,
        telegram_owner_user_id=OWNER_USER,
    )
    values.update(overrides)
    kernel = Kernel.boot(settings=Settings(**values))
    fake = FakeTelegram()
    kernel.ctx.require_service("intake_service").transport = fake
    return kernel, fake


def create(service, *, key="n1", priority=NotificationPriority.HIGH, interaction=None, **fields):
    data = dict(
        realm_id="default",
        type=NotificationType.INTERACTION if interaction else NotificationType.GENERAL,
        priority=priority,
        title="Agent needs input",
        body="Which environment should I deploy to?",
        deduplication_key=key,
        interaction=interaction,
    )
    data.update(fields)
    return service.create(NotificationCreate(**data), principal_id="user:local")


def freeform(request_id="r1", **kw) -> InteractionRequest:
    return InteractionRequest(
        request_id=request_id,
        kind=InteractionKind.MCP_OPERATOR_INPUT,
        prompt="Which environment should I deploy to?",
        allow_freeform=True,
        continuation_mode="none",
        **kw,
    )


def approval(request_id="a1") -> InteractionRequest:
    return InteractionRequest(
        request_id=request_id,
        kind=InteractionKind.APPROVAL,
        prompt="Approve merging the PR?",
        choices=[
            InteractionChoice(id="approve", label="Approve", value=True),
            InteractionChoice(id="reject", label="Reject", value=False),
        ],
        continuation_mode="none",
    )


def permission(request_id="p1") -> InteractionRequest:
    return InteractionRequest(
        request_id=request_id,
        kind=InteractionKind.ACP_PERMISSION,
        prompt="Run rm -rf build",
        choices=[
            InteractionChoice(id="allow_always", label="Allow always", value="allow_always"),
            InteractionChoice(id="allow_once", label="Allow once", value="allow_once"),
            InteractionChoice(id="reject_once", label="Reject", value="reject_once"),
        ],
        continuation_mode="none",
    )


_update_ids = iter(range(9000, 99999))


def update(text, *, chat=OWNER_CHAT, user=OWNER_USER, reply_to=None, update_id=None, bot=False):
    uid = update_id if update_id is not None else next(_update_ids)
    message = {
        "message_id": 700 + uid % 1000,
        "date": 1_700_000_000,
        "chat": {"id": int(chat), "type": "private"},
        "from": {"id": int(user), "is_bot": bot, "first_name": "Owner"},
        "text": text,
    }
    if reply_to is not None:
        message["reply_to_message"] = {"message_id": int(reply_to)}
    return {"update_id": uid, "message": message}


def push_of(kernel, notification) -> dict:
    kernel.ctx.require_service("telegram_push").drain()
    return push_store.get_push(kernel.ctx.store, notification.id)


def reply(kernel, payload, app=None):
    return asyncio.run(kernel.ctx.require_service("telegram_replies").handle_update(payload, app=app))


def state(kernel, notification) -> Notification:
    return kernel.ctx.store.get_notification(notification.id, realm_id=notification.realm_id)


# --------------------------------------------------------------------------- push


def test_high_priority_notification_is_pushed_with_replies_and_token(tmp_path):
    kernel, fake = boot(tmp_path)
    service = kernel.ctx.require_service("notifications")
    notice = create(service, interaction=freeform(), card_id="card-1", session_id="sess-12345678")
    kernel.ctx.require_service("telegram_push").drain()
    assert len(fake.sent) == 1
    sent = fake.sent[0]
    assert sent["conversation_id"] == OWNER_CHAT
    text = sent["text"]
    assert "Agent needs input" in text and "Which environment" in text
    assert "Session: sess-123" in text
    assert "your answer as plain text" in text
    row = push_store.get_push(kernel.ctx.store, notice.id)
    assert row["push_count"] == 1 and f"[#{row['token']}]" in text
    assert push_store.notification_for_message(kernel.ctx.store, OWNER_CHAT, sent["message_id"]) == notice.id


def test_no_push_below_threshold_or_when_disabled(tmp_path):
    kernel, fake = boot(tmp_path)
    service = kernel.ctx.require_service("notifications")
    create(service, key="low", priority=NotificationPriority.NORMAL, interaction=freeform("low"))
    kernel.ctx.require_service("telegram_push").drain()
    assert fake.sent == []

    off_kernel, off_fake = boot(tmp_path / "off", telegram_push_enabled=False)
    create(off_kernel.ctx.require_service("notifications"), key="off", interaction=freeform("off"))
    off_kernel.ctx.require_service("telegram_push").drain()
    assert off_fake.sent == []

    no_chat, no_chat_fake = boot(tmp_path / "nochat", telegram_owner_chat_id="")
    create(no_chat.ctx.require_service("notifications"), key="nochat", interaction=freeform("nc"))
    no_chat.ctx.require_service("telegram_push").drain()
    assert no_chat_fake.sent == []


def test_min_priority_setting_is_honoured(tmp_path):
    kernel, fake = boot(tmp_path, telegram_push_min_priority="urgent")
    service = kernel.ctx.require_service("notifications")
    create(service, key="h", priority=NotificationPriority.HIGH, interaction=freeform("h"))
    create(service, key="u", priority=NotificationPriority.URGENT, interaction=freeform("u"))
    kernel.ctx.require_service("telegram_push").drain()
    assert len(fake.sent) == 1


def test_coalesced_duplicate_and_repeat_push_send_once(tmp_path):
    kernel, fake = boot(tmp_path)
    service = kernel.ctx.require_service("notifications")
    first = create(service, interaction=freeform())
    again = create(service, interaction=freeform())
    assert again.id == first.id and again.coalesced_count == 2
    push = kernel.ctx.require_service("telegram_push")
    push.drain()
    assert push.push(state(kernel, first)) is False
    assert len(fake.sent) == 1


def test_delivery_failure_does_not_break_creation_and_is_retried(tmp_path):
    kernel, fake = boot(tmp_path)
    fake.fail = True
    service = kernel.ctx.require_service("notifications")
    notice = create(service, interaction=freeform())
    push = kernel.ctx.require_service("telegram_push")
    push.drain()
    assert state(kernel, notice) is not None  # in-app notification exists
    row = push_store.get_push(kernel.ctx.store, notice.id)
    assert row["push_count"] == 0 and row["failures"] == 1 and row["last_error"] == "RuntimeError"
    fake.fail = False
    push.sweep(datetime.now(UTC) + timedelta(minutes=2))  # lease/backoff elapsed
    assert len(fake.sent) == 1
    assert push_store.get_push(kernel.ctx.store, notice.id)["push_count"] == 1


def test_renotify_cadence_and_cap(tmp_path):
    kernel, fake = boot(tmp_path, telegram_renotify_hours=4.0, telegram_renotify_max=2)
    notice = create(kernel.ctx.require_service("notifications"), interaction=freeform())
    push = kernel.ctx.require_service("telegram_push")
    push.drain()
    base = datetime.now(UTC)
    push.sweep(base + timedelta(hours=3, minutes=59))
    assert len(fake.sent) == 1
    push.sweep(base + timedelta(hours=4, minutes=1))
    assert len(fake.sent) == 2 and "reminder 1 of 2" in fake.sent[1]["text"]
    push.sweep(base + timedelta(hours=4, minutes=2))
    assert len(fake.sent) == 2  # not due again yet
    push.sweep(base + timedelta(hours=8, minutes=5))
    assert len(fake.sent) == 3 and "reminder 2 of 2" in fake.sent[2]["text"]
    push.sweep(base + timedelta(hours=30))
    assert len(fake.sent) == 3  # capped at M reminders
    # A reply to a reminder binds to the same notification.
    assert push_store.notification_for_message(
        kernel.ctx.store, OWNER_CHAT, fake.sent[2]["message_id"]
    ) == notice.id


def test_no_reminder_once_answered_or_when_disabled(tmp_path):
    kernel, fake = boot(tmp_path)
    service = kernel.ctx.require_service("notifications")
    notice = create(service, interaction=freeform())
    push = kernel.ctx.require_service("telegram_push")
    push.drain()
    asyncio.run(
        service.respond(
            notice, InteractionResponse(idempotency_key="k", value="staging"), principal_id="user:local"
        )
    )
    push.sweep(datetime.now(UTC) + timedelta(hours=5))
    assert len(fake.sent) == 1

    off, off_fake = boot(tmp_path / "off", telegram_renotify_hours=0)
    create(off.ctx.require_service("notifications"), interaction=freeform())
    off.ctx.require_service("telegram_push").drain()
    off.ctx.require_service("telegram_push").sweep(datetime.now(UTC) + timedelta(hours=50))
    assert len(off_fake.sent) == 1


def test_global_rate_limit_caps_pushes_per_hour(tmp_path):
    kernel, fake = boot(tmp_path, telegram_push_rate_limit_per_hour=2)
    service = kernel.ctx.require_service("notifications")
    for index in range(4):
        create(service, key=f"k{index}", interaction=freeform(f"r{index}"))
    kernel.ctx.require_service("telegram_push").drain()
    assert len(fake.sent) == 2


def test_message_is_truncated_redacted_and_hides_sensitive_content(tmp_path):
    kernel, fake = boot(tmp_path)
    service = kernel.ctx.require_service("notifications")
    secret = "ghp_" + "a" * 36
    create(
        service,
        key="long",
        body=("deploy details " * 400) + secret,
        interaction=freeform("long"),
    )
    create(
        service,
        key="sens",
        title="Enter the password",
        body="the password is hunter2hunter2",
        interaction=freeform("sens", sensitive=True),
    )
    kernel.ctx.require_service("telegram_push").drain()
    texts = [m["text"] for m in fake.sent]
    assert all(len(t) < 2000 for t in texts)
    assert not any(secret in t for t in texts)
    sensitive = next(t for t in texts if "Enter the password" in t)
    assert "hunter2" not in sensitive and "marked sensitive" in sensitive


def test_remote_owned_notification_is_not_pushed(tmp_path):
    kernel, fake = boot(tmp_path)
    create(
        kernel.ctx.require_service("notifications"),
        interaction=freeform(),
        owner_instance_id="someone-else",
    )
    kernel.ctx.require_service("telegram_push").drain()
    assert fake.sent == []


# ----------------------------------------------------------------- inbound security


def pushed(kernel, fake, **kw):
    notice = create(kernel.ctx.require_service("notifications"), **kw)
    kernel.ctx.require_service("telegram_push").drain()
    message_id = fake.sent[-1]["message_id"]
    return notice, message_id


def test_reply_from_wrong_chat_or_wrong_user_or_bot_is_rejected(tmp_path):
    kernel, fake = boot(tmp_path)
    notice, mid = pushed(kernel, fake, interaction=freeform())
    before = len(fake.sent)
    wrong_chat = reply(kernel, update("staging", chat="2002", user="2002", reply_to=mid))
    wrong_user = reply(kernel, update("staging", user="3003", reply_to=mid))
    bot = reply(kernel, update("staging", bot=True, reply_to=mid))
    assert wrong_chat.handled is False  # not the owner chat: ordinary intake rules apply
    assert wrong_user.handled and wrong_user.code == "not_owner"
    assert bot.handled and bot.code == "not_owner"
    assert len(fake.sent) == before  # strangers get no response at all
    assert state(kernel, notice).interaction.state == InteractionState.OUTSTANDING


def test_inbound_disabled_when_push_disabled(tmp_path):
    kernel, fake = boot(tmp_path, telegram_push_enabled=False)
    outcome = reply(kernel, update("staging", reply_to=999))
    assert outcome.handled is False


def test_reply_binds_to_the_notification_it_answers(tmp_path):
    kernel, fake = boot(tmp_path)
    first, first_mid = pushed(kernel, fake, key="one", interaction=freeform("r1"))
    second, second_mid = pushed(kernel, fake, key="two", interaction=freeform("r2"))
    outcome = reply(kernel, update("staging", reply_to=second_mid))
    assert outcome.code == "answered"
    assert state(kernel, second).interaction.response == "staging"
    assert state(kernel, second).interaction.response_principal == "user:local"
    assert state(kernel, first).interaction.state == InteractionState.OUTSTANDING


def test_reply_binds_by_explicit_token(tmp_path):
    kernel, fake = boot(tmp_path)
    first, _ = pushed(kernel, fake, key="one", interaction=freeform("r1"))
    second, _ = pushed(kernel, fake, key="two", interaction=freeform("r2"))
    token = push_store.get_push(kernel.ctx.store, first.id)["token"]
    outcome = reply(kernel, update(f"#{token} production"))
    assert outcome.code == "answered"
    assert state(kernel, first).interaction.response == "production"
    assert state(kernel, second).interaction.state == InteractionState.OUTSTANDING


def test_unbound_message_is_not_ours_and_help_is_offered(tmp_path):
    kernel, fake = boot(tmp_path)
    pushed(kernel, fake, interaction=freeform())
    assert reply(kernel, update("build me a thing")).handled is False
    assert reply(kernel, update("hello", reply_to=424242)).handled is False
    helped = reply(kernel, update("/help"))
    assert helped.handled and helped.code == "help" and "take over" in helped.reply


def test_replayed_update_is_ignored(tmp_path):
    kernel, fake = boot(tmp_path)
    notice, mid = pushed(kernel, fake, interaction=freeform())
    payload = update("staging", reply_to=mid, update_id=5151)
    first = reply(kernel, payload)
    acks = len(fake.acks)
    version = state(kernel, notice).version
    second = reply(kernel, payload)
    assert first.code == "answered" and second.code == "replayed"
    assert len(fake.acks) == acks and state(kernel, notice).version == version


def test_second_reply_to_answered_notification_is_polite_noop(tmp_path):
    kernel, fake = boot(tmp_path)
    notice, mid = pushed(kernel, fake, interaction=freeform())
    reply(kernel, update("staging", reply_to=mid))
    version = state(kernel, notice).version
    again = reply(kernel, update("production", reply_to=mid))
    assert again.code == "already" and "Already answered" in again.reply
    assert state(kernel, notice).version == version
    assert state(kernel, notice).interaction.response == "staging"


def test_sensitive_interaction_cannot_be_answered_over_telegram(tmp_path):
    kernel, fake = boot(tmp_path)
    notice, mid = pushed(kernel, fake, interaction=freeform(sensitive=True))
    outcome = reply(kernel, update("hunter2", reply_to=mid))
    assert outcome.code == "sensitive"
    assert state(kernel, notice).interaction.response is None


# ------------------------------------------------------------------- inbound actions


def test_approve_and_reject_choose_the_right_goal_approval_choice(tmp_path):
    kernel, fake = boot(tmp_path)
    yes, yes_mid = pushed(kernel, fake, key="y", interaction=approval("a1"))
    no, no_mid = pushed(kernel, fake, key="n", interaction=approval("a2"))
    assert reply(kernel, update("Approve", reply_to=yes_mid)).code == "answered"
    assert reply(kernel, update("reject", reply_to=no_mid)).code == "answered"
    assert state(kernel, yes).interaction.response == {"choice_id": "approve", "value": True}
    assert state(kernel, no).interaction.response == {"choice_id": "reject", "value": False}


def test_approve_never_picks_a_standing_allow_always_grant(tmp_path):
    kernel, fake = boot(tmp_path)
    notice, mid = pushed(kernel, fake, interaction=permission())
    assert reply(kernel, update("approve", reply_to=mid)).code == "answered"
    assert state(kernel, notice).interaction.response["choice_id"] == "allow_once"
    other, other_mid = pushed(kernel, fake, key="p2", interaction=permission("p2"))
    reply(kernel, update("1", reply_to=other_mid))  # explicit number may pick it
    assert state(kernel, other).interaction.response["choice_id"] == "allow_always"


def test_ambiguous_choice_reply_gets_help_and_changes_nothing(tmp_path):
    kernel, fake = boot(tmp_path)
    interaction = InteractionRequest(
        request_id="c1",
        kind=InteractionKind.CHOICE,
        prompt="Pick one",
        choices=[
            InteractionChoice(id="a", label="Alpha", value="a"),
            InteractionChoice(id="b", label="Beta", value="b"),
        ],
        continuation_mode="none",
    )
    notice, mid = pushed(kernel, fake, interaction=interaction)
    outcome = reply(kernel, update("whatever", reply_to=mid))
    assert outcome.code == "help" and "1. Alpha" in outcome.reply
    assert state(kernel, notice).interaction.state == InteractionState.OUTSTANDING
    assert reply(kernel, update("beta", reply_to=mid)).code == "answered"


def test_cancel_and_failed_delivery_retry(tmp_path):
    kernel, fake = boot(tmp_path)
    service = kernel.ctx.require_service("notifications")
    notice, mid = pushed(kernel, fake, interaction=freeform("c"))
    assert reply(kernel, update("cancel", reply_to=mid)).code == "answered"
    assert state(kernel, notice).interaction.state == InteractionState.CANCELLED

    failing, failing_mid = pushed(kernel, fake, key="f", interaction=freeform("f"))
    calls = []

    async def flaky(response):
        calls.append(response)
        if len(calls) == 1:
            raise RuntimeError("owner gone")

    service.register_delivery_handler(failing.id, flaky)
    outcome = reply(kernel, update("staging", reply_to=failing_mid))
    assert outcome.code == "delivery_failed"
    assert state(kernel, failing).interaction.state == InteractionState.FAILED
    retried = reply(kernel, update("retry", reply_to=failing_mid))
    assert retried.code == "answered" and len(calls) == 2
    assert state(kernel, failing).interaction.state == InteractionState.DELIVERED


def test_answer_goes_through_the_service_with_owner_principal(tmp_path):
    kernel, fake = boot(tmp_path)
    service = kernel.ctx.require_service("notifications")
    notice, mid = pushed(kernel, fake, interaction=freeform())
    real_respond = service.respond
    seen = {}

    async def spy(notification, response, *, principal_id):
        seen.update(principal=principal_id, key=response.idempotency_key, value=response.value)
        return await real_respond(notification, response, principal_id=principal_id)

    service.respond = spy
    outcome = reply(kernel, update("staging", reply_to=mid, update_id=777))
    assert outcome.code == "answered", outcome
    assert seen == {"principal": "user:local", "key": "telegram:777", "value": "staging"}


def test_owner_principal_must_be_authorized_for_the_realm(tmp_path):
    # Same authorization as the web UI: a principal with no membership sees nothing.
    kernel, fake = boot(tmp_path, telegram_owner_principal="user:stranger")
    notice, mid = pushed(kernel, fake, interaction=freeform())
    outcome = reply(kernel, update("staging", reply_to=mid))
    assert outcome.code == "not_found"
    assert state(kernel, notice).interaction.state == InteractionState.OUTSTANDING


# ---------------------------------------------------------------------------- stalls


def stall(kernel, kind, key="s1", **kw):
    subjects = kw.pop("subject_ids", {"session_id": "sess-1", "card_id": "card-1"})
    return notify_stall(
        kernel.ctx,
        kind=kind,
        subject_ids=subjects,
        title="Something is stuck",
        body="It stopped.",
        severity=kw.pop("severity", "error"),
        dedupe_key=key,
        **kw,
    )


def test_stall_helper_dedupes_and_maps_severity(tmp_path):
    kernel, fake = boot(tmp_path)
    first = stall(kernel, StallKind.PROMPT_FAILED)
    again = stall(kernel, StallKind.PROMPT_FAILED)
    assert again.id == first.id and again.coalesced_count == 2
    assert first.priority == NotificationPriority.HIGH and first.session_id == "sess-1"
    assert first.capability == "stall:prompt_failed"
    assert [a.id for a in first.actions] == ["retry", "take_over", "dismiss"]
    assert stall(kernel, StallKind.PROMPT_FAILED, key="s2", severity="info").priority == NotificationPriority.LOW
    assert stall(kernel, StallKind.PROMPT_FAILED, key="s3", severity="critical").priority == NotificationPriority.URGENT
    kernel.ctx.require_service("telegram_push").drain()
    assert len(fake.sent) == 2  # first (HIGH) and the critical one; coalesced/low not pushed


def test_stall_helper_reraises_unresolved_and_can_reopen_resolved(tmp_path):
    kernel, _ = boot(tmp_path)
    service = kernel.ctx.require_service("notifications")
    first = stall(kernel, StallKind.DISPATCH_FAILED)
    service.resolve(first, principal_id="user:local", idempotency_key="r")
    assert stall(kernel, StallKind.DISPATCH_FAILED).id == first.id  # resolved: left alone
    reopened = stall(kernel, StallKind.DISPATCH_FAILED, reopen_resolved=True)
    assert reopened.id != first.id and reopened.resolved_at is None
    assert stall(kernel, StallKind.DISPATCH_FAILED, reopen_resolved=True).id == reopened.id


def test_stall_helper_never_raises(tmp_path):
    kernel, _ = boot(tmp_path)
    assert notify_stall(kernel.ctx, kind="no_such_kind", title="x", dedupe_key="k") is None
    broken = MagicMock()
    broken.services.get.return_value.create.side_effect = RuntimeError("db down")
    assert notify_stall(broken, kind=StallKind.OTHER, title="x", dedupe_key="k") is None


def test_stall_dismiss_resolves_via_service(tmp_path):
    kernel, fake = boot(tmp_path)
    notice = stall(kernel, StallKind.PR_CLOSED, subject_ids={"pr_number": 12})
    kernel.ctx.require_service("telegram_push").drain()
    mid = fake.sent[-1]["message_id"]
    assert "PR #12" in fake.sent[-1]["text"] and "- dismiss" in fake.sent[-1]["text"]
    assert reply(kernel, update("dismiss", reply_to=mid)).code == "dismissed"
    assert state(kernel, notice).resolved_at is not None
    assert reply(kernel, update("dismiss", reply_to=mid)).code == "already"


def test_stall_take_over_resume_and_retry_route_to_session_services(tmp_path):
    kernel, fake = boot(tmp_path)
    manager = MagicMock()
    manager.set_session_control = AsyncMock()
    manager.retry_session = AsyncMock()
    runtime = MagicMock(_closed=False)
    runtime._drain_transcripts = AsyncMock()
    manager.get.return_value = runtime
    kernel.ctx.services["instance_agent"] = manager

    hung = stall(kernel, StallKind.HUNG_TURN, key="h")
    failed = stall(kernel, StallKind.PROMPT_FAILED, key="p")
    kernel.ctx.require_service("telegram_push").drain()
    mids = {m["text"].splitlines()[1]: m["message_id"] for m in fake.sent}
    hung_mid, failed_mid = fake.sent[0]["message_id"], fake.sent[1]["message_id"]

    assert reply(kernel, update("take over", reply_to=hung_mid)).code == "taken_over"
    manager.set_session_control.assert_awaited_once_with("sess-1", "human")
    assert reply(kernel, update("resume", reply_to=hung_mid)).code == "resumed"
    runtime.resume_queue.assert_called_once()
    assert reply(kernel, update("retry", reply_to=failed_mid)).code == "retried"
    manager.retry_session.assert_awaited_once_with("sess-1")
    # resume is not offered on a prompt failure, so it is just help
    assert reply(kernel, update("resume", reply_to=failed_mid)).code == "help"


def test_interaction_take_over_switches_session_to_manual(tmp_path):
    kernel, fake = boot(tmp_path)
    manager = MagicMock()
    manager.set_session_control = AsyncMock()
    kernel.ctx.services["instance_agent"] = manager
    notice, mid = pushed(kernel, fake, interaction=freeform(), session_id="sess-9")
    assert reply(kernel, update("take over", reply_to=mid)).code == "taken_over"
    manager.set_session_control.assert_awaited_once_with("sess-9", "human")
    assert state(kernel, notice).interaction.state == InteractionState.OUTSTANDING


def test_accept_is_not_performed_from_telegram_without_a_handler(tmp_path):
    kernel, fake = boot(tmp_path)
    notice = stall(kernel, StallKind.ACCEPTANCE_NEEDED)
    kernel.ctx.require_service("telegram_push").drain()
    mid = fake.sent[-1]["message_id"]
    outcome = reply(kernel, update("accept", reply_to=mid))
    assert outcome.code == "accept_unavailable" and "card=card-1" in outcome.reply
    assert state(kernel, notice).resolved_at is None

    calls = []

    async def handler(notification, principal, key):
        calls.append((notification.id, principal, key))
        return "Accepted."

    kernel.ctx.require_service("telegram_replies").accept_handler = handler
    assert reply(kernel, update("accept", reply_to=mid, update_id=4242)).code == "accepted"
    assert calls == [(notice.id, "user:local", "telegram:4242")]


def test_dispatch_retry_uses_the_fleet_retry_route_as_owner(tmp_path):
    kernel, fake = boot(tmp_path)
    UserDirectory(tmp_path).ensure_default_user()
    notice = stall(
        kernel,
        StallKind.DISPATCH_FAILED,
        key="d1",
        subject_ids={"dispatch_id": "dispatch-1", "card_id": "card-1"},
    )
    kernel.ctx.require_service("telegram_push").drain()
    mid = fake.sent[-1]["message_id"]
    route = AsyncMock(return_value={})
    app = MagicMock()
    with patch("pa.modules.fleet._retry_dispatch_api", route):
        outcome = reply(kernel, update("retry", reply_to=mid, update_id=31), app=app)
    assert outcome.code == "retried"
    request, dispatch_id, body = route.await_args.args
    assert dispatch_id == "dispatch-1" and body.idempotency_key == "telegram:31"
    assert request.state.principal_id == "user:local" and request.state.user.id == "local"
    assert request.app is app


def test_schedule_retry_uses_the_ledger_retry(tmp_path):
    from pa.scheduling.store import Ledger

    kernel, fake = boot(tmp_path)
    ledger = MagicMock(spec=Ledger)
    ledger.occurrence_owner.return_value = ("user:local", "sched-1")
    kernel.ctx.services["scheduling"] = MagicMock(ledger=ledger)
    notice = stall(
        kernel, StallKind.SCHEDULE_FAILED, key="occ-9", subject_ids={"schedule_id": "sched-1"}
    )
    kernel.ctx.require_service("telegram_push").drain()
    mid = fake.sent[-1]["message_id"]
    assert reply(kernel, update("retry", reply_to=mid, update_id=77)).code == "retried"
    args = ledger.retry.call_args.args
    assert args[:4] == ("user:local", "sched-1", "occ-9", "telegram:77")


# ------------------------------------------------------------------- webhook endpoint


def test_webhook_routes_owner_reply_and_still_ingests_ordinary_messages(tmp_path):
    kernel, fake = boot(tmp_path, telegram_webhook_secret="hook-secret", telegram_allowed_user_ids=[OWNER_USER])
    notice, mid = pushed(kernel, fake, interaction=freeform())
    headers = {"x-telegram-bot-api-secret-token": "hook-secret"}
    with TestClient(kernel.build_app()) as client:
        bad = client.post("/api/intake/webhooks/telegram", json=update("x", reply_to=mid), headers={})
        assert bad.status_code == 401
        ok = client.post(
            "/api/intake/webhooks/telegram", json=update("staging", reply_to=mid), headers=headers
        )
        assert ok.status_code == 200 and ok.json()["handled"] == "notification_reply"
        ordinary = client.post(
            "/api/intake/webhooks/telegram", json=update("please build a thing"), headers=headers
        )
        assert ordinary.json().get("intake_id")  # normal intake path is untouched
    assert state(kernel, notice).interaction.response == "staging"
    intake = kernel.ctx.require_service("intake_service")
    texts = [item.text for item in intake.list()]
    assert texts == ["please build a thing"]  # the answer was not ingested as a goal


# ------------------------------------------------------------ owner acceptance by reply


def _merged_card_awaiting_acceptance(kernel, title="Ship the importer"):
    from pa.domain.models import CardCreate, CardUpdate, CompletionEvidence

    store = kernel.ctx.store
    card = store.create_card(
        CardCreate(
            title=title,
            lane="waiting",
            completion_requirement={
                "mode": "explicit_acceptance",
                "criteria": "Importer handles the sample file",
                "milestones": ["integrated"],
                "acceptance_principals": ["user:local"],
            },
        )
    )
    return store.update_card(
        card.id,
        CardUpdate(expected_version=card.updated_at),
        principal_id="instance:pr-supervisor",
        integration_evidence=CompletionEvidence(
            requirement_revision=card.completion_requirement.revision,
            subject_revision="a" * 40,
            outcome="integrated",
        ),
        idempotency_key="integrated",
    )


def _announce(kernel, card):
    """Run the verification scheduler's cycle step that raises the acceptance notice."""
    verification = kernel.ctx.require_service("verification")
    asyncio.run(verification.process(card))
    kernel.ctx.require_service("telegram_push").drain()


def test_telegram_accept_completes_the_card_as_the_owner_with_channel_provenance(tmp_path):
    kernel, fake = boot(tmp_path)
    with TestClient(kernel.build_app()):
        card = _merged_card_awaiting_acceptance(kernel)
        assert card.completion_status["reason_code"] == "acceptance_pending"
        _announce(kernel, card)
        [push] = fake.pushes
        assert "acceptance" in push["text"].lower() or "accept" in push["text"].lower()
        outcome = reply(kernel, update("accept", reply_to=push["message_id"], update_id=6001))
        assert outcome.code == "accepted", outcome.reply
        done = kernel.ctx.store.get_card(card.id)
        assert done.lane == "done" and done.completion_status["accepted"]
        receipt = done.completion_evidence[-1]
        assert receipt.outcome == "human_override" and receipt.actor == "user:local" and receipt.actor_kind == "human"
        assert "channel:telegram" in receipt.references and "telegram-update:telegram:6001" in receipt.references
        assert any(r.startswith("notification:") for r in receipt.references)
        notice = kernel.ctx.store.list_notifications(realm_id="default")[0]
        assert notice.resolved_at is not None
        # A replayed delivery of the same update is a no-op.
        assert reply(kernel, update("accept", reply_to=push["message_id"], update_id=6001)).code == "replayed"
        assert len(kernel.ctx.store.get_card(card.id).completion_evidence) == len(done.completion_evidence)


def test_telegram_accept_refuses_a_stale_requirement_revision_and_other_senders(tmp_path):
    from pa.domain.models import CardUpdate

    kernel, fake = boot(tmp_path)
    with TestClient(kernel.build_app()):
        card = _merged_card_awaiting_acceptance(kernel)
        _announce(kernel, card)
        mid = fake.pushes[-1]["message_id"]
        # A stranger replying never reaches the card.
        assert reply(kernel, update("accept", reply_to=mid, user="9999")).code == "not_owner"
        assert kernel.ctx.store.get_card(card.id).lane == "waiting"
        # The owner changes what "done" means after the notice went out.
        kernel.ctx.store.update_card(
            card.id,
            CardUpdate(
                expected_version=card.updated_at,
                field_intent=["completion_requirement"],
                completion_requirement=card.completion_requirement.model_copy(
                    update={"criteria": "Importer handles two files"}
                ),
            ),
        )
        outcome = reply(kernel, update("accept", reply_to=mid, update_id=6002))
        assert outcome.code == "accept_stale" and "out of date" in outcome.reply
        assert kernel.ctx.store.get_card(card.id).lane == "waiting"


def test_telegram_accept_only_acts_on_acceptance_notifications(tmp_path):
    kernel, fake = boot(tmp_path)
    with TestClient(kernel.build_app()):
        card = _merged_card_awaiting_acceptance(kernel)
        # An offered-by-hand accept action on some other kind of notice must not complete the card.
        notice = stall(
            kernel,
            StallKind.PR_CLOSED,
            subject_ids={"card_id": card.id},
            actions=["accept", "dismiss"],
        )
        kernel.ctx.require_service("telegram_push").drain()
        outcome = reply(kernel, update("accept", reply_to=fake.pushes[-1]["message_id"], update_id=6003))
        assert outcome.code == "accept_unavailable"
        assert kernel.ctx.store.get_card(card.id).lane == "waiting"
        assert state(kernel, notice).resolved_at is None
