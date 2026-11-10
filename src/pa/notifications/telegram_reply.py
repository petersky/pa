"""Turn an owner's Telegram reply into the action on the notification it answers.

Security model (all enforced here, before any action):

* Only the configured owner chat AND owner user may act. Everyone else is ignored
  (a stranger replying to a pushed message gets no response and no side effect).
* A reply binds to exactly one notification: the message it replies to (every
  Telegram message id PA pushed is recorded), or an explicit ``#token`` that
  starts the text. A message that binds to nothing is not ours and flows on to
  ordinary intake untouched.
* Replays are harmless. The Telegram ``update_id`` is claimed once, and the
  action itself carries the idempotency key ``telegram:<update_id>``; answering
  an already-answered notification only produces a polite "already answered".
* Every action calls the same internal service the web UI uses (the notification
  service for answers, dismissals and delivery retries; the agent session manager
  for take over, resume and retry; the dispatch and schedule retry routes), acting
  as the configured owner principal, so authorization and audit are unchanged.
* Sensitive interactions are never answered over Telegram.

Card acceptance is deliberately not performed from here: PA only records a human
acceptance for a signed-in browser session. The ``accept`` reply therefore points
the owner to the card, unless an ``accept_handler`` is installed.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pa.domain.notifications import (
    InteractionChoice,
    InteractionResponse,
    InteractionState,
    Notification,
)
from pa.execution.progress import sanitize_text
from pa.intake import telegram_push_store as push_store
from pa.notifications.service import NotificationConflict
from pa.notifications.stall import StallKind, stall_kind_of, stall_verbs
from pa.notifications.telegram_push import is_approval_like

logger = logging.getLogger(__name__)

_TOKEN = re.compile(r"^\s*#([0-9a-f]{6})\b[\s:,-]*", re.IGNORECASE)
_POSITIVE_WORDS = {"approve", "approved", "yes", "y", "allow", "ok", "okay", "accept", "confirm"}
_NEGATIVE_WORDS = {"reject", "rejected", "no", "n", "deny", "denied", "decline"}
_STICKY = ("always", "forever", "remember", "all")
_RETRY = {"retry", "try again", "resend"}
_CANCEL = {"cancel", "skip", "abort"}
_TAKE_OVER = {"take over", "takeover", "take control"}
_RESUME = {"resume", "continue"}
_DISMISS = {"dismiss", "ack", "acknowledge", "done", "resolve", "ok", "okay"}
_ACCEPT = {"accept", "approve", "yes", "y"}
_HELP = {"/help"}


@dataclass
class ReplyOutcome:
    handled: bool
    reply: str | None = None
    code: str = ""


def _norm(text: str) -> str:
    return re.sub(r"[\s_-]+", " ", text.strip().lower()).strip(" .!")


class TelegramReplyRouter:
    def __init__(self, ctx) -> None:
        self.ctx = ctx
        # Optional hook for deployments that can record a human acceptance.
        self.accept_handler: Callable[[Notification, str, str], Awaitable[str | None]] | None = None

    # -- entry point ---------------------------------------------------------

    async def handle_update(self, payload: dict[str, Any], app: Any = None) -> ReplyOutcome:
        settings = self.ctx.settings
        if not (
            getattr(settings, "telegram_push_enabled", False)
            and str(settings.telegram_owner_chat_id).strip()
        ):
            return ReplyOutcome(False)
        message = payload.get("message")
        if not isinstance(message, dict):
            return ReplyOutcome(False)
        chat = message.get("chat") or {}
        sender = message.get("from") or {}
        chat_id = str(chat.get("id") or "")
        owner_chat = str(settings.telegram_owner_chat_id).strip()
        if chat_id != owner_chat:
            return ReplyOutcome(False)
        text = str(message.get("text") or "").strip()
        reply_to = (message.get("reply_to_message") or {}).get("message_id")
        store = self.ctx.store
        bound_id = None
        if reply_to is not None:
            bound_id = await asyncio.to_thread(
                push_store.notification_for_message, store, chat_id, str(reply_to)
            )
        token_match = _TOKEN.match(text)
        if bound_id is None and token_match:
            bound_id = await asyncio.to_thread(
                push_store.notification_for_token, store, chat_id, token_match.group(1)
            )
        is_help = _norm(text).split("@", 1)[0] in _HELP
        if bound_id is None and not is_help:
            return ReplyOutcome(False)
        if not self._sender_is_owner(sender, owner_chat):
            logger.warning("Ignoring Telegram reply from a non-owner sender")
            return ReplyOutcome(True, code="not_owner")
        message_id = str(message.get("message_id") or "")
        update_id = str(payload.get("update_id") or "")
        if bound_id is None:
            return ReplyOutcome(True, self.help_text(None), "help")
        if not update_id:
            return ReplyOutcome(True, code="missing_update_id")
        first = await asyncio.to_thread(
            push_store.claim_update, store, update_id, now=datetime.now(UTC)
        )
        if not first:
            return ReplyOutcome(True, code="replayed")
        body = text[token_match.end() :] if token_match else text
        try:
            reply, code = await self._dispatch(bound_id, body, update_id, app)
        except Exception:  # noqa: BLE001 - a reply must never crash the webhook
            logger.exception("Telegram reply handling failed")
            reply, code = "That did not work. Try again in the PA app.", "error"
        await asyncio.to_thread(
            push_store.finish_update, store, update_id, notification_id=bound_id, outcome=code
        )
        await self._send(chat_id, reply, message_id)
        return ReplyOutcome(True, reply, code)

    # -- internals -----------------------------------------------------------

    def _sender_is_owner(self, sender: dict[str, Any], owner_chat: str) -> bool:
        if sender.get("is_bot"):
            return False
        owner_user = str(self.ctx.settings.telegram_owner_user_id).strip()
        if not owner_user and owner_chat.lstrip("-").isdigit() and not owner_chat.startswith("-"):
            owner_user = owner_chat  # a private chat holds only its own user
        return bool(owner_user) and str(sender.get("id") or "") == owner_user

    def principal(self, user_id: str | None = None) -> str:
        """The PA principal replies act as: a /link binding wins over the setting."""
        intake = self.ctx.services.get("intake_service")
        user = str(self.ctx.settings.telegram_owner_user_id).strip() or user_id
        if intake is not None and user:
            from pa.intake.models import Channel

            try:
                binding = intake.identity(self.ctx.settings.primary_realm, Channel.TELEGRAM, user)
            except Exception:  # noqa: BLE001
                binding = None
            if binding is not None:
                return binding.principal_id
        return str(self.ctx.settings.telegram_owner_principal)

    def _realms(self, principal: str) -> set[str]:
        subscribed = set(self.ctx.settings.subscribed_realms)
        membership = self.ctx.services.get("membership")
        if not membership:
            return subscribed
        member = principal.removeprefix("user:") if principal.startswith("user:") else principal
        return {realm for realm in subscribed if membership.has_role(realm, member)}

    async def _send(self, chat_id: str, text: str | None, reply_to: str) -> None:
        if not text:
            return
        intake = self.ctx.services.get("intake_service")
        transport = getattr(intake, "transport", None)
        if transport is None:
            return
        try:
            await asyncio.to_thread(
                transport.send_telegram,
                conversation_id=chat_id,
                thread_id=None,
                reply_to_message_id=reply_to or None,
                text=text[:3900],
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Telegram acknowledgement failed: %s", type(exc).__name__)

    def help_text(self, notification: Notification | None) -> str:
        if notification is None:
            return (
                "PA replies: reply to a PA message (or start with its #token) with "
                "your answer, a number or option name, approve / reject, cancel, retry, "
                "take over, resume or dismiss."
            )
        lines = ["I could not tell what to do with that. Reply with:"]
        interaction = notification.interaction
        if interaction is not None:
            if interaction.choices:
                for index, choice in enumerate(interaction.choices[:8], 1):
                    lines.append(f"{index}. {sanitize_text(choice.label, limit=80)}")
            if interaction.allow_freeform or interaction.response_schema:
                lines.append("- your answer as plain text")
            if interaction.allow_cancel:
                lines.append("- cancel")
            if interaction.state == InteractionState.FAILED:
                lines.append("- retry")
        else:
            for verb in stall_verbs(notification) or ["dismiss"]:
                lines.append("- " + verb.replace("_", " "))
        return "\n".join(lines)

    async def _dispatch(
        self, notification_id: str, body: str, update_id: str, app: Any
    ) -> tuple[str, str]:
        principal = self.principal()
        service = self.ctx.require_service("notifications")
        realms = self._realms(principal)
        try:
            notification = await asyncio.to_thread(
                self._load_authorized, service, notification_id, principal, realms
            )
        except KeyError:
            return "That notification no longer exists.", "not_found"
        key = f"telegram:{update_id}"
        verb = _norm(body)
        owner = notification.owner_instance_id
        if owner and owner != self.ctx.settings.instance_id:
            return "That request belongs to another PA instance. Answer it in the PA app.", "remote"
        if notification.interaction is not None:
            return await self._interaction(service, notification, body, verb, key, principal, app)
        return await self._stall(service, notification, verb, key, principal, app)

    @staticmethod
    def _load_authorized(service, notification_id: str, principal: str, realms: set[str]):
        return service.get_authorized(notification_id, principal_id=principal, realms=realms)

    # -- interactions --------------------------------------------------------

    async def _interaction(
        self,
        service,
        notification: Notification,
        body: str,
        verb: str,
        key: str,
        principal: str,
        app: Any,
    ) -> tuple[str, str]:
        interaction = notification.interaction
        assert interaction is not None
        if interaction.sensitive:
            return "This request is marked sensitive; answer it in the PA app.", "sensitive"
        if notification.resolved_at is not None or interaction.state not in {
            InteractionState.OUTSTANDING,
            InteractionState.FAILED,
            InteractionState.ANSWERED,
            InteractionState.DELIVERY_PENDING,
        }:
            return f"Already answered ({interaction.state.value}). Nothing was changed.", "already"
        if verb in _TAKE_OVER:
            return await self._take_over(notification, app)
        retry_possible = interaction.state == InteractionState.FAILED or (
            interaction.state in {InteractionState.ANSWERED, InteractionState.DELIVERY_PENDING}
            and not service.delivery_active(notification.id)
        )
        if verb in _RETRY:
            if not retry_possible:
                return "There is no failed answer to retry.", "no_retry"
            return await self._respond(
                service, notification, InteractionResponse(idempotency_key=key, retry=True), principal,
                "Retried delivery of your earlier answer.",
            )
        if interaction.state != InteractionState.OUTSTANDING:
            if interaction.state == InteractionState.FAILED:
                return "Delivery of your earlier answer failed. Reply retry to send it again.", "failed"
            return "Already answered; delivery is in progress. Nothing was changed.", "already"
        if verb in _CANCEL:
            if not interaction.allow_cancel:
                return "This request cannot be cancelled.", "no_cancel"
            return await self._respond(
                service, notification, InteractionResponse(idempotency_key=key, cancel=True), principal,
                "Cancelled.",
            )
        response = self._build_response(notification, body, verb, key)
        if response is None:
            return self.help_text(notification), "help"
        return await self._respond(service, notification, response, principal, "Answered.")

    def _build_response(
        self, notification: Notification, body: str, verb: str, key: str
    ) -> InteractionResponse | None:
        interaction = notification.interaction
        assert interaction is not None
        choices = interaction.choices
        multiple = (interaction.response_schema or {}).get("type") == "array"
        if choices:
            picked = self._pick_choices(notification, verb, multiple)
            if picked:
                if multiple:
                    return InteractionResponse(idempotency_key=key, choice_ids=[c.id for c in picked])
                return InteractionResponse(idempotency_key=key, choice_id=picked[0].id)
        text = body.strip()
        if text.lower().startswith("answer:"):
            text = text[len("answer:") :].strip()
        if not text:
            return None
        if interaction.response_schema and not choices:
            kind = interaction.response_schema.get("type")
            if kind == "string":
                return InteractionResponse(idempotency_key=key, value=text)
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                return None
            if isinstance(parsed, dict):
                return InteractionResponse(idempotency_key=key, fields=parsed)
            return InteractionResponse(idempotency_key=key, value=parsed) if parsed is not None else None
        if interaction.allow_freeform:
            return InteractionResponse(idempotency_key=key, value=text)
        return None

    def _pick_choices(
        self, notification: Notification, verb: str, multiple: bool
    ) -> list[InteractionChoice]:
        interaction = notification.interaction
        assert interaction is not None
        choices = interaction.choices
        parts = [p for p in re.split(r"[,\s]+", verb) if p] if multiple else [verb]
        picked: list[InteractionChoice] = []
        for part in parts:
            match = self._match_one(notification, part)
            if match is None:
                return []
            picked.append(match)
        if len({c.id for c in picked}) != len(picked):
            return []
        return picked

    def _match_one(self, notification: Notification, token: str) -> InteractionChoice | None:
        interaction = notification.interaction
        assert interaction is not None
        choices = interaction.choices
        if token.isdigit():
            index = int(token)
            return choices[index - 1] if 1 <= index <= len(choices) else None
        for choice in choices:
            if token in {choice.id.lower(), _norm(choice.label)}:
                return choice
        if is_approval_like(notification) or {c.id.lower() for c in choices} & {"approve", "reject"}:
            wanted = (
                True if token in _POSITIVE_WORDS else False if token in _NEGATIVE_WORDS else None
            )
            if wanted is not None:
                return self._by_sense(choices, wanted)
        return None

    @staticmethod
    def _by_sense(choices: list[InteractionChoice], positive: bool) -> InteractionChoice | None:
        words = _POSITIVE_WORDS if positive else _NEGATIVE_WORDS
        found: list[InteractionChoice] = []
        for choice in choices:
            label_words = set(re.split(r"[^a-z]+", f"{choice.id} {choice.label}".lower()))
            if any(sticky in label_words for sticky in _STICKY):
                continue  # never pick a standing "always" grant on a one-word reply
            if choice.value is positive and isinstance(choice.value, bool):
                found.append(choice)
            elif label_words & words:
                found.append(choice)
        if len(found) == 1:
            return found[0]
        once = [c for c in found if "once" in f"{c.id} {c.label}".lower()]
        return once[0] if len(once) == 1 else None

    async def _respond(
        self,
        service,
        notification: Notification,
        response: InteractionResponse,
        principal: str,
        success: str,
    ) -> tuple[str, str]:
        try:
            await service.respond(notification, response, principal_id=principal)
        except NotificationConflict as exc:
            if exc.code in {"interaction_already_resolved", "interaction_expired"}:
                return f"Already answered or closed ({exc.code.replace('_', ' ')}).", "already"
            if exc.code == "delivery_failed":
                return "Your answer was recorded but delivery failed. Reply retry to try again.", "delivery_failed"
            return sanitize_text(f"Not accepted: {exc}", limit=300), exc.code
        return success, "answered"

    # -- stalls --------------------------------------------------------------

    async def _stall(
        self, service, notification: Notification, verb: str, key: str, principal: str, app: Any
    ) -> tuple[str, str]:
        offered = set(stall_verbs(notification)) | {"dismiss"}
        action = None
        for name, words in (
            ("retry", _RETRY),
            ("take_over", _TAKE_OVER),
            ("resume", _RESUME),
            ("accept", _ACCEPT),
            ("dismiss", _DISMISS),
        ):
            if verb in words and name in offered:
                action = name
                break
        if action is None:
            return self.help_text(notification), "help"
        if notification.resolved_at is not None:
            return "Already handled. Nothing was changed.", "already"
        if action == "dismiss":
            try:
                await asyncio.to_thread(
                    service.resolve, notification, principal_id=principal, idempotency_key=key
                )
            except NotificationConflict as exc:
                return sanitize_text(f"Not accepted: {exc}", limit=300), exc.code
            return "Dismissed.", "dismissed"
        if action == "take_over":
            return await self._take_over(notification, app)
        if action == "resume":
            return await self._resume(notification)
        if action == "accept":
            return await self._accept(notification, principal, key)
        return await self._retry(notification, principal, key, app)

    async def _take_over(self, notification: Notification, app: Any) -> tuple[str, str]:
        if not notification.session_id:
            return "There is no session to take over for this notification.", "no_session"
        manager = self.ctx.services.get("instance_agent")
        if manager is None:
            return "The session manager is unavailable.", "unavailable"
        try:
            await manager.set_session_control(notification.session_id, "human")
        except LookupError:
            return "That session no longer exists.", "not_found"
        except ValueError as exc:
            return sanitize_text(f"Cannot take over: {exc}", limit=200), "refused"
        return "Session is now under manual control. Open PA to continue.", "taken_over"

    async def _resume(self, notification: Notification) -> tuple[str, str]:
        manager = self.ctx.services.get("instance_agent")
        runtime = manager.get(notification.session_id) if manager and notification.session_id else None
        if runtime is None or getattr(runtime, "_closed", False):
            return "That session is not live. Reply retry to restart it.", "not_live"
        runtime.resume_queue()
        drain = getattr(runtime, "_drain_transcripts", None)
        if callable(drain):
            pending = drain()
            if asyncio.iscoroutine(pending):
                await pending
        return "Queue resumed.", "resumed"

    async def _accept(self, notification: Notification, principal: str, key: str) -> tuple[str, str]:
        if self.accept_handler is not None:
            message = await self.accept_handler(notification, principal, key)
            if message:
                return message, "accepted"
        link = ""
        base = str(getattr(self.ctx.settings, "instance_url", "") or "").rstrip("/")
        if base and notification.card_id:
            link = f" Open {base}/work?card={notification.card_id}"
        return (
            "Acceptance is recorded only from a signed-in PA session, so it cannot be "
            "done from Telegram." + link,
            "accept_unavailable",
        )

    async def _retry(
        self, notification: Notification, principal: str, key: str, app: Any
    ) -> tuple[str, str]:
        kind = stall_kind_of(notification)
        if kind == StallKind.DISPATCH_FAILED:
            return await self._retry_dispatch(notification, principal, key, app)
        if kind == StallKind.SCHEDULE_FAILED:
            return await self._retry_schedule(notification, key)
        if notification.session_id:
            manager = self.ctx.services.get("instance_agent")
            if manager is None:
                return "The session manager is unavailable.", "unavailable"
            try:
                await manager.retry_session(notification.session_id)
            except LookupError:
                return "That session no longer exists.", "not_found"
            except Exception as exc:  # noqa: BLE001 - same failure the UI would show
                return sanitize_text(f"Retry failed: {exc}", limit=300), "retry_failed"
            return "Retrying the session.", "retried"
        return "Retry is not available for this notification.", "no_retry"

    async def _retry_dispatch(
        self, notification: Notification, principal: str, key: str, app: Any
    ) -> tuple[str, str]:
        if not notification.dispatch_id or app is None:
            return "Retry is not available for this dispatch.", "no_retry"
        from fastapi import HTTPException

        from pa.modules.fleet import DispatchControlBody, _retry_dispatch_api

        request = self._owner_request(app, principal)
        if request is None:
            return "The owner account could not be resolved.", "no_owner"
        try:
            await _retry_dispatch_api(
                request, notification.dispatch_id, DispatchControlBody(idempotency_key=key)
            )
        except HTTPException as exc:
            detail = exc.detail if isinstance(exc.detail, dict) else {"message": str(exc.detail)}
            return sanitize_text(f"Retry refused: {detail.get('message') or detail.get('code')}", limit=300), "retry_refused"
        return "Dispatch retry queued.", "retried"

    async def _retry_schedule(self, notification: Notification, key: str) -> tuple[str, str]:
        scheduler = self.ctx.services.get("scheduling")
        occurrence_id = (notification.deduplication_key or "").rsplit(":", 1)[-1]
        if scheduler is None or not occurrence_id:
            return "Retry is not available for this schedule.", "no_retry"
        ledger = scheduler.ledger
        found = await asyncio.to_thread(ledger.occurrence_owner, occurrence_id)
        if not found:
            return "That scheduled occurrence no longer exists.", "not_found"
        owner, schedule_id = found
        try:
            await asyncio.to_thread(
                ledger.retry, owner, schedule_id, occurrence_id, key, datetime.now(UTC).timestamp()
            )
        except (KeyError, ValueError) as exc:
            return sanitize_text(f"Retry refused: {exc}", limit=300), "retry_refused"
        return "Scheduled prompt will be retried.", "retried"

    def _owner_request(self, app: Any, principal: str):
        """A request carrying the owner's identity, as the scheduler builds for delivery."""
        from starlette.requests import Request

        from pa.auth.users import UserDirectory

        user = UserDirectory(self.ctx.settings.data_dir).get(principal.removeprefix("user:"))
        if user is None:
            return None
        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/api/intake/webhooks/telegram",
                "headers": [],
                "app": app,
                "query_string": b"",
            }
        )
        request.state.user = user
        request.state.principal_id = principal
        request.state.user_authenticated = True
        request.state.instance_authenticated = False
        return request
