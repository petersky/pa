"""Push high-priority notifications to the owner's Telegram chat.

This reuses the intake transport (``IntakeService.transport.send_telegram``) and its
bot token; it adds no network client of its own. Creating a notification never
waits on, or fails because of, Telegram: ``submit`` hands the work to a single
background thread, and every error is logged and recorded, not raised.

Per notification PA keeps one row (``telegram_push``) holding a short reply token
and the push count, plus every Telegram message id it sent. The inbound router
(``pa.notifications.telegram_reply``) uses those to bind a reply to the exact
notification it answers. Message bodies are sanitized with the same redaction used
for progress text, truncated, and omitted for sensitive interactions.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any

from pa.domain.notifications import (
    InteractionKind,
    InteractionState,
    Notification,
    NotificationPriority,
)
from pa.execution.progress import sanitize_text
from pa.intake import telegram_push_store as push_store
from pa.intake.security import SlidingWindowLimiter
from pa.notifications.stall import stall_kind_of, stall_verbs

logger = logging.getLogger(__name__)

_PRIORITY_ORDER = {
    NotificationPriority.LOW: 0,
    NotificationPriority.NORMAL: 1,
    NotificationPriority.HIGH: 2,
    NotificationPriority.URGENT: 3,
}
# A notification older than this that was never pushed (feature just enabled, or
# Telegram was down for long) is left alone instead of flooding the chat.
FIRST_PUSH_WINDOW = timedelta(hours=1)
MAX_BODY_CHARS = 700
MAX_TITLE_CHARS = 160
MAX_CHOICES_LISTED = 8


def needs_attention(notification: Notification, now: datetime | None = None) -> bool:
    """True while a human still has something to do about this notification."""
    current = now or datetime.now(UTC)
    if notification.resolved_at is not None:
        return False
    if notification.expires_at and notification.expires_at <= current:
        return False
    interaction = notification.interaction
    if interaction is not None:
        return interaction.state in {InteractionState.OUTSTANDING, InteractionState.FAILED}
    return notification.acknowledged_at is None


def is_approval_like(notification: Notification) -> bool:
    interaction = notification.interaction
    return bool(
        interaction
        and interaction.kind in {InteractionKind.APPROVAL, InteractionKind.ACP_PERMISSION}
    )


class TelegramNotificationPush:
    def __init__(self, ctx) -> None:
        self.ctx = ctx
        self.limiter = SlidingWindowLimiter()
        self._executor: ThreadPoolExecutor | None = None
        self._lock = threading.Lock()

    # -- configuration -------------------------------------------------------

    @property
    def settings(self):
        return self.ctx.settings

    def transport(self):
        intake = self.ctx.services.get("intake_service")
        return getattr(intake, "transport", None)

    def enabled(self) -> bool:
        settings = self.settings
        return bool(
            getattr(settings, "telegram_push_enabled", False)
            and getattr(settings, "telegram_bot_token", "")
            and str(getattr(settings, "telegram_owner_chat_id", "")).strip()
        )

    def owner_chat_id(self) -> str:
        return str(self.settings.telegram_owner_chat_id).strip()

    def eligible(self, notification: Notification, now: datetime | None = None) -> bool:
        if not self.enabled():
            return False
        minimum = NotificationPriority(self.settings.telegram_push_min_priority)
        if _PRIORITY_ORDER[notification.priority] < _PRIORITY_ORDER[minimum]:
            return False
        local = self.settings.instance_id
        # The owning instance pushes; a replica of a remote notice must not duplicate it.
        if notification.owner_instance_id not in (None, local):
            return False
        return needs_attention(notification, now)

    # -- outbound ------------------------------------------------------------

    def submit(self, notification: Notification) -> None:
        """Queue a push for a just-created notification; never blocks or raises."""
        try:
            if not self.eligible(notification):
                return
            with self._lock:
                if self._executor is None:
                    self._executor = ThreadPoolExecutor(
                        max_workers=1, thread_name_prefix="pa-telegram-push"
                    )
                executor = self._executor
            executor.submit(self._push_safely, notification.id, notification.realm_id)
        except Exception:  # noqa: BLE001
            logger.exception("Telegram push could not be queued")

    def drain(self) -> None:
        """Wait for queued pushes (tests and shutdown)."""
        with self._lock:
            executor = self._executor
        if executor is not None:
            executor.submit(lambda: None).result(timeout=30)

    def close(self) -> None:
        with self._lock:
            executor, self._executor = self._executor, None
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=False)

    def _push_safely(self, notification_id: str, realm_id: str) -> None:
        try:
            current = self.ctx.store.get_notification(notification_id, realm_id=realm_id)
            if current is not None:
                self.push(current)
        except Exception:  # noqa: BLE001
            logger.exception("Telegram push failed for notification %s", notification_id)

    def push(self, notification: Notification, *, now: datetime | None = None) -> bool:
        """Send the first push for a notification. True if a message was sent."""
        current = now or datetime.now(UTC)
        if not self.eligible(notification, current):
            return False
        if not self.limiter.allow("push", limit=self._hourly_limit(), window_seconds=3600):
            logger.info("Telegram push rate limit reached; leaving %s in-app", notification.id)
            return False
        chat_id = self.owner_chat_id()
        claim = push_store.claim_first_push(
            self.ctx.store,
            notification.id,
            realm_id=notification.realm_id,
            chat_id=chat_id,
            now=current,
        )
        if claim is None:
            return False
        return self._send(notification, claim, reminder=0, now=current)

    def remind(self, notification: Notification, *, now: datetime | None = None) -> bool:
        current = now or datetime.now(UTC)
        hours = float(self.settings.telegram_renotify_hours)
        maximum = int(self.settings.telegram_renotify_max)
        if hours <= 0 or maximum <= 0 or not self.eligible(notification, current):
            return False
        if not self.limiter.allow("push", limit=self._hourly_limit(), window_seconds=3600):
            return False
        claim = push_store.claim_reminder(
            self.ctx.store,
            notification.id,
            now=current,
            interval=timedelta(hours=hours),
            max_reminders=maximum,
        )
        if claim is None:
            return False
        return self._send(notification, claim, reminder=int(claim["push_count"]), now=current)

    def _load(self, notification_id: str) -> Notification | None:
        for realm_id in self.settings.subscribed_realms:
            item = self.ctx.store.get_notification(notification_id, realm_id=realm_id)
            if item is not None:
                return item
        return None

    def _hourly_limit(self) -> int:
        return int(self.settings.telegram_push_rate_limit_per_hour)

    def _send(
        self, notification: Notification, claim: dict[str, Any], *, reminder: int, now: datetime
    ) -> bool:
        chat_id = str(claim["chat_id"])
        transport = self.transport()
        try:
            if transport is None:
                raise RuntimeError("intake transport unavailable")
            text = self.format_message(
                notification, token=str(claim["token"]), reminder=reminder
            )
            result = transport.send_telegram(
                conversation_id=chat_id,
                thread_id=None,
                reply_to_message_id=None,
                text=text,
            )
        except Exception as exc:  # noqa: BLE001 - delivery failure must stay contained
            push_store.record_failure(
                self.ctx.store,
                notification.id,
                error=type(exc).__name__,
                now=now,
            )
            logger.warning(
                "Telegram push for %s failed: %s", notification.id, type(exc).__name__
            )
            return False
        push_store.record_sent(
            self.ctx.store,
            notification.id,
            chat_id=chat_id,
            message_id=(result or {}).get("provider_message_id"),
            now=now,
        )
        return True

    def sweep(self, now: datetime | None = None) -> None:
        """Retry recent unsent pushes and send due reminders (called every ~30s)."""
        if not self.enabled():
            return
        current = now or datetime.now(UTC)
        store = self.ctx.store
        minimum = _PRIORITY_ORDER[NotificationPriority(self.settings.telegram_push_min_priority)]
        priorities = [p for p, order in _PRIORITY_ORDER.items() if order >= minimum]
        cutoff = current - FIRST_PUSH_WINDOW
        for realm_id in self.settings.subscribed_realms:
            for priority in priorities:
                for item in store.list_notifications(
                    realm_id=realm_id, priority=priority.value, resolved=False, limit=200
                ):
                    if item.created_at < cutoff:
                        continue
                    row = push_store.get_push(store, item.id)
                    if row is None or int(row["push_count"]) == 0:
                        self.push(item, now=current)
        hours = float(self.settings.telegram_renotify_hours)
        maximum = int(self.settings.telegram_renotify_max)
        if hours > 0 and maximum > 0:
            for notification_id in push_store.reminder_candidates(
                store,
                now=current,
                interval=timedelta(hours=hours),
                max_reminders=maximum,
            ):
                item = self._load(notification_id)
                if item is None or not needs_attention(item, current):
                    push_store.close_push(store, notification_id)
                    continue
                self.remind(item, now=current)
        push_store.prune(store, older_than=timedelta(days=30), now=current)

    # -- formatting ----------------------------------------------------------

    def format_message(self, notification: Notification, *, token: str, reminder: int = 0) -> str:
        lines: list[str] = []
        label = "URGENT" if notification.priority == NotificationPriority.URGENT else "Needs you"
        head = f"PA {label}"
        if reminder:
            head += f" (reminder {reminder} of {int(self.settings.telegram_renotify_max)})"
        lines.append(head)
        lines.append(sanitize_text(notification.title, limit=MAX_TITLE_CHARS))
        context = self._context_line(notification)
        if context:
            lines.append(context)
        interaction = notification.interaction
        sensitive = bool(interaction and interaction.sensitive)
        if sensitive:
            lines.append("Details hidden: this request is marked sensitive. Answer it in the PA app.")
        else:
            body = notification.body or notification.summary
            if interaction and interaction.prompt and interaction.prompt not in (body or ""):
                body = interaction.prompt
            excerpt = sanitize_text(body, limit=MAX_BODY_CHARS) if body else ""
            if excerpt:
                lines.append("")
                lines.append(excerpt)
        if interaction and interaction.choices and not sensitive:
            lines.append("")
            for index, choice in enumerate(interaction.choices[:MAX_CHOICES_LISTED], 1):
                lines.append(f"{index}. {sanitize_text(choice.label, limit=80)}")
            hidden = len(interaction.choices) - MAX_CHOICES_LISTED
            if hidden > 0:
                lines.append(f"... and {hidden} more (answer in the PA app)")
        lines.append("")
        lines.extend(self._reply_hints(notification))
        lines.append(f"[#{token}]")
        link = self._link(notification)
        if link:
            lines.append(link)
        return "\n".join(lines)[:3900]

    def _context_line(self, notification: Notification) -> str:
        parts: list[str] = []
        if notification.card_id:
            title = ""
            try:
                labels = self.ctx.store.notification_context_labels([notification])
                title = labels["card"].get((notification.realm_id, notification.card_id), "")
            except Exception:  # noqa: BLE001
                title = ""
            parts.append(
                "Card: " + (sanitize_text(title, limit=80) if title else notification.card_id[:12])
            )
        if notification.session_id:
            parts.append("Session: " + notification.session_id[:8])
        if notification.dispatch_id:
            parts.append("Dispatch: " + notification.dispatch_id[:8])
        if notification.pr_number:
            parts.append(f"PR #{notification.pr_number}")
        return " | ".join(parts)

    def _reply_hints(self, notification: Notification) -> list[str]:
        interaction = notification.interaction
        hints = ["Reply to this message with:"]
        if interaction is not None:
            if interaction.sensitive:
                return ["Answer in the PA app."]
            if interaction.state == InteractionState.FAILED:
                hints.append("- retry (send your earlier answer again)")
            if interaction.choices:
                hints.append("- a number or option name to choose")
            if is_approval_like(notification):
                hints.append("- approve or reject")
            if interaction.allow_freeform or interaction.response_schema:
                hints.append("- your answer as plain text")
            if interaction.allow_cancel:
                hints.append("- cancel")
            if notification.session_id:
                hints.append("- take over (switch the session to manual control)")
        else:
            verbs = stall_verbs(notification) or (
                ["dismiss"] if stall_kind_of(notification) else []
            )
            for verb in verbs:
                hints.append(f"- {verb.replace('_', ' ')}")
            if not verbs:
                hints.append("- dismiss")
        hints.append("(or start a reply with the #token below)")
        return hints

    def _link(self, notification: Notification) -> str:
        base = str(getattr(self.settings, "instance_url", "") or "").rstrip("/")
        if not base:
            return ""
        if notification.destination_url and notification.destination_url.startswith("/"):
            return base + notification.destination_url
        if notification.card_id:
            return f"{base}/work?card={notification.card_id}"
        if notification.session_id:
            return f"{base}/agent?session={notification.session_id}"
        return base

