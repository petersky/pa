"""One call to tell the owner that agent work is stuck and needs a human.

``notify_stall`` creates a single, deduplicated notification through the existing
``NotificationService``. High-severity stalls get HIGH priority, so they are also
pushed to the owner's Telegram chat when that channel is enabled
(see ``pa.notifications.telegram_push``), and the owner can answer from there.

Adopting it is one function call::

    notify_stall(
        ctx,                                   # AppContext or NotificationService
        kind=StallKind.PROMPT_FAILED,
        subject_ids={"session_id": sid, "card_id": cid},
        title="Prompt failed",
        body="The turn ended with an error: ...",
        severity="error",
        dedupe_key=f"{sid}:{prompt_id}",       # one incident = one key
    )

Stall kinds (what is stuck, and which replies make sense):

========================== ================================================ ===========================
kind                       meaning                                          default Telegram replies
========================== ================================================ ===========================
prompt_failed              a prompt/turn ended in an error                  retry, take over, dismiss
connection_lost            the provider connection dropped mid-turn         retry, take over, dismiss
recovery_blocked           a session cannot be recovered automatically      retry, take over, dismiss
hung_turn                  a turn made no progress for too long             resume, take over, dismiss
verification_failed        independent verification rejected the work       retry, take over, dismiss
acceptance_needed          a card waits for human acceptance                accept, dismiss
pr_closed                  a supervised PR closed without merging           dismiss
dispatch_failed            a dispatch failed and may be retried             retry, dismiss
completion_undeliverable   a finished run's completion could not be         dismiss
                           delivered to the card's authority
schedule_failed            a scheduled prompt failed terminally             retry, dismiss
manual_review              the post-turn evaluator asked for a human        take over, dismiss
other                      anything else that needs attention               dismiss
========================== ================================================ ===========================

``subject_ids`` may carry ``card_id``, ``session_id``, ``dispatch_id``,
``project_id``, ``pr_number``, ``watch_id`` and ``realm_id``; these become the
notification's own fields (and its in-app link). Any other key (for example
``occurrence_id``) is only listed in the body for reference, so fold such ids into
``dedupe_key``.

Deduplication: ``dedupe_key`` plus the kind identify one incident. While that
notification is unresolved a repeat call coalesces into it (its count rises; no
second notification and no second push). Once the owner resolves it, a repeat is a
no-op unless ``reopen_resolved=True``, which starts a fresh generation. The helper
never raises: a failure to notify is logged and returns ``None`` so the stalled
code path is never made worse by its own alerting.
"""

from __future__ import annotations

import logging
from datetime import datetime
from enum import StrEnum
from typing import Any

from pa.domain.notifications import (
    Notification,
    NotificationAction,
    NotificationCreate,
    NotificationPriority,
    NotificationSeverity,
    NotificationType,
    NotificationVisibility,
)
from pa.execution.progress import sanitize_text

logger = logging.getLogger(__name__)

STALL_CAPABILITY_PREFIX = "stall:"
MAX_STALL_GENERATIONS = 50


class StallKind(StrEnum):
    PROMPT_FAILED = "prompt_failed"
    CONNECTION_LOST = "connection_lost"
    RECOVERY_BLOCKED = "recovery_blocked"
    HUNG_TURN = "hung_turn"
    VERIFICATION_FAILED = "verification_failed"
    ACCEPTANCE_NEEDED = "acceptance_needed"
    PR_CLOSED = "pr_closed"
    DISPATCH_FAILED = "dispatch_failed"
    COMPLETION_UNDELIVERABLE = "completion_undeliverable"
    SCHEDULE_FAILED = "schedule_failed"
    MANUAL_REVIEW = "manual_review"
    OTHER = "other"


# Reply verbs the owner can use from Telegram (or any future channel) on a stall.
VERBS: dict[str, tuple[str, str]] = {
    # verb: (NotificationAction.kind, label)
    "retry": ("respond", "Retry"),
    "take_over": ("respond", "Take over"),
    "resume": ("respond", "Resume"),
    "accept": ("respond", "Accept"),
    "dismiss": ("resolve", "Dismiss"),
}

_DEFAULT_ACTIONS: dict[StallKind, tuple[str, ...]] = {
    StallKind.PROMPT_FAILED: ("retry", "take_over", "dismiss"),
    StallKind.CONNECTION_LOST: ("retry", "take_over", "dismiss"),
    StallKind.RECOVERY_BLOCKED: ("retry", "take_over", "dismiss"),
    StallKind.HUNG_TURN: ("resume", "take_over", "dismiss"),
    StallKind.VERIFICATION_FAILED: ("retry", "take_over", "dismiss"),
    StallKind.ACCEPTANCE_NEEDED: ("accept", "dismiss"),
    StallKind.PR_CLOSED: ("dismiss",),
    StallKind.DISPATCH_FAILED: ("retry", "dismiss"),
    StallKind.COMPLETION_UNDELIVERABLE: ("dismiss",),
    StallKind.SCHEDULE_FAILED: ("retry", "dismiss"),
    StallKind.MANUAL_REVIEW: ("take_over", "dismiss"),
    StallKind.OTHER: ("dismiss",),
}

_TYPES: dict[StallKind, NotificationType] = {
    StallKind.DISPATCH_FAILED: NotificationType.DISPATCH_FAILURE,
    StallKind.COMPLETION_UNDELIVERABLE: NotificationType.DISPATCH_FAILURE,
    StallKind.PR_CLOSED: NotificationType.PR_EVENT,
    StallKind.VERIFICATION_FAILED: NotificationType.REVIEW_EVENT,
    StallKind.ACCEPTANCE_NEEDED: NotificationType.REVIEW_EVENT,
}

_SEVERITIES: dict[str, tuple[NotificationSeverity, NotificationPriority]] = {
    "info": (NotificationSeverity.INFO, NotificationPriority.LOW),
    "warning": (NotificationSeverity.WARNING, NotificationPriority.NORMAL),
    "error": (NotificationSeverity.ERROR, NotificationPriority.HIGH),
    "critical": (NotificationSeverity.CRITICAL, NotificationPriority.URGENT),
}

_FIELD_SUBJECTS = (
    "card_id",
    "session_id",
    "dispatch_id",
    "project_id",
    "pr_number",
    "watch_id",
)


def stall_kind_of(notification: Notification) -> StallKind | None:
    """The stall kind a notification was created with, if it is a stall."""
    capability = notification.capability or ""
    if not capability.startswith(STALL_CAPABILITY_PREFIX):
        return None
    try:
        return StallKind(capability[len(STALL_CAPABILITY_PREFIX) :])
    except ValueError:
        return None


def stall_verbs(notification: Notification) -> list[str]:
    """Reply verbs offered by a stall notification (from its action ids)."""
    return [action.id for action in notification.actions if action.id in VERBS]


def severity_profile(
    severity: str | NotificationSeverity,
) -> tuple[NotificationSeverity, NotificationPriority]:
    key = str(getattr(severity, "value", severity)).lower()
    try:
        return _SEVERITIES[key]
    except KeyError:
        raise ValueError(
            f"severity must be one of {sorted(_SEVERITIES)}, got {severity!r}"
        ) from None


def _resolve_service(target: Any):
    services = getattr(target, "services", None)
    if services is not None:
        return services.get("notifications")
    return target if hasattr(target, "create") else None


def _build_actions(
    actions: list[str | NotificationAction] | None, kind: StallKind
) -> list[NotificationAction]:
    chosen = _DEFAULT_ACTIONS[kind] if actions is None else actions
    built: list[NotificationAction] = []
    for item in chosen:
        if isinstance(item, NotificationAction):
            built.append(item)
            continue
        if item not in VERBS:
            raise ValueError(f"unknown stall action {item!r}; use one of {sorted(VERBS)}")
        action_kind, label = VERBS[item]
        built.append(
            NotificationAction(
                id=item,
                kind=action_kind,  # type: ignore[arg-type]
                label=label,
                method="POST" if action_kind == "respond" else None,
            )
        )
    return built


def notify_stall(
    target: Any,
    *,
    kind: StallKind | str,
    subject_ids: dict[str, Any] | None = None,
    title: str,
    body: str = "",
    severity: str | NotificationSeverity = "error",
    dedupe_key: str,
    actions: list[str | NotificationAction] | None = None,
    principal_id: str | None = None,
    reopen_resolved: bool = False,
    expires_at: datetime | None = None,
    private_to: str | None = None,
) -> Notification | None:
    """Create (or re-raise) one deduplicated stall notification. Never raises."""
    try:
        stall = StallKind(kind)
        service = _resolve_service(target)
        if service is None:
            logger.warning("notify_stall(%s): notification service unavailable", stall)
            return None
        settings = getattr(getattr(service, "ctx", None), "settings", None) or getattr(
            target, "settings", None
        )
        subjects = dict(subject_ids or {})
        realm_id = str(
            subjects.pop("realm_id", None) or getattr(settings, "primary_realm", "default")
        )
        fields: dict[str, Any] = {}
        for name in _FIELD_SUBJECTS:
            value = subjects.pop(name, None)
            if value not in (None, ""):
                fields[name] = int(value) if name == "pr_number" else str(value)[:200]
        extras = {key: value for key, value in subjects.items() if value not in (None, "")}
        severity_value, priority = severity_profile(severity)
        text = sanitize_text(body, limit=4000) if body else ""
        if extras:
            text += ("\n\n" if text else "") + "Reference: " + ", ".join(
                f"{key}={sanitize_text(value, limit=100)}" for key, value in sorted(extras.items())
            )
        base_key = f"stall:{stall.value}:{dedupe_key}"[:480]
        store = service.store
        key = base_key
        if reopen_resolved:
            for generation in range(1, MAX_STALL_GENERATIONS + 1):
                existing = store.find_notification_by_dedup(realm_id, key)
                if existing is None or existing.resolved_at is None:
                    break
                key = f"{base_key}#{generation}"
        create = NotificationCreate(
            realm_id=realm_id,
            type=_TYPES.get(stall, NotificationType.GENERAL),
            severity=severity_value,
            priority=priority,
            title=sanitize_text(title, limit=300) or stall.value.replace("_", " "),
            body=text,
            summary=sanitize_text(body or title, limit=1000),
            capability=STALL_CAPABILITY_PREFIX + stall.value,
            actions=_build_actions(actions, stall),
            deduplication_key=key,
            expires_at=expires_at,
            **(
                {"visibility": NotificationVisibility.PRINCIPAL, "principal_id": private_to}
                if private_to
                else {}
            ),
            **fields,
        )
        return service.create(
            create,
            principal_id=principal_id or "system:stall",
        )
    except Exception:  # noqa: BLE001 - alerting must never worsen the stall
        logger.exception("notify_stall failed for kind=%s key=%s", kind, dedupe_key)
        return None


# -- ready-made adapters for the sites that have no notification context ----------


def notify_dispatch_failed(target: Any, record: Any) -> Notification | None:
    """A dispatch failed in the worker. Re-raises (rather than duplicates) per dispatch."""
    code = getattr(record, "error_code", None) or "dispatch_failed"
    retryable = bool(getattr(record, "retryable", False))
    return notify_stall(
        target,
        kind=StallKind.DISPATCH_FAILED,
        subject_ids={
            "realm_id": getattr(record, "realm_id", None),
            "card_id": getattr(record, "card_id", None),
            "session_id": getattr(record, "session_id", None),
            "dispatch_id": record.dispatch_id,
            "project_id": getattr(record, "project_id", None),
            "error_code": code,
        },
        title="Dispatch failed",
        body=f"The dispatch failed ({code}): {getattr(record, 'last_error', '') or 'no detail'}",
        severity="error",
        dedupe_key=record.dispatch_id,
        actions=["retry", "dismiss"] if retryable else ["dismiss"],
        reopen_resolved=True,
    )


def notify_completion_undeliverable(target: Any, record: Any) -> Notification | None:
    """A finished run's completion could not be delivered to the card's authority."""
    delivery_class = getattr(record, "completion_delivery_class", None) or "undeliverable"
    return notify_stall(
        target,
        kind=StallKind.COMPLETION_UNDELIVERABLE,
        subject_ids={
            "realm_id": getattr(record, "realm_id", None),
            "card_id": getattr(record, "card_id", None),
            "session_id": getattr(record, "session_id", None),
            "dispatch_id": record.dispatch_id,
            "project_id": getattr(record, "project_id", None),
            "delivery_class": delivery_class,
        },
        title="Finished work could not be delivered",
        body=(
            "The agent finished, but its completion could not be delivered to the "
            f"card's authority ({delivery_class}): {getattr(record, 'last_error', '') or 'no detail'}"
        ),
        severity="error",
        dedupe_key=f"{record.dispatch_id}:{delivery_class}",
    )


def notify_reconciliation_abandoned(target: Any, record: Any, reason: str) -> Notification | None:
    """Card reconciliation could not run, so the completion was delivered without it."""
    return notify_stall(
        target,
        kind=StallKind.RECOVERY_BLOCKED,
        subject_ids={
            "realm_id": getattr(record, "realm_id", None),
            "card_id": getattr(record, "card_id", None),
            "session_id": getattr(record, "session_id", None),
            "dispatch_id": record.dispatch_id,
            "project_id": getattr(record, "project_id", None),
        },
        title="Card reconciliation skipped",
        body=reason,
        severity="warning",
        dedupe_key=f"reconcile:{record.dispatch_id}",
    )


def notify_schedule_failed(
    target: Any, *, occurrence: dict[str, Any], owner: str | None, error: str | None
) -> Notification | None:
    """A scheduled prompt exhausted its delivery attempts."""
    return notify_stall(
        target,
        kind=StallKind.SCHEDULE_FAILED,
        subject_ids={
            "schedule_id": occurrence.get("schedule_id"),
            "occurrence_id": occurrence["id"],
        },
        title="Scheduled prompt failed",
        body=f"A scheduled prompt could not be delivered after repeated attempts: {error or 'unknown error'}",
        severity="error",
        dedupe_key=str(occurrence["id"]),
        private_to=owner,
    )
