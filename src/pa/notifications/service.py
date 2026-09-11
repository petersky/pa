"""Application service for idempotent notification and interaction lifecycles."""

from __future__ import annotations

import asyncio
import inspect
import json
import threading
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from jsonschema import ValidationError as JsonSchemaValidationError
from jsonschema import validate as validate_json_schema

from pa.core.context import AppContext
from pa.domain.notifications import (
    ContinuationTransfer,
    ContinuationTransferRequest,
    DeliveryState,
    InteractionKind,
    InteractionResponse,
    InteractionState,
    Notification,
    NotificationCreate,
    NotificationVisibility,
    NotificationVersionConflict,
)
from pa.execution.dispatch import TERMINAL_DISPATCH_STATES
from pa.execution.progress import sanitize_text

DeliveryHandler = Callable[[InteractionResponse], Awaitable[None] | None]


class NotificationConflict(RuntimeError):
    def __init__(
        self, code: str, message: str, *, notification: Notification | None = None
    ):
        super().__init__(message)
        self.code = code
        self.notification = notification


def can_view_notification(
    notification: Notification, principal_id: str, realms: set[str]
) -> bool:
    if notification.realm_id not in realms:
        return False
    return not (
        notification.visibility == NotificationVisibility.PRINCIPAL
        and notification.principal_id != principal_id
    )


class NotificationService:
    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx
        self.store = ctx.store
        self._delivery_handlers: dict[str, DeliveryHandler] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._mutation_locks: dict[str, threading.RLock] = {}
        self._create_lock = threading.RLock()
        self._expire_mono: dict[str, float] = {}

    def register_delivery_handler(
        self, notification_id: str, handler: DeliveryHandler
    ) -> None:
        self._delivery_handlers[notification_id] = handler

    def unregister_delivery_handler(self, notification_id: str) -> None:
        self._delivery_handlers.pop(notification_id, None)

    async def _save_async(
        self, notification: Notification, *, principal_id: str,
        previous: Notification | None = None,
    ) -> None:
        # Response stages may overlap read/ack/coalescing writes. Merge only
        # those metadata fields: never rebase a changed answer, route, contract,
        # expiry, resolution, or delivery state. Transfers retain strict CAS.
        metadata = {"read_at", "acknowledged_at", "coalesced_count"}
        excluded = metadata | {"version", "updated_at", "idempotency_keys"}
        for _ in range(8):
            try:
                await asyncio.to_thread(
                    self.store.save_notification,
                    notification,
                    principal_id=principal_id,
                    instance_id=self.ctx.settings.instance_id,
                    expected_version=notification.version - 1,
                )
                return
            except NotificationVersionConflict as exc:
                latest = await asyncio.to_thread(
                    self.store.get_notification, notification.id,
                    realm_id=notification.realm_id,
                )
                if (
                    previous is None or latest is None
                    or latest.model_dump(exclude=excluded) != previous.model_dump(exclude=excluded)
                ):
                    raise NotificationConflict("notification_version_conflict", str(exc)) from exc
                new_keys = [key for key in notification.idempotency_keys if key not in previous.idempotency_keys]
                for field in metadata:
                    setattr(notification, field, getattr(latest, field))
                notification.idempotency_keys = list(dict.fromkeys([*latest.idempotency_keys, *new_keys]))[-128:]
                notification.version = latest.version + 1
                notification.updated_at = datetime.now(UTC)
                previous = latest
        raise NotificationConflict("notification_version_conflict", "Notification is changing; retry the recorded response delivery")

    async def _deliver_expiry(self, notification: Notification) -> None:
        handler = self._delivery_handlers.get(notification.id)
        if not handler or not notification.interaction:
            return
        response = InteractionResponse(
            idempotency_key=f"expiry-delivery:{notification.id}", cancel=True
        )
        try:
            result = handler(response)
            if inspect.isawaitable(result):
                await result
            notification.interaction.delivery_attempts += 1
            notification.interaction.delivery_error = None
            notification.interaction.delivered_at = datetime.now(UTC)
            notification.delivery_state = DeliveryState.DELIVERED
            notification.delivery_updated_at = notification.interaction.delivered_at
        except Exception as exc:  # noqa: BLE001 - protocol handlers are provider-owned
            notification.interaction.delivery_attempts += 1
            notification.interaction.delivery_error = (
                "Delivery to the owning request failed"
                if notification.interaction.sensitive
                else sanitize_text(exc, limit=1000)
            )
            notification.delivery_state = DeliveryState.FAILED
            notification.delivery_updated_at = datetime.now(UTC)
        notification.version += 1
        notification.updated_at = datetime.now(UTC)
        await self._save_async(notification, principal_id="system:expiry")
        self._publish(notification)

    def create(
        self,
        data: NotificationCreate,
        *,
        principal_id: str,
        instance_id: str | None = None,
    ) -> Notification:
        with self._create_lock:
            return self._create_unlocked(
                data, principal_id=principal_id, instance_id=instance_id
            )

    def _create_unlocked(
        self,
        data: NotificationCreate,
        *,
        principal_id: str,
        instance_id: str | None = None,
    ) -> Notification:
        instance_id = instance_id or self.ctx.settings.instance_id
        if data.deduplication_key:
            for _ in range(8):
                existing = self.store.find_notification_by_dedup(
                    data.realm_id, data.deduplication_key
                )
                if existing is None:
                    break
                if existing.resolved_at is not None:
                    return existing
                existing.coalesced_count += 1
                existing.updated_at = datetime.now(UTC)
                existing.version += 1
                try:
                    self.store.save_notification(
                        existing, principal_id=principal_id, instance_id=instance_id,
                        expected_version=existing.version - 1,
                    )
                except NotificationVersionConflict:
                    continue
                return existing
            else:
                raise NotificationConflict(
                    "notification_version_conflict", "Notification is changing; retry coalescing"
                )
        payload = data.model_dump(exclude={"id"})
        payload.update(
            id=(
                data.id
                or (
                    str(
                        uuid5(
                            NAMESPACE_URL,
                            f"pa-notification:{data.realm_id}:{data.deduplication_key}",
                        )
                    )
                    if data.deduplication_key
                    else uuid4().hex
                )
            ),
            source_instance_id=data.source_instance_id or instance_id,
            source_instance_name=data.source_instance_name
            or self.ctx.settings.instance_name,
            owner_instance_id=data.owner_instance_id or instance_id,
            owner_url=(
                data.owner_url
                or (
                    self.ctx.settings.instance_url
                    if (data.owner_instance_id or instance_id) == instance_id
                    else None
                )
                or None
            ),
            delivery_state=(
                DeliveryState.LOCAL
                if (data.owner_instance_id or instance_id) == instance_id
                else DeliveryState.REMOTE
            ),
        )
        notification = Notification(**payload)
        self.store.save_notification(
            notification, principal_id=principal_id, instance_id=instance_id, expected_version=0
        )
        self._publish(notification)
        return notification

    def get_authorized(
        self, notification_id: str, *, principal_id: str, realms: set[str]
    ) -> Notification:
        notification = self.store.get_notification(notification_id)
        if not notification or not can_view_notification(
            notification, principal_id, realms
        ):
            raise KeyError(notification_id)
        return notification

    def list_inbox(
        self,
        *,
        principal_id: str,
        realms: set[str],
        realm_id: str,
        **filters: Any,
    ) -> tuple[list[Notification], int]:
        records = self.list_authorized(
            principal_id=principal_id,
            realms=realms,
            realm_id=realm_id,
            **filters,
        )
        outstanding_count = 0
        if realm_id in realms:
            outstanding_count = self.store.count_outstanding_notifications(
                realm_id=realm_id, principal_id=principal_id
            )
        return records, outstanding_count

    def list_authorized(
        self,
        *,
        principal_id: str,
        realms: set[str],
        realm_id: str,
        **filters: Any,
    ) -> list[Notification]:
        if realm_id not in realms:
            return []
        return self.store.list_notifications(
            realm_id=realm_id, principal_id=principal_id, **filters
        )

    def _mutate(
        self,
        notification: Notification,
        *,
        principal_id: str,
        idempotency_key: str,
        mutation: Callable[[Notification, datetime], None],
    ) -> Notification:
        lock = self._mutation_locks.setdefault(notification.id, threading.RLock())
        with lock:
            for _ in range(8):
                current = self.store.get_notification(
                    notification.id, realm_id=notification.realm_id
                )
                if not current:
                    raise KeyError(notification.id)
                if idempotency_key in current.idempotency_keys:
                    return current
                now = datetime.now(UTC)
                before = current.model_dump(mode="python")
                mutation(current, now)
                if current.model_dump(mode="python") == before:
                    return current
                current.idempotency_keys = [
                    *current.idempotency_keys[-126:],
                    idempotency_key,
                ]
                current.version += 1
                current.updated_at = now
                try:
                    self.store.save_notification(
                        current,
                        principal_id=principal_id,
                        instance_id=self.ctx.settings.instance_id,
                        expected_version=current.version - 1,
                    )
                except NotificationVersionConflict:
                    continue
                self._publish(current)
                return current
        raise NotificationConflict("notification_version_conflict", "Notification is changing; retry mutation")

    def mark_read(
        self, notification: Notification, *, principal_id: str, idempotency_key: str
    ) -> Notification:
        return self._mutate(
            notification,
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            mutation=lambda item, now: setattr(item, "read_at", item.read_at or now),
        )

    def acknowledge(
        self, notification: Notification, *, principal_id: str, idempotency_key: str
    ) -> Notification:
        return self._mutate(
            notification,
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            mutation=lambda item, now: setattr(
                item, "acknowledged_at", item.acknowledged_at or now
            ),
        )

    def resolve(
        self, notification: Notification, *, principal_id: str, idempotency_key: str
    ) -> Notification:
        def resolve_item(item: Notification, now: datetime) -> None:
            if item.interaction and item.interaction.state in {
                InteractionState.OUTSTANDING,
                InteractionState.ANSWERED,
                InteractionState.DELIVERY_PENDING,
            }:
                raise NotificationConflict(
                    "interaction_response_required",
                    "Answer or cancel the interaction so its correlated protocol request is completed",
                    notification=item,
                )
            item.resolved_at = item.resolved_at or now

        return self._mutate(
            notification,
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            mutation=resolve_item,
        )

    def supersede(
        self, notification: Notification, *, principal_id: str, idempotency_key: str
    ) -> Notification:
        def supersede_item(item: Notification, now: datetime) -> None:
            if item.interaction and item.interaction.state in {
                InteractionState.OUTSTANDING,
                InteractionState.ANSWERED,
                InteractionState.DELIVERY_PENDING,
                InteractionState.FAILED,
            }:
                item.interaction.state = InteractionState.SUPERSEDED
            item.resolved_at = item.resolved_at or now

        return self._mutate(
            notification,
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            mutation=supersede_item,
        )

    async def expire_due(self, *, realm_id: str, limit: int = 200) -> int:
        now = datetime.now(UTC)
        expired = 0
        records = await asyncio.to_thread(
            self.store.list_notifications,
            realm_id=realm_id,
            resolved=False,
            limit=limit,
        )
        for notification in records:
            if not notification.expires_at or notification.expires_at > now:
                continue
            if (
                notification.owner_instance_id
                and notification.owner_instance_id != self.ctx.settings.instance_id
            ):
                continue

            lock = self._locks.setdefault(notification.id, asyncio.Lock())
            async with lock:
                current = await asyncio.to_thread(
                    self.store.get_notification,
                    notification.id,
                    realm_id=realm_id,
                )
                if not current or current.resolved_at is not None:
                    continue

                def expire_item(item: Notification, stamp: datetime) -> None:
                    if item.interaction:
                        item.interaction.state = InteractionState.EXPIRED
                    item.resolved_at = item.resolved_at or stamp

                current = await asyncio.to_thread(
                    self._mutate,
                    current,
                    principal_id="system:expiry",
                    idempotency_key=(
                        f"expire:{current.id}:{current.expires_at.isoformat()}"
                    ),
                    mutation=expire_item,
                )
                await self._deliver_expiry(current)
                expired += 1
        return expired

    def prune(
        self, *, realm_id: str, retention_days: int = 90, max_records: int = 10_000
    ) -> int:
        cutoff = datetime.now(UTC) - timedelta(days=max(1, retention_days))
        candidates: list[Notification] = []
        offset = 0
        while offset < max_records + 200:
            records = self.store.list_notifications(
                realm_id=realm_id, limit=200, offset=offset
            )
            if not records:
                break
            for item in records:
                if item.outstanding:
                    continue
                if item.updated_at >= cutoff and offset < max_records:
                    continue
                candidates.append(item)
            offset += len(records)
        removed = 0
        for item in candidates:
            if self.store.delete_notification(
                item.id,
                realm_id=realm_id,
                principal_id="system:retention",
                instance_id=self.ctx.settings.instance_id,
            ):
                removed += 1
        return removed

    @staticmethod
    def _validated_response(
        notification: Notification, response: InteractionResponse
    ) -> Any:
        interaction = notification.interaction
        if not interaction:
            raise NotificationConflict(
                "not_interactive",
                "Notification has no response contract",
                notification=notification,
            )
        if response.retry:
            if (
                interaction.state not in {InteractionState.FAILED, InteractionState.ANSWERED, InteractionState.DELIVERY_PENDING}
                or interaction.response is None
            ):
                raise NotificationConflict(
                    "delivery_retry_not_available",
                    "There is no failed response delivery to retry",
                    notification=notification,
                )
            return interaction.response
        if response.cancel:
            if not interaction.allow_cancel:
                raise NotificationConflict(
                    "cancellation_not_allowed",
                    "This request cannot be cancelled",
                    notification=notification,
                )
            return {"cancelled": True}
        if response.choice_id is not None or response.choice_ids is not None:
            multiple = (interaction.response_schema or {}).get("type") == "array"
            if multiple != (response.choice_ids is not None):
                raise NotificationConflict("invalid_selection_mode", "Use the request's single or multiple selection contract", notification=notification)
            ids = response.choice_ids if multiple else [response.choice_id]
            available = {choice.id: choice for choice in interaction.choices}
            if len(set(ids)) != len(ids) or any(key not in available for key in ids):
                raise NotificationConflict("invalid_choice", "The selected choice is not available or is duplicated", notification=notification)
            values = [available[key].value for key in ids]
            if multiple:
                try:
                    validate_json_schema(values, interaction.response_schema)
                except JsonSchemaValidationError as exc:
                    raise NotificationConflict("response_validation_failed", "Selection did not match the required schema", notification=notification) from exc
                return {"choice_ids": ids, "values": values}
            return {"choice_id": ids[0], "value": values[0]}
        value = response.fields if response.fields is not None else response.value
        if (
            not interaction.allow_freeform
            and not interaction.response_schema
        ):
            raise NotificationConflict(
                "freeform_not_allowed",
                "This request requires a listed choice",
                notification=notification,
            )
        if interaction.response_schema:
            try:
                validate_json_schema(value, interaction.response_schema)
            except JsonSchemaValidationError as exc:
                raise NotificationConflict(
                    "response_validation_failed",
                    (
                        "Response did not match the required schema"
                        if interaction.sensitive
                        else sanitize_text(exc.message, limit=500)
                    ),
                    notification=notification,
                ) from exc
        return value

    @staticmethod
    def _delivery_response(
        interaction, response: InteractionResponse
    ) -> InteractionResponse:
        """Rebuild the exact recorded answer for a delivery-only retry."""
        if not response.retry:
            return response
        stored = interaction.response
        key = response.idempotency_key
        if stored == {"cancelled": True}:
            return InteractionResponse(idempotency_key=key, cancel=True)
        if isinstance(stored, dict) and "choice_ids" in stored:
            return InteractionResponse(idempotency_key=key, choice_ids=stored["choice_ids"])
        if isinstance(stored, dict) and "choice_id" in stored:
            return InteractionResponse(
                idempotency_key=key, choice_id=str(stored["choice_id"])
            )
        if interaction.response_schema and isinstance(stored, dict):
            return InteractionResponse(idempotency_key=key, fields=stored)
        return InteractionResponse(idempotency_key=key, value=stored)

    def _validate_continuation_target(
        self, notification: Notification, transfer: ContinuationTransferRequest,
        *, principal_id: str,
    ) -> None:
        """Fail closed on missing or inconsistent local ownership evidence."""
        def reject(message: str) -> None:
            raise NotificationConflict("invalid_continuation_target", message)

        local = self.ctx.settings.instance_id
        ledger = self.ctx.services.get("dispatch_store")
        manager = self.ctx.services.get("instance_agent")
        if not ledger or not manager or notification.owner_instance_id != local:
            reject("Repair requires the local notification owner and dispatch ledger")
        if (notification.session_id, notification.dispatch_id) != (
            transfer.expected_session_id, transfer.expected_dispatch_id
        ):
            reject("Original session or dispatch does not match")
        if transfer.successor_session_id == notification.session_id:
            reject("Successor must be a different session")
        if not notification.card_id or not notification.project_id:
            reject("Explicit card and project provenance is required")
        old = self.store.get_session(transfer.expected_session_id)
        successor = self.store.get_session(transfer.successor_session_id)
        for session, dispatch_id in (
            (old, transfer.expected_dispatch_id),
            (successor, transfer.successor_dispatch_id),
        ):
            record = ledger.get(dispatch_id)
            if not session or not record:
                reject("Session or dispatch evidence is missing")
            if (
                session.dispatch_id != dispatch_id
                or session.lifecycle_owner != "dispatch"
                or record.session_id != session.id
                or session.origin_instance_id != local
                or session.authority_instance_id != local
                or record.authority_instance_id != local
                or record.target_instance_id != local
                or session.realm_id != notification.realm_id
                or record.realm_id != notification.realm_id
                or session.card_id != notification.card_id
                or record.card_id != notification.card_id
                or session.project_id != notification.project_id
                or record.project_id != notification.project_id
                or session.principal_id != principal_id
                or record.principal_id != principal_id
                or (notification.principal_id and notification.principal_id != principal_id)
            ):
                reject("Authority, realm, card, project, principal, or dispatch binding differs")
        old_record = ledger.get(transfer.expected_dispatch_id)
        if old_record.state not in TERMINAL_DISPATCH_STATES:
            reject("Original dispatch is not terminal")
        old_runtime = manager.get(old.id)
        if old_runtime is not None and not old_runtime._closed:
            reject("Original session has a live runtime")
        if old.status not in {"closed", "recovery_blocked"} or not (
            not old.external_session_id
            or (old.recovery_json.get("blocked") and old.recovery_json.get("context_lost"))
        ):
            reject("Original session is not demonstrably unrecoverable")
        binding = successor.execution_binding
        expected_binding = {
            "execution_card_id": notification.card_id,
            "execution_project_id": notification.project_id,
            "dispatch_id": transfer.successor_dispatch_id,
            "realm_id": notification.realm_id,
            "principal_id": principal_id,
            "origin_instance_id": local,
        }
        if any(binding.get(key) != value for key, value in expected_binding.items()):
            reject("Successor immutable execution binding is missing or mismatched")
        successor_record = ledger.get(transfer.successor_dispatch_id)
        if successor_record.state not in {"running", "completed"} or not successor_record.recoverable:
            reject("Successor dispatch is not recoverable")
        runtime = manager.get(successor.id)
        live = runtime is not None and not runtime._closed and runtime.connected
        if (
            successor.status in {"closed", "recovery_blocked"}
            or successor.archived_at is not None
            or successor.config_json.get("execution_context_boundary")
            or (runtime is not None and getattr(runtime, "_execution_boundary_reserved", False))
            or successor.recovery_json.get("blocked")
            or successor.recovery_json.get("context_lost")
            or (not live and not successor.external_session_id)
        ):
            reject("Successor has no recoverable provider context")
        interaction = notification.interaction
        if interaction.response_principal and interaction.response_principal != principal_id:
            reject("Recorded response belongs to another principal")
        prompt_id = interaction.continuation_prompt_id or (
            f"notification-response:{notification.id}:{interaction.request_id}"
        )
        durable = old.config_json.get("durable_runtime") or {}
        admissions = list(durable.get("queued_prompts") or [])
        if durable.get("in_flight"):
            admissions.append(durable["in_flight"])
        if self.store.get_prompt_acceptance(old.id, prompt_id) or any(
            item.get("id") == prompt_id for item in admissions
        ):
            reject("Original session already admitted this response; delivery is ambiguous")

    async def transfer_continuation(
        self, notification: Notification, request: ContinuationTransferRequest,
        *, principal_id: str, realms: set[str],
    ) -> Notification:
        """Repair routing once, preserving the original interaction and any answer."""
        lock = self._locks.setdefault(notification.id, asyncio.Lock())
        async with lock:
            current = self.get_authorized(notification.id, principal_id=principal_id, realms=realms)
            receipt = current.continuation_transfer
            if receipt:
                if (
                    receipt.actor_principal == principal_id
                    and {key: getattr(receipt, key) for key in type(request).model_fields}
                    == request.model_dump()
                ):
                    return current
                raise NotificationConflict("continuation_already_transferred", "This notification already has an explicit successor")
            if request.idempotency_key in current.idempotency_keys:
                raise NotificationConflict("idempotency_conflict", "Key already used by another notification operation")
            if current.version != request.expected_version:
                raise NotificationConflict("notification_version_conflict", "Notification changed; inspect its current state")
            interaction = current.interaction
            now = datetime.now(UTC)
            if (
                not interaction
                or interaction.kind != InteractionKind.MCP_OPERATOR_INPUT
                or interaction.protocol_method != "pa/report_dispatch_progress.operator_input"
                or interaction.continuation_mode != "prompt"
                or current.id in self._delivery_handlers
                or interaction.state not in {InteractionState.OUTSTANDING, InteractionState.FAILED}
                or current.resolved_at is not None
                or interaction.delivered_at is not None
                or (current.expires_at and current.expires_at <= now)
                or (interaction.deadline and interaction.deadline <= now)
            ):
                raise NotificationConflict("continuation_not_transferable", "Only pending MCP operator-input prompt continuations can be repaired")
            manager = self.ctx.services.get("instance_agent")
            if manager is None:
                raise NotificationConflict("invalid_continuation_target", "Session manager is unavailable")
            from pa.instance.agent_session import SessionAdmissionInProgress

            try:
                async with manager.continuation_transfer_guard(
                    request.expected_session_id, request.successor_session_id
                ):
                    self._validate_continuation_target(current, request, principal_id=principal_id)
                    current.continuation_transfer = ContinuationTransfer(
                        **request.model_dump(), actor_principal=principal_id,
                        authority_instance_id=self.ctx.settings.instance_id, transferred_at=now,
                    )
                    current.version += 1
                    current.updated_at = now
                    await self._save_async(current, principal_id=principal_id)
            except SessionAdmissionInProgress as exc:
                raise NotificationConflict("session_admission_conflict", str(exc)) from exc
            self._publish(current)
            return current

    async def respond(
        self,
        notification: Notification,
        response: InteractionResponse,
        *,
        principal_id: str,
    ) -> Notification:
        lock = self._locks.setdefault(notification.id, asyncio.Lock())
        async with lock:
            current = await asyncio.to_thread(
                self.store.get_notification,
                notification.id,
                realm_id=notification.realm_id,
            )
            if not current:
                raise KeyError(notification.id)
            if current.continuation_transfer and principal_id != current.continuation_transfer.actor_principal:
                raise NotificationConflict("response_principal_mismatch", "Response must belong to the authorized continuation principal")
            if current.continuation_transfer and response.idempotency_key == current.continuation_transfer.idempotency_key:
                raise NotificationConflict("idempotency_conflict", "Key already used for continuation transfer")
            if response.idempotency_key in current.idempotency_keys:
                if not response.retry and current.interaction and self._validated_response(current, response) != current.interaction.response:
                    raise NotificationConflict("idempotency_conflict", "This response key already recorded a different answer", notification=current)
                return current
            interaction = current.interaction
            if not interaction:
                raise NotificationConflict(
                    "not_interactive",
                    "Notification has no interaction request",
                    notification=current,
                )
            if interaction.state not in {
                InteractionState.OUTSTANDING,
                InteractionState.ANSWERED,
                InteractionState.DELIVERY_PENDING,
                InteractionState.FAILED,
            }:
                code = (
                    "interaction_expired"
                    if interaction.state == InteractionState.EXPIRED
                    else "interaction_already_resolved"
                )
                raise NotificationConflict(
                    code,
                    f"The request is already {interaction.state.value}",
                    notification=current,
                )
            if interaction.deadline and interaction.deadline <= datetime.now(UTC):
                interaction.state = InteractionState.EXPIRED
                current.resolved_at = datetime.now(UTC)
                current.version += 1
                current.updated_at = datetime.now(UTC)
                await self._save_async(current, principal_id="system:expiry")
                self._publish(current)
                await self._deliver_expiry(current)
                raise NotificationConflict(
                    "interaction_expired",
                    "The request has expired",
                    notification=current,
                )
            value = self._validated_response(current, response)
            if (
                interaction.response is not None
                and interaction.response != value
            ):
                raise NotificationConflict(
                    "delivery_retry_mismatch",
                    "Retry the same response because the previous delivery may have partially succeeded",
                    notification=current,
                )
            previous = current.model_copy(deep=True)
            if interaction.continuation_mode == "prompt" and not interaction.continuation_prompt_id:
                interaction.continuation_prompt_id = f"notification-response:{current.id}:{interaction.request_id}"
            now = datetime.now(UTC)
            delivery_response = self._delivery_response(interaction, response)
            if not response.retry:
                interaction.response = value
                interaction.response_principal = principal_id
                interaction.responded_at = now
                interaction.response_summary = (
                    "Sensitive response recorded"
                    if interaction.sensitive
                    else sanitize_text(value, limit=500)
                )
            interaction.state = (
                InteractionState.CANCELLED
                if delivery_response.cancel
                else InteractionState.ANSWERED
            )
            current.delivery_state = DeliveryState.PENDING
            current.delivery_updated_at = now
            current.idempotency_keys = [
                *current.idempotency_keys[-126:],
                response.idempotency_key,
            ]
            current.version += 1
            current.updated_at = now
            await self._save_async(current, principal_id=principal_id, previous=previous)
            self._publish(current)
            if not delivery_response.cancel:
                previous = current.model_copy(deep=True)
                interaction.state = InteractionState.DELIVERY_PENDING
                current.version += 1
                current.updated_at = datetime.now(UTC)
                await self._save_async(current, principal_id="system:delivery", previous=previous)
                self._publish(current)
            previous = current.model_copy(deep=True)
            try:
                await self._deliver(current, delivery_response)
            except Exception as exc:
                interaction.delivery_attempts += 1
                interaction.delivery_error = (
                    "Delivery to the owning request failed"
                    if interaction.sensitive
                    else sanitize_text(exc, limit=1000)
                )
                interaction.state = InteractionState.FAILED
                current.delivery_state = DeliveryState.FAILED
                current.delivery_updated_at = datetime.now(UTC)
                current.version += 1
                current.updated_at = datetime.now(UTC)
                await self._save_async(current, principal_id="system:delivery", previous=previous)
                self._publish(current)
                raise NotificationConflict(
                    "delivery_failed", interaction.delivery_error, notification=current
                ) from exc
            interaction.delivery_attempts += 1
            interaction.delivery_error = None
            interaction.delivered_at = datetime.now(UTC)
            interaction.state = (
                InteractionState.CANCELLED
                if delivery_response.cancel
                else InteractionState.DELIVERED
            )
            current.delivery_state = DeliveryState.DELIVERED
            current.delivery_updated_at = interaction.delivered_at
            current.resolved_at = current.resolved_at or interaction.delivered_at
            current.version += 1
            current.updated_at = interaction.delivered_at
            await self._save_async(current, principal_id="system:delivery", previous=previous)
            self._publish(current)
            return current

    async def _deliver(
        self, notification: Notification, response: InteractionResponse
    ) -> None:
        handler = self._delivery_handlers.get(notification.id)
        if notification.continuation_transfer and (
            handler is not None
            or notification.interaction is None
            or notification.interaction.kind != InteractionKind.MCP_OPERATOR_INPUT
            or notification.interaction.protocol_method != "pa/report_dispatch_progress.operator_input"
            or notification.interaction.continuation_mode != "prompt"
        ):
            raise RuntimeError("Transferred continuations cannot deliver provider-owned requests")
        if handler:
            result = handler(response)
            if inspect.isawaitable(result):
                await result
            return
        interaction = notification.interaction
        if not interaction:
            return
        if interaction.protocol_method == "pa/collaboration_mode_approval":
            collaboration = self.ctx.services.get("collaboration")
            if collaboration is None:
                collaboration = self.ctx.services.get("collaboration_service")
            handler = getattr(collaboration, "handle_mode_approval", None)
            if not callable(handler):
                raise RuntimeError("Collaboration approval service is unavailable")
            result = handler(notification, response)
            if inspect.isawaitable(result):
                await result
            return
        if interaction.continuation_mode == "none":
            return
        manager = self.ctx.services.get("instance_agent")
        if (
            interaction.continuation_mode == "prompt"
            and manager
            and notification.session_id
        ):
            destination_session_id = notification.session_id
            transfer = notification.continuation_transfer
            if transfer:
                self._validate_continuation_target(
                    notification, transfer, principal_id=transfer.actor_principal
                )
                destination_session_id = transfer.successor_session_id
            prompt_id = interaction.continuation_prompt_id
            # A crash after durable queue admission must never enqueue a new turn.
            if prompt_id and await asyncio.to_thread(
                self.store.get_prompt_acceptance, destination_session_id, prompt_id
            ):
                return
            runtime = manager.get(destination_session_id)
            if runtime is None:
                runtime = await manager.recover_session(destination_session_id)
            if transfer:
                self._validate_continuation_target(
                    notification, transfer, principal_id=transfer.actor_principal
                )
            envelope = {
                "schema": "pa.interaction-response/v1",
                "request_id": interaction.request_id,
                "notification_id": notification.id,
                "session_id": notification.session_id,
                "dispatch_id": notification.dispatch_id,
                "response": interaction.response,
            }
            runtime.enqueue(
                "A correlated user response was recorded. Apply it only to the originating "
                "request, preserve its authorization scope, revalidate external state, and "
                "report the result without quoting private response values.\n"
                + json.dumps(envelope, ensure_ascii=False, separators=(",", ":")),
                card_id=notification.card_id,
                project_id=notification.project_id,
                principal_id=interaction.response_principal,
                source="notification-response",
                prompt_id=prompt_id,
            )
            drain = getattr(runtime, "_drain_transcripts", None)
            if callable(drain):
                pending = drain()
                if inspect.isawaitable(pending):
                    await pending
            return
        raise RuntimeError("The owning protocol request is no longer recoverable")

    def _publish(self, notification: Notification) -> None:
        broker = self.ctx.services.get("live_updates")
        if broker:
            broker.publish(
                notification.realm_id,
                {
                    "type": "notifications-changed",
                    "notification_id": notification.id,
                    "version": notification.version,
                },
            )
