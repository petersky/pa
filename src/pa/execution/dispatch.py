"""Durable, idempotent fleet dispatch and completion mutations."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from threading import RLock
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import httpx
from pydantic import BaseModel, Field

from pa.core.io import atomic_write_json

if TYPE_CHECKING:
    from pa.core.async_runtime import AsyncRuntime

logger = logging.getLogger(__name__)

DISPATCH_STAGES = {
    "queued",
    "checking_sync",
    "materializing",
    "starting_session",
    "delivering_prompt",
    "running",
    "failed",
    "completion_pending",
    "completed",
    "cancelled",
}
TERMINAL_DISPATCH_STATES = {"failed", "completed", "cancelled"}
RECOVERABLE_DISPATCH_STATES = {
    "checking_sync",
    "materializing",
    "starting_session",
    "delivering_prompt",
    # Legacy records written by the synchronous implementation.
    "dispatching",
    "dispatched",
    "materialized",
}


class DispatchEvent(BaseModel):
    seq: int
    state: str
    message: str
    detail: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class DispatchRecord(BaseModel):
    dispatch_id: str = Field(default_factory=lambda: str(uuid4()))
    mutation_id: str
    idempotency_key: str | None = None
    request_fingerprint: str | None = None
    card_id: str | None = None
    project_id: str | None = None
    realm_id: str = "default"
    card_version: str | None = None
    card_snapshot: dict[str, Any] | None = None
    request_payload: dict[str, Any] = Field(default_factory=dict)
    principal_id: str = "user:local"
    authority_instance_id: str
    authority_instance_name: str | None = None
    authority_url: str
    target_instance_id: str
    target_instance_name: str | None = None
    session_id: str | None = None
    resume_requested: bool = False
    resume_session_id: str | None = None
    state: str = "queued"
    stage_attempts: int = 0
    attempts: int = 0
    last_error: str | None = None
    error_code: str | None = None
    recoverable: bool = True
    cancel_requested: bool = False
    prompt_acknowledged_at: datetime | None = None
    prompt_ack: dict[str, Any] | None = None
    knowledge_recorded_at: datetime | None = None
    completion_payload: dict[str, Any] | None = None
    card_disposition_payload: dict[str, Any] | None = None
    card_disposition_status: str | None = None
    card_disposition_reason: str | None = None
    card_lane_before: str | None = None
    card_lane_after: str | None = None
    acknowledged_at: datetime | None = None
    events: list[DispatchEvent] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def public_dict(self) -> dict[str, Any]:
        data = self.model_dump(
            mode="json", exclude={"request_payload", "card_snapshot"}
        )
        data["can_retry"] = self.state in {"failed", "cancelled"} and self.recoverable
        data["can_cancel"] = self.state in {
            "queued",
            "checking_sync",
            "materializing",
            "starting_session",
        }
        data["completion_outbox"] = {
            "pending": self.state == "completion_pending",
            "attempts": self.attempts,
            "last_error": self.last_error
            if self.state == "completion_pending"
            else None,
        }
        data["agent_turn"] = {
            "completed": self.completion_payload is not None,
            "stop_reason": (self.completion_payload or {}).get("stop_reason"),
        }
        data["dispatch_completion"] = {
            "completed": self.state in {"completed", "acknowledged"},
            "acknowledged_at": self.acknowledged_at.isoformat()
            if self.acknowledged_at
            else None,
        }
        data["card_completion"] = {
            "status": self.card_disposition_status or "not_requested",
            "lane_before": self.card_lane_before,
            "lane_after": self.card_lane_after,
            "reason": self.card_disposition_reason,
        }
        return data


class DispatchStore:
    """Atomic JSON ledger shared by dispatch admission, worker, and outbox."""

    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir / "dispatch_mutations.json"
        self._records: dict[str, DispatchRecord] = {}
        self._lock = RLock()
        self._load()

    def _load(self) -> None:
        try:
            payload = json.loads(self.path.read_text())
            self._records = {
                key: DispatchRecord.model_validate(value)
                for key, value in payload.items()
            }
        except OSError, ValueError:
            self._records = {}
            return
        migrated = False
        for record in self._records.values():
            if (
                record.card_id
                and record.state in {"completed", "acknowledged"}
                and record.card_disposition_status is None
            ):
                record.card_disposition_status = "legacy_unrecorded"
                record.card_disposition_reason = (
                    "This dispatch completed before the card-disposition contract; "
                    "the stored card lane was left unchanged."
                )
                migrated = True
        if migrated:
            self._save()

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(
            self.path,
            {
                key: value.model_dump(mode="json")
                for key, value in self._records.items()
            },
        )

    def get(self, dispatch_id: str) -> DispatchRecord | None:
        with self._lock:
            return self._records.get(dispatch_id)

    def list(
        self, *, target_instance_id: str | None = None, limit: int = 100
    ) -> list[DispatchRecord]:
        with self._lock:
            records = list(self._records.values())
        if target_instance_id:
            records = [
                record
                for record in records
                if record.target_instance_id == target_instance_id
            ]
        return sorted(records, key=lambda record: record.updated_at, reverse=True)[
            :limit
        ]

    def by_session(self, session_id: str) -> DispatchRecord | None:
        with self._lock:
            matches = sorted(
                (r for r in self._records.values() if r.session_id == session_id),
                key=lambda record: record.updated_at,
                reverse=True,
            )
        return next(
            (
                record
                for record in matches
                if record.state not in {"completed", "acknowledged", "cancelled"}
            ),
            matches[0] if matches else None,
        )

    def by_idempotency(
        self, target_instance_id: str, idempotency_key: str
    ) -> DispatchRecord | None:
        with self._lock:
            return next(
                (
                    record
                    for record in self._records.values()
                    if record.target_instance_id == target_instance_id
                    and record.idempotency_key == idempotency_key
                ),
                None,
            )

    def put(self, record: DispatchRecord) -> DispatchRecord:
        with self._lock:
            existing = self._records.get(record.dispatch_id)
            if existing and existing.mutation_id != record.mutation_id:
                raise ValueError("dispatch id already belongs to another mutation")
            record.updated_at = datetime.now(UTC)
            self._records[record.dispatch_id] = record
            self._save()
        return record

    def transition(
        self,
        record: DispatchRecord,
        state: str,
        message: str,
        *,
        detail: dict[str, Any] | None = None,
    ) -> DispatchRecord:
        if state not in DISPATCH_STAGES:
            raise ValueError(f"unknown dispatch state: {state}")
        record.state = state
        record.events.append(
            DispatchEvent(
                seq=(record.events[-1].seq + 1 if record.events else 1),
                state=state,
                message=message,
                detail=detail or {},
            )
        )
        return self.put(record)

    def fail(
        self,
        record: DispatchRecord,
        message: str,
        *,
        code: str = "dispatch_failed",
        recoverable: bool = True,
        detail: dict[str, Any] | None = None,
    ) -> DispatchRecord:
        record.last_error = message
        record.error_code = code
        record.recoverable = recoverable
        return self.transition(record, "failed", message, detail=detail)

    def runnable(self) -> list[DispatchRecord]:
        return [record for record in self.list(limit=1000) if record.state == "queued"]

    def pending(self) -> list[DispatchRecord]:
        return [
            record
            for record in self.list(limit=1000)
            if record.state == "completion_pending"
        ]

    def reconcile_interrupted(self) -> list[DispatchRecord]:
        """Make pre-restart work retryable without losing its identity or session."""
        reconciled: list[DispatchRecord] = []
        for record in self.list(limit=1000):
            if record.state not in RECOVERABLE_DISPATCH_STATES:
                continue
            if not record.request_payload:
                self.fail(
                    record,
                    "This legacy dispatch was interrupted before durable job details were recorded; retry it from Fleet Operations.",
                    code="orphaned_legacy_dispatch",
                )
                reconciled.append(record)
                continue
            previous_state = record.state
            record.cancel_requested = False
            record.last_error = None
            record.error_code = None
            self.transition(
                record,
                "queued",
                "Recovered interrupted dispatch after restart.",
                detail={"previous_state": previous_state},
            )
            reconciled.append(record)
        return reconciled


class DispatchWorker:
    """Runs admitted dispatches outside the initiating HTTP request."""

    def __init__(
        self,
        store: DispatchStore,
        handler: Callable[[DispatchRecord], Awaitable[None]],
        *,
        concurrency: int = 4,
        async_runtime: AsyncRuntime | None = None,
    ) -> None:
        self.store = store
        self.handler = handler
        self.concurrency = max(1, concurrency)
        self.async_runtime = async_runtime
        self._runner: asyncio.Task[None] | None = None
        self._active: dict[str, asyncio.Task[None]] = {}
        self._wake = asyncio.Event()
        self._closing = False

    def start(self) -> None:
        if not self._runner or self._runner.done():
            self._closing = False
            self._runner = asyncio.create_task(self._run())

    async def _offload(self, operation: str, call, *args, **kwargs):
        if self.async_runtime:
            return await self.async_runtime.run_blocking(
                operation, call, *args, **kwargs
            )
        return await asyncio.to_thread(call, *args, **kwargs)

    def wake(self) -> None:
        self._wake.set()

    async def close(self) -> None:
        self._closing = True
        self._wake.set()
        tasks = [task for task in self._active.values() if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._runner:
            try:
                await asyncio.wait_for(self._runner, timeout=1.0)
            except asyncio.TimeoutError:
                self._runner.cancel()

    async def _run(self) -> None:
        await self._offload(
            "dispatch.reconcile_interrupted", self.store.reconcile_interrupted
        )
        while not self._closing:
            self._active = {
                key: task for key, task in self._active.items() if not task.done()
            }
            available = self.concurrency - len(self._active)
            runnable = await self._offload(
                "dispatch.runnable_read", self.store.runnable
            )
            for record in runnable:
                if available <= 0:
                    break
                if record.dispatch_id in self._active:
                    continue
                task = asyncio.create_task(self._execute(record))
                self._active[record.dispatch_id] = task
                available -= 1
            if self._active:
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=0.2)
                except asyncio.TimeoutError:
                    pass
                continue
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass

    async def _execute(self, record: DispatchRecord) -> None:
        record.stage_attempts += 1
        await self._offload("dispatch.record_write", self.store.put, record)
        try:
            await self.handler(record)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Dispatch %s failed", record.dispatch_id)
            detail = getattr(exc, "detail", None)
            code = "dispatch_failed"
            recoverable = True
            message = str(detail or exc)
            if isinstance(detail, dict):
                code = str(detail.get("code") or code)
                recoverable = bool(detail.get("recoverable", True))
                message = str(detail.get("message") or detail)
            await self._offload(
                "dispatch.record_fail",
                self.store.fail,
                record,
                message,
                code=code,
                recoverable=recoverable,
                detail=detail if isinstance(detail, dict) else {},
            )


class CompletionOutbox:
    """Retries completion until the authoritative origin acknowledges it."""

    def __init__(
        self,
        store: DispatchStore,
        token: str,
        *,
        retry_seconds: float = 5.0,
        async_runtime: AsyncRuntime | None = None,
    ) -> None:
        self.store = store
        self.token = token
        self.retry_seconds = retry_seconds
        self.async_runtime = async_runtime
        self._task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()
        self._closing = False
        self._client: httpx.AsyncClient | None = None

    async def _offload(self, operation: str, call, *args, **kwargs):
        if self.async_runtime:
            return await self.async_runtime.run_blocking(
                operation, call, *args, **kwargs
            )
        return await asyncio.to_thread(call, *args, **kwargs)

    def _http_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    connect=3.0, read=10.0, write=10.0, pool=2.0
                ),
                limits=httpx.Limits(
                    max_connections=4, max_keepalive_connections=2
                ),
            )
        return self._client

    def start(self) -> None:
        if not self._task or self._task.done():
            self._closing = False
            self._task = asyncio.create_task(self._run())

    def queue(self, session_id: str, payload: dict[str, Any]) -> bool:
        record = self.store.by_session(session_id)
        if not record or record.state not in {"running", "completion_pending"}:
            return False
        record.completion_payload = payload
        record.last_error = None
        self.store.transition(
            record,
            "completion_pending",
            "Agent turn completed; dispatch completion queued for delivery to the authority.",
        )
        self._wake.set()
        return True

    async def drain(self, timeout: float = 5.0) -> None:
        async def wait_empty() -> None:
            while await self._offload(
                "dispatch.completion_pending_read", self.store.pending
            ):
                self._wake.set()
                await asyncio.sleep(0.05)

        try:
            await asyncio.wait_for(wait_empty(), timeout=timeout)
        except asyncio.TimeoutError:
            pass

    async def close(self, timeout: float = 5.0) -> None:
        await self.drain(timeout)
        self._closing = True
        self._wake.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=1.0)
            except asyncio.TimeoutError:
                self._task.cancel()
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _run(self) -> None:
        while not self._closing:
            pending = await self._offload(
                "dispatch.completion_pending_read", self.store.pending
            )
            if not pending:
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), self.retry_seconds)
                except asyncio.TimeoutError:
                    pass
                continue
            for record in pending:
                await self._send(record)
            await asyncio.sleep(self.retry_seconds)

    async def _send(self, record: DispatchRecord) -> None:
        record.attempts += 1
        await self._offload("dispatch.record_write", self.store.put, record)
        headers = {"Idempotency-Key": record.mutation_id}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        try:
            request = self._http_client().post(
                f"{record.authority_url.rstrip('/')}/api/fleet/dispatch/{record.dispatch_id}/complete",
                json={
                    "mutation_id": record.mutation_id,
                    "card_id": record.card_id,
                    "realm_id": record.realm_id,
                    "card_version": record.card_version,
                    "source_instance_id": record.target_instance_id,
                    "session_id": record.session_id,
                    "result": record.completion_payload or {},
                    "disposition": (record.completion_payload or {}).get(
                        "card_disposition"
                    ),
                },
                headers=headers,
            )
            response = (
                await self.async_runtime.observe(
                    "http.dispatch_completion", request, timeout=15.0
                )
                if self.async_runtime
                else await request
            )
            if response.status_code in {200, 208}:
                try:
                    acknowledgement = await self._offload(
                        "dispatch.response_json", response.json
                    )
                except ValueError:
                    acknowledgement = {}
                disposition = acknowledgement.get("card_disposition") or {}
                if isinstance(disposition, dict):
                    record.card_disposition_status = disposition.get("status")
                    record.card_disposition_reason = disposition.get("reason")
                    record.card_lane_before = disposition.get("lane_before")
                    record.card_lane_after = disposition.get("lane_after")
                record.acknowledged_at = datetime.now(UTC)
                record.last_error = None
                await self._offload(
                    "dispatch.record_complete",
                    self.store.transition,
                    record,
                    "completed",
                    "Authority acknowledged dispatch completion separately from card disposition.",
                )
            else:
                record.last_error = (
                    f"HTTP {response.status_code}: {response.text[:500]}"
                )
                await self._offload("dispatch.record_write", self.store.put, record)
        except httpx.HTTPError as exc:
            record.last_error = str(exc)
            await self._offload("dispatch.record_write", self.store.put, record)
