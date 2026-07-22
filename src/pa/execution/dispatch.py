"""Durable, idempotent fleet dispatch and completion mutations."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from threading import RLock
from typing import Any
from uuid import uuid4

import httpx
from pydantic import BaseModel, Field

from pa.core.io import atomic_write_json

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
    ) -> None:
        self.store = store
        self.handler = handler
        self.concurrency = max(1, concurrency)
        self._runner: asyncio.Task[None] | None = None
        self._active: dict[str, asyncio.Task[None]] = {}
        self._wake = asyncio.Event()
        self._closing = False

    def start(self) -> None:
        self.store.reconcile_interrupted()
        if not self._runner or self._runner.done():
            self._closing = False
            self._runner = asyncio.create_task(self._run())

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
        while not self._closing:
            self._active = {
                key: task for key, task in self._active.items() if not task.done()
            }
            available = self.concurrency - len(self._active)
            for record in self.store.runnable():
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
        self.store.put(record)
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
            self.store.fail(
                record,
                message,
                code=code,
                recoverable=recoverable,
                detail=detail if isinstance(detail, dict) else {},
            )


class CompletionOutbox:
    """Retries completion until the authoritative origin acknowledges it."""

    def __init__(
        self, store: DispatchStore, token: str, *, retry_seconds: float = 5.0
    ) -> None:
        self.store = store
        self.token = token
        self.retry_seconds = retry_seconds
        self._task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()
        self._closing = False

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
            "Completion queued for asynchronous delivery to the authority.",
        )
        self._wake.set()
        return True

    async def drain(self, timeout: float = 5.0) -> None:
        async def wait_empty() -> None:
            while self.store.pending():
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

    async def _run(self) -> None:
        while not self._closing:
            pending = self.store.pending()
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
        self.store.put(record)
        headers = {"Idempotency-Key": record.mutation_id}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    f"{record.authority_url.rstrip('/')}/api/fleet/dispatch/{record.dispatch_id}/complete",
                    json={
                        "mutation_id": record.mutation_id,
                        "card_id": record.card_id,
                        "realm_id": record.realm_id,
                        "card_version": record.card_version,
                        "source_instance_id": record.target_instance_id,
                        "session_id": record.session_id,
                        "result": record.completion_payload or {},
                    },
                    headers=headers,
                )
            if response.status_code in {200, 208}:
                record.acknowledged_at = datetime.now(UTC)
                record.last_error = None
                self.store.transition(
                    record,
                    "completed",
                    "Authority acknowledged remote completion.",
                )
            else:
                record.last_error = (
                    f"HTTP {response.status_code}: {response.text[:500]}"
                )
                self.store.put(record)
        except httpx.HTTPError as exc:
            record.last_error = str(exc)
            self.store.put(record)
