"""Durable, idempotent fleet dispatch mutations."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
from pydantic import BaseModel, Field

from pa.core.io import atomic_write_json

logger = logging.getLogger(__name__)


class DispatchRecord(BaseModel):
    dispatch_id: str = Field(default_factory=lambda: str(uuid4()))
    mutation_id: str
    card_id: str
    realm_id: str
    card_version: str
    authority_instance_id: str
    authority_instance_name: str | None = None
    authority_url: str
    target_instance_id: str
    session_id: str | None = None
    state: str = "materialized"
    attempts: int = 0
    last_error: str | None = None
    completion_payload: dict[str, Any] | None = None
    acknowledged_at: datetime | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class DispatchStore:
    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir / "dispatch_mutations.json"
        self._records: dict[str, DispatchRecord] = {}
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
        return self._records.get(dispatch_id)

    def by_session(self, session_id: str) -> DispatchRecord | None:
        return next(
            (r for r in self._records.values() if r.session_id == session_id), None
        )

    def put(self, record: DispatchRecord) -> DispatchRecord:
        existing = self._records.get(record.dispatch_id)
        if existing and existing.mutation_id != record.mutation_id:
            raise ValueError("dispatch id already belongs to another mutation")
        record.updated_at = datetime.now(UTC)
        self._records[record.dispatch_id] = record
        self._save()
        return record

    def pending(self) -> list[DispatchRecord]:
        return [r for r in self._records.values() if r.state == "completion_pending"]


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
        if not record or record.state == "acknowledged":
            return False
        record.state = "completion_pending"
        record.completion_payload = payload
        record.last_error = None
        self.store.put(record)
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
                record.state = "acknowledged"
                record.acknowledged_at = datetime.now(UTC)
                record.last_error = None
            else:
                record.last_error = (
                    f"HTTP {response.status_code}: {response.text[:500]}"
                )
        except httpx.HTTPError as exc:
            record.last_error = str(exc)
        self.store.put(record)
