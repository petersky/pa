"""Durable, ref-preserving recovery for referenced sync objects."""

from __future__ import annotations

import asyncio
import base64
import json
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from pa.core.io import atomic_write_json
from pa.domain.models import CardEvent, SyncCommit
from pa.sync.event_log import EventHistoryObjectError
from pa.sync.object_store import object_hash

MAX_RECOVERY_PEERS = 4
MAX_RECOVERY_OBJECTS = 20_000


class SyncRecovery:
    def __init__(self, settings, engine, projection_rebuilder) -> None:
        self.settings = settings
        self.engine = engine
        self.log = engine.log
        self.store = engine.store
        self.projection_rebuilder = projection_rebuilder
        self.path = settings.data_dir / "sync_recovery.json"
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self.state: dict[str, Any] = self._load()

    def _load(self) -> dict[str, Any]:
        try:
            value = json.loads(self.path.read_text())
            return value if isinstance(value, dict) else {}
        except (OSError, ValueError):
            return {}

    def _save(self, **updates: Any) -> None:
        self.state.update(updates, updated_at=datetime.now(UTC).isoformat())
        atomic_write_json(self.path, {"version": 1, **self.state}, mode=0o600)

    def degraded(self) -> bool:
        return self.state.get("state") in {"recovering", "unrecoverable"}

    def mark_healthy(self) -> None:
        self._save(state="healthy", code=None, object_kind=None, object_hash=None)

    def public(self) -> dict[str, Any]:
        allowed = {"state", "realm_id", "object_kind", "object_hash", "code", "attempts", "updated_at"}
        result = {key: value for key, value in self.state.items() if key in allowed}
        if self.degraded():
            result["next_steps"] = [
                "Restore connectivity to an authenticated healthy realm peer.",
                "Retry supported sync recovery/reconcile after the peer is available.",
                "Do not edit refs or object files manually.",
            ]
        return result

    def start(self, failures: list[tuple[str, EventHistoryObjectError]]) -> asyncio.Task:
        if self._task and not self._task.done():
            return self._task
        self._task = asyncio.create_task(self.recover(failures), name="sync-object-recovery")
        return self._task

    async def recover(self, failures: list[tuple[str, EventHistoryObjectError]]) -> bool:
        async with self._lock:
            for realm_id, failure in failures:
                self._save(state="recovering", realm_id=realm_id, code=failure.code,
                           object_kind=failure.diagnostic.get("object_kind"),
                           object_hash=failure.diagnostic.get("object_hash"), attempts=[])
                if not await self._recover_realm(realm_id):
                    self._save(state="unrecoverable")
                    return False
            self._save(state="healthy", code=None, object_kind=None, object_hash=None)
            return True

    async def retry(self, realm_id: str) -> bool:
        async with self._lock:
            self._save(state="recovering", realm_id=realm_id, attempts=[])
            recovered = await self._recover_realm(realm_id)
            self._save(state="healthy" if recovered else "unrecoverable")
            return recovered

    async def _recover_realm(self, realm_id: str) -> bool:
        head = self.log.get_head(realm_id)
        if not head:
            return True
        routes = self.engine.peer_table.prefer_same_zone(realm_id, self.settings.zone)[:MAX_RECOVERY_PEERS]
        attempts: list[dict[str, str]] = []
        for route in routes:
            peer = route.target_instance_id or "configured_peer"
            try:
                async with asyncio.timeout(20.0):
                    fetched = await self._fetch_chain(route.target_url, realm_id, head)
                for expected, raw in fetched.items():
                    self.store.repair(expected, raw)
                self.log.ensure_indexed(realm_id, head)
                self.projection_rebuilder(realm_id)
                attempts.append({"peer": peer, "result": "recovered"})
                self._save(attempts=attempts)
                return True
            except Exception as exc:
                attempts.append({"peer": peer, "result": self._safe_error(exc)})
                self._save(attempts=attempts)
        return False

    @staticmethod
    def _safe_error(exc: Exception) -> str:
        if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
            return "timeout"
        if isinstance(exc, EventHistoryObjectError):
            return exc.code
        return "unavailable_or_invalid"

    async def _fetch_chain(self, peer_url: str, realm_id: str, head: str) -> dict[str, bytes]:
        fetched: dict[str, bytes] = {}
        pending: list[tuple[str, str]] = [(head, "commit")]
        seen: set[str] = set()
        while pending:
            expected, kind = pending.pop()
            if expected in seen:
                continue
            seen.add(expected)
            if len(seen) > MAX_RECOVERY_OBJECTS:
                raise ValueError("recovery object limit exceeded")
            raw = self.store.get(expected)
            try:
                model = self._validate(expected, raw, kind, realm_id)
            except EventHistoryObjectError:
                response = await self.engine._request(
                    "POST", f"{peer_url.rstrip('/')}/api/sync/get", payload={"hashes": [expected]}
                )
                response.raise_for_status()
                body = await self.engine._response_json(response)
                encoded = body.get("objects", {}).get(expected) if isinstance(body, dict) else None
                if not isinstance(encoded, str):
                    raise EventHistoryObjectError(f"missing_{'event' if kind == 'event' else 'parent'}", expected, kind)
                try:
                    raw = base64.b64decode(encoded, validate=True)
                except ValueError as exc:
                    raise EventHistoryObjectError("corrupt_object", expected, kind) from exc
                model = self._validate(expected, raw, kind, realm_id)
                fetched[expected] = raw
            if kind == "commit":
                assert isinstance(model, SyncCommit)
                pending.extend((value, "commit") for value in model.parent_hashes)
                pending.extend((value, "event") for value in model.event_hashes)
        return fetched

    @staticmethod
    def _validate(expected: str, raw: bytes | None, kind: str, realm_id: str):
        if raw is None:
            raise EventHistoryObjectError("missing_event" if kind == "event" else "missing_parent", expected, kind)
        if object_hash(raw) != expected:
            raise EventHistoryObjectError("corrupt_object", expected, kind)
        try:
            value = json.loads(raw)
            if not isinstance(value, dict) or value.get("schema_version", 1) != 1:
                raise ValueError
            model = (CardEvent if kind == "event" else SyncCommit).model_validate(value)
        except (UnicodeDecodeError, json.JSONDecodeError, ValidationError, ValueError) as exc:
            raise EventHistoryObjectError("corrupt_object", expected, kind) from exc
        if model.realm_id != realm_id:
            raise EventHistoryObjectError("corrupt_object", expected, kind)
        return model
