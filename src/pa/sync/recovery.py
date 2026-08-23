"""Durable, ref-preserving recovery for referenced sync objects."""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
from pydantic import ValidationError

from pa.core.io import atomic_write_json
from pa.domain.models import CardEvent, SyncCommit
from pa.sync.event_log import EventHistoryObjectError
from pa.sync.object_store import object_hash

MAX_RECOVERY_PEERS = 4
MAX_RECOVERY_FETCHED_OBJECTS = 32
MAX_RECOVERY_PEER_REQUESTS = 64
MAX_RECOVERY_HEAD_CHANGES = 8


class RecoveryLimitError(RuntimeError):
    """Privacy-safe bounded-work failure exposed through recovery diagnostics."""

    def __init__(self, code: str) -> None:
        super().__init__(code.replace("_", " "))
        self.code = code


class _PeerRequired(RuntimeError):
    def __init__(self, failure: EventHistoryObjectError) -> None:
        super().__init__(failure.code)
        self.failure = failure


@dataclass
class _RecoveryBudget:
    peer_requests: int = 0
    fetched_objects: int = 0
    validation_passes: int = 0
    head_changes: int = 0
    limit_hit: str | None = None

    def reserve_request(self) -> None:
        if self.peer_requests >= MAX_RECOVERY_PEER_REQUESTS:
            self.limit_hit = "peer_request_limit"
            raise RecoveryLimitError("peer_request_limit_exceeded")
        self.peer_requests += 1

    def record_fetched_object(self) -> None:
        if self.fetched_objects >= MAX_RECOVERY_FETCHED_OBJECTS:
            self.limit_hit = "fetched_object_limit"
            raise RecoveryLimitError("fetched_object_limit_exceeded")
        self.fetched_objects += 1

    def record_head_change(self) -> None:
        self.head_changes += 1
        if self.head_changes > MAX_RECOVERY_HEAD_CHANGES:
            self.limit_hit = "head_change_limit"
            raise RecoveryLimitError("head_change_limit_exceeded")

    def public(self) -> dict[str, int | str | None]:
        return {
            "peer_requests": self.peer_requests,
            "fetched_objects": self.fetched_objects,
            "validation_passes": self.validation_passes,
            "head_changes": self.head_changes,
            "max_peer_requests": MAX_RECOVERY_PEER_REQUESTS,
            "max_fetched_objects": MAX_RECOVERY_FETCHED_OBJECTS,
            "max_head_changes": MAX_RECOVERY_HEAD_CHANGES,
            "limit_hit": self.limit_hit,
        }


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
        self._save(
            state="healthy",
            code=None,
            object_kind=None,
            object_hash=None,
            recovery_head=None,
        )

    def public(self) -> dict[str, Any]:
        allowed = {
            "state",
            "realm_id",
            "object_kind",
            "object_hash",
            "code",
            "attempts",
            "work",
            "updated_at",
        }
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
                recovery_head = self.log.get_head(realm_id)
                self._save(
                    state="recovering",
                    realm_id=realm_id,
                    code=failure.code,
                    object_kind=failure.diagnostic.get("object_kind"),
                    object_hash=failure.diagnostic.get("object_hash"),
                    recovery_head=recovery_head,
                    attempts=[],
                    work=_RecoveryBudget().public(),
                )
                if not await self._recover_realm(
                    realm_id, failure=failure, failure_head=recovery_head
                ):
                    self._save(state="unrecoverable")
                    return False
            self.mark_healthy()
            return True

    async def retry(self, realm_id: str) -> bool:
        async with self._lock:
            failure = self._saved_failure(realm_id)
            failure_head = self.state.get("recovery_head")
            self._save(
                state="recovering",
                realm_id=realm_id,
                attempts=[],
                work=_RecoveryBudget().public(),
            )
            recovered = await self._recover_realm(
                realm_id,
                failure=failure,
                failure_head=(failure_head if isinstance(failure_head, str) else None),
            )
            if recovered:
                self.mark_healthy()
            else:
                self._save(state="unrecoverable")
            return recovered

    def _saved_failure(self, realm_id: str) -> EventHistoryObjectError | None:
        if self.state.get("realm_id") != realm_id:
            return None
        expected = self.state.get("object_hash")
        kind = self.state.get("object_kind")
        code = self.state.get("code")
        if not all(isinstance(value, str) and value for value in (expected, kind, code)):
            return None
        return EventHistoryObjectError(code, expected, kind)

    async def _recover_realm(
        self,
        realm_id: str,
        *,
        failure: EventHistoryObjectError | None = None,
        failure_head: str | None = None,
    ) -> bool:
        head = self.log.get_head(realm_id)
        if not head:
            return True
        budget = _RecoveryBudget()
        attempts: list[dict[str, Any]] = []

        # A retry may find that another supported operation already restored the
        # object. Validate locally before requiring peer availability.
        try:
            await self._repair_incrementally(
                None,
                realm_id,
                failure=failure,
                failure_head=failure_head,
                budget=budget,
            )
            self._save(attempts=attempts, work=budget.public())
            return True
        except _PeerRequired:
            pass
        except Exception as exc:
            attempts.append({"peer": "local", "result": self._safe_error(exc)})
            self._save(attempts=attempts, work=budget.public())
            return False

        routes = self.engine.peer_table.prefer_same_zone(
            realm_id, self.settings.zone
        )[:MAX_RECOVERY_PEERS]
        if not routes:
            attempts.append({"peer": "none", "result": "no_authenticated_peer"})
            self._save(attempts=attempts, work=budget.public())
            return False
        for route in routes:
            peer = route.target_instance_id or "configured_peer"
            try:
                await self._repair_incrementally(
                    route.target_url,
                    realm_id,
                    failure=failure,
                    failure_head=failure_head,
                    budget=budget,
                )
                attempts.append({"peer": peer, "result": "recovered"})
                self._save(attempts=attempts, work=budget.public())
                return True
            except Exception as exc:
                attempts.append({"peer": peer, "result": self._safe_error(exc)})
                self._save(attempts=attempts, work=budget.public())
                if isinstance(exc, RecoveryLimitError):
                    break
        return False

    @staticmethod
    def _safe_error(exc: Exception) -> str:
        if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
            return "timeout"
        if isinstance(exc, RecoveryLimitError):
            return exc.code
        if isinstance(exc, httpx.TransportError):
            return "peer_unavailable"
        if isinstance(exc, httpx.HTTPStatusError):
            return "peer_http_error"
        if isinstance(exc, EventHistoryObjectError):
            return exc.code
        return "unavailable_or_invalid"

    async def _repair_incrementally(
        self,
        peer_url: str | None,
        realm_id: str,
        *,
        failure: EventHistoryObjectError | None,
        failure_head: str | None,
        budget: _RecoveryBudget,
    ) -> None:
        current_failure = failure
        while True:
            head = self.log.get_head(realm_id)
            if not head:
                return
            if failure_head and head != failure_head:
                current_failure = None

            if current_failure is not None:
                expected = current_failure.diagnostic.get("object_hash")
                kind = current_failure.diagnostic.get("object_kind")
                if not isinstance(expected, str) or kind not in {"commit", "event"}:
                    raise current_failure
                try:
                    self._validate(expected, self.store.get(expected), kind, realm_id)
                except EventHistoryObjectError:
                    if peer_url is None:
                        raise _PeerRequired(current_failure)
                    await self._fetch_object(
                        peer_url, realm_id, expected, kind, budget=budget
                    )
                current_failure = None

            budget.validation_passes += 1
            try:
                await self.engine._offload(
                    "sync.recovery.verify_index",
                    self.log.verify_index,
                    realm_id,
                    head,
                    timeout=120.0,
                )
            except EventHistoryObjectError as exc:
                current_failure = exc
                failure_head = head
                continue

            if self.log.get_head(realm_id) != head:
                budget.record_head_change()
                failure_head = None
                continue

            await self.engine._offload(
                "sync.recovery.rebuild_projection",
                self.projection_rebuilder,
                realm_id,
                timeout=120.0,
            )
            if self.log.get_head(realm_id) != head:
                budget.record_head_change()
                failure_head = None
                continue
            return

    async def _fetch_object(
        self,
        peer_url: str,
        realm_id: str,
        expected: str,
        kind: str,
        *,
        budget: _RecoveryBudget,
    ) -> None:
        budget.reserve_request()
        response = await self.engine._request(
            "POST",
            f"{peer_url.rstrip('/')}/api/sync/get",
            payload={"hashes": [expected]},
        )
        response.raise_for_status()
        body = await self.engine._response_json(response)
        encoded = body.get("objects", {}).get(expected) if isinstance(body, dict) else None
        if not isinstance(encoded, str):
            raise EventHistoryObjectError(
                "missing_event" if kind == "event" else "missing_parent",
                expected,
                kind,
            )
        budget.record_fetched_object()
        try:
            raw = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise EventHistoryObjectError("corrupt_object", expected, kind) from exc
        self._validate(expected, raw, kind, realm_id)
        await self.engine._offload(
            "sync.recovery.install_object",
            self.store.repair,
            expected,
            raw,
            timeout=30.0,
        )

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
