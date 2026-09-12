"""Durable, ref-preserving recovery for referenced sync objects."""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import re
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx
from pydantic import ValidationError

from pa.core.io import atomic_write_json
from pa.domain.models import CardEvent, SyncCommit
from pa.sync.event_log import MAX_HISTORY_COMMITS, EventHistoryObjectError
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
    """One owned job per realm; HTTP waiters never own blocking workers.

    Admission is object-only and internal: no supplied object/hash/proof is
    accepted by the endpoint. Evidence is produced by canonical traversal and
    checked again against immutable references and the current durable head.
    """

    request_timeout = 120.0
    worker_queue_timeout = 120.0

    def __init__(
        self,
        settings,
        engine,
        projection_rebuilder,
        *,
        projection_head=None,
        on_health_change=None,
    ) -> None:
        self.settings = settings
        self.engine = engine
        self.log = engine.log
        self.store = engine.store
        self.projection_rebuilder = projection_rebuilder
        self.projection_head = projection_head
        self.on_health_change = on_health_change
        self.path = settings.data_dir / "sync_recovery.json"
        self._state_lock = threading.RLock()
        self._task: asyncio.Task | None = None
        self._jobs: dict[str, asyncio.Task] = {}
        self._closing = False
        self._loop = asyncio.get_running_loop()
        self.state: dict[str, Any] = self._load()
        self.realms = self.state.setdefault("realms", {})
        # Migrate old evidence, but never invent a canonical reference for it.
        if not self.realms and self.state.get("realm_id"):
            self.realms[self.state["realm_id"]] = {
                k: v for k, v in self.state.items() if k != "realms"
            }
        for record in self.realms.values():
            if record.get("state") == "recovering":
                record.update(
                    state="recovering",
                    phase="interrupted",
                    active_residual_worker=False,
                )
        self.log.on_history_failure = self.note_failure

    def _load(self) -> dict[str, Any]:
        try:
            value = json.loads(self.path.read_text())
            return value if isinstance(value, dict) else {}
        except OSError, ValueError:
            return {}

    def _save(self, *, realm_id: str | None = None, **updates: Any) -> None:
        with self._state_lock:
            realm_id = realm_id or self.state.get("realm_id")
            if realm_id:
                record = self.realms.setdefault(realm_id, {})
                record.update(
                    updates, realm_id=realm_id, updated_at=datetime.now(UTC).isoformat()
                )
                self.state = dict(record)
            else:
                self.state.update(updates)
            self.state["realms"] = self.realms
            atomic_write_json(self.path, {"version": 2, **self.state}, mode=0o600)
        if self.on_health_change:
            self.on_health_change(not self.degraded())

    def degraded(self) -> bool:
        with self._state_lock:
            return any(r.get("state") != "healthy" for r in self.realms.values())

    def mark_healthy(self) -> None:
        # Startup may establish health before any recovery evidence exists.
        if not self.realms:
            self._save(state="healthy")

    def public(self, realm_id: str | None = None) -> dict[str, Any]:
        allowed = {
            "state",
            "realm_id",
            "object_kind",
            "object_hash",
            "code",
            "attempts",
            "work",
            "updated_at",
            "operation_id",
            "head_hash",
            "reference_hash",
            "active_residual_worker",
            "phase",
            "resume_count",
            "previous_operation_id",
        }
        with self._state_lock:
            record = (
                self.realms.get(realm_id, {})
                if realm_id
                else next(
                    (r for r in self.realms.values() if r.get("state") == "recovering"),
                    next(
                        (
                            r
                            for r in self.realms.values()
                            if r.get("state") != "healthy"
                        ),
                        self.state,
                    ),
                )
            )
            result = {k: v for k, v in record.items() if k in allowed}
            if not realm_id:
                result["state"] = (
                    "recovering"
                    if any(r.get("state") == "recovering" for r in self.realms.values())
                    else "unrecoverable"
                    if self.degraded()
                    else "healthy"
                )
            return result

    async def close(self) -> None:
        self._closing = True
        self.log.on_history_failure = None
        # Drain owners before closing their peer client or blocking runtime.
        await asyncio.gather(
            *(asyncio.shield(t) for t in self._jobs.values()), return_exceptions=True
        )

    def note_failure(self, realm_id: str, failure: EventHistoryObjectError) -> None:
        # Called on the projection/index worker. Save before returning so the
        # mutation gate closes even if the original HTTP waiter has disappeared.
        evidence = {
            key: value
            if isinstance(value := failure.diagnostic.get(key), str)
            and re.fullmatch(r"[0-9a-f]{64}", value)
            else None
            for key in ("object_hash", "head_hash", "reference_hash")
        }
        kind = failure.diagnostic.get("object_kind")
        evidence["object_kind"] = kind if kind in {"event", "commit"} else None
        with self._state_lock:
            self._save(
                realm_id=realm_id, state="recovering", code=failure.code, **evidence
            )
        if not self._closing:
            self._loop.call_soon_threadsafe(self._ensure_job, realm_id)

    def _ensure_job(
        self, realm_id: str, request_key: str | None = None, *, resume: bool = False
    ) -> asyncio.Future[bool]:
        record = self.realms.get(realm_id, {})
        if request_key and request_key in record.get("stale_request_keys", []):
            raise RecoveryLimitError("stale_recovery_head")
        task = self._jobs.get(realm_id)
        if task is not None and not task.done():
            if request_key:
                keys = list(self.realms[realm_id].get("request_keys", []))
                if request_key not in keys:
                    if len(keys) >= 64:
                        raise RecoveryLimitError("request_identity_limit")
                    self._save(realm_id=realm_id, request_keys=[*keys, request_key])
            return task
        record = self.realms.get(realm_id, {})
        head = self.log.get_head(realm_id)
        operation_head = record.get("operation_head_hash", record.get("head_hash"))
        if request_key in record.get("request_keys", []) and operation_head != head:
            raise RecoveryLimitError("stale_recovery_head")
        if (
            task is not None
            and operation_head == head
            and request_key is not None
            and request_key in record.get("request_keys", [])
        ):
            return task
        same_head = operation_head == head
        same_key = request_key is not None and request_key in record.get(
            "request_keys", []
        )
        # A terminal receipt survives process exit. Startup explicitly requests
        # authoritative verification of degraded records instead of replaying it.
        if (
            task is None
            and same_head
            and same_key
            and not resume
            and record.get("phase") == "complete"
        ):
            receipt = self._loop.create_future()
            receipt.set_result(record.get("state") == "healthy")
            return receipt
        resuming = (
            task is None
            and same_head
            and record.get("operation_id")
            and (resume or record.get("phase") == "interrupted")
        )
        keys = list(record.get("request_keys", [])) if resuming else []
        if request_key and request_key not in keys:
            keys.append(request_key)
        stale_keys = list(record.get("stale_request_keys", []))
        if not same_head:
            stale_keys = list(
                dict.fromkeys([*stale_keys, *record.get("request_keys", [])])
            )
        if len(keys) > 64:
            raise RecoveryLimitError("request_identity_limit")
        updates = {}
        if record.get("head_hash") != head:
            # Old evidence cannot authorize installation at a new head. Obtain
            # fresh evidence through canonical verification of this generation.
            updates.update(
                object_hash=None,
                object_kind=None,
                reference_hash=None,
                head_hash=head,
                code=None,
            )
        self._save(
            realm_id=realm_id,
            state="recovering",
            operation_id=record["operation_id"] if resuming else str(uuid4()),
            operation_head_hash=head,
            previous_operation_id=record.get("previous_operation_id")
            if resuming
            else record.get("operation_id"),
            resume_count=int(record.get("resume_count", 0)) + 1 if resuming else 0,
            request_keys=keys,
            stale_request_keys=stale_keys,
            active_residual_worker=True,
            phase="resuming" if resuming else "starting",
            attempts=[],
            work=_RecoveryBudget().public(),
            **updates,
        )
        task = asyncio.create_task(
            self._run_job(realm_id, head), name="sync-object-recovery"
        )
        self._jobs[realm_id] = task
        task.add_done_callback(
            lambda done: done.exception() if not done.cancelled() else None
        )
        return task

    def start(
        self, failures: list[tuple[str, EventHistoryObjectError]]
    ) -> asyncio.Task:
        if self._task and not self._task.done():
            return self._task
        self._task = asyncio.create_task(
            self.recover(failures), name="sync-object-recovery-start"
        )
        return self._task

    async def recover(
        self, failures: list[tuple[str, EventHistoryObjectError]]
    ) -> bool:
        for realm_id, failure in failures:
            self.note_failure(realm_id, failure)
        affected = {realm for realm, _ in failures} | {
            realm
            for realm, record in self.realms.items()
            if realm in self.settings.subscribed_realms
            and record.get("state") != "healthy"
        }
        # Create all owners before waiting on any slow realm.
        jobs = [self._ensure_job(realm, resume=True) for realm in sorted(affected)]
        results = await asyncio.gather(*(asyncio.shield(job) for job in jobs))
        return all(results) and not self.degraded()

    async def retry(
        self, realm_id: str, *, request_key: str | None = None
    ) -> bool | None:
        # Persist only a digest, never an arbitrary caller-supplied identity.
        identity = (
            hashlib.sha256(request_key.encode()).hexdigest() if request_key else None
        )
        task = self._ensure_job(realm_id, identity)
        try:
            async with asyncio.timeout(self.request_timeout):
                return await asyncio.shield(task)
        except TimeoutError:
            if task.done():
                return task.result()
            return None

    async def _run_job(self, realm_id: str, head: str | None) -> bool:
        failure = self._saved_failure(realm_id)
        record = self.realms.get(realm_id, {})
        failure_head = record.get("head_hash")
        self._save(realm_id=realm_id, head_hash=head)
        try:
            recovered = await self._recover_realm(
                realm_id, failure=failure, failure_head=failure_head
            )

            def finish() -> None:
                with self.log._lock, self.log._refs_file_lock():
                    self.log._load_refs()
                    if self.log._refs.get(self.log.ref_key(realm_id)) != head:
                        raise RecoveryLimitError("stale_recovery_head")
                    if (
                        recovered
                        and head
                        and not self.log.index.status(realm_id, head).get("ready")
                    ):
                        raise RecoveryLimitError("index_not_verified")
                    if (
                        recovered
                        and self.projection_head
                        and self.projection_head(realm_id) != head
                    ):
                        raise RecoveryLimitError("projection_not_current")
                    self._save(
                        realm_id=realm_id,
                        state="healthy" if recovered else "unrecoverable",
                        head_hash=head,
                        active_residual_worker=False,
                        phase="complete",
                        **(
                            {
                                "code": None,
                                "object_kind": None,
                                "object_hash": None,
                                "reference_hash": None,
                            }
                            if recovered
                            else {
                                "code": next(
                                    (
                                        a["result"]
                                        for a in reversed(
                                            self.realms[realm_id].get("attempts", [])
                                        )
                                    ),
                                    "unavailable_or_invalid",
                                )
                            }
                        ),
                    )

            await self.engine._offload(
                "sync.recovery.finish", finish, wait_for_completion=True
            )
            return recovered
        except Exception as exc:
            self._save(
                realm_id=realm_id,
                state="unrecoverable",
                code=self._safe_error(exc),
                active_residual_worker=False,
                phase="complete",
            )
            return False

    def _saved_failure(self, realm_id: str) -> EventHistoryObjectError | None:
        record = self.realms.get(realm_id, {})
        expected, kind, code = (
            record.get(k) for k in ("object_hash", "object_kind", "code")
        )
        if not all(isinstance(v, str) and v for v in (expected, kind, code)):
            return None
        failure = EventHistoryObjectError(code, expected, kind)
        failure.diagnostic.update(
            {k: record[k] for k in ("head_hash", "reference_hash") if k in record}
        )
        return failure

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
            self._save(realm_id=realm_id, attempts=attempts, work=budget.public())
            return True
        except _PeerRequired as exc:
            failure = exc.failure
            failure_head = failure.diagnostic.get("head_hash")
        except Exception as exc:
            attempts.append({"peer": "local", "result": self._safe_error(exc)})
            self._save(realm_id=realm_id, attempts=attempts, work=budget.public())
            return False

        if not self.settings.sync_token:
            attempts.append({"peer": "none", "result": "no_authenticated_peer"})
            self._save(realm_id=realm_id, attempts=attempts, work=budget.public())
            return False
        routes = self.engine.peer_table.prefer_same_zone(realm_id, self.settings.zone)[
            :MAX_RECOVERY_PEERS
        ]
        if not routes:
            attempts.append({"peer": "none", "result": "no_authenticated_peer"})
            self._save(realm_id=realm_id, attempts=attempts, work=budget.public())
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
                self._save(realm_id=realm_id, attempts=attempts, work=budget.public())
                return True
            except Exception as exc:
                attempts.append({"peer": peer, "result": self._safe_error(exc)})
                self._save(realm_id=realm_id, attempts=attempts, work=budget.public())
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
        # Legacy records without canonical reference evidence are hints only.
        if current_failure is not None and not current_failure.diagnostic.get(
            "reference_hash"
        ):
            current_failure = None
        while True:
            head = self.log.get_head(realm_id)
            if not head:
                return
            if failure_head and head != failure_head:
                raise RecoveryLimitError("stale_recovery_head")

            if current_failure is not None:
                expected = current_failure.diagnostic.get("object_hash")
                kind = current_failure.diagnostic.get("object_kind")
                if not isinstance(expected, str) or kind not in {"commit", "event"}:
                    raise current_failure
                await self.engine._offload(
                    "sync.recovery.reference",
                    self._check_reference,
                    realm_id,
                    head,
                    current_failure,
                    wait_for_completion=True,
                )
                try:
                    self._validate(expected, self.store.get(expected), kind, realm_id)
                except EventHistoryObjectError:
                    if peer_url is None:
                        raise _PeerRequired(current_failure)
                    await self._fetch_object(
                        peer_url, realm_id, expected, kind, budget=budget, head=head
                    )
                current_failure = None

            budget.validation_passes += 1
            self._save(realm_id=realm_id, phase="verifying", work=budget.public())
            try:
                await self.engine._offload(
                    "sync.recovery.verify_index",
                    self.log.verify_index,
                    realm_id,
                    head,
                    timeout=self.worker_queue_timeout,
                    wait_for_completion=True,
                )
            except EventHistoryObjectError as exc:
                current_failure = exc
                failure_head = head
                continue

            if self.log.get_head(realm_id) != head:
                budget.record_head_change()
                raise RecoveryLimitError("stale_recovery_head")

            self._save(realm_id=realm_id, phase="reprojecting", work=budget.public())
            await self.engine._offload(
                "sync.recovery.rebuild_projection",
                self.projection_rebuilder,
                realm_id,
                *([head] if self.projection_head is not None else []),
                timeout=self.worker_queue_timeout,
                wait_for_completion=True,
            )
            if self.log.get_head(realm_id) != head:
                budget.record_head_change()
                raise RecoveryLimitError("stale_recovery_head")
            return

    def _check_reference(
        self, realm_id: str, head: str, failure: EventHistoryObjectError
    ) -> None:
        if any(
            not isinstance(value := failure.diagnostic.get(key), str)
            or not re.fullmatch(r"[0-9a-f]{64}", value)
            for key in ("object_hash", "head_hash", "reference_hash")
        ):
            raise RecoveryLimitError("invalid_recovery_reference")
        if self.log.get_head(realm_id) != head:
            raise RecoveryLimitError("stale_recovery_head")
        if failure.diagnostic.get("head_hash") != head:
            raise RecoveryLimitError("stale_recovery_head")
        reference = failure.diagnostic.get("reference_hash")
        expected = failure.diagnostic.get("object_hash")
        kind = failure.diagnostic.get("object_kind")
        # Canonical commit bytes establish reachability; a present/indexed tip
        # alone never establishes completeness. No event history pre-scan.
        if kind == "commit" and expected == head and reference == head:
            return
        pending, seen = [head], set()
        while pending:
            candidate = pending.pop()
            if candidate in seen:
                continue
            seen.add(candidate)
            if len(seen) > MAX_HISTORY_COMMITS:
                raise RecoveryLimitError("reference_limit_exceeded")
            commit = self.log.get_commit(candidate)
            if commit is None or commit.realm_id != realm_id:
                raise RecoveryLimitError("invalid_recovery_reference")
            if candidate == reference:
                links = commit.event_hashes if kind == "event" else commit.parent_hashes
                if expected in links:
                    return
                break
            pending.extend(commit.parent_hashes)
        raise RecoveryLimitError("invalid_recovery_reference")

    async def _fetch_object(
        self,
        peer_url: str,
        realm_id: str,
        expected: str,
        kind: str,
        *,
        budget: _RecoveryBudget,
        head: str,
    ) -> None:
        budget.reserve_request()
        self._save(realm_id=realm_id, phase="fetching", work=budget.public())
        response = await self.engine._request(
            "POST",
            f"{peer_url.rstrip('/')}/api/sync/get",
            payload={"hashes": [expected]},
        )
        response.raise_for_status()
        body = await self.engine._response_json(response)
        encoded = (
            body.get("objects", {}).get(expected) if isinstance(body, dict) else None
        )
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

        def install() -> None:
            # Share the ref fence with ordinary ref writers. Network and history
            # work stay outside this narrow critical section.
            with self.log._lock, self.log._refs_file_lock():
                self.log._load_refs()
                if self.log._refs.get(self.log.ref_key(realm_id)) != head:
                    raise RecoveryLimitError("stale_recovery_head")
                self.store.repair(expected, raw)

        await self.engine._offload(
            "sync.recovery.install_object",
            install,
            timeout=30.0,
            wait_for_completion=True,
        )

    @staticmethod
    def _validate(expected: str, raw: bytes | None, kind: str, realm_id: str):
        if raw is None:
            raise EventHistoryObjectError(
                "missing_event" if kind == "event" else "missing_parent", expected, kind
            )
        if object_hash(raw) != expected:
            raise EventHistoryObjectError("corrupt_object", expected, kind)
        try:
            value = json.loads(raw)
            if (
                not isinstance(value, dict)
                or type(value.get("schema_version", 1)) is not int
                or value.get("schema_version", 1) != 1
            ):
                raise ValueError
            model = (CardEvent if kind == "event" else SyncCommit).model_validate(value)
        except (
            UnicodeDecodeError,
            json.JSONDecodeError,
            ValidationError,
            ValueError,
        ) as exc:
            raise EventHistoryObjectError("corrupt_object", expected, kind) from exc
        if model.realm_id != realm_id:
            raise EventHistoryObjectError("corrupt_object", expected, kind)
        return model
