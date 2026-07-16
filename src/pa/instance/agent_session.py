"""Multi-session ACP agent runtime for a PA instance."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from pa.acp.client import (
    AgentConnection,
    normalize_session_update,
    permission_cancelled,
    permission_selected,
    usage_to_dict,
)
from pa.acp.providers.registry import DEFAULT_PROVIDER_ID
from pa.acp.providers.resolve import resolve_agent_provider
from pa.acp.surfaces import (
    SURFACE_CHAT_DEFAULT,
    SURFACE_EXECUTION,
    AgentInvocationContext,
    surface_for_label,
)
from pa.config import Settings
from pa.core.preferences import get_preferences_store
from pa.domain.models import AgentSession, TranscriptEvent
from pa.domain.store import Store
from pa.instance.quiesce import (
    ImageAttachment,
    QueuedPrompt,
    QuiesceProgress,
    QuiesceSnapshot,
    SessionSnapshot,
    clear_quiesce_snapshot,
    load_quiesce_snapshot,
    save_quiesce_snapshot,
)

logger = logging.getLogger(__name__)

_RETRY_SECONDS = 30
_QUIESCE_POLL_SECONDS = 0.4
PromptAction = Literal["append", "prepend", "interrupt"]


@contextmanager
def _agent_env_overlay(extra: dict[str, str]):
    prev: dict[str, str | None] = {}
    for key, value in extra.items():
        prev[key] = os.environ.get(key)
        os.environ[key] = value
    try:
        yield
    finally:
        for key, old in prev.items():
            if old is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old


def _session_dir(data_dir: Path, session_id: str) -> Path:
    path = data_dir / "sessions" / session_id
    path.mkdir(parents=True, exist_ok=True)
    return path


class AgentSessionRuntime:
    """Owns one ACP subprocess + connection for a single PA session."""

    def __init__(
        self,
        manager: AgentSessionManager,
        session: AgentSession,
        *,
        agent_env: dict[str, str] | None = None,
    ) -> None:
        self.manager = manager
        self.settings = manager.settings
        self.store = manager.store
        self.session = session
        self.agent_env = dict(agent_env or {})
        self.connection: AgentConnection | None = None
        self._prompt_lock = asyncio.Lock()
        self._queue: list[QueuedPrompt] = []
        self._queue_paused = False
        self._in_flight: QueuedPrompt | None = None
        self._drain_task: asyncio.Task[None] | None = None
        self._subscribers: list[asyncio.Queue[dict[str, Any]]] = []
        self._pending_permissions: dict[str, asyncio.Future[Any]] = {}
        self._permission_requests: dict[str, dict[str, Any]] = {}
        self._seq = self.store.next_transcript_seq(session.id) - 1
        self._transcript_buffer: list[TranscriptEvent] = []
        self._closed = False
        self._turn_started_at: datetime | None = None

    @property
    def session_id(self) -> str:
        return self.session.id

    @property
    def connected(self) -> bool:
        return bool(self.connection and self.connection.connected)

    @property
    def prompting(self) -> bool:
        return bool(self.connection and self.connection.prompting) or self._prompt_lock.locked()

    @property
    def queue_paused(self) -> bool:
        return self._queue_paused

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=500)
        self._subscribers.append(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        if queue in self._subscribers:
            self._subscribers.remove(queue)

    def _emit_live(self, event: dict[str, Any]) -> None:
        dead: list[asyncio.Queue[dict[str, Any]]] = []
        for sub in self._subscribers:
            try:
                sub.put_nowait(event)
            except asyncio.QueueFull:
                dead.append(sub)
        for sub in dead:
            self.unsubscribe(sub)

    def _append_transcript(self, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        self._seq += 1
        te = TranscriptEvent(
            session_id=self.session_id,
            seq=self._seq,
            event_type=event_type,
            payload=payload,
        )
        self._transcript_buffer.append(te)
        if len(self._transcript_buffer) >= 8:
            self._flush_transcript()
        event = {
            "id": te.id,
            "seq": te.seq,
            "type": event_type,
            "session_id": self.session_id,
            "payload": payload,
            "created_at": te.created_at.isoformat(),
        }
        self._emit_live(event)
        return event

    def _flush_transcript(self) -> None:
        if not self._transcript_buffer:
            return
        batch = list(self._transcript_buffer)
        self._transcript_buffer.clear()
        try:
            self.store.append_transcript_events(batch)
        except Exception:
            logger.exception("Failed to persist transcript events")
            self._transcript_buffer = batch + self._transcript_buffer

    async def _on_acp_update(self, _external_session_id: str, update: Any) -> None:
        normalized = normalize_session_update(update)
        event_type = str(normalized.get("type") or "session_update")
        if event_type == "usage_update" and normalized.get("usage"):
            metrics = dict(self.session.metrics_json or {})
            metrics["usage"] = normalized["usage"]
            self.session.metrics_json = metrics
            self.store.save_session(self.session)
        if event_type == "current_mode_update" and normalized.get("mode_id"):
            self.session.mode_id = normalized["mode_id"]
            self.store.save_session(self.session)
        if event_type == "config_option_update":
            options = normalized.get("config_options")
            if options is not None:
                cfg = dict(self.session.config_json or {})
                cfg["options"] = options
                self.session.config_json = cfg
                self.store.save_session(self.session)
                if self.connection:
                    self.connection.config_options = options
        self._append_transcript(event_type, normalized)

    async def _on_permission(
        self, _external_session_id: str, request: dict[str, Any]
    ) -> Any:
        if self.manager.should_auto_approve(self.session.principal_id):
            options = request.get("options") or []
            option_id = None
            for kind in ("allow_always", "allow_once"):
                for opt in options:
                    if isinstance(opt, dict) and opt.get("kind") == kind:
                        option_id = opt.get("optionId") or opt.get("option_id")
                        break
                if option_id:
                    break
            if not option_id and options and isinstance(options[0], dict):
                option_id = options[0].get("optionId") or options[0].get("option_id")
            if option_id:
                response = permission_selected(option_id)
                self._append_transcript(
                    "permission_resolved",
                    {
                        "request_id": request.get("request_id"),
                        "response": response.model_dump(mode="json", by_alias=True),
                        "auto": True,
                    },
                )
                return response

        request_id = str(request.get("request_id") or uuid4())
        request["request_id"] = request_id
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        self._pending_permissions[request_id] = future
        self._permission_requests[request_id] = request
        self._append_transcript("permission_request", request)
        try:
            return await future
        finally:
            self._pending_permissions.pop(request_id, None)
            self._permission_requests.pop(request_id, None)

    async def start(
        self,
        *,
        resume_external_id: str | None = None,
        queued_prompts: list[QueuedPrompt] | None = None,
        queue_paused: bool = False,
        provider_spec=None,
    ) -> AgentSession:
        wire_path = _session_dir(self.settings.data_dir, self.session_id) / "wire.jsonl"
        provider_id = self.session.agent_name or DEFAULT_PROVIDER_ID
        if provider_id in {"instance", ""}:
            provider_id = DEFAULT_PROVIDER_ID
        self.connection = AgentConnection(
            self.settings,
            self.store,
            agent_name=provider_id,
            provider_spec=provider_spec,
            on_update=self._on_acp_update,
            on_permission=self._on_permission,
            wire_path=wire_path,
            auto_approve=False,
        )
        with _agent_env_overlay(self.agent_env):
            self.session = await self.connection.connect(
                resume_external_id=resume_external_id,
                cwd=self.session.cwd,
                existing_session=self.session,
                title=self.session.title,
                label=self.session.label,
                principal_id=self.session.principal_id,
                card_id=self.session.card_id,
                project_id=self.session.project_id,
            )
        # Persist resolved provider id on the session.
        if self.connection and self.connection.agent_name:
            self.session.agent_name = self.connection.agent_name
            self.store.save_session(self.session)
        self._queue_paused = queue_paused
        if queued_prompts:
            for item in queued_prompts:
                item.session_id = self.session_id
            self._queue = list(queued_prompts)
        self._append_transcript(
            "session_started",
            {
                "external_session_id": self.session.external_session_id,
                "cwd": self.session.cwd,
                "label": self.session.label,
                "model_id": self.session.model_id,
                "mode_id": self.session.mode_id,
            },
        )
        self._flush_transcript()
        self._start_drain()
        return self.session

    def _start_drain(self) -> None:
        if self._drain_task and not self._drain_task.done():
            return
        if self._queue_paused or not self._queue:
            return
        self._drain_task = asyncio.create_task(self._drain_queue())

    async def _drain_queue(self) -> None:
        while self._queue and not self._queue_paused and not self._closed and self.connected:
            if self.manager.quiescing:
                break
            item = self._queue.pop(0)
            self._append_transcript("queue_dequeued", {"id": item.id, "message": item.message})
            try:
                await self._run_prompt(item)
            except Exception as exc:
                logger.exception("Queued prompt failed for session %s", self.session_id)
                self._append_transcript(
                    "error",
                    {"message": str(exc), "queued_prompt_id": item.id},
                )
                self._queue.insert(0, item)
                break
        self._flush_transcript()

    def enqueue(
        self,
        message: str,
        *,
        images: list[ImageAttachment] | None = None,
        action: PromptAction = "append",
        card_id: str | None = None,
        project_id: str | None = None,
        principal_id: str | None = None,
        cwd: str | None = None,
        agent_env: dict[str, str] | None = None,
        source: str = "api",
    ) -> QueuedPrompt:
        item = QueuedPrompt(
            message=message,
            images=list(images or []),
            session_id=self.session_id,
            card_id=card_id or self.session.card_id,
            project_id=project_id or self.session.project_id,
            principal_id=principal_id or self.session.principal_id,
            cwd=cwd or self.session.cwd,
            agent_env=dict(agent_env or self.agent_env),
            source=source,
        )
        if action == "prepend":
            self._queue.insert(0, item)
        else:
            self._queue.append(item)
        self._append_transcript(
            "queue_enqueued",
            {"id": item.id, "message": message, "action": action, "position": 0 if action == "prepend" else len(self._queue) - 1},
        )
        self._flush_transcript()
        if not self._queue_paused:
            self._start_drain()
        return item

    async def prompt(
        self,
        message: str,
        item_id: str | None = None,
        *,
        images: list[ImageAttachment] | None = None,
        principal_id: str | None = None,
        project_id: str | None = None,
        agent_env: dict[str, str] | None = None,
        cwd: str | None = None,
        action: PromptAction = "append",
        _from_queue: bool = False,
        wait: bool = True,
    ) -> str:
        if self.manager.quiescing or self._closed:
            if _from_queue:
                raise RuntimeError("Session is quiescing or closed")
            item = self.enqueue(
                message,
                images=images,
                action=action,
                card_id=item_id,
                project_id=project_id,
                principal_id=principal_id,
                cwd=cwd,
                agent_env=agent_env,
            )
            return "queued"

        if self.prompting and not _from_queue:
            if action == "interrupt":
                await self.cancel(pause_queue=False)
            else:
                self.enqueue(
                    message,
                    images=images,
                    action=action,
                    card_id=item_id,
                    project_id=project_id,
                    principal_id=principal_id,
                    cwd=cwd,
                    agent_env=agent_env,
                )
                return "queued"

        item = QueuedPrompt(
            message=message,
            images=list(images or []),
            session_id=self.session_id,
            card_id=item_id or self.session.card_id,
            project_id=project_id or self.session.project_id,
            principal_id=principal_id or self.session.principal_id,
            cwd=cwd or self.session.cwd,
            agent_env=dict(agent_env or self.agent_env),
            source="in_flight",
        )
        if not wait and not _from_queue:
            # Chat UI / SSE path: accept immediately and run the turn in the background.
            if self._queue_paused:
                self.enqueue(
                    message,
                    images=images,
                    action=action,
                    card_id=item_id,
                    project_id=project_id,
                    principal_id=principal_id,
                    cwd=cwd,
                    agent_env=agent_env,
                )
                return "queued"
            self._queue.insert(0, item)
            self._append_transcript(
                "queue_enqueued",
                {"id": item.id, "message": message, "action": "run", "position": 0},
            )
            self._flush_transcript()
            self._start_drain()
            return "started"
        return await self._run_prompt(item)

    async def _run_prompt(self, item: QueuedPrompt) -> str:
        if not self.connection:
            raise RuntimeError("Session not connected")
        env = dict(item.agent_env or {})
        if item.cwd:
            env["PWD"] = item.cwd
        async with self._prompt_lock:
            self._in_flight = item
            self._turn_started_at = datetime.now(UTC)
            self._append_transcript(
                "user_message",
                {
                    "id": item.id,
                    "message": item.message,
                    "source": item.source,
                    "images": [image.public_dict() for image in item.images],
                },
            )
            self._flush_transcript()
            try:
                with _agent_env_overlay(env):
                    stop_reason = await self.connection.prompt(
                        item.message,
                        images=item.images,
                        item_id=item.card_id,
                        principal_id=item.principal_id,
                        project_id=item.project_id,
                        cwd=item.cwd,
                    )
                usage = self.connection.last_usage
                if usage:
                    metrics = dict(self.session.metrics_json or {})
                    metrics["last_usage"] = usage
                    self.session.metrics_json = metrics
                    self.store.save_session(self.session)
                self._append_transcript(
                    "turn_completed",
                    {
                        "stop_reason": stop_reason,
                        "usage": usage,
                        "queued_prompt_id": item.id,
                    },
                )
                self._flush_transcript()
                return stop_reason
            finally:
                self._in_flight = None
                self._turn_started_at = None

    async def cancel(self, *, pause_queue: bool = True) -> None:
        if pause_queue:
            self._queue_paused = True
        if self.connection:
            try:
                await self.connection.cancel()
            except Exception:
                logger.exception("Cancel failed for session %s", self.session_id)
        self._append_transcript("cancelled", {"pause_queue": pause_queue})
        self._flush_transcript()

    def pause_queue(self) -> None:
        self._queue_paused = True
        self._append_transcript("queue_paused", {})
        self._flush_transcript()

    def resume_queue(self) -> None:
        self._queue_paused = False
        self._append_transcript("queue_resumed", {})
        self._flush_transcript()
        self._start_drain()

    def remove_queued(self, prompt_id: str) -> bool:
        before = len(self._queue)
        self._queue = [q for q in self._queue if q.id != prompt_id]
        removed = len(self._queue) != before
        if removed:
            self._append_transcript("queue_removed", {"id": prompt_id})
            self._flush_transcript()
        return removed

    def reorder_queue(self, prompt_ids: list[str]) -> list[QueuedPrompt]:
        by_id = {q.id: q for q in self._queue}
        ordered = [by_id[i] for i in prompt_ids if i in by_id]
        remaining = [q for q in self._queue if q.id not in prompt_ids]
        self._queue = ordered + remaining
        self._append_transcript("queue_reordered", {"ids": [q.id for q in self._queue]})
        self._flush_transcript()
        return list(self._queue)

    async def respond_permission(
        self,
        request_id: str,
        *,
        allow: bool,
        option_id: str | None = None,
        remember: bool | None = None,
        scope: Literal["user", "global"] = "user",
        principal_id: str | None = None,
    ) -> bool:
        future = self._pending_permissions.get(request_id)
        if not future or future.done():
            return False
        if allow:
            if not option_id:
                pending = self._permission_requests.get(request_id) or {}
                options = pending.get("options") or []
                for kind in ("allow_once", "allow_always"):
                    for opt in options:
                        if isinstance(opt, dict) and opt.get("kind") == kind:
                            option_id = opt.get("optionId") or opt.get("option_id")
                            break
                    if option_id:
                        break
                if not option_id and options and isinstance(options[0], dict):
                    option_id = options[0].get("optionId") or options[0].get("option_id")
            if not option_id:
                return False
            response = permission_selected(option_id)
        else:
            response = permission_cancelled()
        if remember and allow:
            self.manager.set_auto_approve(True, scope=scope, principal_id=principal_id)
        future.set_result(response)
        self._append_transcript(
            "permission_resolved",
            {
                "request_id": request_id,
                "response": response.model_dump(mode="json", by_alias=True),
                "remember": remember,
            },
        )
        self._flush_transcript()
        return True

    async def set_model(self, model_id: str) -> None:
        if not self.connection:
            raise RuntimeError("Session not connected")
        await self.connection.set_model(model_id)
        self.session = self.connection.session or self.session
        self._append_transcript("model_changed", {"model_id": model_id})
        self._flush_transcript()

    async def set_mode(self, mode_id: str) -> None:
        if not self.connection:
            raise RuntimeError("Session not connected")
        await self.connection.set_mode(mode_id)
        self.session = self.connection.session or self.session
        self._append_transcript("mode_changed", {"mode_id": mode_id})
        self._flush_transcript()

    async def set_config(self, config_id: str, value: str | bool) -> None:
        if not self.connection:
            raise RuntimeError("Session not connected")
        await self.connection.set_config(config_id, value)
        self.session = self.connection.session or self.session
        self._append_transcript("config_changed", {"config_id": config_id, "value": value})
        self._flush_transcript()

    def snapshot(self) -> dict[str, Any]:
        self._flush_transcript()
        events = self.store.list_transcript_events(self.session_id, after_seq=0, limit=2000)
        conn = self.connection
        return {
            "session": self.session.model_dump(mode="json"),
            "connected": self.connected,
            "prompting": self.prompting,
            "queue_paused": self._queue_paused,
            "queue": [q.public_dict() for q in self._queue],
            "in_flight": self._in_flight.model_dump(mode="json") if self._in_flight else None,
            "models": conn.models if conn else None,
            "modes": conn.modes if conn else None,
            "config_options": conn.config_options if conn else None,
            "metrics": self.session.metrics_json,
            "turn_started_at": self._turn_started_at.isoformat() if self._turn_started_at else None,
            "transcript": [e.model_dump(mode="json") for e in events],
            "pending_permissions": [
                self._permission_requests[rid]
                for rid in self._pending_permissions
                if rid in self._permission_requests
            ],
        }

    def to_session_snapshot(self) -> SessionSnapshot:
        queued = list(self._queue)
        if self._in_flight:
            queued = [self._in_flight, *queued]
        return SessionSnapshot(
            session_id=self.session.id,
            external_session_id=self.session.external_session_id,
            agent_name=self.session.agent_name,
            status="idle",
            cwd=self.session.cwd or (self.connection.session_cwd if self.connection else None),
            title=self.session.title,
            label=self.session.label,
            model_id=self.session.model_id,
            mode_id=self.session.mode_id,
            card_id=self.session.card_id or self.session.item_id,
            project_id=self.session.project_id,
            principal_id=self.session.principal_id,
            prompting=False,
            queue_paused=self._queue_paused,
            queued_prompts=queued,
            in_flight=None,
        )

    async def close(self) -> None:
        self._closed = True
        self._queue_paused = True
        if self._drain_task and not self._drain_task.done():
            self._drain_task.cancel()
        for req_id, fut in list(self._pending_permissions.items()):
            if not fut.done():
                fut.set_result(permission_cancelled())
            self._pending_permissions.pop(req_id, None)
            self._permission_requests.pop(req_id, None)
        self._append_transcript("session_closed", {})
        self._flush_transcript()
        if self.connection:
            await self.connection.disconnect()
            self.connection = None
        self.session.status = "closed"
        self.session.updated_at = datetime.now(UTC)
        self.store.save_session(self.session)


class AgentSessionManager:
    """Tracks many concurrent ACP sessions (one subprocess each)."""

    def __init__(self, settings: Settings, store: Store) -> None:
        self.settings = settings
        self.store = store
        self._runtimes: dict[str, AgentSessionRuntime] = {}
        self._quiescing = False
        self._accepting = True
        self._last_error: str | None = None
        self._resume_on_start = True
        self._default_label = "default"
        self._lock = asyncio.Lock()

    # Compatibility aliases used by existing call sites
    @property
    def connected(self) -> bool:
        return any(rt.connected for rt in self._runtimes.values())

    @property
    def last_error(self) -> str | None:
        return self._last_error

    @property
    def prompting(self) -> bool:
        return any(rt.prompting for rt in self._runtimes.values())

    @property
    def quiescing(self) -> bool:
        return self._quiescing

    def get(self, session_id: str) -> AgentSessionRuntime | None:
        return self._runtimes.get(session_id)

    def list_sessions(self) -> list[AgentSession]:
        return [rt.session for rt in self._runtimes.values()]

    def list_runtimes(self) -> list[AgentSessionRuntime]:
        return list(self._runtimes.values())

    def progress(self) -> QuiesceProgress:
        active = sum(1 for rt in self._runtimes.values() if rt.connected)
        queued = sum(len(rt._queue) + (1 if rt._in_flight else 0) for rt in self._runtimes.values())
        return QuiesceProgress(
            phase="quiescing" if self._quiescing else ("prompting" if self.prompting else "idle"),
            connected=self.connected,
            prompting=self.prompting,
            quiescing=self._quiescing,
            active_sessions=active,
            queued_prompts=queued,
            message=self._status_message(),
            done=False,
            error=self._last_error,
            snapshot={
                "sessions": [
                    {
                        "session_id": rt.session_id,
                        "external_session_id": rt.session.external_session_id,
                        "status": rt.session.status,
                        "cwd": rt.session.cwd,
                        "label": rt.session.label,
                        "prompting": rt.prompting,
                        "queued": len(rt._queue),
                    }
                    for rt in self._runtimes.values()
                ]
            },
        )

    def _status_message(self) -> str:
        active = sum(1 for rt in self._runtimes.values() if rt.connected)
        prompting = sum(1 for rt in self._runtimes.values() if rt.prompting)
        queued = sum(len(rt._queue) for rt in self._runtimes.values())
        if self._quiescing and prompting:
            return f"Waiting for {prompting} ACP turn{'s' if prompting != 1 else ''} to finish…"
        if self._quiescing:
            return "Capturing ACP session state…"
        if prompting:
            return f"{prompting} ACP session{'s' if prompting != 1 else ''} working, {queued} queued"
        if active:
            return f"{active} ACP session{'s' if active != 1 else ''} idle, {queued} queued"
        return "ACP agent offline"

    def should_auto_approve(self, principal_id: str | None) -> bool:
        """Resolve auto-approve: user prefs (if present) → global prefs → False (UI prompt)."""
        user_id = None
        if principal_id and principal_id.startswith("user:"):
            user_id = principal_id[5:]
        if user_id:
            user_store = get_preferences_store(self.settings.data_dir, user_id=user_id)
            if user_store.path.exists():
                return bool(user_store.load().agent_auto_approve_permissions)
        return bool(
            get_preferences_store(self.settings.data_dir).load().agent_auto_approve_permissions
        )

    def set_auto_approve(
        self,
        value: bool,
        *,
        scope: Literal["user", "global"] = "user",
        principal_id: str | None = None,
    ) -> None:
        if scope == "global":
            get_preferences_store(self.settings.data_dir).update(
                agent_auto_approve_permissions=value
            )
            return
        user_id = None
        if principal_id and principal_id.startswith("user:"):
            user_id = principal_id[5:]
        if not user_id:
            get_preferences_store(self.settings.data_dir).update(
                agent_auto_approve_permissions=value
            )
            return
        get_preferences_store(self.settings.data_dir, user_id=user_id).update(
            agent_auto_approve_permissions=value
        )

    async def start(self, *, resume: bool | None = None) -> None:
        if not self.settings.agent_enabled:
            logger.info("Instance agent disabled")
            return
        if resume is not None:
            self._resume_on_start = resume
        self._accepting = True
        self._quiescing = False

        if self._resume_on_start:
            snapshot = load_quiesce_snapshot(self.settings.data_dir)
            if snapshot and snapshot.resume and snapshot.sessions:
                for sess in snapshot.sessions:
                    try:
                        await self._resume_from_snapshot(sess, snapshot)
                    except Exception as exc:
                        self._last_error = str(exc)
                        logger.exception("Failed to resume session %s", sess.session_id)
                # Legacy top-level queue → default session
                if snapshot.queued_prompts:
                    default = await self.attach_default()
                    for item in snapshot.queued_prompts:
                        item.session_id = default.session_id
                        default._queue.append(item)
                    default._start_drain()
                clear_quiesce_snapshot(self.settings.data_dir)
                return

        # Ensure a default session exists for the instance chat surface.
        try:
            await self.attach_default()
            self._last_error = None
        except Exception as exc:
            self._last_error = str(exc)
            logger.exception("Failed to start default agent session")

    async def _resume_from_snapshot(
        self, snap: SessionSnapshot, full: QuiesceSnapshot
    ) -> AgentSessionRuntime:
        existing = self.store.get_session(snap.session_id) if snap.session_id else None
        session = existing or AgentSession(
            id=snap.session_id or str(uuid4()),
            agent_name=snap.agent_name or "instance",
            external_session_id=snap.external_session_id,
            status="idle",
            cwd=snap.cwd,
            title=snap.title,
            label=snap.label,
            model_id=snap.model_id,
            mode_id=snap.mode_id,
            card_id=snap.card_id,
            project_id=snap.project_id,
            principal_id=snap.principal_id,
        )
        session.cwd = snap.cwd or session.cwd
        session.label = snap.label or session.label
        session.title = snap.title or session.title
        runtime = AgentSessionRuntime(self, session)
        queued = list(snap.queued_prompts)
        if snap.in_flight:
            queued.insert(0, snap.in_flight)
        await runtime.start(
            resume_external_id=snap.external_session_id,
            queued_prompts=queued,
            queue_paused=snap.queue_paused,
        )
        self._runtimes[runtime.session_id] = runtime
        return runtime

    async def reconnect(self) -> bool:
        """Reconnect the default session (compat with chrome reconnect button)."""
        try:
            runtime = await self.attach_default()
            if runtime.connected:
                self._last_error = None
                return True
            # Force recreate
            await runtime.close()
            self._runtimes.pop(runtime.session_id, None)
            runtime = await self.create_session(label=self._default_label, title="Instance agent")
            self._last_error = None
            return runtime.connected
        except Exception as exc:
            self._last_error = str(exc)
            logger.exception("Agent reconnect failed")
            return False

    async def create_session(
        self,
        *,
        label: str | None = None,
        title: str | None = None,
        cwd: str | None = None,
        principal_id: str | None = None,
        card_id: str | None = None,
        project_id: str | None = None,
        agent_env: dict[str, str] | None = None,
        resume_external_id: str | None = None,
        existing: AgentSession | None = None,
        surface: str | None = None,
        provider_override: str | None = None,
        project_tool_config: dict | None = None,
    ) -> AgentSessionRuntime:
        if not self.settings.agent_enabled:
            raise RuntimeError("Agent disabled")
        if not self._accepting or self._quiescing:
            raise RuntimeError("Agent is quiescing")

        surface_key = surface or surface_for_label(label, project_id=project_id)
        ctx = AgentInvocationContext(
            surface=surface_key,
            principal_id=principal_id,
            card_id=card_id,
            project_id=project_id,
            provider_override=provider_override,
        )
        # When resuming an existing session, keep its provider unless explicitly overridden.
        if existing and existing.agent_name and existing.agent_name not in {
            "instance",
            "",
        } and not provider_override:
            provider_id = existing.agent_name
            from pa.acp.providers.registry import get_provider
            from pa.acp.providers.resolve import _spawn_overrides

            cmd_o, args_o = _spawn_overrides(self.settings, provider_id)
            resolved_spec = get_provider(provider_id).resolve_spawn(
                command_override=cmd_o,
                args_override=args_o,
                extra_env=agent_env,
                data_dir=self.settings.data_dir,
            )
            source = "session"
        else:
            resolved = resolve_agent_provider(
                self.settings,
                ctx,
                project_tool_config=project_tool_config,
                extra_env=agent_env,
            )
            provider_id = resolved.provider_id
            resolved_spec = resolved.spec
            source = resolved.source

        session = existing or AgentSession(
            agent_name=provider_id,
            status="connecting",
            cwd=cwd or str(self.settings.data_dir),
            title=title,
            label=label,
            principal_id=principal_id,
            card_id=card_id,
            project_id=project_id,
            item_id=card_id,
        )
        if existing:
            if label is not None:
                session.label = label
            if title is not None:
                session.title = title
            if cwd is not None:
                session.cwd = cwd
            if principal_id is not None:
                session.principal_id = principal_id
            if card_id is not None:
                session.card_id = card_id
                session.item_id = card_id
            if project_id is not None:
                session.project_id = project_id
            if not provider_override and session.agent_name in {"instance", ""}:
                session.agent_name = provider_id
            elif provider_override or not existing:
                session.agent_name = provider_id
            elif source != "session":
                # New resolution for fresh connect without resume identity mismatch
                if not resume_external_id:
                    session.agent_name = provider_id
        else:
            session.agent_name = provider_id
        self.store.save_session(session)

        runtime = AgentSessionRuntime(self, session, agent_env=agent_env)
        try:
            await runtime.start(
                resume_external_id=resume_external_id,
                provider_spec=resolved_spec,
            )
            self._last_error = None
        except Exception as exc:
            self._last_error = str(exc)
            session.status = "disconnected"
            self.store.save_session(session)
            raise
        self._runtimes[runtime.session_id] = runtime
        return runtime

    async def attach_default(
        self,
        *,
        principal_id: str | None = None,
        cwd: str | None = None,
        agent_env: dict[str, str] | None = None,
        provider_override: str | None = None,
    ) -> AgentSessionRuntime:
        async with self._lock:
            for rt in self._runtimes.values():
                if rt.session.label == self._default_label and rt.connected and not rt._closed:
                    return rt
            existing = self.store.get_session_by_label(self._default_label)
            if existing and existing.id in self._runtimes:
                rt = self._runtimes[existing.id]
                if rt.connected and not rt._closed:
                    return rt
            return await self.create_session(
                label=self._default_label,
                title="Instance agent",
                cwd=cwd,
                principal_id=principal_id,
                agent_env=agent_env,
                existing=existing if existing and existing.status != "closed" else None,
                resume_external_id=(
                    existing.external_session_id
                    if existing and existing.status != "closed"
                    else None
                ),
                surface=SURFACE_CHAT_DEFAULT,
                provider_override=provider_override,
            )

    def enqueue_prompt(
        self,
        message: str,
        *,
        images: list[ImageAttachment] | None = None,
        card_id: str | None = None,
        project_id: str | None = None,
        principal_id: str | None = None,
        cwd: str | None = None,
        agent_env: dict[str, str] | None = None,
        source: str = "api",
        session_id: str | None = None,
    ) -> QueuedPrompt:
        runtime = None
        if session_id:
            runtime = self._runtimes.get(session_id)
        if runtime is None:
            # Best-effort: use default if present
            for rt in self._runtimes.values():
                if rt.session.label == self._default_label:
                    runtime = rt
                    break
        if runtime is None:
            item = QueuedPrompt(
                message=message,
                images=list(images or []),
                session_id=session_id,
                card_id=card_id,
                project_id=project_id,
                principal_id=principal_id,
                cwd=cwd,
                agent_env=dict(agent_env or {}),
                source=source,
            )
            return item
        return runtime.enqueue(
            message,
            images=images,
            card_id=card_id,
            project_id=project_id,
            principal_id=principal_id,
            cwd=cwd,
            agent_env=agent_env,
            source=source,
        )

    async def prompt(
        self,
        message: str,
        item_id: str | None = None,
        *,
        images: list[ImageAttachment] | None = None,
        principal_id: str | None = None,
        project_id: str | None = None,
        agent_env: dict[str, str] | None = None,
        cwd: str | None = None,
        session_id: str | None = None,
        action: PromptAction = "append",
        _from_queue: bool = False,
        wait: bool = True,
        surface: str | None = None,
        provider_override: str | None = None,
    ) -> str:
        if session_id:
            runtime = self._runtimes.get(session_id)
            if not runtime:
                raise RuntimeError(f"Unknown session: {session_id}")
        else:
            if surface == SURFACE_EXECUTION:
                runtime = await self.create_session(
                    label="execution",
                    title="Execution",
                    cwd=cwd,
                    principal_id=principal_id,
                    project_id=project_id,
                    card_id=item_id,
                    agent_env=agent_env,
                    surface=SURFACE_EXECUTION,
                    provider_override=provider_override,
                )
            else:
                runtime = await self.attach_default(
                    principal_id=principal_id,
                    cwd=cwd,
                    agent_env=agent_env,
                    provider_override=provider_override,
                )
        return await runtime.prompt(
            message,
            images=images,
            item_id=item_id,
            principal_id=principal_id,
            project_id=project_id,
            agent_env=agent_env,
            cwd=cwd,
            action=action,
            _from_queue=_from_queue,
            wait=wait,
        )

    async def stop(self) -> None:
        for runtime in list(self._runtimes.values()):
            try:
                if runtime.connection:
                    await runtime.connection.disconnect()
            except Exception:
                logger.exception("Error disconnecting session %s", runtime.session_id)
        self._runtimes.clear()

    async def quiesce(
        self,
        *,
        reason: str = "restart",
        timeout: float = 300.0,
        on_progress: Callable[[QuiesceProgress], Awaitable[None] | None] | None = None,
    ) -> QuiesceSnapshot:
        self._quiescing = True
        self._accepting = False

        async def _emit(phase: str, *, done: bool = False, error: str | None = None) -> None:
            progress = self.progress()
            progress.phase = phase
            progress.done = done
            progress.error = error
            progress.message = self._status_message() if not done else (
                "ACP sessions quiesced" if not error else error
            )
            if on_progress:
                result = on_progress(progress)
                if asyncio.iscoroutine(result):
                    await result

        await _emit("quiescing")
        deadline = asyncio.get_running_loop().time() + timeout
        while any(rt.prompting for rt in self._runtimes.values()):
            if asyncio.get_running_loop().time() >= deadline:
                await _emit("timeout", done=True, error="Timed out waiting for ACP turn")
                raise TimeoutError("Timed out waiting for active ACP session to finish")
            await _emit("waiting")
            await asyncio.sleep(_QUIESCE_POLL_SECONDS)

        await _emit("capturing")
        sessions: list[SessionSnapshot] = []
        for runtime in list(self._runtimes.values()):
            snap = runtime.to_session_snapshot()
            sessions.append(snap)
            runtime.session.status = "quiesced"
            runtime.session.updated_at = datetime.now(UTC)
            self.store.save_session(runtime.session)
            runtime._flush_transcript()
            if runtime.connection:
                await runtime.connection.disconnect()
                runtime.connection = None

        snapshot = QuiesceSnapshot(
            reason=reason,
            resume=True,
            sessions=sessions,
            queued_prompts=[],
        )
        save_quiesce_snapshot(self.settings.data_dir, snapshot)
        self._runtimes.clear()

        progress = QuiesceProgress(
            phase="done",
            connected=False,
            prompting=False,
            active_sessions=snapshot.active_count,
            queued_prompts=snapshot.queued_count,
            message=(
                f"Quiesced {snapshot.active_count} ACP session"
                f"{'' if snapshot.active_count == 1 else 's'}"
                f", {snapshot.queued_count} queued prompt"
                f"{'' if snapshot.queued_count == 1 else 's'}"
            ),
            done=True,
            snapshot=snapshot.model_dump(mode="json"),
        )
        if on_progress:
            result = on_progress(progress)
            if asyncio.iscoroutine(result):
                await result
        return snapshot


# Back-compat alias
InstanceAgent = AgentSessionManager

_instance_agent: AgentSessionManager | None = None


def get_instance_agent(settings: Settings, store: Store) -> AgentSessionManager:
    global _instance_agent
    if _instance_agent is None:
        _instance_agent = AgentSessionManager(settings, store)
    return _instance_agent


def reset_instance_agent() -> None:
    global _instance_agent
    _instance_agent = None
