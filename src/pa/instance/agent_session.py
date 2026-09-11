"""Multi-session ACP agent runtime for a PA instance."""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager, nullcontext
from datetime import UTC, datetime, timedelta
from functools import wraps
from pathlib import Path
from threading import RLock
from typing import TYPE_CHECKING, Any, Literal
from uuid import uuid4

from pa.acp.client import (
    AgentConnection,
    normalize_session_update,
    permission_cancelled,
    permission_selected,
)
from pa.acp.configuration import (
    SessionConfigurationRequest,
    normalized_session_config_json,
)
from pa.acp.environment import (
    ASSIGNED_SERVICE_DISPATCH_ENV,
    ASSIGNED_SERVICE_MODE_ENV,
    ASSIGNED_SERVICE_SESSION_ENV,
)
from pa.acp.final_message import (
    assemble_final_assistant_message,
    is_agent_message_type,
    likely_user_input_request,
)
from pa.acp.providers.registry import DEFAULT_PROVIDER_ID, known_provider_ids
from pa.acp.providers.resolve import resolve_agent_provider, resolve_provider_id
from pa.acp.startup_trace import SessionStartupTrace
from pa.acp.surfaces import (
    SURFACE_CHAT_DEFAULT,
    SURFACE_EXECUTION,
    AgentInvocationContext,
    surface_for_label,
)
from pa.agent.context import compose_session_prompt
from pa.browser.manager import BrowserManager
from pa.config import Settings
from pa.core.async_runtime import AsyncRuntimeClosed
from pa.core.preferences import get_preferences_store
from pa.domain.models import AgentSession, RestartHandoff, TranscriptEvent
from pa.domain.store import Store
from pa.execution.progress import sanitize_text
from pa.execution.session_presentation import build_session_presentation
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
from pa.knowledge.capture import capture_from_updates
from pa.repository.workspace import (
    WorkspaceManager,
    WorkspaceProvisioningError,
    context_environment,
    provider_execution_policy,
)

if TYPE_CHECKING:
    from pa.core.async_runtime import AsyncRuntime

logger = logging.getLogger(__name__)

class WorkspaceBindingMismatch(WorkspaceProvisioningError):
    """Recovery found a different workspace; the original fence remains binding."""

    remedy = (
        "Recovery found a workspace that differs from this session's original binding. "
        "Inspect the original repository, branch, base commit and lease before retrying. "
        "Restore that exact workspace through PA; do not replace the binding. If it "
        "cannot be restored, preserve this conversation and explicitly start a new "
        "linked attempt after the original dispatch is terminal."
    )


_RETRY_SECONDS = 30
_RECOVERY_BASE_SECONDS = 5
_RECOVERY_MAX_SECONDS = 300
_RECOVERY_MAX_ATTEMPTS = 8
_QUIESCE_POLL_SECONDS = 0.4
TURN_WAITING_SECONDS = 15.0
TRANSCRIPT_WINDOW_LIMIT = 1000
_TURN_STREAM_EVENT_TYPES = {
    "agent_message_chunk",
    "agent_message",
    "agent_thought_chunk",
    "user_message_chunk",
    "tool_call",
    "tool_call_update",
    "plan",
}
PromptAction = Literal["append", "prepend", "interrupt"]


class PromptAdmissionBlocked(RuntimeError):
    """A durable prompt was accepted but policy blocked provider delivery."""


class SessionAdmissionInProgress(RuntimeError):
    """Another local task owns startup for this exact session."""


def _fenced_session_admission(method):
    """Keep recovery from treating an unpublished provider as an abandoned one."""
    signature = inspect.signature(method)

    @wraps(method)
    async def admitted(self, *args, **kwargs):
        arguments = signature.bind(self, *args, **kwargs).arguments
        existing = arguments.get("existing")
        snapshot = arguments.get("snap")
        session_id = (
            existing.id if existing is not None
            else snapshot.session_id if snapshot is not None
            else arguments.get("session_id")
        ) or str(uuid4())
        if snapshot is None and existing is None:
            kwargs["session_id"] = session_id
        if session_id in self._admitting_sessions:
            raise SessionAdmissionInProgress("Exact session admission is already in progress")
        current = self.get(session_id)
        if current and not current._closed and current.connected:
            raise SessionAdmissionInProgress("Exact session already has a live runtime")
        self._admitting_sessions.add(session_id)
        try:
            return await method(self, *args, **kwargs)
        finally:
            self._admitting_sessions.discard(session_id)

    return admitted

_AUTOMATIC_PROMPT_PREFIXES = (
    "card-enrichment:",
    "card-reconciliation:",
    "pr-supervisor",
    "post-turn",
    "evaluation",
    "reconciliation",
    "dispatch",
    "goal",
    "recovery",
    "restart-handoff:",
)


def _is_automatic_source(source: str) -> bool:
    normalized = (source or "api").strip().casefold()
    return normalized.startswith(_AUTOMATIC_PROMPT_PREFIXES)


def _prompt_authority(source: str, action: PromptAction) -> tuple[int, str]:
    """Return deterministic durable ordering; lower values run first."""
    normalized = (source or "api").strip().casefold()
    if action == "interrupt":
        return 0, "operator_interrupt"
    if normalized in {"api", "ui", "operator", "in_flight"}:
        return 10, "operator_input"
    if normalized.startswith(_AUTOMATIC_PROMPT_PREFIXES):
        return 200, "automatic_reconciliation"
    return 100, f"source:{normalized or 'unknown'}"
_DURABLE_RUNTIME_KEY = "durable_runtime"
RECOVERY_BLOCKED_STATUS = "recovery_blocked"
AUTO_RECOVERY_SESSION_STATUSES = frozenset(
    {
        "provisioning",
        "provisioning_failed",
        "connecting",
        "configuring",
        "configuration_failed",
        "prompting",
        "recoverable_interrupted",
    }
)
RECOVERY_RETAINED_SESSION_STATUSES = AUTO_RECOVERY_SESSION_STATUSES | frozenset(
    {
        "connected",
        "idle",
        "disconnected",
        "quiesced",
        RECOVERY_BLOCKED_STATUS,
    }
)
_EAGER_DURABLE_LIFECYCLES = frozenset(
    {
        "admitted",
        "prompting",
        "queued",
        "permission_pending",
        "completion_pending",
        "reconciliation_pending",
        "recoverable_interrupted",
    }
)


def _project_recovery_block(exc: BaseException) -> bool:
    message = str(exc).lower()
    return any(
        marker in message
        for marker in (
            "project has no linked repositories",
            "project is not available on this instance",
            "project repository links are not materialized on this instance",
            "is not available on this instance; sync or link",
        )
    )


class AgentStartupNotReady(RuntimeError):
    """Raised when session traffic arrives before durable recovery finishes."""


class AgentSessionRecoveryError(RuntimeError):
    """Raised when a durable PA session cannot be recovered."""


def _session_dir(data_dir: Path, session_id: str) -> Path:
    path = data_dir / "sessions" / session_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def _live_event_size(event: dict[str, Any]) -> int:
    """Conservative byte bound without serializing provider payloads on-loop."""
    pending: list[Any] = [event]
    size = 0
    visited = 0
    while pending:
        value = pending.pop()
        visited += 1
        if visited > 4096 or size > 2 * 1024 * 1024:
            return 2 * 1024 * 1024 + 1
        if isinstance(value, str):
            size += len(value) * 4 + 16
        elif isinstance(value, dict):
            if len(value) > 4096:
                return 2 * 1024 * 1024 + 1
            pending.extend(value.keys())
            pending.extend(value.values())
            size += 64
        elif isinstance(value, (list, tuple)):
            if len(value) > 4096:
                return 2 * 1024 * 1024 + 1
            pending.extend(value)
            size += 64
        elif value is None or isinstance(value, (bool, int, float)):
            size += 64
        else:
            return 2 * 1024 * 1024 + 1
    return size


class AgentSessionRuntime:
    """Owns one ACP subprocess + connection for a single PA session."""

    def __init__(
        self,
        manager: AgentSessionManager,
        session: AgentSession,
        *,
        agent_env: dict[str, str] | None = None,
        mcp_private_env: dict[str, str] | None = None,
        initial_transcript_seq: int | None = None,
        startup_trace: SessionStartupTrace | None = None,
    ) -> None:
        self.manager = manager
        self.async_runtime = (
            manager.async_runtime if isinstance(manager, AgentSessionManager) else None
        )
        self.settings = manager.settings
        self.store = manager.store
        self.session = session
        self.agent_env = dict(agent_env or {})
        self.agent_env.setdefault("PA_BROWSER_SESSION_ID", session.id)
        # This mapping is never merged into the provider OS environment, prompt
        # snapshots, execution context, or transcripts. Values passed through an
        # ACP MCP server descriptor must nevertheless be non-secret.
        self.mcp_private_env = dict(mcp_private_env or {})
        self.startup_trace = startup_trace
        self.connection: AgentConnection | None = None
        self._prompt_lock = asyncio.Lock()
        self._prompt_admission_lock = asyncio.Lock()
        self._queue: list[QueuedPrompt] = []
        self._queue_paused = False
        self._in_flight: QueuedPrompt | None = None
        self._draining_prompt: QueuedPrompt | None = None
        self._restart_receipts: dict[str, RestartHandoff] = {}
        self._drain_task: asyncio.Task[None] | None = None
        self._subscribers: list[asyncio.Queue[dict[str, Any]]] = []
        self._recent_live_events: deque[dict[str, Any]] = deque()
        self._recent_live_bytes = 0
        self._recent_live_sizes: deque[int] = deque()
        self._pending_permissions: dict[str, asyncio.Future[Any]] = {}
        self._permission_requests: dict[str, dict[str, Any]] = {}
        self._permission_notification_ids: dict[str, str] = {}
        self._pending_elicitations: dict[str, asyncio.Future[Any]] = {}
        self._elicitation_requests: dict[str, dict[str, Any]] = {}
        self._elicitation_notification_ids: dict[str, str] = {}
        self._seq = (
            initial_transcript_seq
            if initial_transcript_seq is not None
            else self.store.next_transcript_seq(session.id) - 1
        )
        self._transcript_buffer: list[TranscriptEvent] = []
        self._transcript_queue: asyncio.Queue[list[TranscriptEvent]] = asyncio.Queue(
            maxsize=128
        )
        self._transcript_writer_task: asyncio.Task[None] | None = None
        self._closed = False
        self._turn_started_at: datetime | None = None
        self._turn_agent_events: list[dict[str, Any]] = []
        self._turn_streamed = False
        self._runtime_observed_at: datetime = datetime.now(UTC)
        self._connection_generation = 0

    async def _offload(
        self, operation: str, call, *args, timeout: float | None = None, **kwargs
    ):
        async_runtime = getattr(self, "async_runtime", None)
        if async_runtime:
            return await async_runtime.run_blocking(
                operation, call, *args, timeout=timeout, **kwargs
            )
        kwargs.pop("wait_for_completion", None)
        return await asyncio.to_thread(call, *args, **kwargs)

    def _save_session_preserving_external_browser(self) -> None:
        persisted = self.store.get_session(self.session_id)
        if persisted:
            current_connection = (persisted.config_json or {}).get("provider_connection_id")
            own_connection = (self.session.config_json or {}).get("provider_connection_id")
            if current_connection and current_connection != own_connection:
                # A superseded runtime may finish flushing after its replacement
                # starts. Preserve the current owner's queue and lifecycle.
                return
            # These fields are owned by conversation actions, not provider turns.
            # A runtime may predate an archive/pin operation or provider teardown.
            self.session.archived_at = persisted.archived_at
            self.session.archive_reason = persisted.archive_reason
            self.session.pinned_at = persisted.pinned_at
        persisted_browser = dict(
            ((persisted.config_json or {}).get("browser") or {}) if persisted else {}
        )
        if persisted_browser:
            config = dict(self.session.config_json or {})
            config["browser"] = persisted_browser
            self.session.config_json = config
        self.store.save_session(
            self.session,
            expected_connection_id=(self.session.config_json or {}).get("provider_connection_id") or "",
        )

    async def _save_session_preserving_external_browser_async(self) -> None:
        await self._offload(
            "sqlite.agent_session_save",
            self._save_session_preserving_external_browser,
        )

    def _checkpoint_runtime(self, *, lifecycle: str | None = None) -> None:
        """Persist recoverable execution ownership before an API acknowledges it."""
        config = dict(self.session.config_json or {})
        previous = dict(config.get(_DURABLE_RUNTIME_KEY) or {})
        config[_DURABLE_RUNTIME_KEY] = {
            "version": 1,
            "lifecycle": lifecycle or previous.get("lifecycle") or "admitted",
            "queue_paused": self._queue_paused,
            "queued_prompts": [item.model_dump(mode="json") for item in self._queue],
            "in_flight": (
                self._in_flight.model_dump(mode="json") if self._in_flight else None
            ),
            "last_event_cursor": self._seq,
            "pending_permissions": list(self._permission_requests.values()),
            "pending_elicitations": list(self._elicitation_requests.values()),
            "pending_interaction": previous.get("pending_interaction"),
            "updated_at": datetime.now(UTC).isoformat(),
        }
        self.session.config_json = config
        self.session.updated_at = datetime.now(UTC)
        self._save_session_preserving_external_browser()

    async def _checkpoint_runtime_async(self, *, lifecycle: str | None = None) -> None:
        await self._offload(
            "sqlite.agent_runtime_checkpoint",
            self._checkpoint_runtime,
            lifecycle=lifecycle,
        )

    @property
    def session_id(self) -> str:
        return self.session.id

    @property
    def connected(self) -> bool:
        return bool(self.connection and self.connection.connected)

    @property
    def prompting(self) -> bool:
        # The in-flight item is the runtime's authoritative turn lifecycle.
        # Connection status and the prompt lock can remain active briefly while
        # terminal events are flushed, which must not make a refreshed UI
        # resurrect a completed turn.
        return self._in_flight is not None

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
        if "presentation" not in event:
            try:
                presentation = build_session_presentation(
                    self.session,
                    runtime=self,
                    quiescing=bool(
                        getattr(getattr(self, "manager", None), "quiescing", False)
                    ),
                    startup_complete=bool(
                        getattr(
                            getattr(self, "manager", None),
                            "startup_complete",
                            True,
                        )
                    ),
                )
            except (AttributeError, TypeError, ValueError):
                presentation = None
            if presentation is not None:
                event = {**event, "presentation": presentation}
        for sub in self._subscribers:
            try:
                sub.put_nowait(event)
            except asyncio.QueueFull:
                # Favor current state over stale output deltas. The persisted
                # transcript remains lossless and can fill gaps on reconnect.
                sub.get_nowait()
                sub.put_nowait(event)

    def _append_transcript(
        self, event_type: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        self._runtime_observed_at = datetime.now(UTC)
        self._seq += 1
        te = TranscriptEvent(
            session_id=self.session_id,
            seq=self._seq,
            event_type=event_type,
            payload=payload,
        )
        self._transcript_buffer.append(te)
        if len(self._transcript_buffer) > 4096:
            self._queue_paused = True
            raise RuntimeError(
                "Transcript persistence backlog exceeded 4096 events; session paused"
            )
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
        size = _live_event_size(event)
        self._recent_live_events.append(event)
        self._recent_live_sizes.append(size)
        self._recent_live_bytes += size
        while len(self._recent_live_events) > 2048 or self._recent_live_bytes > 2 * 1024 * 1024:
            self._recent_live_events.popleft()
            self._recent_live_bytes -= self._recent_live_sizes.popleft()
        self._emit_live(event)
        return event

    def _flush_transcript(self) -> None:
        if not self._transcript_buffer:
            return
        if not getattr(self, "async_runtime", None):
            batch = list(self._transcript_buffer)
            self._transcript_buffer.clear()
            try:
                self.store.append_transcript_events(batch)
            except Exception:
                logger.exception("Failed to persist transcript events")
                self._transcript_buffer = batch + self._transcript_buffer
            return
        if self._transcript_queue.full():
            return
        batch = list(self._transcript_buffer)
        self._transcript_buffer.clear()
        self._transcript_queue.put_nowait(batch)
        if not self._transcript_writer_task or self._transcript_writer_task.done():
            writer = self._write_transcripts()
            try:
                self._transcript_writer_task = asyncio.create_task(
                    writer,
                    name=f"pa-transcript-{self.session_id}",
                )
            except RuntimeError:
                # Task creation can fail after loop shutdown has begun. Close the
                # unscheduled coroutine and persist every batch that would otherwise
                # remain stranded behind queue.join().
                writer.close()
                self._transcript_writer_task = None
                batches: list[list[TranscriptEvent]] = []
                while not self._transcript_queue.empty():
                    batches.append(self._transcript_queue.get_nowait())
                    self._transcript_queue.task_done()
                events = [event for queued in batches for event in queued]
                try:
                    self.store.append_transcript_events(events)
                except Exception:
                    logger.exception("Failed to persist transcript events")
                    self._transcript_buffer = events + self._transcript_buffer

    async def _write_transcripts(self) -> None:
        while not self._transcript_queue.empty():
            batch = await self._transcript_queue.get()
            delay = 0.05
            try:
                while True:
                    try:
                        await self._offload(
                            "sqlite.transcript_append",
                            self.store.append_transcript_events,
                            batch,
                            # A timed-out thread still owns this batch. Await
                            # its actual result instead of starting concurrent
                            # retries of the same write every 30 seconds.
                            wait_for_completion=True,
                        )
                        break
                    except AsyncRuntimeClosed:
                        # Shutdown has closed the thread pool. Retrying would
                        # spin forever, block queue.join(), and hang stop().
                        try:
                            self.store.append_transcript_events(batch)
                        except Exception:
                            logger.exception(
                                "Failed to persist transcript events during shutdown"
                            )
                            self._transcript_buffer = batch + self._transcript_buffer
                        break
                    except Exception:
                        logger.exception(
                            "Failed to persist transcript events; retrying"
                        )
                        await asyncio.sleep(delay)
                        delay = min(delay * 2, 2.0)
            except asyncio.CancelledError:
                # Cancellation can also arrive during retry backoff. Keep the
                # whole batch, including its immutable IDs, available to drain.
                self._transcript_buffer = batch + self._transcript_buffer
                raise
            finally:
                self._transcript_queue.task_done()
            self._flush_transcript()

    async def _drain_transcripts(
        self, *, timeout: float | None = 10.0, raise_on_timeout: bool = False
    ) -> None:
        self._flush_transcript()
        if not getattr(self, "async_runtime", None):
            return
        try:
            async with asyncio.timeout(timeout):
                await self._transcript_queue.join()
                self._flush_transcript()
                await self._transcript_queue.join()
        except TimeoutError:
            logger.error(
                "Timed out draining transcript for session %s", self.session_id
            )
            if raise_on_timeout:
                raise TimeoutError(
                    f"Transcript drain timed out: session={self.session_id} "
                    f"queued_batches={self._transcript_queue.qsize()} "
                    f"buffered_events={len(self._transcript_buffer)} "
                    f"writer_active={bool(self._transcript_writer_task and not self._transcript_writer_task.done())}"
                ) from None

    async def _reconcile_confirmed_collaboration(self, mode: Any) -> None:
        if mode not in {"default", "plan"}:
            return
        cfg = dict(self.session.config_json or {})
        values = dict(cfg.get("values") or {})
        values["collaboration_mode"] = mode
        cfg["values"] = values
        collaboration_state = dict(cfg.get("collaboration") or {})
        collaboration_state.update(
            current_mode=mode,
            pending=None,
            confirmed_by="provider_config_update",
            confirmed_at=datetime.now(UTC).isoformat(),
        )
        cfg["collaboration"] = collaboration_state
        self.session.config_json = cfg
        collaboration = getattr(self.manager, "collaboration_service", None)
        reconcile = getattr(collaboration, "reconcile_provider_mode", None)
        if callable(reconcile):
            result = reconcile(self.session, mode)
            if inspect.isawaitable(result):
                await result

    async def _on_acp_update(self, _external_session_id: str, update: Any) -> None:
        normalized = normalize_session_update(update)
        event_type = str(normalized.get("type") or "session_update")
        if event_type in _TURN_STREAM_EVENT_TYPES:
            self._turn_streamed = True
        if is_agent_message_type(event_type) and self._in_flight:
            self._turn_agent_events.append(dict(normalized))
        if event_type == "usage_update" and normalized.get("usage"):
            metrics = dict(self.session.metrics_json or {})
            metrics["usage"] = normalized["usage"]
            self.session.metrics_json = metrics
            await self._save_session_preserving_external_browser_async()
        configuration_state = (
            (self.session.config_json or {}).get("configuration") or {}
        ).get("state")
        if (
            event_type == "current_mode_update"
            and normalized.get("mode_id")
            and configuration_state != "applying"
        ):
            self.session.mode_id = normalized["mode_id"]
            await self._save_session_preserving_external_browser_async()
        if event_type in {"config_option_update", "config_options_update"} and configuration_state != "applying":
            options = normalized.get("config_options")
            if options is not None:
                cfg = dict(self.session.config_json or {})
                cfg["options"] = options
                cfg, confirmed = normalized_session_config_json(
                    cfg,
                    model_id=self.session.model_id,
                    mode_id=self.session.mode_id,
                )
                if confirmed.get("model_id"):
                    self.session.model_id = str(confirmed["model_id"])
                if confirmed.get("mode_id"):
                    self.session.mode_id = str(confirmed["mode_id"])
                confirmed_collaboration = confirmed["values"].get(
                    "collaboration_mode"
                )
                self.session.config_json = cfg
                if confirmed_collaboration in {"default", "plan"}:
                    await self._reconcile_confirmed_collaboration(
                        confirmed_collaboration
                    )
                await self._save_session_preserving_external_browser_async()
                if self.connection:
                    self.connection.config_options = options
        if event_type == "available_commands_update":
            collaboration = getattr(self.manager, "collaboration_service", None)
            if collaboration is not None:
                collaboration.capture_available_commands(
                    self.session,
                    list(normalized.get("available_commands") or []),
                    connection_generation=self._connection_generation,
                )
        self._append_transcript(event_type, normalized)
        await self._report_progress(normalized)

    async def _report_progress(self, update: dict[str, Any]) -> None:
        handler = self.manager.progress_handler
        if not handler:
            return
        try:
            if inspect.iscoroutinefunction(handler):
                result = handler(self.session_id, update)
            else:
                result = await self._offload(
                    "agent.progress_callback",
                    handler,
                    self.session_id,
                    update,
                    timeout=15.0,
                )
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            # Progress is a retryable side channel. It must not interrupt the
            # ACP transcript or successful agent work.
            logger.exception(
                "Failed to queue dispatch progress for session %s", self.session_id
            )

    @staticmethod
    def _stored_interaction_response(notice, *, retry_key: str):
        """Rebuild a protocol response after reconnect without exposing its value."""
        from pa.domain.notifications import InteractionResponse

        interaction = notice.interaction
        if not interaction or interaction.response is None:
            return None
        stored = interaction.response
        if stored == {"cancelled": True}:
            return InteractionResponse(idempotency_key=retry_key, cancel=True)
        if isinstance(stored, dict) and "choice_ids" in stored:
            return InteractionResponse(idempotency_key=retry_key, choice_ids=stored["choice_ids"])
        if isinstance(stored, dict) and "choice_id" in stored:
            return InteractionResponse(
                idempotency_key=retry_key, choice_id=str(stored["choice_id"])
            )
        if interaction.response_schema and isinstance(stored, dict):
            return InteractionResponse(idempotency_key=retry_key, fields=stored)
        return InteractionResponse(idempotency_key=retry_key, value=stored)

    async def _on_permission(
        self, _external_session_id: str, request: dict[str, Any]
    ) -> Any:
        if await self.manager.should_auto_approve_async(self.session.principal_id):
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
        notification_service = getattr(self.manager, "notification_service", None)
        notification_id = None
        if notification_service:
            from pa.domain.notifications import (
                InteractionChoice,
                InteractionKind,
                InteractionRequest,
                InteractionResponse,
                NotificationAction,
                NotificationCreate,
                NotificationPriority,
                NotificationType,
                NotificationVisibility,
                concise_notification_summary,
                interaction_notification_title,
            )

            options = request.get("options") or []
            choices = []
            for option in options:
                if not isinstance(option, dict):
                    continue
                option_id = option.get("optionId") or option.get("option_id")
                if not option_id:
                    continue
                choices.append(
                    InteractionChoice(
                        id=str(option_id),
                        label=str(
                            option.get("name")
                            or option.get("label")
                            or option.get("kind")
                            or option_id
                        ),
                        description=option.get("description"),
                        value=str(option_id),
                    )
                )
            tool = request.get("tool_call") or {}
            title = str(tool.get("title") or tool.get("kind") or "Agent permission")
            notice_title = interaction_notification_title(
                InteractionKind.ACP_PERMISSION, title, choices=choices
            )
            notice = await self._offload(
                "sqlite.notification_create",
                notification_service.create,
                NotificationCreate(
                    realm_id=self.session.realm_id,
                    visibility=(
                        NotificationVisibility.PRINCIPAL
                        if self.session.principal_id
                        else NotificationVisibility.REALM
                    ),
                    principal_id=self.session.principal_id,
                    type=NotificationType.INTERACTION,
                    priority=NotificationPriority.HIGH,
                    title=notice_title,
                    body=title,
                    summary=concise_notification_summary(
                        f"{self.session.agent_name} needs permission: {title}"
                    ),
                    card_id=self.session.card_id or self.session.item_id,
                    session_id=self.session.id,
                    dispatch_id=self.session.dispatch_id,
                    project_id=self.session.project_id,
                    destination_url=f"/agent?session={self.session.id}",
                    deduplication_key=(
                        "acp-permission:"
                        f"{self.session.id}:"
                        f"{tool.get('toolCallId') or tool.get('tool_call_id') or request_id}"
                    ),
                    actions=[
                        NotificationAction(
                            id="respond",
                            kind="respond",
                            label="Respond",
                            method="POST",
                        )
                    ],
                    interaction=InteractionRequest(
                        request_id=request_id,
                        kind=InteractionKind.ACP_PERMISSION,
                        prompt=title,
                        choices=choices,
                        allow_cancel=bool(
                            request.get(
                                "allowCancel", request.get("allow_cancel", True)
                            )
                        ),
                        protocol_method="session/request_permission",
                        protocol_request_id=request_id,
                        continuation_mode="protocol",
                    ),
                ),
                principal_id=self.session.principal_id or "user:local",
            )
            notification_id = notice.id
            self._permission_notification_ids[request_id] = notification_id

            async def deliver_permission(response: InteractionResponse) -> None:
                if future.done():
                    return
                if response.cancel:
                    future.set_result(permission_cancelled())
                else:
                    selected = response.choice_id
                    if not selected and isinstance(response.value, str):
                        selected = response.value
                    if not selected:
                        raise ValueError("Permission response requires an option")
                    future.set_result(permission_selected(selected))
                self._append_transcript(
                    "permission_resolved",
                    {
                        "request_id": request_id,
                        "notification_id": notification_id,
                        "response": "cancelled"
                        if response.cancel
                        else {"option_id": response.choice_id},
                    },
                )
                self._flush_transcript()
                await self._drain_transcripts()

            notification_service.register_delivery_handler(
                notification_id, deliver_permission
            )
            recovered = self._stored_interaction_response(
                notice, retry_key=f"protocol-retry:{request_id}:{uuid4()}"
            )
            if notice.interaction and notice.interaction.state.value == "expired":
                await deliver_permission(
                    InteractionResponse(
                        idempotency_key=f"protocol-expired:{request_id}", cancel=True
                    )
                )
            elif recovered:
                if notice.interaction and notice.interaction.state.value in {
                    "delivered",
                    "cancelled",
                }:
                    await deliver_permission(recovered)
                else:
                    await notification_service.respond(
                        notice,
                        recovered,
                        principal_id=(
                            notice.interaction.response_principal
                            or self.session.principal_id
                            or "user:local"
                        ),
                    )
        await self._checkpoint_runtime_async(lifecycle="permission_pending")
        try:
            return await future
        finally:
            if notification_service and notification_id:
                notification_service.unregister_delivery_handler(notification_id)
            self._pending_permissions.pop(request_id, None)
            self._permission_requests.pop(request_id, None)
            self._permission_notification_ids.pop(request_id, None)
            await self._checkpoint_runtime_async(
                lifecycle="prompting" if self._in_flight else "ready"
            )

    async def _on_elicitation(
        self, _external_session_id: str, request: dict[str, Any]
    ) -> dict[str, Any]:
        request_id = str(request.get("request_id") or uuid4())
        request["request_id"] = request_id
        if str(request.get("method") or "").endswith("/cancel"):
            service = getattr(self.manager, "notification_service", None)
            notification_id = self._elicitation_notification_ids.get(request_id)
            if service and notification_id:
                notice = await self._offload(
                    "sqlite.notification_read",
                    self.store.get_notification,
                    notification_id,
                    realm_id=self.session.realm_id,
                )
                if notice:
                    await self._offload(
                        "sqlite.notification_supersede",
                        service.supersede,
                        notice,
                        principal_id="system:provider",
                        idempotency_key=f"provider-cancel:{request_id}",
                    )
            existing = self._pending_elicitations.get(request_id)
            if existing and not existing.done():
                existing.set_result({"action": "cancel"})
            return {"action": "cancel"}
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        self._pending_elicitations[request_id] = future
        self._elicitation_requests[request_id] = request
        self._append_transcript("elicitation_request", request)
        notification_service = getattr(self.manager, "notification_service", None)
        notification_id = None
        if notification_service:
            from pa.domain.notifications import (
                InteractionChoice,
                InteractionKind,
                InteractionRequest,
                InteractionResponse,
                NotificationAction,
                NotificationCreate,
                NotificationPriority,
                NotificationType,
                NotificationVisibility,
                concise_notification_summary,
                interaction_notification_title,
            )

            raw_choices = request.get("choices") or request.get("options") or []
            choices = []
            for index, option in enumerate(raw_choices):
                if isinstance(option, dict):
                    choice_id = str(
                        option.get("id")
                        or option.get("value")
                        or option.get("optionId")
                        or index
                    )
                    label = str(option.get("label") or option.get("name") or choice_id)
                    value = option.get("value", choice_id)
                    description = option.get("description")
                else:
                    choice_id = str(index)
                    label = str(option)
                    value = option
                    description = None
                choices.append(
                    InteractionChoice(
                        id=choice_id,
                        label=label,
                        description=description,
                        value=value,
                    )
                )
            prompt = str(
                request.get("message")
                or request.get("prompt")
                or request.get("title")
                or "Agent input requested"
            )
            response_schema = (
                request.get("requestedSchema")
                or request.get("requested_schema")
                or request.get("schema")
            )
            notice_title = interaction_notification_title(
                InteractionKind.ACP_ELICITATION,
                prompt,
                choices=choices,
                response_schema=response_schema,
            )
            notice = await self._offload(
                "sqlite.notification_create",
                notification_service.create,
                NotificationCreate(
                    realm_id=self.session.realm_id,
                    visibility=(
                        NotificationVisibility.PRINCIPAL
                        if self.session.principal_id
                        else NotificationVisibility.REALM
                    ),
                    principal_id=self.session.principal_id,
                    type=NotificationType.INTERACTION,
                    priority=NotificationPriority.HIGH,
                    title=notice_title,
                    body=prompt,
                    summary=concise_notification_summary(prompt),
                    card_id=self.session.card_id or self.session.item_id,
                    session_id=self.session.id,
                    dispatch_id=self.session.dispatch_id,
                    project_id=self.session.project_id,
                    destination_url=f"/agent?session={self.session.id}",
                    deduplication_key=f"acp-elicitation:{self.session.id}:{request_id}",
                    actions=[
                        NotificationAction(
                            id="respond",
                            kind="respond",
                            label="Respond",
                            method="POST",
                            input_schema=response_schema,
                        )
                    ],
                    interaction=InteractionRequest(
                        request_id=request_id,
                        kind=InteractionKind.ACP_ELICITATION,
                        prompt=prompt,
                        choices=choices,
                        response_schema=response_schema,
                        allow_freeform=bool(
                            request.get(
                                "allowFreeform",
                                request.get(
                                    "allow_freeform",
                                    not choices and response_schema is None,
                                ),
                            )
                        ),
                        allow_cancel=bool(
                            request.get(
                                "allowCancel", request.get("allow_cancel", True)
                            )
                        ),
                        sensitive=bool(request.get("sensitive", False)),
                        protocol_method=str(
                            request.get("method") or "elicitation/create"
                        ),
                        protocol_request_id=request_id,
                        continuation_mode="protocol",
                    ),
                ),
                principal_id=self.session.principal_id or "user:local",
            )
            notification_id = notice.id
            self._elicitation_notification_ids[request_id] = notification_id

            async def deliver_elicitation(response: InteractionResponse) -> None:
                if future.done():
                    return
                if response.cancel:
                    future.set_result({"action": "cancel"})
                    return
                if response.fields is not None:
                    content: Any = response.fields
                elif response.choice_ids is not None:
                    by_id = {item.id: item.value for item in choices}
                    content = [by_id[key] for key in response.choice_ids]
                elif response.choice_id is not None:
                    choice = next(
                        (item for item in choices if item.id == response.choice_id),
                        None,
                    )
                    content = choice.value if choice else response.choice_id
                else:
                    content = response.value
                future.set_result({"action": "accept", "content": content})

            notification_service.register_delivery_handler(
                notification_id, deliver_elicitation
            )
            recovered = self._stored_interaction_response(
                notice, retry_key=f"protocol-retry:{request_id}:{uuid4()}"
            )
            if notice.interaction and notice.interaction.state.value == "expired":
                await deliver_elicitation(
                    InteractionResponse(
                        idempotency_key=f"protocol-expired:{request_id}", cancel=True
                    )
                )
            elif recovered:
                if notice.interaction and notice.interaction.state.value in {
                    "delivered",
                    "cancelled",
                }:
                    await deliver_elicitation(recovered)
                else:
                    await notification_service.respond(
                        notice,
                        recovered,
                        principal_id=(
                            notice.interaction.response_principal
                            or self.session.principal_id
                            or "user:local"
                        ),
                    )
        await self._checkpoint_runtime_async(lifecycle="elicitation_pending")
        try:
            return await future
        finally:
            if notification_service and notification_id:
                notification_service.unregister_delivery_handler(notification_id)
            self._pending_elicitations.pop(request_id, None)
            self._elicitation_requests.pop(request_id, None)
            self._elicitation_notification_ids.pop(request_id, None)
            await self._checkpoint_runtime_async(
                lifecycle="prompting" if self._in_flight else "ready"
            )

    async def start(
        self,
        *,
        resume_external_id: str | None = None,
        require_restore: bool = False,
        queued_prompts: list[QueuedPrompt] | None = None,
        queue_paused: bool = False,
        provider_spec=None,
        initial_configuration: SessionConfigurationRequest | None = None,
        _defer_drain: bool = False,
    ) -> AgentSession:
        if self.manager._should_abort_admission():
            raise RuntimeError("Agent is quiescing")
        browser_config = dict((self.session.config_json or {}).get("browser") or {})
        if browser_config.get("attached"):
            attachment = await self.manager.browser.attach(
                self.session_id,
                url=str(browser_config.get("url") or "about:blank"),
                width=browser_config.get("width"),
                height=browser_config.get("height"),
                device_scale_factor=float(
                    browser_config.get("device_scale_factor") or 1
                ),
            )
            self.agent_env.update(attachment.environment())
        session_dir = await self._offload(
            "agent.session_mkdir",
            _session_dir,
            self.settings.data_dir,
            self.session_id,
            timeout=10.0,
        )
        wire_path = session_dir / "wire.jsonl"
        provider_id = self.session.agent_name or DEFAULT_PROVIDER_ID
        if provider_id in {"instance", ""}:
            provider_id = DEFAULT_PROVIDER_ID
        self._connection_generation += 1
        self._runtime_observed_at = datetime.now(UTC)
        self.connection = AgentConnection(
            self.settings,
            self.store,
            agent_name=provider_id,
            provider_spec=provider_spec,
            on_update=self._on_acp_update,
            on_permission=self._on_permission,
            on_elicitation=self._on_elicitation,
            wire_path=wire_path,
            auto_approve=False,
            async_runtime=self.async_runtime,
            extra_env=self.agent_env,
            mcp_private_env=self.mcp_private_env,
            startup_trace=self.startup_trace,
        )
        try:
            self.session = await self.connection.connect(
                resume_external_id=resume_external_id,
                require_restore=require_restore,
                cwd=self.session.cwd,
                existing_session=self.session,
                title=self.session.title,
                label=self.session.label,
                principal_id=self.session.principal_id,
                card_id=self.session.card_id,
                project_id=self.session.project_id,
            )
            persisted = dict(
                ((self.session.config_json or {}).get("configuration") or {}).get(
                    "requested"
                )
                or {}
            )
            configuration = initial_configuration
            if configuration is None and persisted:
                configuration = SessionConfigurationRequest.from_dict(persisted)
            configuration_phase = (
                self.startup_trace.phase("session_configuration")
                if self.startup_trace
                else nullcontext()
            )
            with configuration_phase:
                if configuration is not None and not configuration.empty:
                    await self.connection.configure(configuration, force=True)
                    self.session = self.connection.session or self.session
        except Exception as exc:
            failed_configuration = dict(
                ((self.session.config_json or {}).get("configuration") or {})
            )
            self._append_transcript(
                "session_admission_failed",
                {
                    "stage": "configuration"
                    if failed_configuration.get("state") == "failed"
                    else "provider_startup",
                    "error": str(exc)[:1000],
                    "configuration": failed_configuration,
                },
            )
            self._flush_transcript()
            await self._drain_transcripts()
            try:
                await self.connection.disconnect()
            except Exception:
                logger.exception(
                    "Failed to terminate provider after startup failure for %s",
                    self.session_id,
                )
            self.connection = None
            if failed_configuration.get("state") == "failed":
                self.session.status = "configuration_failed"
                await self._save_session_preserving_external_browser_async()
            raise
        # Persist resolved provider id on the session.
        if self.connection and self.connection.agent_name:
            self.session.agent_name = self.connection.agent_name
            config = dict(self.session.config_json or {})
            config["provider_session_recovery"] = {
                "resume": bool(self.connection._resume_supported),
                "load": bool(self.connection._load_supported),
            }
            self.session.config_json = config
            from pa.execution.selection_audit import bind_confirmed_defaults

            bind_confirmed_defaults(self.session)
            await self._save_session_preserving_external_browser_async()
        self._queue_paused = queue_paused
        if queued_prompts:
            for item in queued_prompts:
                item.session_id = self.session_id
                if "priority" not in item.model_fields_set:
                    item.priority, item.turn_reason = _prompt_authority(
                        item.source, "append"
                    )
            self._queue = sorted(queued_prompts, key=lambda item: item.priority)
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
        await self._checkpoint_runtime_async(lifecycle="ready")
        self._flush_transcript()
        await self._drain_transcripts()
        await self._drain_transcripts()
        if not _defer_drain:
            self._start_drain()
        return self.session

    async def set_browser_attached(
        self,
        attached: bool,
        *,
        url: str = "about:blank",
        width: int | None = None,
        height: int | None = None,
        device_scale_factor: float = 1,
    ) -> dict:
        if self.prompting:
            raise RuntimeError(
                "Wait for the current turn to finish before changing the browser attachment"
            )
        external_id = self.session.external_session_id
        if self.connection:
            await self.connection.disconnect()
            self.connection = None
        config = dict(self.session.config_json or {})
        if attached:
            attachment = await self.manager.browser.attach(
                self.session_id,
                url=url,
                width=width,
                height=height,
                device_scale_factor=device_scale_factor,
            )
            self.agent_env.update(attachment.environment())
            state = await attachment.state()
            config["browser"] = {
                "attached": True,
                "attachment_id": attachment.id,
                "url": state.get("url") or url,
                "width": attachment.width,
                "height": attachment.height,
                "device_scale_factor": attachment.device_scale_factor,
            }
        else:
            await self.manager.browser.detach(self.session_id)
            for key in (
                "PA_BROWSER_CDP_URL",
                "PA_BROWSER_TARGET_ID",
                "PA_BROWSER_ATTACHMENT_ID",
            ):
                self.agent_env.pop(key, None)
            config["browser"] = {"attached": False}
            state = {"attached": False}
        self.session.config_json = config
        await self._offload(
            "sqlite.agent_session_save", self.store.save_session, self.session
        )
        await self.start(resume_external_id=external_id)
        self._append_transcript("browser_attachment_changed", state)
        self._flush_transcript()
        await self._drain_transcripts()
        return state

    async def browser_state(self) -> dict:
        attachment = self.manager.browser.get(self.session_id)
        if not attachment:
            return {"attached": False}
        return await attachment.state()

    async def resize_browser(
        self,
        width: int,
        height: int,
        *,
        device_scale_factor: float = 1,
    ) -> dict:
        attachment = self.manager.browser.get(self.session_id)
        if not attachment:
            raise RuntimeError("No browser is attached")
        await attachment.resize(
            width,
            height,
            device_scale_factor=device_scale_factor,
        )
        state = await attachment.state()
        config = dict(self.session.config_json or {})
        browser_config = dict(config.get("browser") or {})
        browser_config.update(
            attached=True,
            attachment_id=attachment.id,
            url=state.get("url") or browser_config.get("url") or "about:blank",
            width=attachment.width,
            height=attachment.height,
            device_scale_factor=attachment.device_scale_factor,
        )
        config["browser"] = browser_config
        self.session.config_json = config
        await self._offload(
            "sqlite.agent_session_save", self.store.save_session, self.session
        )
        self._append_transcript("browser_attachment_changed", state)
        self._flush_transcript()
        await self._drain_transcripts()
        return state

    def _restart_continuation_receipt(self, item: QueuedPrompt) -> RestartHandoff | None:
        """Validate cached evidence; delivery callers must refresh it off-loop first."""
        if not item.source.startswith("restart-handoff:"):
            return None
        receipt = self._restart_receipts.get(item.id)
        if (
            receipt is None
            or item.source != f"restart-handoff:{receipt.id}"
            or receipt.session_id != self.session_id
            or receipt.continuation_prompt_id != item.id
            or receipt.continuation_prompt != item.message
            or item.session_id != self.session_id
            or item.principal_id != self.session.principal_id
            or item.cwd != self.session.cwd
            or item.agent_env != self._merged_agent_env(None)
            or item.publication_fence
            or receipt.status != "continuation_queued"
            or receipt.delivered_at is not None
            or receipt.card_id != item.card_id
            or receipt.project_id != item.project_id
            or item.images
            or (receipt.instance_id and receipt.instance_id != self.settings.instance_id)
            or receipt.execution_binding != self.session.execution_binding
        ):
            return None
        return receipt

    async def _refresh_restart_receipt(self, item: QueuedPrompt) -> None:
        # Cached evidence is only a presentation hint. Invalidate before yielding
        # and reload off-loop both before dequeue and at the execution boundary.
        self._restart_receipts.pop(item.id, None)
        if not item.source.startswith("restart-handoff:"):
            return
        receipt = await self._offload(
            "sqlite.restart_continuation_authorization",
            self.store.get_restart_handoff,
            item.source.split(":", 1)[1],
        )
        if receipt is not None:
            self._restart_receipts[item.id] = receipt

    def _needs_restart_validation(self, item: QueuedPrompt) -> bool:
        # This schedules validation only; it is never permission to deliver.
        return self.session.purpose == "chat" and item.source.startswith("restart-handoff:")

    def _prompt_eligible(self, item: QueuedPrompt) -> bool:
        return (
            self.session.control_mode != "human"
            or not _is_automatic_source(item.source)
            or (
                self.session.purpose == "chat"
                and self._restart_continuation_receipt(item) is not None
            )
        )

    def _start_drain(self) -> None:
        if self._drain_task and not self._drain_task.done():
            return
        if self._queue_paused or not self._queue:
            return
        if not any(self._prompt_eligible(item) or self._needs_restart_validation(item)
                   for item in self._queue):
            return
        self._drain_task = asyncio.create_task(self._drain_queue())

    async def _drain_queue(self) -> None:
        self._restart_receipts.clear()
        while (
            self._queue
            and not self._queue_paused
            and not self._closed
            and self.connected
        ):
            if self.manager.quiescing:
                break
            # Inspect queue order before doing I/O. A receipt behind an eligible
            # user prompt must never delay that prompt. Restart the scan after
            # each await because priority, membership and scope may have changed.
            validated: set[str] = set()
            unavailable: set[str] = set()
            eligible_index = None
            while not (self._queue_paused or self._closed or not self.connected
                       or self.manager.quiescing):
                candidate = next((candidate for candidate in self._queue
                                  if candidate.id not in unavailable
                                  and (self._prompt_eligible(candidate)
                                       or (self._needs_restart_validation(candidate)
                                           and candidate.id not in validated))), None)
                if candidate is None:
                    break
                if (candidate.source.startswith("restart-handoff:")
                        and candidate.id not in validated):
                    validated.add(candidate.id)
                    try:
                        await self._refresh_restart_receipt(candidate)
                    except Exception:
                        self._restart_receipts.pop(candidate.id, None)
                        unavailable.add(candidate.id)
                        logger.exception("Restart receipt authorization unavailable for %s", candidate.id)
                    continue
                eligible_index = self._queue.index(candidate)
                break
            if eligible_index is None:
                break
            item = self._queue[eligible_index]
            if item.admission_pending:
                try:
                    async with self._prompt_admission_lock:
                        await self._finish_prompt_admission(item)
                except Exception:
                    logger.exception("Dispatch admission remains pending for %s", item.id)
                    break
                # Admission may have yielded while the queue was edited.
                if item not in self._queue:
                    continue
                if self._queue_paused or not self._prompt_eligible(item):
                    break
            # Provider admission performs async work before _in_flight is set.
            # Keep ownership visible to receipt replay throughout that interval.
            self._draining_prompt = item
            self._queue.remove(item)
            self._append_transcript(
                "queue_dequeued",
                {
                    "id": item.id,
                    "message": item.message,
                    "source": item.source,
                    "priority": item.priority,
                    "turn_reason": item.turn_reason,
                    "supersedes": item.supersedes,
                },
            )
            try:
                await self._run_prompt(item)
                await self._refresh_restart_receipt(item)
                receipt = self._restart_continuation_receipt(item)
                if receipt is not None:
                    await self._offload(
                        "sqlite.restart_handoff_delivered",
                        self.store.update_restart_handoff,
                        receipt.id,
                        status="continuation_delivered",
                        delivered=True,
                    )
                if item.publication_fence:
                    self._queue_paused = True
                    self._append_transcript(
                        "publication_fence_established",
                        {
                            "prompt_id": item.id,
                            "reason": item.turn_reason,
                            "queued_prompts_blocked": len(self._queue),
                        },
                    )
                    await self._checkpoint_runtime_async(lifecycle="paused_for_review")
            except PromptAdmissionBlocked as exc:
                logger.info(
                    "Prompt %s is blocked before provider delivery for session %s: %s",
                    item.id,
                    self.session_id,
                    exc,
                )
                await self._preserve_blocked_prompt(item, exc)
                break
            except Exception as exc:
                logger.exception("Queued prompt failed for session %s", self.session_id)
                self._append_transcript(
                    "error",
                    {"message": str(exc), "queued_prompt_id": item.id},
                )
                if not any(queued.id == item.id for queued in self._queue):
                    self._queue.insert(0, item)
                await self._checkpoint_runtime_async(
                    lifecycle="recoverable_interrupted"
                )
                break
            finally:
                self._restart_receipts.pop(item.id, None)
                self._draining_prompt = None
        self._flush_transcript()

    def _require_execution_context_active(self):
        if getattr(self, "_execution_boundary_reserved", False) or (
            self.session.config_json or {}
        ).get("execution_context_boundary"):
            from pa.execution.selection import SelectionError

            raise SelectionError(
                "context_boundary_fenced",
                "This source is reserved for a linked attempt; inspect its context-boundary target instead of submitting another prompt",
            )

    async def _preserve_blocked_prompt(
        self,
        item: QueuedPrompt,
        exc: PromptAdmissionBlocked,
        *,
        record_acceptance: bool = False,
    ) -> None:
        """Keep a pre-provider admission durable without inventing a completion."""
        if record_acceptance:
            self._append_transcript(
                "queue_enqueued",
                {
                    "id": item.id,
                    "message": item.message,
                    "images": [image.public_dict() for image in item.images],
                    "action": "run",
                    "position": 0,
                    "source": item.source,
                    "priority": item.priority,
                    "turn_reason": item.turn_reason,
                },
            )
        self._append_transcript(
            "prompt_blocked",
            {
                "queued_prompt_id": item.id,
                "reason": str(exc),
                "before_provider_delivery": True,
            },
        )
        if not any(queued.id == item.id for queued in self._queue):
            self._queue.insert(0, item)
            self._queue.sort(key=lambda queued: queued.priority)
        await self._checkpoint_runtime_async(lifecycle="admission_blocked")
        self._flush_transcript()
        block = (self.session.config_json or {}).get("execution_selection_block")
        pending = (self.session.config_json or {}).get(
            "execution_pending_settings"
        ) or {}
        if (
            block
            and block.get("prompt_id") == item.id
            and not (
                pending.get("state") == "failed" and pending.get("notification_id")
            )
        ):
            from pa.execution.selection_interactions import settings_blocked

            try:
                await settings_blocked(self, block)
            except Exception as delivery_error:
                logger.warning(
                    "Selection block notification unavailable (%s); durable session block retained",
                    type(delivery_error).__name__,
                )

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
        prompt_audit: list[dict[str, Any]] | None = None,
        prompt_id: str | None = None,
        acceptance_result: str | None = None,
        _defer_drain: bool = False,
        _durable_admission: bool = False,
    ) -> QueuedPrompt:
        self._require_execution_context_active()
        cwd = self._validated_cwd(cwd)
        requested_images = [image.public_dict() for image in (images or [])]
        if prompt_id:
            accepted = ([self._in_flight] if self._in_flight else []) + self._queue
            for queued in accepted:
                if queued.id != prompt_id:
                    continue
                queued_images = [image.public_dict() for image in queued.images]
                if queued.message != message or queued_images != requested_images:
                    raise RuntimeError(
                        f"Prompt id {prompt_id} was already accepted with different content"
                    )
                if acceptance_result is not None:
                    accepted_result = queued.acceptance_result or acceptance_result
                    position = (
                        self._queue.index(queued) if queued in self._queue else 0
                    )
                    self._append_transcript(
                        "queue_enqueued",
                        {
                            "id": queued.id,
                            "message": queued.message,
                            "images": queued_images,
                            "action": "append",
                            "position": position,
                            "source": source,
                            "acceptance_result": accepted_result,
                        },
                    )
                    self._flush_transcript()
                return queued
        priority, turn_reason = _prompt_authority(source, action)
        supersedes = [
            queued.id for queued in self._queue if queued.priority > priority
        ]
        item = QueuedPrompt(
            id=prompt_id or str(uuid4()),
            message=message,
            images=list(images or []),
            session_id=self.session_id,
            card_id=card_id or self.session.card_id,
            project_id=project_id or self.session.project_id,
            principal_id=principal_id or self.session.principal_id,
            cwd=cwd,
            agent_env=self._merged_agent_env(agent_env),
            source=source,
            priority=priority,
            turn_reason=turn_reason,
            supersedes=supersedes,
            publication_fence=action == "interrupt",
            prompt_audit=list(prompt_audit or []),
            acceptance_result=acceptance_result,
            admission_pending=_durable_admission,
            admission_action=action if _durable_admission else None,
        )
        if action in {"prepend", "interrupt"}:
            self._queue.insert(0, item)
        else:
            self._queue.append(item)
        self._queue.sort(key=lambda queued: queued.priority)
        if _durable_admission:
            # Persist work before publishing acceptance. The per-item fence is
            # also restored after a crash, so no provider can overtake this save.
            try:
                self._checkpoint_runtime(lifecycle="admission_pending")
            except Exception:
                self._queue = [queued for queued in self._queue if queued.id != item.id]
                raise
            return item
        self._append_transcript(
            "queue_enqueued",
            {
                "id": item.id,
                "message": message,
                "images": [image.public_dict() for image in item.images],
                "action": action,
                "position": self._queue.index(item),
                "source": source,
                "priority": item.priority,
                "turn_reason": item.turn_reason,
                "supersedes": item.supersedes,
                "acceptance_result": item.acceptance_result,
            },
        )
        if not _is_automatic_source(source):
            self._record_human_activity()
        try:
            self._checkpoint_runtime(lifecycle="queued")
        except Exception:
            self._queue = [queued for queued in self._queue if queued.id != item.id]
            raise
        self._flush_transcript()
        if not _is_automatic_source(source):
            self._checkpoint_runtime(lifecycle="queued")
        if (
            not self._queue_paused
            and not _defer_drain
            and (self._prompt_eligible(item) or self._needs_restart_validation(item))
        ):
            self._start_drain()
        return item

    async def _finish_prompt_admission(self, item: QueuedPrompt) -> TranscriptEvent:
        """Finish an actual persisted queue admission before provider delivery."""
        self._require_execution_context_active()
        accepted = await self._offload(
            "sqlite.prompt_acceptance_read", self.store.get_prompt_acceptance,
            self.session_id, item.id, wait_for_completion=True,
        )
        if accepted is None:
            self._append_transcript("queue_enqueued", {
                "id": item.id, "message": item.message,
                "images": [image.public_dict() for image in item.images],
                "action": item.admission_action, "source": item.source,
                "position": self._queue.index(item) if item in self._queue else 0,
            })
            self._flush_transcript()
            await self._drain_transcripts(timeout=None)
            accepted = await self._offload(
                "sqlite.prompt_acceptance_read", self.store.get_prompt_acceptance,
                self.session_id, item.id, wait_for_completion=True,
            )
        if accepted is None:
            raise RuntimeError("Dispatch prompt acceptance is not durable yet")
        await self._checkpoint_runtime_async(lifecycle="queued")
        # Keep the fence through the awaited checkpoint: failure or cancellation
        # must leave the item recoverable, and another drain must not deliver it
        # while this handoff is in flight. Recovery of the persisted pending item
        # rechecks this same receipt; the next runtime checkpoint saves release.
        item.admission_pending = False
        return accepted

    async def admit_dispatch_prompt(
        self, message: str, *, prompt_id: str, action: PromptAction = "append",
        images=None, item_id=None, principal_id=None, project_id=None, source: str,
    ) -> None:
        """Recoverably enqueue an identity-bound dispatch under admission lock."""
        item = next((queued for queued in self._queue if queued.id == prompt_id), None)
        if item is None:
            item = await self._offload(
                "sqlite.dispatch_queue_admit", self.enqueue,
                message, prompt_id=prompt_id, action=action, images=images,
                card_id=item_id, principal_id=principal_id, project_id=project_id,
                source=source, _defer_drain=True, _durable_admission=True,
                wait_for_completion=True,
            )
        if item.admission_pending:
            await self._finish_prompt_admission(item)
        if action == "interrupt" and self.prompting:
            await self.cancel(pause_queue=False)
        self._start_drain()

    def _record_human_activity(self) -> None:
        self.session.human_activity_at = datetime.now(UTC)
        config = dict(self.session.config_json or {})
        durable = dict(config.get(_DURABLE_RUNTIME_KEY) or {})
        durable.pop("pending_interaction", None)
        config[_DURABLE_RUNTIME_KEY] = durable
        self.session.config_json = config

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
        prompt_id: str | None = None,
        source: str = "api",
        _from_queue: bool = False,
        wait: bool = True,
    ) -> str:
        self._require_execution_context_active()
        cwd = self._validated_cwd(cwd)
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
                source=source,
                prompt_id=prompt_id,
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
                    source=source,
                    prompt_id=prompt_id,
                )
                return "queued"

        if self.session.control_mode == "human" and _is_automatic_source(source):
            self.enqueue(
                message,
                images=images,
                action=action,
                card_id=item_id,
                project_id=project_id,
                principal_id=principal_id,
                cwd=cwd,
                agent_env=agent_env,
                source=source,
                prompt_id=prompt_id,
            )
            return "queued"
        priority, turn_reason = _prompt_authority(source, action)
        item = QueuedPrompt(
            id=prompt_id or str(uuid4()),
            message=message,
            images=list(images or []),
            session_id=self.session_id,
            card_id=item_id or self.session.card_id,
            project_id=project_id or self.session.project_id,
            principal_id=principal_id or self.session.principal_id,
            cwd=cwd,
            agent_env=self._merged_agent_env(agent_env),
            source=source,
            priority=priority,
            turn_reason=turn_reason,
            publication_fence=action == "interrupt",
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
                    source=source,
                    prompt_id=prompt_id,
                )
                return "queued"
            self._queue.insert(0, item)
            self._append_transcript(
                "queue_enqueued",
                {
                    "id": item.id,
                    "message": message,
                    "images": [image.public_dict() for image in item.images],
                    "action": "run",
                    "position": 0,
                },
            )
            self._flush_transcript()
            self._start_drain()
            if not _is_automatic_source(source):
                self._record_human_activity()
                await self._checkpoint_runtime_async(lifecycle="queued")
            return "started"
        if not _is_automatic_source(source):
            self._record_human_activity()
            await self._checkpoint_runtime_async(lifecycle="prompting")
        try:
            result = await self._run_prompt(item)
        except PromptAdmissionBlocked as exc:
            logger.info(
                "Prompt %s is blocked before provider delivery for session %s: %s",
                item.id,
                self.session_id,
                exc,
            )
            await self._preserve_blocked_prompt(
                item, exc, record_acceptance=not _from_queue
            )
            return "blocked"
        if item.publication_fence:
            self._queue_paused = True
            self._append_transcript(
                "publication_fence_established",
                {
                    "prompt_id": item.id,
                    "reason": item.turn_reason,
                    "queued_prompts_blocked": len(self._queue),
                },
            )
            await self._checkpoint_runtime_async(lifecycle="paused_for_review")
        return result

    def _validated_cwd(self, requested: str | None) -> str | None:
        """Keep every turn inside the workspace fenced to this session."""
        expected = self.session.cwd
        context = (self.session.config_json or {}).get("execution_context")
        if not context or not expected:
            return requested or expected
        normalize = lambda value: os.path.normcase(
            os.path.abspath(os.path.expanduser(value))
        )
        if requested and normalize(requested) != normalize(expected):
            raise RuntimeError(
                "Prompt cwd cannot override the session's leased workspace"
            )
        return expected

    def _merged_agent_env(self, extra: dict[str, str] | None) -> dict[str, str]:
        merged = dict(self.agent_env)
        merged.update(extra or {})
        # Execution boundaries are manager-owned even when user credentials or
        # browser variables are supplied for an individual turn.
        for key in (
            "PA_EXECUTION_CONTEXT",
            "PA_WORKSPACE_ROOT",
            "PA_WRITABLE_ROOTS",
            "PA_DEPENDENCY_CACHE",
        ):
            if key in self.agent_env:
                merged[key] = self.agent_env[key]
        return merged

    async def _run_prompt(self, item: QueuedPrompt) -> str:
        if item.admission_pending:
            async with self._prompt_admission_lock:
                await self._finish_prompt_admission(item)
        if not self.connection:
            raise RuntimeError("Session not connected")
        item.cwd = self._validated_cwd(item.cwd)
        item.agent_env = self._merged_agent_env(item.agent_env)
        try:
            await self._offload(
                "workspace.lease_renew",
                self.manager.workspace_manager.renew_session,
                self.session_id,
            )
        except Exception:
            logger.exception("Could not renew workspace lease for %s", self.session_id)
        async with self._prompt_lock:
            self._require_execution_context_active()
            collaboration = getattr(self.manager, "collaboration_service", None)
            from pa.execution.selection_settings import apply_pending

            from pa.execution.selection_audit import begin_prompt
            from pa.execution.selection import SelectionError

            try:
                await apply_pending(self)
                selection_attempt = await self._offload(
                    "selection.prompt_identity", begin_prompt, self, item
                )
            except SelectionError as exc:
                block = {
                    "id": "prompt:" + item.id,
                    "code": exc.code,
                    "message": str(exc),
                    "prompt_id": item.id,
                    "observed_at": datetime.now(UTC).isoformat(),
                }
                self.session.config_json = {
                    **(self.session.config_json or {}),
                    "execution_selection_block": block,
                }
                raise PromptAdmissionBlocked(str(exc)) from exc
            if collaboration is not None:
                # This is the exact between-turn boundary. Revalidate and apply
                # a durable pending transition before the next prompt is built.
                try:
                    await collaboration.prepare_turn(self)
                except Exception as exc:
                    raise PromptAdmissionBlocked(str(exc)) from exc
            await self._refresh_restart_receipt(item)
            if _is_automatic_source(item.source) and (
                self._queue_paused or not self._prompt_eligible(item)
            ):
                raise PromptAdmissionBlocked(
                    "Automatic prompt held: the queue was paused or its authorization changed during admission"
                )
            self._in_flight = item
            self._turn_started_at = datetime.now(UTC)
            self._turn_agent_events = []
            self._turn_streamed = False
            await self._supersede_obsolete_final_input_fallbacks(item)
            await self._checkpoint_runtime_async(lifecycle="prompting")
            try:
                composition = await self._offload(
                    "agent.prompt_compose",
                    compose_session_prompt,
                    self.store,
                    self.settings,
                    self.session,
                    item.message,
                    card_id=item.card_id,
                    project_id=item.project_id,
                    timeout=30.0,
                )
                prompt_audit = list(item.prompt_audit) + composition.audit_records()
                from pa.prompts import PROMPTS

                remote_default = PROMPTS.render(
                    "dispatch.remote.default", provider=self.session.agent_name
                )
                if item.message == remote_default.text:
                    prompt_audit.insert(0, remote_default.audit_record())
                if item.source == "recovery":
                    definition = PROMPTS.get("session.recovery.resume")
                    prompt_audit.insert(
                        0,
                        {
                            "key": definition.key,
                            "version": definition.version,
                            "source": definition.source,
                            "scope": definition.scope,
                            "provider": self.session.agent_name,
                            "resolved_context": {},
                        },
                    )
                if item.source == "pr-supervisor":
                    for key in (
                        "pr_supervisor.action.required",
                        "pr_supervisor.action.green",
                        "pr_supervisor.action.merged",
                    ):
                        definition = PROMPTS.get(key)
                        if definition.template in item.message:
                            prompt_audit.insert(
                                0,
                                {
                                    "key": definition.key,
                                    "version": definition.version,
                                    "source": definition.source,
                                    "scope": definition.scope,
                                    "provider": self.session.agent_name,
                                    "resolved_context": {},
                                },
                            )
                if item.source.startswith("card-reconciliation:"):
                    definition = PROMPTS.get("card.reconciliation.disposition")
                    prompt_audit.insert(
                        0,
                        {
                            "key": definition.key,
                            "version": definition.version,
                            "source": definition.source,
                            "scope": definition.scope,
                            "provider": self.session.agent_name,
                            "resolved_context": {},
                        },
                    )
                config = dict(self.session.config_json or {})
                audit_history = list(config.get("prompt_audit") or [])
                audit_entry = {"prompt_id": item.id, "prompts": prompt_audit}
                existing_index = next(
                    (
                        index
                        for index, entry in enumerate(audit_history)
                        if entry.get("prompt_id") == item.id
                    ),
                    None,
                )
                first_attempt = existing_index is None
                if first_attempt:
                    audit_history.append(audit_entry)
                else:
                    audit_history[existing_index] = audit_entry
                config["prompt_audit"] = audit_history[-50:]
                self.session.config_json = config
                await self._save_session_preserving_external_browser_async()
                if first_attempt:
                    self._append_transcript(
                        "prompt_rendered", {"id": item.id, "prompts": prompt_audit}
                    )
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
                await self._drain_transcripts()
            except BaseException:
                self._finish_turn_state()
                raise
            stall_task = asyncio.create_task(self._watch_turn_waiting(item))
            import time

            selection_started = time.monotonic()
            protocol_completed, selection_stop_reason = False, None
            try:
                try:
                    stop_reason = await self.connection.prompt(
                        composition.text,
                        images=item.images,
                        item_id=item.card_id,
                        principal_id=item.principal_id,
                        project_id=item.project_id,
                        cwd=item.cwd,
                    )
                    protocol_completed, selection_stop_reason = True, stop_reason
                except Exception as exc:
                    from pa.acp.errors import classify_acp_failure, format_acp_error

                    if self._is_connection_loss(exc):
                        self._finish_turn_state()
                        self._notify_connection_lost(item, exc)
                        await self._drain_transcripts()
                        return "connection_lost"
                    classified = classify_acp_failure(
                        exc,
                        provider_id=self.session.agent_name,
                        stage="prompt",
                    )
                    self._append_transcript(
                        "prompt_failed",
                        {
                            "id": item.id,
                            "error": format_acp_error(exc)[:1000],
                            "failure": classified,
                        },
                    )
                    self._flush_transcript()
                    await self._drain_transcripts()
                    raise RuntimeError(classified.get("message") or str(exc)) from exc
                usage = self.connection.last_usage if self.connection else None
                if usage:
                    metrics = dict(self.session.metrics_json or {})
                    metrics["last_usage"] = usage
                    self.session.metrics_json = metrics
                    await self._save_session_preserving_external_browser_async()
                # Clear snapshot-visible turn state before publishing the
                # terminal event. A page refresh after turn_completed must see
                # prompting=false and no per-turn start time.
                self._finish_turn_state()
                self._append_transcript(
                    "turn_completed",
                    {
                        "stop_reason": stop_reason,
                        "usage": usage,
                        "queued_prompt_id": item.id,
                    },
                )
                self._flush_transcript()
                await self._drain_transcripts()
                final_text = assemble_final_assistant_message(self._turn_agent_events)
                needs_input = await self._surface_final_input_fallback(final_text, item)
                metrics = dict(self.session.metrics_json or {})
                metrics["turns"] = int(metrics.get("turns") or 0) + 1
                self.session.metrics_json = metrics
                if self.session.purpose == "one_shot_job":
                    self.session.workflow_state = "active" if needs_input else "succeeded"
                    self.session.workflow_outcome = {
                        **dict(self.session.workflow_outcome or {}),
                        "summary": (
                            "The job is waiting for requested input."
                            if needs_input
                            else sanitize_text(final_text, limit=1000)
                            or "The one-shot job completed."
                        ),
                        "next_expected_event": (
                            "A response to the requested input."
                            if needs_input
                            else None
                        ),
                        "completed_at": None if needs_input else datetime.now(UTC).isoformat(),
                    }
                await self._save_session_preserving_external_browser_async()
                if (
                    self.connection
                    and self.connection.last_memory_candidate
                    and self.manager.settings.memory_auto_capture_enabled
                ):
                    try:
                        await self._offload(
                            "agent.knowledge_candidate",
                            capture_from_updates,
                            self.store,
                            session_id=self.session_id,
                            item_id=item.card_id,
                            updates=[],
                            enabled=True,
                            eligible=True,
                            timeout=60.0,
                        )
                    except Exception:
                        logger.exception("Failed to queue optional Memory candidate")
                    finally:
                        self.connection.last_memory_candidate = False
                if self.manager.completion_handler and item.card_id:
                    try:
                        from pa.execution.disposition import (
                            claims_card_disposition_contract,
                            extract_card_disposition,
                        )

                        disposition, disposition_error = extract_card_disposition(
                            final_text
                        )
                        payload = {
                            "stop_reason": stop_reason,
                            "usage": usage,
                            "queued_prompt_id": item.id,
                            "prompt_source": item.source,
                            "provider_status": (
                                "connected" if self.connected else "disconnected"
                            ),
                            "session_status": self.session.status,
                            "final_outcome_text": sanitize_text(
                                final_text, limit=8_000
                            ),
                        }
                        if disposition:
                            payload["card_disposition"] = disposition
                        elif disposition_error:
                            payload["card_disposition_error"] = disposition_error[:1000]
                        if disposition or claims_card_disposition_contract(final_text):
                            self._append_transcript(
                                "card_disposition",
                                {
                                    "content_type": (
                                        "application/vnd.pa.card-disposition+json;"
                                        "version=1"
                                    ),
                                    "contract": disposition,
                                    "raw": final_text,
                                    "persistence": "pending",
                                    "authority_acknowledged": False,
                                    "status": "valid" if disposition else "invalid",
                                    "reason": disposition_error,
                                },
                            )
                            self._flush_transcript()
                            await self._drain_transcripts()
                        await self._report_progress(
                            {
                                "type": "turn_completed",
                                "summary": (
                                    disposition.get("outcome")
                                    if isinstance(disposition, dict)
                                    else "Agent turn ended."
                                ),
                                "result": payload,
                            }
                        )
                        if inspect.iscoroutinefunction(self.manager.completion_handler):
                            result = self.manager.completion_handler(
                                self.session_id, payload
                            )
                        else:
                            result = await self._offload(
                                "agent.completion_callback",
                                self.manager.completion_handler,
                                self.session_id,
                                payload,
                                timeout=30.0,
                            )
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception:
                        # Completion delivery is an outbox operation. A transport
                        # failure must never turn successful agent work into a
                        # failed turn or lose the durable pending mutation.
                        logger.exception("Failed to queue card completion")
                return stop_reason
            finally:
                stall_task.cancel()
                try:
                    await stall_task
                except asyncio.CancelledError:
                    pass
                self._finish_turn_state()
                if selection_attempt:
                    try:
                        from pa.execution.selection_audit import finish_prompt

                        await self._offload(
                            "selection.prompt_outcome",
                            finish_prompt,
                            self,
                            selection_attempt,
                            completed=protocol_completed,
                            latency_ms=(time.monotonic() - selection_started) * 1000,
                            stop_reason=selection_stop_reason,
                        )
                    except Exception:
                        # Never replay a successfully executed prompt solely
                        # because optional feedback persistence failed.
                        logger.exception(
                            "Could not persist execution-selection outcome evidence"
                        )

    async def _surface_final_input_fallback(
        self, final_text: str, item: QueuedPrompt
    ) -> bool:
        prompt = likely_user_input_request(final_text)
        service = getattr(self.manager, "notification_service", None)
        if not prompt or not service:
            return False
        existing = await self._offload(
            "sqlite.notification_list",
            service.list_authorized,
            principal_id=self.session.principal_id or "user:local",
            realms={self.session.realm_id},
            realm_id=self.session.realm_id,
            outstanding=True,
            limit=200,
            offset=0,
        )
        if any(
            notice.session_id == self.session_id
            and notice.interaction
            and notice.interaction.kind.value != "final_output_fallback"
            for notice in existing
        ):
            return True
        import hashlib

        from pa.domain.notifications import (
            InteractionKind,
            InteractionRequest,
            NotificationAction,
            NotificationCreate,
            NotificationPriority,
            NotificationType,
            NotificationVisibility,
            concise_notification_summary,
            interaction_notification_title,
        )

        digest = hashlib.sha256(prompt.encode()).hexdigest()[:24]
        title = interaction_notification_title(
            InteractionKind.FINAL_OUTPUT_FALLBACK, prompt
        )
        notice = await self._offload(
            "sqlite.notification_create",
            service.create,
            NotificationCreate(
                realm_id=self.session.realm_id,
                visibility=(
                    NotificationVisibility.PRINCIPAL
                    if self.session.principal_id
                    else NotificationVisibility.REALM
                ),
                principal_id=self.session.principal_id,
                type=NotificationType.INTERACTION,
                priority=NotificationPriority.HIGH,
                title=title,
                body=final_text,
                summary=concise_notification_summary(prompt),
                card_id=item.card_id,
                session_id=self.session_id,
                dispatch_id=self.session.dispatch_id,
                project_id=item.project_id,
                destination_url=f"/agent?session={self.session_id}",
                deduplication_key=f"final-input:{self.session_id}:{digest}",
                actions=[
                    NotificationAction(
                        id="respond",
                        kind="respond",
                        label="Reply",
                        method="POST",
                    )
                ],
                interaction=InteractionRequest(
                    kind=InteractionKind.FINAL_OUTPUT_FALLBACK,
                    prompt=prompt,
                    allow_freeform=True,
                    allow_cancel=True,
                    protocol_method="pa/final-output-fallback",
                    continuation_mode="prompt",
                ),
            ),
            principal_id=self.session.principal_id or "user:local",
        )
        self._append_transcript(
            "interaction_request",
            {
                "request_id": notice.interaction.request_id,
                "notification_id": notice.id,
                "kind": "final_output_fallback",
            },
        )
        config = dict(self.session.config_json or {})
        durable = dict(config.get(_DURABLE_RUNTIME_KEY) or {})
        durable["pending_interaction"] = {
            "kind": "input",
            "count": 1,
            "request_ids": [notice.interaction.request_id],
            "notification_id": notice.id,
            "action": "Respond to the agent's requested input.",
        }
        config[_DURABLE_RUNTIME_KEY] = durable
        self.session.config_json = config
        await self._save_session_preserving_external_browser_async()
        self._flush_transcript()
        await self._drain_transcripts()
        return True

    async def _supersede_obsolete_final_input_fallbacks(
        self, item: QueuedPrompt
    ) -> None:
        """Close fallback requests only after a later operator prompt proves progress."""
        service = getattr(self.manager, "notification_service", None)
        if not service or item.turn_reason != "operator_input":
            return
        notices = await self._offload(
            "sqlite.notification_list",
            service.list_authorized,
            principal_id=self.session.principal_id or "user:local",
            realms={self.session.realm_id},
            realm_id=self.session.realm_id,
            outstanding=True,
            limit=200,
            offset=0,
        )
        superseded = []
        for notice in notices:
            interaction = notice.interaction
            if (
                notice.session_id != self.session_id
                or not interaction
                or interaction.kind.value != "final_output_fallback"
            ):
                continue
            updated = await self._offload(
                "sqlite.notification_supersede",
                service.supersede,
                notice,
                principal_id=self.session.principal_id or "user:local",
                idempotency_key=f"superseded-by-prompt:{item.id}:{notice.id}",
            )
            superseded.append(updated.id)
        if superseded:
            self._append_transcript(
                "interaction_superseded",
                {
                    "notification_ids": superseded,
                    "evidence": "later_operator_prompt_accepted",
                    "queued_prompt_id": item.id,
                },
            )

    def _turn_waiting_payload(
        self, item: QueuedPrompt, elapsed_s: float
    ) -> dict[str, Any]:
        pending = bool(self._pending_permissions)
        if pending:
            message = (
                "The agent is waiting for a permission response. "
                "Approve or deny the request to continue."
            )
        else:
            message = (
                "The agent has not streamed any output yet. Thinking models "
                "can stay quiet for a while; use Stop if this looks stuck."
            )
        return {
            "id": item.id,
            "elapsed_s": int(elapsed_s),
            "pending_permissions": pending,
            "message": message,
        }

    async def _watch_turn_waiting(self, item: QueuedPrompt) -> None:
        elapsed = 0.0
        while True:
            await asyncio.sleep(TURN_WAITING_SECONDS)
            elapsed += TURN_WAITING_SECONDS
            if self._in_flight is not item or self._turn_streamed:
                return
            self._append_transcript(
                "turn_waiting", self._turn_waiting_payload(item, elapsed)
            )
            self._flush_transcript()

    def _finish_turn_state(self) -> None:
        self._in_flight = None
        self._turn_started_at = None
        self._turn_streamed = False
        self._checkpoint_runtime(lifecycle="ready")

    def _is_connection_loss(self, exc: BaseException) -> bool:
        if isinstance(exc, ConnectionError):
            return True
        msg = str(exc).lower()
        return (
            "connection closed" in msg
            or "not connected to agent" in msg
            or "separator is not found" in msg
            or "chunk exceed the limit" in msg
        )

    def _notify_connection_lost(self, item: QueuedPrompt, exc: BaseException) -> None:
        logger.warning(
            "ACP connection lost for session %s during prompt %s: %s",
            self.session_id,
            item.id,
            exc,
        )
        self._append_transcript(
            "connection_lost",
            {
                "message": "Connection to the agent was lost while handling this prompt. It may or may not have reached the agent — if you don't see a response, you may want to retry.",
                "queued_prompt_id": item.id,
                "detail": str(exc),
            },
        )
        self.session.status = "recoverable_interrupted"
        self._checkpoint_runtime(lifecycle="recoverable_interrupted")
        self._flush_transcript()
        self.manager.request_recovery(self.session_id)

    async def cancel(self, *, pause_queue: bool = True) -> None:
        self._restart_receipts.clear()
        if pause_queue:
            self._queue_paused = True
        if self.connection:
            try:
                await self.connection.cancel()
            except Exception:
                logger.exception("Cancel failed for session %s", self.session_id)
        self._append_transcript("cancelled", {"pause_queue": pause_queue})
        await self._checkpoint_runtime_async(lifecycle="ready")
        self._flush_transcript()
        await self._drain_transcripts()

    def pause_queue(self) -> None:
        self._restart_receipts.clear()
        self._queue_paused = True
        self._append_transcript("queue_paused", {})
        self._checkpoint_runtime(lifecycle="paused")
        self._flush_transcript()

    def resume_queue(self) -> None:
        self._queue_paused = False
        self._append_transcript("queue_resumed", {})
        self._checkpoint_runtime(lifecycle="queued" if self._queue else "ready")
        self._flush_transcript()
        self._start_drain()

    async def set_control_mode(self, mode: Literal["automation", "human"]) -> None:
        """Persist takeover before changing which durable prompts may drain."""
        if mode == self.session.control_mode:
            return
        self._restart_receipts.clear()
        self.session.control_mode = mode
        self.session.updated_at = datetime.now(UTC)
        self._append_transcript(
            "session_control_changed",
            {"mode": mode, "automatic_prompts_held": mode == "human"},
        )
        await self._checkpoint_runtime_async(
            lifecycle="taken_over" if mode == "human" else "queued"
            if self._queue
            else "ready"
        )
        self._flush_transcript()
        await self._drain_transcripts()
        if mode == "automation":
            self._start_drain()

    async def release_provider(self, *, reason: str) -> None:
        """Release an idle process without ending the durable conversation."""
        if self.prompting or self._queue or self._pending_permissions or self._pending_elicitations:
            raise RuntimeError("Session still has active provider obligations")
        self.session.status = "available"
        self._append_transcript("provider_released", {"reason": reason})
        await self._checkpoint_runtime_async(lifecycle="available")
        self._flush_transcript()
        await self._drain_transcripts()
        connection = self.connection
        self.connection = None
        self._closed = True
        if connection:
            await connection.disconnect()

    def remove_queued(self, prompt_id: str) -> bool:
        before = len(self._queue)
        self._queue = [q for q in self._queue if q.id != prompt_id]
        removed = len(self._queue) != before
        if removed:
            self._append_transcript("queue_removed", {"id": prompt_id})
            self._checkpoint_runtime(lifecycle="queued" if self._queue else "ready")
            self._flush_transcript()
        return removed

    def reorder_queue(self, prompt_ids: list[str]) -> list[QueuedPrompt]:
        by_id = {q.id: q for q in self._queue}
        ordered = [by_id[i] for i in prompt_ids if i in by_id]
        remaining = [q for q in self._queue if q.id not in prompt_ids]
        self._queue = ordered + remaining
        self._append_transcript("queue_reordered", {"ids": [q.id for q in self._queue]})
        self._checkpoint_runtime(lifecycle="queued" if self._queue else "ready")
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
                    option_id = options[0].get("optionId") or options[0].get(
                        "option_id"
                    )
            if not option_id:
                return False
            response = permission_selected(option_id)
        else:
            response = permission_cancelled()
        if remember and allow:
            await self.manager.set_auto_approve_async(
                True, scope=scope, principal_id=principal_id
            )
        notification_service = getattr(self.manager, "notification_service", None)
        notification_id = self._permission_notification_ids.get(request_id)
        if notification_service and notification_id:
            from pa.domain.notifications import InteractionResponse

            notice = await self._offload(
                "sqlite.notification_read",
                self.store.get_notification,
                notification_id,
                realm_id=self.session.realm_id,
            )
            if not notice:
                return False
            await notification_service.respond(
                notice,
                InteractionResponse(
                    idempotency_key=f"session-permission:{request_id}:{uuid4()}",
                    choice_id=option_id if allow else None,
                    cancel=not allow,
                ),
                principal_id=principal_id or self.session.principal_id or "user:local",
            )
        else:
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
            await self._drain_transcripts()
        return True

    async def set_model(self, model_id: str) -> None:
        if not self.connection:
            raise RuntimeError("Session not connected")
        if (self.session.config_json or {}).get("execution_selection"):
            from pa.execution.selection import legacy_preferences
            from pa.execution.selection_settings import request_settings

            await request_settings(
                self,
                legacy_preferences(model_id=model_id),
                principal=self.session.principal_id or "user:local",
                key=str(uuid4()),
                expected_version=self.session.updated_at,
                defer=False,
            )
            return
        await self.connection.set_model(model_id)
        self.session = self.connection.session or self.session
        self._append_transcript("model_changed", {"model_id": model_id})
        self._flush_transcript()
        await self._drain_transcripts()

    async def configure(self, requested: SessionConfigurationRequest) -> dict[str, Any]:
        if not self.connection:
            raise RuntimeError("Session not connected")
        if (self.session.config_json or {}).get("execution_selection") and (
            requested.model_id
            or requested.model_provider
            or requested.reasoning
            or requested.config
        ):
            from pa.execution.selection import SelectionError, legacy_preferences
            from pa.execution.selection_settings import request_settings

            if requested.mode_id and requested.mode_id != self.session.mode_id:
                raise SelectionError(
                    "separate_permission_action",
                    "Change permission mode through its separate authority action before requesting model settings.",
                )
            await request_settings(
                self,
                legacy_preferences(
                    model_id=requested.model_id,
                    model_provider=requested.model_provider,
                    effort=requested.reasoning,
                    config=requested.config,
                ),
                principal=self.session.principal_id or "user:local",
                key=str(uuid4()),
                expected_version=self.session.updated_at,
                defer=False,
            )
            return dict((self.session.config_json.get("configuration") or {}).get("effective") or {})
        if self.prompting:
            raise RuntimeError(
                "Wait for the current turn to finish before changing session configuration"
            )
        async with self._prompt_lock:
            effective = await self.connection.configure(requested, merge=True)
            self.session = self.connection.session or self.session
            await self._reconcile_confirmed_collaboration(
                dict(effective.get("config") or {}).get("collaboration_mode")
            )
            await self._save_session_preserving_external_browser_async()
            self._append_transcript(
                "configuration_changed",
                {"requested": requested.as_dict(), "effective": effective},
            )
            self._flush_transcript()
            await self._drain_transcripts()
            return effective

    async def set_mode(self, mode_id: str) -> None:
        if not self.connection:
            raise RuntimeError("Session not connected")
        await self.connection.set_mode(mode_id)
        self.session = self.connection.session or self.session
        self._append_transcript("mode_changed", {"mode_id": mode_id})
        self._flush_transcript()
        await self._drain_transcripts()

    async def set_config(self, config_id: str, value: str | bool) -> None:
        if not self.connection:
            raise RuntimeError("Session not connected")
        if (self.session.config_json or {}).get("execution_selection"):
            from pa.acp.configuration import find_option, option_id

            mode_option = find_option(self.connection.config_options or [], "mode")
            if not mode_option or option_id(mode_option) != config_id:
                from pa.execution.selection import legacy_preferences
                from pa.execution.selection_settings import request_settings

                await request_settings(
                    self,
                    legacy_preferences(config={config_id: value}),
                    principal=self.session.principal_id or "user:local",
                    key=str(uuid4()),
                    expected_version=self.session.updated_at,
                    defer=False,
                )
                return
        await self.connection.set_config(config_id, value)
        self.session = self.connection.session or self.session
        if config_id == "collaboration_mode":
            await self._reconcile_confirmed_collaboration(value)
            await self._save_session_preserving_external_browser_async()
        self._append_transcript(
            "config_changed", {"config_id": config_id, "value": value}
        )
        self._flush_transcript()
        await self._drain_transcripts()

    def snapshot(self, *, include_transcript: bool = True) -> dict[str, Any]:
        """Return runtime state, optionally including the bounded durable transcript.

        Request paths that only need live metadata must leave transcript persistence
        to its background writer and use the paginated history API separately.
        """
        events: list[TranscriptEvent] = []
        has_older = False
        if include_transcript:
            self._flush_transcript()
            events = self.store.list_transcript_events_before(
                self.session_id,
                limit=TRANSCRIPT_WINDOW_LIMIT + 1,
            )
            has_older = len(events) > TRANSCRIPT_WINDOW_LIMIT
            events = events[-TRANSCRIPT_WINDOW_LIMIT:]
        conn = self.connection
        configuration = dict(
            ((self.session.config_json or {}).get("configuration") or {})
        )
        from pa.execution.selection import selection_presentation
        snapshot = {
            "session": self.session.model_dump(mode="json"),
            "presentation": build_session_presentation(
                self.session,
                runtime=self,
                quiescing=bool(
                    getattr(getattr(self, "manager", None), "quiescing", False)
                ),
                startup_complete=bool(
                    getattr(getattr(self, "manager", None), "startup_complete", True)
                ),
            ),
            "connected": self.connected,
            "prompting": self.prompting,
            "queue_paused": self._queue_paused,
            "queue": [q.public_dict() for q in self._queue],
            "in_flight": self._in_flight.model_dump(mode="json")
            if self._in_flight
            else None,
            "models": conn.models if conn else None,
            "modes": conn.modes if conn else None,
            "config_options": conn.config_options if conn else None,
            "configuration": configuration,
            "execution_selection": selection_presentation(
                self.session.config_json or {},
                model_id=self.session.model_id,
                mode_id=self.session.mode_id,
            ),
            "pa_mcp": conn.pa_mcp_health if conn else None,
            "metrics": self.session.metrics_json,
            "turn_started_at": self._turn_started_at.isoformat()
            if self._turn_started_at
            else None,
            "pending_permissions": [
                self._permission_requests[rid]
                for rid in self._pending_permissions
                if rid in self._permission_requests
            ],
            "pending_elicitations": [
                getattr(self, "_elicitation_requests", {})[rid]
                for rid in getattr(self, "_pending_elicitations", {})
                if rid in getattr(self, "_elicitation_requests", {})
            ],
            "restart_handoffs": [
                item.model_dump(mode="json")
                for item in (
                    self.store.list_restart_handoffs(session_id=self.session_id)
                    if callable(getattr(self.store, "list_restart_handoffs", None))
                    else []
                )
            ],
        }
        if include_transcript:
            snapshot["transcript"] = [e.model_dump(mode="json") for e in events]
            snapshot["transcript_page"] = {
                "oldest_seq": events[0].seq if events else None,
                "newest_seq": events[-1].seq if events else None,
                "has_older": has_older,
                "next_before_seq": events[0].seq if has_older and events else None,
                "limit": TRANSCRIPT_WINDOW_LIMIT,
            }
        return snapshot

    def to_session_snapshot(self) -> SessionSnapshot:
        return SessionSnapshot(
            session_id=self.session.id,
            external_session_id=self.session.external_session_id,
            agent_name=self.session.agent_name,
            status="idle",
            cwd=self.session.cwd
            or (self.connection.session_cwd if self.connection else None),
            title=self.session.title,
            label=self.session.label,
            model_id=self.session.model_id,
            mode_id=self.session.mode_id,
            configuration=dict(
                ((self.session.config_json or {}).get("configuration") or {})
            ),
            card_id=self.session.card_id or self.session.item_id,
            project_id=self.session.project_id,
            principal_id=self.session.principal_id,
            authority_instance_id=self.session.authority_instance_id,
            origin_instance_id=self.session.origin_instance_id,
            dispatch_id=self.session.dispatch_id,
            realm_id=self.session.realm_id,
            purpose=self.session.purpose,
            initiating_workflow=dict(self.session.initiating_workflow or {}),
            control_mode=self.session.control_mode,
            archived_at=self.session.archived_at,
            workflow_state=self.session.workflow_state,
            workflow_outcome=dict(self.session.workflow_outcome or {}),
            recovery_json=dict(self.session.recovery_json or {}),
            prompting=False,
            queue_paused=self._queue_paused,
            queued_prompts=list(self._queue),
            in_flight=self._in_flight,
        )

    async def close(
        self,
        *,
        reason: str = "user_close",
        reconcile_workspace: bool = True,
    ) -> bool:
        if self._closed:
            return False
        prior_status = self.session.status
        logger.info(
            "Closing live agent session",
            extra={
                "session_id": self.session_id,
                "prior_status": prior_status,
                "close_reason": reason,
                "prompting": self.prompting,
                "queue_length": len(self._queue),
            },
        )
        self._closed = True
        self._queue_paused = True
        if self._drain_task and not self._drain_task.done():
            self._drain_task.cancel()
        notification_service = getattr(self.manager, "notification_service", None)

        async def supersede_notification(notification_id: str | None) -> None:
            if not notification_service or not notification_id:
                return
            try:
                notice = await self._offload(
                    "sqlite.notification_read",
                    self.store.get_notification,
                    notification_id,
                    realm_id=self.session.realm_id,
                )
                if notice:
                    await self._offload(
                        "sqlite.notification_supersede",
                        notification_service.supersede,
                        notice,
                        principal_id="system:session-close",
                        idempotency_key=(
                            f"session-close:{self.session_id}:{notification_id}"
                        ),
                    )
            except Exception:
                logger.exception(
                    "Failed to supersede interaction %s while closing session %s",
                    notification_id,
                    self.session_id,
                )

        for req_id, fut in list(self._pending_permissions.items()):
            await supersede_notification(
                getattr(self, "_permission_notification_ids", {}).get(req_id)
            )
            if not fut.done():
                fut.set_result(permission_cancelled())
            self._pending_permissions.pop(req_id, None)
            self._permission_requests.pop(req_id, None)
            getattr(self, "_permission_notification_ids", {}).pop(req_id, None)
        pending_elicitations = getattr(self, "_pending_elicitations", {})
        elicitation_requests = getattr(self, "_elicitation_requests", {})
        for req_id, fut in list(pending_elicitations.items()):
            await supersede_notification(
                getattr(self, "_elicitation_notification_ids", {}).get(req_id)
            )
            if not fut.done():
                fut.set_result({"action": "cancel"})
            pending_elicitations.pop(req_id, None)
            elicitation_requests.pop(req_id, None)
            getattr(self, "_elicitation_notification_ids", {}).pop(req_id, None)
        self._append_transcript(
            "session_closed",
            {"reason": reason, "prior_status": prior_status},
        )
        self._flush_transcript()
        await self._drain_transcripts()
        if self.connection:
            try:
                await self.connection.disconnect()
            except Exception:
                logger.exception(
                    "Provider disconnect failed while closing session %s",
                    self.session_id,
                )
            finally:
                self.connection = None
        self.session.status = "closed"
        self.session.updated_at = datetime.now(UTC)
        await self._save_session_preserving_external_browser_async()
        if reconcile_workspace:
            await self.manager.reconcile_closed_sessions([self.session_id])
        logger.info(
            "Live agent session closed",
            extra={
                "session_id": self.session_id,
                "prior_status": prior_status,
                "close_reason": reason,
            },
        )
        self.manager._invalidate_provider_overview()
        return True


class AgentSessionManager:
    """Tracks many concurrent ACP sessions (one subprocess each)."""

    def __init__(
        self,
        settings: Settings,
        store: Store,
        dispatch_store: Any | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self.dispatch_store = dispatch_store
        self._runtimes: dict[str, AgentSessionRuntime] = {}
        self._quiescing = False
        self._accepting = True
        self._last_error: str | None = None
        self._resume_on_start = True
        self._startup_complete = True
        self._startup_phase = "ready"
        self._startup_error: str | None = None
        self._startup_total = 0
        self._startup_eager = 0
        self._startup_deferred = 0
        self._startup_blocked = 0
        self._startup_recovered = 0
        self._startup_failed = 0
        self._startup_session_id: str | None = None
        self._startup_decisions: list[dict[str, str]] = []
        self._restart_handoff_tasks: dict[str, asyncio.Task[None]] = {}
        self._default_label = "default"
        self._lock = asyncio.Lock()
        self._reconnect_lock = asyncio.Lock()
        # Runtime publication also happens after awaited provider startup. A
        # synchronous fleet repair probe must be able to fence that final
        # publication atomically with its no-live-runtime observation.
        self._runtime_lifecycle_lock = RLock()
        self._terminal_repair_fences: dict[str, str] = {}
        self._terminal_repair_fence_acquisitions: dict[str, str] = {}
        self._reconnect_task: asyncio.Task[bool] | None = None
        self._recovery_coordinator_task: asyncio.Task[None] | None = None
        self._recovery_wake = asyncio.Event()
        self._recovery_tasks: dict[str, asyncio.Task[None]] = {}
        self._admitting_sessions: set[str] = set()
        self._recovery_metrics: dict[str, int] = {
            "attempted": 0,
            "succeeded": 0,
            "failed": 0,
            "blocked": 0,
            "exhausted": 0,
            "coalesced": 0,
        }
        self._label_locks: dict[str, asyncio.Lock] = {}
        self.async_runtime: AsyncRuntime | None = None
        self.browser = BrowserManager(settings.data_dir)
        self.workspace_manager = WorkspaceManager(settings, store)
        self.workspace_manager.live_runtime = lambda session_id: (
            session_id in self._admitting_sessions
            or ((runtime := self.get(session_id)) is not None and not runtime._closed)
        )
        self.completion_handler: (
            Callable[[str, dict[str, Any]], Awaitable[Any] | Any] | None
        ) = None
        self.progress_handler: (
            Callable[[str, dict[str, Any]], Awaitable[Any] | Any] | None
        ) = None
        self.assigned_mcp_environment_resolver: (
            Callable[[AgentSession], dict[str, str] | None] | None
        ) = None

    def _invalidate_provider_overview(self) -> None:
        """Discard local provider evidence after an ACP runtime lifecycle change."""
        from pa.fleet.overview import cache_for

        try:
            cache_for(self.settings.data_dir).invalidate(
                self.settings.instance_id, "providers"
            )
        except OSError, RuntimeError, ValueError:
            logger.warning(
                "Could not invalidate Fleet provider snapshot", exc_info=True
            )

    async def _offload(
        self, operation: str, call, *args, timeout: float | None = None, **kwargs
    ):
        if self.async_runtime:
            return await self.async_runtime.run_blocking(
                operation, call, *args, timeout=timeout, **kwargs
            )
        kwargs.pop("wait_for_completion", None)
        return await asyncio.to_thread(call, *args, **kwargs)

    async def record_card_disposition_status(
        self, session_id: str, payload: dict[str, Any]
    ) -> None:
        """Persist an owning-authority acknowledgement into the chat transcript."""
        runtime = self.get(session_id)
        if runtime and not getattr(runtime, "_closed", False):
            try:
                runtime._append_transcript("card_disposition", payload)
                runtime._flush_transcript()
                await runtime._drain_transcripts()
            except Exception:
                failed = dict(payload)
                failed.update(
                    {
                        "persistence": "failed",
                        "authority_acknowledged": True,
                        "status": "persistence_failed",
                        "reason": (
                            "PA acknowledged the disposition, but the local "
                            "transcript acknowledgement could not be persisted."
                        ),
                    }
                )
                runtime._emit_live(
                    {
                        "type": "card_disposition",
                        "session_id": session_id,
                        "payload": failed,
                        "created_at": datetime.now(UTC).isoformat(),
                    }
                )
                raise
            return
        event = TranscriptEvent(
            session_id=session_id,
            seq=self.store.next_transcript_seq(session_id),
            event_type="card_disposition",
            payload=payload,
        )
        await self._offload(
            "sqlite.card_disposition_append",
            self.store.append_transcript_events,
            [event],
        )

    async def _new_runtime(
        self,
        session: AgentSession,
        *,
        agent_env: dict[str, str] | None = None,
        mcp_private_env: dict[str, str] | None = None,
        startup_trace: SessionStartupTrace | None = None,
    ) -> AgentSessionRuntime:
        supplied_mcp_environment = dict(mcp_private_env or {})
        derived_mcp_environment: dict[str, str] = {}
        if self.assigned_mcp_environment_resolver is not None:
            derived_mcp_environment = dict(
                self.assigned_mcp_environment_resolver(session) or {}
            )
        assigned_names = {
            ASSIGNED_SERVICE_MODE_ENV,
            ASSIGNED_SERVICE_DISPATCH_ENV,
            ASSIGNED_SERVICE_SESSION_ENV,
        }
        supplied_assignment = assigned_names & supplied_mcp_environment.keys()
        if supplied_assignment and not derived_mcp_environment:
            raise AgentSessionRecoveryError(
                "assigned MCP environment is not backed by the durable dispatch ledger"
            )
        mismatched = {
            name
            for name, value in derived_mcp_environment.items()
            if name in supplied_mcp_environment
            and supplied_mcp_environment[name] != value
        }
        if mismatched:
            raise AgentSessionRecoveryError(
                "assigned MCP environment conflicts with the durable dispatch binding"
            )
        supplied_mcp_environment.update(derived_mcp_environment)
        initial_seq = await self._offload(
            "sqlite.transcript_sequence",
            lambda: self.store.next_transcript_seq(session.id) - 1,
        )
        return AgentSessionRuntime(
            self,
            session,
            agent_env=agent_env,
            mcp_private_env=supplied_mcp_environment,
            initial_transcript_seq=initial_seq,
            startup_trace=startup_trace,
        )

    def label_lock(self, label: str) -> asyncio.Lock:
        return self._label_locks.setdefault(label, asyncio.Lock())

    def begin_startup(self) -> None:
        """Fence external session admission while durable recovery runs."""
        self._startup_complete = False
        self._startup_phase = "recovering"
        self._startup_error = None
        self._startup_total = 0
        self._startup_eager = 0
        self._startup_deferred = 0
        self._startup_blocked = 0
        self._startup_recovered = 0
        self._startup_failed = 0
        self._startup_session_id = None
        self._startup_decisions = []

    def complete_startup(self, error: BaseException | None = None) -> None:
        self._startup_error = str(error)[:1000] if error else None
        self._startup_phase = "failed" if error else "ready"
        self._startup_complete = error is None
        self._startup_session_id = None
        if error is None and self.settings.agent_enabled:
            self._start_recovery_coordinator()

    @property
    def startup_complete(self) -> bool:
        return self._startup_complete

    def startup_state(self) -> dict[str, Any]:
        return {
            "phase": self._startup_phase,
            "complete": self._startup_complete,
            "error": self._startup_error,
            "total": self._startup_total,
            "eager": self._startup_eager,
            "deferred": self._startup_deferred,
            "blocked": self._startup_blocked,
            "recovered": self._startup_recovered,
            "failed": self._startup_failed,
            "session_id": self._startup_session_id,
        }

    def startup_recovery_diagnostics(self) -> list[dict[str, str]]:
        """Explain per-session startup decisions after the snapshot is cleared."""
        return list(self._startup_decisions)

    def request_recovery(self, _session_id: str | None = None) -> None:
        """Wake the server-owned coordinator after loss, network return, or demand."""
        self._start_recovery_coordinator()
        self._recovery_wake.set()

    def recovery_diagnostics(self) -> dict[str, Any]:
        pending = []
        oldest_pending_admission: str | None = None
        contradictory_states = 0
        unintended_chat_closures = 0
        for session in self.store.list_sessions():
            recovery = dict(session.recovery_json or {})
            durable = dict((session.config_json or {}).get(_DURABLE_RUNTIME_KEY) or {})
            admissions = list(durable.get("queued_prompts") or [])
            if durable.get("in_flight"):
                admissions.append(durable["in_flight"])
            for admission in admissions:
                created_at = str(admission.get("created_at") or "")
                if created_at and (
                    oldest_pending_admission is None
                    or created_at < oldest_pending_admission
                ):
                    oldest_pending_admission = created_at
            runtime = self.get(session.id)
            if admissions and runtime and not runtime._queue and not runtime._in_flight:
                contradictory_states += 1
            if (
                session.purpose == "chat"
                and session.status == "closed"
                and session.archived_at is None
            ):
                unintended_chat_closures += 1
            if recovery.get("next_retry_at") or recovery.get("blocked"):
                pending.append(
                    {
                        "session_id": session.id,
                        "attempts": int(recovery.get("attempts") or 0),
                        "next_retry_at": recovery.get("next_retry_at"),
                        "blocked": bool(recovery.get("blocked")),
                        "code": recovery.get("code"),
                    }
                )
        return {
            "metrics": dict(self._recovery_metrics),
            "in_flight": sorted(self._recovery_tasks),
            "pending": pending,
            "oldest_pending_admission": oldest_pending_admission,
            "contradictory_states": contradictory_states,
            "unintended_chat_closures": unintended_chat_closures,
        }

    def _start_recovery_coordinator(self) -> None:
        task = self._recovery_coordinator_task
        if task and not task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Some embedders finish their startup fence synchronously.  The
            # first async demand/startup continuation will start the loop.
            return
        self._recovery_coordinator_task = loop.create_task(
            self._recovery_loop(), name="pa-agent-recovery-coordinator"
        )
        self._recovery_wake.set()

    async def _recovery_loop(self) -> None:
        while self._accepting and not self._quiescing:
            try:
                await self._recover_unscheduled_restart_handoffs()
                await self._recovery_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Agent recovery coordinator sweep failed")
            self._recovery_wake.clear()
            try:
                await asyncio.wait_for(self._recovery_wake.wait(), timeout=5.0)
            except TimeoutError:
                pass

    async def _recover_unscheduled_restart_handoffs(self) -> None:
        if not self._startup_complete:
            return
        pending = await self._offload(
            "sqlite.restart_handoffs_watchdog", self.store.list_restart_handoffs,
            statuses=("requested", "waiting_for_turn_end"),
        )
        for receipt in pending:
            self._schedule_restart_handoff(receipt.id)
        if self._resume_on_start:
            await self._resume_restart_handoffs(replay_only=True)

    async def _recovery_once(self, *, now: datetime | None = None) -> None:
        if not self._startup_complete or self._should_abort_recovery():
            return
        now = now or datetime.now(UTC)
        sessions = await self._offload(
            "agent.recovery_sessions", self.store.list_sessions,
            exclude_statuses=("closed",), include_archived=False, timeout=30.0
        )
        due: list[AgentSession] = []
        for session in sessions:
            if session.id in self._admitting_sessions:
                continue
            if session.archived_at or session.status == "closed":
                continue
            if session.control_mode == "human" and session.purpose == "automated_run":
                continue
            durable = dict((session.config_json or {}).get(_DURABLE_RUNTIME_KEY) or {})
            if durable.get("queue_paused"):
                continue
            eligibility = await self._automatic_recovery_eligibility(session)
            if not eligibility:
                continue
            runtime = self.get(session.id)
            if runtime and runtime.connected:
                continue
            recovery = dict(session.recovery_json or {})
            if recovery.get("blocked"):
                continue
            raw_retry = recovery.get("next_retry_at")
            if raw_retry:
                try:
                    if datetime.fromisoformat(raw_retry) > now:
                        continue
                except (TypeError, ValueError):
                    pass
            if session.id in self._recovery_tasks:
                self._recovery_metrics["coalesced"] += 1
                continue
            due.append(session)
        for session in due[: self.settings.agent_recovery_concurrency]:
            task = asyncio.create_task(
                self._coordinate_recovery(session.id),
                name=f"pa-agent-recover-{session.id}",
            )
            self._recovery_tasks[session.id] = task
            task.add_done_callback(
                lambda completed, sid=session.id: (
                    self._recovery_tasks.pop(sid, None)
                    if self._recovery_tasks.get(sid) is completed
                    else None
                )
            )

    async def _coordinate_recovery(self, session_id: str) -> None:
        if session_id in self._admitting_sessions:
            return
        self._recovery_metrics["attempted"] += 1
        runtime = self.get(session_id)
        if runtime and not runtime.connected and not runtime.prompting:
            connection = runtime.connection
            runtime.connection = None
            runtime._closed = True
            if connection:
                try:
                    await connection.disconnect()
                except Exception:
                    logger.debug("Failed runtime disconnect before recovery", exc_info=True)
            with self._runtime_lifecycle_lock:
                if self._runtimes.get(session_id) is runtime:
                    self._runtimes.pop(session_id, None)
        try:
            recovered = await self.recover_session(session_id)
        except SessionAdmissionInProgress:
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            session = await self._offload(
                "sqlite.agent_session_read", self.store.get_session, session_id
            )
            if session:
                await self._mark_recovery_interrupted(
                    self._snapshot_from_persisted(session), exc
                )
                current = await self._offload(
                    "sqlite.agent_session_read", self.store.get_session, session_id
                )
                if current and current.recovery_json.get("blocked"):
                    self._recovery_metrics["blocked"] += 1
                    if current.recovery_json.get("exhausted"):
                        self._recovery_metrics["exhausted"] += 1
            self._recovery_metrics["failed"] += 1
            return
        recovered.session.recovery_json = {}
        await recovered._save_session_preserving_external_browser_async()
        self._recovery_metrics["succeeded"] += 1

    def require_startup_complete(self) -> None:
        if not self._startup_complete:
            raise AgentStartupNotReady(
                "Durable ACP session recovery is still in progress"
                if self._startup_phase != "failed"
                else "Durable ACP session recovery failed"
            )

    async def _prepare_workspace(
        self,
        session: AgentSession,
        *,
        requested_cwd: str | None,
        provider_id: str,
        mode_id: str | None = None,
    ) -> dict[str, str]:
        """Provision or recover the durable workspace before spawning a provider."""
        prior_config = dict(session.config_json or {})
        prior_context = dict(prior_config.get("execution_context") or {})
        binding = dict(session.execution_binding or {})
        persisted_binding = dict(binding)
        if not binding:
            # Legacy repair deliberately prefers the durable lease over mutable
            # card associations.  This preserves both records and recovers the
            # provider against the fence that actually owns its worktree.
            leases = await self._offload(
                "workspace.session_leases",
                lambda: [
                    lease for lease in self.workspace_manager.list()
                    if lease.session_id == session.id and lease.state != "cleaned"
                ],
                timeout=30.0,
            )
            legacy_context_repos = list(prior_context.get("repositories") or [])
            if leases:
                first = leases[0]
                binding = {
                    "version": 1,
                    "repository_ids": [lease.repository_id for lease in leases],
                    "execution_card_id": first.card_id,
                    "execution_project_id": first.project_id,
                    "worktree_paths": [lease.worktree_path for lease in leases],
                    "lease_ids": [lease.id for lease in leases],
                    "branch": first.branch,
                    "base_sha": first.base_sha,
                    "cwd": first.worktree_path,
                    "origin_instance_id": session.origin_instance_id,
                    "legacy_mismatch": bool(
                        first.card_id != session.card_id
                        or first.project_id != session.project_id
                    ),
                }
            elif legacy_context_repos:
                binding = {
                    "version": 1,
                    "repository_ids": [
                        str(repo.get("repository_id")) for repo in legacy_context_repos
                        if repo.get("repository_id")
                    ],
                    "execution_card_id": prior_context.get("card_id", session.card_id),
                    "execution_project_id": prior_context.get("project_id", session.project_id),
                    "worktree_paths": [
                        str(repo.get("worktree_path")) for repo in legacy_context_repos
                        if repo.get("worktree_path")
                    ],
                    "lease_ids": [str(repo.get("lease_id")) for repo in legacy_context_repos if repo.get("lease_id")],
                    "cwd": prior_context.get("cwd") or session.cwd,
                    "origin_instance_id": session.origin_instance_id,
                }
            else:
                binding = {
                    "version": 1,
                    "execution_card_id": session.card_id,
                    "execution_project_id": session.project_id,
                    "origin_instance_id": session.origin_instance_id,
                }
            if leases or legacy_context_repos:
                await self._offload(
                    "sqlite.execution_binding_legacy_repair",
                    self.store.set_session_execution_binding,
                    session.id,
                    binding,
                    reason="legacy_workspace_recovered",
                    expected_binding=persisted_binding,
                )
                session.execution_binding = dict(binding)
                persisted_binding = dict(session.execution_binding or {})
        execution_card_id = binding.get("execution_card_id")
        execution_project_id = binding.get("execution_project_id")
        prior_authority = dict(prior_context.get("authority_instance") or {})
        prior_attachments = dict(prior_context.get("attachments") or {})
        materialization_plan = dict(prior_context.get("materialization_plan") or {})
        execution_profile = materialization_plan.get("profile")
        authority_instance = (
            {
                "id": session.authority_instance_id,
                "name": prior_authority.get("name") or session.authority_instance_id,
            }
            if session.authority_instance_id
            else prior_authority or None
        )
        provenance = {
            "version": 1,
            "realm_id": session.realm_id,
            "principal_id": session.principal_id,
            "dispatch_id": session.dispatch_id,
        }
        data_dir = self.settings.data_dir.expanduser().resolve()

        def unusable_cwd(value: str | None) -> str | None:
            if not value:
                return None
            path = Path(value).expanduser().resolve()
            if path == data_dir or data_dir in path.parents:
                return "stale_data_dir_cwd_removed"
            if not path.is_dir():
                return "missing_cwd_removed"
            return None

        drop_reason = unusable_cwd(binding.get("cwd")) or unusable_cwd(requested_cwd)
        if drop_reason:
            dropped = binding.get("cwd") or requested_cwd
            logger.warning(
                "Ignoring unusable session cwd %s (%s) for session %s; "
                "rematerializing an allowed workspace",
                dropped,
                drop_reason,
                session.id,
            )
            if unusable_cwd(requested_cwd):
                requested_cwd = None
            if unusable_cwd(binding.get("cwd")):
                binding.pop("cwd", None)
                await self._offload(
                    "sqlite.execution_binding_stale_cwd_remove",
                    self.store.set_session_execution_binding,
                    session.id,
                    binding,
                    reason=drop_reason,
                    expected_binding=persisted_binding,
                )
                session.execution_binding = dict(binding)
                persisted_binding = dict(session.execution_binding or {})
        dispatch = (
            await self._offload(
                "dispatch.workspace_admission_read", self.dispatch_store.get, session.dispatch_id
            )
            if self.dispatch_store is not None and session.dispatch_id else None
        )
        allow_concurrent_workspace = (
            bool(dispatch and dispatch.allow_concurrent) if session.dispatch_id else True
        )
        session.status = "provisioning"
        config = dict(session.config_json or {})
        config["provisioning"] = {
            "state": "provisioning",
            "stage": "workspace",
            "retryable": True,
        }
        session.config_json = config
        await self._offload(
            "sqlite.agent_session_save", self.store.save_session, session
        )
        try:
            workspace = None
            if execution_profile == "repository":
                if execution_project_id:
                    project = await self._offload(
                        "sqlite.project_read",
                        self.store.get_project,
                        execution_project_id,
                    )
                    if project is None:
                        raise WorkspaceProvisioningError(
                            f"Execution project {execution_project_id} is not available on this instance; sync or link the original project checkout, then retry exact-session recovery"
                        )
                    workspace = await self._offload(
                        "workspace.project_provision",
                        self.workspace_manager.provision_project,
                        allow_concurrent=allow_concurrent_workspace,
                        project_id=execution_project_id,
                        session_id=session.id,
                        card_id=execution_card_id,
                        realm_id=getattr(
                            project, "realm_id", self.settings.primary_realm
                        ),
                        provider_id=provider_id,
                        timeout=900.0,
                    )
                else:
                    workspace = await self._offload(
                        "workspace.contract_provision",
                        self.workspace_manager.provision_contract,
                        allow_concurrent=allow_concurrent_workspace,
                        repositories=list(
                            materialization_plan.get("repositories") or []
                        ),
                        session_id=session.id,
                        card_id=execution_card_id,
                        project_id=None,
                        realm_id=session.realm_id,
                        provider_id=provider_id,
                        timeout=900.0,
                    )
                if workspace is None or not workspace.repositories:
                    raise WorkspaceProvisioningError(
                        "Repository materialization did not produce a verified leased worktree"
                    )
            elif not execution_profile and execution_project_id:
                project = await self._offload(
                    "sqlite.project_read", self.store.get_project, execution_project_id
                )
                if project is None:
                    raise WorkspaceProvisioningError(
                        f"Execution project {execution_project_id} is not available on this instance; sync or link the original project checkout, then retry exact-session recovery"
                    )
                workspace = await self._offload(
                    "workspace.project_provision",
                    self.workspace_manager.provision_project,
                    allow_concurrent=allow_concurrent_workspace,
                    project_id=execution_project_id,
                    session_id=session.id,
                    card_id=execution_card_id,
                    realm_id=getattr(project, "realm_id", self.settings.primary_realm),
                    provider_id=provider_id,
                    timeout=900.0,
                )
            if workspace is None and (
                execution_profile == "repository" or execution_project_id
            ):
                raise WorkspaceProvisioningError(
                    "Project has no linked repositories to provision"
                )
            if workspace is None:
                workspace = await self._offload(
                    "workspace.scratch_provision",
                    self.workspace_manager.scratch_workspace,
                    session_id=session.id,
                    card_id=execution_card_id,
                    project_id=execution_project_id,
                    requested_cwd=binding.get("cwd") or requested_cwd,
                    provider_id=provider_id,
                    workspace_kind=(
                        "operational"
                        if execution_profile == "operations"
                        else "artifact"
                        if execution_profile == "research"
                        else "scratch"
                    ),
                    timeout=120.0,
                )
            context = workspace.execution_context(self.settings, provider_id)
            repos = list(context.get("repositories") or [])
            materialized = {
                "repository_ids": [r.get("repository_id") for r in repos],
                "worktree_paths": [r.get("worktree_path") for r in repos],
                "lease_ids": [r.get("lease_id") for r in repos],
                "branch": repos[0].get("branch") if repos else None,
                "base_sha": repos[0].get("base_sha") if repos else None,
                "cwd": workspace.cwd,
            }
            mismatches = [
                key for key, value in materialized.items()
                if key in binding and binding[key] != value
            ]
            if mismatches:
                raise WorkspaceBindingMismatch(
                    "Original execution binding differs in: " + ", ".join(mismatches)
                    + ". " + WorkspaceBindingMismatch.remedy
                )
            # Only add missing materialization facts. Existing provenance is never
            # rewritten, including a base SHA retained when an unusable cwd was removed.
            final_binding = {**binding, **materialized}
            if final_binding != persisted_binding:
                await self._offload(
                    "sqlite.execution_binding_materialize",
                    self.store.set_session_execution_binding,
                    session.id,
                    final_binding,
                    reason=(
                        "workspace_materialized"
                        if persisted_binding
                        else "workspace_binding_initialized"
                    ),
                    expected_binding=persisted_binding,
                )
                session.execution_binding = dict(final_binding)
                persisted_binding = dict(session.execution_binding or {})
            execution_policy = provider_execution_policy(provider_id, mode_id)
            if execution_policy:
                context["approval_policy"] = execution_policy["approval_policy"]
                provider_context = dict(context.get("provider_context") or {})
                provider_context.update(execution_policy)
                context["provider_context"] = provider_context
            if authority_instance:
                context["authority_instance"] = authority_instance
            if prior_attachments:
                context["attachments"] = prior_attachments
            context["realm_id"] = session.realm_id
            context["principal_id"] = session.principal_id
            context["dispatch_id"] = session.dispatch_id
            context["provenance"] = provenance
            session.cwd = workspace.cwd
            config = dict(session.config_json or {})
            config["execution_context"] = context
            config["provisioning"] = {
                "state": "ready",
                "stage": "verified",
                "retryable": True,
            }
            session.config_json = config
            session.status = "connecting"
            await self._offload(
                "sqlite.agent_session_save", self.store.save_session, session
            )
            return context_environment(context)
        except Exception as exc:
            binding_blocked = isinstance(exc, WorkspaceBindingMismatch)
            project_blocked = bool(execution_project_id and _project_recovery_block(exc))
            workspace_blocked = project_blocked or binding_blocked
            session.status = (
                RECOVERY_BLOCKED_STATUS if workspace_blocked else "provisioning_failed"
            )
            config = dict(session.config_json or {})
            if binding_blocked:
                pass  # Preserve original context as evidence for exact-workspace repair.
            elif session.dispatch_id:
                config["execution_context"] = {
                    "authority_instance": authority_instance,
                    "provenance": provenance,
                }
            else:
                config.pop("execution_context", None)
            config["provisioning"] = {
                "state": "blocked" if workspace_blocked else "failed",
                "stage": "workspace",
                "retryable": not workspace_blocked,
                "manual_retry": workspace_blocked,
                "automatic_retry": not workspace_blocked,
                "error_code": (
                    "workspace_binding_mismatch"
                    if binding_blocked
                    else "project_unavailable_on_instance"
                    if project_blocked
                    else "workspace_provisioning_failed"
                ),
                "action": (
                    WorkspaceBindingMismatch.remedy
                    if binding_blocked
                    else "Sync the project and repository links to this instance, or "
                    "link its checkout; then retry this session. Close the session "
                    "if it is no longer needed."
                    if project_blocked
                    else "Correct the workspace configuration, then retry"
                ),
                "error": str(exc)[:1000],
            }
            if project_blocked:
                config["provisioning"]["retry_on"] = "project_availability_change"
            session.config_json = config
            session.cwd = binding.get("cwd")
            await self._offload(
                "sqlite.agent_session_save", self.store.save_session, session
            )
            if isinstance(exc, WorkspaceProvisioningError):
                raise
            raise WorkspaceProvisioningError(str(exc)) from exc

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

    def _should_abort_admission(self) -> bool:
        from pa.server.shutdown import is_shutting_down

        return (not self._accepting) or self._quiescing or is_shutting_down()

    def _should_abort_recovery(self) -> bool:
        from pa.server.shutdown import is_shutting_down

        return self._quiescing or is_shutting_down()

    def get(self, session_id: str) -> AgentSessionRuntime | None:
        with self._runtime_lifecycle_lock:
            return self._runtimes.get(session_id)

    def acquire_terminal_repair_fence(
        self,
        session_id: str,
        *,
        fence_id: str,
        acquisition_id: str | None = None,
    ) -> AgentSessionRuntime | None:
        """Fence future runtime publication after observing no live runtime.

        Provider startup is asynchronous and used to publish directly into the
        runtime map after its final await. The lifecycle lock makes that
        publication race with this check instead of racing after it. A fence is
        intentionally retained: terminal repair is only valid for a closed,
        nonrecoverable dispatch, so publishing a later runtime for that exact PA
        session would invalidate the evidence used to retire it.
        """
        with self._runtime_lifecycle_lock:
            runtime = self._runtimes.get(session_id)
            if runtime is not None and not getattr(runtime, "_closed", False):
                return runtime
            self._terminal_repair_fences[session_id] = fence_id
            self._terminal_repair_fence_acquisitions[session_id] = (
                acquisition_id or str(uuid4())
            )
            return None

    def terminal_repair_fence_id(self, session_id: str) -> str | None:
        ledger = self.dispatch_store
        with self._runtime_lifecycle_lock:
            fence_acquisition_id = self._terminal_repair_fence_acquisitions.get(
                session_id
            )
            fence_id = self._terminal_repair_fences.get(session_id)
        if ledger is None:
            return fence_id
        record = ledger.by_session(session_id)
        reservation = record.terminal_repair_reservation if record else None
        reservation_state = reservation.get("state") if reservation else None
        if (
            record
            and record.target_instance_id == self.settings.instance_id
            and reservation
            and reservation_state in {"prepared", "committed"}
        ):
            durable_fence_id = str(reservation.get("reservation_id") or "").strip()
            if durable_fence_id:
                with self._runtime_lifecycle_lock:
                    if session_id not in self._terminal_repair_fences:
                        self._terminal_repair_fences[session_id] = durable_fence_id
                        self._terminal_repair_fence_acquisitions[session_id] = (
                            f"durable:{uuid4()}"
                        )
                    return self._terminal_repair_fences[session_id]
        repair_lost = bool(
            record
            and (
                reservation_state in {"superseded_by_completion", "aborted"}
                or record.state not in {"queued", "dispatching", "running"}
                or record.completion_payload is not None
                or record.acknowledged_at is not None
            )
        )
        if fence_id is not None and repair_lost:
            released = self.release_terminal_repair_fence(
                session_id,
                fence_id=fence_id,
                acquisition_id=fence_acquisition_id,
            )
            if released:
                return None
            # A same-key retry may have installed a newer acquisition between
            # the snapshot above and the generation-checked release. Never tell
            # admission that the session is unfenced after losing that race.
            with self._runtime_lifecycle_lock:
                return self._terminal_repair_fences.get(session_id)
        return fence_id

    def release_terminal_repair_fence(
        self,
        session_id: str,
        *,
        fence_id: str,
        acquisition_id: str | None = None,
    ) -> bool:
        """Release only the exact losing ephemeral repair fence."""
        with self._runtime_lifecycle_lock:
            if self._terminal_repair_fences.get(session_id) != fence_id:
                return False
            if (
                acquisition_id is not None
                and self._terminal_repair_fence_acquisitions.get(session_id)
                != acquisition_id
            ):
                return False
            self._terminal_repair_fences.pop(session_id, None)
            self._terminal_repair_fence_acquisitions.pop(session_id, None)
            return True

    def bind_dispatch_store(self, dispatch_store: Any | None) -> None:
        """Bind the canonical ledger used to restore durable repair fences."""
        self.dispatch_store = dispatch_store

    def _require_not_terminal_repair_fenced(self, session_id: str) -> None:
        fence_id = self.terminal_repair_fence_id(session_id)
        if fence_id is not None:
            raise AgentSessionRecoveryError(
                "PA session admission is fenced by terminal dispatch repair"
            )

    async def _publish_runtime(self, runtime: AgentSessionRuntime) -> None:
        """Publish a started runtime unless terminal evidence fenced its session."""
        durable_fence_id = self.terminal_repair_fence_id(runtime.session_id)
        with self._runtime_lifecycle_lock:
            fence_id = self._terminal_repair_fences.get(runtime.session_id)
            fence_id = fence_id or durable_fence_id
            if fence_id is None:
                self._runtimes[runtime.session_id] = runtime
                return
        await runtime.close(
            reason="terminal_dispatch_repair_fenced",
            reconcile_workspace=False,
        )
        raise RuntimeError(
            "session runtime publication was fenced by terminal dispatch repair"
        )

    def list_sessions(self) -> list[AgentSession]:
        with self._runtime_lifecycle_lock:
            return [rt.session for rt in self._runtimes.values()]

    def list_runtimes(self) -> list[AgentSessionRuntime]:
        with self._runtime_lifecycle_lock:
            return list(self._runtimes.values())

    async def set_session_control(
        self, session_id: str, mode: Literal["automation", "human"]
    ) -> AgentSession:
        session = await self._offload(
            "sqlite.agent_session_read", self.store.get_session, session_id
        )
        if not session:
            raise LookupError("Session not found")
        if session.purpose != "automated_run":
            raise ValueError("Takeover is available only for automated runs")
        runtime = self.get(session_id)
        if runtime and not runtime._closed:
            await runtime.set_control_mode(mode)
            return runtime.session
        session.control_mode = mode
        session.updated_at = datetime.now(UTC)
        await self._offload(
            "sqlite.agent_session_save", self.store.save_session, session
        )
        if mode == "automation":
            self.request_recovery(session_id)
        return session

    async def release_session_process(self, session_id: str, *, reason: str) -> bool:
        runtime = self.get(session_id)
        if not runtime or runtime._closed:
            return False
        try:
            await runtime.release_provider(reason=reason)
        finally:
            with self._runtime_lifecycle_lock:
                if self._runtimes.get(session_id) is runtime and runtime._closed:
                    self._runtimes.pop(session_id, None)
            self._invalidate_provider_overview()
        return True

    async def archive_session(
        self, session_id: str, *, reason: str = "user_archive"
    ) -> AgentSession:
        session = await self._offload(
            "sqlite.agent_session_read", self.store.get_session, session_id
        )
        if not session:
            raise LookupError("Session not found")
        if session.purpose != "chat":
            raise ValueError("Only conversations can be archived")
        runtime = self.get(session_id)
        if runtime and (
            runtime.prompting
            or runtime._queue
            or runtime._pending_permissions
            or runtime._pending_elicitations
        ):
            raise RuntimeError("Stop or finish active work before archiving this conversation")
        now = datetime.now(UTC)
        session.archived_at = now
        session.archive_reason = reason
        session.status = "available"
        session.updated_at = now
        await self._offload(
            "sqlite.agent_session_save", self.store.save_session, session
        )
        if runtime and not runtime._closed:
            runtime.session = session
            await self.release_session_process(session_id, reason=reason)
        return session

    async def unarchive_session(self, session_id: str) -> AgentSession:
        session = await self._offload(
            "sqlite.agent_session_read", self.store.get_session, session_id
        )
        if not session:
            raise LookupError("Session not found")
        if session.purpose != "chat":
            raise ValueError("Only conversations can be unarchived")
        session.archived_at = None
        session.archive_reason = None
        session.status = "available" if session.status == "closed" else session.status
        session.updated_at = datetime.now(UTC)
        await self._offload("sqlite.agent_session_save", self.store.save_session, session)
        return session

    async def pin_session(self, session_id: str, *, pinned: bool) -> AgentSession:
        session = await self._offload(
            "sqlite.agent_session_read", self.store.get_session, session_id
        )
        if not session:
            raise LookupError("Session not found")
        if session.purpose != "chat":
            raise ValueError("Only conversations can be pinned")
        session.pinned_at = datetime.now(UTC) if pinned else None
        session.updated_at = datetime.now(UTC)
        await self._offload("sqlite.agent_session_save", self.store.save_session, session)
        return session

    async def reconcile_closed_sessions(self, session_ids: list[str]) -> None:
        """Expire closed-session leases, then reconcile and collect once."""
        unique_session_ids = list(dict.fromkeys(session_ids))
        for session_id in unique_session_ids:
            try:
                await self._offload(
                    "workspace.expire_session",
                    self.workspace_manager.expire_session,
                    session_id,
                    timeout=30.0,
                )
            except Exception:
                logger.exception(
                    "Workspace expiration after session close failed for %s",
                    session_id,
                )
        try:
            await self._offload(
                "workspace.reconcile_terminal_state",
                self.workspace_manager.reconcile_terminal_state,
                timeout=30.0,
            )
            active_session_ids = {
                runtime.session_id
                for runtime in self.list_runtimes()
                if not runtime._closed
            }
            await self._offload(
                "workspace.collect_garbage",
                self.workspace_manager.collect_garbage,
                active_session_ids=active_session_ids,
                timeout=120.0,
            )
        except Exception:
            # Session closure is authoritative. Cleanup is recoverable via the
            # explicit workspace reconciliation API or the next agent startup.
            logger.exception(
                "Workspace reconciliation after closing sessions failed",
                extra={"session_ids": unique_session_ids},
            )

    def progress(self) -> QuiesceProgress:
        live = [
            rt
            for rt in self._runtimes.values()
            if rt.connected and not getattr(rt, "_closed", False)
        ]
        connected = len(live)
        prompting = sum(1 for rt in live if rt.prompting)
        queued = sum(len(rt._queue) for rt in live)
        provider_concurrency: dict[str, dict[str, int]] = {}
        for runtime in live:
            provider = (runtime.session.agent_name or "unknown").strip().lower()
            counts = provider_concurrency.setdefault(
                provider,
                {
                    "connected_runtimes": 0,
                    "idle_sessions": 0,
                    "prompting_turns": 0,
                    "active_capacity_consumers": 0,
                    "queued_prompts": 0,
                },
            )
            counts["connected_runtimes"] += 1
            counts["queued_prompts"] += len(runtime._queue)
            if runtime.prompting:
                counts["prompting_turns"] += 1
                counts["active_capacity_consumers"] += 1
            else:
                counts["idle_sessions"] += 1
        return QuiesceProgress(
            phase="quiescing"
            if self._quiescing
            else ("prompting" if self.prompting else "idle"),
            connected=self.connected,
            prompting=self.prompting,
            quiescing=self._quiescing,
            # active_sessions remains a mixed-version alias for connected
            # runtimes. Placement never uses it when the typed fields exist.
            active_sessions=connected,
            connected_runtimes=connected,
            idle_sessions=connected - prompting,
            prompting_turns=prompting,
            active_capacity_consumers=prompting,
            queued_prompts=queued,
            provider_concurrency=provider_concurrency,
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
                        "provider": rt.session.agent_name,
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
        return "Agent ready; provider processes start on demand"

    def _default_requires_provider_resolution(
        self, label: str | None, resume_external_id: str | None
    ) -> bool:
        """Use current defaults when the instance session has nothing to resume."""
        return label == self._default_label and not resume_external_id

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
            get_preferences_store(self.settings.data_dir)
            .load()
            .agent_auto_approve_permissions
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

    async def should_auto_approve_async(self, principal_id: str | None) -> bool:
        return await self._offload(
            "preferences.agent_auto_approve_read",
            self.should_auto_approve,
            principal_id,
            timeout=10.0,
        )

    async def set_auto_approve_async(
        self,
        value: bool,
        *,
        scope: Literal["user", "global"] = "user",
        principal_id: str | None = None,
    ) -> None:
        await self._offload(
            "preferences.agent_auto_approve_write",
            self.set_auto_approve,
            value,
            scope=scope,
            principal_id=principal_id,
            timeout=10.0,
        )

    async def _project_recovery_available(self, session: AgentSession) -> bool:
        if not session.project_id:
            return False

        def project_ready() -> bool:
            project = self.store.get_project(session.project_id)
            if project is None:
                return False
            list_links = getattr(self.store, "list_project_repositories", None)
            if not callable(list_links):
                return False
            realm_id = getattr(project, "realm_id", self.settings.primary_realm)
            return bool(list_links(session.project_id, realm_id=realm_id))

        return await self._offload(
            "agent.project_recovery_availability",
            project_ready,
            timeout=30.0,
        )

    async def _automatic_recovery_eligibility(
        self, session: AgentSession
    ) -> str | None:
        durable = dict((session.config_json or {}).get(_DURABLE_RUNTIME_KEY) or {})
        if session.archived_at is not None or durable.get("queue_paused"):
            return None
        if session.purpose == "automated_run" and session.control_mode == "human":
            return None
        if session.status in AUTO_RECOVERY_SESSION_STATUSES:
            return "status"
        if (
            durable.get("in_flight")
            or durable.get("queued_prompts")
            or durable.get("pending_permissions")
            or durable.get("lifecycle") in _EAGER_DURABLE_LIFECYCLES
        ):
            return "durable_obligation"
        if session.status != RECOVERY_BLOCKED_STATUS:
            return None
        if await self._project_recovery_available(session):
            return "project_available"
        return None

    @staticmethod
    def _recovery_action(session: AgentSession) -> str:
        provisioning = dict((session.config_json or {}).get("provisioning") or {})
        return str(
            provisioning.get("action")
            or "Retry the session after correcting its workspace configuration, "
            "or close it if it is no longer needed."
        )

    async def start(self, *, resume: bool | None = None) -> None:
        if resume is not None:
            self._resume_on_start = resume
        will_resume = self.settings.agent_enabled and self._resume_on_start
        snapshot, persisted_sessions = await self._offload(
            "agent.startup_state_read",
            lambda: (
                load_quiesce_snapshot(self.settings.data_dir),
                self.store.list_sessions() if will_resume else [],
            ),
            timeout=30.0,
        )
        active_session_ids = {
            session.id
            for session in persisted_sessions
            if session.status in RECOVERY_RETAINED_SESSION_STATUSES
        }
        if will_resume and snapshot and snapshot.resume:
            active_session_ids.update(
                item.session_id
                for item in snapshot.sessions
                if item.session_id and item.status in RECOVERY_RETAINED_SESSION_STATUSES
            )
        try:
            await self._offload(
                "workspace.reconcile_terminal_state",
                self.workspace_manager.reconcile_terminal_state,
                timeout=30.0,
            )
            await self._offload(
                "workspace.collect_garbage",
                self.workspace_manager.collect_garbage,
                active_session_ids=active_session_ids,
                timeout=120.0,
            )
        except Exception:
            logger.exception("Workspace garbage collection failed")
        if not self.settings.agent_enabled:
            if snapshot:
                await self._offload(
                    "agent.quiesce_snapshot_clear",
                    clear_quiesce_snapshot,
                    self.settings.data_dir,
                )
            logger.info("Instance agent disabled")
            return
        from pa.server.shutdown import is_shutting_down

        # Never undo a shutdown fence. stop() leaves quiescing=True; clearing
        # that is fine on a later intentional start, but not once TERM has been
        # observed — otherwise a cancelled startup task can re-admit session/new.
        if is_shutting_down():
            logger.info("Skipping ACP startup because shutdown began")
            return
        self._accepting = True
        self._quiescing = False
        if is_shutting_down():
            self._accepting = False
            self._quiescing = True
            logger.info("Skipping ACP startup because shutdown began")
            return

        if self._resume_on_start:
            persisted_by_id = {session.id: session for session in persisted_sessions}
            recovery_eligibility: dict[str, str] = {}
            for session in persisted_sessions:
                eligibility = await self._automatic_recovery_eligibility(session)
                if eligibility:
                    recovery_eligibility[session.id] = eligibility
                    self._startup_decisions.append(
                        {
                            "session_id": session.id,
                            "decision": "eager",
                            "reason": eligibility,
                        }
                    )
                    if eligibility == "project_available":
                        logger.info(
                            "Project availability changed; retrying blocked ACP "
                            "session %s",
                            session.id,
                        )
                elif session.status == RECOVERY_BLOCKED_STATUS:
                    self._startup_blocked += 1
                    self._startup_decisions.append(
                        {
                            "session_id": session.id,
                            "decision": "blocked",
                            "reason": self._recovery_action(session),
                        }
                    )
                    logger.info(
                        "ACP recovery remains blocked for session %s: %s",
                        session.id,
                        self._recovery_action(session),
                    )
                elif session.status != "closed":
                    if session.status in RECOVERY_RETAINED_SESSION_STATUSES:
                        self._startup_deferred += 1
                        self._startup_decisions.append(
                            {
                                "session_id": session.id,
                                "decision": "deferred",
                                "reason": session.status,
                            }
                        )
                    logger.info(
                        "Deferring ACP recovery for session %s with passive "
                        "status %s",
                        session.id,
                        session.status,
                    )
            recovery: dict[str, SessionSnapshot] = {}
            if snapshot and snapshot.resume:
                recovery.update(
                    {
                        item.session_id: item
                        for item in snapshot.sessions
                        if item.session_id
                        and (
                            item.session_id in recovery_eligibility
                            or (
                                item.session_id not in persisted_by_id
                                and item.status in AUTO_RECOVERY_SESSION_STATUSES
                            )
                        )
                    }
                )
            # Graceful quiesce is an optimization, not the durable owner. A
            # sleeping host, SIGKILL, or power loss never gets a shutdown hook.
            # Reconcile every durable nonterminal admission that was not in the
            # quiesce file so it cannot silently disappear after restart.
            for session in reversed(persisted_sessions):
                if session.id not in recovery_eligibility or session.id in recovery:
                    continue
                recovery[session.id] = self._snapshot_from_persisted(session)
            recovery_items = list(recovery.values())
            recovery_items.sort(
                key=lambda item: (
                    0
                    if item.in_flight
                    else 1
                    if item.queued_prompts
                    else 2,
                    item.session_id or "",
                )
            )
            self._startup_total = len(recovery_items)
            self._startup_eager = len(recovery_items)

            async def recover_one(sess: SessionSnapshot) -> None:
                if self._should_abort_recovery():
                    return
                self._startup_session_id = sess.session_id
                try:
                    recovered = await self._resume_from_snapshot(
                        sess, snapshot or QuiesceSnapshot(reason="recovery")
                    )
                    if recovered is not None and recovered.session.recovery_json:
                        recovered.session.recovery_json = {}
                        await recovered._save_session_preserving_external_browser_async()
                    self._startup_recovered += 1
                    self._startup_decisions.append(
                        {
                            "session_id": sess.session_id or "",
                            "decision": "recovered",
                            "reason": "startup",
                        }
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if self._should_abort_recovery():
                        return
                    self._startup_failed += 1
                    self._startup_decisions.append(
                        {
                            "session_id": sess.session_id or "",
                            "decision": "failed",
                            "reason": str(exc)[:1000],
                        }
                    )
                    self._last_error = str(exc)
                    recovery_state = await self._mark_recovery_interrupted(sess, exc)
                    if recovery_state == RECOVERY_BLOCKED_STATUS:
                        session = persisted_by_id.get(sess.session_id or "")
                        logger.warning(
                            "ACP recovery blocked for session %s: %s",
                            sess.session_id,
                            self._recovery_action(session) if session else str(exc),
                        )
                    else:
                        logger.exception(
                            "Failed to resume session %s", sess.session_id
                        )

            try:
                iterator = iter(recovery_items)

                async def worker() -> None:
                    while not self._should_abort_recovery():
                        try:
                            sess = next(iterator)
                        except StopIteration:
                            return
                        await recover_one(sess)

                await asyncio.gather(
                    *(
                        worker()
                        for _ in range(
                            min(
                                self.settings.agent_recovery_concurrency,
                                len(recovery_items),
                            )
                        )
                    )
                )
                self._startup_session_id = None
                # Legacy top-level queue → default session
                if (
                    not self._should_abort_recovery()
                    and snapshot
                    and snapshot.resume
                    and snapshot.queued_prompts
                ):
                    default = await self.attach_default(_startup_recovery=True)
                    for item in snapshot.queued_prompts:
                        item.session_id = default.session_id
                        default._queue.append(item)
                    await default._checkpoint_runtime_async(lifecycle="queued")
                    default._start_drain()
            finally:
                if snapshot:
                    await self._offload(
                        "agent.quiesce_snapshot_clear",
                        clear_quiesce_snapshot,
                        self.settings.data_dir,
                    )
        elif snapshot:
            await self._offload(
                "agent.quiesce_snapshot_clear",
                clear_quiesce_snapshot,
                self.settings.data_dir,
            )

        # A no-resume boot is intentionally inert until an explicit admission.
        # Durable nonterminal sessions remain recoverable on a later normal boot.
        # The default provider is admitted lazily by attach_default() when an
        # operator actually opens or prompts it. Startup must remain runtime-free
        # when every retained session is passive.

    async def request_restart_handoff(
        self, *, session_id: str, continuation_prompt: str,
        idempotency_key: str,
    ) -> RestartHandoff:
        """Persist a self-restart receipt before waiting for the caller's turn."""
        session = await self._offload(
            "sqlite.agent_session_read", self.store.get_session, session_id
        )
        if not session or session.status == "closed":
            raise AgentSessionRecoveryError("Exact durable session is not recoverable")
        prompt = continuation_prompt.strip()
        if not idempotency_key.strip():
            raise ValueError("idempotency_key is required")
        from uuid import NAMESPACE_URL, uuid5

        handoff_id = str(uuid5(NAMESPACE_URL, f"pa-restart:{session_id}:{idempotency_key}"))
        handoff = RestartHandoff(
            id=handoff_id,
            session_id=session_id,
            idempotency_key=idempotency_key,
            continuation_prompt=prompt,
            continuation_prompt_id=f"restart-handoff:{handoff_id}",
            card_id=session.card_id,
            project_id=session.project_id,
            instance_id=self.settings.instance_id,
            execution_binding=dict(session.execution_binding or {}),
        )
        def committed(receipt):
            if receipt.status == "requested":
                self._schedule_restart_handoff(receipt.id)

        if self.async_runtime:
            handoff = await self.async_runtime.run_blocking(
                "sqlite.restart_handoff_create", self.store.create_restart_handoff,
                handoff, on_commit=committed,
            )
        else:
            handoff = await self._offload(
                "sqlite.restart_handoff_create", self.store.create_restart_handoff, handoff
            )
        if handoff.status == "failed":
            return await self.retry_restart_handoff(
                session_id=session_id, handoff_id=handoff.id
            )
        if handoff.status == "requested":
            self._schedule_restart_handoff(handoff.id)
        return handoff

    async def edit_restart_handoff(
        self, *, session_id: str, handoff_id: str, continuation_prompt: str
    ) -> RestartHandoff:
        return await self._offload(
            "sqlite.restart_handoff_edit",
            self.store.edit_restart_handoff,
            handoff_id,
            session_id=session_id,
            continuation_prompt=continuation_prompt,
        )

    def _schedule_restart_handoff(self, handoff_id: str) -> None:
        task = self._restart_handoff_tasks.get(handoff_id)
        if task is not None and not task.done():
            return
        task = asyncio.create_task(
            self._execute_restart_handoff(handoff_id),
            name=f"pa-restart-handoff-{handoff_id}",
        )
        self._restart_handoff_tasks[handoff_id] = task
        task.add_done_callback(
            lambda completed, receipt_id=handoff_id: (
                self._restart_handoff_tasks.pop(receipt_id, None)
                if self._restart_handoff_tasks.get(receipt_id) is completed
                else None
            )
        )

    async def _execute_restart_handoff(self, handoff_id: str) -> None:
        """Wait outside the initiating turn, flush, quiesce, then ask the host to restart."""
        stage = "waiting_for_turn_end"
        try:
            handoff = await self._offload(
                "sqlite.restart_handoff_read", self.store.get_restart_handoff, handoff_id
            )
            if not handoff or handoff.status not in {
                "requested", "waiting_for_turn_end", "quiescing"
            }:
                return
            await self._offload(
                "sqlite.restart_handoff_waiting", self.store.update_restart_handoff,
                handoff_id, status="waiting_for_turn_end"
            )
            runtime = self.get(handoff.session_id)
            while runtime and runtime.prompting:
                await asyncio.sleep(_QUIESCE_POLL_SECONDS)
                runtime = self.get(handoff.session_id)
            # Assigned-session traffic can become usable while background startup
            # recovery is still finishing. The durable request remains accepted,
            # but shutdown must not race that internal fence.
            while not self._startup_complete:
                if self._startup_phase == "failed":
                    raise AgentStartupNotReady(
                        "Durable ACP session recovery failed before restart"
                    )
                await asyncio.sleep(_QUIESCE_POLL_SECONDS)
            if runtime:
                runtime._flush_transcript()
                await runtime._drain_transcripts(raise_on_timeout=True)
            stage = "quiescing"
            await self._offload(
                "sqlite.restart_handoff_quiescing", self.store.update_restart_handoff,
                handoff_id, status="quiescing"
            )
            await self.quiesce(reason=f"restart-handoff:{handoff_id}")
            stage = "restarting"
            await self._offload(
                "sqlite.restart_handoff_restarting", self.store.update_restart_handoff,
                handoff_id, status="restarting", increment_attempts=True
            )
            from pa.cli.service import request_restart
            await self._offload(
                "service.restart_handoff", request_restart, self.settings,
                operation_id=handoff_id, timeout=30.0,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Deferred restart handoff %s failed", handoff_id)
            await self._offload(
                "sqlite.restart_handoff_failed", self.store.update_restart_handoff,
                handoff_id, status="failed", error=(str(exc) or type(exc).__name__)[:1000],
                failure_stage=stage,
            )

    async def _resume_restart_handoffs(self, *, replay_only: bool = False) -> None:
        async with self.label_lock("restart-handoff-replay"):
            await self._resume_pending_restart_handoffs(replay_only=replay_only)

    async def _resume_pending_restart_handoffs(self, *, replay_only: bool = False) -> None:
        pending = await self._offload(
            "sqlite.restart_handoffs_pending", self.store.list_restart_handoffs,
            statuses=(
                "requested", "waiting_for_turn_end", "quiescing",
                "restarting", "resuming", "continuation_queued",
                "failed",
            )
        )
        for handoff in pending:
            if handoff.status == "failed" and handoff.failure_stage == "resuming":
                completed = await self._offload(
                    "sqlite.restart_handoff_completion", self.store.find_prompt_completion,
                    handoff.session_id, handoff.continuation_prompt_id,
                )
                if completed is not None:
                    await self._offload(
                        "sqlite.restart_handoff_delivered", self.store.update_restart_handoff,
                        handoff.id, status="continuation_delivered", delivered=True,
                    )
                    continue
            # Old versions terminalized transient owner-readiness failures.
            # Only this known retryable stage may re-enter automatic replay.
            transient_failure = (
                handoff.failure_stage == "resuming"
                and "PA MCP owner channel api_not_ready (endpoint=" in (handoff.error or "")
            )
            if handoff.status == "failed" and not transient_failure:
                continue
            if handoff.status in {"requested", "waiting_for_turn_end", "quiescing"}:
                if not replay_only and handoff.id not in self._restart_handoff_tasks:
                    self._schedule_restart_handoff(handoff.id)
                continue
            session = await self._offload(
                "sqlite.restart_handoff_session", self.store.get_session, handoff.session_id
            )
            if not session or session.status == "closed" or session.archived_at:
                continue
            if session.control_mode == "human" and session.purpose == "automated_run":
                # Taking over an automated run remains a workflow pause. The
                # chat continuation exception must not reactivate that workflow.
                continue
            durable = dict((session.config_json or {}).get(_DURABLE_RUNTIME_KEY) or {})
            recovery = dict(session.recovery_json or {})
            runtime = self.get(handoff.session_id)
            if durable.get("queue_paused") and not (runtime and runtime.connected):
                continue
            if recovery.get("blocked"):
                continue
            retry_at = recovery.get("next_retry_at")
            if retry_at and not (runtime and runtime.connected):
                if datetime.fromisoformat(retry_at) > datetime.now(UTC):
                    continue
            try:
                completed = await self._offload(
                    "sqlite.restart_handoff_completion",
                    self.store.find_prompt_completion,
                    handoff.session_id,
                    handoff.continuation_prompt_id,
                )
                if completed is not None:
                    # The provider turn may have completed before the process
                    # could advance the receipt. Never replay completed work.
                    await self._offload(
                        "sqlite.restart_handoff_delivered",
                        self.store.update_restart_handoff,
                        handoff.id, status="continuation_delivered", delivered=True,
                    )
                    continue
                if not handoff.continuation_prompt.strip():
                    await self._offload(
                        "sqlite.restart_handoff_no_continuation",
                        self.store.update_restart_handoff,
                        handoff.id,
                        status="restart_completed",
                        delivered=True,
                    )
                    continue
                if handoff.status != "continuation_queued":
                    await self._offload(
                        "sqlite.restart_handoff_resuming", self.store.update_restart_handoff,
                        handoff.id, status="resuming"
                    )
                runtime = self.get(handoff.session_id)
                if runtime is None or runtime._closed or not runtime.connected:
                    runtime = await self.recover_session(
                        handoff.session_id, _startup_recovery=True, _defer_drain=True
                    )
                accepted = list(runtime._queue)
                if runtime._in_flight is not None:
                    accepted.append(runtime._in_flight)
                draining = getattr(runtime, "_draining_prompt", None)
                if draining is not None:
                    accepted.append(draining)
                already_accepted = any(
                    item.id == handoff.continuation_prompt_id for item in accepted
                )
                # A recovered queue must remain paused until this receipt is
                # reconciled. Otherwise it can finish during provider startup
                # and a resuming receipt would enqueue the same work again.
                completed = await self._offload(
                    "sqlite.restart_handoff_completion",
                    self.store.find_prompt_completion,
                    handoff.session_id, handoff.continuation_prompt_id,
                )
                if completed is not None:
                    await self._offload(
                        "sqlite.restart_handoff_delivered", self.store.update_restart_handoff,
                        handoff.id, status="continuation_delivered", delivered=True,
                    )
                    runtime._start_drain()
                    continue
                if handoff.status == "continuation_queued":
                    # Recovery restores the checkpointed queue. A queued receipt
                    # is not proof that its provider runtime is still alive.
                    if not already_accepted:
                        raise AgentSessionRecoveryError(
                            "Restart receipt has no matching queued, in-flight, or "
                            "completed prompt; inspect the preserved prompt lifecycle"
                        )
                    runtime._start_drain()
                    continue
                if not already_accepted:
                    runtime.enqueue(
                        handoff.continuation_prompt,
                        prompt_id=handoff.continuation_prompt_id,
                        source=f"restart-handoff:{handoff.id}",
                        card_id=handoff.card_id,
                        project_id=handoff.project_id,
                        _defer_drain=True,
                    )
                await self._offload(
                    "sqlite.restart_handoff_queued", self.store.update_restart_handoff,
                    handoff.id, status="continuation_queued"
                )
                runtime._start_drain()
            except SessionAdmissionInProgress:
                # Another exact-session recovery owns provider admission. Its
                # durable result will be reconciled by the next watchdog sweep.
                continue
            except Exception as exc:
                transient = "PA MCP owner channel api_not_ready (endpoint=" in str(exc)
                if transient:
                    await self._mark_recovery_interrupted(
                        self._snapshot_from_persisted(session), exc
                    )
                await self._offload(
                    "sqlite.restart_handoff_resume_failed",
                    self.store.update_restart_handoff, handoff.id,
                    status="failed", error=str(exc)[:1000],
                    failure_stage="resuming",
                )

    async def resume_restart_handoffs_after_startup(self) -> None:
        """Replay receipts only after normal prompt admission is declared ready."""
        self.require_startup_complete()
        self._start_recovery_coordinator()
        if self._resume_on_start:
            await self._resume_restart_handoffs()

    async def retry_restart_handoff(
        self, *, session_id: str, handoff_id: str
    ) -> RestartHandoff:
        """Retry exact-session continuation delivery from the same durable receipt."""
        session = await self._offload(
            "sqlite.agent_session_read", self.store.get_session, session_id
        )
        if not session or session.status == "closed":
            raise AgentSessionRecoveryError("Exact durable session is not recoverable")
        handoff = await self._offload(
            "sqlite.restart_handoff_retry",
            self.store.retry_restart_handoff,
            handoff_id,
            session_id=session_id,
        )
        if handoff.status == "requested":
            self._schedule_restart_handoff(handoff.id)
        elif handoff.status in {"resuming", "continuation_queued"}:
            await self._resume_restart_handoffs()
            handoff = await self._offload(
                "sqlite.restart_handoff_read",
                self.store.get_restart_handoff,
                handoff_id,
            )
        return handoff

    def _snapshot_from_persisted(self, session: AgentSession) -> SessionSnapshot:
        durable = dict((session.config_json or {}).get(_DURABLE_RUNTIME_KEY) or {})
        completed_prompt_ids: set[str] = set()
        try:
            events = self.store.list_transcript_events_before(session.id, limit=1000)
        except (AttributeError, OSError, RuntimeError, ValueError):
            events = []
        for event in events:
            if event.event_type not in {"turn_completed", "prompt_failed"}:
                continue
            prompt_id = (event.payload or {}).get("queued_prompt_id") or (
                event.payload or {}
            ).get("id")
            if prompt_id:
                completed_prompt_ids.add(str(prompt_id))
        queued = [
            QueuedPrompt.model_validate(item)
            for item in durable.get("queued_prompts") or []
            if str(item.get("id") or "") not in completed_prompt_ids
        ]
        in_flight_raw = durable.get("in_flight")
        if (
            in_flight_raw
            and str(in_flight_raw.get("id") or "") in completed_prompt_ids
        ):
            in_flight_raw = None
        return SessionSnapshot(
            session_id=session.id,
            external_session_id=session.external_session_id,
            agent_name=session.agent_name,
            status=session.status,
            cwd=session.cwd,
            title=session.title,
            label=session.label,
            model_id=session.model_id,
            mode_id=session.mode_id,
            configuration=dict(
                ((session.config_json or {}).get("configuration") or {})
            ),
            card_id=session.card_id or session.item_id,
            project_id=session.project_id,
            principal_id=session.principal_id,
            authority_instance_id=session.authority_instance_id,
            origin_instance_id=session.origin_instance_id,
            dispatch_id=session.dispatch_id,
            realm_id=session.realm_id,
            purpose=session.purpose,
            initiating_workflow=dict(session.initiating_workflow or {}),
            control_mode=session.control_mode,
            archived_at=session.archived_at,
            workflow_state=session.workflow_state,
            workflow_outcome=dict(session.workflow_outcome or {}),
            recovery_json=dict(session.recovery_json or {}),
            prompting=bool(in_flight_raw),
            queue_paused=bool(durable.get("queue_paused")),
            queued_prompts=queued,
            in_flight=(
                QueuedPrompt.model_validate(in_flight_raw) if in_flight_raw else None
            ),
        )

    async def _mark_recovery_interrupted(
        self, snapshot: SessionSnapshot, exc: BaseException
    ) -> str | None:
        if not snapshot.session_id:
            return None
        session = await self._offload(
            "sqlite.agent_session_read", self.store.get_session, snapshot.session_id
        )
        if not session or session.status == "closed":
            return None
        config = dict(session.config_json or {})
        # Classify the current failure, not a stale blocked marker. The project
        # may have arrived since the last boot and exposed a different failure.
        from pa.acp.errors import classify_acp_failure

        classified = classify_acp_failure(
            exc, provider_id=session.agent_name, stage="session_recovery"
        )
        previous_recovery = dict(session.recovery_json or {})
        attempts = int(previous_recovery.get("attempts") or 0) + 1
        code = str(classified.get("code") or "recovery_failed")
        binding_blocked = isinstance(exc, WorkspaceBindingMismatch)
        if binding_blocked:
            code = "workspace_binding_mismatch"
        actionable = (
            binding_blocked
            or not bool(classified.get("recoverable", True))
            or "auth" in code
            or "credential" in code
            or "config" in code
        )
        exhausted = attempts >= _RECOVERY_MAX_ATTEMPTS
        lowered_error = str(exc).casefold()
        context_lost = any(
            marker in lowered_error
            for marker in (
                "provider session is unavailable",
                "provider thread is unavailable",
                "existing provider conversation could not be restored",
                "session restore is unsupported",
                "session not found by provider",
            )
        )
        blocked = bool(
            (session.project_id and _project_recovery_block(exc))
            or actionable
            or exhausted
            or context_lost
        )
        recovery_state = (
            RECOVERY_BLOCKED_STATUS if blocked else "recoverable_interrupted"
        )
        durable = dict(config.get(_DURABLE_RUNTIME_KEY) or {})
        durable.update(
            lifecycle=recovery_state,
            recovery_error=str(exc)[:1000],
            updated_at=datetime.now(UTC).isoformat(),
        )
        if blocked:
            durable["recovery_action"] = self._recovery_action(session)
        config[_DURABLE_RUNTIME_KEY] = durable
        session.config_json = config
        retry_delay = min(
            _RECOVERY_MAX_SECONDS,
            _RECOVERY_BASE_SECONDS * (2 ** max(0, attempts - 1)),
        )
        if binding_blocked:
            remedy = WorkspaceBindingMismatch.remedy
        elif session.project_id and _project_recovery_block(exc):
            remedy = self._recovery_action(session)
        elif context_lost:
            remedy = (
                "The original provider context is unavailable. Continue in a new "
                "linked chat to preserve saved history with an explicit context boundary."
            )
        elif blocked:
            remedy = (
                classified.get("action")
                or classified.get("message")
                or "Correct the provider configuration, then retry."
            )
        else:
            remedy = None
        session.recovery_json = {
            "version": 1,
            "attempts": attempts,
            "last_attempt_at": datetime.now(UTC).isoformat(),
            "last_error": str(exc)[:1000],
            "code": code,
            "blocked": blocked,
            "exhausted": exhausted,
            "context_lost": context_lost,
            "next_retry_at": (
                None
                if blocked
                else (datetime.now(UTC) + timedelta(seconds=retry_delay)).isoformat()
            ),
            "remedy": remedy,
        }
        session.status = recovery_state
        session.updated_at = datetime.now(UTC)
        await self._offload(
            "sqlite.agent_session_save", self.store.save_session, session
        )
        return recovery_state

    async def retry_session(self, session_id: str) -> AgentSessionRuntime:
        """Explicitly retry a durable, nonterminal session regardless of auto policy."""
        self.require_startup_complete()
        async with self._lock:
            runtime = self.get(session_id)
            if runtime and not getattr(runtime, "_closed", False):
                return runtime
            session = await self._offload(
                "sqlite.agent_session_read", self.store.get_session, session_id
            )
            if not session:
                raise LookupError("Session not found")
            if session.status == "closed":
                raise RuntimeError("Closed sessions cannot be retried")
            if session.status not in RECOVERY_RETAINED_SESSION_STATUSES:
                raise RuntimeError(
                    f"Session status {session.status!r} is not eligible for recovery"
                )
            logger.info(
                "Explicit ACP recovery retry requested for session %s",
                session_id,
            )
            snapshot = self._snapshot_from_persisted(session)
            try:
                recovered = await self._resume_from_snapshot(
                    snapshot, QuiesceSnapshot(reason="explicit_retry")
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._last_error = str(exc)
                recovery_state = await self._mark_recovery_interrupted(snapshot, exc)
                if recovery_state == RECOVERY_BLOCKED_STATUS:
                    logger.warning(
                        "Explicit ACP recovery retry remains blocked for session "
                        "%s: %s",
                        session_id,
                        self._recovery_action(session),
                    )
                else:
                    logger.exception(
                        "Explicit ACP recovery retry failed for session %s",
                        session_id,
                    )
                raise
            if recovered is None:
                raise RuntimeError("Session recovery did not create a runtime")
            self._last_error = None
            return recovered

    @staticmethod
    def _recovery_queue(
        snap: SessionSnapshot, session: AgentSession, workspace_env: dict[str, str]
    ) -> list[QueuedPrompt]:
        """Restore accepted work with its stable ids and restart correlation."""
        queued = list(snap.queued_prompts)
        interrupted = snap.in_flight
        # Version-1 snapshots briefly encoded an interrupted turn as the first
        # queued item. Preserve recovery semantics when reading those files.
        if interrupted is None and queued and queued[0].source == "in_flight":
            interrupted = queued.pop(0)
        if interrupted:
            if interrupted.source != "recovery" and not interrupted.source.startswith("restart-handoff:"):
                from pa.prompts import PROMPTS

                recovery = PROMPTS.render(
                    "session.recovery.resume", provider=session.agent_name
                )
                interrupted = interrupted.model_copy(
                    update={
                        "message": f"{recovery.text}\n\n{interrupted.message}",
                        "source": "recovery",
                    }
                )
            queued.insert(0, interrupted)
        for item in queued:
            item.cwd = session.cwd
            merged_env = dict(item.agent_env or {})
            merged_env.update(workspace_env)
            item.agent_env = merged_env
        return queued

    @_fenced_session_admission
    async def _resume_from_snapshot(
        self, snap: SessionSnapshot, full: QuiesceSnapshot
    ) -> AgentSessionRuntime | None:
        if self._should_abort_recovery():
            raise RuntimeError("Agent is quiescing")
        existing = (
            await self._offload(
                "sqlite.agent_session_read", self.store.get_session, snap.session_id
            )
            if snap.session_id
            else None
        )
        if existing and existing.status == "closed":
            logger.info(
                "Skipping quiesce snapshot for durably closed session %s",
                existing.id,
            )
            return None
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
            config_json={"configuration": dict(snap.configuration)}
            if snap.configuration
            else {},
            card_id=snap.card_id,
            project_id=snap.project_id,
            principal_id=snap.principal_id,
            authority_instance_id=snap.authority_instance_id,
            origin_instance_id=snap.origin_instance_id,
            dispatch_id=snap.dispatch_id,
            realm_id=snap.realm_id,
            purpose=snap.purpose,
            initiating_workflow=dict(snap.initiating_workflow or {}),
            control_mode=snap.control_mode,
            archived_at=snap.archived_at,
            workflow_state=snap.workflow_state,
            workflow_outcome=dict(snap.workflow_outcome or {}),
            recovery_json=dict(snap.recovery_json or {}),
        )
        session.cwd = snap.cwd or session.cwd
        session.label = snap.label or session.label
        session.title = snap.title or session.title
        if snap.configuration and not (
            (session.config_json or {}).get("configuration")
        ):
            config = dict(session.config_json or {})
            config["configuration"] = dict(snap.configuration)
            session.config_json = config
        provider_spec = None
        if self._default_requires_provider_resolution(
            session.label, snap.external_session_id
        ):
            resolved = await self._offload(
                "agent.provider_resolve",
                resolve_agent_provider,
                self.settings,
                AgentInvocationContext(
                    surface=SURFACE_CHAT_DEFAULT,
                    principal_id=session.principal_id,
                ),
                timeout=30.0,
            )
            session.agent_name = resolved.provider_id
            provider_spec = resolved.spec
            await self._offload(
                "sqlite.agent_session_save", self.store.save_session, session
            )
        workspace_env = await self._prepare_workspace(
            session,
            requested_cwd=snap.cwd,
            provider_id=session.agent_name,
            mode_id=session.mode_id,
        )
        if self._should_abort_recovery():
            raise RuntimeError("Agent is quiescing")
        if provider_spec is not None:
            provider_spec.env.update(workspace_env)
        runtime = await self._new_runtime(session, agent_env=workspace_env)
        if self._should_abort_recovery():
            await runtime.close()
            raise RuntimeError("Agent is quiescing")
        queued = self._recovery_queue(snap, session, workspace_env)
        await runtime.start(
            resume_external_id=snap.external_session_id,
            require_restore=bool(snap.external_session_id),
            queued_prompts=queued,
            queue_paused=snap.queue_paused,
            provider_spec=provider_spec,
        )
        if self._should_abort_recovery():
            await runtime.close()
            raise RuntimeError("Agent is quiescing")
        await self._publish_runtime(runtime)
        self._invalidate_provider_overview()
        return runtime

    async def reconnect(self) -> bool:
        """Reconnect the default session (compat with chrome reconnect button)."""
        self.require_startup_complete()
        async with self._reconnect_lock:
            task = self._reconnect_task
            if task is None or task.done():
                task = asyncio.create_task(
                    self._reconnect_default(),
                    name="agent-default-reconnect",
                )
                self._reconnect_task = task
        try:
            # A disconnected HTTP client must not cancel the coalesced reconnect
            # still awaited by other callers.
            return await asyncio.shield(task)
        finally:
            if task.done():
                async with self._reconnect_lock:
                    if self._reconnect_task is task:
                        self._reconnect_task = None

    async def _reconnect_default(self) -> bool:
        """Perform one reconnect attempt shared by all concurrent callers."""
        try:
            runtime = await self.attach_default()
            if runtime.connected:
                self._last_error = None
                return True
            self._last_error = (
                f"Retained default session {runtime.session_id} is disconnected; "
                "retry recovery or explicitly close it before starting a replacement."
            )
            return False
        except Exception as exc:
            self._last_error = str(exc)
            logger.exception("Agent reconnect failed")
            return False

    @_fenced_session_admission
    async def create_session(
        self,
        *,
        session_id: str | None = None,
        label: str | None = None,
        title: str | None = None,
        cwd: str | None = None,
        principal_id: str | None = None,
        card_id: str | None = None,
        project_id: str | None = None,
        authority_instance_id: str | None = None,
        dispatch_id: str | None = None,
        realm_id: str | None = None,
        agent_env: dict[str, str] | None = None,
        mcp_private_env: dict[str, str] | None = None,
        resume_external_id: str | None = None,
        existing: AgentSession | None = None,
        surface: str | None = None,
        provider_override: str | None = None,
        project_tool_config: dict | None = None,
        initial_configuration: SessionConfigurationRequest | None = None,
        execution_context_seed: dict[str, Any] | None = None,
        execution_preferences: Any | None = None,
        execution_selection: dict[str, Any] | None = None,
        task_assessment: Any | None = None,
        purpose: Literal["chat", "automated_run", "one_shot_job", "unknown"]
        | None = None,
        initiating_workflow: dict[str, Any] | None = None,
        control_mode: Literal["automation", "human"] | None = None,
        startup_trace: SessionStartupTrace | None = None,
        _startup_recovery: bool = False,
        require_restore: bool = False,
        context_source_session_id: str | None = None,
        _linked_boundary: dict | None = None,
        _defer_drain: bool = False,
    ) -> AgentSessionRuntime:
        if context_source_session_id:
            from pa.execution.selection_boundary import create_linked_session

            options = dict(locals())
            for key in (
                "self",
                "context_source_session_id",
                "_linked_boundary",
                "create_linked_session",
            ):
                options.pop(key, None)
            return await create_linked_session(self, context_source_session_id, options)
        if existing and (existing.config_json or {}).get("execution_context_boundary"):
            from pa.execution.selection import SelectionError

            raise SelectionError("context_boundary_fenced", "This source has a durable linked attempt; resume that target, not the superseded native context")
        # Capture accepted work before startup checkpoints an empty runtime.
        recovery_snapshot = (
            await self._offload(
                "agent.recovery_snapshot", self._snapshot_from_persisted, existing
            )
            if existing is not None else None
        )
        if not self.settings.agent_enabled:
            raise RuntimeError("Agent disabled")
        if not _startup_recovery and not self._startup_complete:
            self.require_startup_complete()
        if self._should_abort_admission():
            raise RuntimeError("Agent is quiescing")

        agent_env = dict(agent_env or {})
        mcp_private_env = dict(mcp_private_env or {})

        effective_principal_id = (
            principal_id
            if principal_id is not None
            else existing.principal_id
            if existing
            else None
        )
        surface_key = surface or surface_for_label(label, project_id=project_id)
        # This is the final common admission gate for PA-owned ACP sessions.
        # A durable attempt is reused before looking at mutable defaults/catalogs.
        from pa.execution.selection import (
            ExecutionPreferences,
            SelectionConstraints,
            SelectionError,
            legacy_preferences,
            validate_reuse,
        )
        from pa.execution.selection_service import (
            SelectionService,
            selected_configuration,
        )

        selection_service = getattr(self, "_selection_service", None)
        if selection_service is None:
            selection_service = self._selection_service = SelectionService(
                self.settings, self.store, self
            )
        prefs = ExecutionPreferences.model_validate(execution_preferences or {})
        receipt = (
            (existing.config_json or {}).get("execution_selection")
            if existing
            else execution_selection
        )
        explicit = initial_configuration or SessionConfigurationRequest()
        source_card_id = card_id or (initiating_workflow or {}).get("card_id")
        selection_card = None
        if source_card_id and not existing:
            selection_card = await self._offload(
                "selection.card_read",
                self.store.get_card,
                source_card_id,
                realm_id=realm_id or self.settings.primary_realm,
            )
            if not selection_card:
                raise SelectionError(
                    "selection_card_unavailable",
                    "Sync the originating card and its policy before starting this execution.",
                )
        if receipt:
            from pa.execution.selection import validate_attempt_request

            lineage = (
                existing.config_json if existing else {"execution_selection": receipt}
            )
            validate_attempt_request(lineage, prefs)
            validate_attempt_request(
                lineage,
                legacy_preferences(
                    provider=provider_override,
                    model_id=explicit.model_id,
                    model_provider=explicit.model_provider,
                    effort=explicit.reasoning,
                    config=explicit.config,
                ),
            )
            if receipt["selected"]["instance_id"] != self.settings.instance_id:
                raise SelectionError(
                    "selection_instance_mismatch",
                    "The selected tuple belongs to another instance; do not reroute an existing attempt.",
                )
            provider_override = receipt["selected"]["harness"]
            initial_configuration = selected_configuration(
                receipt,
                explicit,
                native_binding=(existing.config_json or {}).get(
                    "execution_native_binding"
                )
                if existing
                else None,
            )
            if existing:
                await self._offload(
                    "selection.attempt_authority",
                    selection_service.revalidate_attempt,
                    receipt,
                    realm=existing.realm_id,
                    principal=effective_principal_id,
                    surface=surface_key,
                )
            if not existing:
                from pa.execution.selection import Preference, SelectionConstraints

                owner = receipt.get("context") or {}
                if (
                    owner.get("realm") != (realm_id or self.settings.primary_realm)
                    or owner.get("principal")
                    != (effective_principal_id or "user:local")
                    or owner.get("card_id") != source_card_id
                ):
                    raise SelectionError(
                        "selection_receipt_scope_mismatch",
                        "The authority receipt belongs to another realm, principal, or card; obtain a matching admission receipt.",
                    )
                selected = receipt["selected"]
                fixed = ExecutionPreferences(
                    **{
                        key: Preference(intent="required", value=selected[key])
                        for key in (
                            "harness",
                            "connection",
                            "model_provider",
                            "model",
                            "reasoning",
                        )
                        if selected.get(key) is not None
                    },
                    options={
                        k: Preference(intent="required", value=v)
                        for k, v in selected.get("options", {}).items()
                    },
                )
                await self._offload(
                    "selection.target_revalidation",
                    selection_service.resolve,
                    candidates=await selection_service.local_catalog(refresh=True),
                    principal=effective_principal_id,
                    realm=realm_id or self.settings.primary_realm,
                    surface=surface_key,
                    overrides=fixed,
                    card=selection_card,
                    project_config=project_tool_config,
                    constraints=[
                        SelectionConstraints.model_validate(c)
                        for c in [
                            *receipt.get("constraints", []),
                            *(
                                (
                                    (_linked_boundary["source"].config_json or {}).get(
                                        "execution_selection"
                                    )
                                    or {}
                                ).get("constraints", [])
                                if _linked_boundary
                                else []
                            ),
                        ]
                    ],
                )
                await self._offload(
                    "selection.target_receipt",
                    selection_service.store.save_decision,
                    receipt,
                    owner.get("realm", realm_id or self.settings.primary_realm),
                    owner.get("principal", effective_principal_id or "user:local"),
                )
        elif existing:
            # Legacy resumptions have no policy receipt. Preserve native identity
            # and settings, documenting that migration did not resolve a new tuple.
            if provider_override and provider_override != existing.agent_name:
                raise SelectionError(
                    "context_boundary_required",
                    "Changing harness requires a linked new attempt, not an in-place resume.",
                )
        else:
            if (
                project_tool_config is None
                and selection_card
                and selection_card.project_id
            ):
                selection_project = await self._offload(
                    "selection.project_read",
                    self.store.get_project,
                    selection_card.project_id,
                )
                project_tool_config = (
                    selection_project.tool_config if selection_project else None
                )
            candidates = await selection_service.local_catalog(refresh=True)
            receipt = await self._offload(
                "selection.resolve",
                selection_service.resolve,
                candidates=candidates,
                principal=effective_principal_id,
                realm=realm_id or self.settings.primary_realm,
                surface=surface_key,
                card=selection_card,
                project_config=project_tool_config,
                overrides=prefs,
                assessment=task_assessment,
                constraints=[
                    SelectionConstraints.model_validate(c)
                    for c in (
                        (_linked_boundary["source"].config_json or {}).get(
                            "execution_selection"
                        )
                        or {}
                    ).get("constraints", [])
                ]
                if _linked_boundary
                else (),
                legacy=legacy_preferences(
                    provider=provider_override,
                    model_id=explicit.model_id,
                    model_provider=explicit.model_provider,
                    effort=explicit.reasoning,
                    config=explicit.config,
                ),
                persist=True,
            )
            provider_override = receipt["selected"]["harness"]
            initial_configuration = selected_configuration(receipt, explicit)
        if _linked_boundary:
            from pa.execution.selection_boundary import fence_source

            await fence_source(self, _linked_boundary, receipt)
        ctx = AgentInvocationContext(
            surface=surface_key,
            principal_id=effective_principal_id,
            card_id=card_id,
            project_id=project_id,
            provider_override=provider_override,
        )
        non_resumable_default = bool(
            existing
            and self._default_requires_provider_resolution(
                label or existing.label,
                resume_external_id or existing.external_session_id,
            )
        )

        def resolve_provider_spec():
            # When resuming an existing session, keep its provider unless
            # explicitly overridden. Provider discovery reads configuration and
            # executable metadata, so the complete resolution stays off-loop.
            if (
                existing
                and existing.agent_name
                and existing.agent_name not in {"instance", ""}
                and not provider_override
                and not non_resumable_default
            ):
                provider_id = existing.agent_name
                from pa.acp.providers.registry import get_provider
                from pa.acp.providers.resolve import _spawn_overrides

                cmd_o, args_o = _spawn_overrides(self.settings, provider_id)
                spec = get_provider(provider_id).resolve_spawn(
                    command_override=cmd_o,
                    args_override=args_o,
                    extra_env=agent_env,
                    data_dir=self.settings.data_dir,
                )
                return provider_id, spec, "session"
            resolved = resolve_agent_provider(
                self.settings,
                ctx,
                project_tool_config=project_tool_config,
                extra_env=agent_env,
            )
            return resolved.provider_id, resolved.spec, resolved.source

        provider_phase = (
            startup_trace.phase("provider_resolution")
            if startup_trace
            else nullcontext()
        )
        with provider_phase:
            provider_id, resolved_spec, source = await self._offload(
                "agent.provider_resolve", resolve_provider_spec, timeout=30.0
            )

        requested_mode = (
            initial_configuration.mode_id
            if initial_configuration is not None
            else existing.mode_id
            if existing
            else None
        )
        requested_model_provider = (
            initial_configuration.model_provider
            if initial_configuration is not None
            else None
        )
        requested_model = (
            initial_configuration.model_id
            if initial_configuration is not None
            else None
        )
        named_connection = bool(
            receipt and receipt["selected"].get("connection_revision")
        )
        if named_connection:
            from pa.execution.selection_connections import apply_selected_connection

            resolved_spec = await self._offload(
                "selection.connection_overlay",
                apply_selected_connection,
                resolved_spec,
                receipt["selected"],
                self.settings.data_dir,
            )
        if provider_id == "codex" and requested_mode:
            # codex-acp chooses its sandbox before ACP initialize/session-new.
            # Applying the mode later is too late and can silently start a
            # workspace-write provider for an agent-full-access dispatch.
            resolved_spec.env["INITIAL_AGENT_MODE"] = requested_mode
        if provider_id == "openinterpreter" and not named_connection:
            from pa.acp.errors import ProviderStartError
            from pa.acp.providers.openinterpreter import (
                _spawn_args,
                preflight_session_start,
            )

            failure = await self._offload(
                "agent.openinterpreter_preflight",
                preflight_session_start,
                self.settings.data_dir,
                model_provider=requested_model_provider,
                model_id=requested_model,
                timeout=30.0,
            )
            if failure:
                raise ProviderStartError(failure)
            # Session overrides / explicit defaults must reach the process before
            # initialize; host config.toml alone cannot express per-session picks.
            if requested_model_provider or requested_model:
                resolved_spec.args = _spawn_args(
                    model_provider=requested_model_provider,
                    model=requested_model,
                )

        inferred_purpose = purpose or (
            "automated_run"
            if dispatch_id or surface_key == SURFACE_EXECUTION
            else "chat"
        )
        inferred_control = control_mode or (
            "automation" if inferred_purpose != "chat" else "human"
        )
        session = existing or AgentSession(
            id=session_id or str(uuid4()),
            agent_name=provider_id,
            origin_instance_id=self.settings.instance_id,
            origin_instance_name=self.settings.instance_name,
            status="provisioning",
            cwd=None,
            title=title,
            label=label,
            principal_id=principal_id,
            card_id=card_id,
            project_id=project_id,
            authority_instance_id=authority_instance_id or self.settings.instance_id,
            dispatch_id=dispatch_id,
            lifecycle_owner="dispatch" if dispatch_id else "standalone",
            purpose=inferred_purpose,
            initiating_workflow=dict(initiating_workflow or {}),
            control_mode=inferred_control,
            workflow_state=(
                "active"
                if inferred_purpose in {"automated_run", "one_shot_job"}
                else "not_applicable"
            ),
            human_activity_at=(
                datetime.now(UTC)
                if inferred_purpose == "chat"
                else None
            ),
            realm_id=realm_id or self.settings.primary_realm,
            item_id=card_id,
        )
        if existing:
            session.origin_instance_id = (
                session.origin_instance_id or self.settings.instance_id
            )
            session.authority_instance_id = (
                session.authority_instance_id
                or authority_instance_id
                or self.settings.instance_id
            )
            session.origin_instance_name = (
                session.origin_instance_name or self.settings.instance_name
            )
            if label is not None:
                session.label = label
            if title is not None:
                session.title = title
            if principal_id is not None:
                session.principal_id = principal_id
            if card_id is not None:
                session.card_id = card_id
                session.item_id = card_id
            if project_id is not None:
                session.project_id = project_id
            if authority_instance_id is not None:
                session.authority_instance_id = authority_instance_id
            if dispatch_id is not None:
                session.dispatch_id = dispatch_id
            if purpose is not None:
                session.purpose = purpose
            if initiating_workflow is not None:
                session.initiating_workflow = dict(initiating_workflow)
            if control_mode is not None:
                session.control_mode = control_mode
            if realm_id is not None:
                session.realm_id = realm_id
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
        if startup_trace:
            startup_trace.attach(session)
        if requested_mode:
            session.mode_id = requested_mode
        if receipt:
            session.config_json = {
                **(session.config_json or {}),
                "execution_selection": receipt,
            }
        if _linked_boundary:
            session.config_json["execution_context_boundary_from"] = {
                k: v for k, v in _linked_boundary["receipt"].items() if k != "selection"
            }
        if execution_context_seed:
            config = dict(session.config_json or {})
            execution = dict(config.get("execution_context") or {})
            execution.update(execution_context_seed)
            config["execution_context"] = execution
            session.config_json = config
        workspace_phase = (
            startup_trace.phase("workspace_preparation")
            if startup_trace
            else nullcontext()
        )
        with workspace_phase:
            workspace_env = await self._prepare_workspace(
                session,
                requested_cwd=cwd or (existing.cwd if existing else None),
                provider_id=provider_id,
                mode_id=requested_mode,
            )
        effective_agent_env = dict(agent_env or {})
        effective_agent_env.update(workspace_env)
        resolved_spec.env.update(workspace_env)

        runtime = await self._new_runtime(
            session,
            agent_env=effective_agent_env,
            mcp_private_env=mcp_private_env,
            startup_trace=startup_trace,
        )
        prior_status = session.status
        try:
            start_kwargs: dict[str, Any] = {
                "resume_external_id": resume_external_id,
                "provider_spec": resolved_spec,
            }
            if require_restore:
                start_kwargs["require_restore"] = True
            if _defer_drain:
                start_kwargs["_defer_drain"] = True
            if recovery_snapshot is not None:
                start_kwargs["queued_prompts"] = self._recovery_queue(
                    recovery_snapshot, session, workspace_env
                )
                start_kwargs["queue_paused"] = recovery_snapshot.queue_paused
            if initial_configuration is not None:
                start_kwargs["initial_configuration"] = initial_configuration
            await runtime.start(**start_kwargs)
            self._last_error = None
        except Exception as exc:
            from pa.acp.errors import classify_acp_failure, format_acp_error

            self._last_error = format_acp_error(exc)
            configuration = dict(
                ((session.config_json or {}).get("configuration") or {})
            )
            classified = classify_acp_failure(
                exc,
                provider_id=provider_id,
                stage=(
                    "session_configuration"
                    if configuration.get("state") == "failed"
                    else "provider_startup"
                ),
            )
            config = dict(session.config_json or {})
            diagnostics = dict(config.get("diagnostics") or {})
            diagnostics["last_startup_failure"] = classified
            config["diagnostics"] = diagnostics
            if require_restore:
                durable = dict(config.get("durable_runtime") or {})
                durable["recovery_error"] = classified.get("message") or str(exc)
                durable["recovery_attempted_at"] = datetime.now(UTC).isoformat()
                config["durable_runtime"] = durable
            session.config_json = config
            if require_restore:
                session.status = prior_status
            else:
                session.status = (
                    "configuration_failed"
                    if configuration.get("state") == "failed"
                    else "disconnected"
                )
            await self._offload(
                "sqlite.agent_session_save", self.store.save_session, session
            )
            try:
                await self._offload(
                    "workspace.session_fence",
                    self.workspace_manager.fence_session,
                    session.id,
                    stage="session_configuration"
                    if configuration.get("state") == "failed"
                    else "provider_startup",
                    error=classified.get("message") or str(exc),
                    timeout=30.0,
                )
            except Exception:
                logger.exception(
                    "Could not fence workspace after session startup failure for %s",
                    session.id,
                )
            raise RuntimeError(classified.get("message") or str(exc)) from exc
        publication_phase = (
            startup_trace.phase("persistence_publication")
            if startup_trace
            else nullcontext()
        )
        with publication_phase:
            await self._offload(
                "sqlite.agent_session_save", self.store.save_session, runtime.session
            )
            await self._publish_runtime(runtime)
        self._invalidate_provider_overview()
        return runtime

    async def attach_default(
        self,
        *,
        principal_id: str | None = None,
        cwd: str | None = None,
        agent_env: dict[str, str] | None = None,
        provider_override: str | None = None,
        initial_configuration: SessionConfigurationRequest | None = None,
        execution_preferences: Any | None = None,
        task_assessment: Any | None = None,
        startup_trace: SessionStartupTrace | None = None,
        _startup_recovery: bool = False,
    ) -> AgentSessionRuntime:
        async with self._lock:
            for rt in self._runtimes.values():
                if (
                    rt.session.label == self._default_label
                    and rt.connected
                    and not rt._closed
                ):
                    return rt
            existing = await self._offload(
                "sqlite.agent_session_read",
                self.store.get_session_by_label,
                self._default_label,
            )
            if existing and existing.id in self._runtimes:
                rt = self._runtimes[existing.id]
                if rt.connected and not rt._closed:
                    return rt
            if (
                existing
                and existing.status != "closed"
                and existing.origin_instance_id
                and existing.origin_instance_id != self.settings.instance_id
            ):
                raise AgentSessionRecoveryError(
                    "The retained default session belongs to another instance; "
                    "recover it on its owning instance or explicitly close it before "
                    "starting a replacement."
            )
            try:
                runtime = await self.create_session(
                    label=self._default_label,
                    title=existing.title if existing else "Instance agent",
                    cwd=existing.cwd if existing and existing.cwd else cwd,
                    principal_id=(
                        existing.principal_id
                        if existing and existing.principal_id
                        else principal_id
                    ),
                    agent_env=agent_env,
                    existing=(
                        existing if existing and existing.status != "closed" else None
                    ),
                    resume_external_id=(
                        existing.external_session_id
                        if existing and existing.status != "closed"
                        else None
                    ),
                    surface=SURFACE_CHAT_DEFAULT,
                    provider_override=provider_override,
                    initial_configuration=initial_configuration,
                    execution_preferences=execution_preferences,
                    task_assessment=task_assessment,
                    startup_trace=startup_trace,
                    _startup_recovery=_startup_recovery,
                )
            except Exception as exc:
                if existing and existing.status != "closed":
                    await self._mark_recovery_interrupted(
                        self._snapshot_from_persisted(existing), exc
                    )
                    raise AgentSessionRecoveryError(
                        f"Retained default session {existing.id} could not be "
                        f"recovered: {exc}. Retry recovery or explicitly close it "
                        "before starting a replacement."
                    ) from exc
                raise
            config = dict(runtime.session.config_json or {})
            config["browser_default_selected"] = True
            runtime.session.config_json = config
            await self._offload(
                "sqlite.agent_session_save", self.store.save_session, runtime.session
            )
            return runtime

    @asynccontextmanager
    async def continuation_transfer_guard(self, old_session_id: str, successor_session_id: str):
        """Exclude provider admission while a notification route is compared/saved.

        This is not a provider permission transfer or a session recovery. The
        original must have no runtime; an already live successor is permitted.
        Admission checks and reservation happen without yielding on the manager
        event loop, as in _fenced_session_admission.
        """
        session_ids = {old_session_id, successor_session_id}
        if session_ids & self._admitting_sessions:
            raise SessionAdmissionInProgress("Session admission is in progress")
        with self._runtime_lifecycle_lock:
            runtime = self.get(old_session_id)
            if runtime is not None and not runtime._closed:
                raise SessionAdmissionInProgress("Original session has a live runtime")
            self._admitting_sessions.update(session_ids)
        try:
            yield
        finally:
            self._admitting_sessions.difference_update(session_ids)

    async def recover_session(
        self,
        session_id: str,
        *,
        provider_override: str | None = None,
        _startup_recovery: bool = False,
        _defer_drain: bool = False,
    ) -> AgentSessionRuntime:
        """Reconnect one durable PA session without creating a second PA identity."""
        if not _startup_recovery:
            self.require_startup_complete()
        async with self.label_lock(f"recover:{session_id}"):
            self._require_not_terminal_repair_fenced(session_id)
            runtime = self.get(session_id)
            if runtime and not runtime._closed and runtime.connected:
                return runtime
            if runtime and not runtime._closed:
                connection = runtime.connection
                runtime.connection = None
                runtime._closed = True
                if connection:
                    await connection.disconnect()
                with self._runtime_lifecycle_lock:
                    if self._runtimes.get(session_id) is runtime:
                        self._runtimes.pop(session_id, None)
            session = await self._offload(
                "sqlite.agent_session_read", self.store.get_session, session_id
            )
            if session is None:
                raise AgentSessionRecoveryError("PA session was deleted")
            if session.status == "closed" and not session.external_session_id:
                raise AgentSessionRecoveryError(
                    "PA session is closed and has no resumable provider identity"
                )
            if session.status == RECOVERY_BLOCKED_STATUS:
                raise AgentSessionRecoveryError("PA session recovery is blocked")
            if (
                session.origin_instance_id
                and session.origin_instance_id != self.settings.instance_id
            ):
                raise AgentSessionRecoveryError(
                    "PA session belongs to another instance and is unavailable locally"
                )
            if not provider_override and session.agent_name not in known_provider_ids():

                def resolve_rollout_provider() -> str:
                    known = set(known_provider_ids())
                    resolved, _ = resolve_provider_id(
                        self.settings,
                        AgentInvocationContext(
                            surface=surface_for_label(
                                session.label, project_id=session.project_id
                            ),
                            principal_id=session.principal_id,
                            card_id=session.card_id or session.item_id,
                            project_id=session.project_id,
                        ),
                    )
                    if resolved in known:
                        return resolved
                    configured = (self.settings.agent_provider or "").strip().lower()
                    if configured in known:
                        return configured
                    return DEFAULT_PROVIDER_ID

                provider_override = await self._offload(
                    "agent.recovery_provider_resolve",
                    resolve_rollout_provider,
                    timeout=30.0,
                )
            return await self.create_session(
                label=session.label,
                title=session.title,
                cwd=session.cwd,
                principal_id=session.principal_id,
                card_id=session.card_id or session.item_id,
                project_id=session.project_id,
                existing=session,
                resume_external_id=session.external_session_id,
                provider_override=provider_override,
                require_restore=bool(session.external_session_id),
                _startup_recovery=_startup_recovery,
                _defer_drain=_defer_drain,
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
            self._require_not_terminal_repair_fenced(session_id)
            runtime = self.get(session_id)
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
        source: str | None = None,
    ) -> str:
        self.require_startup_complete()
        if session_id:
            self._require_not_terminal_repair_fenced(session_id)
            runtime = self.get(session_id)
            if not runtime:
                runtime = await self.recover_session(session_id)
        else:
            if surface == SURFACE_EXECUTION:
                scope_key = (
                    f"execution:card:{item_id}"
                    if item_id
                    else f"execution:project:{project_id or 'standalone'}"
                )

                def matches_execution_scope(candidate: AgentSession) -> bool:
                    if candidate.label != "execution" or candidate.status == "closed":
                        return False
                    if item_id:
                        return candidate.card_id == item_id
                    if project_id:
                        return (
                            candidate.card_id is None
                            and candidate.project_id == project_id
                        )
                    return candidate.card_id is None and candidate.project_id is None

                def verify_project_fence(candidate: AgentSession) -> None:
                    if (
                        project_id
                        and candidate.project_id
                        and project_id != candidate.project_id
                    ):
                        raise RuntimeError(
                            "Execution session is fenced to a different project"
                        )

                async with self.label_lock(scope_key):
                    runtime = next(
                        (
                            candidate
                            for candidate in self._runtimes.values()
                            if matches_execution_scope(candidate.session)
                            and candidate.connected
                            and not candidate._closed
                        ),
                        None,
                    )
                    if runtime is None:
                        persisted_sessions = await self._offload(
                            "sqlite.agent_sessions_list", self.store.list_sessions
                        )
                        existing = next(
                            (
                                candidate
                                for candidate in persisted_sessions
                                if matches_execution_scope(candidate)
                            ),
                            None,
                        )
                        if existing:
                            verify_project_fence(existing)
                        runtime = await self.create_session(
                            label="execution",
                            title="Execution",
                            cwd=cwd,
                            principal_id=principal_id,
                            project_id=project_id,
                            card_id=item_id,
                            agent_env=agent_env,
                            existing=existing,
                            resume_external_id=(
                                existing.external_session_id if existing else None
                            ),
                            surface=SURFACE_EXECUTION,
                            provider_override=provider_override,
                        )
                    else:
                        verify_project_fence(runtime.session)
            else:
                runtime = await self.attach_default(
                    principal_id=principal_id,
                    cwd=cwd,
                    agent_env=agent_env,
                    provider_override=provider_override,
                )
        effective_source = source or (
            "dispatch" if surface == SURFACE_EXECUTION else "api"
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
            source=effective_source,
            _from_queue=_from_queue,
            wait=wait,
        )

    async def stop(self, *, fast: bool = False) -> None:
        self._accepting = False
        self._quiescing = True
        if self._recovery_coordinator_task:
            self._recovery_coordinator_task.cancel()
        for task in list(self._recovery_tasks.values()):
            task.cancel()

        async def stop_runtime(runtime: AgentSessionRuntime) -> None:
            try:
                runtime._flush_transcript()
                await runtime._drain_transcripts(timeout=0.25 if fast else 5.0)
                if runtime.connection:
                    await runtime.connection.disconnect(
                        timeout=0.5 if fast else 5.0,
                        force=fast,
                    )
            except Exception:
                logger.exception("Error disconnecting session %s", runtime.session_id)

        await asyncio.gather(
            *(stop_runtime(runtime) for runtime in list(self._runtimes.values()))
        )
        self._runtimes.clear()
        try:
            await asyncio.wait_for(self.browser.close(), timeout=1.0 if fast else 5.0)
        except TimeoutError:
            logger.error("Timed out stopping browser runtime")

    async def quiesce(
        self,
        *,
        reason: str = "restart",
        timeout: float = 300.0,
        on_progress: Callable[[QuiesceProgress], Awaitable[None] | None] | None = None,
    ) -> QuiesceSnapshot:
        from pa.server.shutdown import is_shutting_down

        # Admission is transactional until a quiesce snapshot is committed (or a
        # real process shutdown fence is active). A timed-out handoff must not
        # leave Start Session returning agent_draining forever.
        prior_accepting = self._accepting
        prior_quiescing = self._quiescing
        committed = False
        self._quiescing = True
        self._accepting = False

        async def _emit(
            phase: str, *, done: bool = False, error: str | None = None
        ) -> None:
            progress = self.progress()
            progress.phase = phase
            progress.done = done
            progress.error = error
            progress.message = (
                self._status_message()
                if not done
                else ("ACP sessions quiesced" if not error else error)
            )
            if on_progress:
                result = on_progress(progress)
                if asyncio.iscoroutine(result):
                    await result

        def _restore_admission_if_needed() -> None:
            nonlocal committed
            if committed or is_shutting_down():
                return
            self._accepting = prior_accepting
            self._quiescing = prior_quiescing

        try:
            await _emit("quiescing")
            deadline = asyncio.get_running_loop().time() + timeout
            while any(rt.prompting for rt in self._runtimes.values()):
                if asyncio.get_running_loop().time() >= deadline:
                    await _emit(
                        "timeout", done=True, error="Timed out waiting for ACP turn"
                    )
                    blockers = []
                    for rt in self._runtimes.values():
                        if not rt.prompting:
                            continue
                        last_tool = next((event for event in reversed(
                            getattr(rt, "_recent_live_events", ())
                        ) if event.get("type") in {"tool_call", "tool_call_update"}), {})
                        tool = last_tool.get("payload") or {}
                        blockers.append(
                            f"session={rt.session_id} queued={len(rt._queue)} "
                            f"prompt={getattr(getattr(rt, '_in_flight', None), 'id', None)} "
                            f"tool={sanitize_text(str(tool.get('title') or tool.get('tool_call_id') or 'unknown'), limit=120)} "
                            f"tool_status={tool.get('status', 'unknown')} "
                            f"transcript_batches={rt._transcript_queue.qsize() if hasattr(rt, '_transcript_queue') else 'unknown'}"
                        )
                    if self.async_runtime:
                        blockers.extend(
                            f"operation={item['operation']} phase={item['phase']} lock_wait_ms={item['lock_wait_ms']}"
                            for item in self.async_runtime.snapshot().get("active_operations", [])
                        )
                    raise TimeoutError("Quiesce deadline: " + "; ".join(blockers))
                await _emit("waiting")
                await asyncio.sleep(_QUIESCE_POLL_SECONDS)

            await _emit("capturing")
            sessions: list[SessionSnapshot] = []
            disconnects = []
            for runtime in list(self._runtimes.values()):
                snap = runtime.to_session_snapshot()
                sessions.append(snap)
                runtime.session.status = "quiesced"
                runtime.session.updated_at = datetime.now(UTC)
                await self._offload(
                    "sqlite.agent_session_save",
                    self.store.save_session,
                    runtime.session,
                )
                runtime._flush_transcript()
                await runtime._drain_transcripts(raise_on_timeout=True)
                if runtime.connection:
                    remaining = max(0.1, deadline - asyncio.get_running_loop().time())
                    disconnects.append(
                        runtime.connection.disconnect(timeout=min(5.0, remaining))
                    )
                    runtime.connection = None
            await asyncio.gather(*disconnects)

            snapshot = QuiesceSnapshot(
                reason=reason,
                resume=True,
                sessions=sessions,
                queued_prompts=[],
            )
            await self._offload(
                "agent.quiesce_snapshot_write",
                save_quiesce_snapshot,
                self.settings.data_dir,
                snapshot,
                wait_for_completion=True,
            )
            self._runtimes.clear()
            committed = True

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
        except BaseException:
            _restore_admission_if_needed()
            raise


# Back-compat alias
InstanceAgent = AgentSessionManager

_instance_agent: AgentSessionManager | None = None


def get_instance_agent(
    settings: Settings,
    store: Store,
    dispatch_store: Any | None = None,
) -> AgentSessionManager:
    global _instance_agent
    if _instance_agent is None:
        _instance_agent = AgentSessionManager(
            settings, store, dispatch_store=dispatch_store
        )
    elif dispatch_store is not None:
        _instance_agent.bind_dispatch_store(dispatch_store)
    return _instance_agent


def reset_instance_agent() -> None:
    global _instance_agent
    _instance_agent = None
