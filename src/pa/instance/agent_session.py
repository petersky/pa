"""Multi-session ACP agent runtime for a PA instance."""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from uuid import uuid4

from pa.acp.client import (
    AgentConnection,
    normalize_session_update,
    permission_cancelled,
    permission_selected,
)
from pa.acp.configuration import SessionConfigurationRequest
from pa.acp.final_message import (
    assemble_final_assistant_message,
    is_agent_message_type,
)
from pa.acp.providers.registry import DEFAULT_PROVIDER_ID, known_provider_ids
from pa.acp.providers.resolve import resolve_agent_provider, resolve_provider_id
from pa.acp.surfaces import (
    SURFACE_CHAT_DEFAULT,
    SURFACE_EXECUTION,
    AgentInvocationContext,
    surface_for_label,
)
from pa.agent.context import compose_session_prompt
from pa.browser.manager import BrowserManager
from pa.config import Settings
from pa.core.preferences import get_preferences_store
from pa.domain.models import AgentSession, TranscriptEvent
from pa.domain.store import Store
from pa.execution.progress import sanitize_text
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

_RETRY_SECONDS = 30
_QUIESCE_POLL_SECONDS = 0.4
TRANSCRIPT_WINDOW_LIMIT = 1000
PromptAction = Literal["append", "prepend", "interrupt"]
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


class AgentSessionRuntime:
    """Owns one ACP subprocess + connection for a single PA session."""

    def __init__(
        self,
        manager: AgentSessionManager,
        session: AgentSession,
        *,
        agent_env: dict[str, str] | None = None,
        initial_transcript_seq: int | None = None,
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
        self.connection: AgentConnection | None = None
        self._prompt_lock = asyncio.Lock()
        self._prompt_admission_lock = asyncio.Lock()
        self._queue: list[QueuedPrompt] = []
        self._queue_paused = False
        self._in_flight: QueuedPrompt | None = None
        self._drain_task: asyncio.Task[None] | None = None
        self._subscribers: list[asyncio.Queue[dict[str, Any]]] = []
        self._pending_permissions: dict[str, asyncio.Future[Any]] = {}
        self._permission_requests: dict[str, dict[str, Any]] = {}
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
        return await asyncio.to_thread(call, *args, **kwargs)

    def _save_session_preserving_external_browser(self) -> None:
        persisted = self.store.get_session(self.session_id)
        persisted_browser = dict(
            ((persisted.config_json or {}).get("browser") or {}) if persisted else {}
        )
        if persisted_browser:
            config = dict(self.session.config_json or {})
            config["browser"] = persisted_browser
            self.session.config_json = config
        self.store.save_session(self.session)

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
                            timeout=30.0,
                        )
                        break
                    except asyncio.CancelledError:
                        self._transcript_buffer = batch + self._transcript_buffer
                        raise
                    except Exception:
                        logger.exception(
                            "Failed to persist transcript events; retrying"
                        )
                        await asyncio.sleep(delay)
                        delay = min(delay * 2, 2.0)
            finally:
                self._transcript_queue.task_done()
            self._flush_transcript()

    async def _drain_transcripts(self, *, timeout: float = 10.0) -> None:
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

    async def _on_acp_update(self, _external_session_id: str, update: Any) -> None:
        normalized = normalize_session_update(update)
        event_type = str(normalized.get("type") or "session_update")
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
        if event_type == "config_option_update" and configuration_state != "applying":
            options = normalized.get("config_options")
            if options is not None:
                cfg = dict(self.session.config_json or {})
                cfg["options"] = options
                self.session.config_json = cfg
                await self._save_session_preserving_external_browser_async()
                if self.connection:
                    self.connection.config_options = options
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
        await self._checkpoint_runtime_async(lifecycle="permission_pending")
        try:
            return await future
        finally:
            self._pending_permissions.pop(request_id, None)
            self._permission_requests.pop(request_id, None)
            await self._checkpoint_runtime_async(
                lifecycle="prompting" if self._in_flight else "ready"
            )

    async def start(
        self,
        *,
        resume_external_id: str | None = None,
        queued_prompts: list[QueuedPrompt] | None = None,
        queue_paused: bool = False,
        provider_spec=None,
        initial_configuration: SessionConfigurationRequest | None = None,
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
            wire_path=wire_path,
            auto_approve=False,
            async_runtime=self.async_runtime,
            extra_env=self.agent_env,
        )
        try:
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
            persisted = dict(
                ((self.session.config_json or {}).get("configuration") or {}).get(
                    "requested"
                )
                or {}
            )
            configuration = initial_configuration
            if configuration is None and persisted:
                configuration = SessionConfigurationRequest.from_dict(persisted)
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
            await self._save_session_preserving_external_browser_async()
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
        await self._checkpoint_runtime_async(lifecycle="ready")
        self._flush_transcript()
        await self._drain_transcripts()
        await self._drain_transcripts()
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

    def _start_drain(self) -> None:
        if self._drain_task and not self._drain_task.done():
            return
        if self._queue_paused or not self._queue:
            return
        self._drain_task = asyncio.create_task(self._drain_queue())

    async def _drain_queue(self) -> None:
        while (
            self._queue
            and not self._queue_paused
            and not self._closed
            and self.connected
        ):
            if self.manager.quiescing:
                break
            item = self._queue.pop(0)
            self._append_transcript(
                "queue_dequeued", {"id": item.id, "message": item.message}
            )
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
        prompt_audit: list[dict[str, Any]] | None = None,
        prompt_id: str | None = None,
    ) -> QueuedPrompt:
        cwd = self._validated_cwd(cwd)
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
            prompt_audit=list(prompt_audit or []),
        )
        if action == "prepend":
            self._queue.insert(0, item)
        else:
            self._queue.append(item)
        self._append_transcript(
            "queue_enqueued",
            {
                "id": item.id,
                "message": message,
                "images": [image.public_dict() for image in item.images],
                "action": action,
                "position": 0 if action == "prepend" else len(self._queue) - 1,
            },
        )
        try:
            self._checkpoint_runtime(lifecycle="queued")
        except Exception:
            self._queue = [queued for queued in self._queue if queued.id != item.id]
            raise
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
        prompt_id: str | None = None,
        _from_queue: bool = False,
        wait: bool = True,
    ) -> str:
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
                    prompt_id=prompt_id,
                )
                return "queued"

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
            return "started"
        return await self._run_prompt(item)

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
            self._in_flight = item
            self._turn_started_at = datetime.now(UTC)
            self._turn_agent_events = []
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
                except Exception as exc:
                    if self._is_connection_loss(exc):
                        self._finish_turn_state()
                        self._notify_connection_lost(item, exc)
                        await self._drain_transcripts()
                        return "connection_lost"
                    raise
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

                        final_text = assemble_final_assistant_message(
                            self._turn_agent_events
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
                self._finish_turn_state()

    def _finish_turn_state(self) -> None:
        self._in_flight = None
        self._turn_started_at = None
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
        self._flush_transcript()

    async def cancel(self, *, pause_queue: bool = True) -> None:
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
        await self.connection.set_model(model_id)
        self.session = self.connection.session or self.session
        self._append_transcript("model_changed", {"model_id": model_id})
        self._flush_transcript()
        await self._drain_transcripts()

    async def configure(self, requested: SessionConfigurationRequest) -> dict[str, Any]:
        if not self.connection:
            raise RuntimeError("Session not connected")
        if self.prompting:
            raise RuntimeError(
                "Wait for the current turn to finish before changing session configuration"
            )
        async with self._prompt_lock:
            effective = await self.connection.configure(requested, merge=True)
            self.session = self.connection.session or self.session
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
        await self.connection.set_config(config_id, value)
        self.session = self.connection.session or self.session
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
        snapshot = {
            "session": self.session.model_dump(mode="json"),
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
        for req_id, fut in list(self._pending_permissions.items()):
            if not fut.done():
                fut.set_result(permission_cancelled())
            self._pending_permissions.pop(req_id, None)
            self._permission_requests.pop(req_id, None)
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

    def __init__(self, settings: Settings, store: Store) -> None:
        self.settings = settings
        self.store = store
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
        self._default_label = "default"
        self._lock = asyncio.Lock()
        self._reconnect_lock = asyncio.Lock()
        self._reconnect_task: asyncio.Task[bool] | None = None
        self._label_locks: dict[str, asyncio.Lock] = {}
        self.async_runtime: AsyncRuntime | None = None
        self.browser = BrowserManager(settings.data_dir)
        self.workspace_manager = WorkspaceManager(settings, store)
        self.completion_handler: (
            Callable[[str, dict[str, Any]], Awaitable[Any] | Any] | None
        ) = None
        self.progress_handler: (
            Callable[[str, dict[str, Any]], Awaitable[Any] | Any] | None
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
    ) -> AgentSessionRuntime:
        initial_seq = await self._offload(
            "sqlite.transcript_sequence",
            lambda: self.store.next_transcript_seq(session.id) - 1,
        )
        return AgentSessionRuntime(
            self,
            session,
            agent_env=agent_env,
            initial_transcript_seq=initial_seq,
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

    def complete_startup(self, error: BaseException | None = None) -> None:
        self._startup_error = str(error)[:1000] if error else None
        self._startup_phase = "failed" if error else "ready"
        self._startup_complete = error is None
        self._startup_session_id = None

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
        if requested_cwd:
            requested_path = Path(requested_cwd).expanduser().resolve()
            data_dir = self.settings.data_dir.expanduser().resolve()
            if requested_path == data_dir or data_dir in requested_path.parents:
                logger.warning(
                    "Ignoring stale session cwd inside PA_DATA_DIR for session %s; "
                    "rematerializing an allowed workspace",
                    session.id,
                )
                requested_cwd = None
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
                if session.project_id:
                    project = await self._offload(
                        "sqlite.project_read",
                        self.store.get_project,
                        session.project_id,
                    )
                    if project is None:
                        raise WorkspaceProvisioningError(
                            f"Project {session.project_id} is not available on this instance; sync or link the project checkout, then retry workspace provisioning"
                        )
                    workspace = await self._offload(
                        "workspace.project_provision",
                        self.workspace_manager.provision_project,
                        project_id=session.project_id,
                        session_id=session.id,
                        card_id=session.card_id,
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
                        repositories=list(
                            materialization_plan.get("repositories") or []
                        ),
                        session_id=session.id,
                        card_id=session.card_id,
                        project_id=None,
                        realm_id=session.realm_id,
                        provider_id=provider_id,
                        timeout=900.0,
                    )
                if workspace is None or not workspace.repositories:
                    raise WorkspaceProvisioningError(
                        "Repository materialization did not produce a verified leased worktree"
                    )
            elif not execution_profile and session.project_id:
                project = await self._offload(
                    "sqlite.project_read", self.store.get_project, session.project_id
                )
                if project is None:
                    raise WorkspaceProvisioningError(
                        f"Project {session.project_id} is not available on this instance; sync or link the project checkout, then retry workspace provisioning"
                    )
                workspace = await self._offload(
                    "workspace.project_provision",
                    self.workspace_manager.provision_project,
                    project_id=session.project_id,
                    session_id=session.id,
                    card_id=session.card_id,
                    realm_id=getattr(project, "realm_id", self.settings.primary_realm),
                    provider_id=provider_id,
                    timeout=900.0,
                )
            if workspace is None and (
                execution_profile == "repository" or session.project_id
            ):
                raise WorkspaceProvisioningError(
                    "Project has no linked repositories to provision"
                )
            if workspace is None:
                workspace = await self._offload(
                    "workspace.scratch_provision",
                    self.workspace_manager.scratch_workspace,
                    session_id=session.id,
                    card_id=session.card_id,
                    project_id=session.project_id,
                    requested_cwd=requested_cwd,
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
            project_blocked = bool(session.project_id and _project_recovery_block(exc))
            session.status = (
                RECOVERY_BLOCKED_STATUS if project_blocked else "provisioning_failed"
            )
            config = dict(session.config_json or {})
            if session.dispatch_id:
                config["execution_context"] = {
                    "authority_instance": authority_instance,
                    "provenance": provenance,
                }
            else:
                config.pop("execution_context", None)
            config["provisioning"] = {
                "state": "blocked" if project_blocked else "failed",
                "stage": "workspace",
                "retryable": not project_blocked,
                "manual_retry": project_blocked,
                "automatic_retry": not project_blocked,
                "error_code": (
                    "project_unavailable_on_instance"
                    if project_blocked
                    else "workspace_provisioning_failed"
                ),
                "action": (
                    "Sync the project and repository links to this instance, or "
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
            session.cwd = None
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
        return self._runtimes.get(session_id)

    def list_sessions(self) -> list[AgentSession]:
        return [rt.session for rt in self._runtimes.values()]

    def list_runtimes(self) -> list[AgentSessionRuntime]:
        return list(self._runtimes.values())

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
        return "ACP agent offline"

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
        if session.status in AUTO_RECOVERY_SESSION_STATUSES:
            return "status"
        durable = dict((session.config_json or {}).get(_DURABLE_RUNTIME_KEY) or {})
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
                    if eligibility == "project_available":
                        logger.info(
                            "Project availability changed; retrying blocked ACP "
                            "session %s",
                            session.id,
                        )
                elif session.status == RECOVERY_BLOCKED_STATUS:
                    self._startup_blocked += 1
                    logger.info(
                        "ACP recovery remains blocked for session %s: %s",
                        session.id,
                        self._recovery_action(session),
                    )
                elif session.status != "closed":
                    if session.status in RECOVERY_RETAINED_SESSION_STATUSES:
                        self._startup_deferred += 1
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
                    await self._resume_from_snapshot(
                        sess, snapshot or QuiesceSnapshot(reason="recovery")
                    )
                    self._startup_recovered += 1
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if self._should_abort_recovery():
                        return
                    self._startup_failed += 1
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

    @staticmethod
    def _snapshot_from_persisted(session: AgentSession) -> SessionSnapshot:
        durable = dict((session.config_json or {}).get(_DURABLE_RUNTIME_KEY) or {})
        queued = [
            QueuedPrompt.model_validate(item)
            for item in durable.get("queued_prompts") or []
        ]
        in_flight_raw = durable.get("in_flight")
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
        blocked = bool(session.project_id and _project_recovery_block(exc))
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
        if provider_spec is not None:
            provider_spec.env.update(workspace_env)
        runtime = await self._new_runtime(session, agent_env=workspace_env)
        queued = list(snap.queued_prompts)
        interrupted = snap.in_flight
        # Version-1 snapshots briefly encoded an interrupted turn as the first
        # queued item. Preserve recovery semantics when reading those files.
        if interrupted is None and queued and queued[0].source == "in_flight":
            interrupted = queued.pop(0)
        if interrupted:
            if interrupted.source != "recovery":
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
        await runtime.start(
            resume_external_id=snap.external_session_id,
            queued_prompts=queued,
            queue_paused=snap.queue_paused,
            provider_spec=provider_spec,
        )
        self._runtimes[runtime.session_id] = runtime
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
            # Force recreate
            await runtime.close()
            self._runtimes.pop(runtime.session_id, None)
            runtime = await self.create_session(
                label=self._default_label, title="Instance agent"
            )
            self._last_error = None
            return runtime.connected
        except Exception as exc:
            self._last_error = str(exc)
            logger.exception("Agent reconnect failed")
            return False

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
        resume_external_id: str | None = None,
        existing: AgentSession | None = None,
        surface: str | None = None,
        provider_override: str | None = None,
        project_tool_config: dict | None = None,
        initial_configuration: SessionConfigurationRequest | None = None,
        execution_context_seed: dict[str, Any] | None = None,
        _startup_recovery: bool = False,
    ) -> AgentSessionRuntime:
        if not self.settings.agent_enabled:
            raise RuntimeError("Agent disabled")
        if not _startup_recovery and not self._startup_complete:
            self.require_startup_complete()
        if self._should_abort_admission():
            raise RuntimeError("Agent is quiescing")

        effective_principal_id = (
            principal_id
            if principal_id is not None
            else existing.principal_id
            if existing
            else None
        )
        surface_key = surface or surface_for_label(label, project_id=project_id)
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
        if provider_id == "codex" and requested_mode:
            # codex-acp chooses its sandbox before ACP initialize/session-new.
            # Applying the mode later is too late and can silently start a
            # workspace-write provider for an agent-full-access dispatch.
            resolved_spec.env["INITIAL_AGENT_MODE"] = requested_mode

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
        if requested_mode:
            session.mode_id = requested_mode
        if execution_context_seed:
            config = dict(session.config_json or {})
            execution = dict(config.get("execution_context") or {})
            execution.update(execution_context_seed)
            config["execution_context"] = execution
            session.config_json = config
        workspace_env = await self._prepare_workspace(
            session,
            requested_cwd=cwd or (existing.cwd if existing else None),
            provider_id=provider_id,
            mode_id=requested_mode,
        )
        effective_agent_env = dict(agent_env or {})
        effective_agent_env.update(workspace_env)
        resolved_spec.env.update(workspace_env)

        runtime = await self._new_runtime(session, agent_env=effective_agent_env)
        try:
            start_kwargs: dict[str, Any] = {
                "resume_external_id": resume_external_id,
                "provider_spec": resolved_spec,
            }
            if initial_configuration is not None:
                start_kwargs["initial_configuration"] = initial_configuration
            await runtime.start(**start_kwargs)
            self._last_error = None
        except Exception as exc:
            self._last_error = str(exc)
            configuration = dict(
                ((session.config_json or {}).get("configuration") or {})
            )
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
                    error=str(exc),
                    timeout=30.0,
                )
            except Exception:
                logger.exception(
                    "Could not fence workspace after session startup failure for %s",
                    session.id,
                )
            raise
        self._runtimes[runtime.session_id] = runtime
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
                initial_configuration=initial_configuration,
                _startup_recovery=_startup_recovery,
            )

    async def recover_session(
        self,
        session_id: str,
        *,
        provider_override: str | None = None,
    ) -> AgentSessionRuntime:
        """Reconnect one durable PA session without creating a second PA identity."""
        self.require_startup_complete()
        async with self.label_lock(f"recover:{session_id}"):
            runtime = self.get(session_id)
            if runtime and not runtime._closed:
                return runtime
            session = await self._offload(
                "sqlite.agent_session_read", self.store.get_session, session_id
            )
            if session is None:
                raise AgentSessionRecoveryError("PA session was deleted")
            if session.status == "closed":
                raise AgentSessionRecoveryError("PA session is closed")
            if session.status == RECOVERY_BLOCKED_STATUS:
                raise AgentSessionRecoveryError("PA session recovery is blocked")
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
        self.require_startup_complete()
        if session_id:
            runtime = self._runtimes.get(session_id)
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

    async def stop(self, *, fast: bool = False) -> None:
        self._accepting = False
        self._quiescing = True

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

        await _emit("quiescing")
        deadline = asyncio.get_running_loop().time() + timeout
        while any(rt.prompting for rt in self._runtimes.values()):
            if asyncio.get_running_loop().time() >= deadline:
                await _emit(
                    "timeout", done=True, error="Timed out waiting for ACP turn"
                )
                raise TimeoutError("Timed out waiting for active ACP session to finish")
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
            await runtime._drain_transcripts()
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
            timeout=30.0,
        )
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
