import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from pa.acp.client import AgentConnection
from pa.config import Settings
from pa.domain.store import Store
from pa.instance.quiesce import (
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


class InstanceAgent:
    """Each PA instance maintains a dedicated, always-on agent session."""

    def __init__(self, settings: Settings, store: Store) -> None:
        self.settings = settings
        self.store = store
        self._connection: AgentConnection | None = None
        self._prompt_lock = asyncio.Lock()
        self._retry_task: asyncio.Task[None] | None = None
        self._drain_task: asyncio.Task[None] | None = None
        self._last_error: str | None = None
        self._quiescing = False
        self._accepting_prompts = True
        self._pending_prompts: list[QueuedPrompt] = []
        self._in_flight: QueuedPrompt | None = None
        self._resume_on_start = True

    @property
    def connected(self) -> bool:
        return (
            self._connection is not None
            and self._connection.session is not None
            and self._connection.session.status != "disconnected"
        )

    @property
    def last_error(self) -> str | None:
        return self._last_error

    @property
    def prompting(self) -> bool:
        return bool(self._connection and self._connection.prompting)

    @property
    def quiescing(self) -> bool:
        return self._quiescing

    def progress(self) -> QuiesceProgress:
        session = self._connection.session if self._connection else None
        return QuiesceProgress(
            phase="quiescing" if self._quiescing else ("prompting" if self.prompting else "idle"),
            connected=self.connected,
            prompting=self.prompting,
            quiescing=self._quiescing,
            active_sessions=1 if self.connected else 0,
            queued_prompts=len(self._pending_prompts) + (1 if self._in_flight else 0),
            message=self._status_message(),
            done=False,
            error=self._last_error,
            snapshot={
                "external_session_id": session.external_session_id if session else None,
                "status": session.status if session else None,
                "cwd": self._connection.session_cwd if self._connection else None,
            },
        )

    def _status_message(self) -> str:
        if self._quiescing and self.prompting:
            return "Waiting for active ACP turn to finish…"
        if self._quiescing:
            return "Capturing ACP session state…"
        if self.prompting:
            return "ACP session is actively working"
        if self.connected:
            return "ACP session idle"
        return "ACP agent offline"

    async def start(self, *, resume: bool | None = None) -> None:
        if not self.settings.agent_enabled:
            logger.info("Instance agent disabled")
            return
        if resume is not None:
            self._resume_on_start = resume
        await self._connect_once()
        if not self.connected:
            self._start_retry_loop()
        elif self._resume_on_start:
            self._start_drain_task()

    async def reconnect(self) -> bool:
        await self._connect_once()
        if self.connected:
            self._stop_retry_loop()
            self._start_drain_task()
        elif not self._retry_task:
            self._start_retry_loop()
        return self.connected

    async def _connect_once(self) -> None:
        if self._connection:
            await self._connection.disconnect()
            self._connection = None

        snapshot = load_quiesce_snapshot(self.settings.data_dir) if self._resume_on_start else None
        resume_id = None
        cwd = None
        existing = None
        if snapshot and snapshot.resume and snapshot.sessions:
            sess = snapshot.sessions[0]
            resume_id = sess.external_session_id
            cwd = sess.cwd
            if sess.session_id:
                existing = self.store.get_session(sess.session_id)
            if snapshot.queued_prompts:
                self._pending_prompts = list(snapshot.queued_prompts)

        self._connection = AgentConnection(self.settings, self.store, agent_name="instance")
        try:
            session = await self._connection.connect(
                resume_external_id=resume_id,
                cwd=cwd,
                existing_session=existing,
            )
            self._last_error = None
            self._accepting_prompts = True
            self._quiescing = False
            logger.info("Instance agent connected: %s", session.external_session_id)
            if snapshot and snapshot.resume:
                clear_quiesce_snapshot(self.settings.data_dir)
        except Exception as exc:
            self._last_error = str(exc)
            logger.exception("Failed to connect instance agent")
            self._connection = None

    def _start_retry_loop(self) -> None:
        if self._retry_task and not self._retry_task.done():
            return
        self._retry_task = asyncio.create_task(self._retry_loop())

    def _stop_retry_loop(self) -> None:
        if self._retry_task and not self._retry_task.done():
            self._retry_task.cancel()
        self._retry_task = None

    def _start_drain_task(self) -> None:
        if self._drain_task and not self._drain_task.done():
            return
        if not self._pending_prompts:
            return
        self._drain_task = asyncio.create_task(self._drain_pending_prompts())

    async def _retry_loop(self) -> None:
        while self.settings.agent_enabled and not self.connected:
            await asyncio.sleep(_RETRY_SECONDS)
            if self.connected:
                break
            logger.info("Retrying instance agent connection")
            await self._connect_once()
            if self.connected:
                self._start_drain_task()

    async def _drain_pending_prompts(self) -> None:
        while self._pending_prompts and self.connected and self._accepting_prompts:
            item = self._pending_prompts.pop(0)
            try:
                await self.prompt(
                    item.message,
                    item_id=item.card_id,
                    principal_id=item.principal_id,
                    project_id=item.project_id,
                    agent_env=item.agent_env or None,
                    cwd=item.cwd,
                    _from_queue=True,
                )
            except Exception:
                logger.exception("Failed to resume queued prompt %s", item.id)
                self._pending_prompts.insert(0, item)
                break

    async def stop(self) -> None:
        self._stop_retry_loop()
        if self._drain_task and not self._drain_task.done():
            self._drain_task.cancel()
        self._drain_task = None
        if self._connection:
            await self._connection.disconnect()
            self._connection = None

    def enqueue_prompt(
        self,
        message: str,
        *,
        card_id: str | None = None,
        project_id: str | None = None,
        principal_id: str | None = None,
        cwd: str | None = None,
        agent_env: dict[str, str] | None = None,
        source: str = "api",
    ) -> QueuedPrompt:
        item = QueuedPrompt(
            message=message,
            card_id=card_id,
            project_id=project_id,
            principal_id=principal_id,
            cwd=cwd,
            agent_env=dict(agent_env or {}),
            source=source,
        )
        self._pending_prompts.append(item)
        return item

    async def prompt(
        self,
        message: str,
        item_id: str | None = None,
        *,
        principal_id: str | None = None,
        project_id: str | None = None,
        agent_env: dict[str, str] | None = None,
        cwd: str | None = None,
        _from_queue: bool = False,
    ) -> str:
        if self._quiescing or not self._accepting_prompts:
            if _from_queue:
                raise RuntimeError("Instance agent is quiescing")
            self.enqueue_prompt(
                message,
                card_id=item_id,
                project_id=project_id,
                principal_id=principal_id,
                cwd=cwd,
                agent_env=agent_env,
            )
            return "queued"
        if not self._connection:
            raise RuntimeError("Instance agent not connected")
        env = dict(agent_env or {})
        if cwd:
            env["PWD"] = cwd
        in_flight = QueuedPrompt(
            message=message,
            card_id=item_id,
            project_id=project_id,
            principal_id=principal_id,
            cwd=cwd,
            agent_env=env,
            source="in_flight",
        )
        async with self._prompt_lock:
            self._in_flight = in_flight
            try:
                with _agent_env_overlay(env):
                    return await self._connection.prompt(
                        message,
                        item_id=item_id,
                        principal_id=principal_id,
                        project_id=project_id,
                        cwd=cwd,
                    )
            finally:
                self._in_flight = None

    async def quiesce(
        self,
        *,
        reason: str = "restart",
        timeout: float = 300.0,
        on_progress: Callable[[QuiesceProgress], Awaitable[None] | None] | None = None,
    ) -> QuiesceSnapshot:
        """Stop accepting prompts, wait for the active turn, and persist resume state.

        Live ACP subprocess handoff is not possible (stdio is owned by this process).
        Instead we wait for the current turn to finish, snapshot session + queue state,
        then disconnect so a restarted PA can resume.
        """
        self._quiescing = True
        self._accepting_prompts = False
        self._stop_retry_loop()
        if self._drain_task and not self._drain_task.done():
            self._drain_task.cancel()
        self._drain_task = None

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
        while self.prompting or self._prompt_lock.locked():
            if asyncio.get_running_loop().time() >= deadline:
                await _emit("timeout", done=True, error="Timed out waiting for ACP turn")
                raise TimeoutError("Timed out waiting for active ACP session to finish")
            await _emit("waiting")
            await asyncio.sleep(_QUIESCE_POLL_SECONDS)

        await _emit("capturing")
        sessions: list[SessionSnapshot] = []
        queued = list(self._pending_prompts)
        if self._in_flight:
            queued.insert(0, self._in_flight)

        if self._connection and self._connection.session:
            sess = self._connection.session
            sessions.append(
                SessionSnapshot(
                    session_id=sess.id,
                    external_session_id=sess.external_session_id,
                    agent_name=sess.agent_name,
                    status="idle",
                    cwd=self._connection.session_cwd or str(self.settings.data_dir),
                    card_id=sess.card_id or sess.item_id,
                    project_id=sess.project_id,
                    principal_id=sess.principal_id,
                    prompting=False,
                    in_flight=None,
                )
            )
            sess.status = "quiesced"
            sess.updated_at = datetime.now(UTC)
            self.store.save_session(sess)

        snapshot = QuiesceSnapshot(
            reason=reason,
            resume=True,
            sessions=sessions,
            queued_prompts=queued,
        )
        save_quiesce_snapshot(self.settings.data_dir, snapshot)

        if self._connection:
            await self._connection.disconnect()
            self._connection = None

        self._pending_prompts = []
        self._in_flight = None
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


_instance_agent: InstanceAgent | None = None


def get_instance_agent(settings: Settings, store: Store) -> InstanceAgent:
    global _instance_agent
    if _instance_agent is None:
        _instance_agent = InstanceAgent(settings, store)
    return _instance_agent


def reset_instance_agent() -> None:
    global _instance_agent
    _instance_agent = None
