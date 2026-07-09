import asyncio
import logging
import os
from contextlib import contextmanager
from typing import Any

from pa.acp.client import AgentConnection
from pa.config import Settings
from pa.domain.store import Store

logger = logging.getLogger(__name__)

_RETRY_SECONDS = 30


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
        self._last_error: str | None = None

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

    async def start(self) -> None:
        if not self.settings.agent_enabled:
            logger.info("Instance agent disabled")
            return
        await self._connect_once()
        if not self.connected:
            self._start_retry_loop()

    async def reconnect(self) -> bool:
        await self._connect_once()
        if self.connected:
            self._stop_retry_loop()
        elif not self._retry_task:
            self._start_retry_loop()
        return self.connected

    async def _connect_once(self) -> None:
        if self._connection:
            await self._connection.disconnect()
            self._connection = None

        self._connection = AgentConnection(self.settings, self.store, agent_name="instance")
        try:
            session = await self._connection.connect()
            self._last_error = None
            logger.info("Instance agent connected: %s", session.external_session_id)
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

    async def _retry_loop(self) -> None:
        while self.settings.agent_enabled and not self.connected:
            await asyncio.sleep(_RETRY_SECONDS)
            if self.connected:
                break
            logger.info("Retrying instance agent connection")
            await self._connect_once()

    async def stop(self) -> None:
        self._stop_retry_loop()
        if self._connection:
            await self._connection.disconnect()
            self._connection = None

    async def prompt(
        self,
        message: str,
        item_id: str | None = None,
        *,
        principal_id: str | None = None,
        project_id: str | None = None,
        agent_env: dict[str, str] | None = None,
        cwd: str | None = None,
    ) -> str:
        if not self._connection:
            raise RuntimeError("Instance agent not connected")
        env = dict(agent_env or {})
        if cwd:
            env["PWD"] = cwd
        async with self._prompt_lock:
            with _agent_env_overlay(env):
                return await self._connection.prompt(
                    message,
                    item_id=item_id,
                    principal_id=principal_id,
                    project_id=project_id,
                    cwd=cwd,
                )


_instance_agent: InstanceAgent | None = None


def get_instance_agent(settings: Settings, store: Store) -> InstanceAgent:
    global _instance_agent
    if _instance_agent is None:
        _instance_agent = InstanceAgent(settings, store)
    return _instance_agent
