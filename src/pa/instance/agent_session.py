import asyncio
import logging
import os
from contextlib import contextmanager
from typing import Any

from pa.acp.client import AgentConnection
from pa.config import Settings
from pa.domain.store import Store

logger = logging.getLogger(__name__)


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
        self._task: asyncio.Task[Any] | None = None

    @property
    def connected(self) -> bool:
        return (
            self._connection is not None
            and self._connection.session is not None
            and self._connection.session.status != "disconnected"
        )

    async def start(self) -> None:
        if not self.settings.agent_enabled:
            logger.info("Instance agent disabled")
            return
        self._connection = AgentConnection(self.settings, self.store, agent_name="instance")
        try:
            session = await self._connection.connect()
            logger.info("Instance agent connected: %s", session.external_session_id)
        except Exception:
            logger.exception("Failed to connect instance agent")
            self._connection = None

    async def stop(self) -> None:
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
        env = agent_env or {}
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
