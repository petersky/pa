from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from acp import PROTOCOL_VERSION, spawn_agent_process, text_block
from acp.interfaces import Client

from pa.acp.mcp_config import pa_mcp_servers
from pa.config import Settings
from pa.domain.models import AgentSession
from pa.domain.store import Store
from pa.knowledge.capture import capture_from_updates
from pa.packaging.paths import resolve_executable


class PAClient(Client):
    """ACP Client implementation — PA's side of the agent conversation."""

    def __init__(self, store: Store, on_update: Any | None = None) -> None:
        self.store = store
        self.on_update = on_update
        self._updates: list[Any] = []

    async def request_permission(
        self, options, session_id, tool_call, **kwargs: Any
    ):
        return {"outcome": {"outcome": "approved"}}

    async def session_update(self, session_id, update, **kwargs: Any) -> None:
        self._updates.append(update)
        if self.on_update:
            await self.on_update(session_id, update)

    def drain_updates(self) -> list[Any]:
        updates, self._updates = self._updates, []
        return updates


class AgentConnection:
    """Manages a single ACP connection to an agent server."""

    def __init__(
        self,
        settings: Settings,
        store: Store,
        agent_name: str = "cursor",
    ) -> None:
        self.settings = settings
        self.store = store
        self.agent_name = agent_name
        self._ctx = None
        self._conn: Any = None
        self._proc: Any = None
        self._client: PAClient | None = None
        self.session: AgentSession | None = None
        self.session_cwd: str | None = None
        self._resume_supported: bool = False
        self._init_response: Any = None

    @property
    def prompting(self) -> bool:
        return bool(self.session and self.session.status == "prompting")

    async def connect(
        self,
        *,
        resume_external_id: str | None = None,
        cwd: str | None = None,
        existing_session: AgentSession | None = None,
    ) -> AgentSession:
        if not self.settings.agent_enabled:
            raise RuntimeError("Agent connection disabled (PA_AGENT_ENABLED=false)")

        self._client = PAClient(self.store)
        command = self.settings.agent_command
        resolved = resolve_executable(command)
        if resolved:
            command = str(resolved)
        self._ctx = spawn_agent_process(
            self._client,
            command,
            *self.settings.agent_args,
        )
        self._conn, self._proc = await self._ctx.__aenter__()  # noqa: SIM117
        self._init_response = await self._conn.initialize(protocol_version=PROTOCOL_VERSION)
        self._resume_supported = _agent_supports_resume(self._init_response)

        session_cwd = cwd or str(self.settings.data_dir)
        self.session_cwd = session_cwd
        mcp = pa_mcp_servers(self.settings)

        resumed = False
        if resume_external_id and self._resume_supported:
            try:
                await self._conn.resume_session(
                    cwd=session_cwd,
                    session_id=resume_external_id,
                    mcp_servers=mcp,
                )
                resumed = True
            except Exception:
                resumed = False

        if resumed:
            if existing_session:
                self.session = existing_session
                self.session.external_session_id = resume_external_id
                self.session.status = "idle"
                self.session.updated_at = datetime.now(UTC)
            else:
                self.session = AgentSession(
                    agent_name=self.agent_name,
                    external_session_id=resume_external_id,
                    status="idle",
                )
            self.store.save_session(self.session)
            return self.session

        acp_session = await self._conn.new_session(
            cwd=session_cwd,
            mcp_servers=mcp,
        )
        if existing_session:
            self.session = existing_session
            self.session.external_session_id = acp_session.session_id
            self.session.status = "connected"
            self.session.updated_at = datetime.now(UTC)
        else:
            self.session = AgentSession(
                agent_name=self.agent_name,
                external_session_id=acp_session.session_id,
                status="connected",
            )
        self.store.save_session(self.session)
        return self.session

    async def prompt(
        self,
        message: str,
        item_id: str | None = None,
        *,
        principal_id: str | None = None,
        project_id: str | None = None,
        cwd: str | None = None,
    ) -> str:
        if not self._conn or not self.session or not self.session.external_session_id:
            raise RuntimeError("Not connected to agent")

        if item_id:
            self.session.item_id = item_id
            self.session.card_id = item_id
        if project_id:
            self.session.project_id = project_id
        if principal_id:
            self.session.principal_id = principal_id
        if cwd:
            self.session_cwd = cwd
        self.session.status = "prompting"
        self.session.updated_at = datetime.now(UTC)
        self.store.save_session(self.session)

        response = await self._conn.prompt(
            session_id=self.session.external_session_id,
            prompt=[text_block(message)],
            message_id=str(uuid4()),
        )

        updates = self._client.drain_updates() if self._client else []
        capture_from_updates(
            self.store,
            session_id=self.session.id,
            item_id=item_id,
            updates=updates,
        )

        self.session.status = "idle"
        self.session.updated_at = datetime.now(UTC)
        self.store.save_session(self.session)
        return str(getattr(response, "stop_reason", "end_turn"))

    async def disconnect(self) -> None:
        if self._ctx:
            await self._ctx.__aexit__(None, None, None)
        self._ctx = None
        self._conn = None
        self._proc = None
        if self.session:
            self.session.status = "disconnected"
            self.store.save_session(self.session)


def _agent_supports_resume(init_response: Any) -> bool:
    caps = getattr(init_response, "agent_capabilities", None) or getattr(
        init_response, "agentCapabilities", None
    )
    if caps is None and isinstance(init_response, dict):
        caps = init_response.get("agentCapabilities") or init_response.get("agent_capabilities")
    if caps is None:
        return False
    session_caps = getattr(caps, "session_capabilities", None) or getattr(
        caps, "sessionCapabilities", None
    )
    if session_caps is None and isinstance(caps, dict):
        session_caps = caps.get("sessionCapabilities") or caps.get("session_capabilities")
    if session_caps is None:
        return False
    resume = getattr(session_caps, "resume", None)
    if resume is None and isinstance(session_caps, dict):
        resume = session_caps.get("resume")
    return bool(resume)
