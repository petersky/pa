from __future__ import annotations

import asyncio
import inspect
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Awaitable, Callable
from uuid import uuid4

from acp import PROTOCOL_VERSION, image_block, text_block
from acp.interfaces import Client
from acp.schema import AllowedOutcome, DeniedOutcome, RequestPermissionResponse

from pa.acp.mcp_config import pa_mcp_servers
from pa.acp.providers.base import AgentProviderSpec
from pa.acp.providers.registry import DEFAULT_PROVIDER_ID, get_provider
from pa.acp.providers.resolve import _spawn_overrides
from pa.acp.transport import spawn_agent
from pa.config import Settings
from pa.domain.models import AgentSession
from pa.domain.store import Store
from pa.instance.quiesce import ImageAttachment
from pa.knowledge.capture import capture_from_updates
from pa.packaging.paths import resolve_executable

logger = logging.getLogger(__name__)

UpdateHandler = Callable[[str, Any], Awaitable[None] | None]
PermissionHandler = Callable[[str, dict[str, Any]], Awaitable[RequestPermissionResponse | dict[str, Any]]]
WireLogger = Callable[[str, dict[str, Any]], None]


def permission_selected(option_id: str) -> RequestPermissionResponse:
    return RequestPermissionResponse(outcome=AllowedOutcome(outcome="selected", option_id=option_id))


def permission_cancelled() -> RequestPermissionResponse:
    return RequestPermissionResponse(outcome=DeniedOutcome(outcome="cancelled"))


def _to_plain(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "model_dump"):
        try:
            return value.model_dump(mode="json", by_alias=True)
        except TypeError:
            return value.model_dump(by_alias=True)
    if isinstance(value, dict):
        return {k: _to_plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_plain(v) for v in value]
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _session_update_type(update: Any) -> str:
    if isinstance(update, dict):
        return str(update.get("sessionUpdate") or update.get("session_update") or "unknown")
    return str(
        getattr(update, "session_update", None)
        or getattr(update, "sessionUpdate", None)
        or type(update).__name__
    )


def _content_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, dict):
        if content.get("type") == "text":
            return str(content.get("text") or "")
        return str(content.get("text") or "")
    text = getattr(content, "text", None)
    if text is not None:
        return str(text)
    return ""


def normalize_session_update(update: Any) -> dict[str, Any]:
    """Normalize an ACP session update into a typed event payload."""
    plain = _to_plain(update)
    update_type = _session_update_type(update)
    payload: dict[str, Any] = {"type": update_type, "raw": plain}

    if isinstance(plain, dict):
        if update_type in {"agent_message_chunk", "agent_thought_chunk", "user_message_chunk"}:
            payload["text"] = _content_text(plain.get("content"))
            payload["message_id"] = plain.get("messageId") or plain.get("message_id")
            meta = plain.get("_meta") or {}
            codex_meta = meta.get("codex") or {} if isinstance(meta, dict) else {}
            payload["phase"] = codex_meta.get("phase") if isinstance(codex_meta, dict) else None
        elif update_type in {"tool_call", "tool_call_update"}:
            payload["tool_call_id"] = plain.get("toolCallId") or plain.get("tool_call_id")
            payload["title"] = plain.get("title")
            payload["status"] = plain.get("status")
            payload["kind"] = plain.get("kind")
            payload["content"] = plain.get("content")
            payload["locations"] = plain.get("locations")
            payload["raw_input"] = plain.get("rawInput") or plain.get("raw_input")
            payload["raw_output"] = plain.get("rawOutput") or plain.get("raw_output")
        elif update_type == "plan":
            payload["entries"] = plain.get("entries") or []
        elif update_type == "usage_update":
            payload["usage"] = plain.get("usage") or plain
        elif update_type == "current_mode_update":
            payload["mode_id"] = plain.get("currentModeId") or plain.get("current_mode_id")
        elif update_type == "config_option_update":
            payload["config_options"] = plain.get("configOptions") or plain.get("config_options")

    return payload


def extract_models_modes_config(response: Any) -> dict[str, Any]:
    models = getattr(response, "models", None)
    modes = getattr(response, "modes", None)
    config_options = getattr(response, "config_options", None)
    result: dict[str, Any] = {
        "models": _to_plain(models),
        "modes": _to_plain(modes),
        "config_options": _to_plain(config_options),
        "model_id": None,
        "mode_id": None,
    }
    if models is not None:
        result["model_id"] = getattr(models, "current_model_id", None) or (
            models.get("currentModelId") if isinstance(models, dict) else None
        )
    if modes is not None:
        result["mode_id"] = getattr(modes, "current_mode_id", None) or (
            modes.get("currentModeId") if isinstance(modes, dict) else None
        )
    return result


def usage_to_dict(usage: Any) -> dict[str, Any]:
    plain = _to_plain(usage) or {}
    if not isinstance(plain, dict):
        return {}
    return plain


class WireJsonlLogger:
    """Append-only ACP wire log for a single session."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, direction: str, message: dict[str, Any]) -> None:
        record = {
            "ts": datetime.now(UTC).isoformat(),
            "direction": direction,
            **message,
        }
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, default=str) + "\n")


class PAClient(Client):
    """ACP Client implementation — PA's side of the agent conversation."""

    def __init__(
        self,
        store: Store,
        *,
        on_update: UpdateHandler | None = None,
        on_permission: PermissionHandler | None = None,
        wire_logger: WireLogger | None = None,
        auto_approve: bool = False,
    ) -> None:
        self.store = store
        self.on_update = on_update
        self.on_permission = on_permission
        self.wire_logger = wire_logger
        self.auto_approve = auto_approve
        self._updates: list[Any] = []

    def _wire(self, direction: str, payload: dict[str, Any]) -> None:
        if self.wire_logger:
            try:
                self.wire_logger(direction, payload)
            except Exception:
                logger.exception("Failed to write ACP wire log")

    async def request_permission(
        self, options, session_id, tool_call, **kwargs: Any
    ) -> RequestPermissionResponse:
        options_plain = _to_plain(options) or []
        tool_plain = _to_plain(tool_call) or {}
        request = {
            "request_id": str(uuid4()),
            "session_id": session_id,
            "options": options_plain,
            "tool_call": tool_plain,
        }
        self._wire("in", {"method": "session/request_permission", "params": request})

        if self.auto_approve:
            option_id = _prefer_allow_option(options_plain)
            if option_id:
                response = permission_selected(option_id)
                self._wire(
                    "out",
                    {
                        "method": "session/request_permission",
                        "result": response.model_dump(mode="json", by_alias=True),
                    },
                )
                return response

        if self.on_permission:
            try:
                response = await self.on_permission(session_id, request)
                if response:
                    if isinstance(response, RequestPermissionResponse):
                        model = response
                    else:
                        model = RequestPermissionResponse.model_validate(response)
                    self._wire(
                        "out",
                        {
                            "method": "session/request_permission",
                            "result": model.model_dump(mode="json", by_alias=True),
                        },
                    )
                    return model
            except Exception:
                logger.exception("Permission handler failed")

        # Default: cancel if no UI response / auto-approve option.
        response = permission_cancelled()
        self._wire(
            "out",
            {
                "method": "session/request_permission",
                "result": response.model_dump(mode="json", by_alias=True),
            },
        )
        return response

    async def session_update(self, session_id, update, **kwargs: Any) -> None:
        self._updates.append(update)
        self._wire(
            "in",
            {
                "method": "session/update",
                "params": {
                    "session_id": session_id,
                    "update": _to_plain(update),
                },
            },
        )
        if self.on_update:
            result = self.on_update(session_id, update)
            if inspect.isawaitable(result):
                await result

    def drain_updates(self) -> list[Any]:
        updates, self._updates = self._updates, []
        return updates


def _prefer_allow_option(options: list[Any]) -> str | None:
    parsed: list[dict[str, Any]] = []
    for opt in options or []:
        if hasattr(opt, "model_dump"):
            parsed.append(opt.model_dump(by_alias=True))
        elif isinstance(opt, dict):
            parsed.append(opt)
    for kind in ("allow_always", "allow_once"):
        for opt in parsed:
            if opt.get("kind") == kind:
                return opt.get("optionId") or opt.get("option_id")
    if parsed:
        return parsed[0].get("optionId") or parsed[0].get("option_id")
    return None


class AgentConnection:
    """Manages a single ACP connection (one subprocess) for one PA session."""

    def __init__(
        self,
        settings: Settings,
        store: Store,
        agent_name: str = DEFAULT_PROVIDER_ID,
        *,
        provider_spec: AgentProviderSpec | None = None,
        on_update: UpdateHandler | None = None,
        on_permission: PermissionHandler | None = None,
        wire_path: Path | None = None,
        auto_approve: bool = False,
    ) -> None:
        self.settings = settings
        self.store = store
        self.agent_name = agent_name
        self.provider_spec = provider_spec
        self.on_update = on_update
        self.on_permission = on_permission
        self.wire_path = wire_path
        self.auto_approve = auto_approve
        self._ctx = None
        self._conn: Any = None
        self._proc: Any = None
        self._client: PAClient | None = None
        self._wire: WireJsonlLogger | None = None
        self.session: AgentSession | None = None
        self.session_cwd: str | None = None
        self._resume_supported: bool = False
        self._disconnect_lock = asyncio.Lock()
        self._init_response: Any = None
        self.models: dict[str, Any] | None = None
        self.modes: dict[str, Any] | None = None
        self.config_options: list[Any] | None = None
        self.last_usage: dict[str, Any] | None = None

    def _resolved_spec(self) -> AgentProviderSpec:
        if self.provider_spec is not None:
            return self.provider_spec
        provider_id = self.agent_name or DEFAULT_PROVIDER_ID
        if provider_id in {"instance", ""}:
            provider_id = DEFAULT_PROVIDER_ID
        provider = get_provider(provider_id)
        command_override, args_override = _spawn_overrides(self.settings, provider_id)
        return provider.resolve_spawn(
            command_override=command_override,
            args_override=args_override,
            data_dir=self.settings.data_dir,
        )

    @property
    def prompting(self) -> bool:
        return bool(self.session and self.session.status == "prompting")

    def _transport_alive(self) -> bool:
        """True when the ACP JSON-RPC transport has not closed/disconnected."""
        conn = self._conn
        if not conn:
            return False
        inner = getattr(conn, "_conn", conn)
        return not (
            getattr(inner, "_closed", False)
            or getattr(inner, "_disconnected", False)
        )

    @property
    def connected(self) -> bool:
        return bool(
            self._transport_alive()
            and self.session
            and self.session.status not in {"disconnected", "closed", "quiesced"}
        )

    def _wire_log(self, direction: str, message: dict[str, Any]) -> None:
        if self._wire:
            self._wire.log(direction, message)

    async def connect(
        self,
        *,
        resume_external_id: str | None = None,
        cwd: str | None = None,
        existing_session: AgentSession | None = None,
        title: str | None = None,
        label: str | None = None,
        principal_id: str | None = None,
        card_id: str | None = None,
        project_id: str | None = None,
    ) -> AgentSession:
        if not self.settings.agent_enabled:
            raise RuntimeError("Agent connection disabled (PA_AGENT_ENABLED=false)")

        if self.wire_path:
            self._wire = WireJsonlLogger(self.wire_path)

        self._client = PAClient(
            self.store,
            on_update=self.on_update,
            on_permission=self.on_permission,
            wire_logger=self._wire_log,
            auto_approve=self.auto_approve,
        )
        spec = self._resolved_spec()
        self.agent_name = spec.id
        command = spec.command
        resolved = resolve_executable(command)
        if resolved:
            command = str(resolved)
        # Apply provider env for the lifetime of this connection spawn.
        import os

        prev_env: dict[str, str | None] = {}
        for key, value in (spec.env or {}).items():
            prev_env[key] = os.environ.get(key)
            os.environ[key] = value
        try:
            self._ctx = spawn_agent(
                self._client,
                command,
                *list(spec.args or []),
            )
            self._conn, self._proc = await self._ctx.__aenter__()  # noqa: SIM117
        finally:
            for key, old in prev_env.items():
                if old is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = old
        self._init_response = await self._conn.initialize(protocol_version=PROTOCOL_VERSION)
        self._resume_supported = _agent_supports_resume(self._init_response)
        self._wire_log(
            "out",
            {
                "method": "initialize",
                "result": {
                    "resume_supported": self._resume_supported,
                    "protocol_version": PROTOCOL_VERSION,
                },
            },
        )

        session_cwd = cwd or str(self.settings.data_dir)
        self.session_cwd = session_cwd
        mcp = pa_mcp_servers(self.settings)

        resumed = False
        session_meta: dict[str, Any] = {}
        if resume_external_id and self._resume_supported:
            try:
                resume_resp = await self._conn.resume_session(
                    cwd=session_cwd,
                    session_id=resume_external_id,
                    mcp_servers=mcp,
                )
                session_meta = extract_models_modes_config(resume_resp)
                resumed = True
                self._wire_log(
                    "out",
                    {
                        "method": "session/resume",
                        "params": {"session_id": resume_external_id, "cwd": session_cwd},
                    },
                )
            except Exception:
                logger.exception("ACP resume failed; creating new session")
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
        else:
            acp_session = await self._conn.new_session(
                cwd=session_cwd,
                mcp_servers=mcp,
            )
            session_meta = extract_models_modes_config(acp_session)
            self._wire_log(
                "out",
                {
                    "method": "session/new",
                    "params": {"cwd": session_cwd},
                    "result": {"session_id": acp_session.session_id},
                },
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

        assert self.session is not None
        self.session.cwd = session_cwd
        if title is not None:
            self.session.title = title
        if label is not None:
            self.session.label = label
        if principal_id is not None:
            self.session.principal_id = principal_id
        if card_id is not None:
            self.session.card_id = card_id
            self.session.item_id = card_id
        if project_id is not None:
            self.session.project_id = project_id

        self._apply_session_meta(session_meta)
        self.store.save_session(self.session)
        return self.session

    def _apply_session_meta(self, meta: dict[str, Any]) -> None:
        if not self.session:
            return
        self.models = meta.get("models")
        self.modes = meta.get("modes")
        self.config_options = meta.get("config_options")
        if meta.get("model_id"):
            self.session.model_id = meta["model_id"]
        if meta.get("mode_id"):
            self.session.mode_id = meta["mode_id"]
        config = dict(self.session.config_json or {})
        if meta.get("models") is not None:
            config["models"] = meta.get("models")
        if meta.get("modes") is not None:
            config["modes"] = meta.get("modes")
        if meta.get("config_options") is not None:
            config["options"] = meta.get("config_options")
        self.session.config_json = config

    async def prompt(
        self,
        message: str,
        item_id: str | None = None,
        *,
        images: list[ImageAttachment] | None = None,
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
            self.session.cwd = cwd
        self.session.status = "prompting"
        self.session.updated_at = datetime.now(UTC)
        self.store.save_session(self.session)

        message_id = str(uuid4())
        self._wire_log(
            "out",
            {
                "method": "session/prompt",
                "params": {
                    "session_id": self.session.external_session_id,
                    "message_id": message_id,
                    "message": message,
                },
            },
        )
        prompt = []
        if message:
            prompt.append(text_block(message))
        prompt.extend(
            image_block(image.data, image.mime_type) for image in images or []
        )
        try:
            response = await self._conn.prompt(
                session_id=self.session.external_session_id,
                prompt=prompt,
                message_id=message_id,
            )
        except ConnectionError:
            await self._mark_transport_dead()
            raise
        except Exception:
            if not self._transport_alive():
                await self._mark_transport_dead()
            raise

        updates = self._client.drain_updates() if self._client else []
        capture_from_updates(
            self.store,
            session_id=self.session.id,
            item_id=item_id,
            updates=updates,
        )

        usage = usage_to_dict(getattr(response, "usage", None))
        if usage:
            self.last_usage = usage
            metrics = dict(self.session.metrics_json or {})
            metrics["last_usage"] = usage
            metrics["turns"] = int(metrics.get("turns") or 0) + 1
            self.session.metrics_json = metrics

        stop_reason = str(getattr(response, "stop_reason", "end_turn"))
        self._wire_log(
            "in",
            {
                "method": "session/prompt",
                "result": {"stop_reason": stop_reason, "usage": usage},
            },
        )

        self.session.status = "idle"
        self.session.updated_at = datetime.now(UTC)
        self.store.save_session(self.session)
        return stop_reason

    async def _mark_transport_dead(self) -> None:
        """Drop a dead ACP transport without blocking on a hung subprocess exit."""
        async with self._disconnect_lock:
            ctx = self._ctx
            self._ctx = None
            self._conn = None
            self._proc = None
            if self.session and self.session.status not in {"closed", "quiesced"}:
                self.session.status = "disconnected"
                self.session.updated_at = datetime.now(UTC)
                self.store.save_session(self.session)
            if ctx is not None:
                try:
                    await asyncio.wait_for(
                        ctx.__aexit__(None, None, None), timeout=2.0
                    )
                except Exception:
                    logger.debug(
                        "ACP transport cleanup after death failed", exc_info=True
                    )

    async def cancel(self) -> None:
        if not self._conn or not self.session or not self.session.external_session_id:
            return
        self._wire_log(
            "out",
            {
                "method": "session/cancel",
                "params": {"session_id": self.session.external_session_id},
            },
        )
        await self._conn.cancel(session_id=self.session.external_session_id)

    async def set_model(self, model_id: str) -> None:
        if not self._conn or not self.session or not self.session.external_session_id:
            raise RuntimeError("Not connected to agent")
        await self._conn.set_session_model(
            model_id=model_id,
            session_id=self.session.external_session_id,
        )
        self.session.model_id = model_id
        self.session.updated_at = datetime.now(UTC)
        self.store.save_session(self.session)

    async def set_mode(self, mode_id: str) -> None:
        if not self._conn or not self.session or not self.session.external_session_id:
            raise RuntimeError("Not connected to agent")
        await self._conn.set_session_mode(
            mode_id=mode_id,
            session_id=self.session.external_session_id,
        )
        self.session.mode_id = mode_id
        self.session.updated_at = datetime.now(UTC)
        self.store.save_session(self.session)

    async def set_config(self, config_id: str, value: str | bool) -> None:
        if not self._conn or not self.session or not self.session.external_session_id:
            raise RuntimeError("Not connected to agent")
        await self._conn.set_config_option(
            config_id=config_id,
            session_id=self.session.external_session_id,
            value=value,
        )
        config = dict(self.session.config_json or {})
        values = dict(config.get("values") or {})
        values[config_id] = value
        config["values"] = values
        self.session.config_json = config
        self.session.updated_at = datetime.now(UTC)
        self.store.save_session(self.session)

    async def disconnect(self) -> None:
        async with self._disconnect_lock:
            ctx = self._ctx
            self._ctx = None
            self._conn = None
            self._proc = None
            if ctx:
                await ctx.__aexit__(None, None, None)
        if self.session and self.session.status not in {"closed", "quiesced"}:
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
