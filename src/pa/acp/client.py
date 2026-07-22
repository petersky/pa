from __future__ import annotations

import asyncio
import copy
import inspect
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable
from uuid import uuid4

from acp import PROTOCOL_VERSION, image_block, text_block
from acp.exceptions import RequestError
from acp.interfaces import Client
from acp.schema import (
    AllowedOutcome,
    ClientCapabilities,
    DeniedOutcome,
    FileSystemCapabilities,
    ReadTextFileResponse,
    RequestPermissionResponse,
    WriteTextFileResponse,
)

from pa.acp.mcp_config import pa_mcp_servers
from pa.acp.configuration import (
    ACPConfigurationError,
    SessionConfigurationRequest,
    advertised_state_values,
    find_option,
    find_option_by_id,
    option_current_value,
    option_id,
    state_current_value,
    validate_option_value,
)
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

if TYPE_CHECKING:
    from pa.core.async_runtime import AsyncRuntime

logger = logging.getLogger(__name__)

UpdateHandler = Callable[[str, Any], Awaitable[None] | None]
PermissionHandler = Callable[
    [str, dict[str, Any]], Awaitable[RequestPermissionResponse | dict[str, Any]]
]
WireLogger = Callable[[str, dict[str, Any]], None]

# Cursor ACP sends vendor client methods (e.g. cursor/update_todos) without the
# ACP `_` extension prefix. Treat those as optional acknowledgements.
_TOLERATED_CLIENT_METHOD_PREFIXES = ("cursor/", "elicitation/")


def _tolerated_client_method(method: str) -> bool:
    if not isinstance(method, str) or not method:
        return False
    name = method[1:] if method.startswith("_") else method
    return name.startswith(_TOLERATED_CLIENT_METHOD_PREFIXES)


def permission_selected(option_id: str) -> RequestPermissionResponse:
    return RequestPermissionResponse(
        outcome=AllowedOutcome(outcome="selected", option_id=option_id)
    )


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
        return str(
            update.get("sessionUpdate") or update.get("session_update") or "unknown"
        )
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
        if update_type in {
            "agent_message_chunk",
            "agent_thought_chunk",
            "user_message_chunk",
        }:
            payload["text"] = _content_text(plain.get("content"))
            payload["message_id"] = plain.get("messageId") or plain.get("message_id")
            meta = plain.get("_meta") or {}
            codex_meta = meta.get("codex") or {} if isinstance(meta, dict) else {}
            payload["phase"] = (
                codex_meta.get("phase") if isinstance(codex_meta, dict) else None
            )
        elif update_type in {"tool_call", "tool_call_update"}:
            payload["tool_call_id"] = plain.get("toolCallId") or plain.get(
                "tool_call_id"
            )
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
            payload["mode_id"] = plain.get("currentModeId") or plain.get(
                "current_mode_id"
            )
        elif update_type == "config_option_update":
            payload["config_options"] = plain.get("configOptions") or plain.get(
                "config_options"
            )

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
        async_runtime: AsyncRuntime | None = None,
    ) -> None:
        self.store = store
        self.on_update = on_update
        self.on_permission = on_permission
        self.wire_logger = wire_logger
        self.auto_approve = auto_approve
        self.async_runtime = async_runtime
        self._updates: list[Any] = []

    async def _offload(
        self, operation: str, call, *args, timeout: float | None = None, **kwargs
    ):
        if self.async_runtime:
            return await self.async_runtime.run_blocking(
                operation, call, *args, timeout=timeout, **kwargs
            )
        return await asyncio.to_thread(call, *args, **kwargs)

    def _wire(self, direction: str, payload: dict[str, Any]) -> None:
        if self.wire_logger:
            try:
                self.wire_logger(direction, payload)
            except Exception:
                logger.exception("Failed to write ACP wire log")

    def on_connect(self, conn: Any) -> None:
        """Acknowledge Cursor vendor methods that arrive without the `_` prefix.

        The stock ACP client router only routes `_…` to ``ext_method``. Cursor
        still emits ``cursor/update_todos`` (and related) as plain methods, which
        otherwise surface as noisy ``Method not found`` background-task errors.
        """
        inner = getattr(conn, "_conn", None)
        original = getattr(inner, "_handler", None)
        if inner is None or original is None:
            return

        async def handler(method: str, params: Any, is_notification: bool) -> Any:
            try:
                return await original(method, params, is_notification)
            except RequestError as exc:
                if exc.code != -32601 or not _tolerated_client_method(method):
                    raise
                name = method[1:] if method.startswith("_") else method
                payload = params if isinstance(params, dict) else {}
                if is_notification:
                    await self.ext_notification(name, payload)
                    return None
                return await self.ext_method(name, payload)

        inner._handler = handler

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

    async def read_text_file(
        self,
        path: str,
        session_id: str,
        limit: int | None = None,
        line: int | None = None,
        **kwargs: Any,
    ) -> ReadTextFileResponse:
        """Serve ACP client-side file reads advertised during initialization."""
        target = Path(path)
        if not target.is_absolute():
            raise ValueError("ACP file paths must be absolute")
        content = await self._offload(
            "acp.file_read", target.read_text, encoding="utf-8", timeout=30.0
        )
        if line is not None or limit is not None:
            lines = content.splitlines(keepends=True)
            start = max((line or 1) - 1, 0)
            stop = None if limit is None else start + limit
            content = "".join(lines[start:stop])
        self._wire(
            "in",
            {
                "method": "fs/read_text_file",
                "params": {
                    "session_id": session_id,
                    "path": path,
                    "line": line,
                    "limit": limit,
                },
            },
        )
        return ReadTextFileResponse(content=content)

    async def write_text_file(
        self,
        content: str,
        path: str,
        session_id: str,
        **kwargs: Any,
    ) -> WriteTextFileResponse:
        """Serve ACP client-side file writes advertised during initialization."""
        target = Path(path)
        if not target.is_absolute():
            raise ValueError("ACP file paths must be absolute")
        await self._offload(
            "acp.file_write",
            target.write_text,
            content,
            encoding="utf-8",
            timeout=30.0,
        )
        self._wire(
            "in",
            {
                "method": "fs/write_text_file",
                "params": {"session_id": session_id, "path": path},
            },
        )
        return WriteTextFileResponse()

    async def ext_method(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """Acknowledge optional agent extensions that PA does not interpret."""
        self._wire(
            "in",
            {"method": f"_{method}", "params": params, "ignored": True},
        )
        return {}

    async def ext_notification(self, method: str, params: dict[str, Any]) -> None:
        self._wire(
            "in",
            {"method": f"_{method}", "params": params, "ignored": True},
        )

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
        async_runtime: AsyncRuntime | None = None,
        extra_env: dict[str, str] | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self.agent_name = agent_name
        self.provider_spec = provider_spec
        self.on_update = on_update
        self.on_permission = on_permission
        self.wire_path = wire_path
        self.auto_approve = auto_approve
        self.async_runtime = async_runtime
        self.extra_env = dict(extra_env or {})
        self._ctx = None
        self._conn: Any = None
        self._proc: Any = None
        self._client: PAClient | None = None
        self._wire: WireJsonlLogger | None = None
        self.session: AgentSession | None = None
        self.session_cwd: str | None = None
        self._resume_supported: bool = False
        self._load_supported: bool = False
        self._list_supported: bool = False
        self._disconnect_lock = asyncio.Lock()
        self._configuration_lock = asyncio.Lock()
        self._init_response: Any = None
        self.models: dict[str, Any] | None = None
        self.modes: dict[str, Any] | None = None
        self.config_options: list[Any] | None = None
        self.last_usage: dict[str, Any] | None = None
        self._wire_lock = asyncio.Lock()
        self._wire_tasks: set[asyncio.Task[None]] = set()
        self._wire_task_limit = 1024

    async def _offload(
        self, operation: str, call, *args, timeout: float | None = None, **kwargs
    ):
        if self.async_runtime:
            return await self.async_runtime.run_blocking(
                operation, call, *args, timeout=timeout, **kwargs
            )
        return await asyncio.to_thread(call, *args, **kwargs)

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
            getattr(inner, "_closed", False) or getattr(inner, "_disconnected", False)
        )

    @property
    def connected(self) -> bool:
        return bool(
            self._transport_alive()
            and self.session
            and self.session.status not in {"disconnected", "closed", "quiesced"}
        )

    def _wire_log(self, direction: str, message: dict[str, Any]) -> None:
        if not self._wire:
            return
        if not self.async_runtime:
            self._wire.log(direction, message)
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self._wire.log(direction, message)
            return
        if len(self._wire_tasks) >= self._wire_task_limit:
            logger.error("ACP wire-log queue is full; dropping one diagnostic record")
            return

        async def write() -> None:
            async with self._wire_lock:
                assert self._wire is not None
                await self._offload(
                    "acp.wire_append",
                    self._wire.log,
                    direction,
                    message,
                    timeout=10.0,
                )

        task = loop.create_task(write(), name="pa-acp-wire-log")
        self._wire_tasks.add(task)
        task.add_done_callback(self._wire_tasks.discard)

    async def _drain_wire_logs(self) -> None:
        pending = set(self._wire_tasks)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

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
            self._wire = await self._offload(
                "acp.wire_init", WireJsonlLogger, self.wire_path, timeout=10.0
            )

        self._client = PAClient(
            self.store,
            on_update=self.on_update,
            on_permission=self.on_permission,
            wire_logger=self._wire_log,
            auto_approve=self.auto_approve,
            async_runtime=self.async_runtime,
        )

        def resolve_launch() -> tuple[AgentProviderSpec, str]:
            spec = self._resolved_spec()
            command = spec.command
            resolved = resolve_executable(command)
            return spec, str(resolved) if resolved else command

        spec, command = await self._offload(
            "acp.provider_resolve", resolve_launch, timeout=30.0
        )
        self.agent_name = spec.id
        import os

        # Pass a per-process environment. Mutating os.environ around an await
        # races concurrent session spawns and can leak one principal's provider
        # settings into another process.
        child_env = {**os.environ, **(spec.env or {}), **self.extra_env}
        self._ctx = spawn_agent(
            self._client,
            command,
            *list(spec.args or []),
            env=child_env,
        )
        self._conn, self._proc = await self._ctx.__aenter__()  # noqa: SIM117
        self._init_response = await self._conn.initialize(
            protocol_version=PROTOCOL_VERSION,
            client_capabilities=ClientCapabilities(
                fs=FileSystemCapabilities(
                    read_text_file=True,
                    write_text_file=True,
                )
            ),
        )
        self._resume_supported = _agent_supports_resume(self._init_response)
        self._load_supported = _agent_supports_load(self._init_response)
        self._list_supported = _agent_supports_session_list(self._init_response)
        if spec.session_load_supported is False:
            self._load_supported = False
        elif spec.session_load_supported is True:
            self._load_supported = True
        self._wire_log(
            "out",
            {
                "method": "initialize",
                "result": {
                    "resume_supported": self._resume_supported,
                    "load_supported": self._load_supported,
                    "list_supported": self._list_supported,
                    "protocol_version": PROTOCOL_VERSION,
                },
            },
        )

        session_cwd = cwd or str(self.settings.data_dir)
        self.session_cwd = session_cwd
        mcp = pa_mcp_servers(self.settings)

        restored = False
        session_meta: dict[str, Any] = {}
        restore_method = (
            "session/resume"
            if self._resume_supported
            else "session/load"
            if self._load_supported
            else None
        )
        if resume_external_id and restore_method:
            load_cwd = session_cwd
            skip_restore = False
            if restore_method == "session/load" and self._list_supported:
                resolved = await _resolve_session_load_target(
                    self._conn,
                    session_id=resume_external_id,
                    cwd=session_cwd,
                )
                if resolved is None:
                    skip_restore = True
                else:
                    resume_external_id, load_cwd = resolved
                    self.session_cwd = load_cwd
            if not skip_restore:
                try:
                    restore = (
                        self._conn.resume_session
                        if restore_method == "session/resume"
                        else self._conn.load_session
                    )
                    self._wire_log(
                        "out",
                        {
                            "method": restore_method,
                            "params": {
                                "session_id": resume_external_id,
                                "cwd": load_cwd,
                            },
                        },
                    )
                    restore_resp = await restore(
                        cwd=load_cwd,
                        session_id=resume_external_id,
                        mcp_servers=mcp,
                    )
                    session_meta = extract_models_modes_config(restore_resp)
                    restored = True
                except Exception as exc:
                    # Cursor wraps unknown session ids as Invalid params with
                    # data.message "Session … not found"; fall back quietly.
                    logger.warning(
                        "ACP %s failed (%s); creating new session",
                        restore_method,
                        _format_acp_error(exc),
                    )
                    restored = False

        if restored:
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
        # Prefer the cwd actually used for resume/load (may come from session/list).
        self.session.cwd = self.session_cwd or session_cwd
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
        await self._offload(
            "sqlite.agent_session_save", self.store.save_session, self.session
        )
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
        configuration = dict(
            ((self.session.config_json or {}).get("configuration") or {})
        )
        if configuration.get("state") in {"applying", "failed"}:
            raise ACPConfigurationError(
                "ACP session configuration is not confirmed; the prompt was not delivered. "
                "Retry session admission after resolving the provider compatibility error."
            )

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
        await self._offload(
            "sqlite.agent_session_save", self.store.save_session, self.session
        )

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
        await self._offload(
            "agent.knowledge_capture",
            capture_from_updates,
            self.store,
            session_id=self.session.id,
            item_id=item_id,
            updates=updates,
            timeout=60.0,
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
        await self._offload(
            "sqlite.agent_session_save", self.store.save_session, self.session
        )
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
                await self._offload(
                    "sqlite.agent_session_save", self.store.save_session, self.session
                )
            if ctx is not None:
                try:
                    await asyncio.wait_for(ctx.__aexit__(None, None, None), timeout=2.0)
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
        await self.configure(
            SessionConfigurationRequest.from_values(model_id=model_id), merge=True
        )

    async def set_mode(self, mode_id: str) -> None:
        await self.configure(
            SessionConfigurationRequest.from_values(mode_id=mode_id), merge=True
        )

    async def set_config(self, config_id: str, value: str | bool) -> None:
        await self.configure(
            SessionConfigurationRequest.from_values(config={config_id: value}),
            merge=True,
        )

    async def configure(
        self,
        requested: SessionConfigurationRequest,
        *,
        merge: bool = False,
        force: bool = False,
    ) -> dict[str, Any]:
        """Apply and verify one complete configuration behind an admission barrier."""
        if not self._conn or not self.session or not self.session.external_session_id:
            raise RuntimeError("Not connected to agent")
        if requested.empty:
            return {}

        async with self._configuration_lock:
            config = dict(self.session.config_json or {})
            previous = dict(config.get("configuration") or {})
            previous_request = SessionConfigurationRequest.from_dict(
                previous.get("requested")
            )
            desired = previous_request.merged(requested) if merge else requested
            requested_dict = desired.as_dict()
            if (
                not force
                and previous.get("state") == "ready"
                and previous.get("requested") == requested_dict
            ):
                return dict(previous.get("effective") or {})

            options = [
                dict(item)
                for item in (_to_plain(self.config_options) or [])
                if isinstance(item, dict)
            ]
            working_options = copy.deepcopy(options)
            working_models = copy.deepcopy(self.models)
            working_modes = copy.deepcopy(self.modes)
            set_config_option = getattr(self._conn, "set_config_option", None)
            set_model = getattr(self._conn, "set_session_model", None)
            set_mode = getattr(self._conn, "set_session_mode", None)
            config_supported = callable(set_config_option)
            session_id = self.session.external_session_id

            actions: list[tuple[str, str, str, str | bool]] = []
            strategies: dict[str, str] = {}
            bound_options: dict[str, tuple[str | bool, str]] = {}

            def bind_option(
                setting: str,
                option: dict[str, Any] | None,
                value: str | bool,
            ) -> None:
                if option is None or not config_supported:
                    advertised = "no matching advertised config option"
                    if option is not None:
                        advertised = "the client has no set_config_option method"
                    raise ACPConfigurationError(
                        "ACP configuration compatibility error: the agent cannot apply "
                        f"requested {setting} {value!r} ({advertised}). Upgrade the ACP "
                        "client/provider or choose an advertised session option."
                    )
                oid = option_id(option)
                assert oid is not None
                validate_option_value(option, value, label=setting)
                existing = bound_options.get(oid)
                if existing and existing[0] != value:
                    raise ACPConfigurationError(
                        "ACP configuration compatibility error: configuration option "
                        f"{oid!r} received conflicting values {existing[0]!r} and {value!r}."
                    )
                if not existing:
                    bound_options[oid] = (value, setting)
                    actions.append((setting, "config", oid, value))
                strategies[setting] = f"config:{oid}"

            def build_plan() -> None:
                if desired.model_id:
                    advertised_models = advertised_state_values(
                        self.models,
                        collection_names=("availableModels", "available_models"),
                        id_names=("modelId", "model_id", "id"),
                    )
                    if self.models is not None and callable(set_model):
                        if (
                            advertised_models
                            and desired.model_id not in advertised_models
                        ):
                            supported = ", ".join(sorted(advertised_models))
                            raise ACPConfigurationError(
                                "ACP configuration compatibility error: requested model "
                                f"{desired.model_id!r} is not advertised by the agent. "
                                f"Supported models: {supported}."
                            )
                        actions.append(
                            ("model", "dedicated", "model", desired.model_id)
                        )
                        strategies["model"] = "dedicated:set_session_model"
                    else:
                        bind_option(
                            "model",
                            find_option(options, "model"),
                            desired.model_id,
                        )

                if desired.mode_id:
                    advertised_modes = advertised_state_values(
                        self.modes,
                        collection_names=("availableModes", "available_modes"),
                        id_names=("id", "modeId", "mode_id"),
                    )
                    if self.modes is not None and callable(set_mode):
                        if advertised_modes and desired.mode_id not in advertised_modes:
                            supported = ", ".join(sorted(advertised_modes))
                            raise ACPConfigurationError(
                                "ACP configuration compatibility error: requested mode "
                                f"{desired.mode_id!r} is not advertised by the agent. "
                                f"Supported modes: {supported}."
                            )
                        actions.append(("mode", "dedicated", "mode", desired.mode_id))
                        strategies["mode"] = "dedicated:set_session_mode"
                    else:
                        bind_option(
                            "mode", find_option(options, "mode"), desired.mode_id
                        )

                if desired.reasoning:
                    bind_option(
                        "reasoning",
                        find_option(options, "reasoning"),
                        desired.reasoning,
                    )

                for config_id, value in sorted(desired.config.items()):
                    bind_option(
                        f"config {config_id!r}",
                        find_option_by_id(options, config_id),
                        value,
                    )

            history = list(previous.get("history") or [])
            if previous:
                history.append(
                    {key: value for key, value in previous.items() if key != "history"}
                )
            history = history[-20:]
            attempt = int(previous.get("attempt") or 0) + 1
            ready_status = self.session.status
            config["configuration"] = {
                "state": "applying",
                "attempt": attempt,
                "requested": requested_dict,
                "strategies": strategies,
                "history": history,
                "started_at": datetime.now(UTC).isoformat(),
            }
            self.session.config_json = config
            self.session.status = "configuring"
            self.session.updated_at = datetime.now(UTC)
            await self._offload(
                "sqlite.agent_session_save", self.store.save_session, self.session
            )

            try:
                build_plan()
                applying_config = dict(self.session.config_json or {})
                applying = dict(applying_config.get("configuration") or {})
                applying["strategies"] = dict(strategies)
                applying_config["configuration"] = applying
                self.session.config_json = applying_config
                await self._offload(
                    "sqlite.agent_session_save", self.store.save_session, self.session
                )
                for setting, strategy, target, value in actions:
                    if strategy == "dedicated" and target == "model":
                        await set_model(model_id=value, session_id=session_id)
                        if isinstance(working_models, dict):
                            if "currentModelId" in working_models:
                                working_models["currentModelId"] = value
                            else:
                                working_models["current_model_id"] = value
                        continue
                    if strategy == "dedicated" and target == "mode":
                        await set_mode(mode_id=value, session_id=session_id)
                        if isinstance(working_modes, dict):
                            if "currentModeId" in working_modes:
                                working_modes["currentModeId"] = value
                            else:
                                working_modes["current_mode_id"] = value
                        continue
                    response = await set_config_option(
                        config_id=target,
                        session_id=session_id,
                        value=value,
                    )
                    plain = _to_plain(response)
                    response_options = (
                        plain.get("configOptions") or plain.get("config_options")
                        if isinstance(plain, dict)
                        else None
                    )
                    if response_options is None:
                        raise ACPConfigurationError(
                            "ACP configuration compatibility error: the agent accepted "
                            f"{setting}, but did not return configuration state to verify it."
                        )
                    verified_options = [
                        dict(item)
                        for item in response_options
                        if isinstance(item, dict)
                    ]
                    verified = find_option_by_id(verified_options, target)
                    effective_value = (
                        option_current_value(verified) if verified is not None else None
                    )
                    if verified is None or effective_value != value:
                        raise ACPConfigurationError(
                            "ACP configuration compatibility error: the agent did not "
                            f"confirm {setting}={value!r}; effective value was "
                            f"{effective_value!r}."
                        )
                    working_options = verified_options

                effective_values = {
                    oid: option_current_value(option)
                    for option in working_options
                    if (oid := option_id(option)) is not None
                    and option_current_value(option) is not None
                }
                effective_model = state_current_value(
                    working_models,
                    ("currentModelId", "current_model_id"),
                )
                effective_mode = state_current_value(
                    working_modes,
                    ("currentModeId", "current_mode_id"),
                )
                model_option = None
                mode_option = None
                reasoning_option = None
                if desired.model_id and strategies.get("model", "").startswith(
                    "config:"
                ):
                    model_option = find_option(working_options, "model")
                    effective_model = (
                        option_current_value(model_option) if model_option else None
                    )
                if desired.mode_id and strategies.get("mode", "").startswith("config:"):
                    mode_option = find_option(working_options, "mode")
                    effective_mode = (
                        option_current_value(mode_option) if mode_option else None
                    )
                if desired.reasoning:
                    reasoning_option = find_option(working_options, "reasoning")
                effective_reasoning = (
                    option_current_value(reasoning_option) if reasoning_option else None
                )
                effective = {
                    "model_id": effective_model,
                    "mode_id": effective_mode,
                    "reasoning": effective_reasoning,
                    "config": effective_values,
                }
                config = dict(self.session.config_json or {})
                config["values"] = effective_values
                config["options"] = working_options
                if working_models is not None:
                    config["models"] = working_models
                if working_modes is not None:
                    config["modes"] = working_modes
                config["configuration"] = {
                    "state": "ready",
                    "attempt": attempt,
                    "requested": requested_dict,
                    "effective": effective,
                    "strategies": strategies,
                    "history": history,
                    "confirmed_at": datetime.now(UTC).isoformat(),
                }
                self.models = working_models
                self.modes = working_modes
                self.config_options = working_options
                if effective_model:
                    self.session.model_id = str(effective_model)
                if effective_mode:
                    self.session.mode_id = str(effective_mode)
                self.session.config_json = config
                self.session.status = (
                    "idle"
                    if ready_status in {"configuration_failed", "disconnected"}
                    else ready_status
                )
                self.session.updated_at = datetime.now(UTC)
                await self._offload(
                    "sqlite.agent_session_save", self.store.save_session, self.session
                )
                return effective
            except Exception as exc:
                message = str(exc)
                if not isinstance(exc, ACPConfigurationError):
                    message = (
                        "ACP configuration compatibility error: the provider failed while "
                        f"applying requested session settings: {exc}"
                    )
                failed_config = dict(self.session.config_json or {})
                failed_config["configuration"] = {
                    "state": "failed",
                    "attempt": attempt,
                    "requested": requested_dict,
                    "strategies": strategies,
                    "history": history,
                    "failed_at": datetime.now(UTC).isoformat(),
                    "error": message[:1000],
                }
                self.session.config_json = failed_config
                self.session.status = "configuration_failed"
                self.session.updated_at = datetime.now(UTC)
                await self._offload(
                    "sqlite.agent_session_save", self.store.save_session, self.session
                )
                if isinstance(exc, ACPConfigurationError):
                    raise
                raise ACPConfigurationError(message) from exc

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
            await self._offload(
                "sqlite.agent_session_save", self.store.save_session, self.session
            )
        await self._drain_wire_logs()


def _agent_supports_resume(init_response: Any) -> bool:
    caps = getattr(init_response, "agent_capabilities", None) or getattr(
        init_response, "agentCapabilities", None
    )
    if caps is None and isinstance(init_response, dict):
        caps = init_response.get("agentCapabilities") or init_response.get(
            "agent_capabilities"
        )
    if caps is None:
        return False
    session_caps = getattr(caps, "session_capabilities", None) or getattr(
        caps, "sessionCapabilities", None
    )
    if session_caps is None and isinstance(caps, dict):
        session_caps = caps.get("sessionCapabilities") or caps.get(
            "session_capabilities"
        )
    if session_caps is None:
        return False
    resume = getattr(session_caps, "resume", None)
    if resume is None and isinstance(session_caps, dict):
        resume = session_caps.get("resume")
    return bool(resume)


def _agent_supports_load(init_response: Any) -> bool:
    caps = getattr(init_response, "agent_capabilities", None) or getattr(
        init_response, "agentCapabilities", None
    )
    if caps is None and isinstance(init_response, dict):
        caps = init_response.get("agentCapabilities") or init_response.get(
            "agent_capabilities"
        )
    if caps is None:
        return False
    load = getattr(caps, "load_session", None)
    if load is None:
        load = getattr(caps, "loadSession", None)
    if load is None and isinstance(caps, dict):
        load = caps.get("loadSession")
        if load is None:
            load = caps.get("load_session")
    return bool(load)


def _format_acp_error(exc: BaseException) -> str:
    data = getattr(exc, "data", None)
    if data is None:
        return str(exc)
    return f"{exc} ({data})"


def _agent_supports_session_list(init_response: Any) -> bool:
    caps = getattr(init_response, "agent_capabilities", None) or getattr(
        init_response, "agentCapabilities", None
    )
    if caps is None and isinstance(init_response, dict):
        caps = init_response.get("agentCapabilities") or init_response.get(
            "agent_capabilities"
        )
    if caps is None:
        return False
    session_caps = getattr(caps, "session_capabilities", None) or getattr(
        caps, "sessionCapabilities", None
    )
    if session_caps is None and isinstance(caps, dict):
        session_caps = caps.get("sessionCapabilities") or caps.get(
            "session_capabilities"
        )
    if session_caps is None:
        return False
    listed = getattr(session_caps, "list", None)
    if listed is None and isinstance(session_caps, dict):
        listed = session_caps.get("list")
    return listed is not None


def _session_info_id(info: Any) -> str | None:
    if info is None:
        return None
    sid = getattr(info, "session_id", None) or getattr(info, "sessionId", None)
    if sid is None and isinstance(info, dict):
        sid = info.get("sessionId") or info.get("session_id")
    return str(sid) if sid else None


def _session_info_cwd(info: Any) -> str | None:
    if info is None:
        return None
    listed_cwd = getattr(info, "cwd", None)
    if listed_cwd is None and isinstance(info, dict):
        listed_cwd = info.get("cwd")
    return str(listed_cwd) if listed_cwd else None


async def _resolve_session_load_target(
    conn: Any,
    *,
    session_id: str,
    cwd: str,
) -> tuple[str, str] | None:
    """Resolve session/load params, or None when the session should not be loaded.

    Cursor returns JSON-RPC Invalid params with ``Session "<id>" not found`` for
    unknown / not-yet-persisted ids. When ``session/list`` is available, only load
    ids that appear there and prefer the listed ``cwd``.
    """
    list_sessions = getattr(conn, "list_sessions", None)
    if list_sessions is None:
        return session_id, cwd
    try:
        listed = await list_sessions()
    except Exception as exc:
        logger.debug(
            "ACP session/list failed (%s); attempting load with cwd=%s",
            _format_acp_error(exc),
            cwd,
        )
        return session_id, cwd

    sessions = getattr(listed, "sessions", None)
    if sessions is None and isinstance(listed, dict):
        sessions = listed.get("sessions")
    if sessions is None:
        return session_id, cwd

    for info in sessions:
        if _session_info_id(info) != session_id:
            continue
        return session_id, _session_info_cwd(info) or cwd

    logger.info(
        "ACP session %s not present in session/list; creating new session",
        session_id,
    )
    return None
