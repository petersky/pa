from __future__ import annotations

import asyncio
import copy
import inspect
import json
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import nullcontext
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any
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

from pa.acp.auxiliary_mcp import load_auxiliary_mcp_state, resolve_auxiliary_mcp_servers
from pa.acp.configuration import (
    ACPConfigurationError,
    SessionConfigurationRequest,
    advertised_state_values,
    find_option,
    find_option_by_id,
    option_current_value,
    option_id,
    parse_model_selector,
    state_current_value,
    validate_option_value,
)
from pa.acp.environment import (
    inject_agent_github_environment,
    sanitize_provider_environment,
)
from pa.acp.final_message import normalize_provider_phase
from pa.acp.mcp_config import (
    McpHandshakeError,
    OwnerChannelError,
    apply_codex_owner_sandbox_environment,
    owner_sandbox_directories,
    pa_mcp_servers,
    probe_owner_channel,
    probe_pa_mcp_stdio,
)
from pa.acp.providers.base import AgentProviderSpec
from pa.acp.providers.registry import DEFAULT_PROVIDER_ID, get_provider
from pa.acp.providers.resolve import _spawn_overrides
from pa.acp.sandbox_health import sandbox_health_registry
from pa.acp.startup_trace import SessionStartupTrace
from pa.acp.transport import spawn_agent
from pa.config import Settings
from pa.core.logging import redact_log_text
from pa.domain.models import AgentSession
from pa.domain.store import Store
from pa.instance.quiesce import ImageAttachment
from pa.knowledge.capture import has_policy_memory_candidate
from pa.packaging.paths import build_service_path, resolve_executable

if TYPE_CHECKING:
    from pa.core.async_runtime import AsyncRuntime

logger = logging.getLogger(__name__)

UpdateHandler = Callable[[str, Any], Awaitable[None] | None]
PermissionHandler = Callable[
    [str, dict[str, Any]], Awaitable[RequestPermissionResponse | dict[str, Any]]
]
ElicitationHandler = Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]]
WireLogger = Callable[[str, dict[str, Any]], None]

# Cursor ACP sends vendor client methods (e.g. cursor/update_todos) without the
# ACP `_` extension prefix. Treat those as optional acknowledgements.
_TOLERATED_CLIENT_METHOD_PREFIXES = ("cursor/", "elicitation/")

# Built-in providers receive PA MCP through session/new mcpServers and spawn
# their own child. A second pre-session stdio handshake is duplicate work and
# cannot prove that the provider sandbox can reach the owner socket. Keep this
# set explicit so a future provider still gets the admission probe by default.
_DELEGATED_PA_MCP_STDIO_PROBE_PROVIDERS = frozenset(
    {"codex", "cursor", "openinterpreter"}
)


def _tolerated_client_method(method: str) -> bool:
    if not isinstance(method, str) or not method:
        return False
    name = method.removeprefix("_")
    return name.startswith(_TOLERATED_CLIENT_METHOD_PREFIXES)


def _is_hard_mcp_startup_failure(detail: str) -> bool:
    """Return True for Codex MCP handshake failures, not cancelled races."""
    text = (detail or "").lower()
    if not text.strip():
        return False
    if "startup was cancelled" in text and "failed to start" not in text:
        return False
    return True


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


_THOUGHT_UPDATE_TYPES = {
    "agent_thought_chunk",
    "agent_thought",
    "thought",
    "thought_chunk",
    "reasoning",
    "reasoning_chunk",
}
_PROVIDER_OUTPUT_UPDATE_TYPES = {
    "agent_message_chunk",
    "agent_message",
    "agent_message_final",
    "agent_thought_chunk",
    "tool_call",
    "tool_call_update",
    "plan",
}


def normalize_session_update(update: Any) -> dict[str, Any]:
    """Normalize an ACP session update into a typed event payload."""
    plain = _to_plain(update)
    update_type = _session_update_type(update)
    if update_type in _THOUGHT_UPDATE_TYPES:
        update_type = "agent_thought_chunk"
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
            payload["phase"] = normalize_provider_phase(
                codex_meta.get("phase") if isinstance(codex_meta, dict) else None
            )
            payload["content_mode"] = (
                plain.get("contentMode")
                or plain.get("content_mode")
                or plain.get("operation")
                or (
                    codex_meta.get("contentMode")
                    or codex_meta.get("content_mode")
                    or codex_meta.get("operation")
                    if isinstance(codex_meta, dict)
                    else None
                )
            )
            payload["final"] = bool(
                plain.get("final")
                or plain.get("isFinal")
                or plain.get("is_final")
                or (
                    codex_meta.get("final") or codex_meta.get("is_final")
                    if isinstance(codex_meta, dict)
                    else False
                )
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
        elif update_type in {"config_option_update", "config_options_update"}:
            payload["config_options"] = plain.get("configOptions") or plain.get(
                "config_options"
            )
        elif update_type == "available_commands_update":
            # ACP defines this as a complete replacement snapshot. Preserve the
            # raw provider records so PA can retain future action metadata.
            payload["available_commands"] = plain.get("availableCommands") or plain.get(
                "available_commands"
            ) or []

    return payload


def has_provider_turn_output(updates: list[Any]) -> bool:
    """Return whether a turn emitted response, thought, tool, or plan output."""
    return any(
        str(normalize_session_update(update).get("type") or "")
        in _PROVIDER_OUTPUT_UPDATE_TYPES
        for update in updates
    )


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
        on_elicitation: ElicitationHandler | None = None,
        wire_logger: WireLogger | None = None,
        auto_approve: bool = False,
        async_runtime: AsyncRuntime | None = None,
    ) -> None:
        self.store = store
        self.on_update = on_update
        self.on_permission = on_permission
        self.on_elicitation = on_elicitation
        self.wire_logger = wire_logger
        self.auto_approve = auto_approve
        self.async_runtime = async_runtime
        self._updates: list[Any] = []
        self._mcp_startup_failures: dict[str, str] = {}
        self._mcp_startup_successes: set[str] = set()
        self._mcp_startup_events: dict[str, asyncio.Event] = {}

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
                name = method.removeprefix("_")
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
        normalized = normalize_session_update(update)
        if (
            normalized.get("type") == "tool_call"
            and normalized.get("tool_call_id") == "mcp_startup.pa"
        ):
            key = str(session_id)
            status = str(normalized.get("status") or "").lower()
            if status == "failed":
                detail = redact_log_text(
                    json.dumps(normalized.get("content") or [], default=str)
                )
                self._mcp_startup_failures[key] = detail[:1000]
            elif status in {"completed", "success", "succeeded"}:
                self._mcp_startup_successes.add(key)
            event = self._mcp_startup_events.get(key)
            if event is not None:
                event.set()
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

    async def wait_for_pa_mcp_startup_failure(
        self, session_id: str, *, timeout: float
    ) -> str | None:
        """Observe PA MCP startup until success, hard failure, or a deadline.

        Codex ACP forwards both handshake errors and a generic "startup was
        cancelled" terminal state. Recent Codex app-server builds emit that
        cancelled status spuriously during thread/start, including for servers
        that then become ready. Treat cancelled-only reports as non-fatal and
        keep watching for a success or an actual failed-to-start error.
        """
        key = str(session_id)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        event = self._mcp_startup_events.setdefault(key, asyncio.Event())
        ignored_cancellation = False
        try:
            while True:
                if key in self._mcp_startup_successes:
                    return None
                failure = self._mcp_startup_failures.get(key)
                if failure and _is_hard_mcp_startup_failure(failure):
                    return failure
                if failure and not ignored_cancellation:
                    logger.info(
                        "Ignoring Codex PA MCP startup cancellation",
                        extra={"pa_mcp_startup": failure[:200]},
                    )
                    ignored_cancellation = True
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return None
                if event.is_set():
                    event.clear()
                    continue
                try:
                    await asyncio.wait_for(event.wait(), timeout=remaining)
                except TimeoutError:
                    failure = self._mcp_startup_failures.get(key)
                    if failure and _is_hard_mcp_startup_failure(failure):
                        return failure
                    return None
        finally:
            self._mcp_startup_events.pop(key, None)
            self._mcp_startup_failures.pop(key, None)
            self._mcp_startup_successes.discard(key)

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
        """Handle interoperable elicitation extensions; acknowledge other optional calls."""
        name = method.removeprefix("_")
        if name.startswith("elicitation/") and self.on_elicitation:
            session_id = str(
                params.get("sessionId") or params.get("session_id") or ""
            )
            request = {
                **params,
                "request_id": str(
                    params.get("requestId")
                    or params.get("request_id")
                    or params.get("elicitationId")
                    or params.get("elicitation_id")
                    or uuid4()
                ),
                "method": name,
            }
            self._wire("in", {"method": name, "params": request})
            response = await self.on_elicitation(session_id, request)
            self._wire("out", {"method": name, "result": response})
            return response
        self._wire(
            "in",
            {"method": f"_{method}", "params": params, "ignored": True},
        )
        return {}

    async def ext_notification(self, method: str, params: dict[str, Any]) -> None:
        name = method.removeprefix("_")
        if name.endswith("elicitation/cancel") and self.on_elicitation:
            session_id = str(
                params.get("sessionId") or params.get("session_id") or ""
            )
            request = {
                **params,
                "request_id": str(
                    params.get("requestId")
                    or params.get("request_id")
                    or params.get("elicitationId")
                    or params.get("elicitation_id")
                    or uuid4()
                ),
                "method": name,
            }
            self._wire("in", {"method": name, "params": request})
            await self.on_elicitation(session_id, request)
            return
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
        on_elicitation: ElicitationHandler | None = None,
        wire_path: Path | None = None,
        auto_approve: bool = False,
        async_runtime: AsyncRuntime | None = None,
        extra_env: dict[str, str] | None = None,
        mcp_private_env: dict[str, str] | None = None,
        startup_trace: SessionStartupTrace | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self.agent_name = agent_name
        self.provider_spec = provider_spec
        self.on_update = on_update
        self.on_permission = on_permission
        self.on_elicitation = on_elicitation
        self.wire_path = wire_path
        self.auto_approve = auto_approve
        self.async_runtime = async_runtime
        self.extra_env = dict(extra_env or {})
        self.mcp_private_env = dict(mcp_private_env or {})
        self.startup_trace = startup_trace
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
        self.last_memory_candidate: bool = False
        self.pa_mcp_health: dict[str, Any] = {
            "state": "not_probed",
            "last_success": None,
            "last_failure": None,
        }
        self.auxiliary_mcp_provenance: list[dict[str, Any]] = []
        self._wire_queue: deque[tuple[str, dict[str, Any]]] = deque()
        self._wire_queue_limit = 256
        self._wire_task: asyncio.Task[None] | None = None
        self._wire_dropped = 0
        self._wire_drop_report_at = 0.0

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
        if len(self._wire_queue) >= self._wire_queue_limit:
            self._wire_dropped += 1
            now = loop.time()
            if now >= self._wire_drop_report_at:
                logger.warning(
                    "ACP wire-log pressure dropped %s diagnostic record(s); "
                    "further reports are suppressed for 30 seconds",
                    self._wire_dropped,
                )
                self._wire_dropped = 0
                self._wire_drop_report_at = now + 30.0
            return

        self._wire_queue.append((direction, message))
        if self._wire_task and not self._wire_task.done():
            return
        self._wire_task = loop.create_task(
            self._flush_wire_logs(), name="pa-acp-wire-log"
        )

    async def _flush_wire_logs(self) -> None:
        try:
            while self._wire_queue:
                direction, message = self._wire_queue.popleft()
                wire = self._wire
                if wire is None:
                    continue
                try:
                    await self._offload(
                        "acp.wire_append",
                        wire.log,
                        direction,
                        message,
                        timeout=10.0,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001
                    self._wire_dropped += 1
                    loop = asyncio.get_running_loop()
                    now = loop.time()
                    if now >= self._wire_drop_report_at:
                        logger.warning(
                            "ACP wire-log pressure dropped %s diagnostic record(s); "
                            "further reports are suppressed for 30 seconds",
                            self._wire_dropped,
                        )
                        self._wire_dropped = 0
                        self._wire_drop_report_at = now + 30.0
        finally:
            self._wire_task = None

    async def _drain_wire_logs(self) -> None:
        task = self._wire_task
        if task:
            await asyncio.gather(task, return_exceptions=True)
        if self._wire_queue:
            # A cancelled writer may leave buffered diagnostics behind. Give
            # disconnect one final bounded attempt to flush them in order.
            self._wire_task = asyncio.create_task(
                self._flush_wire_logs(), name="pa-acp-wire-log-drain"
            )
            await asyncio.gather(self._wire_task, return_exceptions=True)

    async def _abort_connect_if_shutting_down(self, *, stage: str) -> None:
        from pa.server.shutdown import is_shutting_down

        if not is_shutting_down():
            return
        logger.info(
            "ACP connect aborted during %s because shutdown began",
            stage,
        )
        if self._ctx is not None or self._proc is not None:
            await self.disconnect(timeout=0.5, force=True)
        raise RuntimeError("ACP connect aborted: shutting down")

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
        await self._abort_connect_if_shutting_down(stage="preflight")
        mcp = pa_mcp_servers(
            self.settings,
            session_environment=self.extra_env,
            private_environment=self.mcp_private_env,
        )
        spec = self._resolved_spec()
        provider_id = spec.id or self.agent_name or DEFAULT_PROVIDER_ID
        if provider_id in {"instance", ""}:
            provider_id = DEFAULT_PROVIDER_ID
        auxiliary_state = load_auxiliary_mcp_state(self.settings.data_dir)
        auxiliary, self.auxiliary_mcp_provenance = resolve_auxiliary_mcp_servers(
            auxiliary_state.servers,
            provider=provider_id,
            project_id=project_id,
            card_id=card_id,
        )
        names = [server.name for server in [*mcp, *auxiliary]]
        if len(names) != len(set(names)):
            raise RuntimeError(
                "MCP server name collision in effective session configuration"
            )
        mcp.extend(auxiliary)
        # Codex starts client-provided stdio MCP servers inside its session
        # sandbox.  The private PA owner socket intentionally lives outside the
        # repository, so workspace-write/read-only sessions must admit the
        # socket directory explicitly or Codex cancels the MCP startup before
        # the child can connect.  This grants only the per-instance runtime
        # directory, not PA_DATA_DIR or another mutable PA store.
        mcp_additional_directories: list[str] | None = None
        if provider_id == "codex" and mcp:
            directories = owner_sandbox_directories(self.settings)
            if directories:
                mcp_additional_directories = directories
        if mcp:
            try:
                owner_health = await self._offload(
                    "acp.pa_mcp_owner_probe",
                    probe_owner_channel,
                    self.settings,
                    timeout=5.0,
                )
            except OwnerChannelError as exc:
                self.pa_mcp_health = {
                    "state": "disconnected",
                    "classification": exc.classification,
                    "endpoint_type": exc.endpoint_kind,
                    "last_success": None,
                    "last_failure": datetime.now(UTC).isoformat(),
                    "retry_state": "session_reconnect_required",
                    "recovery": exc.recovery,
                }
                logger.error(
                    "PA MCP owner channel admission failed",
                    extra={"pa_mcp": self.pa_mcp_health},
                )
                raise
            self.pa_mcp_health = {
                **owner_health,
                "server_probe": owner_health,
                "last_success": datetime.now(UTC).isoformat(),
                "last_failure": None,
                "retry_state": "bridge_probe_pending",
            }
            logger.info(
                "PA MCP owner channel verified", extra={"pa_mcp": self.pa_mcp_health}
            )
            if provider_id in _DELEGATED_PA_MCP_STDIO_PROBE_PROVIDERS:
                # Codex, Cursor, and OpenInterpreter launch the supplied stdio
                # MCP server in the actual session. Launching a second child
                # here duplicates the expensive initialize/tools-list handshake
                # and still cannot prove that the provider sandbox can reach
                # the owner socket. The owner-channel probe above still runs.
                delegated_probe = {
                    "state": "delegated",
                    "classification": "provider_context_probe",
                }
                if provider_id == "codex":
                    self.pa_mcp_health.update(
                        state="checking",
                        classification=None,
                        bridge_probe=delegated_probe,
                        retry_state="provider_context_probe_pending",
                    )
                else:
                    self.pa_mcp_health.update(
                        state="connected",
                        classification=None,
                        bridge_probe=delegated_probe,
                        last_success=datetime.now(UTC).isoformat(),
                        last_failure=None,
                        retry_state="connected",
                    )
            else:
                try:
                    bridge_health = await self._offload(
                        "acp.pa_mcp_stdio_probe",
                        partial(
                            probe_pa_mcp_stdio,
                            self.settings,
                            timeout=12.0,
                            session_environment=self.extra_env,
                            private_environment=self.mcp_private_env,
                        ),
                        timeout=15.0,
                    )
                except McpHandshakeError as exc:
                    self.pa_mcp_health = {
                        **self.pa_mcp_health,
                        "state": "disconnected",
                        "classification": f"mcp_{exc.classification}",
                        "bridge_probe": {
                            "state": "disconnected",
                            "classification": exc.classification,
                        },
                        "last_failure": datetime.now(UTC).isoformat(),
                        "retry_state": "session_reconnect_required",
                        "recovery": exc.recovery,
                    }
                    logger.error(
                        "PA MCP stdio bridge admission failed",
                        extra={"pa_mcp": self.pa_mcp_health},
                    )
                    raise
                self.pa_mcp_health.update(
                    state="connected",
                    classification=None,
                    bridge_probe=bridge_health,
                    last_success=datetime.now(UTC).isoformat(),
                    last_failure=None,
                    retry_state="connected",
                )

        if self.wire_path:
            self._wire = await self._offload(
                "acp.wire_init", WireJsonlLogger, self.wire_path, timeout=10.0
            )

        self._client = PAClient(
            self.store,
            on_update=self.on_update,
            on_permission=self.on_permission,
            on_elicitation=self.on_elicitation,
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
        child_env = sanitize_provider_environment(
            os.environ,
            spec.env,
            self.extra_env,
        )
        child_env, _github_auth_source = inject_agent_github_environment(
            child_env, self.settings
        )
        # LaunchAgents often inherit /usr/bin:/bin. Prepend Homebrew and
        # ~/.local/bin so npx/node/codex-acp resolve the same way as a login shell.
        child_env["PATH"] = build_service_path()
        if spec.id == "codex":
            child_env = apply_codex_owner_sandbox_environment(
                child_env, self.settings
            )
        launch_phase = (
            self.startup_trace.phase("provider_launch")
            if self.startup_trace
            else nullcontext()
        )
        with launch_phase:
            self._ctx = spawn_agent(
                self._client,
                command,
                *list(spec.args or []),
                env=child_env,
            )
            try:
                self._conn, self._proc = await self._ctx.__aenter__()
            except Exception as exc:
                health = sandbox_health_registry.failure(
                    spec.id,
                    "workspace-write",
                    exc,
                    metadata={"stage": "provider_spawn", "session_level": True},
                )
                logger.error(
                    "ACP provider sandbox admission failed",
                    extra={"sandbox_health": health},
                )
                raise
        await self._abort_connect_if_shutting_down(stage="initialize")
        initialize_phase = (
            self.startup_trace.phase("provider_initialize")
            if self.startup_trace
            else nullcontext()
        )
        with initialize_phase:
            self._init_response = await self._conn.initialize(
                protocol_version=PROTOCOL_VERSION,
                client_capabilities=ClientCapabilities(
                    fs=FileSystemCapabilities(
                        read_text_file=True,
                        write_text_file=True,
                    )
                ),
            )
        await self._abort_connect_if_shutting_down(stage="post-initialize")
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
        restored = False
        session_meta: dict[str, Any] = {}
        restore_methods = []
        if self._resume_supported:
            restore_methods.append("session/resume")
        if self._load_supported:
            restore_methods.append("session/load")
        session_phase = (
            self.startup_trace.phase("session_creation")
            if self.startup_trace
            else nullcontext()
        )
        with session_phase:
            if resume_external_id:
                for restore_method in restore_methods:
                    load_cwd = session_cwd
                    if restore_method == "session/load" and self._list_supported:
                        resolved = await _resolve_session_load_target(
                            self._conn,
                            session_id=resume_external_id,
                            cwd=session_cwd,
                        )
                        if resolved is None:
                            continue
                        resume_external_id, load_cwd = resolved
                        self.session_cwd = load_cwd
                    await self._abort_connect_if_shutting_down(stage=restore_method)
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
                        restore_kwargs: dict[str, Any] = {
                            "cwd": load_cwd,
                            "session_id": resume_external_id,
                            "mcp_servers": mcp,
                        }
                        if mcp_additional_directories:
                            restore_kwargs["additional_directories"] = (
                                mcp_additional_directories
                            )
                        restore_resp = await restore(**restore_kwargs)
                        session_meta = extract_models_modes_config(restore_resp)
                        restored = True
                        break
                    except Exception as exc:
                        logger.warning(
                            "ACP %s failed (%s); trying the next restore method",
                            restore_method,
                            _format_acp_error(exc),
                        )

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
                # Missing session/list entries fall back to session/new. Never do that
                # while the host is dying — Cursor often omits brand-new unprompted
                # sessions from the next process's session/list, which cascades into
                # more orphan creates on the following boot.
                await self._abort_connect_if_shutting_down(stage="session/new")
                new_session_kwargs: dict[str, Any] = {
                    "cwd": session_cwd,
                    "mcp_servers": mcp,
                }
                if mcp_additional_directories:
                    new_session_kwargs["additional_directories"] = (
                        mcp_additional_directories
                    )
                acp_session = await self._conn.new_session(**new_session_kwargs)
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
        if mcp and spec.id == "codex" and self.session.external_session_id:
            provider_failure = await self._client.wait_for_pa_mcp_startup_failure(
                self.session.external_session_id,
                timeout=2.0,
            )
            if provider_failure:
                self.pa_mcp_health.update(
                    state="disconnected",
                    classification="mcp_provider_context_startup_failed",
                    bridge_probe={
                        "state": "disconnected",
                        "classification": "provider_context_startup_failed",
                    },
                    last_failure=datetime.now(UTC).isoformat(),
                    retry_state="session_reconnect_required",
                )
                raise McpHandshakeError(
                    "provider_context_startup_failed",
                    "Correct the provider sandbox/mode or owner endpoint, then reconnect the session.",
                    provider_failure,
                )
            self.pa_mcp_health["provider_context_probe"] = {
                "state": "usable",
                "classification": "no_startup_failure",
            }
            self.pa_mcp_health.update(
                state="connected",
                classification=None,
                last_success=datetime.now(UTC).isoformat(),
                last_failure=None,
                retry_state="connected",
            )

        sandbox_health_registry.success(
            spec.id,
            "workspace-write",
            metadata={"stage": "session_admitted", "session_level": True},
        )
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
        config = dict(self.session.config_json or {})
        config["auxiliary_mcp"] = {
            "policy": "current instance configuration is reapplied on resume",
            "effective": self.auxiliary_mcp_provenance,
            "applied_at": datetime.now(UTC).isoformat(),
        }
        self.session.config_json = config
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
            (self.session.config_json or {}).get("configuration") or {}
        )
        if configuration.get("state") in {"applying", "failed"}:
            raise ACPConfigurationError(
                "ACP session configuration is not confirmed; the prompt was not delivered. "
                "Retry session admission after resolving the provider compatibility error."
            )
        self.last_usage = None

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
        # Session initialization/configuration updates are not evidence that this
        # prompt produced output. Start the turn's validation window explicitly.
        if self._client:
            self._client.drain_updates()
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

        # Session updates remain durable audit history. Memory creation is a
        # separate, explicit promotion action over that canonical transcript.
        updates = self._client.drain_updates() if self._client else []
        self.last_memory_candidate = bool(
            getattr(self.settings, "memory_auto_capture_enabled", False) is True
            and has_policy_memory_candidate(updates)
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

        if (
            self.agent_name == "openinterpreter"
            and stop_reason == "end_turn"
            and not usage
            and not has_provider_turn_output(updates)
        ):
            from pa.acp.errors import ProviderTurnError

            await self._mark_transport_dead()
            raise ProviderTurnError(
                {
                    "code": "empty_provider_turn",
                    "message": (
                        "OpenInterpreter ended the turn without a response, thought, "
                        "tool event, or usage record. The provider session was "
                        "disconnected so it can be retried safely after checking the "
                        "MiniMax model-provider configuration and credential."
                    ),
                    "recoverable": True,
                    "stage": "prompt",
                    "provider": self.agent_name,
                    "stop_reason": stop_reason,
                }
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
            combined_model_selector: str | None = None

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
                    if option_current_value(option) == value:
                        strategies[setting] = f"config:{oid}:unchanged"
                        return
                    actions.append((setting, "config", oid, value))
                strategies[setting] = f"config:{oid}"

            def build_plan() -> None:
                nonlocal combined_model_selector
                if desired.model_id:
                    advertised_models = advertised_state_values(
                        self.models,
                        collection_names=("availableModels", "available_models"),
                        id_names=("modelId", "model_id", "id"),
                    )
                    if self.models is not None and callable(set_model):
                        provider_model_id = desired.model_id
                        combined = (
                            f"{desired.model_id}[{desired.reasoning}]"
                            if desired.reasoning
                            else None
                        )
                        if (
                            advertised_models
                            and combined in advertised_models
                        ):
                            provider_model_id = combined
                            combined_model_selector = combined
                        elif (
                            advertised_models
                            and provider_model_id not in advertised_models
                        ):
                            supported = ", ".join(sorted(advertised_models))
                            raise ACPConfigurationError(
                                "ACP configuration compatibility error: requested model "
                                f"{desired.model_id!r} is not advertised by the agent. "
                                f"Supported models: {supported}."
                            )
                        actions.append(
                            ("model", "dedicated", "model", provider_model_id)
                        )
                        strategies["model"] = (
                            "dedicated:set_session_model:combined"
                            if combined_model_selector
                            else "dedicated:set_session_model"
                        )
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

                if desired.reasoning and not combined_model_selector:
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
                        hint = ""
                        if setting == "reasoning":
                            hint = (
                                " This model may use fixed thinking and ignore effort "
                                "controls. Choose Provider default, or a model that "
                                "advertises this setting."
                            )
                        raise ACPConfigurationError(
                            "ACP configuration compatibility error: the agent did not "
                            f"confirm {setting}={value!r}; effective value was "
                            f"{effective_value!r}.{hint}"
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
                if combined_model_selector and effective_model == combined_model_selector:
                    effective_model, effective_reasoning = parse_model_selector(
                        effective_model
                    )
                effective = {
                    "model_id": effective_model,
                    "mode_id": effective_mode,
                    "reasoning": effective_reasoning,
                    "model_provider": desired.model_provider,
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

    async def disconnect(self, *, timeout: float = 5.0, force: bool = False) -> None:
        async with self._disconnect_lock:
            ctx = self._ctx
            proc = self._proc
            self._ctx = None
            self._conn = None
            self._proc = None
            if ctx:
                if force and proc and getattr(proc, "returncode", None) is None:
                    try:
                        proc.kill()
                    except ProcessLookupError, OSError:
                        pass
                try:
                    await asyncio.wait_for(
                        ctx.__aexit__(None, None, None), timeout=max(0.1, timeout)
                    )
                except TimeoutError:
                    if proc and getattr(proc, "returncode", None) is None:
                        try:
                            proc.kill()
                        except ProcessLookupError, OSError:
                            pass
                        try:
                            await asyncio.wait_for(proc.wait(), timeout=0.5)
                        except TimeoutError, ProcessLookupError:
                            logger.error("ACP child did not exit after forced kill")
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
    from pa.acp.errors import format_acp_error

    return format_acp_error(exc)


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
