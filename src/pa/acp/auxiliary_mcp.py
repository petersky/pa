"""Per-instance auxiliary MCP definitions, validation, resolution, and probing."""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import tempfile
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from acp.schema import EnvVariable, McpServerStdio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from pa.core.io import atomic_write_json

_NAME = re.compile(r"^[a-z][a-z0-9_-]{0,62}$")
_RESERVED = {"pa", "pa-mcp", "pa_mcp", "personal-assistant"}


class AuxiliaryMcpApplicability(BaseModel):
    providers: list[str] = Field(default_factory=list)
    projects: list[str] = Field(default_factory=list)
    cards: list[str] = Field(default_factory=list)
    session_profiles: list[str] = Field(default_factory=list)


class AuxiliaryMcpServer(BaseModel):
    """Persisted non-secret definition. ``env`` values name credential variables."""

    model_config = ConfigDict(extra="forbid")

    name: str
    enabled: bool = True
    command: str
    args: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    cwd: str | None = None
    startup_timeout_seconds: float = Field(default=15, gt=0, le=300)
    max_restarts: int = Field(default=0, ge=0, le=5)
    required: bool = False
    applicability: AuxiliaryMcpApplicability = Field(
        default_factory=AuxiliaryMcpApplicability
    )

    @field_validator("name")
    @classmethod
    def valid_name(cls, value: str) -> str:
        value = value.strip().lower()
        if not _NAME.fullmatch(value):
            raise ValueError("server name must match [a-z][a-z0-9_-]{0,62}")
        if value in _RESERVED or value.startswith(("pa_", "pa-")):
            raise ValueError(f"server name {value!r} is reserved for PA")
        return value

    @field_validator("command")
    @classmethod
    def valid_command(cls, value: str) -> str:
        value = value.strip()
        if not value or "\x00" in value or "\n" in value:
            raise ValueError("command must be a non-empty executable name or path")
        return value

    @field_validator("args")
    @classmethod
    def valid_args(cls, value: list[str]) -> list[str]:
        if len(value) > 128 or any("\x00" in item for item in value):
            raise ValueError("arguments contain invalid values or exceed 128 entries")
        return value

    @field_validator("env")
    @classmethod
    def valid_env_refs(cls, value: dict[str, str]) -> dict[str, str]:
        pattern = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
        for target, reference in value.items():
            if not pattern.fullmatch(target) or not pattern.fullmatch(reference):
                raise ValueError(
                    "environment keys and references must be variable names"
                )
        return value

    @field_validator("cwd")
    @classmethod
    def valid_cwd(cls, value: str | None) -> str | None:
        if value is None:
            return None
        path = Path(value).expanduser()
        if not path.is_absolute():
            raise ValueError("working directory must be an absolute local path")
        return str(path)


class AuxiliaryMcpCollection(BaseModel):
    servers: list[AuxiliaryMcpServer] = Field(default_factory=list)

    @model_validator(mode="after")
    def unique_names(self) -> AuxiliaryMcpCollection:
        names = [server.name for server in self.servers]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(
                f"duplicate auxiliary MCP server names: {', '.join(duplicates)}"
            )
        return self


class AuxiliaryMcpState(BaseModel):
    version: int = 1
    servers: list[AuxiliaryMcpServer] = Field(default_factory=list)
    mutations: list[dict[str, Any]] = Field(default_factory=list)
    idempotency: dict[str, str] = Field(default_factory=dict)


def load_auxiliary_mcp_state(data_dir: Path) -> AuxiliaryMcpState:
    path = data_dir / "auxiliary_mcp.json"
    try:
        return AuxiliaryMcpState.model_validate(json.loads(path.read_text()))
    except OSError, ValueError:
        return AuxiliaryMcpState()


def save_auxiliary_mcp_state(data_dir: Path, state: AuxiliaryMcpState) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_json(
        data_dir / "auxiliary_mcp.json", state.model_dump(mode="json"), mode=0o600
    )


def import_common_mcp_json(payload: dict[str, Any]) -> AuxiliaryMcpCollection:
    raw = payload.get("mcpServers")
    if not isinstance(raw, dict):
        raise TypeError("JSON import requires an mcpServers object")
    servers = []
    for name, definition in raw.items():
        if not isinstance(definition, dict):
            raise TypeError(f"server {name!r} must be an object")
        env = definition.get("env") or {}
        # Common configs contain secret values. Import only their variable names;
        # operators must place values in the protected service environment.
        servers.append(
            AuxiliaryMcpServer(
                name=name,
                enabled=definition.get("enabled", True),
                command=definition.get("command", ""),
                args=definition.get("args") or [],
                env={key: key for key in env},
                cwd=definition.get("cwd"),
                startup_timeout_seconds=definition.get("startupTimeout", 15),
                required=definition.get("required", False),
            )
        )
    return AuxiliaryMcpCollection(servers=servers)


def applies(
    server: AuxiliaryMcpServer,
    *,
    provider: str | None = None,
    project_id: str | None = None,
    card_id: str | None = None,
    session_profile: str | None = None,
) -> bool:
    app = server.applicability
    checks = (
        (app.providers, provider),
        (app.projects, project_id),
        (app.cards, card_id),
        (app.session_profiles, session_profile),
    )
    return all(
        not allowed or bool(value and value in allowed) for allowed, value in checks
    )


def resolve_auxiliary_mcp_servers(
    definitions: list[AuxiliaryMcpServer | dict[str, Any]],
    *,
    environment: Mapping[str, str] | None = None,
    provider: str | None = None,
    project_id: str | None = None,
    card_id: str | None = None,
    session_profile: str | None = None,
) -> tuple[list[McpServerStdio], list[dict[str, Any]]]:
    environment = os.environ if environment is None else environment
    collection = AuxiliaryMcpCollection(
        servers=[AuxiliaryMcpServer.model_validate(item) for item in definitions]
    )
    resolved: list[McpServerStdio] = []
    provenance: list[dict[str, Any]] = []
    for definition in collection.servers:
        if not definition.enabled or not applies(
            definition,
            provider=provider,
            project_id=project_id,
            card_id=card_id,
            session_profile=session_profile,
        ):
            continue
        executable = shutil.which(definition.command)
        if not executable and Path(definition.command).is_absolute():
            candidate = Path(definition.command)
            executable = (
                str(candidate)
                if candidate.is_file() and os.access(candidate, os.X_OK)
                else None
            )
        missing_env = [
            ref for ref in definition.env.values() if not environment.get(ref)
        ]
        cwd_ok = definition.cwd is None or Path(definition.cwd).is_dir()
        ready = bool(executable and not missing_env and cwd_ok)
        state = "ready" if ready else "unavailable"
        error = None
        if not executable:
            error = "command_not_found"
        elif missing_env:
            error = "credential_reference_unavailable"
        elif not cwd_ok:
            error = "working_directory_unavailable"
        provenance.append(
            {
                "name": definition.name,
                "state": state,
                "required": definition.required,
                "command": definition.command,
                "resolved_command": executable,
                "args": list(definition.args),
                "cwd": definition.cwd,
                "env_references": sorted(definition.env.values()),
                "error": error,
                "source": "instance",
            }
        )
        if not ready:
            if definition.required:
                raise RuntimeError(
                    f"required auxiliary MCP server {definition.name!r} is unavailable: {error}"
                )
            continue
        env = [
            EnvVariable(name=target, value=environment[reference])
            for target, reference in definition.env.items()
        ]
        # ACP's stdio model currently has no cwd field. Use a tiny, explicit
        # launcher only when requested; no shell parsing is involved.
        command = executable
        args = list(definition.args)
        if definition.cwd:
            command = shutil.which("env") or "/usr/bin/env"
            args = ["-C", definition.cwd, executable, *args]
        resolved.append(
            McpServerStdio(name=definition.name, command=command, args=args, env=env)
        )
    return resolved, provenance


async def probe_auxiliary_server(
    definition: AuxiliaryMcpServer,
    *,
    environment: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    started_at = datetime.now(UTC)
    servers, provenance = resolve_auxiliary_mcp_servers(
        [definition], environment=environment
    )
    if not servers:
        return {**provenance[0], "last_probe": started_at.isoformat(), "tool_count": 0}
    server = servers[0]
    params = StdioServerParameters(
        command=server.command,
        args=list(server.args),
        env={item.name: item.value for item in server.env},
    )
    stage = "spawn"
    with tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as stderr:
        try:
            async with asyncio.timeout(definition.startup_timeout_seconds):
                async with stdio_client(params, errlog=stderr) as (read, write):
                    stage = "initialize"
                    async with ClientSession(read, write) as session:
                        await session.initialize()
                        stage = "tools_list"
                        tools = await session.list_tools()
            return {
                **provenance[0],
                "state": "ready",
                "last_probe": started_at.isoformat(),
                "tool_count": len(tools.tools),
                "tool_names": sorted(tool.name for tool in tools.tools),
            }
        except Exception as exc:
            stderr.seek(0)
            # Probe diagnostics never include the child environment. Remove
            # common credential-shaped assignments from subprocess errors too.
            detail = str(exc).strip() or stderr.read().strip()
            detail = re.sub(
                r"(?i)(token|secret|password|api[_-]?key)=\S+", r"\1=[REDACTED]", detail
            )[:500]
            return {
                **provenance[0],
                "state": "unavailable",
                "error": "startup_timeout"
                if isinstance(exc, TimeoutError)
                else f"{stage}_failed",
                "detail": detail,
                "last_probe": started_at.isoformat(),
                "tool_count": 0,
            }
