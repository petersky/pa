"""ACP agent provider contracts."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, Field


class AgentProviderId(StrEnum):
    CURSOR = "cursor"
    CODEX = "codex"


class AgentProviderSpec(BaseModel):
    """Resolved spawn + metadata for an ACP server."""

    id: str
    display_name: str
    command: str
    args: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    docs_key: str = ""
    install_method: str = "path"  # path | npm | npx
    npm_package: str | None = None
    prefer_new_session_on_resume_failure: bool = True
    capability_notes: str = ""


class ProviderStatus(BaseModel):
    id: str
    display_name: str
    installed: bool = False
    available: bool = False
    command: str | None = None
    resolved_path: str | None = None
    version: str | None = None
    auth_configured: bool = False
    auth_method: str = "none"
    auth_status: str | None = None
    auth_error: str | None = None
    login_in_progress: bool = False
    codex_cli_installed: bool | None = None
    codex_cli_path: str | None = None
    codex_cli_version: str | None = None
    install_method: str | None = None
    last_probe: dict[str, Any] | None = None
    error: str | None = None
    meta: dict[str, Any] = Field(default_factory=dict)


class ProviderInstallResult(BaseModel):
    id: str
    ok: bool
    message: str
    version: str | None = None
    command: str | None = None
    detail: dict[str, Any] = Field(default_factory=dict)


class ProviderConfigureBody(BaseModel):
    """Non-secret config + optional secret fields written to local credential file."""

    env: dict[str, str] = Field(default_factory=dict)
    secrets: dict[str, str] = Field(default_factory=dict)
    no_browser: bool | None = None
    codex_path: str | None = None
    initial_agent_mode: str | None = None


@runtime_checkable
class AgentProvider(Protocol):
    @property
    def id(self) -> str: ...

    @property
    def display_name(self) -> str: ...

    def default_spec(self) -> AgentProviderSpec: ...

    def resolve_spawn(
        self,
        *,
        command_override: str | None = None,
        args_override: list[str] | None = None,
        extra_env: dict[str, str] | None = None,
        data_dir: Path | None = None,
    ) -> AgentProviderSpec: ...

    def status(self, data_dir: Path) -> ProviderStatus: ...

    def install(self, data_dir: Path) -> ProviderInstallResult: ...

    def update(self, data_dir: Path) -> ProviderInstallResult: ...

    def configure(
        self, data_dir: Path, body: ProviderConfigureBody
    ) -> ProviderStatus: ...

    def probe(self, data_dir: Path) -> dict[str, Any]: ...
