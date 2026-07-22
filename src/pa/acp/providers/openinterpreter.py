"""OpenInterpreter ACP provider (``interpreter acp``)."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from pa.acp.providers.base import (
    AgentProviderId,
    AgentProviderSpec,
    ProviderConfigureBody,
    ProviderInstallResult,
    ProviderStatus,
)
from pa.acp.providers.metadata import (
    ProviderMetadata,
    load_credentials,
    load_metadata,
    merge_provider_env,
    save_credentials,
    save_metadata,
)
from pa.core.io import atomic_write_text
from pa.packaging.paths import resolve_executable

_DEFAULT_COMMAND = "interpreter"
_DEFAULT_ARGS = ["acp"]
_INSTALL_URL = "https://www.openinterpreter.com/install"
_MAX_INSTALLER_BYTES = 4 * 1024 * 1024
_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")
_ENV_KEY = re.compile(r"^[A-Z_][A-Z0-9_]*$")
_WIRE_APIS = {"responses", "chat", "messages"}
_NO_AUTH_PROVIDERS = {"ollama", "lmstudio"}


class OpenInterpreterProvider:
    id = AgentProviderId.OPENINTERPRETER.value
    display_name = "OpenInterpreter"

    def default_spec(self) -> AgentProviderSpec:
        return AgentProviderSpec(
            id=self.id,
            display_name=self.display_name,
            command=_DEFAULT_COMMAND,
            args=list(_DEFAULT_ARGS),
            docs_key="openinterpreter",
            install_method="official",
            capability_notes=(
                "OpenInterpreter ACP server. See docs/acp/openinterpreter.md."
            ),
        )

    def resolve_spawn(
        self,
        *,
        command_override: str | None = None,
        args_override: list[str] | None = None,
        extra_env: dict[str, str] | None = None,
        data_dir: Path | None = None,
    ) -> AgentProviderSpec:
        spec = self.default_spec()
        resolved = resolve_executable(_DEFAULT_COMMAND) or shutil.which(
            _DEFAULT_COMMAND
        )
        spec.command = command_override or (str(resolved) if resolved else spec.command)
        if args_override is not None:
            spec.args = list(args_override)

        env: dict[str, str] = {}
        if data_dir is not None:
            env.update(merge_provider_env(data_dir, self.id))
        if extra_env:
            env.update(extra_env)
        if data_dir is not None:
            env["INTERPRETER_HOME"] = str(_managed_home(data_dir))
        spec.env = env
        return spec

    def status(self, data_dir: Path) -> ProviderStatus:
        spec = self.resolve_spawn(data_dir=data_dir)
        resolved = resolve_executable(_DEFAULT_COMMAND) or shutil.which(
            _DEFAULT_COMMAND
        )
        meta = load_metadata(data_dir, self.id)
        creds = load_credentials(data_dir, self.id)
        configuration = dict(meta.configuration) if meta else {}
        model_provider = str(configuration.get("model_provider") or "").strip()
        no_auth = model_provider in _NO_AUTH_PROVIDERS
        auth_configured = bool(creds) or no_auth
        if creds:
            auth_status = "Model provider credential configured on this host."
        elif no_auth:
            auth_status = f"{model_provider} does not require a stored API key."
        else:
            auth_status = "No model provider credential stored by PA."
        return ProviderStatus(
            id=self.id,
            display_name=self.display_name,
            installed=bool(resolved),
            available=bool(resolved),
            command=spec.command,
            resolved_path=str(resolved) if resolved else None,
            version=_version(str(resolved))
            if resolved
            else (meta.version if meta else None),
            auth_configured=auth_configured,
            auth_method=model_provider or ("environment" if creds else "none"),
            auth_status=auth_status,
            install_method=meta.install_method if meta else "official",
            last_probe=meta.last_probe if meta else None,
            meta={
                "args": spec.args,
                "interpreter_home": spec.env.get("INTERPRETER_HOME"),
                "config_path": str(_config_path(data_dir)),
                "configuration": configuration,
                "credential_keys": sorted(creds),
                "install_url": _INSTALL_URL,
            },
        )

    def install(self, data_dir: Path) -> ProviderInstallResult:
        existing = resolve_executable(_DEFAULT_COMMAND) or shutil.which(
            _DEFAULT_COMMAND
        )
        if existing:
            return self._record_install(data_dir, Path(existing), "already installed")
        try:
            proc = _run_official_installer(_managed_home(data_dir))
        except (OSError, subprocess.TimeoutExpired, ValueError) as exc:
            return ProviderInstallResult(
                id=self.id,
                ok=False,
                message=f"OpenInterpreter install failed: {exc}",
                command=_DEFAULT_COMMAND,
            )
        resolved = resolve_executable(_DEFAULT_COMMAND) or shutil.which(
            _DEFAULT_COMMAND
        )
        if proc.returncode != 0 or not resolved:
            detail = _output_tail(proc)
            message = (
                f"OpenInterpreter installer exited {proc.returncode}: {detail}"
                if proc.returncode != 0
                else "Installer completed but `interpreter` is not on the PA service PATH"
            )
            return ProviderInstallResult(
                id=self.id,
                ok=False,
                message=message,
                command=_DEFAULT_COMMAND,
                detail={"output_tail": detail},
            )
        return self._record_install(data_dir, Path(resolved), "installed")

    def _record_install(
        self, data_dir: Path, resolved: Path, action: str
    ) -> ProviderInstallResult:
        version = _version(str(resolved))
        current = load_metadata(data_dir, self.id) or ProviderMetadata(
            provider_id=self.id
        )
        current.install_method = "official"
        current.version = version
        current.command = str(resolved)
        save_metadata(data_dir, current)
        return ProviderInstallResult(
            id=self.id,
            ok=True,
            message=f"OpenInterpreter {action} at {resolved}",
            version=version,
            command=str(resolved),
        )

    def update(self, data_dir: Path) -> ProviderInstallResult:
        resolved = resolve_executable(_DEFAULT_COMMAND) or shutil.which(
            _DEFAULT_COMMAND
        )
        if not resolved:
            return self.install(data_dir)
        spec = self.resolve_spawn(data_dir=data_dir)
        try:
            proc = subprocess.run(
                [str(resolved), "update"],
                capture_output=True,
                text=True,
                timeout=600,
                check=False,
                env={**os.environ, **spec.env},
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return ProviderInstallResult(
                id=self.id, ok=False, message=str(exc), command=str(resolved)
            )
        if proc.returncode == 0:
            return self._record_install(data_dir, Path(resolved), "updated")
        return ProviderInstallResult(
            id=self.id,
            ok=False,
            message=f"OpenInterpreter update failed: {_output_tail(proc)}",
            command=str(resolved),
        )

    def configure(self, data_dir: Path, body: ProviderConfigureBody) -> ProviderStatus:
        meta = load_metadata(data_dir, self.id) or ProviderMetadata(provider_id=self.id)
        env = dict(meta.env)
        if "INTERPRETER_HOME" in body.env:
            raise ValueError("INTERPRETER_HOME is managed by PA for this provider")
        env.update(body.env)
        configuration = dict(meta.configuration)
        for key in (
            "model",
            "model_provider",
            "model_provider_name",
            "model_provider_base_url",
            "model_provider_env_key",
            "model_provider_wire_api",
        ):
            value = getattr(body, key)
            if value is None:
                continue
            normalized = value.strip()
            if normalized:
                configuration[key] = normalized
            else:
                configuration.pop(key, None)
        _validate_configuration(configuration)

        config_path = _config_path(data_dir)
        _write_managed_config(config_path, configuration)
        meta.env = env
        meta.configuration = configuration
        meta.configured = True
        meta.install_method = meta.install_method or "official"
        save_metadata(data_dir, meta)
        if body.secrets:
            save_credentials(data_dir, self.id, body.secrets)
        return self.status(data_dir)

    def probe(self, data_dir: Path) -> dict[str, Any]:
        from datetime import UTC, datetime

        from pa.acp.providers.probe import probe_acp_initialize

        result = probe_acp_initialize(self.resolve_spawn(data_dir=data_dir))
        meta = load_metadata(data_dir, self.id) or ProviderMetadata(provider_id=self.id)
        meta.last_probe = result
        meta.last_probe_at = datetime.now(UTC).isoformat()
        save_metadata(data_dir, meta)
        return result


def _managed_home(data_dir: Path) -> Path:
    return data_dir / "agent_providers" / "openinterpreter" / "home"


def _config_path(data_dir: Path) -> Path:
    return _managed_home(data_dir) / "config.toml"


def _validate_configuration(configuration: dict[str, Any]) -> None:
    provider = str(configuration.get("model_provider") or "")
    if provider and not _IDENTIFIER.fullmatch(provider):
        raise ValueError(
            "model_provider must start with a letter and contain only letters, "
            "numbers, underscores, or hyphens"
        )
    env_key = str(configuration.get("model_provider_env_key") or "")
    if env_key and not _ENV_KEY.fullmatch(env_key):
        raise ValueError(
            "model_provider_env_key must be an uppercase env variable name"
        )
    wire_api = str(configuration.get("model_provider_wire_api") or "")
    if wire_api and wire_api not in _WIRE_APIS:
        raise ValueError("model_provider_wire_api must be responses, chat, or messages")
    provider_fields = (
        "model_provider_name",
        "model_provider_base_url",
        "model_provider_env_key",
        "model_provider_wire_api",
    )
    if any(configuration.get(key) for key in provider_fields) and not provider:
        raise ValueError("model_provider is required for custom provider configuration")


def _write_managed_config(path: Path, configuration: dict[str, Any]) -> None:
    lines = ["# Managed by PA for the OpenInterpreter ACP provider."]
    for key in ("model", "model_provider"):
        value = configuration.get(key)
        if value:
            lines.append(f"{key} = {_toml_string(str(value))}")
    provider = configuration.get("model_provider")
    provider_fields = {
        "name": configuration.get("model_provider_name"),
        "base_url": configuration.get("model_provider_base_url"),
        "env_key": configuration.get("model_provider_env_key"),
        "wire_api": configuration.get("model_provider_wire_api"),
    }
    if provider and any(provider_fields.values()):
        lines.extend(["", f"[model_providers.{_toml_string(str(provider))}]"])
        for key, value in provider_fields.items():
            if value:
                lines.append(f"{key} = {_toml_string(str(value))}")
    atomic_write_text(path, "\n".join(lines) + "\n", mode=0o600)


def _toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _run_official_installer(interpreter_home: Path) -> subprocess.CompletedProcess[str]:
    interpreter_home.mkdir(parents=True, exist_ok=True)
    env = {
        **os.environ,
        "INTERPRETER_HOME": str(interpreter_home),
        "OPEN_INTERPRETER_NONINTERACTIVE": "1",
    }
    if os.name == "nt":
        powershell = shutil.which("powershell") or shutil.which("pwsh")
        if not powershell:
            raise OSError("PowerShell is required for the OpenInterpreter installer")
        return subprocess.run(
            [
                powershell,
                "-NoProfile",
                "-Command",
                "$ProgressPreference='SilentlyContinue'; "
                "& ([scriptblock]::Create((Invoke-WebRequest "
                "-UseBasicParsing https://www.openinterpreter.com/install.ps1).Content))",
            ],
            capture_output=True,
            text=True,
            timeout=600,
            check=False,
            env=env,
        )

    request = urllib.request.Request(
        _INSTALL_URL, headers={"User-Agent": "PA OpenInterpreter provider installer"}
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        if urlparse(response.geturl()).scheme != "https":
            raise ValueError("OpenInterpreter installer redirected to a non-HTTPS URL")
        payload = response.read(_MAX_INSTALLER_BYTES + 1)
    if len(payload) > _MAX_INSTALLER_BYTES:
        raise ValueError("OpenInterpreter installer exceeded the size limit")
    if b"#!/" not in payload[:128]:
        raise ValueError("OpenInterpreter installer did not look like a shell script")
    with tempfile.TemporaryDirectory(prefix="pa-openinterpreter-") as tmp:
        installer = Path(tmp) / "install.sh"
        installer.write_bytes(payload)
        return subprocess.run(
            ["sh", str(installer)],
            capture_output=True,
            text=True,
            timeout=600,
            check=False,
            env=env,
        )


def _version(command: str) -> str | None:
    try:
        proc = subprocess.run(
            [command, "--version"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except OSError, subprocess.TimeoutExpired:
        return None
    text = (proc.stdout or proc.stderr or "").strip()
    return text.splitlines()[0][:120] if text else None


def _output_tail(proc: subprocess.CompletedProcess[str]) -> str:
    return (proc.stderr or proc.stdout or "").strip()[-500:]
