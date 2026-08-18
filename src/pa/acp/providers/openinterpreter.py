"""OpenInterpreter ACP provider (``interpreter acp``)."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
import urllib.request
from datetime import UTC, datetime
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
# Common built-in model backends. The installed interpreter binary can expand
# this list at runtime; these defaults keep options usable before a session.
_BUILTIN_PROVIDER_ENV_KEYS: dict[str, str | None] = {
    "openai": "OPENAI_API_KEY",
    "anthropic": "ANTHROPIC_API_KEY",
    "openrouter": "OPENROUTER_API_KEY",
    "groq": "GROQ_API_KEY",
    "deepseek": "DEEPSEEK_API_KEY",
    "moonshotai": "MOONSHOT_API_KEY",
    "moonshotai-cn": "MOONSHOT_API_KEY",
    "kimi-for-coding": "KIMI_API_KEY",
    "zai": "ZAI_API_KEY",
    "zai-coding-plan": "ZAI_API_KEY",
    "zhipuai": "ZHIPU_API_KEY",
    "zhipuai-coding-plan": "ZHIPU_API_KEY",
    "zhipu": "ZHIPU_API_KEY",
    "minimax": "MINIMAX_API_KEY",
    "minimax-cn": "MINIMAX_API_KEY",
    "minimax-coding-plan": "MINIMAX_API_KEY",
    "minimax-cn-coding-plan": "MINIMAX_API_KEY",
    "ollama": None,
    "lmstudio": None,
}
_STATIC_MODES = [
    {"id": "read-only", "name": "Read Only"},
    {"id": "workspace-write", "name": "Workspace Write"},
    {"id": "full-access", "name": "Full Access"},
]
_PROVIDER_HEADER = re.compile(
    rb'\{\s*"id":\s*"(?P<id>[^"]+)"\s*,\s*"name":\s*"(?P<name>(?:\\.|[^"\\])*)"'
    rb'\s*,\s*"env_key":\s*"(?P<env>[^"]*)"\s*,\s*"base_url":\s*"(?P<url>[^"]*)"'
    rb'\s*,\s*"wire_api":\s*"(?P<wire>[^"]*)"',
)


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
            _repair_managed_config_if_needed(data_dir)
            env.update(merge_provider_env(data_dir, self.id))
        if extra_env:
            env.update(extra_env)
        if data_dir is not None:
            env["INTERPRETER_HOME"] = str(_managed_home(data_dir))
        spec.env = env
        return spec

    def status(self, data_dir: Path) -> ProviderStatus:
        _repair_managed_config_if_needed(data_dir)
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
        attempted_at = datetime.now(UTC).isoformat()
        if creds:
            auth_status = "Model provider credential configured on this host."
        elif no_auth:
            auth_status = f"{model_provider} does not require a stored API key."
        else:
            auth_status = "No model provider credential stored by PA."
        options = provider_options_snapshot(
            data_dir,
            model_provider=model_provider or None,
            credentials=creds,
        )
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
            auth_state="authenticated" if auth_configured else "not_configured",
            auth_status=auth_status,
            auth_evidence=["configured_credential"]
            if creds
            else (["no_auth_required"] if no_auth else []),
            last_attempted_at=attempted_at,
            last_successful_at=attempted_at,
            install_method=meta.install_method if meta else "official",
            last_probe=meta.last_probe if meta else None,
            meta={
                "args": spec.args,
                "interpreter_home": spec.env.get("INTERPRETER_HOME"),
                "config_path": str(_config_path(data_dir)),
                "configuration": configuration,
                "credential_keys": sorted(creds),
                "install_url": _INSTALL_URL,
                "model_providers": options.get("model_providers"),
                "options": {
                    "models": options.get("models"),
                    "modes": options.get("modes"),
                    "config_options": options.get("config_options"),
                },
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
        except subprocess.TimeoutExpired:
            return ProviderInstallResult(
                id=self.id,
                ok=False,
                message="OpenInterpreter install timed out",
                command=_DEFAULT_COMMAND,
            )
        except (OSError, ValueError) as exc:
            return ProviderInstallResult(
                id=self.id,
                ok=False,
                message=f"OpenInterpreter install failed ({type(exc).__name__})",
                command=_DEFAULT_COMMAND,
            )
        resolved = resolve_executable(_DEFAULT_COMMAND) or shutil.which(
            _DEFAULT_COMMAND
        )
        if proc.returncode != 0 or not resolved:
            message = (
                f"OpenInterpreter installer failed (exit {proc.returncode})"
                if proc.returncode != 0
                else "Installer completed but `interpreter` is not on the PA service PATH"
            )
            return ProviderInstallResult(
                id=self.id,
                ok=False,
                message=message,
                command=_DEFAULT_COMMAND,
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
        except subprocess.TimeoutExpired:
            return ProviderInstallResult(
                id=self.id,
                ok=False,
                message="OpenInterpreter update timed out",
                command=str(resolved),
            )
        except OSError as exc:
            return ProviderInstallResult(
                id=self.id,
                ok=False,
                message=f"OpenInterpreter update failed ({type(exc).__name__})",
                command=str(resolved),
            )
        if proc.returncode == 0:
            return self._record_install(data_dir, Path(resolved), "updated")
        return ProviderInstallResult(
            id=self.id,
            ok=False,
            message=f"OpenInterpreter update failed (exit {proc.returncode})",
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
    custom_name = str(configuration.get("model_provider_name") or "").strip()
    custom_base_url = str(configuration.get("model_provider_base_url") or "").strip()
    if (custom_name or custom_base_url) and not provider:
        raise ValueError("model_provider is required for custom provider configuration")
    if bool(custom_name) ^ bool(custom_base_url):
        raise ValueError(
            "custom model providers require both model_provider_name and "
            "model_provider_base_url"
        )
    if any(
        configuration.get(key)
        for key in ("model_provider_env_key", "model_provider_wire_api")
    ) and (custom_name or custom_base_url) and not (custom_name and custom_base_url):
        raise ValueError(
            "custom model providers require both model_provider_name and "
            "model_provider_base_url"
        )


def _write_managed_config(path: Path, configuration: dict[str, Any]) -> None:
    """Write host defaults for OpenInterpreter without clobbering built-ins.

    Incomplete ``[model_providers.*]`` tables (env_key/wire_api without
    name/base_url) override built-in backends and crash ``interpreter acp``
    during initialize. Only emit a custom provider table when both name and
    base_url are present.
    """
    lines = ["# Managed by PA for the OpenInterpreter ACP provider."]
    for key in ("model", "model_provider"):
        value = configuration.get(key)
        if value:
            lines.append(f"{key} = {_toml_string(str(value))}")
    provider = str(configuration.get("model_provider") or "").strip()
    custom_name = str(configuration.get("model_provider_name") or "").strip()
    custom_base_url = str(configuration.get("model_provider_base_url") or "").strip()
    if provider and custom_name and custom_base_url:
        provider_fields = {
            "name": custom_name,
            "base_url": custom_base_url,
            "env_key": configuration.get("model_provider_env_key"),
            "wire_api": configuration.get("model_provider_wire_api"),
        }
        lines.extend(["", f"[model_providers.{_toml_key(provider)}]"])
        for key, value in provider_fields.items():
            if value:
                lines.append(f"{key} = {_toml_string(str(value))}")
    atomic_write_text(path, "\n".join(lines) + "\n", mode=0o600)


def _toml_key(value: str) -> str:
    if _IDENTIFIER.fullmatch(value) and "-" not in value:
        return value
    return _toml_string(value)


def _toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _spawn_args(
    *,
    model_provider: str | None = None,
    model: str | None = None,
) -> list[str]:
    args: list[str] = []
    if model_provider:
        args.extend(["-c", f"model_provider={_toml_string(model_provider)}"])
    if model:
        args.extend(["-c", f"model={_toml_string(model)}"])
    args.extend(_DEFAULT_ARGS)
    return args


def _repair_managed_config_if_needed(data_dir: Path) -> bool:
    """Rewrite managed config.toml when an incomplete custom override is present."""
    path = _config_path(data_dir)
    if not path.exists():
        return False
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - py<3.11
        import tomli as tomllib  # type: ignore
    try:
        parsed = tomllib.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    providers = parsed.get("model_providers") or {}
    if not isinstance(providers, dict) or not providers:
        return False
    meta = load_metadata(data_dir, AgentProviderId.OPENINTERPRETER.value)
    configuration = dict(meta.configuration) if meta else {}
    current = str(
        configuration.get("model_provider") or parsed.get("model_provider") or ""
    ).strip()
    entry = providers.get(current) if current else None
    if not isinstance(entry, dict):
        # Unexpected tables for other keys still need a clean rewrite from metadata.
        needs_repair = True
    else:
        has_custom_meta = bool(
            configuration.get("model_provider_name")
            and configuration.get("model_provider_base_url")
        )
        incomplete = not (entry.get("name") and entry.get("base_url"))
        needs_repair = incomplete and not has_custom_meta
    if not needs_repair:
        return False
    if not configuration.get("model_provider") and parsed.get("model_provider"):
        configuration["model_provider"] = str(parsed["model_provider"])
    if not configuration.get("model") and parsed.get("model"):
        configuration["model"] = str(parsed["model"])
    # Drop incomplete custom fragments from persisted metadata so rewrite stays clean.
    for key in (
        "model_provider_name",
        "model_provider_base_url",
        "model_provider_env_key",
        "model_provider_wire_api",
    ):
        if key in configuration and not (
            configuration.get("model_provider_name")
            and configuration.get("model_provider_base_url")
        ):
            # Keep env_key in metadata for credential mapping when present; never
            # write incomplete TOML overrides.
            if key in {"model_provider_name", "model_provider_base_url"}:
                configuration.pop(key, None)
    _write_managed_config(path, configuration)
    if meta is not None:
        meta.configuration = configuration
        save_metadata(data_dir, meta)
    return True


def builtin_model_provider_env_key(model_provider: str | None) -> str | None:
    provider = str(model_provider or "").strip()
    if not provider:
        return None
    if provider in _BUILTIN_PROVIDER_ENV_KEYS:
        return _BUILTIN_PROVIDER_ENV_KEYS[provider]
    discovered = {
        item["id"]: item.get("env_key") for item in discover_builtin_model_providers()
    }
    env_key = discovered.get(provider)
    return str(env_key) if env_key else None


def discover_builtin_model_providers(
    *, command: str | None = None
) -> list[dict[str, Any]]:
    """Return built-in OpenInterpreter model backends (id/name/env_key/…)."""
    resolved = (
        resolve_executable(command or _DEFAULT_COMMAND)
        or shutil.which(command or _DEFAULT_COMMAND)
    )
    providers: list[dict[str, Any]] = []
    if resolved:
        try:
            data = Path(resolved).resolve().read_bytes()
        except OSError:
            data = b""
        seen: set[str] = set()
        for match in _PROVIDER_HEADER.finditer(data):
            provider_id = match.group("id").decode("utf-8", "replace")
            if provider_id in seen:
                continue
            seen.add(provider_id)
            name = json.loads(
                b'"' + match.group("name") + b'"'
            )
            providers.append(
                {
                    "id": provider_id,
                    "name": name,
                    "env_key": match.group("env").decode("utf-8", "replace") or None,
                    "base_url": match.group("url").decode("utf-8", "replace"),
                    "wire_api": match.group("wire").decode("utf-8", "replace"),
                    "requires_auth": provider_id not in _NO_AUTH_PROVIDERS,
                    "source": "binary",
                }
            )
        # Attach model catalogs for known providers when nearby in the binary.
        for provider in providers:
            provider["models"] = _extract_models_for_provider(data, provider["id"])
    if providers:
        # Ensure local no-auth backends remain listed even if the binary layout
        # changes.
        have = {item["id"] for item in providers}
        for provider_id in sorted(_NO_AUTH_PROVIDERS):
            if provider_id not in have:
                providers.append(
                    {
                        "id": provider_id,
                        "name": provider_id,
                        "env_key": None,
                        "requires_auth": False,
                        "source": "static",
                        "models": [],
                    }
                )
        return providers
    return [
        {
            "id": provider_id,
            "name": provider_id,
            "env_key": env_key,
            "requires_auth": provider_id not in _NO_AUTH_PROVIDERS,
            "source": "static",
            "models": [],
        }
        for provider_id, env_key in _BUILTIN_PROVIDER_ENV_KEYS.items()
    ]


def _extract_models_for_provider(data: bytes, provider_id: str) -> list[dict[str, str]]:
    needle = f'"id": "{provider_id}"'.encode()
    idx = data.find(needle)
    if idx < 0:
        return []
    # Limit the search window to this provider object.
    start = data.rfind(b"{", max(0, idx - 64), idx + 1)
    if start < 0:
        start = idx
    depth = 0
    end = min(len(data), start + 250_000)
    stop = end
    for i in range(start, end):
        byte = data[i]
        if byte == 0x7B:
            depth += 1
        elif byte == 0x7D:
            depth -= 1
            if depth == 0:
                stop = i + 1
                break
    window = data[start:stop]
    models: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in re.finditer(
        rb'"id":\s*"([^"]+)"\s*,\s*"display_name":\s*"((?:\\.|[^"\\])*)"',
        window,
    ):
        model_id = match.group(1).decode("utf-8", "replace")
        if model_id in seen or model_id == provider_id:
            continue
        seen.add(model_id)
        name = json.loads(b'"' + match.group(2) + b'"')
        models.append({"id": model_id, "modelId": model_id, "name": name})
    return models


def provider_options_snapshot(
    data_dir: Path | None = None,
    *,
    model_provider: str | None = None,
    credentials: dict[str, str] | None = None,
    custom_providers: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Synthesize session-start options for OpenInterpreter without a live session."""
    configuration: dict[str, Any] = {}
    creds = dict(credentials or {})
    if data_dir is not None:
        _repair_managed_config_if_needed(data_dir)
        meta = load_metadata(data_dir, AgentProviderId.OPENINTERPRETER.value)
        configuration = dict(meta.configuration) if meta else {}
        if not creds:
            creds = load_credentials(data_dir, AgentProviderId.OPENINTERPRETER.value)
    selected = (
        str(model_provider or "").strip()
        or str(configuration.get("model_provider") or "").strip()
        or None
    )
    current_model = str(configuration.get("model") or "").strip() or None
    builtins = discover_builtin_model_providers()
    providers_by_id = {item["id"]: dict(item) for item in builtins}
    custom = list(custom_providers or [])
    if configuration.get("model_provider_name") and configuration.get(
        "model_provider_base_url"
    ):
        custom_id = str(configuration.get("model_provider") or "").strip()
        if custom_id:
            providers_by_id[custom_id] = {
                "id": custom_id,
                "name": configuration.get("model_provider_name"),
                "env_key": configuration.get("model_provider_env_key"),
                "base_url": configuration.get("model_provider_base_url"),
                "wire_api": configuration.get("model_provider_wire_api"),
                "requires_auth": True,
                "source": "configured",
                "models": [],
            }
    for item in custom:
        provider_id = str(item.get("id") or "").strip()
        if provider_id:
            providers_by_id[provider_id] = {**providers_by_id.get(provider_id, {}), **item}
    model_providers = []
    for provider_id, item in sorted(
        providers_by_id.items(), key=lambda pair: pair[1].get("name") or pair[0]
    ):
        env_key = item.get("env_key")
        requires_auth = bool(item.get("requires_auth", provider_id not in _NO_AUTH_PROVIDERS))
        configured = (not requires_auth) or (
            bool(env_key) and env_key in creds
        ) or (bool(creds) and provider_id == selected)
        model_providers.append(
            {
                "id": provider_id,
                "name": item.get("name") or provider_id,
                "env_key": env_key,
                "requires_auth": requires_auth,
                "configured": configured,
                "source": item.get("source") or "unknown",
            }
        )
    selected_meta = providers_by_id.get(selected or "", {})
    available_models = list(selected_meta.get("models") or [])
    if current_model and not any(
        (m.get("id") or m.get("modelId")) == current_model for m in available_models
    ):
        available_models = [
            {"id": current_model, "modelId": current_model, "name": current_model},
            *available_models,
        ]
    current_model_id = current_model or (
        available_models[0].get("id") or available_models[0].get("modelId")
        if available_models
        else None
    )
    # Do not invent OpenAI-style effort levels. OpenInterpreter only confirms
    # those values when the live model advertises an effort-shaped control;
    # MiniMax-M2.x uses fixed thinking and leaves ACP currentValue at "default".
    config_options: list[dict[str, Any]] = []
    if available_models:
        config_options.insert(
            0,
            {
                "id": "model",
                "name": "Model",
                "category": "model",
                "type": "select",
                "currentValue": current_model_id,
                "options": [
                    {
                        "name": item.get("name") or item.get("id") or item.get("modelId"),
                        "value": item.get("id") or item.get("modelId"),
                    }
                    for item in available_models
                    if item.get("id") or item.get("modelId")
                ],
            },
        )
    return {
        "provider": AgentProviderId.OPENINTERPRETER.value,
        "model_provider": selected,
        "model_providers": model_providers,
        "models": {
            "availableModels": available_models,
            "currentModelId": current_model_id,
        },
        "modes": {
            "availableModes": list(_STATIC_MODES),
            "currentModeId": "workspace-write",
        },
        "config_options": config_options,
        "supports_model_provider": True,
        "cached": True,
        "source": "openinterpreter_catalog",
    }


def preflight_session_start(
    data_dir: Path,
    *,
    model_provider: str | None = None,
    model_id: str | None = None,
) -> dict[str, Any] | None:
    """Return a typed failure dict when OpenInterpreter cannot start, else None."""
    _repair_managed_config_if_needed(data_dir)
    meta = load_metadata(data_dir, AgentProviderId.OPENINTERPRETER.value)
    configuration = dict(meta.configuration) if meta else {}
    selected = (
        str(model_provider or "").strip()
        or str(configuration.get("model_provider") or "").strip()
    )
    if not selected:
        return {
            "code": "model_provider_missing",
            "message": (
                "OpenInterpreter has no model_provider configured. Set one with "
                "`pa agent-provider configure --provider openinterpreter "
                "--model-provider <id>` or choose a model provider in the new-session UI."
            ),
            "recoverable": True,
        }
    if not _IDENTIFIER.fullmatch(selected):
        return {
            "code": "invalid_model_provider",
            "message": f"Invalid OpenInterpreter model_provider {selected!r}.",
            "recoverable": True,
        }
    creds = load_credentials(data_dir, AgentProviderId.OPENINTERPRETER.value)
    env_key = (
        str(configuration.get("model_provider_env_key") or "").strip()
        or builtin_model_provider_env_key(selected)
    )
    if selected not in _NO_AUTH_PROVIDERS:
        if not creds:
            return {
                "code": "auth_missing",
                "message": (
                    f"OpenInterpreter model provider {selected!r} has no API credential "
                    "stored on this host. Configure it in Settings / "
                    "`pa agent-provider configure` (secrets stay on the host)."
                ),
                "recoverable": True,
                "model_provider": selected,
            }
        if env_key and env_key not in creds and selected not in _NO_AUTH_PROVIDERS:
            return {
                "code": "auth_missing",
                "message": (
                    f"OpenInterpreter expects credential env {env_key} for "
                    f"model provider {selected!r}, but it is not stored on this host."
                ),
                "recoverable": True,
                "model_provider": selected,
                "env_key": env_key,
            }
    options = provider_options_snapshot(
        data_dir, model_provider=selected, credentials=creds
    )
    models = (options.get("models") or {}).get("availableModels") or []
    requested_model = str(model_id or "").strip()
    if requested_model and models:
        known = {
            str(item.get("id") or item.get("modelId") or "")
            for item in models
            if isinstance(item, dict)
        }
        if requested_model not in known:
            supported = ", ".join(sorted(m for m in known if m)[:12])
            return {
                "code": "invalid_model",
                "message": (
                    f"Model {requested_model!r} is not advertised for OpenInterpreter "
                    f"provider {selected!r}."
                    + (f" Supported models include: {supported}." if supported else "")
                ),
                "recoverable": True,
                "model_provider": selected,
                "model_id": requested_model,
            }
    resolved = resolve_executable(_DEFAULT_COMMAND) or shutil.which(_DEFAULT_COMMAND)
    if not resolved:
        return {
            "code": "provider_not_installed",
            "message": (
                "OpenInterpreter is not installed on this host. Run "
                "`pa agent-provider install --provider openinterpreter`."
            ),
            "recoverable": True,
        }
    return None


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
            timeout=0.4,
            check=False,
        )
    except OSError, subprocess.TimeoutExpired:
        return None
    text = (proc.stdout or proc.stderr or "").strip()
    return text.splitlines()[0][:120] if text else None
