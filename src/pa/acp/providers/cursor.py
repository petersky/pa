"""Cursor ACP provider (`agent acp`)."""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

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
    save_metadata,
)
from pa.packaging.paths import resolve_executable

logger = logging.getLogger(__name__)

_DEFAULT_COMMAND = "agent"
_DEFAULT_ARGS = ["acp"]


class CursorProvider:
    id = AgentProviderId.CURSOR.value
    display_name = "Cursor"

    def default_spec(self) -> AgentProviderSpec:
        return AgentProviderSpec(
            id=self.id,
            display_name=self.display_name,
            command=_DEFAULT_COMMAND,
            args=list(_DEFAULT_ARGS),
            docs_key="cursor",
            install_method="path",
            capability_notes="Cursor CLI ACP server. See docs/acp/cursor.md.",
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
        if command_override:
            spec.command = command_override
        else:
            resolved = resolve_executable(_DEFAULT_COMMAND) or resolve_executable(
                "cursor-agent"
            )
            if resolved:
                spec.command = str(resolved)
        if args_override is not None:
            spec.args = list(args_override)
        env: dict[str, str] = {}
        if data_dir is not None:
            env.update(merge_provider_env(data_dir, self.id))
        if extra_env:
            env.update(extra_env)
        spec.env = env
        return spec

    def status(self, data_dir: Path) -> ProviderStatus:
        started = time.perf_counter()
        attempted_at = datetime.now(UTC).isoformat()
        spec = self.resolve_spawn(data_dir=data_dir)
        resolved = resolve_executable(spec.command) or (
            Path(shutil.which(spec.command)) if shutil.which(spec.command) else None
        )
        meta = load_metadata(data_dir, self.id)
        credentials = load_credentials(data_dir, self.id)
        auth = _cursor_auth_status(
            str(resolved) if resolved else None,
            env={**spec.env, **credentials},
        )
        version = _version(str(resolved) if resolved else spec.command)
        duration_ms = (time.perf_counter() - started) * 1000
        return ProviderStatus(
            id=self.id,
            display_name=self.display_name,
            installed=bool(resolved),
            available=bool(resolved),
            command=spec.command,
            resolved_path=str(resolved) if resolved else None,
            version=version or (meta.version if meta else None),
            auth_configured=auth[1],
            auth_method=auth[2],
            auth_state=auth[0],
            auth_status=auth[3],
            auth_error=auth[4],
            auth_evidence=["cursor_cli_status"] if resolved else [],
            last_attempted_at=attempted_at,
            last_successful_at=attempted_at
            if auth[0] not in {"timed_out", "probe_failed", "unknown"}
            else None,
            probe_duration_ms=duration_ms,
            install_method="path",
            last_probe=meta.last_probe if meta else None,
            meta={"args": spec.args},
        )

    def install(self, data_dir: Path) -> ProviderInstallResult:
        st = self.status(data_dir)
        if not st.available:
            return ProviderInstallResult(
                id=self.id,
                ok=False,
                message=(
                    "Cursor `agent` binary not found on PATH. "
                    "Install Cursor CLI and ensure `agent` is available."
                ),
                command=st.command,
            )
        save_metadata(
            data_dir,
            ProviderMetadata(
                provider_id=self.id,
                install_method="path",
                version=st.version,
                command=st.resolved_path or st.command,
                configured=True,
            ),
        )
        return ProviderInstallResult(
            id=self.id,
            ok=True,
            message=f"Cursor agent available at {st.resolved_path}",
            version=st.version,
            command=st.resolved_path,
        )

    def update(self, data_dir: Path) -> ProviderInstallResult:
        resolved = resolve_executable(_DEFAULT_COMMAND) or shutil.which(_DEFAULT_COMMAND)
        if not resolved:
            return self.install(data_dir)
        try:
            proc = subprocess.run(
                [str(resolved), "update"],
                capture_output=True,
                text=True,
                timeout=120,
                check=False,
            )
            ok = proc.returncode == 0
            message = (
                "Updated Cursor CLI"
                if ok
                else f"Cursor CLI update failed (exit {proc.returncode})"
            )
            version = _version(str(resolved))
            if ok:
                save_metadata(
                    data_dir,
                    ProviderMetadata(
                        provider_id=self.id,
                        install_method="path",
                        version=version,
                        command=str(resolved),
                        configured=True,
                    ),
                )
            return ProviderInstallResult(
                id=self.id,
                ok=ok,
                message=message,
                version=version,
                command=str(resolved),
            )
        except subprocess.TimeoutExpired:
            return ProviderInstallResult(
                id=self.id,
                ok=False,
                message="Cursor CLI update timed out",
                command=str(resolved),
            )
        except OSError as exc:
            return ProviderInstallResult(
                id=self.id,
                ok=False,
                message=f"Cursor CLI update failed ({type(exc).__name__})",
                command=str(resolved),
            )

    def configure(
        self, data_dir: Path, body: ProviderConfigureBody
    ) -> ProviderStatus:
        meta = load_metadata(data_dir, self.id) or ProviderMetadata(provider_id=self.id)
        meta.env.update(body.env)
        meta.configured = True
        meta.install_method = "path"
        save_metadata(data_dir, meta)
        if body.secrets:
            from pa.acp.providers.metadata import save_credentials

            save_credentials(data_dir, self.id, body.secrets)
        return self.status(data_dir)

    def probe(self, data_dir: Path) -> dict[str, Any]:
        from pa.acp.providers.probe import probe_acp_initialize

        spec = self.resolve_spawn(data_dir=data_dir)
        result = probe_acp_initialize(spec)
        meta = load_metadata(data_dir, self.id) or ProviderMetadata(provider_id=self.id)
        meta.last_probe = result
        from datetime import UTC, datetime

        meta.last_probe_at = datetime.now(UTC).isoformat()
        save_metadata(data_dir, meta)
        return result


def _cursor_auth_status(
    executable: str | None, *, env: dict[str, str]
) -> tuple[str, bool, str, str, str | None]:
    """Probe Cursor's supported CLI status command without relaying its output."""
    if env.get("CURSOR_API_KEY"):
        return (
            "authenticated",
            True,
            "api_key",
            "Cursor API key configured for the target PA process.",
            None,
        )
    if not executable:
        return (
            "unavailable",
            False,
            "none",
            "Cursor CLI is not installed for the PA service user.",
            "cursor CLI not found",
        )
    try:
        proc = subprocess.run(
            [executable, "status"],
            capture_output=True,
            text=True,
            timeout=2.5,
            check=False,
            env={**os.environ, **env},
        )
    except subprocess.TimeoutExpired:
        return (
            "timed_out",
            False,
            "unknown",
            "Cursor authentication status timed out; retry on the target.",
            "cursor status timed out",
        )
    except OSError as exc:
        return (
            "probe_failed",
            False,
            "unknown",
            "Unable to run Cursor authentication status for the PA service user.",
            f"cursor status failed: {type(exc).__name__}",
        )
    output = "\n".join((proc.stdout or "", proc.stderr or "")).strip()
    normalized = re.sub(r"\s+", " ", output).lower()
    if any(
        marker in normalized
        for marker in (
            "not authenticated",
            "not logged in",
            "signed out",
            "login required",
        )
    ):
        return (
            "signed_out",
            False,
            "cursor_account",
            "Signed out of Cursor for the PA service user.",
            None,
        )
    authenticated = any(
        marker in normalized
        for marker in ("authenticated", "logged in", "login successful")
    )
    if proc.returncode == 0 and authenticated:
        return (
            "authenticated",
            True,
            "cursor_account",
            "Signed in to Cursor for the PA service user.",
            None,
        )
    if proc.returncode == 0:
        return (
            "unknown",
            False,
            "unknown",
            "Cursor status succeeded, but its authentication state was not recognized.",
            None,
        )
    return (
        "probe_failed",
        False,
        "unknown",
        "Cursor could not validate authentication for the PA service user.",
        f"cursor status exited {proc.returncode}",
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
        text = (proc.stdout or proc.stderr or "").strip()
        return text.splitlines()[0][:120] if text else None
    except (OSError, subprocess.TimeoutExpired):
        return None
