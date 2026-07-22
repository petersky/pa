"""Cursor ACP provider (`agent acp`)."""

from __future__ import annotations

import logging
import shutil
import subprocess
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
        spec = self.resolve_spawn(data_dir=data_dir)
        resolved = resolve_executable(spec.command) or (
            Path(shutil.which(spec.command)) if shutil.which(spec.command) else None
        )
        meta = load_metadata(data_dir, self.id)
        version = _version(str(resolved) if resolved else spec.command)
        return ProviderStatus(
            id=self.id,
            display_name=self.display_name,
            installed=bool(resolved),
            available=bool(resolved),
            command=spec.command,
            resolved_path=str(resolved) if resolved else None,
            version=version or (meta.version if meta else None),
            auth_configured=True,  # Cursor uses its own login; not tracked here
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
            msg = (proc.stdout or proc.stderr or "").strip() or (
                "Updated" if ok else "Update failed"
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
                message=msg[:500],
                version=version,
                command=str(resolved),
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return ProviderInstallResult(
                id=self.id, ok=False, message=str(exc), command=str(resolved)
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


def _version(command: str) -> str | None:
    try:
        proc = subprocess.run(
            [command, "--version"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        text = (proc.stdout or proc.stderr or "").strip()
        return text.splitlines()[0][:120] if text else None
    except (OSError, subprocess.TimeoutExpired):
        return None
