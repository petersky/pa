"""Codex ACP provider (`codex-acp` / `@agentclientprotocol/codex-acp`)."""

from __future__ import annotations

import logging
import os
import re
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
    load_credentials,
    load_metadata,
    merge_provider_env,
    save_credentials,
    save_metadata,
)
from pa.acp.providers.codex_auth import get_codex_login_store, resolve_codex_cli
from pa.packaging.paths import resolve_executable

logger = logging.getLogger(__name__)

NPM_PACKAGE = "@agentclientprotocol/codex-acp"
CODEX_CLI_NPM_PACKAGE = "@openai/codex"
_DEFAULT_COMMAND = "codex-acp"
_NPX_ARGS = ["-y", NPM_PACKAGE]


class CodexProvider:
    id = AgentProviderId.CODEX.value
    display_name = "Codex"

    def default_spec(self) -> AgentProviderSpec:
        return AgentProviderSpec(
            id=self.id,
            display_name=self.display_name,
            command=_DEFAULT_COMMAND,
            args=[],
            docs_key="codex",
            install_method="npm",
            npm_package=NPM_PACKAGE,
            capability_notes="Codex ACP adapter. See docs/acp/codex.md.",
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
        else:
            resolved = resolve_executable(_DEFAULT_COMMAND) or shutil.which(
                _DEFAULT_COMMAND
            )
            if resolved:
                spec.command = str(resolved)
                spec.args = list(args_override) if args_override is not None else []
            else:
                # Fall back to npx without requiring a global install.
                npx = shutil.which("npx")
                if npx:
                    spec.command = npx
                    spec.args = list(_NPX_ARGS)
                    if args_override:
                        spec.args = list(_NPX_ARGS) + list(args_override)
                elif args_override is not None:
                    spec.args = list(args_override)
        env: dict[str, str] = {}
        if data_dir is not None:
            env.update(merge_provider_env(data_dir, self.id))
            # Headless hosts: hide ChatGPT browser auth by default when configured.
            meta = load_metadata(data_dir, self.id)
            if meta and meta.env.get("NO_BROWSER"):
                env.setdefault("NO_BROWSER", meta.env["NO_BROWSER"])
        if extra_env:
            env.update(extra_env)
        spec.env = env
        return spec

    def status(self, data_dir: Path) -> ProviderStatus:
        spec = self.resolve_spawn(data_dir=data_dir)
        direct = resolve_executable(_DEFAULT_COMMAND) or shutil.which(_DEFAULT_COMMAND)
        meta = load_metadata(data_dir, self.id)
        creds = load_credentials(data_dir, self.id)
        codex_cli = resolve_codex_cli()
        auth = _codex_auth_status(codex_cli, creds=creds, env=spec.env)
        login_job = get_codex_login_store(data_dir).latest_active()
        version = None
        if direct:
            version = _run_version([str(direct), "--version"])
        elif shutil.which("npx"):
            version = _run_version(["npx", "-y", NPM_PACKAGE, "--version"])
        return ProviderStatus(
            id=self.id,
            display_name=self.display_name,
            installed=bool(direct)
            or bool(meta and meta.install_method in {"npm", "npx"}),
            available=bool(direct) or bool(shutil.which("npx")),
            command=spec.command,
            resolved_path=str(direct) if direct else None,
            version=version or (meta.version if meta else None),
            auth_configured=auth[0],
            auth_method=auth[1],
            auth_status=auth[2],
            auth_error=auth[3],
            login_in_progress=login_job is not None,
            codex_cli_installed=codex_cli is not None,
            codex_cli_path=codex_cli,
            codex_cli_version=_run_version([codex_cli, "--version"])
            if codex_cli
            else None,
            install_method=meta.install_method
            if meta
            else ("npm" if direct else "npx"),
            last_probe=meta.last_probe if meta else None,
            meta={
                "args": spec.args,
                "npm_package": NPM_PACKAGE,
                "codex_cli_required_for_login": True,
                "active_login_job_id": login_job.job_id if login_job else None,
            },
        )

    def install(self, data_dir: Path) -> ProviderInstallResult:
        npm = shutil.which("npm")
        if not npm:
            if shutil.which("npx"):
                save_metadata(
                    data_dir,
                    ProviderMetadata(
                        provider_id=self.id,
                        install_method="npx",
                        command="npx",
                        configured=False,
                    ),
                )
                return ProviderInstallResult(
                    id=self.id,
                    ok=True,
                    message=(
                        f"npm not found; will run via `npx -y {NPM_PACKAGE}` on demand"
                    ),
                    command="npx",
                )
            return ProviderInstallResult(
                id=self.id,
                ok=False,
                message="npm/npx not found — install Node.js to use Codex ACP",
            )
        try:
            proc = subprocess.run(
                [npm, "install", "-g", NPM_PACKAGE],
                capture_output=True,
                text=True,
                timeout=300,
                check=False,
            )
            ok = proc.returncode == 0
            detail = (proc.stdout or "")[-400:]
            err = (proc.stderr or "")[-400:]
            resolved = shutil.which(_DEFAULT_COMMAND)
            version = (
                _run_version([resolved or _DEFAULT_COMMAND, "--version"])
                if ok
                else None
            )
            if ok:
                save_metadata(
                    data_dir,
                    ProviderMetadata(
                        provider_id=self.id,
                        install_method="npm",
                        version=version,
                        command=str(resolved or _DEFAULT_COMMAND),
                        configured=False,
                    ),
                )
            return ProviderInstallResult(
                id=self.id,
                ok=ok,
                message=("Installed " + NPM_PACKAGE)
                if ok
                else f"Install failed: {err or detail}",
                version=version,
                command=str(resolved) if resolved else _DEFAULT_COMMAND,
                detail={"stdout_tail": detail, "stderr_tail": err},
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return ProviderInstallResult(id=self.id, ok=False, message=str(exc))

    def update(self, data_dir: Path) -> ProviderInstallResult:
        npm = shutil.which("npm")
        if not npm:
            return ProviderInstallResult(
                id=self.id,
                ok=False,
                message="npm not found — cannot update global package",
            )
        try:
            proc = subprocess.run(
                [npm, "install", "-g", f"{NPM_PACKAGE}@latest"],
                capture_output=True,
                text=True,
                timeout=300,
                check=False,
            )
            ok = proc.returncode == 0
            resolved = shutil.which(_DEFAULT_COMMAND)
            version = (
                _run_version([resolved or _DEFAULT_COMMAND, "--version"])
                if ok
                else None
            )
            if ok:
                save_metadata(
                    data_dir,
                    ProviderMetadata(
                        provider_id=self.id,
                        install_method="npm",
                        version=version,
                        command=str(resolved or _DEFAULT_COMMAND),
                        configured=True,
                    ),
                )
            err = (proc.stderr or proc.stdout or "").strip()[-500:]
            return ProviderInstallResult(
                id=self.id,
                ok=ok,
                message="Updated" if ok else f"Update failed: {err}",
                version=version,
                command=str(resolved) if resolved else None,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return ProviderInstallResult(id=self.id, ok=False, message=str(exc))

    def configure(self, data_dir: Path, body: ProviderConfigureBody) -> ProviderStatus:
        meta = load_metadata(data_dir, self.id) or ProviderMetadata(provider_id=self.id)
        env = dict(meta.env)
        env.update(body.env)
        if body.no_browser is not None:
            if body.no_browser:
                env["NO_BROWSER"] = "1"
            else:
                env.pop("NO_BROWSER", None)
        if body.codex_path:
            env["CODEX_PATH"] = body.codex_path
        if body.initial_agent_mode:
            env["INITIAL_AGENT_MODE"] = body.initial_agent_mode
        meta.env = env
        meta.configured = True
        save_metadata(data_dir, meta)
        if body.secrets:
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


def _run_version(cmd: list[str]) -> str | None:
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=30, check=False
        )
        text = (proc.stdout or proc.stderr or "").strip()
        return text.splitlines()[0][:120] if text else None
    except OSError, subprocess.TimeoutExpired:
        return None


def install_codex_cli() -> ProviderInstallResult:
    """Install the official CLI separately from the ACP adapter."""
    npm = shutil.which("npm")
    if not npm:
        return ProviderInstallResult(
            id="codex-cli",
            ok=False,
            message="npm not found — install Node.js, then install @openai/codex",
        )
    try:
        proc = subprocess.run(
            [npm, "install", "--global", CODEX_CLI_NPM_PACKAGE],
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return ProviderInstallResult(
            id="codex-cli", ok=False, message="Codex CLI install timed out"
        )
    except OSError as exc:
        return ProviderInstallResult(
            id="codex-cli",
            ok=False,
            message=f"Unable to start npm: {type(exc).__name__}",
        )
    resolved = resolve_codex_cli()
    if proc.returncode != 0:
        return ProviderInstallResult(
            id="codex-cli",
            ok=False,
            message=f"Codex CLI install failed (npm exit {proc.returncode}); inspect npm logs on the target",
        )
    return ProviderInstallResult(
        id="codex-cli",
        ok=resolved is not None,
        message="Installed official Codex CLI"
        if resolved
        else "npm completed but codex is not on the PA service PATH",
        command=resolved,
        version=_run_version([resolved, "--version"]) if resolved else None,
    )


def _codex_auth_status(
    codex_cli: str | None, *, creds: dict[str, str], env: dict[str, str]
) -> tuple[bool, str, str, str | None]:
    """Return configured, method, actionable status, error without credential values."""
    combined = {**os.environ, **env, **creds}
    if combined.get("CODEX_ACCESS_TOKEN"):
        return (
            True,
            "access_token",
            "Access token configured for the target PA process.",
            None,
        )
    if combined.get("CODEX_API_KEY") or combined.get("OPENAI_API_KEY"):
        return True, "api_key", "API key configured for the target PA process.", None
    if not codex_cli:
        return (
            False,
            "none",
            "Codex CLI is not installed; install it before ChatGPT sign-in.",
            "codex CLI not found",
        )
    try:
        proc = subprocess.run(
            [codex_cli, "login", "status"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return (
            False,
            "unknown",
            "Codex login status timed out; retry status on the target.",
            "codex login status timed out",
        )
    except OSError as exc:
        return (
            False,
            "unknown",
            "Unable to run Codex login status on the target.",
            f"codex login status failed: {type(exc).__name__}",
        )
    output = "\n".join((proc.stdout or "", proc.stderr or "")).strip()
    normalized = re.sub(r"\s+", " ", output).lower()
    if proc.returncode == 0:
        if "chatgpt" in normalized:
            return True, "chatgpt_oauth", "Signed in with ChatGPT on the target.", None
        if "access token" in normalized:
            return (
                True,
                "access_token",
                "Signed in with a Codex access token on the target.",
                None,
            )
        if "api key" in normalized or "api_key" in normalized:
            return True, "api_key", "Signed in with an API key on the target.", None
        if "not logged in" in normalized or "not authenticated" in normalized:
            return False, "none", "Not signed in to Codex on the target.", None
        return (
            True,
            "unknown",
            "Codex reports a login, but its authentication method is unknown.",
            None,
        )
    if "not logged in" in normalized or "not authenticated" in normalized:
        return False, "none", "Not signed in to Codex on the target.", None
    # Never relay command output: future CLI versions could include sensitive detail.
    return (
        False,
        "unknown",
        "Codex could not validate the stored login; sign in again or log out on the target.",
        f"codex login status exited {proc.returncode}",
    )
