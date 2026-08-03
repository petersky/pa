"""Evidence-rich, read-only post-install diagnostics for PA instances."""

from __future__ import annotations

import asyncio
import json
import os
import re
import socket
import stat
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import httpx
import typer

from pa import __version__
from pa.acp.environment import (
    private_provider_environment_names,
    sanitize_provider_environment,
)
from pa.acp.mcp_config import (
    McpHandshakeError,
    OwnerChannelError,
    owner_endpoint,
    probe_owner_channel,
    probe_pa_mcp_stdio,
)
from pa.cli import presentation as ui
from pa.cli import service as svc
from pa.config import get_settings
from pa.core.logging import redact_log_text
from pa.domain.instance_config import load_instance_config
from pa.install.metadata import load_install_metadata
from pa.packaging.service_env import service_environment

SECRET_NAMES = re.compile(
    r"(?:TOKEN|SECRET|PASSWORD|CREDENTIAL|COOKIE|KEY)$", re.IGNORECASE
)
LIST_ENV = {
    "PA_WEB_LISTENERS",
    "PA_SUBSCRIBED_REALMS",
    "PA_PEERS",
    "PA_CAPABILITIES",
    "PA_AGENT_ARGS",
}


@dataclass
class Command:
    command: str
    mutates_state: bool = False
    restarts_pa: bool = False
    may_interrupt_sessions: bool = False


@dataclass
class Finding:
    code: str
    severity: str
    cause: str
    impact: str
    evidence: dict[str, Any] = field(default_factory=dict)
    next_commands: list[Command] = field(default_factory=list)
    verification_command: str = "pa doctor"
    documentation: str | None = None
    root_cause: str | None = None


def _safe(name: str, value: Any) -> Any:
    if value is None:
        return None
    if SECRET_NAMES.search(name):
        return "<redacted>"
    return redact_log_text(str(value))


def _canonical(name: str, value: str | None) -> Any:
    if value is None:
        return None
    if name in LIST_ENV:
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                # These settings are sets operationally; order/JSON spacing is not drift.
                return sorted(
                    {str(item).strip() for item in parsed if str(item).strip()}
                )
        except TypeError, ValueError:
            return sorted({item.strip() for item in value.split(",") if item.strip()})
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    return value


async def _check_health(url: str, token: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(
                f"{url.rstrip('/')}/api/health",
                headers={"Authorization": f"Bearer {token}"} if token else {},
            )
            return response.status_code == 200
    except httpx.HTTPError:
        return False


async def _fetch_status(url: str, token: str) -> dict[str, Any] | None:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(
                f"{url.rstrip('/')}/api/status",
                headers={"Authorization": f"Bearer {token}"} if token else {},
            )
            payload = response.json() if response.status_code == 200 else None
            return payload if isinstance(payload, dict) else None
    except httpx.HTTPError, ValueError:
        return None


async def _check_peers(peers: list[str], token: str) -> list[tuple[str, bool]]:
    results = []
    async with httpx.AsyncClient(timeout=5.0) as client:
        for peer in peers:
            try:
                response = await client.get(
                    f"{peer.rstrip('/')}/api/health",
                    headers={"Authorization": f"Bearer {token}"} if token else {},
                )
                results.append((peer, response.status_code == 200))
            except httpx.HTTPError:
                results.append((peer, False))
    return results


def _binary_version(path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        result = subprocess.run(
            [str(path), "version"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except OSError, subprocess.TimeoutExpired:
        return None
    match = re.search(r"\bpa\s+v?([0-9][^\s]*)", result.stdout, re.IGNORECASE)
    return match.group(1) if result.returncode == 0 and match else None


def _same_path(left: str | Path | None, right: str | Path | None) -> bool:
    try:
        return bool(
            left
            and right
            and Path(left).expanduser().resolve() == Path(right).expanduser().resolve()
        )
    except OSError:
        return str(left) == str(right)


def _repair_commands(active_sessions: int) -> list[Command]:
    risk = active_sessions > 0
    return [
        Command("pa install --service-only --no-start", True),
        Command(
            "systemctl --user daemon-reload"
            if sys.platform.startswith("linux")
            else "pa install --service-only --no-start",
            True,
        ),
        Command("pa restart", True, True, risk),
    ]


def _environment_sources(loaded: svc.LoadedServiceDefinition, name: str) -> list[str]:
    """Locate unit/drop-in files mentioning a variable without exposing its value."""
    sources: list[str] = []
    for value in (loaded.unit_path, *loaded.drop_in_paths):
        if not value:
            continue
        path = Path(value)
        try:
            if re.search(rf"\b{re.escape(name)}\b", path.read_text(errors="replace")):
                sources.append(str(path))
        except OSError:
            continue
    return sources


def _service_findings(
    settings: Any, status: Any, service_bin: Path | None, active_sessions: int
) -> tuple[svc.LoadedServiceDefinition | None, list[Finding], set[str]]:
    findings: list[Finding] = []
    explanatory: set[str] = set()
    if not svc.service_supported():
        return (
            None,
            [
                Finding(
                    "PA-DOC-SERVICE-UNSUPPORTED",
                    "info",
                    f"No supported service manager on {sys.platform}.",
                    "Automatic background-service checks are unavailable.",
                )
            ],
            explanatory,
        )
    if not status.installed:
        return (
            None,
            [
                Finding(
                    "PA-DOC-SERVICE-NOT-INSTALLED",
                    "warning",
                    "The PA service unit is not installed.",
                    "PA will not start automatically.",
                    next_commands=[
                        Command(
                            "pa install --service-only", True, True, active_sessions > 0
                        )
                    ],
                )
            ],
            explanatory,
        )
    if not status.running:
        findings.append(
            Finding(
                "PA-DOC-SERVICE-STOPPED",
                "error",
                f"The {status.backend} service is stopped.",
                "Public and owner APIs are unavailable.",
                next_commands=[Command("pa start", True, True)],
            )
        )
        return None, findings, explanatory
    loaded = svc.loaded_service_definition()
    if loaded is None:
        findings.append(
            Finding(
                "PA-DOC-SERVICE-DEFINITION-UNAVAILABLE",
                "error",
                "The active service manager definition could not be read.",
                "Service drift cannot be verified.",
                next_commands=[
                    Command(
                        "systemctl --user status pa-server.service"
                        if status.backend == "systemd"
                        else "launchctl print gui/$UID/com.pa.server"
                    )
                ],
            )
        )
        return None, findings, explanatory
    if service_bin and loaded.command and not _same_path(service_bin, loaded.command):
        explanatory.add("service_drift")
        findings.append(
            Finding(
                "PA-DOC-SERVICE-BINARY-DRIFT",
                "error",
                "The loaded service executes a different PA binary.",
                "Restarts continue running the wrong installation.",
                {
                    "expected": str(service_bin),
                    "loaded": loaded.command,
                    "unit": loaded.unit_path,
                },
                _repair_commands(active_sessions),
                root_cause="service_drift",
            )
        )
    expected = service_environment(settings)
    semantic_expected = dict(expected)
    if settings.subscribed_realms:
        semantic_expected["PA_SUBSCRIBED_REALMS"] = json.dumps(
            settings.subscribed_realms
        )
    if settings.peers:
        semantic_expected["PA_PEERS"] = json.dumps(settings.peers)
    process_environment = loaded.process_environment or {}
    for name, wanted in semantic_expected.items():
        if not name.startswith("PA_"):
            continue
        actual = loaded.environment.get(name)
        canonical_expected, canonical_loaded = (
            _canonical(name, wanted),
            _canonical(name, actual),
        )
        process = process_environment.get(name)
        evidence = {
            "name": name,
            "expected": _safe(name, wanted),
            "loaded": _safe(name, actual),
            "expected_source": str(settings.data_dir / "config.json"),
            "loaded_source": loaded.unit_path,
            "drop_ins": list(loaded.drop_in_paths),
            "supplying_files": _environment_sources(loaded, name),
            "process": _safe(name, process),
        }
        if actual is None and process is not None:
            explanatory.add("stale_process_environment")
            findings.append(
                Finding(
                    "PA-DOC-SERVICE-PROCESS-ENV-STALE",
                    "error",
                    f"The running process retains obsolete {name} even though the loaded unit no longer supplies it.",
                    "Runtime behavior may override config.json until PA restarts.",
                    evidence,
                    [Command("pa restart", True, True, active_sessions > 0)],
                    root_cause="stale_process_environment",
                )
            )
            continue
        if actual is None and name not in expected:
            continue
        if canonical_expected == canonical_loaded:
            if wanted != actual or name not in expected:
                findings.append(
                    Finding(
                        "PA-DOC-SERVICE-ENV-NORMALIZATION",
                        "info",
                        f"{name} is semantically equal to config.json despite redundant or different serialization.",
                        "No service repair is required; regeneration may remove the redundant service value.",
                        evidence,
                    )
                )
            if process is not None and _canonical(name, process) != canonical_loaded:
                explanatory.add("stale_process_environment")
                findings.append(
                    Finding(
                        "PA-DOC-SERVICE-PROCESS-ENV-STALE",
                        "error",
                        f"The running process has a stale {name} value.",
                        "Runtime behavior differs from the loaded unit.",
                        evidence,
                        [Command("pa restart", True, True, active_sessions > 0)],
                        root_cause="stale_process_environment",
                    )
                )
            continue
        explanatory.add("service_drift")
        code = (
            "PA-DOC-SERVICE-ENV-MISSING"
            if actual is None
            else "PA-DOC-SERVICE-ENV-DRIFT"
        )
        findings.append(
            Finding(
                code,
                "error",
                f"The loaded service value for {name} differs from resolved configuration.",
                "The server and CLI may resolve different endpoints or fleet settings.",
                evidence,
                _repair_commands(active_sessions),
                root_cause="service_drift",
            )
        )
    return loaded, findings, explanatory


def _socket_evidence(path: Path) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "path": str(path),
        "exists": path.exists() or path.is_symlink(),
    }
    try:
        info = path.lstat()
        evidence.update(
            type="socket" if stat.S_ISSOCK(info.st_mode) else "other",
            owner_uid=info.st_uid,
            mode=oct(stat.S_IMODE(info.st_mode)),
        )
    except OSError as exc:
        evidence["path_error"] = f"{type(exc).__name__}: errno={exc.errno}"
    try:
        parent = path.parent.stat()
        evidence["parent"] = {
            "path": str(path.parent),
            "owner_uid": parent.st_uid,
            "mode": oct(stat.S_IMODE(parent.st_mode)),
            "searchable": os.access(path.parent, os.X_OK),
        }
    except OSError as exc:
        evidence["parent"] = {
            "path": str(path.parent),
            "error": f"{type(exc).__name__}: errno={exc.errno}",
        }
    if evidence.get("type") == "socket":
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            probe.settimeout(0.2)
            probe.connect(str(path))
            evidence["listener"] = "accepting"
        except PermissionError:
            evidence["listener"] = "permission_denied"
        except ConnectionRefusedError:
            evidence["listener"] = "stale_socket"
        except OSError as exc:
            evidence["listener"] = f"unreachable_errno_{exc.errno}"
        finally:
            probe.close()
    else:
        evidence["listener"] = "missing"
    return evidence


def _owner_findings(
    settings: Any,
    environment: Mapping[str, str],
    public_status: dict[str, Any] | None,
    service_causes: set[str],
) -> tuple[bool, list[Finding]]:
    endpoint = owner_endpoint(settings, environment)
    configured_endpoint = owner_endpoint(settings, os.environ)
    evidence: dict[str, Any] = {
        "configured_endpoint": _safe(
            "PA_OWNER_API_URL",
            os.environ.get("PA_OWNER_API_URL") or os.environ.get("PA_OWNER_SOCKET"),
        ),
        "loaded_endpoint": endpoint.uds or endpoint.url,
        "expected_endpoint": configured_endpoint.uds or configured_endpoint.url,
        "expected_socket_path": configured_endpoint.uds,
        "endpoint_type": endpoint.kind,
    }
    if endpoint.uds:
        evidence["socket"] = _socket_evidence(Path(endpoint.uds))
    reported = dict((public_status or {}).get("owner_channel") or {})
    evidence["server_listener"] = {
        key: reported.get(key)
        for key in (
            "endpoint_type",
            "state",
            "last_success",
            "last_failure",
            "failure_classification",
            "retry_state",
        )
    }
    try:
        probe_owner_channel(settings, timeout=4.0, environment=environment)
        return True, []
    except OwnerChannelError as exc:
        listener = evidence.get("socket", {}).get("listener")
        classification = {
            "missing": "MISSING-LISTENER",
            "stale_socket": "STALE-SOCKET",
            "permission_denied": "PERMISSION-DENIED",
        }.get(listener, exc.classification.upper().replace("_", "-"))
        causal = bool(service_causes & {"service_drift", "stale_process_environment"})
        cause = (
            "Service environment drift selects the wrong owner endpoint; repair that shared root cause first."
            if causal
            else f"The owner channel probe failed: {classification.lower().replace('-', ' ')}."
        )
        commands = (
            _repair_commands(int((public_status or {}).get("session_count") or 0))
            if causal
            else [
                Command("pa logs --stderr -n 100"),
                Command(
                    "pa restart",
                    True,
                    True,
                    bool((public_status or {}).get("session_count")),
                ),
            ]
        )
        return False, [
            Finding(
                f"PA-DOC-OWNER-{classification}",
                "error",
                cause,
                "ACP sessions cannot use PA tools and the MCP handshake cannot complete.",
                evidence,
                commands,
                "pa doctor --verbose",
                root_cause="service_drift" if causal else "owner_channel",
            )
        ]


def _provider_findings(settings: Any) -> list[Finding]:
    from pa.acp.providers.registry import DEFAULT_PROVIDER_ID
    from pa.acp.providers.resolve import list_provider_summaries

    selected = (settings.agent_provider or DEFAULT_PROVIDER_ID).strip().lower()
    findings = []
    for provider in list_provider_summaries(settings.data_dir):
        provider_id = str(provider.get("id") or "unknown")
        if provider.get("available"):
            continue
        required = provider_id == selected
        severity = "error" if required and settings.agent_enabled else "info"
        commands: list[Command] = []
        if required:
            commands = [
                Command(f"pa agent-provider install --provider {provider_id}", True),
                Command(f"pa agent-provider login --provider {provider_id}", True),
                Command(f"pa agent-provider status --provider {provider_id}"),
            ]
        findings.append(
            Finding(
                "PA-DOC-PROVIDER-REQUIRED-UNAVAILABLE"
                if required
                else "PA-DOC-PROVIDER-OPTIONAL-UNINSTALLED",
                severity,
                f"{provider_id} is {'the configured provider and is unavailable' if required else 'optional and intentionally unused/unavailable'}.",
                "Agent execution is blocked."
                if required
                else "No impact while another configured provider is usable.",
                {"provider": provider_id, "configured": required, "available": False},
                commands,
            )
        )
    return findings


def _dedupe_plan(findings: list[Finding]) -> list[Command]:
    commands, seen = [], set()
    priority = {"service_drift": 0, "stale_process_environment": 0, "owner_channel": 1}
    for finding in sorted(
        findings, key=lambda item: priority.get(item.root_cause or "", 2)
    ):
        for command in finding.next_commands:
            if command.command not in seen:
                seen.add(command.command)
                commands.append(command)
    if findings and "pa doctor --verbose" not in seen:
        commands.append(Command("pa doctor --verbose"))
    return commands


def _render_human(findings: list[Finding], plan: list[Command], verbose: bool) -> None:
    for finding in findings:
        label = {"error": "FAIL", "warning": "WARN", "info": "INFO"}.get(
            finding.severity, "INFO"
        )
        ui.status(
            label,
            f"{finding.code}: {finding.cause}",
            err=finding.severity == "error",
        )
        ui.echo(
            f"         Impact: {finding.impact}",
            style="muted",
            err=finding.severity == "error",
        )
        if verbose and finding.evidence:
            typer.echo(
                "         Evidence: " + json.dumps(finding.evidence, sort_keys=True)
            )
        if finding.next_commands:
            typer.echo(
                "         Next: "
                + " ; ".join(command.command for command in finding.next_commands)
            )
        typer.echo(f"         Verify: {finding.verification_command}")
    if plan:
        ui.heading(
            "\nOrdered repair plan (review before running; doctor made no changes):"
        )
        for index, command in enumerate(plan, 1):
            flags = [
                label
                for enabled, label in (
                    (command.mutates_state, "mutates state"),
                    (command.restarts_pa, "restarts PA"),
                    (command.may_interrupt_sessions, "may interrupt sessions"),
                )
                if enabled
            ]
            typer.echo(
                f"  {index}. {command.command}"
                + (f"  [{', '.join(flags)}]" if flags else "  [read-only]")
            )


def run_doctor(*, verbose: bool = False, json_output: bool = False) -> int:
    settings = get_settings()
    config = load_instance_config(settings.data_dir)
    install = load_install_metadata(settings.data_dir)
    status = svc.get_status(settings)
    findings: list[Finding] = []
    pa_bin, service_bin = svc.find_pa_binary(), svc.find_service_binary()
    if not pa_bin:
        findings.append(
            Finding(
                "PA-DOC-BINARY-MISSING",
                "error",
                "The pa binary is not in PATH.",
                "CLI repair and service commands cannot run.",
            )
        )
    if not config:
        findings.append(
            Finding(
                "PA-DOC-CONFIG-MISSING",
                "error",
                "config.json is missing.",
                "The instance identity cannot be resolved.",
                next_commands=[Command("pa init", True)],
            )
        )
    if not install:
        findings.append(
            Finding(
                "PA-DOC-INSTALL-METADATA-MISSING",
                "warning",
                "install.json is missing.",
                "Installed-version comparisons are unavailable.",
                next_commands=[Command("pa install --record-only", True)],
            )
        )
    if svc.legacy_plaintext_sync_credential():
        findings.append(
            Finding(
                "PA-DOC-FLEET-CREDENTIAL-LEGACY-PLAINTEXT",
                "warning",
                "The host service definition contains a legacy plaintext fleet credential.",
                "Routine service-manager inspection may disclose a usable fleet credential.",
                {"credential_value": "<redacted>"},
                [
                    Command("pa install --service-only", True),
                    Command("pa restart", True, True, True),
                    Command(
                        "pa fleet rotate-credential --idempotency-key legacy-exposure-<date>",
                        True,
                    ),
                ],
                "pa service-inspect",
                root_cause="legacy_plaintext_credential",
            )
        )
    credential_state = svc.sync_credential_permissions(settings)
    if credential_state in {"missing", "unsafe_permissions"}:
        findings.append(
            Finding(
                "PA-DOC-FLEET-CREDENTIAL-STORAGE",
                "error",
                "The managed fleet credential file is missing or not mode 0600.",
                "The host service cannot load the credential through protected storage.",
                {"state": credential_state, "credential_value": "<redacted>"},
                [Command("pa install --service-only", True)],
                "pa service-inspect",
                root_cause="credential_storage",
            )
        )
    instance_url = settings.instance_url or f"http://{settings.host}:{settings.port}"
    public_ok = asyncio.run(_check_health(instance_url, settings.sync_token))
    public_status = asyncio.run(_fetch_status(instance_url, settings.sync_token))
    if not public_ok:
        findings.append(
            Finding(
                "PA-DOC-PUBLIC-HEALTH-UNREACHABLE",
                "error",
                f"The public health endpoint at {instance_url} is unreachable.",
                "PA's UI and HTTP API are unavailable.",
                next_commands=[
                    Command("pa logs --stderr -n 100"),
                    Command("pa restart", True, True),
                ],
            )
        )
    active_sessions = int((public_status or {}).get("session_count") or 0)
    cli_version = _binary_version(pa_bin)
    service_version = _binary_version(service_bin)
    running_version = str((public_status or {}).get("version") or "") or None
    installed_version = install.version if install else None
    observed_versions = {
        "module": __version__,
        "cli": cli_version,
        "service": service_version,
        "running": running_version,
        "installed": installed_version,
    }
    known_versions = {value for value in observed_versions.values() if value}
    if len(known_versions) > 1:
        findings.append(
            Finding(
                "PA-DOC-PINNED-VERSION-MISMATCH",
                "error",
                "The CLI, loaded service, running server, or install metadata report different PA versions.",
                "ACP may launch a stale executable with an incompatible MCP or owner-channel contract.",
                observed_versions,
                _repair_commands(active_sessions),
                root_cause="service_drift",
            )
        )
    loaded, service_findings, service_causes = _service_findings(
        settings, status, service_bin, active_sessions
    )
    findings.extend(service_findings)
    environment = dict(loaded.environment if loaded else os.environ)
    # Prefer the running server's authoritative ephemeral socket over a CLI-side
    # fallback derived from a potentially different runtime directory.
    reported_owner = dict((public_status or {}).get("owner_channel") or {})
    discovered_socket = str(reported_owner.get("socket_path") or "").strip()
    if reported_owner.get("endpoint_type") == "unix" and discovered_socket:
        environment["PA_OWNER_SOCKET"] = discovered_socket
    provider_environment = dict(
        (public_status or {}).get("provider_execution_environment") or {}
    )
    sanitized = sanitize_provider_environment(environment)
    leaked = private_provider_environment_names(sanitized)
    if not provider_environment.get("sanitized") or leaked:
        findings.append(
            Finding(
                "PA-DOC-PROVIDER-ENV-UNSAFE",
                "error",
                "The provider execution environment is not confirmed secret-safe.",
                "Private PA control values could reach an agent subprocess.",
                {
                    "server_sanitized": provider_environment.get("sanitized"),
                    "private_names_retained": sorted(leaked),
                },
                [Command("pa restart", True, True, active_sessions > 0)],
            )
        )
    owner_ok, owner_findings = _owner_findings(
        settings, environment, public_status, service_causes
    )
    findings.extend(owner_findings)
    # A subsequent doctor run performs MCP immediately once owner reachability returns.
    if owner_ok:
        try:
            probe_pa_mcp_stdio(settings, timeout=12.0, owner_environment=environment)
        except McpHandshakeError as exc:
            incompatible = exc.classification == "dependency_incompatible"
            findings.append(
                Finding(
                    "PA-DOC-MCP-DEPENDENCY-INCOMPATIBLE"
                    if incompatible
                    else "PA-DOC-MCP-HANDSHAKE-FAILED",
                    "error",
                    f"The MCP handshake failed: {exc.classification}.",
                    "Agents cannot enumerate or call PA tools.",
                    {
                        "phase": exc.phase,
                        "detail": redact_log_text(exc.detail),
                        "root_exception": redact_log_text(
                            exc.root_exception or "unknown"
                        ),
                        **{
                            key: _safe(key, value)
                            for key, value in exc.context.items()
                        },
                    },
                    [
                        Command("pa doctor --verbose"),
                        Command("pa update", True)
                        if incompatible
                        else Command("pa logs --stderr -n 100"),
                        Command("pa restart", True, True, active_sessions > 0)
                        if incompatible
                        else Command("pa version"),
                    ],
                    root_cause="mcp_dependency"
                    if incompatible
                    else "mcp_bootstrap",
                )
            )
    findings.extend(_provider_findings(settings))
    if settings.peers:
        for peer, ok in asyncio.run(_check_peers(settings.peers, settings.sync_token)):
            if not ok:
                findings.append(
                    Finding(
                        "PA-DOC-PEER-UNREACHABLE",
                        "warning",
                        f"Peer {peer} is unreachable.",
                        "Replication with this peer is delayed.",
                        {"peer": peer},
                        [Command(f"curl -fsS {peer.rstrip('/')}/api/health")],
                    )
                )
    plan = _dedupe_plan(
        [item for item in findings if item.severity in {"error", "warning"}]
    )
    payload = {
        "schema_version": 1,
        "instance": {
            "name": settings.instance_name,
            "id": settings.instance_id,
            "data_dir": str(settings.data_dir),
        },
        "read_only": True,
        "summary": {
            level: sum(item.severity == level for item in findings)
            for level in ("error", "warning", "info")
        },
        "diagnostics": [asdict(item) for item in findings],
        "repair_plan": [asdict(item) for item in plan],
    }
    if json_output:
        typer.echo(json.dumps(payload, indent=2, sort_keys=True))
    else:
        ui.heading(f"PA doctor — {settings.instance_name}\n")
        _render_human(findings, plan, verbose)
        failed = any(item.severity == "error" for item in findings)
        ui.echo(
            "\nDoctor found blocking failures."
            if failed
            else "\nDoctor checks passed.",
            style="failure" if failed else "success",
            err=failed,
        )
    return 1 if any(item.severity == "error" for item in findings) else 0
