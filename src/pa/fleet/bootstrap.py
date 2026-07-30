"""Durable, resumable fleet-machine onboarding.

The bootstrap job is the canonical record for turning an SSH target into a PA
worker. Secret input is kept in a process-local vault; every persisted field is
safe to return through ordinary authenticated APIs.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import platform
import shlex
import subprocess
import threading
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import httpx
from pydantic import BaseModel, Field, model_validator

from pa.config import Settings
from pa.core.io import atomic_write_json
from pa.core.logging import redact_log_text
from pa.domain.models import FleetInstance
from pa.fleet.policy import (
    WORKLOAD_PROFILES,
    InstanceParticipationPolicy,
    ParticipationMode,
)
from pa.fleet.registry import FleetRegistry
from pa.fleet.remote_install import (
    InstallJobStore,
    RemoteInstallRequest,
    _connect_ssh,
    run_install_job,
)

BOOTSTRAP_SCHEMA_VERSION = 1
TERMINAL_BOOTSTRAP_STATES = {
    "ready",
    "partially_ready",
    "blocked",
    "cancelled",
}
_MAX_LOG_EVENTS = 2000
_MAX_JOBS = 200
_RETENTION_DAYS = 90
BootstrapInputKind = Literal[
    "host_key",
    "ssh_password",
    "key_passphrase",
    "sudo_password",
    "provider_login",
    "github_login",
    "operator_confirmation",
]


class BootstrapPhase(StrEnum):
    RESOLVE_TARGET = "resolve_target"
    PREFLIGHT_HOST = "preflight_host"
    INSTALL_PA = "install_pa"
    JOIN_FLEET = "join_fleet"
    START_SERVICE = "start_service"
    VERIFY_PA = "verify_pa"
    INSTALL_PROVIDERS = "install_providers"
    PROVIDER_AUTH = "provider_auth"
    GITHUB_REPOSITORIES = "github_repositories"
    APPLY_POLICY = "apply_policy"
    RUN_PROBES = "run_probes"
    SMOKE_DISPATCH = "smoke_dispatch"
    CLASSIFY_READINESS = "classify_readiness"


class BootstrapState(StrEnum):
    PLANNED = "planned"
    RUNNING = "running"
    WAITING_INPUT = "waiting_input"
    RETRYABLE = "retryable"
    READY = "ready"
    PARTIALLY_READY = "partially_ready"
    BLOCKED = "blocked"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"


class PhaseState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    SKIPPED = "skipped"
    WAITING_INPUT = "waiting_input"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ReadinessClass(StrEnum):
    PENDING = "pending"
    READY = "ready"
    PARTIALLY_READY = "partially_ready"
    BLOCKED = "blocked"


class BootstrapRequest(BaseModel):
    """Non-secret requested onboarding profile."""

    target: str = Field(min_length=1, max_length=500)
    instance_name: str = Field(min_length=1, max_length=120)
    instance_url: str = Field(min_length=1, max_length=1000)
    user: str = Field(default="", max_length=255)
    host: str = Field(default="", max_length=500)
    port: int = Field(default=22, ge=1, le=65535)
    identity_file: str = Field(default="", max_length=2000)
    proxy_jump: str = Field(default="", max_length=1000)
    channel: Literal["release", "beta", "dev"] = "release"
    release_ref: str = Field(default="", max_length=255)
    realm: str = Field(default="default", min_length=1, max_length=255)
    existing_install_action: Literal[
        "abort", "join_only", "repair", "upgrade", "replace", "install"
    ] = "install"
    confirm_replace: bool = False
    worker_profile: Literal["sync_ui", "manual", "research", "code", "operations"] = (
        "manual"
    )
    providers: list[str] = Field(default_factory=list, max_length=20)
    repositories: list[str] = Field(default_factory=list, max_length=100)
    github_transport: Literal["none", "https", "ssh"] = "none"
    browser: bool = False
    repository_cache: bool = False
    automatic_placement: bool = False
    dispatch_capacity: int = Field(default=1, ge=1, le=256)
    provider_capacities: dict[str, int] = Field(default_factory=dict)
    smoke_dispatch: bool = False
    smoke_card_id: str = Field(default="", max_length=255)
    host_key_policy: Literal["strict", "pinned"] = "strict"
    host_key_fingerprint: str = Field(default="", max_length=255)
    sudo_policy: Literal["never", "prompt"] = "never"

    @model_validator(mode="after")
    def normalize(self) -> BootstrapRequest:
        self.target = self.target.strip()
        self.host = self.host.strip()
        self.user = self.user.strip()
        self.instance_url = self.instance_url.rstrip("/")
        self.providers = sorted(
            {item.strip().lower() for item in self.providers if item.strip()}
        )
        self.repositories = sorted(
            {item.strip() for item in self.repositories if item.strip()}
        )
        self.provider_capacities = {
            str(key): int(value)
            for key, value in sorted(self.provider_capacities.items())
            if int(value) > 0
        }
        if self.automatic_placement and self.worker_profile in {"sync_ui", "manual"}:
            raise ValueError(
                "automatic_placement requires a verified research, code, or operations profile"
            )
        if self.host_key_policy == "pinned" and not self.host_key_fingerprint:
            raise ValueError("pinned host-key policy requires host_key_fingerprint")
        if self.smoke_dispatch and not self.smoke_card_id:
            raise ValueError("smoke_dispatch requires smoke_card_id")
        if self.existing_install_action == "replace" and not self.confirm_replace:
            raise ValueError(
                "replace requires confirm_replace=true because it may discard target state"
            )
        return self


class TargetDiscovery(BaseModel):
    target: str
    host: str
    user: str
    port: int
    identity_files: list[str] = Field(default_factory=list)
    proxy_jump: str = ""
    hostname_source: str = "request"
    host_key_fingerprint: str = ""
    host_key_algorithm: str = ""
    host_key_known: bool = False
    host_key_state: Literal["known", "unknown", "changed", "unavailable"] = (
        "unavailable"
    )
    warnings: list[str] = Field(default_factory=list)


class BootstrapPhaseRecord(BaseModel):
    phase: BootstrapPhase
    state: PhaseState = PhaseState.PENDING
    attempts: int = 0
    started_at: datetime | None = None
    completed_at: datetime | None = None
    summary: str = ""
    recovery_action: str = ""


class BootstrapLogEvent(BaseModel):
    seq: int
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    phase: BootstrapPhase | None = None
    level: Literal["info", "warning", "error", "audit"] = "info"
    category: str
    message: str
    privileged: bool = False
    secret_bearing: bool = False


class RequiredInput(BaseModel):
    kind: BootstrapInputKind
    prompt: str
    phase: BootstrapPhase
    expires_at: datetime | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class BootstrapJob(BaseModel):
    schema_version: int = BOOTSTRAP_SCHEMA_VERSION
    job_id: str = Field(default_factory=lambda: str(uuid4()))
    idempotency_key: str
    request_digest: str
    actor: str
    authority_instance_id: str
    authority_url: str
    state: BootstrapState = BootstrapState.PLANNED
    readiness: ReadinessClass = ReadinessClass.PENDING
    readiness_reason: str = ""
    current_phase: BootstrapPhase = BootstrapPhase.RESOLVE_TARGET
    request: BootstrapRequest
    discovery: TargetDiscovery | None = None
    phases: list[BootstrapPhaseRecord] = Field(default_factory=list)
    plan: list[dict[str, Any]] = Field(default_factory=list)
    checkpoints: dict[str, dict[str, Any]] = Field(default_factory=dict)
    evidence: dict[str, Any] = Field(default_factory=dict)
    required_input: RequiredInput | None = None
    log_events: list[BootstrapLogEvent] = Field(default_factory=list)
    linked_instance_id: str = ""
    linked_provider_login_jobs: dict[str, str] = Field(default_factory=dict)
    linked_repositories: list[str] = Field(default_factory=list)
    linked_smoke_dispatch_id: str = ""
    cancel_requested: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @model_validator(mode="after")
    def ensure_phases(self) -> BootstrapJob:
        existing = {record.phase: record for record in self.phases}
        self.phases = [
            existing.get(phase, BootstrapPhaseRecord(phase=phase))
            for phase in BootstrapPhase
        ]
        return self

    def phase_record(self, phase: BootstrapPhase) -> BootstrapPhaseRecord:
        return next(record for record in self.phases if record.phase == phase)


class BootstrapSecretVault:
    """Process-local, one-job secret storage with explicit consumption."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._values: dict[str, dict[str, str]] = {}

    def put(self, job_id: str, values: dict[str, str]) -> None:
        cleaned = {key: value for key, value in values.items() if value}
        if not cleaned:
            return
        with self._lock:
            self._values.setdefault(job_id, {}).update(cleaned)

    def get(self, job_id: str) -> dict[str, str]:
        with self._lock:
            return dict(self._values.get(job_id, {}))

    def clear(self, job_id: str, *names: str) -> None:
        with self._lock:
            if not names:
                self._values.pop(job_id, None)
                return
            values = self._values.get(job_id)
            if not values:
                return
            for name in names:
                values.pop(name, None)
            if not values:
                self._values.pop(job_id, None)


class BootstrapJobStore:
    """Atomic JSON-backed bootstrap jobs with restart recovery."""

    def __init__(self, data_dir: Path) -> None:
        self.directory = data_dir / "fleet_bootstrap_jobs"
        self.directory.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._jobs: dict[str, BootstrapJob] = {}
        self._idempotency: dict[str, str] = {}
        self.secrets = BootstrapSecretVault()
        self._load()

    def _load(self) -> None:
        with self._lock:
            for path in sorted(self.directory.glob("*.json")):
                try:
                    job = BootstrapJob.model_validate_json(path.read_text())
                except OSError, ValueError:
                    continue
                if job.state in {BootstrapState.RUNNING, BootstrapState.CANCELLING}:
                    job.state = BootstrapState.RETRYABLE
                    job.readiness_reason = (
                        "The authority restarted while this job was active; resume "
                        "from the first incomplete phase."
                    )
                    job.required_input = None
                    self._persist(job)
                self._jobs[job.job_id] = job
                self._idempotency[job.idempotency_key] = job.job_id
            self._prune()

    def create(
        self,
        request: BootstrapRequest,
        *,
        idempotency_key: str,
        actor: str,
        authority_instance_id: str,
        authority_url: str,
        discovery: TargetDiscovery | None,
        secrets: dict[str, str] | None = None,
    ) -> tuple[BootstrapJob, bool]:
        digest = _request_digest(request)
        with self._lock:
            existing_id = self._idempotency.get(idempotency_key)
            if existing_id:
                existing = self._jobs[existing_id]
                if existing.request_digest != digest:
                    raise ValueError(
                        "idempotency key is already bound to a different bootstrap request"
                    )
                if secrets:
                    self.secrets.put(existing.job_id, secrets)
                return existing, True
            job = BootstrapJob(
                idempotency_key=idempotency_key,
                request_digest=digest,
                actor=actor,
                authority_instance_id=authority_instance_id,
                authority_url=authority_url.rstrip("/"),
                request=request,
                discovery=discovery,
                plan=build_bootstrap_plan(request, discovery),
            )
            self._jobs[job.job_id] = job
            self._idempotency[idempotency_key] = job.job_id
            if secrets:
                self.secrets.put(job.job_id, secrets)
            self.append(
                job,
                category="job_created",
                message="Durable bootstrap plan created; no target mutation has run.",
                level="audit",
            )
            self._persist(job)
            self._prune()
            return job, False

    def get(self, job_id: str) -> BootstrapJob | None:
        with self._lock:
            return self._jobs.get(job_id)

    def list(self, *, include_terminal: bool = True) -> list[BootstrapJob]:
        with self._lock:
            jobs = list(self._jobs.values())
        if not include_terminal:
            jobs = [
                job for job in jobs if job.state.value not in TERMINAL_BOOTSTRAP_STATES
            ]
        return sorted(jobs, key=lambda item: item.created_at, reverse=True)

    def save(self, job: BootstrapJob) -> BootstrapJob:
        with self._lock:
            job.updated_at = datetime.now(UTC)
            self._jobs[job.job_id] = job
            self._persist(job)
            return job

    def append(
        self,
        job: BootstrapJob,
        *,
        category: str,
        message: object,
        phase: BootstrapPhase | None = None,
        level: Literal["info", "warning", "error", "audit"] = "info",
        privileged: bool = False,
        secret_bearing: bool = False,
    ) -> None:
        safe = redact_log_text(message)
        if secret_bearing:
            safe = "Secret-bearing input was handled in memory and discarded."
        with self._lock:
            seq = job.log_events[-1].seq + 1 if job.log_events else 1
            job.log_events.append(
                BootstrapLogEvent(
                    seq=seq,
                    phase=phase,
                    level=level,
                    category=category[:120],
                    message=safe[:4000],
                    privileged=privileged,
                    secret_bearing=secret_bearing,
                )
            )
            if len(job.log_events) > _MAX_LOG_EVENTS:
                job.log_events = job.log_events[-1500:]
            job.updated_at = datetime.now(UTC)
            self._persist(job)

    def _persist(self, job: BootstrapJob) -> None:
        atomic_write_json(
            self.directory / f"{job.job_id}.json",
            job.model_dump(mode="json"),
            mode=0o600,
        )

    def _prune(self) -> None:
        cutoff = datetime.now(UTC) - timedelta(days=_RETENTION_DAYS)
        removable = sorted(
            (
                job
                for job in self._jobs.values()
                if job.state.value in TERMINAL_BOOTSTRAP_STATES
                and job.updated_at < cutoff
            ),
            key=lambda item: item.updated_at,
        )
        terminal = sorted(
            (
                job
                for job in self._jobs.values()
                if job.state.value in TERMINAL_BOOTSTRAP_STATES
            ),
            key=lambda item: item.updated_at,
        )
        if len(self._jobs) > _MAX_JOBS:
            removable.extend(terminal[: len(self._jobs) - _MAX_JOBS])
        for job in {item.job_id: item for item in removable}.values():
            self._jobs.pop(job.job_id, None)
            self._idempotency.pop(job.idempotency_key, None)
            self.secrets.clear(job.job_id)
            try:
                (self.directory / f"{job.job_id}.json").unlink()
            except FileNotFoundError:
                pass


def _request_digest(request: BootstrapRequest) -> str:
    payload = json.dumps(
        request.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def build_bootstrap_plan(
    request: BootstrapRequest, discovery: TargetDiscovery | None
) -> list[dict[str, Any]]:
    mutations = {
        BootstrapPhase.RESOLVE_TARGET: False,
        BootstrapPhase.PREFLIGHT_HOST: False,
        BootstrapPhase.INSTALL_PA: request.existing_install_action
        not in {"abort", "join_only"},
        BootstrapPhase.JOIN_FLEET: request.existing_install_action != "abort",
        BootstrapPhase.START_SERVICE: request.existing_install_action != "abort",
        BootstrapPhase.VERIFY_PA: False,
        BootstrapPhase.INSTALL_PROVIDERS: bool(request.providers),
        BootstrapPhase.PROVIDER_AUTH: bool(request.providers),
        BootstrapPhase.GITHUB_REPOSITORIES: bool(
            request.repositories or request.github_transport != "none"
        ),
        BootstrapPhase.APPLY_POLICY: True,
        BootstrapPhase.RUN_PROBES: False,
        BootstrapPhase.SMOKE_DISPATCH: request.smoke_dispatch,
        BootstrapPhase.CLASSIFY_READINESS: False,
    }
    interactions = {
        BootstrapPhase.RESOLVE_TARGET: (
            ["host_key_confirmation"]
            if discovery and discovery.host_key_state != "known"
            else []
        ),
        BootstrapPhase.PREFLIGHT_HOST: ["ssh_auth_if_required"],
        BootstrapPhase.INSTALL_PA: (
            ["sudo_if_required"] if request.sudo_policy == "prompt" else []
        ),
        BootstrapPhase.PROVIDER_AUTH: (
            ["device_or_browser_login"] if request.providers else []
        ),
        BootstrapPhase.GITHUB_REPOSITORIES: (
            ["github_login"] if request.github_transport != "none" else []
        ),
    }
    return [
        {
            "order": index,
            "phase": phase.value,
            "mutation": mutations[phase],
            "privileged": phase
            in {BootstrapPhase.INSTALL_PA, BootstrapPhase.START_SERVICE},
            "required_interactions": interactions.get(phase, []),
        }
        for index, phase in enumerate(BootstrapPhase, 1)
    ]


def _ssh_config(target: str) -> dict[str, list[str]]:
    """Resolve OpenSSH aliases without executing a remote command."""
    try:
        proc = subprocess.run(
            ["ssh", "-G", target],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except OSError, subprocess.TimeoutExpired:
        return {}
    if proc.returncode:
        return {}
    values: dict[str, list[str]] = {}
    for line in proc.stdout.splitlines():
        key, _, value = line.partition(" ")
        if key and value:
            values.setdefault(key.lower(), []).append(value.strip())
    return values


def _known_host_key_lines(host: str, port: int) -> list[str]:
    known_hosts = Path("~/.ssh/known_hosts").expanduser()
    if not known_hosts.is_file():
        return []
    lookup = host if port == 22 else f"[{host}]:{port}"
    try:
        proc = subprocess.run(
            ["ssh-keygen", "-F", lookup, "-f", os.fspath(known_hosts)],
            capture_output=True,
            timeout=5,
            check=False,
        )
    except OSError, subprocess.TimeoutExpired:
        return []
    if proc.returncode:
        return []
    return [
        line
        for line in proc.stdout.decode("utf-8", errors="replace").splitlines()
        if line and not line.startswith("#")
    ]


async def discover_target(target: str) -> TargetDiscovery:
    """Resolve OpenSSH settings and return an untrusted key fingerprint to confirm."""
    config = await asyncio.to_thread(_ssh_config, target)
    raw_host = target.rsplit("@", 1)[-1]
    user_from_target = target.rsplit("@", 1)[0] if "@" in target else ""
    host = (config.get("hostname") or [raw_host])[0]
    user = (config.get("user") or [user_from_target or platform.node()])[0]
    try:
        port = int((config.get("port") or ["22"])[0])
    except ValueError:
        port = 22
    identities = [
        value
        for value in config.get("identityfile", [])
        if value and value.lower() != "none"
    ]
    proxy_jump = (config.get("proxyjump") or [""])[0]
    known_lines = await asyncio.to_thread(_known_host_key_lines, host, port)
    known = bool(known_lines)
    fingerprint = ""
    algorithm = ""
    warnings: list[str] = []
    state: Literal["known", "unknown", "changed", "unavailable"] = (
        "known" if known else "unknown"
    )
    try:
        import asyncssh

        async with asyncio.timeout(15):
            key = await asyncssh.get_server_host_key(host, port)
        fingerprint = str(key.get_fingerprint("sha256"))
        algorithm = str(key.get_algorithm())
        if known_lines:
            known_fingerprints: set[str] = set()
            for line in known_lines:
                parts = line.split()
                if len(parts) < 3:
                    continue
                try:
                    known_key = asyncssh.import_public_key(f"{parts[-2]} {parts[-1]}")
                except Exception:
                    continue
                known_fingerprints.add(str(known_key.get_fingerprint("sha256")))
            state = "known" if fingerprint in known_fingerprints else "changed"
            if state == "changed":
                warnings.append(
                    "The live SSH host key differs from known_hosts; onboarding is blocked."
                )
    except Exception as exc:
        state = "unavailable"
        warnings.append(
            redact_log_text(f"Host-key discovery failed: {type(exc).__name__}: {exc}")
        )
    return TargetDiscovery(
        target=target,
        host=host,
        user=user,
        port=port,
        identity_files=identities,
        proxy_jump=proxy_jump if proxy_jump.lower() != "none" else "",
        hostname_source="openssh_config" if config else "request",
        host_key_fingerprint=fingerprint,
        host_key_algorithm=algorithm,
        host_key_known=known,
        host_key_state=state,
        warnings=warnings,
    )


async def _run_preflight(
    request: BootstrapRequest,
    secrets: dict[str, str],
) -> dict[str, Any]:
    remote = RemoteInstallRequest(
        host=request.host,
        user=request.user,
        port=request.port,
        identity_file=request.identity_file,
        password=secrets.get("password", ""),
        passphrase=secrets.get("passphrase", ""),
        instance_name=request.instance_name,
        instance_url=request.instance_url,
        channel=request.channel,
        realm=request.realm,
        host_key_policy=request.host_key_policy,
        host_key_fingerprint=request.host_key_fingerprint,
        proxy_jump=request.proxy_jump,
    )
    conn = await _connect_ssh(remote)
    command = (
        "set -eu; "
        "printf 'os='; uname -s; printf 'arch='; uname -m; "
        "printf 'home='; printf '%s\\n' \"$HOME\"; "
        "printf 'shell='; printf '%s\\n' \"$SHELL\"; "
        "printf 'disk_kb='; df -Pk \"$HOME\" | awk 'NR==2 {print $4}'; "
        "printf 'service_manager='; "
        "if command -v launchctl >/dev/null; then echo launchd; "
        "elif command -v systemctl >/dev/null; then echo systemd; else echo none; fi; "
        "printf 'pa='; command -v pa || true; "
        "printf 'node='; command -v node || true; "
        "printf 'npm='; command -v npm || true; "
        "printf 'npm_prefix='; npm config get prefix 2>/dev/null || true; "
        "printf 'gh='; command -v gh || true"
    )
    try:
        result = await asyncio.wait_for(conn.run(command, check=False), timeout=30)
    finally:
        conn.close()
        await conn.wait_closed()
    if result.exit_status:
        raise RuntimeError(f"preflight command exited {result.exit_status}")
    values: dict[str, str] = {}
    for line in str(result.stdout).splitlines():
        key, sep, value = line.partition("=")
        if sep:
            values[key.strip()] = value.strip()
    if values.get("os") not in {"Darwin", "Linux"}:
        raise RuntimeError(f"unsupported target OS: {values.get('os') or 'unknown'}")
    if values.get("arch") not in {"arm64", "aarch64", "x86_64", "amd64"}:
        raise RuntimeError(
            f"unsupported target architecture: {values.get('arch') or 'unknown'}"
        )
    if values.get("service_manager") == "none":
        raise RuntimeError("target has neither launchd nor systemd")
    return values


async def _target_api(
    settings: Settings,
    instance_url: str,
    method: str,
    path: str,
    *,
    client: httpx.AsyncClient | None = None,
    body: dict[str, Any] | None = None,
    timeout: float = 30.0,
) -> httpx.Response:
    owns_client = client is None
    client = client or httpx.AsyncClient()
    headers = {"Accept": "application/json"}
    if settings.sync_token:
        headers["Authorization"] = f"Bearer {settings.sync_token}"
    try:
        response = await client.request(
            method,
            f"{instance_url.rstrip('/')}{path}",
            headers=headers,
            json=body,
            timeout=timeout,
        )
        response.raise_for_status()
        return response
    finally:
        if owns_client:
            await client.aclose()


async def _verify_target_pa(
    settings: Settings,
    instance_url: str,
    expected_instance_id: str,
    *,
    client: httpx.AsyncClient | None,
) -> dict[str, Any]:
    ready = await _target_api(
        settings, instance_url, "GET", "/api/ready", client=client
    )
    actual_id = ready.headers.get("X-PA-Instance-ID", "").strip()
    if not actual_id:
        raise RuntimeError("target /api/ready omitted X-PA-Instance-ID")
    if expected_instance_id and actual_id != expected_instance_id:
        raise RuntimeError(
            f"target identity mismatch: expected {expected_instance_id}, received {actual_id}"
        )
    payload = ready.json()
    if payload.get("status") not in {"ready", "ok"}:
        raise RuntimeError(f"target reported non-ready state: {payload}")
    return {
        "ready": True,
        "instance_id": actual_id,
        "build": payload.get("build") or payload.get("version"),
        "response": payload,
    }


async def _install_and_probe_providers(
    settings: Settings,
    instance_url: str,
    providers: list[str],
    *,
    client: httpx.AsyncClient | None,
) -> tuple[dict[str, Any], list[str]]:
    evidence: dict[str, Any] = {}
    authentication_needed: list[str] = []
    for provider in providers:
        installed = await _target_api(
            settings,
            instance_url,
            "POST",
            f"/api/agent/providers/{provider}/install",
            client=client,
            timeout=900.0,
        )
        install_payload = installed.json()
        if not bool(install_payload.get("ok", True)):
            raise RuntimeError(
                f"{provider} installation failed: "
                f"{install_payload.get('message') or install_payload.get('error')}"
            )
        status = await _target_api(
            settings,
            instance_url,
            "GET",
            f"/api/agent/providers/{provider}",
            client=client,
            timeout=60.0,
        )
        provider_status = status.json()
        if not provider_status.get("available"):
            raise RuntimeError(
                f"{provider} is installed but unavailable to the PA service user"
            )
        auth_state = str(provider_status.get("auth_state") or "unknown")
        if auth_state != "authenticated":
            authentication_needed.append(provider)
        evidence[provider] = {
            "installed": bool(provider_status.get("installed")),
            "available": bool(provider_status.get("available")),
            "version": provider_status.get("version"),
            "resolved_path": provider_status.get("resolved_path"),
            "auth_state": auth_state,
            "auth_method": provider_status.get("auth_method"),
        }
    return evidence, authentication_needed


async def _provider_auth_states(
    settings: Settings,
    instance_url: str,
    providers: list[str],
    *,
    client: httpx.AsyncClient | None,
) -> tuple[dict[str, str], list[str]]:
    states: dict[str, str] = {}
    pending: list[str] = []
    for provider in providers:
        response = await _target_api(
            settings,
            instance_url,
            "GET",
            f"/api/agent/providers/{provider}",
            client=client,
            timeout=60.0,
        )
        payload = response.json()
        state = str(payload.get("auth_state") or "unknown")
        states[provider] = state
        if not payload.get("available") or state != "authenticated":
            pending.append(provider)
    return states, pending


async def _probe_providers(
    settings: Settings,
    instance_url: str,
    providers: list[str],
    *,
    client: httpx.AsyncClient | None,
) -> dict[str, Any]:
    evidence: dict[str, Any] = {}
    for provider in providers:
        response = await _target_api(
            settings,
            instance_url,
            "POST",
            f"/api/agent/providers/{provider}/probe",
            client=client,
            timeout=90.0,
        )
        payload = response.json()
        if not bool(payload.get("ok")):
            raise RuntimeError(
                f"{provider} ACP probe failed: "
                f"{payload.get('message') or payload.get('error')}"
            )
        evidence[provider] = {
            key: value
            for key, value in payload.items()
            if key not in {"stdout", "stderr", "env", "secrets"}
        }
    return evidence


async def _probe_github_repositories(
    request: BootstrapRequest,
    secrets: dict[str, str],
) -> dict[str, Any]:
    remote = RemoteInstallRequest(
        host=request.host,
        user=request.user,
        port=request.port,
        identity_file=request.identity_file,
        instance_name=request.instance_name,
        instance_url=request.instance_url,
        host_key_policy=request.host_key_policy,
        host_key_fingerprint=request.host_key_fingerprint,
        proxy_jump=request.proxy_jump,
        password=secrets.get("password", ""),
        passphrase=secrets.get("passphrase", ""),
    )
    conn = await _connect_ssh(remote)
    try:
        commands = [
            "command -v gh >/dev/null",
            "gh auth status >/dev/null 2>&1",
            "gh api user --jq .login >/dev/null",
        ]
        for repository in request.repositories:
            quoted = shlex.quote(repository)
            commands.append(
                f"GIT_TERMINAL_PROMPT=0 git ls-remote --heads {quoted} >/dev/null 2>&1"
            )
            commands.append(
                f"permission=$(gh repo view {quoted} --json viewerPermission "
                "--jq .viewerPermission 2>/dev/null); "
                'case "$permission" in ADMIN|MAINTAIN|WRITE) ;; *) exit 42;; esac'
            )
        result = await asyncio.wait_for(
            conn.run("set -eu; " + "; ".join(commands), check=False),
            timeout=120,
        )
    finally:
        conn.close()
        await conn.wait_closed()
    if result.exit_status == 42:
        raise PermissionError(
            "GitHub repository access is read-only; push/PR permission is required."
        )
    if result.exit_status:
        raise RuntimeError(
            "GitHub CLI authentication or requested repository access is unavailable."
        )
    return {
        "authenticated": True,
        "api": True,
        "transport": request.github_transport,
        "repositories": {
            repository: {"read": True, "push": True, "pr_api": True}
            for repository in request.repositories
        },
    }


def _phase_start(
    store: BootstrapJobStore, job: BootstrapJob, phase: BootstrapPhase
) -> BootstrapPhaseRecord:
    record = job.phase_record(phase)
    record.state = PhaseState.RUNNING
    record.attempts += 1
    record.started_at = datetime.now(UTC)
    record.completed_at = None
    record.recovery_action = ""
    job.current_phase = phase
    job.state = BootstrapState.RUNNING
    store.append(
        job,
        category="phase_started",
        message=f"Started {phase.value.replace('_', ' ')}.",
        phase=phase,
        level="audit",
    )
    return record


def _phase_complete(
    store: BootstrapJobStore,
    job: BootstrapJob,
    phase: BootstrapPhase,
    summary: str,
    *,
    skipped: bool = False,
) -> None:
    record = job.phase_record(phase)
    record.state = PhaseState.SKIPPED if skipped else PhaseState.SUCCEEDED
    record.completed_at = datetime.now(UTC)
    record.summary = redact_log_text(summary)[:2000]
    store.append(
        job,
        category="phase_completed",
        message=summary,
        phase=phase,
        level="audit",
    )


def _pause(
    store: BootstrapJobStore,
    job: BootstrapJob,
    *,
    phase: BootstrapPhase,
    kind: BootstrapInputKind,
    prompt: str,
    details: dict[str, Any] | None = None,
    readiness_reason: str = "",
) -> BootstrapJob:
    record = job.phase_record(phase)
    record.state = PhaseState.WAITING_INPUT
    record.summary = prompt
    record.recovery_action = prompt
    job.state = BootstrapState.WAITING_INPUT
    job.readiness = ReadinessClass.PARTIALLY_READY
    job.readiness_reason = readiness_reason or prompt
    job.required_input = RequiredInput(
        kind=kind,
        prompt=prompt,
        phase=phase,
        expires_at=(
            datetime.now(UTC) + timedelta(minutes=15)
            if kind in {"ssh_password", "key_passphrase", "sudo_password", "host_key"}
            else None
        ),
        details=details or {},
    )
    store.append(
        job,
        category="input_required",
        message=prompt,
        phase=phase,
        level="warning",
    )
    return store.save(job)


def _fail(
    store: BootstrapJobStore,
    job: BootstrapJob,
    *,
    phase: BootstrapPhase,
    error: object,
    recovery_action: str,
    retryable: bool = True,
) -> BootstrapJob:
    safe = redact_log_text(error)
    record = job.phase_record(phase)
    record.state = PhaseState.FAILED
    record.completed_at = datetime.now(UTC)
    record.summary = safe[:2000]
    record.recovery_action = recovery_action
    job.state = BootstrapState.RETRYABLE if retryable else BootstrapState.BLOCKED
    job.readiness = ReadinessClass.BLOCKED
    job.readiness_reason = safe[:2000]
    store.append(
        job,
        category="phase_failed",
        message=safe,
        phase=phase,
        level="error",
    )
    store.secrets.clear(job.job_id)
    return store.save(job)


def _cancel_at_boundary(
    store: BootstrapJobStore, job: BootstrapJob
) -> BootstrapJob | None:
    if not job.cancel_requested:
        return None
    job.state = BootstrapState.CANCELLED
    job.readiness = ReadinessClass.PARTIALLY_READY
    job.readiness_reason = (
        "Cancelled at a safe phase boundary; completed checkpoints were preserved."
    )
    job.completed_at = datetime.now(UTC)
    store.secrets.clear(job.job_id)
    store.append(
        job,
        category="cancelled",
        message=job.readiness_reason,
        phase=job.current_phase,
        level="audit",
    )
    return store.save(job)


async def run_bootstrap_job(
    settings: Settings,
    fleet: FleetRegistry,
    store: BootstrapJobStore,
    job: BootstrapJob,
    *,
    domain_store: Any,
    author_instance_id: str,
    async_runtime: Any = None,
    http_client: httpx.AsyncClient | None = None,
) -> BootstrapJob:
    """Run or resume a bootstrap job from its first incomplete phase."""
    if job.state.value in TERMINAL_BOOTSTRAP_STATES:
        return job
    if job.cancel_requested:
        job.state = BootstrapState.CANCELLED
        job.completed_at = datetime.now(UTC)
        store.secrets.clear(job.job_id)
        return store.save(job)
    job.started_at = job.started_at or datetime.now(UTC)
    job.required_input = None
    secrets = store.secrets.get(job.job_id)

    discovery = job.discovery
    phase = BootstrapPhase.RESOLVE_TARGET
    if job.phase_record(phase).state != PhaseState.SUCCEEDED:
        _phase_start(store, job, phase)
        if discovery is None:
            try:
                discovery = await discover_target(job.request.target)
                job.discovery = discovery
            except Exception as exc:
                return _fail(
                    store,
                    job,
                    phase=phase,
                    error=exc,
                    recovery_action="Correct the SSH target or network path and retry.",
                )
        job.request.host = job.request.host or discovery.host
        job.request.user = job.request.user or discovery.user
        job.request.port = discovery.port
        job.request.identity_file = job.request.identity_file or (
            discovery.identity_files[0] if discovery.identity_files else ""
        )
        job.request.proxy_jump = job.request.proxy_jump or discovery.proxy_jump
        if discovery.host_key_state == "changed":
            return _fail(
                store,
                job,
                phase=phase,
                error=(
                    "The live SSH host key differs from known_hosts. "
                    "Possible host impersonation or host replacement."
                ),
                recovery_action=(
                    "Verify the machine through an independent channel and repair "
                    "known_hosts explicitly before creating a new plan."
                ),
                retryable=False,
            )
        if discovery.host_key_state != "known":
            supplied = job.request.host_key_fingerprint
            if not supplied:
                return _pause(
                    store,
                    job,
                    phase=phase,
                    kind="host_key",
                    prompt=(
                        "Confirm the discovered SSH host-key fingerprint before "
                        "any authenticated connection."
                    ),
                    details={
                        "host": discovery.host,
                        "port": discovery.port,
                        "algorithm": discovery.host_key_algorithm,
                        "fingerprint": discovery.host_key_fingerprint,
                        "state": discovery.host_key_state,
                    },
                )
            if supplied != discovery.host_key_fingerprint:
                return _fail(
                    store,
                    job,
                    phase=phase,
                    error="The pinned SSH fingerprint does not match discovery.",
                    recovery_action=(
                        "Investigate the host-key change; do not bypass verification."
                    ),
                    retryable=False,
                )
            job.request.host_key_policy = "pinned"
        _phase_complete(
            store,
            job,
            phase,
            f"Resolved {job.request.user}@{job.request.host}:{job.request.port}.",
        )
        if cancelled := _cancel_at_boundary(store, job):
            return cancelled

    phase = BootstrapPhase.PREFLIGHT_HOST
    if job.phase_record(phase).state != PhaseState.SUCCEEDED:
        _phase_start(store, job, phase)
        try:
            preflight = await _run_preflight(job.request, secrets)
        except Exception as exc:
            text = str(exc).lower()
            if "permission denied" in text or "auth" in text:
                return _pause(
                    store,
                    job,
                    phase=phase,
                    kind=(
                        "key_passphrase"
                        if job.request.identity_file
                        else "ssh_password"
                    ),
                    prompt=(
                        "SSH authentication is required. Submit a short-lived "
                        "password or key passphrase, then resume."
                    ),
                )
            return _fail(
                store,
                job,
                phase=phase,
                error=exc,
                recovery_action=(
                    "Fix the reported host prerequisite or SSH reachability and retry."
                ),
            )
        job.checkpoints[phase.value] = preflight
        _phase_complete(
            store,
            job,
            phase,
            (
                f"Supported {preflight.get('os')} {preflight.get('arch')} host "
                f"with {preflight.get('service_manager')}."
            ),
        )
        if cancelled := _cancel_at_boundary(store, job):
            return cancelled
        if preflight.get("pa") and job.request.existing_install_action == "install":
            return _fail(
                store,
                job,
                phase=phase,
                error=(
                    "An existing PA executable was detected; a clean install "
                    "would overwrite unknown state."
                ),
                recovery_action=(
                    "Create a new plan selecting join_only, repair, upgrade, "
                    "explicitly confirmed replace, or abort."
                ),
                retryable=False,
            )

    if job.request.existing_install_action == "abort":
        return _fail(
            store,
            job,
            phase=BootstrapPhase.INSTALL_PA,
            error="Operator selected abort for existing target state.",
            recovery_action="Create a new plan with join_only, repair, upgrade, or replace.",
            retryable=False,
        )

    phase = BootstrapPhase.INSTALL_PA
    if job.phase_record(phase).state != PhaseState.SUCCEEDED:
        _phase_start(store, job, phase)
        remote = RemoteInstallRequest(
            host=job.request.host,
            user=job.request.user,
            port=job.request.port,
            identity_file=job.request.identity_file,
            password=secrets.get("password", ""),
            passphrase=secrets.get("passphrase", ""),
            instance_name=job.request.instance_name,
            instance_url=job.request.instance_url,
            channel=job.request.channel,
            realm=job.request.realm,
            join_only=job.request.existing_install_action == "join_only",
            host_key_policy=job.request.host_key_policy,
            host_key_fingerprint=job.request.host_key_fingerprint,
            release_ref=job.request.release_ref,
            proxy_jump=job.request.proxy_jump,
        )
        legacy_store = InstallJobStore(store.directory / "legacy")
        legacy = legacy_store.create(remote)
        try:
            result = await run_install_job(
                settings,
                fleet,
                legacy_store,
                legacy,
                remote,
                async_runtime=async_runtime,
                http_client=http_client,
            )
        finally:
            store.secrets.clear(job.job_id, "password", "passphrase", "sudo_password")
        for line in result.log_lines:
            store.append(
                job,
                category="remote_install",
                message=line,
                phase=phase,
            )
        if result.status.value != "succeeded":
            return _fail(
                store,
                job,
                phase=phase,
                error=result.error or "Remote install failed.",
                recovery_action=(
                    "Correct the remote installer failure and retry; completed "
                    "idempotent work will be reused."
                ),
            )
        job.checkpoints[phase.value] = {
            "release_channel": job.request.channel,
            "release_ref": job.request.release_ref or job.request.channel,
            "completed_by": "remote_install_adapter",
        }
        _phase_complete(store, job, phase, "PA installation command completed.")
        if cancelled := _cancel_at_boundary(store, job):
            return cancelled

    for phase, summary in (
        (BootstrapPhase.JOIN_FLEET, "Fleet join completed idempotently."),
        (BootstrapPhase.START_SERVICE, "PA service registered and started."),
    ):
        if job.phase_record(phase).state != PhaseState.SUCCEEDED:
            _phase_start(store, job, phase)
            _phase_complete(store, job, phase, summary)

    matching: FleetInstance | None = next(
        (
            item
            for item in fleet.list_instances()
            if item.name == job.request.instance_name
            or item.url.rstrip("/") == job.request.instance_url
        ),
        None,
    )
    if matching:
        job.linked_instance_id = matching.instance_id
        job.evidence["instance"] = {
            "instance_id": matching.instance_id,
            "name": matching.name,
            "url": matching.url,
        }
        if job.phase_record(BootstrapPhase.APPLY_POLICY).state != PhaseState.SUCCEEDED:
            # Quarantine the new member before optional setup begins. This is
            # replaced only after the requested policy phase is reached.
            quarantine = InstanceParticipationPolicy(
                realm_id=job.request.realm,
                instance_id=matching.instance_id,
                participation_mode=ParticipationMode.DISABLED,
                automatic_dispatch=False,
                manual_dispatch=False,
                reason=(
                    f"Bootstrap job {job.job_id} is incomplete; placement is disabled."
                ),
                source="fleet_bootstrap_quarantine",
                actor=job.actor,
            )
            domain_store.set_instance_participation_policy(
                quarantine,
                principal_id=job.actor,
                instance_id=author_instance_id,
            )
            job.evidence["quarantine_policy"] = quarantine.model_dump(mode="json")

    phase = BootstrapPhase.VERIFY_PA
    if job.phase_record(phase).state != PhaseState.SUCCEEDED:
        _phase_start(store, job, phase)
        try:
            pa_evidence = await _verify_target_pa(
                settings,
                job.request.instance_url,
                job.linked_instance_id,
                client=http_client,
            )
        except Exception as exc:
            return _fail(
                store,
                job,
                phase=phase,
                error=exc,
                recovery_action=(
                    "Fix target readiness, identity, or authority connectivity and retry."
                ),
            )
        job.evidence["pa_readiness"] = pa_evidence
        _phase_complete(
            store,
            job,
            phase,
            f"Verified ready PA identity {pa_evidence['instance_id']}.",
        )

    phase = BootstrapPhase.INSTALL_PROVIDERS
    if job.phase_record(phase).state != PhaseState.SUCCEEDED:
        _phase_start(store, job, phase)
        if job.request.providers:
            try:
                provider_evidence, auth_pending = await _install_and_probe_providers(
                    settings,
                    job.request.instance_url,
                    job.request.providers,
                    client=http_client,
                )
            except Exception as exc:
                return _fail(
                    store,
                    job,
                    phase=phase,
                    error=exc,
                    recovery_action=(
                        "Fix the target-side provider installation or service PATH "
                        "and retry this phase."
                    ),
                )
            job.evidence["provider_installation"] = provider_evidence
            job.evidence["provider_auth_pending"] = auth_pending
            _phase_complete(
                store,
                job,
                phase,
                "Requested providers are installed and visible to the PA service.",
            )
        else:
            _phase_complete(store, job, phase, "No providers requested.", skipped=True)

    phase = BootstrapPhase.PROVIDER_AUTH
    if job.phase_record(phase).state != PhaseState.SUCCEEDED:
        _phase_start(store, job, phase)
        if job.request.providers:
            try:
                states, pending = await _provider_auth_states(
                    settings,
                    job.request.instance_url,
                    job.request.providers,
                    client=http_client,
                )
            except Exception as exc:
                return _fail(
                    store,
                    job,
                    phase=phase,
                    error=exc,
                    recovery_action="Restore target provider status API access and retry.",
                )
            job.evidence["provider_auth"] = states
            if pending:
                return _pause(
                    store,
                    job,
                    phase=phase,
                    kind="provider_login",
                    prompt=(
                        "Complete target-side authentication for the requested providers, "
                        "then resume so PA can independently re-check authentication."
                    ),
                    details={"providers": pending},
                    readiness_reason="pa_joined_provider_auth_pending",
                )
            _phase_complete(store, job, phase, "Requested providers are authenticated.")
        else:
            _phase_complete(
                store,
                job,
                phase,
                "No provider authentication required.",
                skipped=True,
            )

    phase = BootstrapPhase.GITHUB_REPOSITORIES
    if job.phase_record(phase).state != PhaseState.SUCCEEDED:
        _phase_start(store, job, phase)
        if job.request.repositories or job.request.github_transport != "none":
            try:
                github_evidence = await _probe_github_repositories(job.request, secrets)
            except PermissionError as exc:
                return _fail(
                    store,
                    job,
                    phase=phase,
                    error=exc,
                    recovery_action=(
                        "Grant explicit push/PR access to every requested repository "
                        "or remove it from the worker profile."
                    ),
                    retryable=False,
                )
            except Exception as exc:
                if (
                    not secrets.get("password")
                    and not secrets.get("passphrase")
                    and (
                        "permission denied" in str(exc).lower()
                        or "auth" in str(exc).lower()
                    )
                ):
                    return _pause(
                        store,
                        job,
                        phase=phase,
                        kind=(
                            "key_passphrase"
                            if job.request.identity_file
                            else "ssh_password"
                        ),
                        prompt=(
                            "A fresh short-lived SSH password or key passphrase is "
                            "required to run the GitHub readiness probe."
                        ),
                        readiness_reason="pa_joined_ssh_auth_pending",
                    )
                return _pause(
                    store,
                    job,
                    phase=phase,
                    kind="github_login",
                    prompt=(
                        "Authenticate GitHub on the target, then resume so PA can "
                        "independently verify API, read, push, and PR capability."
                    ),
                    details={
                        "transport": job.request.github_transport,
                        "repositories": job.request.repositories,
                    },
                    readiness_reason="pa_joined_github_auth_pending",
                )
            finally:
                store.secrets.clear(job.job_id, "password", "passphrase")
            job.evidence["github"] = github_evidence
            job.linked_repositories = list(job.request.repositories)
            _phase_complete(
                store,
                job,
                phase,
                "GitHub API and requested repository read/push capability verified.",
            )
        else:
            _phase_complete(
                store,
                job,
                phase,
                "GitHub access was not requested.",
                skipped=True,
            )

    phase = BootstrapPhase.APPLY_POLICY
    if job.phase_record(phase).state != PhaseState.SUCCEEDED:
        _phase_start(store, job, phase)
        if not job.linked_instance_id:
            return _fail(
                store,
                job,
                phase=phase,
                error="Joined instance identity is not yet visible in the canonical fleet.",
                recovery_action="Reconcile fleet membership and retry this phase.",
            )
        allowed_profiles = {
            "research": ["research"],
            "code": ["repository"],
            "operations": ["operations"],
            "sync_ui": [],
            "manual": [],
        }[job.request.worker_profile]
        denied_profiles = (
            list(WORKLOAD_PROFILES)
            if job.request.worker_profile in {"sync_ui", "manual"}
            else []
        )
        mode = ParticipationMode.MANUAL_ONLY
        policy = InstanceParticipationPolicy(
            realm_id=job.request.realm,
            instance_id=job.linked_instance_id,
            participation_mode=mode,
            automatic_dispatch=False,
            manual_dispatch=True,
            allowed_profiles=allowed_profiles,
            denied_profiles=denied_profiles,
            allowed_provider_ids=job.request.providers,
            max_concurrent_by_profile={
                profile: job.request.dispatch_capacity for profile in allowed_profiles
            },
            reason=(
                f"Staged by bootstrap job {job.job_id}; automatic placement remains "
                "disabled until final readiness classification."
            ),
            source="fleet_bootstrap",
            actor=job.actor,
        )
        domain_store.set_instance_participation_policy(
            policy,
            principal_id=job.actor,
            instance_id=author_instance_id,
        )
        job.evidence["participation_policy"] = policy.model_dump(mode="json")
        _phase_complete(
            store,
            job,
            phase,
            "Applied verified manual-only participation and requested capacity.",
        )

    phase = BootstrapPhase.RUN_PROBES
    if job.phase_record(phase).state != PhaseState.SUCCEEDED:
        _phase_start(store, job, phase)
        try:
            provider_probes = (
                await _probe_providers(
                    settings,
                    job.request.instance_url,
                    job.request.providers,
                    client=http_client,
                )
                if job.request.providers
                else {}
            )
        except Exception as exc:
            return _fail(
                store,
                job,
                phase=phase,
                error=exc,
                recovery_action=(
                    "Repair the target ACP runtime or service environment and retry."
                ),
            )
        job.evidence["probes"] = {
            "ssh_preflight": "passed",
            "pa_ready_identity": "passed",
            "provider_probe": provider_probes or "not_requested",
            "repository_probe": "not_requested"
            if not job.request.repositories
            else "passed",
        }
        _phase_complete(store, job, phase, "Required readiness probes passed.")

    phase = BootstrapPhase.SMOKE_DISPATCH
    if job.phase_record(phase).state != PhaseState.SUCCEEDED:
        _phase_start(store, job, phase)
        if job.request.smoke_dispatch:
            return _pause(
                store,
                job,
                phase=phase,
                kind="operator_confirmation",
                prompt=(
                    "Run the bounded smoke card on the named instance and submit "
                    "its terminal dispatch result before final readiness."
                ),
                details={
                    "card_id": job.request.smoke_card_id,
                    "instance_id": job.linked_instance_id,
                },
            )
        _phase_complete(
            store, job, phase, "Smoke dispatch not requested.", skipped=True
        )

    phase = BootstrapPhase.CLASSIFY_READINESS
    if job.phase_record(phase).state != PhaseState.SUCCEEDED:
        _phase_start(store, job, phase)
        _phase_complete(
            store,
            job,
            phase,
            "The requested worker profile is dispatch-ready.",
        )
    if job.request.automatic_placement:
        staged = domain_store.get_instance_participation_policy(
            job.linked_instance_id, job.request.realm
        )
        if not staged:
            return _fail(
                store,
                job,
                phase=phase,
                error="Staged participation policy is missing.",
                recovery_action="Reapply the bootstrap participation phase and retry.",
            )
        enabled = staged.model_copy(deep=True)
        enabled.participation_mode = ParticipationMode.AUTOMATIC
        enabled.automatic_dispatch = True
        enabled.manual_dispatch = True
        enabled.reason = (
            f"Automatically enabled by bootstrap job {job.job_id} after every "
            "requested readiness signal passed."
        )
        enabled.enablement_confirmation_reason = (
            "Operator requested automatic placement in the bootstrap plan."
        )
        enabled.source = "fleet_bootstrap_ready"
        enabled.actor = job.actor
        domain_store.set_instance_participation_policy(
            enabled,
            principal_id=job.actor,
            instance_id=author_instance_id,
        )
        job.evidence["participation_policy"] = enabled.model_dump(mode="json")
    job.state = BootstrapState.READY
    job.readiness = ReadinessClass.READY
    job.readiness_reason = (
        "All required phases and requested capabilities are verified."
    )
    job.completed_at = datetime.now(UTC)
    store.secrets.clear(job.job_id)
    store.append(
        job,
        category="readiness_classified",
        message="Instance classified ready.",
        phase=phase,
        level="audit",
    )
    return store.save(job)


def accept_bootstrap_input(
    store: BootstrapJobStore,
    job: BootstrapJob,
    *,
    kind: str,
    value: str = "",
    confirmed: bool = False,
    details: dict[str, Any] | None = None,
) -> BootstrapJob:
    required = job.required_input
    if not required or required.kind != kind:
        raise ValueError("input does not match the job's current requirement")
    if required.expires_at and required.expires_at < datetime.now(UTC):
        raise ValueError("the short-lived input window expired; retry the phase")
    if kind == "host_key":
        expected = str(required.details.get("fingerprint") or "")
        if not confirmed or not value or value != expected:
            raise ValueError(
                "explicit confirmation of the exact fingerprint is required"
            )
        job.request.host_key_policy = "pinned"
        job.request.host_key_fingerprint = value
        record = job.phase_record(required.phase)
        record.state = PhaseState.PENDING
    elif kind in {"ssh_password", "key_passphrase", "sudo_password"}:
        if not value:
            raise ValueError("secret input cannot be empty")
        key = {
            "ssh_password": "password",
            "key_passphrase": "passphrase",
            "sudo_password": "sudo_password",
        }[kind]
        store.secrets.put(job.job_id, {key: value})
        job.phase_record(required.phase).state = PhaseState.PENDING
    elif kind in {
        "provider_login",
        "github_login",
        "operator_confirmation",
    }:
        if not confirmed:
            raise ValueError("explicit completion confirmation is required")
        detail = details or {}
        if kind == "provider_login":
            job.evidence["provider_auth"] = {
                "confirmed": True,
                "providers": job.request.providers,
                "evidence": detail,
            }
        elif kind == "github_login":
            job.evidence["github"] = {
                "confirmed": True,
                "transport": job.request.github_transport,
                "repositories": job.request.repositories,
                "evidence": detail,
            }
            job.linked_repositories = list(job.request.repositories)
        else:
            dispatch_id = str(detail.get("dispatch_id") or "")
            outcome = str(detail.get("outcome") or "")
            if required.phase == BootstrapPhase.SMOKE_DISPATCH and (
                not dispatch_id or outcome != "succeeded"
            ):
                raise ValueError(
                    "smoke completion requires dispatch_id and outcome=succeeded"
                )
            job.linked_smoke_dispatch_id = dispatch_id
            job.evidence["smoke_dispatch"] = detail
        record = job.phase_record(required.phase)
        if kind in {"provider_login", "github_login"}:
            record.state = PhaseState.PENDING
            record.completed_at = None
            record.summary = (
                f"{kind.replace('_', ' ').title()} completion claimed; "
                "independent verification is pending."
            )
        else:
            record.state = PhaseState.SUCCEEDED
            record.completed_at = datetime.now(UTC)
            record.summary = f"{kind.replace('_', ' ').title()} completion confirmed."
    job.required_input = None
    job.state = BootstrapState.RETRYABLE
    job.readiness = ReadinessClass.PENDING
    job.readiness_reason = "Input accepted; resume the durable job."
    store.append(
        job,
        category="input_accepted",
        message=(
            "Short-lived secret accepted in memory."
            if kind in {"ssh_password", "key_passphrase", "sudo_password"}
            else f"Accepted explicit {kind.replace('_', ' ')} confirmation."
        ),
        phase=required.phase,
        level="audit",
        secret_bearing=kind in {"ssh_password", "key_passphrase", "sudo_password"},
    )
    return store.save(job)
