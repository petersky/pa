"""Persistent controller-side jobs for updating registered fleet peers."""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import time  # noqa: F401 - retained for compatibility with workflow clock mocks
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
from pydantic import BaseModel, Field, field_validator

from pa.config import Settings
from pa.core.io import atomic_write_json
from pa.fleet.registry import FleetRegistry
from pa.update.channels import compare_versions
from pa.update.registry import ReleaseTrack, normalize_track


class UpdatePhase(StrEnum):
    PENDING = "pending"
    PREFLIGHT = "preflight"
    QUIESCING = "quiescing"
    INSTALLING = "installing"
    RESTARTING = "restarting"
    WAITING_INSTALL = "waiting_install"
    VERIFYING = "verifying"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


TERMINAL_PHASES = {UpdatePhase.SUCCEEDED, UpdatePhase.FAILED}


class FleetUpdateRequest(BaseModel):
    channel: str | None = Field(default=None, pattern=r"^[A-Za-z0-9._-]{1,32}$")
    target_version: str | None = Field(default=None, pattern=r"^[A-Za-z0-9._+-]{1,64}$")
    quiesce_timeout: float = Field(default=300.0, ge=1.0, le=3600.0)
    force: bool = False
    health_timeout: float = Field(default=180.0, ge=10.0, le=1800.0)
    install_timeout: float = Field(default=900.0, ge=10.0, le=7200.0)

    @field_validator("target_version")
    @classmethod
    def normalize_target_version(cls, value: str | None) -> str | None:
        return value.removeprefix("v") if value else None


class FleetUpdateJob(BaseModel):
    job_id: str = Field(default_factory=lambda: str(uuid4()))
    instance_id: str
    instance_name: str
    instance_url: str
    channel: str
    target_version: str | None = None
    quiesce_timeout: float = 300.0
    force: bool = False
    health_timeout: float = 180.0
    install_timeout: float = 900.0
    phase: UpdatePhase = UpdatePhase.PENDING
    current_version: str | None = None
    current_identity: str | None = None
    available_version: str | None = None
    expected_version: str | None = None
    expected_identity: str | None = None
    verified_version: str | None = None
    verified_identity: str | None = None
    error: str | None = None
    events: list[dict[str, Any]] = Field(default_factory=list)
    next_event_seq: int = 1
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime | None = None
    install_deadline: datetime | None = None
    health_deadline: datetime | None = None
    initial_process_id: int | None = None

    def public_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class FleetUpdateJobStore:
    """Durable job and audit store. Payloads intentionally contain no credentials."""

    def __init__(self, data_dir: Path) -> None:
        self.directory = data_dir / "fleet_update_jobs"
        self.lock_directory = self.directory / ".locks"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.lock_directory.mkdir(parents=True, exist_ok=True)
        self._jobs: dict[str, FleetUpdateJob] = {}
        self._tasks: dict[str, asyncio.Task[Any]] = {}
        self._load()

    def _load(self, *, preserve_existing: bool = False) -> None:
        for path in self.directory.glob("*.json"):
            try:
                job = FleetUpdateJob.model_validate_json(path.read_text())
            except OSError, ValueError, json.JSONDecodeError:
                continue
            next_seq = 1
            for event in job.events:
                seq = event.get("seq")
                if not isinstance(seq, int) or seq < next_seq:
                    seq = next_seq
                    event["seq"] = seq
                next_seq = seq + 1
            job.next_event_seq = max(job.next_event_seq, next_seq)
            existing = self._jobs.get(job.job_id) if preserve_existing else None
            if existing is None:
                self._jobs[job.job_id] = job
                continue
            # A running task holds the original model object. Reconcile durable
            # fields in place so get()/SSE and that task continue sharing it.
            for field_name in FleetUpdateJob.model_fields:
                setattr(existing, field_name, getattr(job, field_name))

    def _reload_jobs(self) -> None:
        self._load(preserve_existing=True)

    @contextmanager
    def _instance_lock(self, instance_id: str):
        digest = hashlib.sha256(instance_id.encode()).hexdigest()
        path = self.lock_directory / f"{digest}.lock"
        with path.open("a+") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def create(
        self, instance: Any, request: FleetUpdateRequest, default_channel: str
    ) -> FleetUpdateJob:
        with self._instance_lock(instance.instance_id):
            self._reload_jobs()
            active = self.active_for_instance(instance.instance_id)
            if active:
                raise RuntimeError(active.job_id)
            job = FleetUpdateJob(
                instance_id=instance.instance_id,
                instance_name=instance.name,
                instance_url=instance.url.rstrip("/"),
                channel=request.channel or default_channel,
                target_version=request.target_version,
                quiesce_timeout=request.quiesce_timeout,
                force=request.force,
                health_timeout=request.health_timeout,
                install_timeout=request.install_timeout,
            )
            self._jobs[job.job_id] = job
            self.persist(job)
            return job

    def get(self, job_id: str) -> FleetUpdateJob | None:
        return self._jobs.get(job_id)

    def list(self) -> list[FleetUpdateJob]:
        return sorted(self._jobs.values(), key=lambda job: job.created_at, reverse=True)

    def active_for_instance(self, instance_id: str) -> FleetUpdateJob | None:
        return next(
            (
                j
                for j in self._jobs.values()
                if j.instance_id == instance_id and j.phase not in TERMINAL_PHASES
            ),
            None,
        )

    def persist(self, job: FleetUpdateJob) -> None:
        job.updated_at = datetime.now(UTC)
        atomic_write_json(self.directory / f"{job.job_id}.json", job.public_dict())

    def event(self, job: FleetUpdateJob, phase: UpdatePhase, message: str) -> None:
        job.phase = phase
        job.events.append(
            {
                "seq": job.next_event_seq,
                "at": datetime.now(UTC).isoformat(),
                "phase": phase.value,
                "message": message,
            }
        )
        job.next_event_seq += 1
        job.events = job.events[-500:]
        self.persist(job)

    def events_after(self, job: FleetUpdateJob, after_seq: int) -> list[dict[str, Any]]:
        return [event for event in job.events if int(event.get("seq", 0)) > after_seq]


def _headers(settings: Settings) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {settings.sync_token}",
        "Accept": "application/json",
    }


async def _peer_json(
    client: httpx.AsyncClient, method: str, url: str, settings: Settings, **kwargs: Any
) -> dict[str, Any]:
    response = await client.request(method, url, headers=_headers(settings), **kwargs)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Peer returned an invalid response")
    return payload


async def run_update_job(
    settings: Settings, store: FleetUpdateJobStore, job: FleetUpdateJob
) -> FleetUpdateJob:
    """Run or safely resume a controller job from durable state."""
    base = job.instance_url
    try:
        if not settings.sync_token:
            raise RuntimeError("Fleet sync token is not configured")
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(20.0, read=max(job.quiesce_timeout + 10, 30))
        ) as client:
            if job.phase == UpdatePhase.PENDING:
                status = await _peer_json(client, "GET", f"{base}/api/status", settings)
                if status.get("instance_id") != job.instance_id:
                    raise RuntimeError(
                        "Peer identity did not match the registered fleet instance"
                    )
                job.current_version = str(status.get("version") or "") or None
                process_id = status.get("process_id")
                job.initial_process_id = (
                    int(process_id) if isinstance(process_id, int | str) else None
                )
                job.current_identity = str(status.get("install_revision") or "") or None
                if not job.current_version:
                    raise RuntimeError("Peer did not report its current version")

                track = normalize_track(job.channel)
                checked: dict[str, Any] | None = None
                if not job.target_version or track == ReleaseTrack.DEV:
                    checked = await _peer_json(
                        client,
                        "GET",
                        f"{base}/api/fleet/peer-update-check",
                        settings,
                        params={"channel": job.channel},
                    )
                    job.available_version = (
                        str(checked.get("available_version") or "") or None
                    )
                    if not job.available_version:
                        raise RuntimeError(
                            "Peer update check did not report an available version; "
                            "specify target_version and retry"
                        )
                    if not checked.get("upgrade_available"):
                        raise RuntimeError(
                            f"Peer already reports {job.current_version}; no newer "
                            f"{job.channel} version is available"
                        )
                job.expected_version = job.target_version or job.available_version

                if track == ReleaseTrack.DEV:
                    if job.expected_version != job.available_version:
                        raise RuntimeError(
                            f"Dev channel target must be {job.available_version}, not "
                            f"{job.expected_version}"
                        )
                    job.expected_identity = (
                        str((checked or {}).get("target_identity") or "").lower()
                        or None
                    )
                    if not job.expected_identity:
                        raise RuntimeError(
                            "Dev update check did not resolve an immutable target revision"
                        )
                    if job.current_identity and (
                        job.current_identity.lower() == job.expected_identity
                    ):
                        raise RuntimeError(
                            f"Peer already has dev revision {job.expected_identity}; "
                            "no update was performed"
                        )
                else:
                    try:
                        comparison = compare_versions(
                            job.expected_version, job.current_version
                        )
                    except ValueError as exc:
                        raise RuntimeError(
                            f"Cannot compare requested version {job.expected_version!r} "
                            f"with current version {job.current_version!r}: {exc}"
                        ) from exc
                    if comparison <= 0:
                        relation = (
                            "already running" if comparison == 0 else "newer than"
                        )
                        raise RuntimeError(
                            f"Peer is {relation} requested version {job.expected_version} "
                            f"(current {job.current_version}); target_version must be newer"
                        )
                store.event(
                    job,
                    UpdatePhase.PREFLIGHT,
                    f"Preflight complete: {job.current_version} → {job.expected_version}",
                )

            if not job.expected_version and job.target_version:
                # Backfill jobs written by the first fleet-update release. An
                # explicit target is already durable and safe to verify against.
                job.expected_version = job.target_version
                store.persist(job)

            if not job.expected_version:
                raise RuntimeError(
                    "Cannot safely resume update without a durable expected version; "
                    "start a new update job"
                )
            if (
                normalize_track(job.channel) == ReleaseTrack.DEV
                and not job.expected_identity
            ):
                raise RuntimeError(
                    "Cannot safely resume dev update without a durable target revision; "
                    "start a new update job"
                )

            if job.phase == UpdatePhase.PREFLIGHT:
                quiesce = await _peer_json(
                    client,
                    "POST",
                    f"{base}/api/agent/quiesce",
                    settings,
                    json={
                        "reason": "fleet-update",
                        "timeout": job.quiesce_timeout,
                        "wait": True,
                    },
                )
                if quiesce.get("error") and not job.force:
                    raise RuntimeError(
                        f"Agent drain failed: {quiesce['error']}. "
                        "Retry with force=true to continue."
                    )
                message = "Agent sessions drained"
                if quiesce.get("error"):
                    message = (
                        f"Drain timed out; force policy accepted: {quiesce['error']}"
                    )
                store.event(job, UpdatePhase.QUIESCING, message)

            if job.phase == UpdatePhase.QUIESCING:
                # Persist before dispatch. The operation id makes a retry from this
                # ambiguous crash boundary idempotent on the peer.
                job.install_deadline = datetime.now(UTC) + timedelta(
                    seconds=job.install_timeout
                )
                store.event(
                    job,
                    UpdatePhase.INSTALLING,
                    f"Requesting installation of PA {job.expected_version}",
                )

            if job.phase == UpdatePhase.INSTALLING:
                try:
                    result = await _peer_json(
                        client,
                        "POST",
                        f"{base}/api/fleet/peer-update",
                        settings,
                        json={
                            "channel": job.channel,
                            "target_version": job.expected_version,
                            "target_identity": job.expected_identity,
                            "operation_id": job.job_id,
                        },
                    )
                    accepted_version = str(result.get("target_version") or "") or None
                    if accepted_version != job.expected_version:
                        raise RuntimeError(
                            "Peer accepted an unexpected update version: "
                            f"expected {job.expected_version}, got {accepted_version or 'none'}"
                        )
                    accepted_identity = (
                        str(result.get("target_identity") or "").lower() or None
                    )
                    if (
                        job.expected_identity
                        and accepted_identity != job.expected_identity
                    ):
                        raise RuntimeError(
                            "Peer accepted an unexpected update identity: "
                            f"expected {job.expected_identity}, got "
                            f"{accepted_identity or 'none'}"
                        )
                except httpx.TransportError:
                    # The service may close the response after accepting and restarting.
                    # A recovery retry uses the same operation id and target.
                    pass
                store.event(
                    job,
                    UpdatePhase.WAITING_INSTALL,
                    "Update accepted; waiting for installation to complete",
                )

        if job.phase == UpdatePhase.WAITING_INSTALL:
            if not job.install_deadline:
                job.install_deadline = datetime.now(UTC) + timedelta(
                    seconds=job.install_timeout
                )
                store.persist(job)
            last_install_error = "peer has not reported install completion"
            while datetime.now(UTC) < job.install_deadline:
                try:
                    async with httpx.AsyncClient(timeout=5.0) as client:
                        operation = await _peer_json(
                            client,
                            "GET",
                            f"{base}/api/fleet/peer-update/{job.job_id}",
                            settings,
                        )
                    operation_status = str(operation.get("status") or "")
                    if operation_status == "failed":
                        raise RuntimeError(
                            f"Peer installation failed: {operation.get('error') or 'unknown error'}"
                        )
                    if operation_status in {"installed", "restarting", "completed"}:
                        job.health_deadline = datetime.now(UTC) + timedelta(
                            seconds=job.health_timeout
                        )
                        store.event(
                            job,
                            UpdatePhase.RESTARTING,
                            "Installation complete; waiting for restarted peer health",
                        )
                        break
                    last_install_error = (
                        f"peer install status is {operation_status or 'unknown'}"
                    )
                except httpx.HTTPError as exc:
                    last_install_error = str(exc)
                await asyncio.sleep(2)
            if job.phase == UpdatePhase.WAITING_INSTALL:
                raise RuntimeError(
                    f"Peer installation timed out after {job.install_timeout:g}s: "
                    f"{last_install_error}"
                )
        elif job.phase not in {UpdatePhase.RESTARTING, UpdatePhase.VERIFYING}:
            raise RuntimeError(f"Cannot resume update from phase {job.phase.value}")

        await asyncio.sleep(1)
        if not job.health_deadline:
            job.health_deadline = datetime.now(UTC) + timedelta(
                seconds=job.health_timeout
            )
            store.persist(job)
        last_error = "peer did not become healthy"
        while datetime.now(UTC) < job.health_deadline:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    status = await _peer_json(
                        client, "GET", f"{base}/api/status", settings
                    )
                if status.get("instance_id") != job.instance_id:
                    last_error = "peer identity changed after restart"
                elif (
                    job.initial_process_id is not None
                    and status.get("process_id") == job.initial_process_id
                ):
                    last_error = "peer is healthy but has not restarted yet"
                else:
                    job.verified_version = str(status.get("version") or "") or None
                    job.verified_identity = (
                        str(status.get("install_revision") or "").lower() or None
                    )
                    store.event(
                        job,
                        UpdatePhase.VERIFYING,
                        f"Peer reports version {job.verified_version or 'unknown'}",
                    )
                    if normalize_track(job.channel) == ReleaseTrack.DEV:
                        installed_version = (
                            str(status.get("installed_version") or "") or None
                        )
                        installed_channel = normalize_track(
                            str(status.get("install_channel") or "release")
                        )
                        version_matches_install = False
                        if job.verified_version and installed_version:
                            try:
                                version_matches_install = (
                                    compare_versions(
                                        job.verified_version, installed_version
                                    )
                                    == 0
                                )
                            except ValueError:
                                version_matches_install = False
                        verified = (
                            installed_channel == ReleaseTrack.DEV
                            and job.verified_identity == job.expected_identity
                            and version_matches_install
                        )
                        if not verified:
                            last_error = (
                                f"expected dev revision {job.expected_identity}, peer reports "
                                f"revision {job.verified_identity or 'none'}, channel "
                                f"{installed_channel}, running {job.verified_version or 'none'}, "
                                f"installed {installed_version or 'none'}"
                            )
                    else:
                        try:
                            verified = bool(
                                job.verified_version
                                and compare_versions(
                                    job.verified_version, job.expected_version
                                )
                                == 0
                            )
                        except ValueError:
                            verified = False
                        if not verified:
                            last_error = (
                                f"expected {job.expected_version} (semantic version), peer "
                                f"reports {job.verified_version or 'no version'}"
                            )
                    if not verified:
                        raise RuntimeError(f"Version verification failed: {last_error}")
                    job.completed_at = datetime.now(UTC)
                    store.event(
                        job,
                        UpdatePhase.SUCCEEDED,
                        f"Verified PA {job.verified_version}",
                    )
                    return job
            except httpx.HTTPError as exc:
                last_error = str(exc)
            await asyncio.sleep(2)
        if job.verified_version:
            raise RuntimeError(f"Version verification failed: {last_error}")
        raise RuntimeError(
            f"Peer health verification timed out after {job.health_timeout:g}s: "
            f"{last_error}"
        )
    except Exception as exc:
        job.error = str(exc)
        job.completed_at = datetime.now(UTC)
        store.event(job, UpdatePhase.FAILED, f"Update failed: {exc}")
        return job


def start_update_job(
    settings: Settings, store: FleetUpdateJobStore, job: FleetUpdateJob
) -> None:
    task = asyncio.create_task(run_update_job(settings, store, job))
    store._tasks[job.job_id] = task
    task.add_done_callback(lambda _task: store._tasks.pop(job.job_id, None))


def recover_update_jobs(
    settings: Settings, fleet: FleetRegistry, store: FleetUpdateJobStore
) -> None:
    """Resume non-terminal controller jobs after a controller restart."""
    for job in store.list():
        if job.phase in TERMINAL_PHASES or job.job_id in store._tasks:
            continue
        if not fleet.get_instance(job.instance_id):
            job.error = "Registered fleet target no longer exists"
            job.completed_at = datetime.now(UTC)
            store.event(job, UpdatePhase.FAILED, job.error)
            continue
        store.event(job, job.phase, "Controller restarted; resuming update workflow")
        start_update_job(settings, store, job)
