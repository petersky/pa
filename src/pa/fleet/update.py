"""Persistent controller-side jobs for updating registered fleet peers."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
from pydantic import BaseModel, Field, field_validator

from pa.config import Settings
from pa.core.io import atomic_write_json
from pa.fleet.registry import FleetRegistry


class UpdatePhase(StrEnum):
    PENDING = "pending"
    PREFLIGHT = "preflight"
    QUIESCING = "quiescing"
    INSTALLING = "installing"
    RESTARTING = "restarting"
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
    phase: UpdatePhase = UpdatePhase.PENDING
    current_version: str | None = None
    available_version: str | None = None
    expected_version: str | None = None
    verified_version: str | None = None
    error: str | None = None
    events: list[dict[str, Any]] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime | None = None

    def public_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class FleetUpdateJobStore:
    """Durable job and audit store. Payloads intentionally contain no credentials."""

    def __init__(self, data_dir: Path) -> None:
        self.directory = data_dir / "fleet_update_jobs"
        self.directory.mkdir(parents=True, exist_ok=True)
        self._jobs: dict[str, FleetUpdateJob] = {}
        self._tasks: dict[str, asyncio.Task[Any]] = {}
        self._load()

    def _load(self) -> None:
        for path in self.directory.glob("*.json"):
            try:
                job = FleetUpdateJob.model_validate_json(path.read_text())
            except OSError, ValueError, json.JSONDecodeError:
                continue
            self._jobs[job.job_id] = job

    def create(
        self, instance: Any, request: FleetUpdateRequest, default_channel: str
    ) -> FleetUpdateJob:
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
                "at": datetime.now(UTC).isoformat(),
                "phase": phase.value,
                "message": message,
            }
        )
        job.events = job.events[-500:]
        self.persist(job)


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
                if not job.current_version:
                    raise RuntimeError("Peer did not report its current version")

                if job.target_version:
                    job.expected_version = job.target_version
                else:
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
                    job.expected_version = job.available_version

                if job.expected_version == job.current_version:
                    raise RuntimeError(
                        f"Peer already reports requested version {job.expected_version}; "
                        "no update was performed"
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
                # Persist this checkpoint before the non-idempotent peer request.
                # Recovery from INSTALLING verifies the outcome and never resends it.
                store.event(
                    job,
                    UpdatePhase.INSTALLING,
                    f"Requesting installation of PA {job.expected_version}",
                )
                try:
                    result = await _peer_json(
                        client,
                        "POST",
                        f"{base}/api/fleet/peer-update",
                        settings,
                        json={
                            "channel": job.channel,
                            "target_version": job.expected_version,
                        },
                    )
                    accepted_version = str(result.get("target_version") or "") or None
                    if accepted_version != job.expected_version:
                        raise RuntimeError(
                            "Peer accepted an unexpected update version: "
                            f"expected {job.expected_version}, got {accepted_version or 'none'}"
                        )
                except httpx.ReadError, httpx.RemoteProtocolError:
                    # The service may close the response after accepting and restarting.
                    # The durable INSTALLING checkpoint makes recovery verification-only.
                    pass
                store.event(
                    job,
                    UpdatePhase.RESTARTING,
                    "Update request sent; waiting for the peer service to restart",
                )

        if job.phase == UpdatePhase.INSTALLING:
            store.event(
                job,
                UpdatePhase.RESTARTING,
                "Controller recovered after update dispatch; verifying without resending",
            )
        elif job.phase not in {UpdatePhase.RESTARTING, UpdatePhase.VERIFYING}:
            raise RuntimeError(f"Cannot resume update from phase {job.phase.value}")

        await asyncio.sleep(1)
        deadline = time.monotonic() + job.health_timeout
        last_error = "peer did not become healthy"
        while time.monotonic() < deadline:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    status = await _peer_json(
                        client, "GET", f"{base}/api/status", settings
                    )
                if status.get("instance_id") != job.instance_id:
                    last_error = "peer identity changed after restart"
                else:
                    job.verified_version = str(status.get("version") or "") or None
                    store.event(
                        job,
                        UpdatePhase.VERIFYING,
                        f"Peer reports version {job.verified_version or 'unknown'}",
                    )
                    if job.verified_version != job.expected_version:
                        last_error = (
                            f"expected {job.expected_version}, peer reports "
                            f"{job.verified_version or 'no version'}"
                        )
                        await asyncio.sleep(2)
                        continue
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
        raise RuntimeError(f"Peer health verification timed out: {last_error}")
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
