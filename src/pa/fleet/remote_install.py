"""Owner-side SSH push-install for fleet members.

Credentials (password / passphrase) are accepted for a single job only and
never written to config, job status files, or logs.
"""

from __future__ import annotations

import asyncio
import json
import shlex
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from uuid import uuid4

import httpx

from pa.config import Settings
from pa.fleet.join import ensure_sync_token, owner_public_url
from pa.fleet.registry import FleetRegistry

INSTALL_SCRIPT_URL = (
    "https://raw.githubusercontent.com/petersky/pa/main/scripts/install-remote.sh"
)


class InstallJobStatus(StrEnum):
    PENDING = "pending"
    CONNECTING = "connecting"
    INSTALLING = "installing"
    JOINING = "joining"
    VERIFYING = "verifying"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass
class RemoteInstallRequest:
    host: str
    user: str
    instance_name: str
    instance_url: str
    port: int = 22
    identity_file: str = ""
    password: str = ""
    passphrase: str = ""
    channel: str = "release"
    realm: str = ""
    join_only: bool = False


@dataclass
class InstallJob:
    job_id: str
    status: InstallJobStatus = InstallJobStatus.PENDING
    host: str = ""
    user: str = ""
    instance_name: str = ""
    instance_url: str = ""
    channel: str = "release"
    created_at: str = ""
    updated_at: str = ""
    error: str = ""
    log_lines: list[str] = field(default_factory=list)
    join_token: str = ""  # not persisted to disk

    def append(self, line: str) -> None:
        text = line.rstrip("\n")
        if text:
            self.log_lines.append(text)
            if len(self.log_lines) > 2000:
                self.log_lines = self.log_lines[-1500:]
        self.updated_at = datetime.now(UTC).isoformat()

    def to_public_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "host": self.host,
            "user": self.user,
            "instance_name": self.instance_name,
            "instance_url": self.instance_url,
            "channel": self.channel,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "error": self.error,
            "log": "\n".join(self.log_lines[-200:]),
            "log_lines": list(self.log_lines[-200:]),
        }


class InstallJobStore:
    """In-memory jobs with non-secret status snapshots on disk."""

    def __init__(self, data_dir: Path) -> None:
        self.dir = data_dir / "fleet_jobs"
        self.dir.mkdir(parents=True, exist_ok=True)
        self._jobs: dict[str, InstallJob] = {}

    def create(self, req: RemoteInstallRequest) -> InstallJob:
        now = datetime.now(UTC).isoformat()
        job = InstallJob(
            job_id=str(uuid4()),
            host=req.host,
            user=req.user,
            instance_name=req.instance_name,
            instance_url=req.instance_url.rstrip("/"),
            channel=req.channel,
            created_at=now,
            updated_at=now,
        )
        self._jobs[job.job_id] = job
        self._persist(job)
        return job

    def get(self, job_id: str) -> InstallJob | None:
        return self._jobs.get(job_id)

    def _persist(self, job: InstallJob) -> None:
        # Never write passwords; join_token also omitted from disk.
        path = self.dir / f"{job.job_id}.json"
        payload = job.to_public_dict()
        path.write_text(json.dumps(payload, indent=2) + "\n")


_job_store: InstallJobStore | None = None


def get_job_store(settings: Settings) -> InstallJobStore:
    global _job_store
    if _job_store is None:
        _job_store = InstallJobStore(settings.data_dir)
    return _job_store


def _local_install_script() -> Path | None:
    # Prefer repo checkout when developing; wheel installs fall back to GitHub URL.
    here = Path(__file__).resolve()
    candidates = [
        here.parents[3] / "scripts" / "install-remote.sh",  # src/pa/fleet -> repo root
        here.parents[2] / "scripts" / "install-remote.sh",
    ]
    for path in candidates:
        if path.is_file():
            return path
    return None


def build_remote_env(
    settings: Settings,
    req: RemoteInstallRequest,
    *,
    fleet_token: str,
) -> dict[str, str]:
    owner_url = owner_public_url(settings)
    sync_token = ensure_sync_token(settings)
    realm = req.realm or (settings.subscribed_realms[0] if settings.subscribed_realms else "personal")
    env = {
        "PA_SYNC_TOKEN": sync_token,
        "PA_INSTANCE_NAME": req.instance_name,
        "PA_INSTANCE_URL": req.instance_url.rstrip("/"),
        "PA_FLEET_OWNER_URL": owner_url,
        "PA_FLEET_TOKEN": fleet_token,
        "PA_PEERS": owner_url,
        "PA_REALM": realm,
        "PA_HOST": "0.0.0.0",
        "PA_CHANNEL": req.channel or settings.release_track or "release",
    }
    return env


def _shell_export(env: dict[str, str]) -> str:
    parts = [f"export {k}={shlex.quote(v)}" for k, v in env.items()]
    return " && ".join(parts)


def build_remote_command(
    settings: Settings,
    req: RemoteInstallRequest,
    *,
    fleet_token: str,
) -> str:
    env = build_remote_env(settings, req, fleet_token=fleet_token)
    exports = _shell_export(env)
    if req.join_only:
        return (
            f"{exports} && "
            f"command -v pa >/dev/null || {{ echo 'pa not installed; use full install' >&2; exit 1; }} && "
            f"PA_FLEET_OWNER_URL={shlex.quote(env['PA_FLEET_OWNER_URL'])} "
            f"pa fleet join {shlex.quote(fleet_token)} "
            f"--url {shlex.quote(req.instance_url.rstrip('/'))} "
            f"--name {shlex.quote(req.instance_name)} "
            f"--owner {shlex.quote(env['PA_FLEET_OWNER_URL'])}"
        )
    local_script = _local_install_script()
    if local_script:
        # Script body is uploaded separately; remote runs bash on stdin.
        return f"{exports} && bash -s"
    return (
        f"{exports} && "
        f"curl -fsSL {shlex.quote(INSTALL_SCRIPT_URL)} | bash"
    )


async def _connect_ssh(req: RemoteInstallRequest):
    import asyncssh

    kwargs: dict = {
        "host": req.host,
        "port": req.port,
        "username": req.user,
        "known_hosts": None,
    }
    if req.identity_file:
        kwargs["client_keys"] = [req.identity_file]
    if req.password:
        kwargs["password"] = req.password
    if req.passphrase:
        kwargs["passphrase"] = req.passphrase
    # Prefer agent when no password/identity forced — asyncssh uses agent by default.
    return await asyncssh.connect(**kwargs)


async def _run_remote_install(
    conn,
    req: RemoteInstallRequest,
    command: str,
    job: InstallJob,
    *,
    script_bytes: bytes | None,
) -> int:
    if script_bytes is not None:
        process = await conn.create_process(command)
        process.stdin.write(script_bytes)
        await process.stdin.drain()
        process.stdin.write_eof()
    else:
        process = await conn.create_process(command)

    async def _pump(stream, prefix: str = "") -> None:
        while True:
            line = await stream.readline()
            if not line:
                break
            text = line if isinstance(line, str) else line.decode("utf-8", errors="replace")
            job.append(f"{prefix}{text.rstrip()}")

    await asyncio.gather(_pump(process.stdout), _pump(process.stderr, prefix="[err] "))
    return process.exit_status if process.exit_status is not None else 1


async def verify_remote_health(instance_url: str, *, timeout_s: float = 90.0) -> bool:
    deadline = time.monotonic() + timeout_s
    url = f"{instance_url.rstrip('/')}/api/health"
    async with httpx.AsyncClient(timeout=5.0) as client:
        while time.monotonic() < deadline:
            try:
                resp = await client.get(url)
                if resp.status_code == 200:
                    return True
            except httpx.HTTPError:
                pass
            await asyncio.sleep(2.0)
    return False


async def run_install_job(
    settings: Settings,
    fleet: FleetRegistry,
    store: InstallJobStore,
    job: InstallJob,
    req: RemoteInstallRequest,
) -> InstallJob:
    try:
        job.status = InstallJobStatus.CONNECTING
        job.append(f"Connecting to {req.user}@{req.host}:{req.port}…")
        store._persist(job)

        sync_token = ensure_sync_token(settings)
        join = fleet.create_join_token(created_by="remote-install")
        job.join_token = join.token
        command = build_remote_command(settings, req, fleet_token=join.token)
        # Ensure sync_token is referenced so linters know we persist it for the remote.
        _ = sync_token

        script_bytes: bytes | None = None
        local_script = _local_install_script()
        if local_script and not req.join_only:
            script_bytes = local_script.read_bytes()
            job.append(f"Using local install script: {local_script.name}")
        else:
            job.append("Remote will fetch install-remote.sh from GitHub")

        try:
            conn = await _connect_ssh(req)
        except Exception as exc:
            msg = str(exc)
            if "Permission denied" in msg or "auth" in msg.lower():
                job.error = "SSH authentication failed — check keys, agent, or password."
            else:
                job.error = f"SSH connection failed: {exc}"
            job.status = InstallJobStatus.FAILED
            job.append(job.error)
            store._persist(job)
            return job

        async with conn:
            job.status = InstallJobStatus.INSTALLING if not req.join_only else InstallJobStatus.JOINING
            job.append("Connected. Running remote install…")
            store._persist(job)
            code = await _run_remote_install(conn, req, command, job, script_bytes=script_bytes)
            if code != 0:
                job.status = InstallJobStatus.FAILED
                job.error = f"Remote command exited with code {code}"
                job.append(job.error)
                store._persist(job)
                return job

        job.status = InstallJobStatus.VERIFYING
        job.append(f"Verifying health at {req.instance_url}…")
        store._persist(job)
        ok = await verify_remote_health(req.instance_url)
        if not ok:
            job.status = InstallJobStatus.FAILED
            job.error = "Remote install finished but /api/health did not become ready in time."
            job.append(job.error)
            store._persist(job)
            return job

        job.status = InstallJobStatus.SUCCEEDED
        job.append("Remote instance is healthy and should appear in the fleet list.")
        store._persist(job)
        return job
    except Exception as exc:
        job.status = InstallJobStatus.FAILED
        job.error = str(exc)
        job.append(f"Failed: {exc}")
        store._persist(job)
        return job


def start_install_job_background(
    settings: Settings,
    fleet: FleetRegistry,
    store: InstallJobStore,
    req: RemoteInstallRequest,
) -> InstallJob:
    job = store.create(req)

    async def _runner() -> None:
        await run_install_job(settings, fleet, store, job, req)

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_runner())
    except RuntimeError:
        asyncio.run(_runner())
    return job
