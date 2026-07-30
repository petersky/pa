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
from typing import TYPE_CHECKING
from urllib.parse import quote
from uuid import uuid4

import httpx

from pa.config import Settings
from pa.core.io import atomic_write_json
from pa.core.logging import redact_log_text
from pa.fleet.join import ensure_sync_token, owner_public_url
from pa.fleet.registry import FleetRegistry

if TYPE_CHECKING:
    from pa.core.async_runtime import AsyncRuntime

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
    host_key_policy: str = "strict"
    host_key_fingerprint: str = ""
    release_ref: str = ""
    proxy_jump: str = ""


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
    secret_values: list[str] = field(default_factory=list, repr=False)

    def append(self, line: str) -> None:
        text = redact_log_text(line.rstrip("\n"))
        for secret in self.secret_values:
            if secret:
                text = text.replace(secret, "[redacted]")
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
    """Compatibility job store with durable non-secret restart recovery."""

    def __init__(self, data_dir: Path) -> None:
        self.dir = data_dir / "fleet_jobs"
        self.dir.mkdir(parents=True, exist_ok=True)
        self._jobs: dict[str, InstallJob] = {}
        self._load()

    def _load(self) -> None:
        for path in self.dir.glob("*.json"):
            try:
                payload = json.loads(path.read_text())
                status = InstallJobStatus(str(payload.get("status") or "failed"))
                if status not in {
                    InstallJobStatus.SUCCEEDED,
                    InstallJobStatus.FAILED,
                }:
                    status = InstallJobStatus.FAILED
                    payload["error"] = (
                        "Authority restarted during the legacy install. Use the "
                        "durable bootstrap resume API."
                    )
                job = InstallJob(
                    job_id=str(payload["job_id"]),
                    status=status,
                    host=str(payload.get("host") or ""),
                    user=str(payload.get("user") or ""),
                    instance_name=str(payload.get("instance_name") or ""),
                    instance_url=str(payload.get("instance_url") or ""),
                    channel=str(payload.get("channel") or "release"),
                    created_at=str(payload.get("created_at") or ""),
                    updated_at=str(payload.get("updated_at") or ""),
                    error=str(payload.get("error") or ""),
                    log_lines=[
                        redact_log_text(line)
                        for line in payload.get("log_lines", [])
                        if isinstance(line, str)
                    ],
                )
            except OSError, ValueError, KeyError, TypeError:
                continue
            self._jobs[job.job_id] = job

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
            secret_values=[req.password, req.passphrase],
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
        atomic_write_json(path, payload, mode=0o600)


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
    realm = req.realm or (
        settings.subscribed_realms[0] if settings.subscribed_realms else "personal"
    )
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
    if req.release_ref:
        env["PA_GIT_REF"] = req.release_ref
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
    return f"{exports} && curl -fsSL {shlex.quote(_install_script_url(req))} | bash"


def _install_script_url(req: RemoteInstallRequest) -> str:
    if not req.release_ref:
        return INSTALL_SCRIPT_URL
    ref = quote(req.release_ref, safe="")
    return (
        f"https://raw.githubusercontent.com/petersky/pa/{ref}/scripts/install-remote.sh"
    )


async def _connect_ssh(req: RemoteInstallRequest):
    import asyncssh

    kwargs: dict = {
        "host": req.host,
        "port": req.port,
        "username": req.user,
    }
    if req.host_key_policy == "pinned":
        if not req.host_key_fingerprint:
            raise ValueError("Pinned SSH host-key policy requires a fingerprint")
        key = await asyncssh.get_server_host_key(req.host, req.port)
        actual = str(key.get_fingerprint("sha256"))
        if actual != req.host_key_fingerprint:
            raise ValueError(
                f"SSH host-key fingerprint mismatch: expected "
                f"{req.host_key_fingerprint}, received {actual}"
            )
        # A successful exact fingerprint comparison is the trust boundary for
        # hosts not yet present in known_hosts.
        kwargs["known_hosts"] = None
    elif req.host_key_policy != "strict":
        raise ValueError(f"Unsupported SSH host-key policy: {req.host_key_policy}")
    if req.identity_file:
        kwargs["client_keys"] = [str(Path(req.identity_file).expanduser())]
    if req.proxy_jump:
        kwargs["tunnel"] = req.proxy_jump
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
            text = (
                line
                if isinstance(line, str)
                else line.decode("utf-8", errors="replace")
            )
            job.append(f"{prefix}{text.rstrip()}")

    await asyncio.gather(_pump(process.stdout), _pump(process.stderr, prefix="[err] "))
    return process.exit_status if process.exit_status is not None else 1


async def verify_remote_health(
    instance_url: str,
    *,
    timeout_s: float = 90.0,
    client: httpx.AsyncClient | None = None,
    async_runtime: AsyncRuntime | None = None,
) -> bool:
    deadline = time.monotonic() + timeout_s
    url = f"{instance_url.rstrip('/')}/api/health"
    owns_client = client is None
    client = client or httpx.AsyncClient(timeout=5.0)
    try:
        while time.monotonic() < deadline:
            try:
                request = client.get(url, timeout=5.0)
                resp = (
                    await async_runtime.observe(
                        "http.remote_install_health", request, timeout=6.0
                    )
                    if async_runtime
                    else await request
                )
                if resp.status_code == 200:
                    return True
            except httpx.HTTPError:
                pass
            await asyncio.sleep(2.0)
    finally:
        if owns_client:
            await client.aclose()
    return False


async def run_install_job(
    settings: Settings,
    fleet: FleetRegistry,
    store: InstallJobStore,
    job: InstallJob,
    req: RemoteInstallRequest,
    *,
    async_runtime: AsyncRuntime | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> InstallJob:
    async def offload(operation: str, call, *args, **kwargs):
        if async_runtime:
            return await async_runtime.run_blocking(operation, call, *args, **kwargs)
        return await asyncio.to_thread(call, *args, **kwargs)

    async def persist() -> None:
        await offload("fleet.install_job_write", store._persist, job)

    try:
        job.status = InstallJobStatus.CONNECTING
        job.append(f"Connecting to {req.user}@{req.host}:{req.port}…")
        await persist()

        sync_token = await offload(
            "filesystem.fleet_sync_token", ensure_sync_token, settings
        )
        join = await offload(
            "filesystem.fleet_join_token",
            fleet.create_join_token,
            created_by="remote-install",
        )
        job.join_token = join.token
        command = await offload(
            "filesystem.remote_install_command",
            build_remote_command,
            settings,
            req,
            fleet_token=join.token,
        )
        # Ensure sync_token is referenced so linters know we persist it for the remote.
        _ = sync_token

        script_bytes: bytes | None = None
        local_script = await offload(
            "filesystem.remote_install_script", _local_install_script
        )
        if local_script and not req.join_only:
            script_bytes = await offload(
                "filesystem.remote_install_script", local_script.read_bytes
            )
            job.append(f"Using local install script: {local_script.name}")
        else:
            job.append("Remote will fetch install-remote.sh from GitHub")

        try:
            connect = _connect_ssh(req)
            if async_runtime:
                conn = await async_runtime.observe(
                    "network.remote_install_ssh", connect, timeout=30.0
                )
            else:
                async with asyncio.timeout(30.0):
                    conn = await connect
        except Exception as exc:
            msg = redact_log_text(exc)
            if "Permission denied" in msg or "auth" in msg.lower():
                job.error = (
                    "SSH authentication failed — check keys, agent, or password."
                )
            elif "host key" in msg.lower() or "known host" in msg.lower():
                job.error = (
                    "SSH host-key verification failed — confirm the exact "
                    "fingerprint or repair known_hosts."
                )
            else:
                job.error = f"SSH connection failed: {exc}"
            job.status = InstallJobStatus.FAILED
            job.append(job.error)
            await persist()
            return job

        async with conn:
            job.status = (
                InstallJobStatus.INSTALLING
                if not req.join_only
                else InstallJobStatus.JOINING
            )
            job.append("Connected. Running remote install…")
            await persist()
            install = _run_remote_install(
                conn, req, command, job, script_bytes=script_bytes
            )
            if async_runtime:
                code = await async_runtime.observe(
                    "subprocess.remote_install", install, timeout=900.0
                )
            else:
                async with asyncio.timeout(900.0):
                    code = await install
            if code != 0:
                job.status = InstallJobStatus.FAILED
                job.error = f"Remote command exited with code {code}"
                job.append(job.error)
                await persist()
                return job

        job.status = InstallJobStatus.VERIFYING
        job.append(f"Verifying health at {req.instance_url}…")
        await persist()
        ok = await verify_remote_health(
            req.instance_url,
            client=http_client,
            async_runtime=async_runtime,
        )
        if not ok:
            job.status = InstallJobStatus.FAILED
            job.error = (
                "Remote install finished but /api/health did not become ready in time."
            )
            job.append(job.error)
            await persist()
            return job

        job.status = InstallJobStatus.SUCCEEDED
        job.append("Remote instance is healthy and should appear in the fleet list.")
        await persist()
        return job
    except Exception as exc:
        job.status = InstallJobStatus.FAILED
        job.error = redact_log_text(exc)
        job.append(f"Failed: {exc}")
        await persist()
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
