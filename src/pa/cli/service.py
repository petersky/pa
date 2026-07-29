"""Host service management (launchd on macOS, systemd on Linux)."""

from __future__ import annotations

import hashlib
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable
from xml.sax.saxutils import escape

from pa.config import Settings
from pa.core.logging import redact_log_text
from pa.packaging.service_env import service_environment

LABEL = "com.pa.server"
PLIST_NAME = f"{LABEL}.plist"
SYSTEMD_UNIT = "pa-server.service"

ServiceProgress = Callable[[str], None]


def _report(progress: ServiceProgress | None, message: str) -> None:
    if progress:
        progress(message)


@dataclass
class ServiceStatus:
    installed: bool
    loaded: bool
    running: bool
    plist_path: Path
    log_path: Path
    backend: str = "none"


@dataclass(frozen=True)
class LoadedServiceDefinition:
    """Non-secret process definition reported by the active service manager."""

    backend: str
    command: str | None
    environment: dict[str, str]


@dataclass(frozen=True)
class RestartDiagnostic:
    """Structured evidence from the host-manager restart dispatch boundary."""

    state: str
    backend: str
    command: list[str]
    started_at: str
    completed_at: str
    duration_ms: float
    exit_code: int | None
    signal: int | None
    stdout: str
    stderr: str

    def public_dict(self) -> dict[str, object]:
        return {
            "state": self.state,
            "backend": self.backend,
            "command": self.command,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_ms": self.duration_ms,
            "exit_code": self.exit_code,
            "signal": self.signal,
            "stdout": self.stdout,
            "stderr": self.stderr,
        }


class RestartRejectedError(RuntimeError):
    """The host manager synchronously rejected a restart dispatch."""

    def __init__(self, message: str, diagnostic: RestartDiagnostic) -> None:
        super().__init__(message)
        self.diagnostic = diagnostic


def _is_darwin() -> bool:
    return sys.platform == "darwin"


def _is_linux() -> bool:
    return sys.platform.startswith("linux")


def _launch_agents_dir() -> Path:
    return Path.home() / "Library" / "LaunchAgents"


def _plist_path() -> Path:
    return _launch_agents_dir() / PLIST_NAME


def _systemd_unit_path() -> Path:
    return Path.home() / ".config" / "systemd" / "user" / SYSTEMD_UNIT


def _launchd_template_path() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "packaging"
        / "launchd"
        / "com.pa.server.plist.template"
    )


def _systemd_template_path() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "packaging"
        / "systemd"
        / "pa-server.service.template"
    )


def _domain_target() -> str:
    uid = os.getuid()
    return f"gui/{uid}/{LABEL}"


def find_pa_binary() -> Path | None:
    """Resolve the installed PA CLI, preferring the uv-tool install over a local venv."""
    candidates: list[Path] = []

    from pa.config import default_data_dir
    from pa.install.metadata import load_install_metadata

    meta = load_install_metadata(default_data_dir())
    if meta and meta.pa_bin:
        candidates.append(Path(meta.pa_bin))

    candidates.append(Path.home() / ".local" / "bin" / "pa")

    which = shutil.which("pa")
    if which:
        candidates.append(Path(which))

    seen: set[Path] = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except OSError:
            continue
        if resolved in seen:
            continue
        seen.add(resolved)
        if resolved.is_file() and os.access(resolved, os.X_OK):
            return resolved
    return None


def _is_dev_venv_binary(path: Path) -> bool:
    parts = {part.lower() for part in path.parts}
    return ".venv" in parts or "venv" in parts


def find_service_binary() -> Path | None:
    """Binary to embed in host service units — avoid pinning a local editable venv."""
    preferred = find_pa_binary()
    if preferred and not _is_dev_venv_binary(preferred):
        return preferred

    local = Path.home() / ".local" / "bin" / "pa"
    if local.exists() and os.access(local, os.X_OK):
        return local.resolve()

    return preferred


def _format_plist_env(env: dict[str, str]) -> str:
    lines = []
    for key, value in sorted(env.items()):
        lines.append(f"        <key>{escape(key)}</key>")
        lines.append(f"        <string>{escape(value)}</string>")
    return "\n".join(lines)


def _format_systemd_env(env: dict[str, str]) -> str:
    lines = [f"Environment={key}={value}" for key, value in sorted(env.items())]
    return "\n".join(lines)


def render_plist(settings: Settings, pa_bin: Path) -> bytes:
    template = _launchd_template_path().read_text()
    log_dir = settings.data_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    env = service_environment(settings)
    content = (
        template.replace("{{PA_BIN}}", str(pa_bin))
        .replace("{{PA_LOG_DIR}}", str(log_dir))
        .replace("{{ENV_PLIST}}", _format_plist_env(env))
    )
    return content.encode()


def render_systemd_unit(settings: Settings, pa_bin: Path) -> str:
    template = _systemd_template_path().read_text()
    log_dir = settings.data_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    env = service_environment(settings)
    return (
        template.replace("{{PA_BIN}}", str(pa_bin))
        .replace("{{PA_INSTANCE_NAME}}", settings.instance_name)
        .replace("{{PA_LOG_DIR}}", str(log_dir))
        .replace("{{ENV_LINES}}", _format_systemd_env(env))
    )


def install_plist(settings: Settings, pa_bin: Path | None = None) -> Path:
    if not _is_darwin():
        raise RuntimeError("launchd service management is only supported on macOS")

    bin_path = pa_bin or find_service_binary()
    if not bin_path:
        raise RuntimeError("pa binary not found in PATH")

    agents_dir = _launch_agents_dir()
    agents_dir.mkdir(parents=True, exist_ok=True)
    dest = _plist_path()
    content = render_plist(settings, bin_path)
    if not dest.exists() or dest.read_bytes() != content:
        dest.write_bytes(content)
    return dest


def install_systemd_unit(settings: Settings, pa_bin: Path | None = None) -> Path:
    if not _is_linux():
        raise RuntimeError("systemd service management is only supported on Linux")

    bin_path = pa_bin or find_service_binary()
    if not bin_path:
        raise RuntimeError("pa binary not found in PATH")

    unit_dir = _systemd_unit_path().parent
    unit_dir.mkdir(parents=True, exist_ok=True)
    dest = _systemd_unit_path()
    dest.write_text(render_systemd_unit(settings, bin_path))
    return dest


def install_service(settings: Settings, pa_bin: Path | None = None) -> Path:
    if _is_darwin():
        return install_plist(settings, pa_bin)
    if _is_linux():
        return install_systemd_unit(settings, pa_bin)
    raise RuntimeError(f"Unsupported platform for service install: {sys.platform}")


def uninstall_plist() -> None:
    dest = _plist_path()
    if dest.exists():
        dest.unlink()


def uninstall_systemd_unit() -> None:
    dest = _systemd_unit_path()
    if dest.exists():
        dest.unlink()


def _run_launchctl(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["launchctl", *args],
        capture_output=True,
        text=True,
        check=False,
    )


def _run_systemctl(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["systemctl", "--user", *args],
        capture_output=True,
        text=True,
        check=False,
    )


def _run_systemd_run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["systemd-run", "--user", *args],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )


def _launchd_definition(output: str) -> LoadedServiceDefinition:
    environment: dict[str, str] = {}
    command: str | None = None
    in_environment = False
    for line in output.splitlines():
        stripped = line.strip()
        if stripped == "environment = {":
            in_environment = True
            continue
        if in_environment and stripped == "}":
            in_environment = False
            continue
        if in_environment and "=>" in stripped:
            key, value = stripped.split("=>", 1)
            environment[key.strip()] = value.strip().strip('"')
            continue
        if command is None and stripped.startswith("program = "):
            command = stripped.removeprefix("program = ").strip().strip('"')
    return LoadedServiceDefinition("launchd", command, environment)


def _systemd_environment(output: str) -> dict[str, str]:
    environment: dict[str, str] = {}
    try:
        entries = shlex.split(output)
    except ValueError:
        return environment
    for entry in entries:
        key, separator, value = entry.partition("=")
        if separator and key:
            environment[key] = value
    return environment


def loaded_service_definition() -> LoadedServiceDefinition | None:
    """Read the command and environment held by the active service manager."""
    if _is_darwin():
        result = _run_launchctl("print", _domain_target())
        if result.returncode != 0 or "Could not find service" in result.stdout:
            return None
        return _launchd_definition(result.stdout)
    if _is_linux():
        environment = _run_systemctl(
            "show", SYSTEMD_UNIT, "--property=Environment", "--value"
        )
        command = _run_systemctl(
            "show", SYSTEMD_UNIT, "--property=ExecStart", "--value"
        )
        if environment.returncode != 0 or command.returncode != 0:
            return None
        command_text = command.stdout.strip()
        match = re.search(r"(?:^|[;{ ])path=([^ ;}]+)", command_text)
        executable = match.group(1) if match else None
        if executable is None and command_text:
            try:
                executable = shlex.split(command_text)[0]
            except ValueError:
                executable = None
        return LoadedServiceDefinition(
            "systemd",
            executable,
            _systemd_environment(environment.stdout),
        )
    return None


def _restart_diagnostic(
    *,
    state: str,
    backend: str,
    command: list[str],
    started_at: datetime,
    started_monotonic: float,
    result: subprocess.CompletedProcess[str] | None = None,
    stdout: object = "",
    stderr: object = "",
) -> RestartDiagnostic:
    completed_at = datetime.now(UTC)
    returncode = result.returncode if result is not None else None
    signal = -returncode if returncode is not None and returncode < 0 else None
    exit_code = returncode if returncode is not None and returncode >= 0 else None
    return RestartDiagnostic(
        state=state,
        backend=backend,
        command=command,
        started_at=started_at.isoformat(),
        completed_at=completed_at.isoformat(),
        duration_ms=round((time.monotonic() - started_monotonic) * 1000, 3),
        exit_code=exit_code,
        signal=signal,
        stdout=redact_log_text(result.stdout if result is not None else stdout),
        stderr=redact_log_text(result.stderr if result is not None else stderr),
    )


def _systemd_restart_unit_name(operation_id: str | None) -> str:
    identity = operation_id or f"{os.getpid()}-{time.time_ns()}"
    digest = hashlib.sha256(identity.encode()).hexdigest()[:24]
    return f"pa-update-restart-{digest}.service"


def _launchctl_io_error(result: subprocess.CompletedProcess[str]) -> bool:
    text = f"{result.stderr or ''}\n{result.stdout or ''}".lower()
    return "input/output error" in text or "bootstrap failed: 5" in text


def _launchd_job_loaded() -> bool:
    result = _run_launchctl("print", _domain_target())
    if result.returncode != 0:
        return False
    text = f"{result.stderr or ''}\n{result.stdout or ''}"
    return "Could not find service" not in text


def _launchd_job_summary() -> str:
    result = _run_launchctl("print", _domain_target())
    if result.returncode != 0:
        return "no longer registered"
    values: dict[str, str] = {}
    for raw in result.stdout.splitlines():
        line = raw.strip()
        for key in ("state", "pid", "last exit code"):
            prefix = f"{key} = "
            if line.startswith(prefix):
                values[key] = line.removeprefix(prefix)
    return ", ".join(f"{key}={value}" for key, value in values.items()) or "registered"


def _unload_launchd_job(
    *, timeout: float = 300.0, progress: ServiceProgress | None = None
) -> None:
    """Boot out the LaunchAgent and wait until launchd forgets it.

    A short sleep is not enough: launchd often keeps a SIGTERMed job visible
    briefly, and an immediate re-bootstrap then fails with I/O error 5.
    """
    target = _domain_target()
    _report(
        progress,
        f"Asked launchd to stop PA; waiting up to {timeout:g}s for graceful shutdown.",
    )
    result = _run_launchctl("bootout", target)
    if result.returncode != 0 and "No such process" not in (result.stderr or ""):
        # Still try to wait it out; print may already show the job as gone.
        pass

    deadline = time.monotonic() + timeout
    started = time.monotonic()
    next_progress = 2.0
    delay = 0.25
    while time.monotonic() < deadline:
        if not _launchd_job_loaded():
            # Extra beat so bootstrap is less likely to hit transient error 5.
            time.sleep(0.5)
            _report(progress, "PA shutdown completed; launchd released the job.")
            return
        elapsed = time.monotonic() - started
        if elapsed >= next_progress:
            assessment = (
                "shutdown is slower than expected"
                if elapsed >= 30.0
                else "graceful shutdown is still progressing"
            )
            _report(
                progress,
                f"Still waiting after {elapsed:.0f}s ({_launchd_job_summary()}); "
                f"{assessment}.",
            )
            next_progress = 5.0 if elapsed < 5.0 else next_progress + 5.0
        time.sleep(delay)
        delay = min(delay * 1.5, 1.5)
        _run_launchctl("bootout", target)

    if _launchd_job_loaded():
        raise RuntimeError(
            f"Timed out after {timeout:g}s waiting for launchd job {target} "
            f"({_launchd_job_summary()}). The service manager may be terminating a "
            "hung process; inspect `pa logs` before retrying."
        )


def _bootstrap_launchd_plist(plist: Path, *, attempts: int = 8) -> None:
    gui_domain = f"gui/{os.getuid()}"
    delays = (0.5, 1.0, 1.5, 2.0, 3.0, 4.0, 5.0, 6.0)
    last: subprocess.CompletedProcess[str] | None = None
    for attempt in range(attempts):
        last = _run_launchctl("bootstrap", gui_domain, str(plist))
        if last.returncode == 0:
            return
        # Already loaded is fine for callers that only need the job present.
        text = f"{last.stderr or ''}\n{last.stdout or ''}".lower()
        if "already bootstrapped" in text or "service already loaded" in text:
            return
        if attempt + 1 < attempts and _launchctl_io_error(last):
            time.sleep(delays[min(attempt, len(delays) - 1)])
            continue
        break
    err = ((last.stderr if last else "") or (last.stdout if last else "") or "").strip()
    raise RuntimeError(err or "launchctl bootstrap failed")


def _wait_launchd_running(*, timeout: float = 15.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = _run_launchctl("print", _domain_target())
        if result.returncode == 0 and "state = running" in result.stdout:
            return True
        time.sleep(0.25)
    return False


def bootstrap(*, reload: bool = False) -> None:
    if _is_darwin():
        plist = _plist_path()
        if not plist.exists():
            raise RuntimeError(f"Plist not installed: {plist}")
        if _launchd_job_loaded():
            if reload:
                _unload_launchd_job()
            else:
                return
        _bootstrap_launchd_plist(plist)
        return

    if _is_linux():
        _run_systemctl("daemon-reload")
        result = _run_systemctl("enable", SYSTEMD_UNIT)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "systemctl enable failed")
        return

    raise RuntimeError(f"Unsupported platform: {sys.platform}")


def start(settings: Settings | None = None) -> None:
    settings = settings or Settings()
    if _is_darwin():
        if not _plist_path().exists():
            raise RuntimeError(
                "PA service not installed. Run: pa install --service-only"
            )
        if not _launchd_job_loaded():
            bootstrap()
        if _wait_launchd_running(timeout=5.0):
            return
        result = _run_launchctl("kickstart", "-k", _domain_target())
        if result.returncode != 0:
            bootstrap(reload=True)
            result = _run_launchctl("kickstart", "-k", _domain_target())
        if result.returncode != 0:
            err = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(err or "launchctl kickstart failed")
        if not _wait_launchd_running():
            raise RuntimeError("PA service did not reach running state")
        return

    if _is_linux():
        result = _run_systemctl("start", SYSTEMD_UNIT)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "systemctl start failed")
        return

    raise RuntimeError(f"Unsupported platform: {sys.platform}")


def stop(*, progress: ServiceProgress | None = None) -> None:
    if _is_darwin():
        if not _launchd_job_loaded():
            return
        _unload_launchd_job(progress=progress)
        return

    if _is_linux():
        _report(progress, "Asked systemd to stop PA; waiting for graceful shutdown.")
        result = _run_systemctl("stop", SYSTEMD_UNIT)
        if result.returncode != 0 and "not loaded" not in result.stderr.lower():
            raise RuntimeError(result.stderr.strip() or "systemctl stop failed")
        _report(progress, "PA shutdown completed; systemd stopped the service.")
        return

    raise RuntimeError(f"Unsupported platform: {sys.platform}")


def restart(
    settings: Settings | None = None, *, progress: ServiceProgress | None = None
) -> None:
    settings = settings or Settings()
    if _is_darwin():
        if not _plist_path().exists():
            raise RuntimeError(
                "PA service not installed. Run: pa install --service-only"
            )
        # Unload fully, rewrite the plist, then bootstrap. RunAtLoad starts the
        # job; avoid kickstart -k unless the process never comes up.
        if _launchd_job_loaded():
            _unload_launchd_job(progress=progress)
        pa_bin = find_service_binary()
        if pa_bin:
            install_plist(settings, pa_bin)
        _report(progress, "Starting PA under launchd.")
        _bootstrap_launchd_plist(_plist_path())
        if _wait_launchd_running(timeout=8.0):
            _report(progress, "PA reached the running state.")
            return
        _report(progress, "PA has not started yet; asking launchd to kick-start it.")
        result = _run_launchctl("kickstart", "-k", _domain_target())
        if result.returncode != 0:
            err = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(err or "launchctl kickstart failed")
        if not _wait_launchd_running():
            raise RuntimeError("PA service did not reach running state after restart")
        _report(progress, "PA reached the running state.")
        return

    if _is_linux():
        _report(progress, "Asked systemd to restart PA; waiting for completion.")
        result = _run_systemctl("restart", SYSTEMD_UNIT)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "systemctl restart failed")
        _report(progress, "PA restart completed under systemd.")
        return

    raise RuntimeError(f"Unsupported platform: {sys.platform}")


def _schedule_launchd_rebootstrap(
    plist: Path, *, log_path: Path | None = None
) -> list[str]:
    """Detached bootout+bootstrap so an in-process updater can exit cleanly.

    Writing a new plist is not enough: launchd keeps the previously loaded job
    definition until the service is bootstrapped again. Doing that synchronously
    from inside the running job kills the updater before it records success.
    """
    target = _domain_target()
    gui_domain = f"gui/{os.getuid()}"
    log_file = str(log_path) if log_path else "/dev/null"
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
    # Match ExitTimeOut / unload patience: bootout may need minutes for a
    # draining ACP session, and bootstrap can hit transient I/O error 5.
    # launchctl print can exit 0 with "Could not find service" after bootout —
    # treat that the same as unloaded (see `_launchd_job_loaded`).
    script = f"""
set +e
exec >>{shlex.quote(log_file)} 2>&1
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) scheduling launchd rebootstrap for {shlex.quote(target)}"
sleep 1
deadline=$(( $(date +%s) + 300 ))
job_loaded() {{
  out=$(launchctl print {shlex.quote(target)} 2>&1)
  status=$?
  if [ "$status" -ne 0 ]; then
    return 1
  fi
  case "$out" in
    *"Could not find service"*) return 1 ;;
  esac
  return 0
}}
while [ "$(date +%s)" -lt "$deadline" ]; do
  launchctl bootout {shlex.quote(target)} >/dev/null 2>&1
  if ! job_loaded; then
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) launchd released {shlex.quote(target)}"
    break
  fi
  sleep 1
done
if job_loaded; then
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) timed out waiting for bootout of {shlex.quote(target)}"
  exit 1
fi
sleep 0.5
for delay in 0.5 1 1.5 2 3 4 5 6; do
  if launchctl bootstrap {shlex.quote(gui_domain)} {shlex.quote(str(plist))}; then
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) bootstrap succeeded"
    exit 0
  fi
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) bootstrap failed; retrying in ${{delay}}s"
  sleep "$delay"
done
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) bootstrap failed after retries"
exit 1
"""
    command = ["/bin/bash", "-c", script]
    subprocess.Popen(
        command,
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
    )
    return command


def request_restart(
    settings: Settings | None = None,
    *,
    progress: ServiceProgress | None = None,
    operation_id: str | None = None,
) -> RestartDiagnostic:
    """Ask the host manager to restart the service without managing our own exit.

    This path is for code running inside the PA service. A synchronous
    unload/bootstrap or systemctl restart makes the updater a child of the process
    it is terminating, so it can be killed before it records success or starts the
    replacement service.

    Always rewrite the host unit first so peer updates pick up a new binary path
    or environment instead of restarting the previously loaded definition.
    """
    settings = settings or Settings()
    pa_bin = find_service_binary()
    if pa_bin:
        install_service(settings, pa_bin)

    if _is_darwin():
        if not _plist_path().exists():
            raise RuntimeError(
                "PA service not installed. Run: pa install --service-only"
            )
        started_at = datetime.now(UTC)
        started_monotonic = time.monotonic()
        _report(
            progress,
            "Scheduling a launchd reload so the updated service definition is used.",
        )
        try:
            command = _schedule_launchd_rebootstrap(
                _plist_path(),
                log_path=settings.data_dir / "logs" / "service-rebootstrap.log",
            )
        except OSError as exc:
            command = ["/bin/bash", "-c", "<launchd rebootstrap script>"]
            diagnostic = _restart_diagnostic(
                state="restart_rejected",
                backend="launchd",
                command=command,
                started_at=started_at,
                started_monotonic=started_monotonic,
                stderr=exc,
            )
            raise RestartRejectedError(str(exc), diagnostic) from exc
        _report(progress, "launchd reload scheduled; waiting for host-managed restart.")
        return _restart_diagnostic(
            state="restart_requested",
            backend="launchd",
            command=command,
            started_at=started_at,
            started_monotonic=started_monotonic,
            result=subprocess.CompletedProcess(command, 0, "", ""),
        )

    if _is_linux():
        _report(progress, "Reloading systemd unit definitions.")
        reload_started_at = datetime.now(UTC)
        reload_started_monotonic = time.monotonic()
        reload_command = ["systemctl", "--user", "daemon-reload"]
        reload = _run_systemctl("daemon-reload")
        if reload.returncode != 0:
            diagnostic = _restart_diagnostic(
                state="restart_rejected",
                backend="systemd",
                command=reload_command,
                started_at=reload_started_at,
                started_monotonic=reload_started_monotonic,
                result=reload,
            )
            raise RestartRejectedError(
                reload.stderr.strip() or "systemctl daemon-reload failed", diagnostic
            )

        # systemd-run asks the user manager to create a new transient unit. The
        # delayed systemctl process therefore lives outside pa-server.service's
        # cgroup and survives teardown of the service that requested the update.
        transient_unit = _systemd_restart_unit_name(operation_id)
        restart_command = [
            "systemctl",
            "--user",
            "restart",
            "--no-block",
            SYSTEMD_UNIT,
        ]
        restart_script = f"sleep 1; exec {shlex.join(restart_command)}"
        run_args = [
            "--unit",
            transient_unit,
            "--collect",
            "--no-block",
            "--property",
            "Type=oneshot",
            "--",
            "/bin/sh",
            "-c",
            restart_script,
        ]
        dispatch_command = ["systemd-run", "--user", *run_args]
        started_at = datetime.now(UTC)
        started_monotonic = time.monotonic()
        _report(progress, "Requesting a detached systemd restart unit.")
        try:
            result = _run_systemd_run(*run_args)
        except subprocess.TimeoutExpired as exc:
            diagnostic = _restart_diagnostic(
                state="restart_response_lost",
                backend="systemd",
                command=dispatch_command,
                started_at=started_at,
                started_monotonic=started_monotonic,
                stdout=exc.stdout or "",
                stderr=exc.stderr or exc,
            )
            _report(progress, "Restart requested; waiting for peer health.")
            return diagnostic
        except OSError as exc:
            diagnostic = _restart_diagnostic(
                state="restart_rejected",
                backend="systemd",
                command=dispatch_command,
                started_at=started_at,
                started_monotonic=started_monotonic,
                stderr=exc,
            )
            raise RestartRejectedError(str(exc), diagnostic) from exc
        if result.returncode > 0:
            diagnostic = _restart_diagnostic(
                state="restart_rejected",
                backend="systemd",
                command=dispatch_command,
                started_at=started_at,
                started_monotonic=started_monotonic,
                result=result,
            )
            raise RestartRejectedError(
                result.stderr.strip() or "systemd rejected restart request", diagnostic
            )
        state = (
            "restart_response_lost" if result.returncode < 0 else "restart_requested"
        )
        diagnostic = _restart_diagnostic(
            state=state,
            backend="systemd",
            command=dispatch_command,
            started_at=started_at,
            started_monotonic=started_monotonic,
            result=result,
        )
        _report(progress, "Restart requested; waiting for peer health.")
        return diagnostic

    raise RuntimeError(f"Unsupported platform: {sys.platform}")


def get_status(settings: Settings | None = None) -> ServiceStatus:
    settings = settings or Settings()
    log_path = settings.data_dir / "logs" / "server.log"
    loaded = running = False
    installed = False
    unit_path = _plist_path()
    backend = "none"

    if _is_darwin():
        backend = "launchd"
        unit_path = _plist_path()
        installed = unit_path.exists()
        if installed:
            result = _run_launchctl("print", _domain_target())
            if result.returncode == 0 and "Could not find service" not in (
                f"{result.stderr or ''}\n{result.stdout or ''}"
            ):
                loaded = True
                running = "state = running" in result.stdout

    elif _is_linux():
        backend = "systemd"
        unit_path = _systemd_unit_path()
        installed = unit_path.exists()
        if installed:
            result = _run_systemctl("is-active", SYSTEMD_UNIT)
            running = result.returncode == 0
            loaded = (
                running or _run_systemctl("is-enabled", SYSTEMD_UNIT).returncode == 0
            )

    return ServiceStatus(
        installed=installed,
        loaded=loaded,
        running=running,
        plist_path=unit_path,
        log_path=log_path,
        backend=backend,
    )


def tail_logs(lines: int = 50, follow: bool = False) -> None:
    settings = Settings()
    log_path = settings.data_dir / "logs" / "server.log"
    if not log_path.exists():
        raise RuntimeError(f"Log file not found: {log_path}")
    args = ["tail"]
    if follow:
        args.append("-f")
    args.extend(["-n", str(lines), str(log_path)])
    subprocess.run(args, check=False)


def service_supported() -> bool:
    return _is_darwin() or _is_linux()
