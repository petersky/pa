"""Host service management (launchd on macOS)."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from pa.config import Settings

LABEL = "com.pa.server"
PLIST_NAME = f"{LABEL}.plist"


@dataclass
class ServiceStatus:
    installed: bool
    loaded: bool
    running: bool
    plist_path: Path
    log_path: Path


def _launch_agents_dir() -> Path:
    return Path.home() / "Library" / "LaunchAgents"


def _plist_path() -> Path:
    return _launch_agents_dir() / PLIST_NAME


def _template_path() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "packaging"
        / "launchd"
        / "com.pa.server.plist.template"
    )


def _domain_target() -> str:
    uid = os.getuid()
    return f"gui/{uid}/{LABEL}"


def find_pa_binary() -> Path | None:
    pa = shutil.which("pa")
    if pa:
        return Path(pa)
    local = Path.home() / ".local" / "bin" / "pa"
    if local.exists():
        return local
    return None


def render_plist(settings: Settings, pa_bin: Path) -> bytes:
    template = _template_path().read_text()
    log_dir = settings.data_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    content = (
        template.replace("{{PA_BIN}}", str(pa_bin))
        .replace("{{PA_DATA_DIR}}", str(settings.data_dir))
        .replace("{{PA_HOST}}", settings.host)
        .replace("{{PA_PORT}}", str(settings.port))
        .replace("{{PA_INSTANCE_NAME}}", settings.instance_name)
        .replace("{{PA_LOG_DIR}}", str(log_dir))
    )
    return content.encode()


def install_plist(settings: Settings, pa_bin: Path | None = None) -> Path:
    if sys.platform != "darwin":
        raise RuntimeError("launchd service management is only supported on macOS")

    bin_path = pa_bin or find_pa_binary()
    if not bin_path:
        raise RuntimeError("pa binary not found in PATH")

    agents_dir = _launch_agents_dir()
    agents_dir.mkdir(parents=True, exist_ok=True)
    dest = _plist_path()
    dest.write_bytes(render_plist(settings, bin_path))
    return dest


def uninstall_plist() -> None:
    dest = _plist_path()
    if dest.exists():
        dest.unlink()


def _run_launchctl(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["launchctl", *args],
        capture_output=True,
        text=True,
        check=False,
    )


def bootstrap() -> None:
    if sys.platform != "darwin":
        raise RuntimeError("launchd service management is only supported on macOS")
    plist = _plist_path()
    if not plist.exists():
        raise RuntimeError(f"Plist not installed: {plist}")
    domain = _domain_target()
    bootout = _run_launchctl("bootout", domain)
    if bootout.returncode != 0 and "No such process" not in bootout.stderr:
        pass  # not loaded yet
    result = _run_launchctl("bootstrap", f"gui/{os.getuid()}", str(plist))
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "launchctl bootstrap failed")


def start() -> None:
    if sys.platform != "darwin":
        raise RuntimeError("launchd service management is only supported on macOS")
    result = _run_launchctl("kickstart", "-k", _domain_target())
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "launchctl kickstart failed")


def stop() -> None:
    if sys.platform != "darwin":
        raise RuntimeError("launchd service management is only supported on macOS")
    result = _run_launchctl("bootout", _domain_target())
    if result.returncode != 0 and "No such process" not in result.stderr:
        raise RuntimeError(result.stderr.strip() or "launchctl bootout failed")


def restart() -> None:
    stop()
    start()


def get_status(settings: Settings | None = None) -> ServiceStatus:
    settings = settings or Settings()
    plist = _plist_path()
    log_path = settings.data_dir / "logs" / "server.log"
    loaded = running = False

    if sys.platform == "darwin" and plist.exists():
        result = _run_launchctl("print", _domain_target())
        if result.returncode == 0:
            loaded = True
            running = "state = running" in result.stdout

    return ServiceStatus(
        installed=plist.exists(),
        loaded=loaded,
        running=running,
        plist_path=plist,
        log_path=log_path,
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
