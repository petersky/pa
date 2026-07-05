"""Host service management (launchd on macOS, systemd on Linux)."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from pa.config import Settings
from pa.packaging.service_env import service_environment

LABEL = "com.pa.server"
PLIST_NAME = f"{LABEL}.plist"
SYSTEMD_UNIT = "pa-server.service"


@dataclass
class ServiceStatus:
    installed: bool
    loaded: bool
    running: bool
    plist_path: Path
    log_path: Path
    backend: str = "none"


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
    pa = shutil.which("pa")
    if pa:
        return Path(pa)
    local = Path.home() / ".local" / "bin" / "pa"
    if local.exists():
        return local
    return None


def _format_plist_env(env: dict[str, str]) -> str:
    lines = []
    for key, value in sorted(env.items()):
        lines.append(f"        <key>{key}</key>")
        lines.append(f"        <string>{value}</string>")
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

    bin_path = pa_bin or find_pa_binary()
    if not bin_path:
        raise RuntimeError("pa binary not found in PATH")

    agents_dir = _launch_agents_dir()
    agents_dir.mkdir(parents=True, exist_ok=True)
    dest = _plist_path()
    dest.write_bytes(render_plist(settings, bin_path))
    return dest


def install_systemd_unit(settings: Settings, pa_bin: Path | None = None) -> Path:
    if not _is_linux():
        raise RuntimeError("systemd service management is only supported on Linux")

    bin_path = pa_bin or find_pa_binary()
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


def bootstrap() -> None:
    if _is_darwin():
        plist = _plist_path()
        if not plist.exists():
            raise RuntimeError(f"Plist not installed: {plist}")
        domain = _domain_target()
        bootout = _run_launchctl("bootout", domain)
        if bootout.returncode != 0 and "No such process" not in bootout.stderr:
            pass
        result = _run_launchctl("bootstrap", f"gui/{os.getuid()}", str(plist))
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "launchctl bootstrap failed")
        return

    if _is_linux():
        _run_systemctl("daemon-reload")
        result = _run_systemctl("enable", SYSTEMD_UNIT)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "systemctl enable failed")
        return

    raise RuntimeError(f"Unsupported platform: {sys.platform}")


def start() -> None:
    if _is_darwin():
        result = _run_launchctl("kickstart", "-k", _domain_target())
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "launchctl kickstart failed")
        return

    if _is_linux():
        result = _run_systemctl("start", SYSTEMD_UNIT)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "systemctl start failed")
        return

    raise RuntimeError(f"Unsupported platform: {sys.platform}")


def stop() -> None:
    if _is_darwin():
        result = _run_launchctl("bootout", _domain_target())
        if result.returncode != 0 and "No such process" not in result.stderr:
            raise RuntimeError(result.stderr.strip() or "launchctl bootout failed")
        return

    if _is_linux():
        result = _run_systemctl("stop", SYSTEMD_UNIT)
        if result.returncode != 0 and "not loaded" not in result.stderr.lower():
            raise RuntimeError(result.stderr.strip() or "systemctl stop failed")
        return

    raise RuntimeError(f"Unsupported platform: {sys.platform}")


def restart() -> None:
    if _is_linux():
        result = _run_systemctl("restart", SYSTEMD_UNIT)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "systemctl restart failed")
        return
    stop()
    start()


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
            if result.returncode == 0:
                loaded = True
                running = "state = running" in result.stdout

    elif _is_linux():
        backend = "systemd"
        unit_path = _systemd_unit_path()
        installed = unit_path.exists()
        if installed:
            result = _run_systemctl("is-active", SYSTEMD_UNIT)
            running = result.returncode == 0
            loaded = running or _run_systemctl("is-enabled", SYSTEMD_UNIT).returncode == 0

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
