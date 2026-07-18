"""Host installation logic."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

from pa.cli import service as svc
from pa.config import get_settings, reset_settings
from pa.install.metadata import InstallMetadata, save_install_metadata
from pa.packaging.uv import resolve_uv_binary


def _run(cmd: list[str], *, cwd: Path | None = None) -> None:
    result = subprocess.run(cmd, cwd=cwd, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")


def read_pa_version(pa_bin: Path) -> str:
    result = subprocess.run(
        [str(pa_bin), "version"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        from pa import __version__

        return __version__
    match = re.search(r"(\d+\.\d+\.\S+)", result.stdout)
    if match:
        return match.group(1)
    from pa import __version__

    return __version__


def record_install(*, channel: str = "release", pa_bin: Path | None = None) -> None:
    """Write install.json without running a full install."""
    reset_settings()
    settings = get_settings()
    bin_path = pa_bin or svc.find_pa_binary()
    version = read_pa_version(bin_path) if bin_path else None
    if not version:
        from pa import __version__

        version = __version__
    save_install_metadata(
        settings.data_dir,
        InstallMetadata(
            version=version,
            method="uv-tool",
            channel=channel,
            pa_bin=str(bin_path) if bin_path else None,
        ),
    )


def install_from_path(
    source: Path | None = None,
    *,
    name: str = "local",
    channel: str = "release",
    start_service: bool = True,
) -> None:
    """Install PA via uv tool and register host service."""
    uv = resolve_uv_binary()

    if source:
        _run([uv, "tool", "install", "--force", str(source)])
    else:
        _run([uv, "tool", "install", "--force", "pa"])

    pa_bin = svc.find_pa_binary()
    if not pa_bin:
        raise RuntimeError("pa binary not found after install")

    reset_settings()
    settings = get_settings()
    settings.ensure_dirs()
    (settings.data_dir / "logs").mkdir(parents=True, exist_ok=True)

    config_path = settings.data_dir / "config.json"
    if not config_path.exists():
        _run([str(pa_bin), "init", "--name", name])

    service_bin = svc.find_service_binary() or pa_bin
    if svc.service_supported():
        svc.install_service(settings, service_bin)
        svc.bootstrap()
        if start_service:
            svc.start()

    installed_version = read_pa_version(pa_bin)
    save_install_metadata(
        settings.data_dir,
        InstallMetadata(
            version=installed_version,
            method="uv-tool",
            channel=channel,
            pa_bin=str(pa_bin),
        ),
    )
