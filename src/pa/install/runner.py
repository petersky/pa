"""Host installation logic."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

from pa import __version__
from pa.cli import service as svc
from pa.config import Settings, get_settings, reset_settings
from pa.install.metadata import InstallMetadata, save_install_metadata


def _run(cmd: list[str], *, cwd: Path | None = None) -> None:
    result = subprocess.run(cmd, cwd=cwd, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")


def record_install(*, channel: str = "release", pa_bin: Path | None = None) -> None:
    """Write install.json without running a full install."""
    reset_settings()
    settings = get_settings()
    bin_path = pa_bin or svc.find_pa_binary()
    save_install_metadata(
        settings.data_dir,
        InstallMetadata(
            version=__version__,
            method="uv-tool",
            channel=channel,
            pa_bin=str(bin_path) if bin_path else None,
        ),
    )


def install_from_path(
    source: Path | None = None,
    *,
    name: str = "local",
    start_service: bool = True,
) -> None:
    """Install PA via uv tool and register host service."""
    if not shutil.which("uv"):
        raise RuntimeError("uv is required. Install from https://docs.astral.sh/uv/")

    if source:
        _run(["uv", "tool", "install", "--force", str(source)])
    else:
        _run(["uv", "tool", "install", "--force", "pa"])

    pa_bin = svc.find_pa_binary()
    if not pa_bin:
        raise RuntimeError("pa binary not found after install")

    settings = Settings(instance_name=name)
    settings.ensure_dirs()
    (settings.data_dir / "logs").mkdir(parents=True, exist_ok=True)

    config_path = settings.data_dir / "config.json"
    if not config_path.exists():
        _run([str(pa_bin), "init", "--name", name])

    if svc.service_supported():
        svc.install_service(settings, pa_bin)
        svc.bootstrap()
        if start_service:
            svc.start()

    save_install_metadata(
        settings.data_dir,
        InstallMetadata(
            version=__version__,
            method="uv-tool",
            channel=settings.release_track,
            pa_bin=str(pa_bin),
        ),
    )
