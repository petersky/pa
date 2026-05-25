"""Run pa update checks and upgrades."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pa import __version__
from pa.config import Settings
from pa.install.metadata import InstallMetadata, save_install_metadata
from pa.update.channels import ReleaseInfo, get_channel, is_newer


@dataclass
class UpdateResult:
    current: str
    latest: str | None
    upgrade_available: bool
    release: ReleaseInfo | None = None


def check_update(settings: Settings | None = None) -> UpdateResult:
    settings = settings or Settings()
    channel = get_channel(settings.update_channel, repo=settings.update_repo)
    release = channel.latest()
    latest = release.version if release else None
    return UpdateResult(
        current=__version__,
        latest=latest,
        upgrade_available=bool(latest and is_newer(__version__, latest)),
        release=release,
    )


def run_update(
    settings: Settings | None = None,
    *,
    channel_name: str | None = None,
    restart: bool = False,
) -> UpdateResult:
    settings = settings or Settings()
    name = channel_name or settings.update_channel
    channel = get_channel(name, repo=settings.update_repo)
    release = channel.latest()

    result = UpdateResult(
        current=__version__,
        latest=release.version if release else None,
        upgrade_available=bool(release and is_newer(__version__, release)),
        release=release,
    )

    if not result.upgrade_available or not release:
        return result

    channel.install(release)

    save_install_metadata(
        settings.data_dir,
        InstallMetadata(
            version=release.version,
            method=settings.install_method,
            channel=name,
            installed_at=datetime.now(UTC),
        ),
    )

    if restart:
        from pa.cli import service as svc

        if svc.get_status(settings).installed:
            svc.restart()

    return result
