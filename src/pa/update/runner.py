"""Run pa update checks and upgrades."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from pa import __version__
from pa.config import Settings, reset_settings
from pa.install.metadata import InstallMetadata, save_install_metadata
from pa.update.channels import ReleaseInfo, get_channel, is_newer


@dataclass
class UpdateResult:
    current: str
    latest: str | None
    upgrade_available: bool
    release: ReleaseInfo | None = None


def check_update(
    settings: Settings | None = None,
    *,
    channel_name: str | None = None,
) -> UpdateResult:
    settings = settings or Settings()
    track = channel_name or settings.release_track
    channel = get_channel(track, repo=settings.update_repo)
    release = channel.latest()
    latest = release.version if release else None
    return UpdateResult(
        current=__version__,
        latest=latest,
        upgrade_available=bool(latest and is_newer(__version__, latest, track=track)),
        release=release,
    )


def run_update(
    settings: Settings | None = None,
    *,
    channel_name: str | None = None,
    restart: bool = False,
) -> UpdateResult:
    settings = settings or Settings()
    name = channel_name or settings.release_track
    channel = get_channel(name, repo=settings.update_repo)
    release = channel.latest()

    result = UpdateResult(
        current=__version__,
        latest=release.version if release else None,
        upgrade_available=bool(
            release and is_newer(__version__, release.version, track=name)
        ),
        release=release,
    )

    if not result.upgrade_available or not release:
        return result

    channel.install(release)
    reset_settings()
    from pa.cli import service as svc
    from pa.install.runner import read_pa_version

    pa_bin = svc.find_pa_binary()
    installed_version = read_pa_version(pa_bin) if pa_bin else release.version

    save_install_metadata(
        settings.data_dir,
        InstallMetadata(
            version=installed_version,
            method=settings.install_method,
            channel=name,
            installed_at=datetime.now(UTC),
            pa_bin=str(pa_bin) if pa_bin else None,
        ),
    )

    result.current = installed_version

    if svc.get_status(settings).installed:
        service_bin = svc.find_service_binary()
        if service_bin:
            svc.install_service(settings, service_bin)
        if restart:
            svc.restart(settings)

    return result
