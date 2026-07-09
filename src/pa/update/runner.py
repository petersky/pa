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
    previous: str | None = None
    restarted: bool = False


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
        previous=__version__,
    )


def apply_update(
    settings: Settings | None = None,
    *,
    channel_name: str | None = None,
    restart: bool = False,
    release: ReleaseInfo | None = None,
) -> UpdateResult:
    """Install a previously checked release. Raises if no upgrade is available."""
    settings = settings or Settings()
    name = channel_name or settings.release_track
    channel = get_channel(name, repo=settings.update_repo)
    target = release or channel.latest()
    previous = __version__

    if not target or not is_newer(previous, target.version, track=name):
        return UpdateResult(
            current=previous,
            latest=target.version if target else None,
            upgrade_available=False,
            release=target,
            previous=previous,
        )

    channel.install(target)
    reset_settings()
    from pa.cli import service as svc
    from pa.install.runner import read_pa_version

    pa_bin = svc.find_pa_binary()
    installed_version = read_pa_version(pa_bin) if pa_bin else target.version

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

    restarted = False
    status = svc.get_status(settings)
    if status.installed:
        service_bin = svc.find_service_binary()
        if service_bin:
            svc.install_service(settings, service_bin)
        if restart:
            svc.restart(settings)
            restarted = True

    return UpdateResult(
        current=installed_version,
        latest=target.version,
        upgrade_available=True,
        release=target,
        previous=previous,
        restarted=restarted,
    )


def run_update(
    settings: Settings | None = None,
    *,
    channel_name: str | None = None,
    restart: bool = False,
) -> UpdateResult:
    """Non-interactive update (used by scripts / --yes)."""
    checked = check_update(settings, channel_name=channel_name)
    if not checked.upgrade_available or not checked.release:
        return checked
    return apply_update(
        settings,
        channel_name=channel_name,
        restart=restart,
        release=checked.release,
    )


def format_release_notes(release: ReleaseInfo | None, *, max_chars: int = 4000) -> str:
    if not release:
        return "No release notes available."
    title = release.name or f"PA {release.version}"
    body = (release.notes or "").strip()
    if not body:
        lines = [title, "", "No release notes available for this release."]
        if release.url:
            lines.append(f"Details: {release.url}")
        return "\n".join(lines)

    if len(body) > max_chars:
        body = body[: max_chars - 20].rstrip() + "\n\n… (truncated)"
    lines = [title, "", body]
    if release.url:
        lines.extend(["", f"Details: {release.url}"])
    return "\n".join(lines)
