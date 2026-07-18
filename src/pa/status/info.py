"""Instance status snapshot for CLI and web UI."""

from __future__ import annotations

import os

from pa import __version__
from pa.cli import service as svc
from pa.core.context import AppContext
from pa.install.metadata import load_install_metadata


def build_status_snapshot(ctx: AppContext, *, module_count: int = 0) -> dict:
    settings = ctx.settings
    store = ctx.store
    svc_status = svc.get_status(settings)
    install_meta = load_install_metadata(settings.data_dir)
    pa_bin = svc.find_pa_binary()
    service_bin = svc.find_service_binary()
    items = store.list_items()
    sessions = store.list_sessions()
    knowledge = store.list_knowledge(limit=5)
    assets = ctx.services.get("assets")
    asset_version = assets.version if assets else ""

    service_state = "unavailable"
    if svc_status.installed or svc.service_supported():
        service_state = "running" if svc_status.running else "stopped"

    return {
        "version": __version__,
        "asset_version": asset_version,
        "build_id": f"{__version__}+{asset_version}",
        "instance_name": settings.instance_name,
        "instance_id": settings.instance_id,
        "process_id": os.getpid(),
        "data_dir": str(settings.data_dir),
        "server_url": f"http://{settings.host}:{settings.port}",
        "host": settings.host,
        "port": settings.port,
        "binary": str(pa_bin) if pa_bin else None,
        "service_binary": str(service_bin) if service_bin else None,
        "installed_version": install_meta.version if install_meta else None,
        "install_method": install_meta.method if install_meta else None,
        "install_channel": install_meta.channel if install_meta else None,
        "install_revision": install_meta.source_revision if install_meta else None,
        "release_track": settings.release_track,
        "service": {
            "backend": svc_status.backend,
            "installed": svc_status.installed,
            "loaded": svc_status.loaded,
            "running": svc_status.running,
            "state": service_state,
            "unit_path": str(svc_status.plist_path),
        },
        "debug": settings.debug,
        "agent_enabled": settings.agent_enabled,
        "fleet_id": settings.fleet_id,
        "realms": list(settings.subscribed_realms),
        "zone": settings.zone,
        "peer_count": len(settings.peers),
        "module_count": module_count,
        "item_count": len(items),
        "session_count": len(sessions),
        "knowledge_count": len(knowledge),
    }
