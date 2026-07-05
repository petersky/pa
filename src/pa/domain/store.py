"""Unified store facade — projection + event log + sync."""

from __future__ import annotations

from pathlib import Path

from pa.config import get_settings
from pa.domain.projection import CardProjection
from pa.fleet.membership import MembershipStore
from pa.network.peer_table import PeerTable
from pa.sync.engine import SyncEngine
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore

_store: "Store | None" = None


class Store(CardProjection):
    def __init__(
        self,
        db_path: Path,
        object_store: ObjectStore,
        event_log: EventLog,
        sync_engine: SyncEngine | None = None,
    ) -> None:
        super().__init__(db_path, event_log=event_log)
        self.object_store = object_store
        self.sync_engine = sync_engine


def get_store() -> Store:
    global _store
    if _store is None:
        settings = get_settings()
        obj_store = ObjectStore(settings.objects_dir)
        event_log = EventLog(obj_store, settings.data_dir, settings.instance_id)
        membership = MembershipStore(settings.data_dir)
        peer_table = PeerTable(settings.data_dir)
        for realm in settings.subscribed_realms:
            peer_table.sync_from_settings_peers(realm, settings.peers, settings.zone)
            membership.ensure_realm(realm)
            membership.ensure_owner_membership(realm, "local", fleet_id=settings.fleet_id)
        sync_engine = SyncEngine(settings, obj_store, event_log, peer_table, membership)
        _store = Store(settings.db_path, obj_store, event_log, sync_engine)
    return _store


def reset_store() -> None:
    global _store
    _store = None
