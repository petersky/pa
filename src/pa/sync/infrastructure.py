"""Shared sync infrastructure singletons (object store + event log)."""

from __future__ import annotations

from pa.config import Settings
from pa.fleet.membership import MembershipStore
from pa.network.peer_table import PeerTable
from pa.sync.engine import SyncEngine
from pa.sync.epochs import EpochRegistry
from pa.sync.event_log import EventLog
from pa.sync.gc import GcPlanner
from pa.sync.object_catalog import ObjectCatalog
from pa.sync.object_store import ObjectStore

_object_store: ObjectStore | None = None
_event_log: EventLog | None = None
_membership: MembershipStore | None = None
_peer_table: PeerTable | None = None
_object_catalog: ObjectCatalog | None = None
_epoch_registry: EpochRegistry | None = None
_gc_planner: GcPlanner | None = None
_cached_key: tuple[str, str, str] | None = None


def _make_key(settings: Settings) -> tuple[str, str, str]:
    return (str(settings.data_dir), settings.instance_id, settings.session_secret)


def _reset_if_key_changed(settings: Settings) -> None:
    global _object_store, _event_log, _membership, _peer_table
    global _object_catalog, _epoch_registry, _gc_planner, _cached_key
    key = _make_key(settings)
    if _cached_key is not None and _cached_key != key:
        _object_store = None
        _event_log = None
        _membership = None
        _peer_table = None
        _object_catalog = None
        _epoch_registry = None
        _gc_planner = None
    _cached_key = key


def get_object_catalog(settings: Settings) -> ObjectCatalog:
    global _object_catalog
    _reset_if_key_changed(settings)
    if _object_catalog is None:
        _object_catalog = ObjectCatalog(settings.data_dir / "object_catalog.db")
    return _object_catalog


def get_object_store(settings: Settings) -> ObjectStore:
    global _object_store
    _reset_if_key_changed(settings)
    if _object_store is None:
        catalog = get_object_catalog(settings)
        _object_store = ObjectStore(settings.objects_dir, catalog=catalog)
    return _object_store


def get_epoch_registry(settings: Settings) -> EpochRegistry:
    global _epoch_registry
    _reset_if_key_changed(settings)
    if _epoch_registry is None:
        _epoch_registry = EpochRegistry(settings.data_dir)
    return _epoch_registry


def get_gc_planner(settings: Settings) -> GcPlanner:
    global _gc_planner
    _reset_if_key_changed(settings)
    if _gc_planner is None:
        _gc_planner = GcPlanner(
            settings.data_dir,
            get_object_store(settings),
            get_object_catalog(settings),
            get_epoch_registry(settings),
        )
    return _gc_planner


def get_event_log(settings: Settings) -> EventLog:
    global _event_log
    _reset_if_key_changed(settings)
    if _event_log is None:
        _event_log = EventLog(
            get_object_store(settings),
            settings.data_dir,
            settings.instance_id,
            cursor_secret=settings.session_secret,
        )
    return _event_log


def get_membership_store(settings: Settings) -> MembershipStore:
    global _membership
    _reset_if_key_changed(settings)
    if _membership is None:
        _membership = MembershipStore(settings.data_dir)
    return _membership


def get_peer_table(settings: Settings) -> PeerTable:
    global _peer_table
    _reset_if_key_changed(settings)
    if _peer_table is None:
        _peer_table = PeerTable(settings.data_dir)
    return _peer_table


def get_sync_engine(settings: Settings) -> SyncEngine:
    membership = get_membership_store(settings)
    peer_table = get_peer_table(settings)
    for realm in settings.subscribed_realms:
        peer_table.sync_from_settings_peers(realm, settings.peers, settings.zone)
        membership.ensure_realm(realm)
        membership.ensure_owner_membership(realm, "local", fleet_id=settings.fleet_id)
    return SyncEngine(
        settings,
        get_object_store(settings),
        get_event_log(settings),
        peer_table,
        membership,
    )


def reset_infrastructure() -> None:
    global _object_store, _event_log, _membership, _peer_table
    global _object_catalog, _epoch_registry, _gc_planner, _cached_key
    _object_store = None
    _event_log = None
    _membership = None
    _peer_table = None
    _object_catalog = None
    _epoch_registry = None
    _gc_planner = None
    _cached_key = None
