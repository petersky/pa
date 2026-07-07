"""Shared sync infrastructure singletons (object store + event log)."""

from __future__ import annotations

from pa.config import Settings
from pa.fleet.membership import MembershipStore
from pa.network.peer_table import PeerTable
from pa.sync.engine import SyncEngine
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore

_object_store: ObjectStore | None = None
_event_log: EventLog | None = None
_membership: MembershipStore | None = None
_peer_table: PeerTable | None = None
_cached_key: tuple[str, str] | None = None


def _make_key(settings: Settings) -> tuple[str, str]:
    return (str(settings.data_dir), settings.instance_id)


def _reset_if_key_changed(settings: Settings) -> None:
    global _object_store, _event_log, _membership, _peer_table, _cached_key
    key = _make_key(settings)
    if _cached_key is not None and _cached_key != key:
        _object_store = None
        _event_log = None
        _membership = None
        _peer_table = None
    _cached_key = key


def get_object_store(settings: Settings) -> ObjectStore:
    global _object_store
    _reset_if_key_changed(settings)
    if _object_store is None:
        _object_store = ObjectStore(settings.objects_dir)
    return _object_store


def get_event_log(settings: Settings) -> EventLog:
    global _event_log
    _reset_if_key_changed(settings)
    if _event_log is None:
        _event_log = EventLog(get_object_store(settings), settings.data_dir, settings.instance_id)
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
    global _object_store, _event_log, _membership, _peer_table, _cached_key
    _object_store = None
    _event_log = None
    _membership = None
    _peer_table = None
    _cached_key = None
