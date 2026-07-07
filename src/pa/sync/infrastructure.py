"""Shared sync infrastructure singletons (object store + event log)."""

from __future__ import annotations

from pa.config import Settings
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore

_object_store: ObjectStore | None = None
_event_log: EventLog | None = None
_cached_key: tuple[str, str] | None = None


def _make_key(settings: Settings) -> tuple[str, str]:
    return (str(settings.objects_dir), settings.instance_id)


def get_object_store(settings: Settings) -> ObjectStore:
    global _object_store, _cached_key
    key = _make_key(settings)
    if _object_store is None or _cached_key != key:
        _object_store = ObjectStore(settings.objects_dir)
        _cached_key = key
    return _object_store


def get_event_log(settings: Settings) -> EventLog:
    global _event_log, _cached_key
    key = _make_key(settings)
    if _event_log is None or _cached_key != key:
        _event_log = EventLog(get_object_store(settings), settings.data_dir, settings.instance_id)
        _cached_key = key
    return _event_log


def reset_infrastructure() -> None:
    global _object_store, _event_log, _cached_key
    _object_store = None
    _event_log = None
    _cached_key = None
