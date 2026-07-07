"""Unified store facade — projection + event log + sync."""

from __future__ import annotations

from pathlib import Path

from pa.config import Settings, get_settings
from pa.domain.projection import CardProjection
from pa.sync.infrastructure import get_event_log, get_object_store
from pa.sync.object_store import ObjectStore
from pa.sync.event_log import EventLog

_store: "Store | None" = None


class Store(CardProjection):
    def __init__(
        self,
        db_path: Path,
        object_store: ObjectStore,
        event_log: EventLog,
    ) -> None:
        super().__init__(db_path, event_log=event_log)
        self.object_store = object_store


def get_store(settings: Settings | None = None) -> Store:
    global _store
    settings = settings or get_settings()
    if _store is None:
        obj_store = get_object_store(settings)
        event_log = get_event_log(settings)
        _store = Store(settings.db_path, obj_store, event_log)
    return _store


def reset_store() -> None:
    global _store
    _store = None
    from pa.sync.infrastructure import reset_infrastructure

    reset_infrastructure()
