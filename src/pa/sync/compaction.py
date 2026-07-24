"""Log compaction and observability."""

from __future__ import annotations

import json
import logging
import threading
from datetime import UTC, datetime
from pathlib import Path

from pa.core.io import atomic_write_json
from pa.domain.models import Card
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore

logger = logging.getLogger(__name__)


class SyncMetrics:
    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir / "sync_metrics.json"
        self._lock = threading.Lock()
        self._metrics: dict = {"pushes": 0, "pulls": 0, "objects_imported": 0, "last_sync": None}
        if self.path.exists():
            try:
                self._metrics.update(json.loads(self.path.read_text()))
            except json.JSONDecodeError:
                pass

    def record_push(self) -> None:
        with self._lock:
            self._metrics["pushes"] = self._metrics.get("pushes", 0) + 1
            self._metrics["last_sync"] = datetime.now(UTC).isoformat()
            self._save()

    def record_pull(self, count: int) -> None:
        with self._lock:
            self._metrics["pulls"] = self._metrics.get("pulls", 0) + 1
            self._metrics["objects_imported"] = self._metrics.get("objects_imported", 0) + count
            self._metrics["last_sync"] = datetime.now(UTC).isoformat()
            self._save()

    def _save(self) -> None:
        atomic_write_json(self.path, self._metrics)

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self._metrics)


def compact_realm(
    store: ObjectStore,
    log: EventLog,
    realm_id: str,
    cards: list[Card],
) -> str | None:
    """Create a snapshot object for old card state (compaction)."""
    snapshot = {
        "type": "snapshot",
        "realm_id": realm_id,
        "cards": [c.model_dump(mode="json") for c in cards],
        "timestamp": datetime.now(UTC).isoformat(),
    }
    return store.put_json(snapshot)
