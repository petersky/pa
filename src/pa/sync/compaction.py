"""Log compaction, snapshot epochs, and observability."""

from __future__ import annotations

import json
import logging
import threading
from datetime import UTC, datetime
from pathlib import Path

from pa.core.io import atomic_write_json
from pa.domain.models import Card
from pa.sync.epochs import EpochRegistry, create_snapshot_epoch
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
    *,
    registry: EpochRegistry | None = None,
    authority_instance_id: str | None = None,
    advance_epoch: bool = True,
) -> str | None:
    """Create a content-addressed snapshot epoch for bounded retention.

    Legacy callers still receive an object hash. When ``advance_epoch`` is true
    (default) the snapshot is a versioned epoch root with parent provenance and
    fencing metadata. The epoch does not delete ancestry; GC requires fleet
    acknowledgements separately.
    """
    head = log.get_head(realm_id)
    card_payloads = [c.model_dump(mode="json") for c in cards]
    if not advance_epoch or registry is None or authority_instance_id is None or not head:
        # Compatibility path: unreferenced snapshot object only.
        snapshot = {
            "type": "snapshot",
            "realm_id": realm_id,
            "cards": card_payloads,
            "timestamp": datetime.now(UTC).isoformat(),
            "head_hash": head,
        }
        return store.put_json(snapshot)

    current = registry.current(realm_id)
    fencing_token = int((current or {}).get("fencing_token") or 0) + 1
    epoch = create_snapshot_epoch(
        store,
        realm_id=realm_id,
        head_hash=head,
        authority_instance_id=authority_instance_id,
        fencing_token=fencing_token,
        cards=card_payloads,
        parent_epoch_hash=(current or {}).get("epoch_hash"),
        registry=registry,
    )
    logger.info(
        "Created snapshot epoch %s for realm %s at head %s (fence=%s)",
        epoch.epoch_id,
        realm_id,
        head[:12],
        fencing_token,
    )
    return epoch.object_hash
