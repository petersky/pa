"""Versioned snapshot/checkpoint epoch protocol for bounded sync retention.

Epoch roots are ordinary content-addressed objects, advanced through the same
authority/ref fencing as card mutations. Fleet peers must acknowledge an epoch
before ancestry beneath it becomes reclaimable.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from pa.core.io import atomic_write_json
from pa.sync.object_store import ObjectStore, object_hash

EPOCH_SCHEMA_VERSION = 1
EPOCH_TYPE = "snapshot_epoch"
ACK_TYPE = "snapshot_epoch_ack"


@dataclass(frozen=True)
class SnapshotEpoch:
    """Immutable checkpoint root for one realm."""

    epoch_id: str
    realm_id: str
    head_hash: str
    parent_epoch_hash: str | None
    authority_instance_id: str
    fencing_token: int
    projection_digest: str
    created_at: str
    schema_version: int = EPOCH_SCHEMA_VERSION
    object_hash: str = ""

    def to_payload(self) -> dict[str, Any]:
        return {
            "type": EPOCH_TYPE,
            "schema_version": self.schema_version,
            "epoch_id": self.epoch_id,
            "realm_id": self.realm_id,
            "head_hash": self.head_hash,
            "parent_epoch_hash": self.parent_epoch_hash,
            "authority_instance_id": self.authority_instance_id,
            "fencing_token": self.fencing_token,
            "projection_digest": self.projection_digest,
            "created_at": self.created_at,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any], *, object_hash_value: str = "") -> SnapshotEpoch:
        return cls(
            epoch_id=str(payload["epoch_id"]),
            realm_id=str(payload["realm_id"]),
            head_hash=str(payload["head_hash"]),
            parent_epoch_hash=(
                str(payload["parent_epoch_hash"])
                if payload.get("parent_epoch_hash")
                else None
            ),
            authority_instance_id=str(payload["authority_instance_id"]),
            fencing_token=int(payload.get("fencing_token") or 1),
            projection_digest=str(payload.get("projection_digest") or ""),
            created_at=str(payload.get("created_at") or datetime.now(UTC).isoformat()),
            schema_version=int(payload.get("schema_version") or EPOCH_SCHEMA_VERSION),
            object_hash=object_hash_value,
        )


class EpochRegistry:
    """Durable local registry of epoch roots and peer acknowledgements."""

    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir / "sync_epochs.json"
        self._lock = threading.RLock()
        self._state: dict[str, Any] = {
            "schema_version": EPOCH_SCHEMA_VERSION,
            "realms": {},
        }
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            loaded = json.loads(self.path.read_text())
        except (OSError, json.JSONDecodeError, TypeError):
            return
        if isinstance(loaded, dict):
            self._state.update(loaded)

    def _save(self) -> None:
        atomic_write_json(self.path, self._state, mode=0o600)

    def current(self, realm_id: str) -> dict[str, Any] | None:
        with self._lock:
            realm = self._state.get("realms", {}).get(realm_id)
            return dict(realm) if isinstance(realm, dict) else None

    def record_epoch(self, epoch: SnapshotEpoch) -> dict[str, Any]:
        with self._lock:
            realms = self._state.setdefault("realms", {})
            existing = realms.get(epoch.realm_id) or {}
            prior_token = int(existing.get("fencing_token") or 0)
            if epoch.fencing_token < prior_token:
                raise ValueError("snapshot epoch fencing token moved backwards")
            if (
                epoch.fencing_token == prior_token
                and existing.get("epoch_hash")
                and existing.get("epoch_hash") != epoch.object_hash
            ):
                raise ValueError("snapshot epoch fencing conflict")
            record = {
                "epoch_id": epoch.epoch_id,
                "epoch_hash": epoch.object_hash,
                "head_hash": epoch.head_hash,
                "parent_epoch_hash": epoch.parent_epoch_hash,
                "fencing_token": epoch.fencing_token,
                "projection_digest": epoch.projection_digest,
                "authority_instance_id": epoch.authority_instance_id,
                "created_at": epoch.created_at,
                "acknowledgements": existing.get("acknowledgements")
                if existing.get("epoch_hash") == epoch.object_hash
                else {},
                "reclaimable": False,
                "rebootstrap_required_after_gc": True,
            }
            if "acknowledgements" not in record or not isinstance(
                record["acknowledgements"], dict
            ):
                record["acknowledgements"] = {}
            # Authority auto-acknowledges its own epoch.
            record["acknowledgements"][epoch.authority_instance_id] = {
                "acknowledged_at": datetime.now(UTC).isoformat(),
                "instance_id": epoch.authority_instance_id,
            }
            realms[epoch.realm_id] = record
            self._save()
            return dict(record)

    def acknowledge(
        self,
        realm_id: str,
        *,
        epoch_hash: str,
        instance_id: str,
    ) -> dict[str, Any]:
        with self._lock:
            realm = self._state.setdefault("realms", {}).get(realm_id)
            if not realm or realm.get("epoch_hash") != epoch_hash:
                raise ValueError("epoch acknowledgement does not match current epoch")
            acks = realm.setdefault("acknowledgements", {})
            acks[instance_id] = {
                "acknowledged_at": datetime.now(UTC).isoformat(),
                "instance_id": instance_id,
            }
            self._save()
            return dict(realm)

    def mark_reclaimable_if_quorum(
        self,
        realm_id: str,
        *,
        required_instance_ids: list[str],
    ) -> dict[str, Any]:
        """Enable reclaim only after every subscribed/required peer has ACK'd."""
        with self._lock:
            realm = self._state.setdefault("realms", {}).get(realm_id)
            if not realm:
                raise ValueError("no snapshot epoch for realm")
            acks = set((realm.get("acknowledgements") or {}).keys())
            missing = sorted(set(required_instance_ids) - acks)
            realm["missing_acknowledgements"] = missing
            realm["reclaimable"] = not missing
            realm["acknowledged_at_quorum"] = (
                datetime.now(UTC).isoformat() if not missing else None
            )
            self._save()
            return dict(realm)

    def public(self, realm_id: str) -> dict[str, Any]:
        current = self.current(realm_id)
        if not current:
            return {
                "schema_version": EPOCH_SCHEMA_VERSION,
                "realm_id": realm_id,
                "epoch": None,
                "reclaimable": False,
            }
        return {
            "schema_version": EPOCH_SCHEMA_VERSION,
            "realm_id": realm_id,
            "epoch": current,
            "reclaimable": bool(current.get("reclaimable")),
        }


def projection_digest(cards: list[dict[str, Any]]) -> str:
    canonical = json.dumps(cards, sort_keys=True, separators=(",", ":"), default=str)
    return object_hash(canonical.encode())


def create_snapshot_epoch(
    store: ObjectStore,
    *,
    realm_id: str,
    head_hash: str,
    authority_instance_id: str,
    fencing_token: int,
    cards: list[dict[str, Any]],
    parent_epoch_hash: str | None,
    registry: EpochRegistry,
) -> SnapshotEpoch:
    """Write a content-addressed epoch root and register it locally."""
    epoch = SnapshotEpoch(
        epoch_id=str(uuid4()),
        realm_id=realm_id,
        head_hash=head_hash,
        parent_epoch_hash=parent_epoch_hash,
        authority_instance_id=authority_instance_id,
        fencing_token=fencing_token,
        projection_digest=projection_digest(cards),
        created_at=datetime.now(UTC).isoformat(),
    )
    # Include a compact projection snapshot so peers can rebootstrap without
    # replaying pre-epoch ancestry after acknowledged GC.
    payload = epoch.to_payload()
    payload["cards"] = cards
    object_hash_value = store.put_json(payload)
    epoch = SnapshotEpoch.from_payload(payload, object_hash_value=object_hash_value)
    registry.record_epoch(epoch)
    return epoch


def acknowledge_epoch_object(
    store: ObjectStore,
    registry: EpochRegistry,
    *,
    realm_id: str,
    epoch_hash: str,
    instance_id: str,
) -> dict[str, Any]:
    raw = store.get(epoch_hash)
    if raw is None:
        raise ValueError("epoch object missing locally")
    payload = json.loads(raw.decode())
    if payload.get("type") != EPOCH_TYPE:
        raise ValueError("object is not a snapshot epoch")
    if payload.get("realm_id") != realm_id:
        raise ValueError("epoch realm mismatch")
    ack_payload = {
        "type": ACK_TYPE,
        "schema_version": EPOCH_SCHEMA_VERSION,
        "realm_id": realm_id,
        "epoch_hash": epoch_hash,
        "instance_id": instance_id,
        "acknowledged_at": datetime.now(UTC).isoformat(),
    }
    store.put_json(ack_payload)
    return registry.acknowledge(
        realm_id, epoch_hash=epoch_hash, instance_id=instance_id
    )
