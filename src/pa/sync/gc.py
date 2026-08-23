"""Reachability-indexed garbage-collection planning for sync objects.

Planning is dry-run by default. Deletion requires an acknowledged snapshot
epoch, explicit operator confirmation, and surviving safety pins. Offline peers
that have not acknowledged the epoch keep pre-epoch ancestry pinned and surface
an explicit rebootstrap requirement instead of silent deletion.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from pa.core.io import atomic_write_json
from pa.sync.epochs import EpochRegistry
from pa.sync.object_catalog import ObjectCatalog
from pa.sync.object_store import ObjectStore

GC_PLAN_SCHEMA = 1
DEFAULT_SAFETY_WINDOW = timedelta(days=14)


@dataclass
class GcPin:
    kind: str
    object_hash: str
    reason: str
    source: str = ""


@dataclass
class GcPlan:
    plan_id: str
    realm_id: str
    created_at: str
    dry_run: bool = True
    epoch_hash: str | None = None
    reclaimable: bool = False
    candidates: list[str] = field(default_factory=list)
    pinned: list[dict[str, str]] = field(default_factory=list)
    missing_acks: list[str] = field(default_factory=list)
    rebootstrap_required: bool = True
    safety_window_seconds: int = int(DEFAULT_SAFETY_WINDOW.total_seconds())
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": GC_PLAN_SCHEMA,
            "plan_id": self.plan_id,
            "realm_id": self.realm_id,
            "created_at": self.created_at,
            "dry_run": self.dry_run,
            "epoch_hash": self.epoch_hash,
            "reclaimable": self.reclaimable,
            "candidate_count": len(self.candidates),
            "candidates": self.candidates[:500],
            "candidates_truncated": len(self.candidates) > 500,
            "pinned": self.pinned[:500],
            "missing_acknowledgements": self.missing_acks,
            "rebootstrap_required": self.rebootstrap_required,
            "safety_window_seconds": self.safety_window_seconds,
            "notes": self.notes,
        }


class GcPlanner:
    """Build auditable GC plans from catalog + epoch + pin inputs."""

    def __init__(
        self,
        data_dir: Path,
        store: ObjectStore,
        catalog: ObjectCatalog,
        epochs: EpochRegistry,
    ) -> None:
        self.data_dir = data_dir
        self.store = store
        self.catalog = catalog
        self.epochs = epochs
        self.journal_path = data_dir / "sync_gc_journal.json"
        self._lock = threading.RLock()

    def _load_journal(self) -> dict[str, Any]:
        if not self.journal_path.exists():
            return {"schema_version": GC_PLAN_SCHEMA, "plans": {}, "deletions": []}
        try:
            loaded = json.loads(self.journal_path.read_text())
        except (OSError, json.JSONDecodeError, TypeError):
            return {"schema_version": GC_PLAN_SCHEMA, "plans": {}, "deletions": []}
        return loaded if isinstance(loaded, dict) else {
            "schema_version": GC_PLAN_SCHEMA,
            "plans": {},
            "deletions": [],
        }

    def _save_journal(self, journal: dict[str, Any]) -> None:
        atomic_write_json(self.journal_path, journal, mode=0o600)

    def plan(
        self,
        realm_id: str,
        *,
        reachable_hashes: set[str],
        pins: list[GcPin],
        required_ack_instances: list[str],
        dry_run: bool = True,
        safety_window: timedelta = DEFAULT_SAFETY_WINDOW,
        now: datetime | None = None,
    ) -> GcPlan:
        """Compute reclaim candidates without deleting anything."""
        now = now or datetime.now(UTC)
        epoch = self.epochs.current(realm_id)
        plan = GcPlan(
            plan_id=str(uuid4()),
            realm_id=realm_id,
            created_at=now.isoformat(),
            dry_run=dry_run,
            safety_window_seconds=int(safety_window.total_seconds()),
        )
        if not epoch:
            plan.notes.append(
                "No snapshot epoch exists; refuse reclaim until an epoch is "
                "created, replicated, and acknowledged."
            )
            plan.rebootstrap_required = True
            return self._persist_plan(plan)

        plan.epoch_hash = epoch.get("epoch_hash")
        quorum = self.epochs.mark_reclaimable_if_quorum(
            realm_id, required_instance_ids=required_ack_instances
        )
        plan.missing_acks = list(quorum.get("missing_acknowledgements") or [])
        plan.reclaimable = bool(quorum.get("reclaimable"))
        plan.rebootstrap_required = True
        if not plan.reclaimable:
            plan.notes.append(
                "Epoch is not fleet-acknowledged; offline or unsubscribed peers "
                "must acknowledge or be explicitly declared for rebootstrap "
                "before history can be reclaimed."
            )

        pin_hashes = {pin.object_hash for pin in pins}
        pin_hashes.add(str(epoch.get("epoch_hash")))
        if epoch.get("head_hash"):
            pin_hashes.add(str(epoch["head_hash"]))
        if epoch.get("parent_epoch_hash"):
            pin_hashes.add(str(epoch["parent_epoch_hash"]))
        plan.pinned = [
            {
                "kind": pin.kind,
                "object_hash": pin.object_hash,
                "reason": pin.reason,
                "source": pin.source,
            }
            for pin in pins
        ]

        cutoff_ns = int((now - safety_window).timestamp() * 1_000_000_000)
        catalog_hashes = self.catalog.iter_hashes()
        for object_hash in catalog_hashes:
            if object_hash in reachable_hashes or object_hash in pin_hashes:
                continue
            # Safety window: recently written unreachable objects stay for recovery.
            with self.catalog._db() as conn:
                row = conn.execute(
                    "SELECT mtime_ns, realm_id FROM objects WHERE object_hash=?",
                    (object_hash,),
                ).fetchone()
            if row is None:
                continue
            if row["realm_id"] not in {None, realm_id}:
                continue
            if int(row["mtime_ns"]) > cutoff_ns:
                plan.pinned.append(
                    {
                        "kind": "safety_window",
                        "object_hash": object_hash,
                        "reason": "inside_safety_window",
                        "source": "gc_planner",
                    }
                )
                continue
            plan.candidates.append(object_hash)

        if not plan.reclaimable:
            plan.notes.append(
                f"Dry-run identified {len(plan.candidates)} candidates; none are "
                "deletable until epoch acknowledgements complete."
            )
        return self._persist_plan(plan)

    def _persist_plan(self, plan: GcPlan) -> GcPlan:
        with self._lock:
            journal = self._load_journal()
            plans = journal.setdefault("plans", {})
            plans[plan.plan_id] = plan.to_dict()
            # Retain a bounded audit window.
            if len(plans) > 50:
                for old_id in sorted(plans, key=lambda key: plans[key].get("created_at", ""))[
                    : len(plans) - 50
                ]:
                    plans.pop(old_id, None)
            self._save_journal(journal)
        return plan

    def execute(
        self,
        plan_id: str,
        *,
        confirm: bool,
        delete: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        """Apply a previously audited plan with crash-safe journalled deletes."""
        if not confirm:
            raise ValueError("refusing GC execute without confirm=true")
        with self._lock:
            journal = self._load_journal()
            plan = journal.get("plans", {}).get(plan_id)
            if not plan:
                raise ValueError("unknown GC plan")
            if plan.get("dry_run"):
                raise ValueError("plan was dry-run only; create an execute plan first")
            if not plan.get("reclaimable"):
                raise ValueError("epoch acknowledgements incomplete; GC blocked")
            if plan.get("missing_acknowledgements"):
                raise ValueError("missing peer acknowledgements; GC blocked")

            deleted: list[str] = []
            failed: list[dict[str, str]] = []
            deleter = delete or self._safe_delete
            # Journal intent before side effects for crash restart.
            journal.setdefault("deletions", []).append(
                {
                    "plan_id": plan_id,
                    "started_at": datetime.now(UTC).isoformat(),
                    "candidates": plan.get("candidates", []),
                    "state": "in_progress",
                }
            )
            self._save_journal(journal)

        for object_hash in plan.get("candidates", []):
            try:
                deleter(object_hash)
                deleted.append(object_hash)
                self.catalog.discard(object_hash)
            except Exception as exc:  # noqa: BLE001 - audit each failure
                failed.append({"object_hash": object_hash, "error": str(exc)})

        with self._lock:
            journal = self._load_journal()
            for entry in reversed(journal.get("deletions", [])):
                if entry.get("plan_id") == plan_id and entry.get("state") == "in_progress":
                    entry["state"] = "completed" if not failed else "partial"
                    entry["deleted"] = deleted
                    entry["failed"] = failed
                    entry["completed_at"] = datetime.now(UTC).isoformat()
                    break
            self._save_journal(journal)

        return {
            "plan_id": plan_id,
            "deleted_count": len(deleted),
            "failed_count": len(failed),
            "deleted": deleted[:100],
            "failed": failed[:100],
            "rebootstrap_required": True,
            "notes": [
                "Pre-epoch ancestry was reclaimed. Peers missing the epoch must "
                "rebootstrap from the snapshot epoch root rather than replay "
                "deleted history."
            ],
        }

    def _safe_delete(self, object_hash: str) -> None:
        path = self.store._path_for(object_hash)
        if not path.exists():
            return
        # Rename aside first so a crash mid-delete leaves recoverable tombstones.
        tomb = path.with_name(path.name + f".gc-tomb-{int(time.time_ns())}")
        path.replace(tomb)
        tomb.unlink(missing_ok=True)

    def resume_interrupted(self) -> dict[str, Any]:
        """Complete or report in-progress journalled deletions after crash."""
        with self._lock:
            journal = self._load_journal()
            interrupted = [
                entry
                for entry in journal.get("deletions", [])
                if entry.get("state") == "in_progress"
            ]
        results = []
        for entry in interrupted:
            results.append(
                self.execute(entry["plan_id"], confirm=True)
            )
        return {"resumed": len(results), "results": results}
