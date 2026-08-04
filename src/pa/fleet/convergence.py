"""Durable delivery state for canonical fleet membership projections."""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pa.core.io import atomic_write_json
from pa.domain.models import FleetInstance


class MembershipConvergenceStore:
    """Persist per-peer generation delivery with bounded exponential backoff."""

    SCHEMA_VERSION = 1
    MAX_BACKOFF_SECONDS = 300

    def __init__(self, data_dir: Path, local_instance_id: str) -> None:
        self.path = data_dir / "fleet_membership_convergence.json"
        self.local_instance_id = local_instance_id
        self._lock = threading.RLock()

    @staticmethod
    def _now() -> datetime:
        return datetime.now(UTC)

    def _load(self) -> dict[str, Any]:
        try:
            value = json.loads(self.path.read_text())
        except OSError, json.JSONDecodeError:
            value = {}
        if not isinstance(value, dict):
            value = {}
        value.setdefault("schema_version", self.SCHEMA_VERSION)
        value.setdefault("generation", 0)
        value.setdefault("peers", {})
        return value

    def _save(self, state: dict[str, Any]) -> None:
        state["schema_version"] = self.SCHEMA_VERSION
        state["updated_at"] = self._now().isoformat()
        atomic_write_json(self.path, state, mode=0o600)

    def plan(self, generation: int, members: list[FleetInstance]) -> dict[str, Any]:
        """Record every active peer that must receive the complete roster."""
        with self._lock:
            state = self._load()
            state["generation"] = max(int(state.get("generation", 0)), generation)
            peers = state["peers"]
            active_ids: set[str] = set()
            for member in members:
                if (
                    member.instance_id == self.local_instance_id
                    or member.lifecycle_state != "active"
                    or not member.url
                ):
                    continue
                active_ids.add(member.instance_id)
                item = dict(peers.get(member.instance_id) or {})
                previous_target = int(item.get("target_generation", 0))
                item.update(
                    instance_id=member.instance_id,
                    instance_name=member.name,
                    url=member.url.rstrip("/"),
                    target_generation=generation,
                )
                if int(item.get("applied_generation", 0)) < generation:
                    item.update(
                        status="pending",
                        next_attempt_at=(
                            self._now().isoformat()
                            if previous_target != generation
                            else item.get("next_attempt_at") or self._now().isoformat()
                        ),
                    )
                peers[member.instance_id] = item
            for instance_id in list(peers):
                if instance_id not in active_ids:
                    peers.pop(instance_id, None)
            self._save(state)
            return self.public(state)

    def due(self, generation: int) -> list[dict[str, Any]]:
        with self._lock:
            state = self._load()
        now = self._now()
        result = []
        for item in state["peers"].values():
            if int(item.get("applied_generation", 0)) >= generation:
                continue
            raw_due = item.get("next_attempt_at")
            try:
                due_at = datetime.fromisoformat(raw_due) if raw_due else now
            except ValueError:
                due_at = now
            if due_at <= now:
                result.append(dict(item))
        return sorted(result, key=lambda item: item["instance_id"])

    def applied(self, instance_id: str, generation: int) -> None:
        with self._lock:
            state = self._load()
            item = dict(state["peers"].get(instance_id) or {})
            item.update(
                status="applied",
                applied_generation=generation,
                target_generation=generation,
                attempts=int(item.get("attempts", 0)) + 1,
                last_attempt_at=self._now().isoformat(),
                applied_at=self._now().isoformat(),
                next_attempt_at=None,
                error=None,
                error_code=None,
            )
            state["peers"][instance_id] = item
            self._save(state)

    def failed(
        self,
        instance_id: str,
        generation: int,
        error: str,
        *,
        incompatible: bool = False,
    ) -> None:
        with self._lock:
            state = self._load()
            item = dict(state["peers"].get(instance_id) or {})
            attempts = int(item.get("attempts", 0)) + 1
            delay = min(self.MAX_BACKOFF_SECONDS, max(1, 2 ** min(attempts - 1, 8)))
            item.update(
                status="failed" if incompatible else "pending",
                target_generation=generation,
                attempts=attempts,
                last_attempt_at=self._now().isoformat(),
                next_attempt_at=(self._now() + timedelta(seconds=delay)).isoformat(),
                error=str(error)[:500],
                error_code=("incompatible_peer" if incompatible else "delivery_failed"),
            )
            state["peers"][instance_id] = item
            self._save(state)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return self.public(self._load())

    @staticmethod
    def public(state: dict[str, Any]) -> dict[str, Any]:
        peers = [dict(item) for item in state.get("peers", {}).values()]
        peers.sort(key=lambda item: item.get("instance_id", ""))
        generation = int(state.get("generation", 0))
        return {
            "schema_version": int(state.get("schema_version", 1)),
            "generation": generation,
            "updated_at": state.get("updated_at"),
            "peers": peers,
            "applied": sum(item.get("status") == "applied" for item in peers),
            "pending": sum(item.get("status") == "pending" for item in peers),
            "failed": sum(item.get("status") == "failed" for item in peers),
        }
