"""Fleet registry — instances owned by this fleet."""

from __future__ import annotations

import json
import re
import secrets
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from pa.core.io import atomic_write_json
from pa.domain.models import FleetInstance, FleetJoinToken

_SEMANTIC_MEMBER_FIELDS = {
    "instance_id",
    "name",
    "url",
    "endpoints",
    "zone",
    "capabilities",
    "dispatch_capacity",
    "dispatch_provider_capacities",
    "dispatch_queue_capacity",
    "dispatch_provider_queue_capacities",
    "relay_enabled",
    "lifecycle_state",
    "credential_fingerprint",
}


def semantic_member(inst: FleetInstance) -> dict[str, Any]:
    """Return stable membership truth, excluding local observations/provenance."""
    data = inst.model_dump(mode="json", include=_SEMANTIC_MEMBER_FIELDS)
    data["endpoints"] = sorted(data["endpoints"])
    data["capabilities"] = sorted(data["capabilities"])
    return data


def semantic_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Normalize a snapshot for equality and conflict decisions."""
    members = [
        semantic_member(FleetInstance.model_validate(item))
        for item in snapshot.get("instances", [])
    ]
    return {
        "fleet_id": snapshot.get("fleet_id"),
        "instances": sorted(members, key=lambda item: item["instance_id"]),
    }


def _validated_instances(snapshot: dict[str, Any]) -> list[FleetInstance]:
    incoming = [
        FleetInstance.model_validate(item) for item in snapshot.get("instances", [])
    ]
    ids: set[str] = set()
    endpoints: dict[str, str] = {}
    conflicts: list[dict[str, str]] = []
    for inst in incoming:
        if inst.instance_id in ids:
            conflicts.append({"type": "duplicate_id", "instance_id": inst.instance_id})
        ids.add(inst.instance_id)
        for endpoint in inst.endpoints:
            owner = endpoints.get(endpoint)
            if owner and owner != inst.instance_id:
                conflicts.append(
                    {
                        "type": "duplicate_endpoint",
                        "endpoint": endpoint,
                        "instances": f"{owner},{inst.instance_id}",
                    }
                )
            endpoints[endpoint] = inst.instance_id
    if conflicts:
        raise ValueError(f"ambiguous membership snapshot: {conflicts}")
    return incoming


def reconcile_snapshots(snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    """Deterministically select or merge compatible authenticated rosters."""
    if not snapshots:
        raise ValueError("at least one membership snapshot is required")
    fleet_ids = {str(snapshot.get("fleet_id", "")) for snapshot in snapshots}
    if len(fleet_ids) != 1:
        raise ValueError("membership snapshots belong to different fleets")
    generations = [int(snapshot.get("generation", 0)) for snapshot in snapshots]
    schemas = [int(snapshot.get("schema_version", 1)) for snapshot in snapshots]
    candidates: dict[str, list[tuple[int, dict[str, Any], str]]] = {}
    validated: list[tuple[dict[str, Any], list[FleetInstance]]] = []
    for snapshot in snapshots:
        members = _validated_instances(snapshot)
        validated.append((snapshot, members))
        generation = int(snapshot.get("generation", 0))
        for inst in members:
            candidates.setdefault(inst.instance_id, []).append(
                (
                    generation,
                    semantic_member(inst),
                    json.dumps(
                        inst.model_dump(mode="json"),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                )
            )

    # A newer roster supersedes an older label for the same stable UUID. Only
    # semantic differences at the same highest generation are ambiguous.
    by_id: dict[str, tuple[dict[str, Any], str]] = {}
    for instance_id, values in candidates.items():
        newest_generation = max(item[0] for item in values)
        newest = [item for item in values if item[0] == newest_generation]
        semantics = {
            json.dumps(item[1], sort_keys=True, separators=(",", ":"))
            for item in newest
        }
        if len(semantics) != 1:
            raise ValueError(
                f"conflicting canonical member {instance_id} "
                "requires operator resolution"
            )
        selected = min(newest, key=lambda item: item[2])
        by_id[instance_id] = (selected[1], selected[2])

    endpoint_owners: dict[str, str] = {}
    for instance_id, (stable, _serialized) in by_id.items():
        for endpoint in stable["endpoints"]:
            owner = endpoint_owners.get(endpoint)
            if owner and owner != instance_id:
                raise ValueError(
                    f"conflicting canonical endpoint {endpoint} belongs to both "
                    f"{owner} and {instance_id}"
                )
            endpoint_owners[endpoint] = instance_id
    merged_instances = [json.loads(by_id[key][1]) for key in sorted(by_id)]
    highest = max(generations)
    merged_semantic = {
        "fleet_id": next(iter(fleet_ids)),
        "instances": [by_id[key][0] for key in sorted(by_id)],
    }
    leaders = [
        snapshot
        for snapshot, _members in validated
        if int(snapshot.get("generation", 0)) == highest
        and semantic_snapshot(snapshot) == merged_semantic
    ]
    if leaders:
        selected = min(
            leaders,
            key=lambda snapshot: json.dumps(
                snapshot, sort_keys=True, separators=(",", ":")
            ),
        )
        return {
            **selected,
            "schema_version": max(schemas),
            "instances": merged_instances,
        }
    return {
        "schema_version": max(schemas),
        "fleet_id": next(iter(fleet_ids)),
        "generation": highest + 1,
        "instances": merged_instances,
    }


class FleetRegistry:
    SCHEMA_VERSION = 3

    def __init__(self, data_dir: Path, fleet_id: str) -> None:
        self.fleet_id = fleet_id
        self.instances_path = data_dir / "fleet_instances.json"
        self.tokens_path = data_dir / "fleet_join_tokens.json"
        self.audit_path = data_dir / "fleet_membership_audit.jsonl"
        self._instances: dict[str, FleetInstance] = {}
        self._tokens: dict[str, FleetJoinToken] = {}
        self._generation = 0
        self._load()

    def _load(self) -> None:
        self._reload_instances()
        self._reload_tokens()

    def _reload_instances(self) -> None:
        """Merge instances from disk so CLI and server stay consistent."""
        if not self.instances_path.exists():
            return
        try:
            data = json.loads(self.instances_path.read_text())
            if data.get("fleet_id") not in (None, "", self.fleet_id):
                return
            self._generation = max(self._generation, int(data.get("generation", 0)))
            raw_instances = data.get("instances", [])
            for item in raw_instances:
                inst = FleetInstance.model_validate(item)
                current = self._instances.get(inst.instance_id)
                if (
                    current is None
                    or inst.membership_generation >= current.membership_generation
                ):
                    self._instances[inst.instance_id] = inst
            if "schema_version" not in data:
                # Legacy registries had no version. Member count is only a migration
                # ordering hint; equal-sized but different rosters remain conflicts.
                self._generation = max(self._generation, len(raw_instances))
        except json.JSONDecodeError, ValueError:
            pass

    def _reload_tokens(self) -> None:
        """Merge tokens from disk so CLI-minted tokens work with a live server."""
        if not self.tokens_path.exists():
            return
        try:
            data = json.loads(self.tokens_path.read_text())
            now = datetime.now(UTC)
            for item in data.get("tokens", []):
                tok = FleetJoinToken.model_validate(item)
                if tok.expires_at > now:
                    self._tokens[tok.token] = tok
        except json.JSONDecodeError, ValueError:
            pass

    def _save_instances(self) -> None:
        self.instances_path.parent.mkdir(parents=True, exist_ok=True)
        payload = self.snapshot()
        atomic_write_json(self.instances_path, payload)

    @staticmethod
    def normalize_name(name: str) -> str:
        value = str(name or "").strip()
        if not value:
            raise ValueError("instance name cannot be empty")
        if len(value) > 120:
            raise ValueError("instance name must be at most 120 characters")
        if re.search(r"[\x00-\x1f\x7f]", value):
            raise ValueError("instance name cannot contain control characters")
        return value

    def _validate_name_available(self, instance_id: str, name: str) -> str:
        normalized = self.normalize_name(name)
        for other in self._instances.values():
            if (
                other.instance_id != instance_id
                and other.lifecycle_state == "active"
                and other.name.casefold() == normalized.casefold()
            ):
                raise ValueError(
                    "instance name already belongs to canonical member "
                    f"{other.instance_id}"
                )
        return normalized

    def _audit(
        self, action: str, *, actor: str = "", detail: dict[str, Any] | None = None
    ) -> None:
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "event_id": str(uuid4()),
            "timestamp": datetime.now(UTC).isoformat(),
            "fleet_id": self.fleet_id,
            "generation": self._generation,
            "action": action,
            "actor": actor or "system",
            "detail": detail or {},
        }
        with self.audit_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")

    def snapshot(self) -> dict[str, Any]:
        return {
            "schema_version": self.SCHEMA_VERSION,
            "fleet_id": self.fleet_id,
            "generation": self._generation,
            "instances": [
                item.model_dump(mode="json")
                for item in sorted(
                    self._instances.values(), key=lambda value: value.instance_id
                )
            ],
        }

    @property
    def generation(self) -> int:
        self._reload_instances()
        return self._generation

    def _save_tokens(self) -> None:
        # Drop expired before save
        now = datetime.now(UTC)
        self._tokens = {k: v for k, v in self._tokens.items() if v.expires_at > now}
        payload = {"tokens": [t.model_dump(mode="json") for t in self._tokens.values()]}
        atomic_write_json(self.tokens_path, payload)

    def register_self(
        self,
        instance_id: str,
        name: str,
        url: str,
        *,
        zone: str = "default",
        capabilities: list[str] | None = None,
        dispatch_capacity: int | None = None,
        dispatch_provider_capacities: dict[str, int] | None = None,
        dispatch_queue_capacity: int = 100,
        dispatch_provider_queue_capacities: dict[str, int] | None = None,
        relay_enabled: bool = False,
        actor: str = "",
    ) -> FleetInstance:
        self._reload_instances()
        previous = self._instances.get(instance_id)
        # Startup/health advertisement is not an identity mutation. Once this
        # UUID is canonical, stale local config/defaults must not rename it.
        name = (
            previous.name
            if previous
            else self._validate_name_available(instance_id, name)
        )
        normalized_url = url.rstrip("/")
        if (
            previous
            and previous.lifecycle_state == "active"
            and previous.name == name
            and previous.url == normalized_url
            and previous.zone == zone
            and previous.capabilities == (capabilities or [])
            and previous.dispatch_capacity == dispatch_capacity
            and previous.dispatch_provider_capacities
            == (dispatch_provider_capacities or {})
            and previous.dispatch_queue_capacity == dispatch_queue_capacity
            and previous.dispatch_provider_queue_capacities
            == (dispatch_provider_queue_capacities or {})
            and previous.relay_enabled == relay_enabled
        ):
            previous.last_seen = datetime.now(UTC)
            previous.healthy = True
            return previous
        inst = FleetInstance(
            instance_id=instance_id,
            name=name,
            url=normalized_url,
            zone=zone,
            capabilities=capabilities or [],
            dispatch_capacity=dispatch_capacity,
            dispatch_provider_capacities=dispatch_provider_capacities or {},
            dispatch_queue_capacity=dispatch_queue_capacity,
            dispatch_provider_queue_capacities=(
                dispatch_provider_queue_capacities or {}
            ),
            relay_enabled=relay_enabled,
            joined_by=actor,
            updated_by=actor,
            last_seen=datetime.now(UTC),
            healthy=True,
        )
        if previous:
            inst.joined_at = previous.joined_at
            inst.joined_by = previous.joined_by
        self._generation += 1
        inst.membership_generation = self._generation
        self._instances[instance_id] = inst
        self._save_instances()
        self._audit("member.upsert", actor=actor, detail={"instance_id": instance_id})
        return inst

    def upsert_instance(self, inst: FleetInstance, *, actor: str = "") -> FleetInstance:
        self._reload_instances()
        previous = self._instances.get(inst.instance_id)
        if previous and previous.lifecycle_state == "removed":
            raise ValueError(
                "removed member cannot be reintroduced; explicitly restore it"
            )
        inst.name = self._validate_name_available(inst.instance_id, inst.name)
        incoming_endpoints = set(inst.endpoints)
        for other in self._instances.values():
            if (
                other.instance_id == inst.instance_id
                or other.lifecycle_state == "removed"
            ):
                continue
            collision = incoming_endpoints.intersection(other.endpoints)
            if collision:
                raise ValueError(
                    "endpoint already belongs to canonical member "
                    f"{other.instance_id}: {min(collision)}"
                )
        if previous:
            inst.joined_at = previous.joined_at
            inst.joined_by = previous.joined_by
        self._generation += 1
        inst.membership_generation = self._generation
        inst.updated_at = datetime.now(UTC)
        inst.updated_by = actor
        self._instances[inst.instance_id] = inst
        self._save_instances()
        self._audit(
            "member.upsert", actor=actor, detail={"instance_id": inst.instance_id}
        )
        return inst

    def rename_instance(
        self,
        instance_id: str,
        name: str,
        *,
        actor: str,
        source: str,
        expected_generation: int | None = None,
    ) -> FleetInstance:
        """Version one UUID-preserving canonical rename with explicit fencing."""
        self._reload_instances()
        current = self._instances.get(instance_id)
        if current is None or current.lifecycle_state == "removed":
            raise ValueError("canonical instance does not exist")
        if expected_generation is not None and expected_generation != self._generation:
            raise ValueError(
                "membership generation changed "
                f"(expected {expected_generation}, current {self._generation})"
            )
        normalized = self._validate_name_available(instance_id, name)
        if current.name == normalized:
            return current
        old_name = current.name
        self._generation += 1
        current.name = normalized
        current.membership_generation = self._generation
        current.updated_at = datetime.now(UTC)
        current.updated_by = actor
        self._save_instances()
        self._audit(
            "member.rename",
            actor=actor,
            detail={
                "instance_id": instance_id,
                "old_name": old_name,
                "new_name": normalized,
                "source": source,
                "member_generation": current.membership_generation,
            },
        )
        return current

    def list_instances(self, *, include_removed: bool = False) -> list[FleetInstance]:
        self._reload_instances()
        values = list(self._instances.values())
        if include_removed:
            return values
        return [item for item in values if item.lifecycle_state != "removed"]

    def get_instance(
        self, instance_id: str, *, include_removed: bool = False
    ) -> FleetInstance | None:
        self._reload_instances()
        inst = self._instances.get(instance_id)
        if inst and inst.lifecycle_state == "removed" and not include_removed:
            return None
        return inst

    def remove_instance(self, instance_id: str, *, actor: str = "") -> bool:
        self._reload_instances()
        inst = self._instances.get(instance_id)
        if not inst:
            return False
        if inst.lifecycle_state == "removed":
            return True
        self._generation += 1
        inst.lifecycle_state = "removed"
        inst.removed_at = datetime.now(UTC)
        inst.removed_by = actor
        inst.updated_at = inst.removed_at
        inst.updated_by = actor
        inst.membership_generation = self._generation
        self._save_instances()
        self._audit("member.remove", actor=actor, detail={"instance_id": instance_id})
        return True

    def apply_snapshot(
        self,
        snapshot: dict[str, Any],
        *,
        actor: str = "",
        require_newer: bool = True,
    ) -> dict[str, Any]:
        """Apply an authenticated canonical projection without guessing identities."""
        self._reload_instances()
        if snapshot.get("fleet_id") != self.fleet_id:
            raise ValueError("snapshot belongs to a different fleet")
        schema = int(snapshot.get("schema_version", 1))
        if schema > self.SCHEMA_VERSION:
            raise ValueError(f"unsupported membership schema {schema}")
        incoming_generation = int(snapshot.get("generation", 0))
        if require_newer and incoming_generation < self._generation:
            raise ValueError("stale membership snapshot")
        incoming = _validated_instances(snapshot)
        if incoming_generation == self._generation and semantic_snapshot(
            self.snapshot()
        ) != semantic_snapshot(snapshot):
            raise ValueError("conflicting membership snapshot at the same generation")
        before = self._generation
        before_semantic = semantic_snapshot(self.snapshot())
        self._instances = {inst.instance_id: inst for inst in incoming}
        self._generation = incoming_generation
        self._save_instances()
        changed = before_semantic != semantic_snapshot(self.snapshot())
        self._audit(
            "projection.apply",
            actor=actor,
            detail={
                "before_generation": before,
                "after_generation": self._generation,
                "changed": changed,
            },
        )
        return {
            "changed": changed,
            "before_generation": before,
            "after_generation": self._generation,
            "members": len(self.list_instances()),
        }

    def audit_events(self, *, limit: int = 100) -> list[dict[str, Any]]:
        if not self.audit_path.exists():
            return []
        records: list[dict[str, Any]] = []
        for line in self.audit_path.read_text(encoding="utf-8").splitlines():
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return records[-max(1, min(limit, 1000)) :]

    def create_join_token(
        self, *, ttl_hours: int = 24, created_by: str = ""
    ) -> FleetJoinToken:
        self._reload_tokens()
        token = secrets.token_urlsafe(32)
        join = FleetJoinToken(
            token=token,
            fleet_id=self.fleet_id,
            expires_at=datetime.now(UTC) + timedelta(hours=ttl_hours),
            created_by=created_by,
        )
        self._tokens[token] = join
        self._save_tokens()
        return join

    def consume_join_token(self, token: str) -> FleetJoinToken | None:
        self._reload_tokens()
        join = self._tokens.get(token)
        if not join:
            return None
        if join.expires_at < datetime.now(UTC):
            del self._tokens[token]
            self._save_tokens()
            return None
        del self._tokens[token]
        self._save_tokens()
        return join
