"""Append-only event log with git-style commits."""

from __future__ import annotations

import fcntl
import json
import os
import threading
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from pa.core.io import atomic_write_json
from pa.domain.models import CardEvent, EventType, SyncCommit, SyncRef
from pa.sync.object_store import ObjectStore, object_hash

_AUTOMATIC_METADATA_FIELDS = {("card", "updated_at")}
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_FLEET_EVENT_ENTITY = {
    EventType.INSTANCE_GROUP_CREATED: "instance_group",
    EventType.INSTANCE_GROUP_UPDATED: "instance_group",
    EventType.INSTANCE_GROUP_ARCHIVED: "instance_group",
    EventType.INSTANCE_GROUP_DELETED: "instance_group",
    EventType.INSTANCE_PARTICIPATION_POLICY_UPDATED: "instance_policy",
    EventType.PLACEMENT_DEFAULT_UPDATED: "placement_default",
    EventType.PLACEMENT_DEFAULT_DELETED: "placement_default",
    EventType.NOTIFICATION_UPSERTED: "notification",
    EventType.NOTIFICATION_DELETED: "notification",
}
_FLEET_ENTITY_UPDATE_EVENT = {
    "instance_group": EventType.INSTANCE_GROUP_UPDATED,
    "instance_policy": EventType.INSTANCE_PARTICIPATION_POLICY_UPDATED,
    "placement_default": EventType.PLACEMENT_DEFAULT_UPDATED,
    "notification": EventType.NOTIFICATION_UPSERTED,
}
_FLEET_ENTITY_DELETE_EVENT = {
    "instance_group": EventType.INSTANCE_GROUP_DELETED,
    "placement_default": EventType.PLACEMENT_DEFAULT_DELETED,
    "notification": EventType.NOTIFICATION_DELETED,
}


def _event_entity(event: CardEvent) -> tuple[str | None, str | None]:
    if event.type == EventType.GOAL_UPSERTED:
        goal = event.payload.get("goal") or {}
        goal_id = str(goal.get("id") or "")
        return ("goal", goal_id or None)
    if event.type == EventType.GOAL_GOVERNANCE_UPSERTED:
        entity_type = str(event.payload.get("entity_type") or "")
        entity_id = str(event.payload.get("entity_id") or "")
        identity = f"{entity_type}:{entity_id}" if entity_type and entity_id else None
        return ("goal_governance", identity)
    if event.card_id:
        return "card", event.card_id
    if event.project_id and event.type not in {
        EventType.PLACEMENT_DEFAULT_UPDATED,
        EventType.PLACEMENT_DEFAULT_DELETED,
    }:
        return "project", event.project_id
    entity = _FLEET_EVENT_ENTITY.get(event.type)
    if entity == "instance_group":
        return entity, str(event.payload.get("id") or "") or None
    if entity == "instance_policy":
        return entity, str(event.payload.get("instance_id") or "") or None
    if entity == "placement_default":
        scope_key = event.payload.get("scope_key")
        if not scope_key:
            scope_key = (
                f"project:{event.payload.get('project_id') or '*'}:"
                f"profile:{event.payload.get('workload_profile') or '*'}"
            )
        return entity, str(scope_key)
    if entity == "notification":
        return entity, str(event.payload.get("id") or "") or None
    return None, None


def _canonical_value(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _latest_timestamp_value(left: Any, right: Any) -> Any:
    """Select the latest ISO timestamp with a deterministic malformed fallback."""

    parsed: list[tuple[datetime, str, Any]] = []
    for value in (left, right):
        if isinstance(value, str):
            try:
                stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=UTC)
                parsed.append((stamp.astimezone(UTC), _canonical_value(value), value))
            except ValueError:
                parsed = []
                break
        else:
            parsed = []
            break
    if len(parsed) == 2:
        return max(parsed)[2]
    return max((left, right), key=_canonical_value)


def _notification_conflict_value(
    field: str, left: Any, right: Any, winner: dict[str, Any]
) -> tuple[Any, str]:
    """Merge commutative notification fields without losing durable receipts."""
    if field == "idempotency_keys":
        values = sorted({str(item) for item in [*(left or []), *(right or [])]})
        return values[-128:], "set_union"
    if field in {"version", "coalesced_count"}:
        try:
            return max(int(left or 0), int(right or 0)), "highest_value"
        except TypeError, ValueError:
            pass
    if field.endswith("_at"):
        present = [value for value in (left, right) if value not in {None, ""}]
        if not present:
            return None, "optional_timestamp"
        if field == "created_at":
            return min(present, key=_canonical_value), "earliest_timestamp"
        if len(present) == 1:
            return present[0], "non_null_timestamp"
        return _latest_timestamp_value(*present), "latest_timestamp"
    return winner["value"], "highest_notification_version_then_event_identity"


class EventLog:
    def __init__(self, store: ObjectStore, data_dir: Path, instance_id: str) -> None:
        self.store = store
        self.instance_id = instance_id
        self.refs_path = data_dir / "sync_refs.json"
        self.refs_lock_path = data_dir / "sync_refs.lock"
        self._refs: dict[str, str] = {}
        self._lock = threading.RLock()
        self._load_refs()

    @contextmanager
    def _refs_file_lock(self):
        """Serialize ref read/modify/write cycles across PA processes."""
        self.refs_lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.refs_lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _load_refs(self) -> None:
        self._refs = {}
        if self.refs_path.exists():
            try:
                self._refs = json.loads(self.refs_path.read_text())
            except json.JSONDecodeError:
                self._refs = {}

    def reload_refs(self) -> None:
        with self._lock:
            with self._refs_file_lock():
                self._load_refs()

    def _save_refs(self) -> None:
        atomic_write_json(self.refs_path, self._refs)

    def ref_key(self, realm_id: str) -> str:
        return f"{realm_id}/{self.instance_id}"

    def get_head(self, realm_id: str) -> str | None:
        # Ref files may be advanced by a recovery utility or an older PA process.
        # Always refresh so a long-running server never requires a restart merely
        # to observe the durable head.
        self.reload_refs()
        return self._refs.get(self.ref_key(realm_id))

    def list_refs(self) -> list[SyncRef]:
        self.reload_refs()
        refs: list[SyncRef] = []
        for key, head in self._refs.items():
            if "/" not in key:
                continue
            realm_id, instance_id = key.split("/", 1)
            refs.append(
                SyncRef(realm_id=realm_id, instance_id=instance_id, head_hash=head)
            )
        return refs

    def append_event(
        self,
        event: CardEvent,
        *,
        on_commit: Callable[[SyncCommit], None] | None = None,
    ) -> tuple[CardEvent, SyncCommit]:
        with self._lock:
            with self._refs_file_lock():
                self._load_refs()
                realm_id = event.realm_id
                parent = self._refs.get(self.ref_key(realm_id))
                parent_hashes = [parent] if parent else []
                prior = (
                    self.entity_snapshot(parent, "card", event.card_id)
                    if parent and event.card_id
                    else None
                )
                if event.type == EventType.CARD_CREATED and prior is not None:
                    raise DuplicateCardCreateError(event.card_id or "", parent)
                event = event.model_copy(
                    update={
                        "causal_parent": event.causal_parent or parent,
                        "causal_card_version": event.causal_card_version
                        or (str(prior.get("updated_at")) if prior else None),
                        "field_intent": event.field_intent
                        or sorted(
                            key
                            for key in event.payload
                            if key not in {"id", "realm_id", "updated_at"}
                        ),
                    }
                )
                event_data = event.model_dump(mode="json")
                event_hash = self.store.put_json(event_data)

                commit = SyncCommit(
                    hash="",
                    realm_id=realm_id,
                    instance_id=self.instance_id,
                    parent_hashes=parent_hashes,
                    event_hashes=[event_hash],
                    author_principal=event.author_principal,
                    timestamp=datetime.now(UTC),
                )
                commit.hash = self.store.put_json(commit.model_dump(mode="json"))

                self._refs[self.ref_key(realm_id)] = commit.hash
                self._save_refs()

        if on_commit:
            on_commit(commit)

        return event, commit

    def get_event(self, event_hash: str) -> CardEvent | None:
        data = self.store.get_json(event_hash)
        if not data:
            return None
        return CardEvent.model_validate(data)

    def get_commit(self, commit_hash: str) -> SyncCommit | None:
        data = self.store.get_json(commit_hash)
        if not data:
            return None
        return SyncCommit.model_validate(data)

    def apply_commit_chain(
        self,
        commit_hash: str,
        handler: Callable[[CardEvent], None],
        *,
        seen: set[str] | None = None,
    ) -> None:
        seen = seen or set()
        if commit_hash in seen:
            return
        seen.add(commit_hash)

        commit = self.get_commit(commit_hash)
        if not commit:
            return

        for parent in commit.parent_hashes:
            self.apply_commit_chain(parent, handler, seen=seen)

        for event_hash in commit.event_hashes:
            event = self.get_event(event_hash)
            if event:
                handler(event)

    def merge_heads(
        self,
        realm_id: str,
        head_a: str,
        head_b: str,
        author_principal: str,
        *,
        expected_head: str | None | object = ...,
        automatic_resolutions: list[dict[str, Any]] | None = None,
    ) -> SyncCommit:
        parents = sorted({head_a, head_b})
        normalized_resolutions = sorted(
            (
                {
                    key: resolution[key]
                    for key in (
                        "entity",
                        "id",
                        "field",
                        "value",
                        "strategy",
                        "version",
                    )
                    if key in resolution
                }
                for resolution in (automatic_resolutions or [])
            ),
            key=lambda item: (
                item.get("entity", ""),
                item.get("id", ""),
                item.get("field", ""),
                _canonical_value(item.get("value")),
            ),
        )
        resolution_events: list[CardEvent] = []
        for resolution in normalized_resolutions:
            entity = resolution.get("entity")
            entity_id = resolution.get("id")
            field = resolution.get("field")
            if (
                entity
                not in {
                    "card",
                    "project",
                    "instance_group",
                    "instance_policy",
                    "placement_default",
                    "notification",
                }
                or not entity_id
                or not field
            ):
                continue
            resolution_key = _canonical_value(
                {"parents": parents, **resolution}
            ).encode()
            if entity == "card":
                event_type = EventType.CARD_UPDATED
                payload = {field: resolution.get("value")}
            elif entity == "project":
                event_type = EventType.PROJECT_UPDATED
                payload = {field: resolution.get("value")}
            elif field == "__terminal__" and entity in _FLEET_ENTITY_DELETE_EVENT:
                event_type = _FLEET_ENTITY_DELETE_EVENT[entity]
                payload = (
                    {"id": entity_id}
                    if entity in {"instance_group", "notification"}
                    else {"scope_key": entity_id}
                )
            else:
                event_type = _FLEET_ENTITY_UPDATE_EVENT[entity]
                identity_field = {
                    "instance_group": "id",
                    "instance_policy": "instance_id",
                    "placement_default": "scope_key",
                    "notification": "id",
                }[entity]
                payload = {
                    identity_field: entity_id,
                    field: resolution.get("value"),
                }
                if entity == "notification":
                    payload["version"] = max(1, int(resolution.get("version") or 1))
            resolution_events.append(
                CardEvent(
                    id=f"auto-resolve-{object_hash(resolution_key)}",
                    type=event_type,
                    realm_id=realm_id,
                    card_id=entity_id if entity == "card" else None,
                    project_id=entity_id if entity == "project" else None,
                    author_principal="sync:auto",
                    author_instance="sync-merge",
                    payload=payload,
                    timestamp=_EPOCH,
                )
            )
        merge_id = object_hash("|".join(parents).encode())
        merge_payload: dict[str, Any] = {"merge": True, "parents": parents}
        if normalized_resolutions:
            merge_payload["automatic_resolutions"] = normalized_resolutions
        merge_event = CardEvent(
            id=f"merge-{merge_id}",
            type=EventType.CARD_UPDATED,
            realm_id=realm_id,
            author_principal="sync:auto",
            author_instance="sync-merge",
            payload=merge_payload,
            timestamp=_EPOCH,
        )
        event_hashes = [
            self.store.put_json(event.model_dump(mode="json"))
            for event in [*resolution_events, merge_event]
        ]
        commit = SyncCommit(
            hash="",
            realm_id=realm_id,
            instance_id="sync-merge",
            parent_hashes=parents,
            event_hashes=event_hashes,
            author_principal="sync:auto",
            timestamp=_EPOCH,
        )
        commit.hash = self.store.put_json(commit.model_dump(mode="json"))
        self.advance_ref(realm_id, commit.hash, expected_head=expected_head)
        return commit

    def resolve_heads(
        self,
        realm_id: str,
        local_head: str,
        remote_head: str,
        events: list[CardEvent],
        author_principal: str,
    ) -> SyncCommit:
        """Create a merge commit carrying explicit operator resolutions."""
        parents = sorted({local_head, remote_head})
        resolution_updated_at = datetime.now(UTC).isoformat()
        events = [
            event.model_copy(
                update={
                    "payload": {
                        **event.payload,
                        "updated_at": resolution_updated_at,
                    }
                }
            )
            if event.card_id
            and event.type
            in {EventType.CARD_CREATED, EventType.CARD_UPSERTED, EventType.CARD_UPDATED}
            and "updated_at" not in event.payload
            else event
            for event in events
        ]
        audit_event = CardEvent(
            type=EventType.CARD_UPDATED,
            realm_id=realm_id,
            author_principal=author_principal,
            author_instance=self.instance_id,
            payload={
                "merge": True,
                "resolution": "manual",
                "parents": parents,
                "resolved_events": [
                    {
                        "event_id": event.id,
                        "entity": "card" if event.card_id else "project",
                        "id": event.card_id or event.project_id,
                        "type": event.type.value,
                        "fields": event.payload,
                    }
                    for event in events
                ],
            },
        )
        event_hashes = [
            self.store.put_json(event.model_dump(mode="json"))
            for event in [*events, audit_event]
        ]
        commit = SyncCommit(
            hash="",
            realm_id=realm_id,
            instance_id=self.instance_id,
            parent_hashes=parents,
            event_hashes=event_hashes,
            author_principal=author_principal,
            timestamp=datetime.now(UTC),
        )
        commit.hash = self.store.put_json(commit.model_dump(mode="json"))
        self.advance_ref(realm_id, commit.hash, expected_head=local_head)
        return commit

    def compatible_histories(self, head_a: str, head_b: str) -> tuple[bool, dict]:
        """Detect field-level conflicts in the two branches since their common base."""
        ancestors_a = self._ancestors(head_a)
        ancestors_b = self._ancestors(head_b)
        common = ancestors_a & ancestors_b

        def changes(head: str) -> dict[tuple[str, str], dict[str, dict]]:
            result: dict[tuple[str, str], dict[str, dict]] = {}
            seen: set[str] = set()

            def walk(commit_hash: str) -> None:
                if commit_hash in seen or commit_hash in common:
                    return
                seen.add(commit_hash)
                commit = self.get_commit(commit_hash)
                if not commit:
                    return
                for parent in commit.parent_hashes:
                    walk(parent)
                for event_hash in commit.event_hashes:
                    event = self.get_event(event_hash)
                    if not event or event.payload.get("merge"):
                        continue
                    entity, identity = _event_entity(event)
                    if not identity:
                        continue
                    entity_changes = result.setdefault((entity, identity), {})
                    source = {
                        "instance_id": event.author_instance,
                        "principal": event.author_principal,
                        "event_id": event.id,
                        "event_timestamp": event.timestamp.isoformat(),
                        "version": event.payload.get("version"),
                    }
                    if event.type in {
                        EventType.CARD_DELETED,
                        EventType.PROJECT_ARCHIVED,
                        EventType.INSTANCE_GROUP_DELETED,
                        EventType.PLACEMENT_DEFAULT_DELETED,
                        EventType.NOTIFICATION_DELETED,
                    }:
                        entity_changes["__terminal__"] = {
                            **source,
                            "value": event.type.value,
                        }
                    for field, value in event.payload.items():
                        entity_changes[field] = {**source, "value": value}

            walk(head)
            return result

        left, right = changes(head_a), changes(head_b)
        conflicts = []
        automatic_resolutions = []
        for entity_key in sorted(set(left) & set(right)):
            entity, entity_id = entity_key
            left_fields = left[entity_key]
            right_fields = right[entity_key]
            if "__terminal__" in left_fields or "__terminal__" in right_fields:
                if left_fields != right_fields:
                    if entity in _FLEET_ENTITY_UPDATE_EVENT:
                        terminal = left_fields.get("__terminal__") or right_fields.get(
                            "__terminal__"
                        )
                        automatic_resolutions.append(
                            {
                                "entity": entity,
                                "id": entity_id,
                                "field": "__terminal__",
                                "value": terminal["value"],
                                "strategy": "explicit_delete_wins",
                                "local": left_fields.get("__terminal__")
                                or {"value": "preserve"},
                                "remote": right_fields.get("__terminal__")
                                or {"value": "preserve"},
                            }
                        )
                        continue
                    conflicts.append(
                        {
                            "entity": entity,
                            "id": entity_id,
                            "field": "__terminal__",
                            "local": left_fields.get("__terminal__")
                            or {"value": "preserve"},
                            "remote": right_fields.get("__terminal__")
                            or {"value": "preserve"},
                        }
                    )
                    conflicts[-1]["local"]["snapshot"] = self.entity_snapshot(
                        head_a, entity, entity_id
                    )
                    conflicts[-1]["remote"]["snapshot"] = self.entity_snapshot(
                        head_b, entity, entity_id
                    )
                    continue
            for field in sorted(set(left_fields) & set(right_fields)):
                if left_fields[field]["value"] != right_fields[field]["value"]:
                    if (
                        (entity, field) in _AUTOMATIC_METADATA_FIELDS
                        or entity in _FLEET_ENTITY_UPDATE_EVENT
                    ):
                        if entity in _FLEET_ENTITY_UPDATE_EVENT:
                            choices = [
                                left_fields[field],
                                right_fields[field],
                            ]
                            winner = max(
                                choices,
                                key=lambda item: (
                                    int(item.get("version") or 0),
                                    str(item.get("event_timestamp") or ""),
                                    str(item.get("event_id") or ""),
                                    _canonical_value(item.get("value")),
                                ),
                            )
                            if entity == "notification":
                                value, strategy = _notification_conflict_value(
                                    field,
                                    left_fields[field]["value"],
                                    right_fields[field]["value"],
                                    winner,
                                )
                            else:
                                value = winner["value"]
                                strategy = "highest_policy_version_then_event_identity"
                        else:
                            value = _latest_timestamp_value(
                                left_fields[field]["value"],
                                right_fields[field]["value"],
                            )
                            strategy = "latest_timestamp"
                        automatic_resolutions.append(
                            {
                                "entity": entity,
                                "id": entity_id,
                                "field": field,
                                "value": value,
                                "strategy": strategy,
                                **(
                                    {"version": int(winner.get("version") or 1)}
                                    if entity == "notification"
                                    else {}
                                ),
                                "local": left_fields[field],
                                "remote": right_fields[field],
                            }
                        )
                        continue
                    conflicts.append(
                        {
                            "entity": entity,
                            "id": entity_id,
                            "field": field,
                            "local": left_fields[field],
                            "remote": right_fields[field],
                        }
                    )
        return not conflicts, {
            "conflicts": conflicts,
            "automatic_resolutions": automatic_resolutions,
            "common_ancestors": sorted(common),
        }

    def entity_snapshot(self, head: str, entity: str, entity_id: str) -> dict | None:
        """Materialize one entity at an arbitrary immutable history head."""
        state: dict | None = None
        card_seen_since_delete = False

        def apply(event: CardEvent) -> None:
            nonlocal card_seen_since_delete, state
            event_entity, event_id = _event_entity(event)
            matches = event_entity == entity and event_id == entity_id
            if not matches:
                return
            if event.type == EventType.GOAL_GOVERNANCE_UPSERTED:
                state = dict(event.payload.get("entity") or {})
            elif event.type == EventType.CARD_CREATED:
                if card_seen_since_delete:
                    return
                state = dict(event.payload)
                card_seen_since_delete = True
                return
            elif event.type in {
                EventType.CARD_UPSERTED,
                EventType.PROJECT_CREATED,
                EventType.INSTANCE_GROUP_CREATED,
            }:
                state = dict(event.payload)
                if entity == "card":
                    card_seen_since_delete = True
            elif event.type in {
                EventType.CARD_UPDATED,
                EventType.PROJECT_UPDATED,
                EventType.LEASE_GRANTED,
                EventType.LEASE_RELEASED,
                EventType.INSTANCE_GROUP_UPDATED,
                EventType.INSTANCE_GROUP_ARCHIVED,
                EventType.INSTANCE_PARTICIPATION_POLICY_UPDATED,
                EventType.PLACEMENT_DEFAULT_UPDATED,
                EventType.NOTIFICATION_UPSERTED,
            }:
                if entity == "card":
                    card_seen_since_delete = True
                if state is None:
                    state = {}
                state.update(event.payload)
            elif event.type in {
                EventType.CARD_DELETED,
                EventType.INSTANCE_GROUP_DELETED,
                EventType.PLACEMENT_DEFAULT_DELETED,
                EventType.NOTIFICATION_DELETED,
            }:
                state = None
                if entity == "card":
                    card_seen_since_delete = False
            elif event.type == EventType.PROJECT_ARCHIVED and state is not None:
                state["status"] = "archived"

        self.apply_commit_chain(head, apply)
        return state

    def entity_history(
        self, realm_id: str, entity: str, entity_id: str
    ) -> list[dict[str, Any]]:
        """Return immutable entity events with commit and causal provenance."""
        head = self.get_head(realm_id)
        if not head:
            return []
        records: list[dict[str, Any]] = []
        seen_commits: set[str] = set()
        entity_present = False
        entity_seen_since_delete = False

        def walk(commit_hash: str) -> None:
            nonlocal entity_present, entity_seen_since_delete
            if commit_hash in seen_commits:
                return
            commit = self.get_commit(commit_hash)
            if not commit:
                return
            for parent in commit.parent_hashes:
                walk(parent)
            seen_commits.add(commit_hash)
            for event_hash in commit.event_hashes:
                event = self.get_event(event_hash)
                if not event or _event_entity(event) != (entity, entity_id):
                    continue
                duplicate_create = event.type == EventType.CARD_CREATED and (
                    entity_present or entity_seen_since_delete
                )
                if event.type in {EventType.CARD_CREATED, EventType.CARD_UPSERTED}:
                    entity_present = True
                    entity_seen_since_delete = True
                elif event.type == EventType.CARD_DELETED:
                    entity_present = False
                    entity_seen_since_delete = False
                else:
                    entity_seen_since_delete = True
                records.append(
                    {
                        "event_hash": event_hash,
                        "event": event.model_dump(mode="json"),
                        "commit_hash": commit_hash,
                        "parent_hashes": list(commit.parent_hashes),
                        "commit_instance": commit.instance_id,
                        "commit_principal": commit.author_principal,
                        "commit_timestamp": commit.timestamp.isoformat(),
                        "projection_effect": (
                            "ignored_duplicate_create"
                            if duplicate_create
                            else "applied"
                        ),
                    }
                )

        walk(head)
        return records

    def merge_audit(self, realm_id: str, *, limit: int = 50) -> list[dict]:
        """Return merge decisions embedded in the immutable realm history."""
        head = self.get_head(realm_id)
        if not head:
            return []
        records: list[dict] = []
        seen: set[str] = set()
        stack = [head]
        while stack and len(records) < limit:
            commit_hash = stack.pop()
            if commit_hash in seen:
                continue
            seen.add(commit_hash)
            commit = self.get_commit(commit_hash)
            if not commit:
                continue
            for event_hash in commit.event_hashes:
                event = self.get_event(event_hash)
                if not event or not event.payload.get("merge"):
                    continue
                records.append(
                    {
                        "head": commit.hash,
                        "parents": commit.parent_hashes,
                        "mode": event.payload.get("resolution", "automatic"),
                        "author_principal": commit.author_principal,
                        "author_instance": event.author_instance,
                        "timestamp": commit.timestamp.isoformat(),
                        "resolved_events": event.payload.get("resolved_events", []),
                        "automatic_resolutions": event.payload.get(
                            "automatic_resolutions", []
                        ),
                    }
                )
            stack.extend(reversed(commit.parent_hashes))
        records.sort(key=lambda item: item["timestamp"], reverse=True)
        return records[:limit]

    def _ancestors(self, head: str) -> set[str]:
        result: set[str] = set()
        stack = [head]
        while stack:
            current = stack.pop()
            if current in result:
                continue
            result.add(current)
            commit = self.get_commit(current)
            if commit:
                stack.extend(commit.parent_hashes)
        return result

    def advance_ref(
        self,
        realm_id: str,
        commit_hash: str,
        *,
        expected_head: str | None | object = ...,
    ) -> None:
        """Advance a ref with an optional compare-and-swap precondition."""
        with self._lock:
            with self._refs_file_lock():
                self._load_refs()
                key = self.ref_key(realm_id)
                current = self._refs.get(key)
                if expected_head is not ... and current != expected_head:
                    raise StaleSyncHeadError(realm_id, expected_head, current)
                self._refs[key] = commit_hash
                self._save_refs()

    def is_ancestor(self, ancestor: str, descendant: str) -> bool:
        """Return True if ancestor is on the parent chain of descendant."""
        if ancestor == descendant:
            return True
        seen: set[str] = set()
        stack = [descendant]
        while stack:
            commit_hash = stack.pop()
            if commit_hash in seen:
                continue
            seen.add(commit_hash)
            commit = self.get_commit(commit_hash)
            if not commit:
                continue
            for parent in commit.parent_hashes:
                if parent == ancestor:
                    return True
                stack.append(parent)
        return False

    @staticmethod
    def compute_hash(data: dict) -> str:
        return object_hash(json.dumps(data, default=str, sort_keys=True).encode())


class StaleSyncHeadError(RuntimeError):
    def __init__(self, realm_id: str, expected: object, actual: str | None) -> None:
        super().__init__(
            f"sync head changed for realm {realm_id}: expected {expected!r}, "
            f"found {actual!r}"
        )
        self.realm_id = realm_id
        self.expected = expected
        self.actual = actual


class DuplicateCardCreateError(RuntimeError):
    def __init__(self, card_id: str, parent: str) -> None:
        super().__init__(
            f"card {card_id} already exists at causal parent {parent}; "
            "use an explicit audited upsert/repair operation"
        )
        self.card_id = card_id
        self.parent = parent
