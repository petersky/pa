"""Append-only event log with git-style commits."""

from __future__ import annotations

import base64
import binascii
import fcntl
import hashlib
import hmac
import json
import os
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

from pa.core.io import atomic_write_json
from pa.domain.models import CardEvent, EventType, SyncCommit, SyncRef
from pa.sync.object_store import ObjectStore, object_hash

_AUTOMATIC_METADATA_FIELDS = {("card", "updated_at")}
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MAX_HISTORY_COMMITS = 100_000
HISTORY_PAGE_LIMIT = 500
_SUPPORTED_OBJECT_SCHEMA_VERSION = 1
_ObjectModel = TypeVar("_ObjectModel", bound=BaseModel)
_FLEET_EVENT_ENTITY = {
    EventType.INTAKE_ENVELOPE_UPSERTED: "intake",
    EventType.CHANNEL_IDENTITY_UPSERTED: "channel_identity",
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
    "intake": EventType.INTAKE_ENVELOPE_UPSERTED,
    "channel_identity": EventType.CHANNEL_IDENTITY_UPSERTED,
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
    if event.type == EventType.INTAKE_ENVELOPE_UPSERTED:
        return "intake", str(event.payload.get("id") or "") or None
    if event.type == EventType.CHANNEL_IDENTITY_UPSERTED:
        return "channel_identity", str(event.payload.get("id") or "") or None
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
    if entity in {"intake", "channel_identity"}:
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


def _record_union(left: Any, right: Any) -> list[Any]:
    records: dict[str, Any] = {}
    for item in [*(left or []), *(right or [])]:
        if not isinstance(item, dict):
            continue
        key = str(item.get("id") or _canonical_value(item))
        current = records.get(key)
        if current is None or _canonical_value(item) > _canonical_value(current):
            records[key] = item
    return [records[key] for key in sorted(records)]


def _intake_conflict_value(field: str, left: Any, right: Any) -> tuple[Any, str] | None:
    if field == "receipts":
        return _record_union(left, right), "receipt_id_union"
    if field in {"goal_ids", "modalities"}:
        return sorted(
            {str(item) for item in [*(left or []), *(right or [])]}
        ), "set_union"
    if field == "version":
        return max(int(left or 0), int(right or 0)), "highest_value"
    return None


def _identity_conflict_value(
    field: str, left: Any, right: Any
) -> tuple[Any, str] | None:
    if field == "conversation_ids":
        return sorted(
            {str(item) for item in [*(left or []), *(right or [])]}
        ), "set_union"
    if field == "version":
        return max(int(left or 0), int(right or 0)), "highest_value"
    if field == "revoked_at":
        present = [value for value in (left, right) if value not in {None, ""}]
        return (max(present) if present else None), "revocation_wins"
    return None


class EventLog:
    def __init__(
        self,
        store: ObjectStore,
        data_dir: Path,
        instance_id: str,
        *,
        cursor_secret: str | bytes | None = None,
    ) -> None:
        self.store = store
        self.instance_id = instance_id
        secret = (
            cursor_secret.encode()
            if isinstance(cursor_secret, str)
            else cursor_secret or os.urandom(32)
        )
        self._history_cursor_secret = hmac.new(
            secret, b"pa:event-history-cursor:v1", hashlib.sha256
        ).digest()
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

    def _get_object(
        self,
        object_hash_value: str,
        model: type[_ObjectModel],
        object_kind: str,
    ) -> _ObjectModel | None:
        raw = self.store.get(object_hash_value)
        if raw is None:
            return None
        if object_hash(raw) != object_hash_value:
            raise EventHistoryObjectError(
                "corrupt_object", object_hash_value, object_kind
            )
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EventHistoryObjectError(
                "corrupt_object", object_hash_value, object_kind
            ) from exc
        if not isinstance(data, dict):
            raise EventHistoryObjectError(
                "corrupt_object", object_hash_value, object_kind
            )
        version = data.get("schema_version", 1)
        if (
            not isinstance(version, int)
            or isinstance(version, bool)
            or version != _SUPPORTED_OBJECT_SCHEMA_VERSION
        ):
            raise EventHistoryObjectError(
                "unsupported_object_version",
                object_hash_value,
                object_kind,
                schema_version=version,
            )
        try:
            return model.model_validate(data)
        except ValidationError as exc:
            raise EventHistoryObjectError(
                "corrupt_object", object_hash_value, object_kind
            ) from exc

    def get_event(self, event_hash: str) -> CardEvent | None:
        return self._get_object(event_hash, CardEvent, "event")

    def get_commit(self, commit_hash: str) -> SyncCommit | None:
        return self._get_object(commit_hash, SyncCommit, "commit")

    def _iter_commits_parent_first(
        self,
        commit_hash: str,
        *,
        seen: set[str] | None = None,
        stop: set[str] | None = None,
        max_commits: int = MAX_HISTORY_COMMITS,
    ) -> Iterator[tuple[str, SyncCommit]]:
        """Walk a commit DAG parent-first with deterministic bounded state."""
        visited = seen if seen is not None else set()
        excluded = stop if stop is not None else set()
        active: set[str] = set()
        max_commits = max(1, min(int(max_commits), MAX_HISTORY_COMMITS))
        stack: list[tuple[str, SyncCommit | None]] = [(commit_hash, None)]

        while stack:
            current_hash, expanded_commit = stack.pop()
            if expanded_commit is not None:
                active.remove(current_hash)
                yield current_hash, expanded_commit
                continue

            if current_hash in active:
                raise EventHistoryCycleError(current_hash)
            if current_hash in excluded or current_hash in visited:
                continue

            active.add(current_hash)
            visited.add(current_hash)
            if len(visited) > max_commits:
                raise EventHistoryLimitError(len(visited), max_commits)
            commit = self.get_commit(current_hash)
            if not commit:
                raise EventHistoryObjectError(
                    "missing_parent", current_hash, "commit"
                )
            if any(not parent for parent in commit.parent_hashes):
                raise EventHistoryObjectError(
                    "corrupt_object", current_hash, "commit"
                )
            if len(stack) + len(commit.parent_hashes) + 1 > max_commits:
                raise EventHistoryLimitError(len(visited), max_commits)

            stack.append((current_hash, commit))
            stack.extend((parent, None) for parent in reversed(commit.parent_hashes))

    def apply_commit_chain(
        self,
        commit_hash: str,
        handler: Callable[[CardEvent], None],
        *,
        provenance_handler: Callable[[str, str, CardEvent], None] | None = None,
        seen: set[str] | None = None,
        max_commits: int = MAX_HISTORY_COMMITS,
    ) -> None:
        for current_hash, commit in self._iter_commits_parent_first(
            commit_hash, seen=seen, max_commits=max_commits
        ):
            for event_hash in commit.event_hashes:
                event = self.get_event(event_hash)
                if not event:
                    raise EventHistoryObjectError(
                        "missing_event", event_hash, "event"
                    )
                if provenance_handler is not None:
                    provenance_handler(current_hash, event_hash, event)
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
                    "intake",
                    "channel_identity",
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
                    "intake": "id",
                    "channel_identity": "id",
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
        *,
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
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
            source_operation="sync.resolve_conflicts",
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            operation_result={
                "realm_id": realm_id,
                "resolved": len(events),
                "durable": True,
            },
            operation_result_complete=False,
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

            for _, commit in self._iter_commits_parent_first(head, stop=common):
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
                    if entity == "channel_identity" and field == "principal_id":
                        conflicts.append(
                            {
                                "entity": entity,
                                "id": entity_id,
                                "field": field,
                                "local": left_fields[field],
                                "remote": right_fields[field],
                            }
                        )
                        continue
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
                            elif entity == "intake" and (
                                merged := _intake_conflict_value(
                                    field,
                                    left_fields[field]["value"],
                                    right_fields[field]["value"],
                                )
                            ):
                                value, strategy = merged
                            elif entity == "channel_identity" and (
                                merged := _identity_conflict_value(
                                    field,
                                    left_fields[field]["value"],
                                    right_fields[field]["value"],
                                )
                            ):
                                value, strategy = merged
                            else:
                                value = winner["value"]
                                strategy = (
                                    "highest_entity_version_then_event_identity"
                                    if entity in {"intake", "channel_identity"}
                                    else "highest_policy_version_then_event_identity"
                                )
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

    def find_operation_event(
        self,
        realm_id: str,
        idempotency_key: str,
        *,
        max_commits: int = MAX_HISTORY_COMMITS,
    ) -> tuple[str, str, CardEvent] | None:
        """Find the single durable event attributable to an operation key."""
        head = self.get_head(realm_id)
        if not head:
            return None
        found: tuple[str, str, CardEvent] | None = None
        for commit_hash, commit in self._iter_commits_parent_first(
            head, max_commits=max_commits
        ):
            for event_hash in commit.event_hashes:
                event = self.get_event(event_hash)
                if not event:
                    raise EventHistoryObjectError(
                        "missing_event", event_hash, "event"
                    )
                if event.idempotency_key != idempotency_key:
                    continue
                if found and found[2].id != event.id:
                    raise EventHistoryError(
                        "duplicate_idempotency_key",
                        "multiple durable events claim the same idempotency key",
                        idempotency_key=idempotency_key,
                        first_event_id=found[2].id,
                        second_event_id=event.id,
                    )
                found = (commit_hash, event_hash, event)
        return found

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
                EventType.INTAKE_ENVELOPE_UPSERTED,
                EventType.CHANNEL_IDENTITY_UPSERTED,
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

    @staticmethod
    def _history_query_digest(realm_id: str, entity: str, entity_id: str) -> str:
        canonical = json.dumps(
            {"realm_id": realm_id, "entity": entity, "entity_id": entity_id},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(canonical).hexdigest()

    def _history_cursor(
        self, realm_id: str, head: str, entity: str, entity_id: str, offset: int
    ) -> str:
        payload = {
            "version": 3,
            "realm_id": realm_id,
            "query_digest": self._history_query_digest(
                realm_id, entity, entity_id
            ),
            "head": head,
            "entity": entity,
            "entity_id": entity_id,
            "offset": offset,
        }
        canonical = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        payload["signature"] = hmac.new(
            self._history_cursor_secret, canonical, hashlib.sha256
        ).hexdigest()
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":")
        ).encode()
        return base64.urlsafe_b64encode(encoded).decode().rstrip("=")

    def _decode_history_cursor(
        self, cursor: str, realm_id: str, entity: str, entity_id: str
    ) -> tuple[str, int]:
        try:
            padded = cursor + "=" * (-len(cursor) % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded).decode())
            signature = payload.pop("signature", None)
            canonical = json.dumps(
                payload, sort_keys=True, separators=(",", ":")
            ).encode()
            expected_signature = hmac.new(
                self._history_cursor_secret, canonical, hashlib.sha256
            ).hexdigest()
            if (
                not isinstance(payload, dict)
                or not isinstance(signature, str)
                or not hmac.compare_digest(signature, expected_signature)
                or payload.get("version") != 3
                or payload.get("realm_id") != realm_id
                or payload.get("entity") != entity
                or payload.get("entity_id") != entity_id
                or payload.get("query_digest")
                != self._history_query_digest(realm_id, entity, entity_id)
                or not isinstance(payload.get("head"), str)
                or not isinstance(payload.get("offset"), int)
                or payload["offset"] < 0
            ):
                raise ValueError
            return payload["head"], payload["offset"]
        except (
            binascii.Error,
            ValueError,
            TypeError,
            UnicodeDecodeError,
            json.JSONDecodeError,
        ) as exc:
            raise EventHistoryCursorError() from exc

    def entity_history_page(
        self,
        realm_id: str,
        entity: str,
        entity_id: str,
        *,
        limit: int = 100,
        cursor: str | None = None,
        max_commits: int = MAX_HISTORY_COMMITS,
    ) -> dict[str, Any]:
        """Return one stable page from an immutable head snapshot."""
        offset = 0
        if cursor:
            # Validate cursor scope before touching the requested realm's log.
            head, offset = self._decode_history_cursor(
                cursor, realm_id, entity, entity_id
            )
        else:
            head = self.get_head(realm_id)
        limit = max(1, min(int(limit), MAX_HISTORY_COMMITS))
        if not head:
            return {
                "head": None,
                "events": [],
                "next_cursor": None,
                "has_more": False,
                "scanned_commits": 0,
            }

        records: list[dict[str, Any]] = []
        seen_commits: set[str] = set()
        entity_present = False
        entity_seen_since_delete = False
        matching_index = 0
        has_more = False

        try:
            commits = self._iter_commits_parent_first(
                head, seen=seen_commits, max_commits=max_commits
            )
            for commit_hash, commit in commits:
                for event_hash in commit.event_hashes:
                    event = self.get_event(event_hash)
                    if not event:
                        raise EventHistoryObjectError(
                            "missing_event", event_hash, "event"
                        )
                    if _event_entity(event) != (entity, entity_id):
                        continue
                    duplicate_create = event.type == EventType.CARD_CREATED and (
                        entity_present or entity_seen_since_delete
                    )
                    if event.type in {
                        EventType.CARD_CREATED,
                        EventType.CARD_UPSERTED,
                    }:
                        entity_present = True
                        entity_seen_since_delete = True
                    elif event.type == EventType.CARD_DELETED:
                        entity_present = False
                        entity_seen_since_delete = False
                    else:
                        entity_seen_since_delete = True

                    if matching_index >= offset:
                        if len(records) >= limit:
                            has_more = True
                            break
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
                    matching_index += 1
                if has_more:
                    break
        except EventHistoryLimitError as exc:
            exc.partial_records = records
            exc.next_cursor = self._history_cursor(
                realm_id, head, entity, entity_id, offset + len(records)
            )
            raise

        next_cursor = (
            self._history_cursor(
                realm_id, head, entity, entity_id, offset + len(records)
            )
            if has_more
            else None
        )
        return {
            "head": head,
            "events": records,
            "next_cursor": next_cursor,
            "has_more": has_more,
            "scanned_commits": len(seen_commits),
        }

    def entity_history(
        self, realm_id: str, entity: str, entity_id: str
    ) -> list[dict[str, Any]]:
        """Compatibility helper returning bounded complete entity history."""
        page = self.entity_history_page(
            realm_id,
            entity,
            entity_id,
            limit=MAX_HISTORY_COMMITS,
            max_commits=MAX_HISTORY_COMMITS,
        )
        if page["has_more"]:
            raise EventHistoryLimitError(
                page["scanned_commits"], MAX_HISTORY_COMMITS
            )
        return page["events"]

    def merge_audit(self, realm_id: str, *, limit: int = 50) -> list[dict]:
        """Return merge decisions embedded in the immutable realm history."""
        head = self.get_head(realm_id)
        if not head:
            return []
        records: list[dict] = []
        for commit_hash, commit in self._iter_commits_parent_first(head):
            for event_hash in commit.event_hashes:
                event = self.get_event(event_hash)
                if not event:
                    raise EventHistoryObjectError(
                        "missing_event", event_hash, "event"
                    )
                if not event.payload.get("merge"):
                    continue
                records.append(
                    {
                        "head": commit_hash,
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
        records.sort(
            key=lambda item: (item["timestamp"], item["head"]), reverse=True
        )
        return records[: max(1, min(int(limit), HISTORY_PAGE_LIMIT))]

    def _ancestors(self, head: str) -> set[str]:
        return {
            commit_hash for commit_hash, _ in self._iter_commits_parent_first(head)
        }

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
        """Return True if ancestor is on the bounded parent DAG of descendant."""
        if ancestor == descendant:
            return True
        return any(
            commit_hash == ancestor
            for commit_hash, _ in self._iter_commits_parent_first(descendant)
        )

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


class EventHistoryError(RuntimeError):
    """Bounded machine-readable failure for immutable history traversal."""

    def __init__(self, code: str, message: str, **diagnostic: Any) -> None:
        super().__init__(message)
        self.code = code
        self.diagnostic = diagnostic

    def as_detail(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": str(self),
            "recoverable": False,
            "recovery_state": "operator_action_required",
            **self.diagnostic,
        }


class EventHistoryObjectError(EventHistoryError):
    def __init__(
        self,
        code: str,
        object_hash_value: str,
        object_kind: str,
        *,
        schema_version: object | None = None,
    ) -> None:
        message = {
            "missing_parent": "event history references a missing parent commit",
            "missing_event": "event history references a missing event object",
            "corrupt_object": (
                "event history contains a corrupt content-addressed object"
            ),
            "unsupported_object_version": (
                "event history contains an unsupported object schema version"
            ),
        }.get(code, "event history object validation failed")
        diagnostic: dict[str, Any] = {
            "object_hash": object_hash_value,
            "object_kind": object_kind,
        }
        if schema_version is not None:
            diagnostic["schema_version"] = schema_version
            diagnostic["supported_schema_version"] = (
                _SUPPORTED_OBJECT_SCHEMA_VERSION
            )
        super().__init__(code, message, **diagnostic)


class EventHistoryLimitError(EventHistoryError):
    def __init__(self, visited: int, limit: int) -> None:
        super().__init__(
            "history_limit_exceeded",
            f"event history traversal exceeded the bounded limit of {limit} commits",
            visited=visited,
            limit=limit,
        )
        self.visited = visited
        self.limit = limit
        self.partial_records: list[dict[str, Any]] = []
        self.next_cursor: str | None = None

    def as_detail(self) -> dict[str, Any]:
        return {
            **super().as_detail(),
            "partial": bool(self.partial_records),
            "events": self.partial_records,
            "next_cursor": self.next_cursor,
        }


class EventHistoryCursorError(EventHistoryError):
    def __init__(self) -> None:
        super().__init__(
            "invalid_history_cursor",
            "history cursor is invalid or belongs to another entity",
        )


class EventHistoryCycleError(EventHistoryError):
    def __init__(self, commit_hash: str) -> None:
        super().__init__(
            "history_cycle",
            f"event history contains a parent cycle at commit {commit_hash}",
            commit_hash=commit_hash,
        )
        self.commit_hash = commit_hash
