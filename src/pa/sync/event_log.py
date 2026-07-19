"""Append-only event log with git-style commits."""

from __future__ import annotations

import json
import os
import threading
from contextlib import contextmanager
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

import fcntl

from pa.core.io import atomic_write_json
from pa.domain.models import CardEvent, EventType, SyncCommit, SyncRef
from pa.sync.object_store import ObjectStore, object_hash


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
                event_data = event.model_dump(mode="json")
                event_hash = self.store.put_json(event_data)

                realm_id = event.realm_id
                parent = self._refs.get(self.ref_key(realm_id))
                parent_hashes = [parent] if parent else []

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
    ) -> SyncCommit:
        parents = sorted({head_a, head_b})
        merge_id = object_hash("|".join(parents).encode())
        merge_event = CardEvent(
            id=f"merge-{merge_id}",
            type=EventType.CARD_UPDATED,
            realm_id=realm_id,
            author_principal="sync:auto",
            author_instance="sync-merge",
            payload={"merge": True, "parents": parents},
            timestamp=datetime(1970, 1, 1, tzinfo=UTC),
        )
        event_hash = self.store.put_json(merge_event.model_dump(mode="json"))
        commit = SyncCommit(
            hash="",
            realm_id=realm_id,
            instance_id="sync-merge",
            parent_hashes=parents,
            event_hashes=[event_hash],
            author_principal="sync:auto",
            timestamp=datetime(1970, 1, 1, tzinfo=UTC),
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
        event_hashes = [
            self.store.put_json(event.model_dump(mode="json")) for event in events
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

        def changes(head: str) -> dict[tuple[str, str], dict[str, object]]:
            result: dict[tuple[str, str], dict[str, object]] = defaultdict(dict)
            seen: set[str] = set()
            stack = [head]
            while stack:
                commit_hash = stack.pop()
                if commit_hash in seen or commit_hash in common:
                    continue
                seen.add(commit_hash)
                commit = self.get_commit(commit_hash)
                if not commit:
                    continue
                stack.extend(commit.parent_hashes)
                for event_hash in commit.event_hashes:
                    event = self.get_event(event_hash)
                    if not event or event.payload.get("merge"):
                        continue
                    identity = event.card_id or event.project_id
                    if not identity:
                        continue
                    entity = "card" if event.card_id else "project"
                    if event.type in {
                        EventType.CARD_DELETED,
                        EventType.PROJECT_ARCHIVED,
                    }:
                        result[(entity, identity)].setdefault(
                            "__terminal__", event.type.value
                        )
                    for field, value in event.payload.items():
                        result[(entity, identity)].setdefault(field, value)
            return result

        left, right = changes(head_a), changes(head_b)
        conflicts = []
        for entity in sorted(set(left) & set(right)):
            if "__terminal__" in left[entity] or "__terminal__" in right[entity]:
                if left[entity] != right[entity]:
                    conflicts.append({"entity": entity, "field": "__terminal__"})
                    continue
            for field in sorted(set(left[entity]) & set(right[entity])):
                if left[entity][field] != right[entity][field]:
                    conflicts.append({"entity": entity, "field": field})
        return not conflicts, {
            "conflicts": conflicts,
            "common_ancestors": sorted(common),
        }

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
