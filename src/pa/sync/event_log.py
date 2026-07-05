"""Append-only event log with git-style commits."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

from pa.domain.models import CardEvent, EventType, SyncCommit, SyncRef
from pa.sync.object_store import ObjectStore, object_hash


class EventLog:
    def __init__(self, store: ObjectStore, data_dir: Path, instance_id: str) -> None:
        self.store = store
        self.instance_id = instance_id
        self.refs_path = data_dir / "sync_refs.json"
        self._refs: dict[str, str] = {}
        self._load_refs()

    def _load_refs(self) -> None:
        if self.refs_path.exists():
            try:
                self._refs = json.loads(self.refs_path.read_text())
            except json.JSONDecodeError:
                self._refs = {}

    def _save_refs(self) -> None:
        self.refs_path.write_text(json.dumps(self._refs, indent=2) + "\n")

    def ref_key(self, realm_id: str) -> str:
        return f"{realm_id}/{self.instance_id}"

    def get_head(self, realm_id: str) -> str | None:
        return self._refs.get(self.ref_key(realm_id))

    def list_refs(self) -> list[SyncRef]:
        refs: list[SyncRef] = []
        for key, head in self._refs.items():
            if "/" not in key:
                continue
            realm_id, instance_id = key.split("/", 1)
            refs.append(SyncRef(realm_id=realm_id, instance_id=instance_id, head_hash=head))
        return refs

    def append_event(
        self,
        event: CardEvent,
        *,
        on_commit: Callable[[SyncCommit], None] | None = None,
    ) -> tuple[CardEvent, SyncCommit]:
        event_data = event.model_dump(mode="json")
        event_hash = self.store.put_json(event_data)

        realm_id = event.realm_id
        parent = self.get_head(realm_id)
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
    ) -> SyncCommit:
        merge_event = CardEvent(
            type=EventType.CARD_UPDATED,
            realm_id=realm_id,
            author_principal=author_principal,
            author_instance=self.instance_id,
            payload={"merge": True, "parents": [head_a, head_b]},
        )
        event_hash = self.store.put_json(merge_event.model_dump(mode="json"))
        commit = SyncCommit(
            hash="",
            realm_id=realm_id,
            instance_id=self.instance_id,
            parent_hashes=sorted({head_a, head_b}),
            event_hashes=[event_hash],
            author_principal=author_principal,
            timestamp=datetime.now(UTC),
        )
        commit.hash = self.store.put_json(commit.model_dump(mode="json"))
        self._refs[self.ref_key(realm_id)] = commit.hash
        self._save_refs()
        return commit

    def advance_ref(self, realm_id: str, commit_hash: str) -> None:
        self._refs[self.ref_key(realm_id)] = commit_hash
        self._save_refs()

    @staticmethod
    def compute_hash(data: dict) -> str:
        return object_hash(json.dumps(data, default=str, sort_keys=True).encode())
