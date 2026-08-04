from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from pa.domain.models import CardEvent, EventType
from pa.limbic.models import (
    SENSITIVITY_RANK,
    MemoryMutationContext,
    MemoryQuery,
    MemoryRecord,
    MemoryTier,
    RetrievedMemory,
    WorkingMemoryPacket,
)
from pa.limbic.projection import (
    find_memory_event,
    get_memory_payload,
    list_memory_payloads,
)


class MemoryConflict(ValueError):
    pass


class MemoryService:
    """Append-only tiered memory with scoped, poison-resistant retrieval."""

    def __init__(self, store, instance_id: str) -> None:
        self.store = store
        self.instance_id = instance_id

    def get(self, record_id: str) -> MemoryRecord | None:
        payload = get_memory_payload(self.store, record_id)
        return MemoryRecord.model_validate(payload) if payload else None

    def remember(
        self, record: MemoryRecord, context: MemoryMutationContext
    ) -> MemoryRecord:
        duplicate = find_memory_event(
            self.store, record.realm_id, context.idempotency_key
        )
        if duplicate:
            existing = self.get(duplicate["record_id"])
            if existing:
                return existing
        if self.get(record.id):
            raise MemoryConflict("memory id already exists; use a new immutable record")
        now = datetime.now(UTC)
        changed: list[MemoryRecord] = []
        if record.supersedes:
            prior = self.get(record.supersedes)
            if not prior or prior.realm_id != record.realm_id:
                raise MemoryConflict("superseded memory must exist in the same realm")
            if (
                prior.tier != record.tier
                or prior.subject != record.subject
                or prior.predicate != record.predicate
                or prior.goal_id != record.goal_id
            ):
                raise MemoryConflict(
                    "supersession must preserve tier, subject, predicate, and goal scope"
                )
            if prior.superseded_by and prior.superseded_by != record.id:
                raise MemoryConflict("memory was already superseded by another record")
            prior.superseded_by = record.id
            prior.version += 1
            prior.updated_at = now
            changed.append(prior)
        elif record.tier in {MemoryTier.SEMANTIC, MemoryTier.PROCEDURAL}:
            for prior in self._fact_candidates(record):
                if self._value_key(prior.value) == self._value_key(record.value):
                    continue
                prior.contradiction = True
                prior.contradiction_ids = sorted(
                    set(prior.contradiction_ids) | {record.id}
                )
                prior.version += 1
                prior.updated_at = now
                record.contradiction = True
                record.contradiction_ids = sorted(
                    set(record.contradiction_ids) | {prior.id}
                )
                changed.append(prior)
        record.updated_at = now
        changed.append(record)
        self.store.commit_event(
            CardEvent(
                type=EventType.MEMORY_RECORDED,
                realm_id=record.realm_id,
                author_principal=context.actor_principal,
                author_instance=context.authority_instance_id or self.instance_id,
                payload={
                    "records": [item.model_dump(mode="json") for item in changed],
                    "memory_event": {
                        "event_type": "memory.recorded",
                        "actor_principal": context.actor_principal,
                        "authority_instance_id": context.authority_instance_id,
                        "idempotency_key": context.idempotency_key,
                        "payload": {
                            "tier": record.tier.value,
                            "supersedes": record.supersedes,
                            "contradiction_ids": record.contradiction_ids,
                        },
                    },
                },
            )
        )
        return record

    @staticmethod
    def _value_key(value: Any) -> str:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)

    def _fact_candidates(self, record: MemoryRecord) -> list[MemoryRecord]:
        return [
            item
            for payload in list_memory_payloads(self.store, record.realm_id)
            if (item := MemoryRecord.model_validate(payload)).tier == record.tier
            and item.subject == record.subject
            and item.predicate == record.predicate
            and item.goal_id == record.goal_id
            and item.active()
        ]

    def retrieve(self, query: MemoryQuery) -> list[RetrievedMemory]:
        now = datetime.now(UTC)
        terms = {term for term in query.query.lower().split() if term}
        result: list[RetrievedMemory] = []
        for payload in list_memory_payloads(self.store, query.realm_id):
            record = MemoryRecord.model_validate(payload)
            if record.tier not in query.tiers:
                continue
            if record.goal_id and record.goal_id not in query.goal_ids:
                continue
            if record.allowed_principals and query.requester_principal not in {
                record.owner_principal,
                *record.allowed_principals,
            }:
                continue
            if (
                SENSITIVITY_RANK[record.sensitivity]
                > SENSITIVITY_RANK[query.max_sensitivity]
            ):
                continue
            if not query.include_expired and record.expires_at and record.expires_at <= now:
                continue
            if not query.include_superseded and record.superseded_by:
                continue
            if not query.include_contradictions and record.contradiction:
                continue
            searchable = (
                f"{record.subject} {record.predicate} {record.summary} {record.value}"
            ).lower()
            hits = sum(term in searchable for term in terms)
            if terms and not hits:
                continue
            relevance = 1.0 if not terms else min(1.0, hits / len(terms))
            reason = (
                "realm, authority, goal, sensitivity, retention, and state scopes "
                "passed"
            )
            result.append(
                RetrievedMemory(
                    record=record,
                    relevance=relevance,
                    instruction_trusted=bool(
                        record.provenance.verified
                        and record.tier == MemoryTier.PROCEDURAL
                    ),
                    retrieval_reason=reason,
                )
            )
        result.sort(
            key=lambda item: (item.relevance, item.record.updated_at), reverse=True
        )
        return result[: query.limit]

    def working_packet(self, query: MemoryQuery) -> WorkingMemoryPacket:
        # Cap every tier so one noisy source cannot crowd out the context packet.
        per_tier = max(1, min(10, query.limit // max(1, len(query.tiers))))
        memories: list[RetrievedMemory] = []
        for tier in query.tiers:
            tier_query = query.model_copy(
                update={"tiers": [tier], "limit": per_tier}
            )
            memories.extend(self.retrieve(tier_query))
        memories.sort(
            key=lambda item: (item.relevance, item.record.updated_at), reverse=True
        )
        return WorkingMemoryPacket(
            realm_id=query.realm_id,
            requester_principal=query.requester_principal,
            goal_ids=query.goal_ids,
            memories=memories[: query.limit],
        )
