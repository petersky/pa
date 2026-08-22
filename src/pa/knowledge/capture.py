"""Canonical ACP transcript assembly and explicit Memory promotion."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pa.acp.final_message import normalize_provider_phase
from pa.domain.models import (
    KnowledgeAuditEvent,
    KnowledgeEntry,
    KnowledgeKind,
    KnowledgeSensitivity,
    KnowledgeProvenance,
    KnowledgeStatus,
    KnowledgeTier,
    KnowledgeUpdate,
    TranscriptEvent,
)
from pa.domain.store import Store

TRANSFORMATION_VERSION = "pa.memory.canonical.v1"
_REPLACEMENT_MODES = {"replace", "replacement", "snapshot", "accumulated"}
_AGENT_MESSAGE_TYPES = {
    "agent_message_chunk",
    "agent_message",
    "agent_message_final",
    "assistant_message",
    "message_completed",
}
_FINAL_MESSAGE_TYPES = {
    "agent_message",
    "agent_message_final",
    "assistant_message",
    "message_completed",
}
_CORRUPTION_PATTERNS = (
    (
        "split-word",
        re.compile(r"\b(?:C\s+od\s+ex|[A-Za-z]{2,}\s+[A-Za-z]\s+[A-Za-z]{2,})\b"),
    ),
    (
        "spaced-identifier",
        re.compile(r"\b[0-9a-fA-F]{1,8}(?:\s+-\s+|\s+)[0-9a-fA-F]{4,}\b"),
    ),
    ("spaced-markdown-delimiter", re.compile(r"(?:\*\s+\*|`\s+`|#\s+#)")),
    ("spaced-punctuation", re.compile(r"\w\s+[-/:.]\s+\w")),
)


@dataclass(frozen=True)
class CanonicalTranscript:
    text: str
    content_hash: str
    event_start: int | None
    event_end: int | None
    event_ids: tuple[str, ...]
    message_ids: tuple[str, ...]


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def has_policy_memory_candidate(updates: Iterable[Any]) -> bool:
    """Return true only for the narrow, typed opt-in candidate marker."""

    for update in updates:
        if hasattr(update, "model_dump"):
            update = update.model_dump(mode="json", by_alias=True)
        if not isinstance(update, dict):
            continue
        raw = update.get("raw") if isinstance(update.get("raw"), dict) else update
        meta = raw.get("_meta") if isinstance(raw, dict) else None
        pa_meta = meta.get("pa") if isinstance(meta, dict) else None
        if isinstance(pa_meta, dict) and pa_meta.get("memory_candidate") is True:
            return True
    return False


def _plain_event(value: Any, fallback_seq: int) -> dict[str, Any]:
    if isinstance(value, TranscriptEvent):
        return {
            "id": value.id,
            "seq": value.seq,
            "type": value.event_type,
            "payload": value.payload,
        }
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json", by_alias=True)
    if not isinstance(value, dict):
        return {"id": "", "seq": fallback_seq, "type": "unknown", "payload": {}}

    event_type = str(
        value.get("event_type")
        or value.get("type")
        or value.get("sessionUpdate")
        or value.get("session_update")
        or "unknown"
    )
    if isinstance(value.get("payload"), dict):
        payload = dict(value["payload"])
    else:
        payload = dict(value)
        content = value.get("content")
        if isinstance(content, dict):
            payload["text"] = content.get("text", "")
        elif isinstance(content, str):
            payload["text"] = content
        meta = value.get("_meta") or {}
        codex = meta.get("codex") if isinstance(meta, dict) else {}
        if isinstance(codex, dict):
            payload.setdefault("phase", codex.get("phase"))
            payload.setdefault(
                "content_mode",
                codex.get("contentMode")
                or codex.get("content_mode")
                or codex.get("operation"),
            )
        payload.setdefault(
            "message_id", value.get("messageId") or value.get("message_id")
        )
        payload.setdefault(
            "content_mode",
            value.get("contentMode")
            or value.get("content_mode")
            or value.get("operation"),
        )
    return {
        "id": str(value.get("id") or ""),
        "seq": int(value.get("seq") or fallback_seq),
        "type": event_type,
        "payload": payload,
    }


def _deduplicate(events: Iterable[Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    seen_seqs: set[int] = set()
    for index, value in enumerate(events, start=1):
        event = _plain_event(value, index)
        event_id = event["id"]
        seq = event["seq"]
        if event_id and event_id in seen_ids:
            continue
        if seq and seq in seen_seqs:
            continue
        if event_id:
            seen_ids.add(event_id)
        if seq:
            seen_seqs.add(seq)
        result.append(event)
    return sorted(result, key=lambda event: event["seq"])


def _latest_candidate_turn(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Select the latest completed assistant turn, or the latest partial turn."""
    turns: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    for event in events:
        if event["type"] == "user_message" and current:
            turns.append(current)
            current = []
        current.append(event)
        if event["type"] == "turn_completed":
            turns.append(current)
            current = []
    if current:
        turns.append(current)
    candidates = [
        turn
        for turn in turns
        if any(event["type"] in _AGENT_MESSAGE_TYPES for event in turn)
    ]
    completed = [
        turn for turn in candidates if any(e["type"] == "turn_completed" for e in turn)
    ]
    return (completed or candidates)[-1] if (completed or candidates) else []


def assemble_canonical_transcript(events: Iterable[Any]) -> CanonicalTranscript:
    """Rebuild the canonical final assistant message without inventing whitespace.

    Reasoning, commentary, plans, tool events, and display labels are excluded.
    ACP deltas append exactly as emitted. Typed replacement/accumulated events
    replace the prior content for their message stream.
    """

    selected_turn = _latest_candidate_turn(_deduplicate(events))
    message_events = [
        event
        for event in selected_turn
        if event["type"] in _AGENT_MESSAGE_TYPES
        and normalize_provider_phase(event["payload"].get("phase"))
        not in {"commentary", "analysis", "reasoning", "thought"}
    ]
    final_events = [
        event
        for event in message_events
        if normalize_provider_phase(event["payload"].get("phase")) == "final"
        or event["type"] in _FINAL_MESSAGE_TYPES
        or bool(event["payload"].get("final"))
        or bool(event["payload"].get("is_final"))
    ]
    if final_events:
        message_events = final_events

    streams: dict[str, str] = {}
    stream_order: list[str] = []
    contributing: dict[str, list[dict[str, Any]]] = {}
    for event in message_events:
        payload = event["payload"]
        message_id = str(payload.get("message_id") or "agent")
        if message_id not in streams:
            streams[message_id] = ""
            stream_order.append(message_id)
            contributing[message_id] = []
        text = str(payload.get("text") or "")
        mode = str(payload.get("content_mode") or payload.get("operation") or "delta")
        mode = mode.lower()
        if mode in _REPLACEMENT_MODES:
            streams[message_id] = text
            contributing[message_id] = [event]
        else:
            streams[message_id] += text
            contributing[message_id].append(event)

    # Message streams are protocol content blocks. Concatenating their exact
    # contents preserves the emitted boundary without synthesizing a separator.
    text = "".join(streams[key] for key in stream_order)
    used = [event for key in stream_order for event in contributing[key]]
    used.sort(key=lambda event: event["seq"])
    ids = tuple(event["id"] for event in used if event["id"])
    seqs = [event["seq"] for event in used if event["seq"]]
    return CanonicalTranscript(
        text=text,
        content_hash=content_hash(text),
        event_start=min(seqs) if seqs else None,
        event_end=max(seqs) if seqs else None,
        event_ids=ids,
        message_ids=tuple(key for key in stream_order if key != "agent"),
    )


def promote_from_transcript(
    store: Store,
    *,
    session_id: str,
    actor: str,
    summary: str | None = None,
    start_seq: int | None = None,
    end_seq: int | None = None,
    kind: KnowledgeKind = KnowledgeKind.MEMORY,
    tier: KnowledgeTier = KnowledgeTier.SEMANTIC,
    sensitivity: KnowledgeSensitivity = KnowledgeSensitivity.INTERNAL,
    scope: str = "realm",
    status: KnowledgeStatus = KnowledgeStatus.ACTIVE,
    source_url: str | None = None,
    owner: str | None = None,
    confidence: float | None = None,
    card_id: str | None = None,
    tags: list[str] | None = None,
    supersedes_id: str | None = None,
    review_at: datetime | None = None,
    expires_at: datetime | None = None,
    regenerated_from_id: str | None = None,
    source: str = "promoted",
    provenance_action: str | None = None,
) -> KnowledgeEntry:
    """Explicitly promote a curated summary or the canonical final message."""

    if not store.get_session(session_id):
        raise ValueError(f"Session not found: {session_id}")
    events = store.list_transcript_events_range(
        session_id, start_seq=start_seq, end_seq=end_seq
    )
    canonical = assemble_canonical_transcript(events)
    if not canonical.text.strip():
        raise ValueError(
            "The selected transcript range has no promotable final content"
        )
    promoted_text = canonical.text if summary is None else summary
    if not promoted_text.strip():
        raise ValueError("Memory content cannot be empty")

    action = provenance_action or ("regenerated" if regenerated_from_id else "promoted")
    provenance = KnowledgeProvenance(
        source_session_id=session_id,
        source_event_start=canonical.event_start,
        source_event_end=canonical.event_end,
        source_event_ids=list(canonical.event_ids),
        source_message_ids=list(canonical.message_ids),
        actor=actor,
        action=action,
        transformation=TRANSFORMATION_VERSION,
        source_content_hash=canonical.content_hash,
        regenerated_from_id=regenerated_from_id,
    )
    candidate = KnowledgeEntry(
        session_id=session_id,
        item_id=card_id,
        card_id=card_id,
        summary=promoted_text,
        source=source,
        source_url=source_url,
        kind=kind,
        tier=tier,
        status=status,
        scope=scope,
        owner=owner or actor,
        confidence=confidence,
        sensitivity=sensitivity,
        provenance_trust="verified",
        supersedes_id=supersedes_id,
        review_at=review_at,
        expires_at=expires_at,
        tags=list(tags or []),
        content_hash=content_hash(promoted_text),
        provenance=provenance,
    )
    entry = store.add_knowledge(candidate)
    deduplicated = entry.id != candidate.id
    store.add_knowledge_audit(
        KnowledgeAuditEvent(
            knowledge_id=entry.id,
            action="promotion_deduplicated" if deduplicated else provenance.action,
            actor=actor,
            payload={
                "requested_entry_id": candidate.id,
                "session_id": session_id,
                "source_event_start": canonical.event_start,
                "source_event_end": canonical.event_end,
                "source_event_ids": list(canonical.event_ids),
                "source_content_hash": canonical.content_hash,
                "content_hash": candidate.content_hash,
                "scope": scope,
                "status": status.value,
                "transformation": TRANSFORMATION_VERSION,
            },
        )
    )
    return entry


def capture_from_updates(
    store: Store,
    *,
    session_id: str | None,
    item_id: str | None,
    updates: list[Any],
    enabled: bool = False,
    eligible: bool = False,
    actor: str = "policy:auto-capture",
) -> KnowledgeEntry | None:
    """Create a policy-selected pending candidate; automatic capture is off by default."""

    if (
        not enabled
        or not eligible
        or not session_id
        or not store.get_session(session_id)
    ):
        return None
    # The marker arrives on live ACP updates, but candidate content is always
    # rebuilt from the durable transcript so reconnect/retry delivery cannot
    # change the result.
    canonical = assemble_canonical_transcript(
        store.list_transcript_events_range(session_id)
    )
    if not canonical.text.strip():
        return None
    return promote_from_transcript(
        store,
        session_id=session_id,
        actor=actor,
        kind=KnowledgeKind.MEMORY,
        status=KnowledgeStatus.REVIEW,
        card_id=item_id,
        tags=["generated", "pending-review"],
        source="generated",
        provenance_action="generated_candidate",
    )


def audit_knowledge_records(store: Store) -> list[dict[str, Any]]:
    """Report likely corrupt or unintended records without mutating storage."""

    report: list[dict[str, Any]] = []
    for entry in store.list_knowledge(
        limit=10_000, status=None, curated_only=False
    ):
        reasons: list[str] = []
        if entry.source == "acp_session" or "auto-capture" in entry.tags:
            reasons.append("unintended-auto-capture")
        for label, pattern in _CORRUPTION_PATTERNS:
            if pattern.search(entry.summary):
                reasons.append(label)
        if not reasons:
            continue
        recoverable = False
        if entry.session_id and store.get_session(entry.session_id):
            events = store.list_transcript_events_range(entry.session_id)
            recoverable = bool(assemble_canonical_transcript(events).text.strip())
        report.append(
            {
                "id": entry.id,
                "source": entry.source,
                "status": entry.status.value,
                "reasons": sorted(set(reasons)),
                "session_id": entry.session_id,
                "recoverable": recoverable,
                "repair": "regenerate" if recoverable else "review-only",
            }
        )
    return report


def regenerate_knowledge(store: Store, *, entry_id: str, actor: str) -> KnowledgeEntry:
    """Supersede a record from canonical evidence; never guess missing text."""

    original = store.get_knowledge(entry_id)
    if not original:
        raise ValueError(f"Memory not found: {entry_id}")
    session_id = (
        original.provenance.source_session_id
        if original.provenance
        else original.session_id
    )
    if not session_id or not store.get_session(session_id):
        raise ValueError("Canonical source transcript is unavailable")
    start = original.provenance.source_event_start if original.provenance else None
    end = original.provenance.source_event_end if original.provenance else None
    regenerated = promote_from_transcript(
        store,
        session_id=session_id,
        actor=actor,
        start_seq=start,
        end_seq=end,
        kind=original.kind,
        scope=original.scope,
        status=KnowledgeStatus.ACTIVE,
        source_url=original.source_url,
        owner=original.owner,
        confidence=original.confidence,
        card_id=original.card_id or original.item_id,
        tags=[tag for tag in original.tags if tag != "auto-capture"] + ["regenerated"],
        supersedes_id=original.id,
        review_at=original.review_at,
        expires_at=original.expires_at,
        regenerated_from_id=original.id,
    )
    if regenerated.id == original.id:
        raise ValueError("Canonical regeneration matches the existing record")
    store.update_knowledge(
        original.id, KnowledgeUpdate(status=KnowledgeStatus.SUPERSEDED)
    )
    store.add_knowledge_audit(
        KnowledgeAuditEvent(
            knowledge_id=original.id,
            action="superseded_by_regeneration",
            actor=actor,
            payload={"replacement_id": regenerated.id},
        )
    )
    return regenerated


def record_lifecycle_change(
    store: Store,
    *,
    entry_id: str,
    status: KnowledgeStatus,
    actor: str,
    action: str = "lifecycle_changed",
) -> KnowledgeEntry:
    entry = store.update_knowledge(entry_id, KnowledgeUpdate(status=status))
    if not entry:
        raise ValueError(f"Memory not found: {entry_id}")
    store.add_knowledge_audit(
        KnowledgeAuditEvent(
            knowledge_id=entry.id,
            action=action,
            actor=actor,
            payload={"status": status.value, "at": datetime.now(UTC).isoformat()},
        )
    )
    return entry
