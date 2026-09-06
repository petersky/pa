"""Derived message-complete browser pages; durable events remain unchanged."""

from pa.domain.models import TranscriptEvent


def stream_key(event: TranscriptEvent) -> tuple | None:
    if event.event_type not in {"agent_message_chunk", "agent_thought_chunk"}:
        return None
    payload = event.payload
    return (event.event_type, payload.get("message_id"), payload.get("phase"))


def compact_chunks(events: list[TranscriptEvent]) -> list[TranscriptEvent]:
    """Compact message deltas, retaining exact text and the source sequence span."""
    result: list[TranscriptEvent | None] = []
    positions: dict[tuple, int] = {}
    for event in events:
        key = stream_key(event)
        if event.event_type in {"user_message", "turn_completed", "cancelled", "connection_lost"}:
            positions.clear()
        # Unidentified legacy streams end at tools; identified messages may
        # contain intervening tool updates without losing their prefix.
        if event.event_type == "tool_call":
            positions = {key: value for key, value in positions.items() if key[1]}
        if key is not None and key in positions:
            previous = result[positions[key]]
            assert previous is not None
            result[positions[key]] = None
            payload = dict(event.payload)
            mode = str(payload.get("content_mode") or "delta").lower()
            text = str(payload.get("text") or "")
            if mode not in {"snapshot", "replace", "replacement", "accumulated"}:
                text = str(previous.payload.get("text") or "") + text
            payload.update(text=text, content_mode="snapshot")
            payload["source_first_seq"] = previous.payload.get("source_first_seq", previous.seq)
            event = event.model_copy(update={"payload": payload})
        if key is not None:
            positions[key] = len(result)
        result.append(event)
    return [event for event in result if event is not None]


def complete_page(store, session_id: str, events: list[TranscriptEvent], *, forward: bool):
    """Extend a raw page to stream boundaries using bounded indexed reads.

    A pathological stream fails explicitly instead of returning a silently
    truncated message. The caller's timeout also bounds cold-object hydration.
    """
    events = list(events)
    examined = len(events)
    for direction in ("before", "after") if forward else ("before",):
        ordered = events if direction == "before" else list(reversed(events))
        message = next((event for event in ordered if stream_key(event) is not None), None)
        if message is None:
            continue
        key = stream_key(message)
        while events:
            edge = events[0] if direction == "before" else events[-1]
            if direction == "before":
                batch = store.list_transcript_events_before(session_id, before_seq=edge.seq, limit=1000)
                candidates = reversed(batch)
            else:
                batch = store.list_transcript_events(session_id, after_seq=edge.seq, limit=1000)
                candidates = iter(batch)
            extension = []
            pending = []
            boundary = False
            for candidate in candidates:
                candidate_key = stream_key(candidate)
                if candidate.event_type in {"user_message", "turn_completed", "cancelled", "connection_lost"} or (
                    candidate_key is not None and candidate_key[0] == key[0] and candidate_key != key
                ) or (not key[1] and candidate.event_type == "tool_call"):
                    boundary = True
                    break
                pending.append(candidate)
                if candidate_key == key:
                    extension.extend(pending)
                    pending = []
            # A full batch of intervening tools may separate chunks. Keep the
            # span until the next indexed read can establish the boundary.
            if not boundary and len(batch) == 1000:
                extension.extend(pending)
            examined += len(batch)
            if examined > 20000:
                raise ValueError("Message exceeds the history page work budget; retry with a smaller page.")
            if direction == "before":
                events = list(reversed(extension)) + events
            else:
                events.extend(extension)
            if boundary or len(batch) < 1000:
                break
    if not events:
        return events, None, None, False, False
    oldest, newest = events[0].seq, events[-1].seq
    older = bool(store.list_transcript_events_before(session_id, before_seq=oldest, limit=1))
    newer = bool(store.list_transcript_events(session_id, after_seq=newest, limit=1))
    return compact_chunks(events), oldest, newest, older, newer
