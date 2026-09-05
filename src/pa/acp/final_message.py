"""Canonical provider phase and final-assistant-message handling."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from typing import Any

_FINAL_PHASE_ALIASES = {"final", "final_answer"}
_EXCLUDED_PHASES = {"analysis", "commentary", "reasoning", "thought"}
_REPLACEMENT_MODES = {"accumulated", "replace", "replacement", "snapshot"}
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

_REQUEST_ACTION = (
    r"(?:choose|select|confirm|approve|deny|provide|reply|respond|run|"
    r"sign[ -]?in|log[ -]?in|upload|attach|enter|tell|decide|pick|review)"
)
_INPUT_REQUEST = re.compile(
    rf"(?is)(?:"
    rf"\b(?:please|can you|could you|would you)\s+{_REQUEST_ACTION}\b|"
    rf"\b(?:I|we)\s+(?:still\s+)?need you to\s+{_REQUEST_ACTION}\b|"
    rf"\b(?:I(?:'m| am)|we(?:'re| are)|the agent is)\s+(?:blocked|waiting)\s+"
    rf"(?:on|for|until)\b.{{0,240}}\b(?:you|your)\b|"
    rf"\b(?:to continue|before (?:I|we|the agent) can continue)\b.{{0,240}}"
    rf"\b(?:you|your)\b|"
    rf"\b(?:which|what|where)\b.{{0,240}}\b(?:should (?:I|we)|do you want|"
    rf"would you prefer)\b.{{0,120}}\?|"
    rf"\bdo you (?:approve|confirm|want)\b.{{0,240}}\?"
    rf")"
)
_NO_INPUT_REQUIRED = re.compile(
    r"(?i)\b(?:no (?:user|operator) action (?:is )?required|"
    r"nothing for you to do|no response (?:is )?needed)\b"
)


def _json_object_text(value: str) -> bool:
    candidate = value.strip()
    fenced = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", candidate, re.DOTALL)
    if fenced:
        candidate = fenced.group(1).strip()
    if not (candidate.startswith("{") and candidate.endswith("}")):
        return False
    try:
        return isinstance(json.loads(candidate), dict)
    except (TypeError, ValueError):
        return False


def normalize_provider_phase(value: Any) -> str | None:
    """Map retained and current provider phase aliases to one stable value."""
    if value is None:
        return None
    phase = str(value).strip().lower()
    if not phase:
        return None
    return "final" if phase in _FINAL_PHASE_ALIASES else phase


def is_agent_message_type(event_type: Any) -> bool:
    return str(event_type or "") in _AGENT_MESSAGE_TYPES


def _plain_event(value: Any, order: int) -> dict[str, Any]:
    if isinstance(value, Mapping):
        event_type = str(
            value.get("event_type")
            or value.get("type")
            or value.get("sessionUpdate")
            or value.get("session_update")
            or ""
        )
        payload_value = value.get("payload")
        payload = (
            dict(payload_value) if isinstance(payload_value, Mapping) else dict(value)
        )
        sequence = value.get("seq")
    else:
        event_type = str(
            getattr(value, "event_type", None) or getattr(value, "type", None) or ""
        )
        payload_value = getattr(value, "payload", None)
        payload = dict(payload_value) if isinstance(payload_value, Mapping) else {}
        sequence = getattr(value, "seq", None)
    try:
        sort_key = int(sequence) if sequence is not None else order
    except TypeError, ValueError:
        sort_key = order
    return {"type": event_type, "payload": payload, "order": sort_key}


def assemble_final_assistant_message(events: Iterable[Any]) -> str:
    """Rebuild exactly one final assistant message from one bounded turn.

    Final-phase chunks win over partial response streams. Within that set, the
    latest message ID is authoritative; chunks from commentary, reasoning, and
    other assistant message IDs are never concatenated with it.
    """
    plain = sorted(
        (_plain_event(value, order) for order, value in enumerate(events, start=1)),
        key=lambda event: event["order"],
    )
    messages = [
        event
        for event in plain
        if is_agent_message_type(event["type"])
        and normalize_provider_phase(event["payload"].get("phase"))
        not in _EXCLUDED_PHASES
    ]
    final_messages = [
        event
        for event in messages
        if normalize_provider_phase(event["payload"].get("phase")) == "final"
        or event["type"] in _FINAL_MESSAGE_TYPES
        or bool(event["payload"].get("final"))
        or bool(event["payload"].get("is_final"))
    ]
    candidates = final_messages or messages
    if not candidates:
        return ""

    selected = next(
        (
            event
            for event in reversed(candidates)
            if str(event["payload"].get("text") or "")
        ),
        candidates[-1],
    )
    selected_message_id = selected["payload"].get("message_id")
    selected_text = str(selected["payload"].get("text") or "")
    # A complete machine-readable final object is its own message boundary.
    # Older providers sometimes omitted message IDs and phases, which used to
    # prepend commentary to a final disposition object.
    if selected_message_id in {None, ""} and _json_object_text(selected_text):
        return selected_text.strip()
    if (
        selected_message_id in {None, ""}
        and selected["type"] in _FINAL_MESSAGE_TYPES
        and selected_text
    ):
        return selected_text.strip()
    selected_key = (
        str(selected_message_id)
        if selected_message_id not in {None, ""}
        else "__unidentified__"
    )
    text = ""
    for event in candidates:
        message_id = event["payload"].get("message_id")
        message_key = (
            str(message_id) if message_id not in {None, ""} else "__unidentified__"
        )
        if message_key != selected_key:
            continue
        payload = event["payload"]
        chunk = str(payload.get("text") or "")
        mode = str(
            payload.get("content_mode") or payload.get("operation") or "delta"
        ).lower()
        if mode in _REPLACEMENT_MODES:
            text = chunk
        else:
            text += chunk
    return text.strip()


def likely_user_input_request(text: str) -> str | None:
    """Conservatively identify a final answer that explicitly blocks on a user.

    Structured permission, elicitation, and operator-input mechanisms always
    take precedence. This fallback intentionally avoids treating every final
    question or suggestion as a durable request.
    """
    normalized = str(text or "").strip()
    if not normalized or _NO_INPUT_REQUIRED.search(normalized):
        return None
    # Card dispositions and other final protocol objects are not questions,
    # even when string values happen to contain words such as "run" or "then".
    if _json_object_text(normalized):
        return None
    paragraphs = [
        part.strip() for part in re.split(r"\n\s*\n", normalized) if part.strip()
    ]
    if paragraphs and _json_object_text(paragraphs[-1]):
        return None
    match = _INPUT_REQUEST.search(normalized)
    if not match:
        return None
    selected = next(
        (part for part in reversed(paragraphs) if _INPUT_REQUEST.search(part)),
        match.group(0),
    )
    return selected[:8000]
