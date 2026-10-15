"""Negotiated conversation extensions; advertising is not lifecycle safety."""
from pa.acp.client import _to_plain


def conversation_capabilities(initialize):
    payload = _to_plain(initialize) or {}
    if not isinstance(payload, dict):
        payload = {}
    caps = payload.get("agentCapabilities", payload.get("agent_capabilities", {})) or {}
    if not isinstance(caps, dict):
        caps = {}
    sessions = caps.get("sessionCapabilities", caps.get("session_capabilities", {})) or {}
    if not isinstance(sessions, dict):
        sessions = {}
    meta = payload.get("_meta", payload.get("field_meta", {})) or {}
    if not isinstance(meta, dict):
        meta = {}
    steering = meta.get("steering") or {}
    advertised = isinstance(steering, dict) and steering.get("supported") is True
    return {
        "fork": isinstance(sessions.get("fork"), dict) or sessions.get("fork") is True,
        "steering_advertised": advertised,
        "steering_available": False,
        "steering_reason": (
            "This provider may start a new turn when steering races with completion, without a durable delivery ID or observable completion receipt. PA queues input safely."
            if advertised else "This connection does not advertise a supported steering contract. PA queues input safely."
        ),
    }
