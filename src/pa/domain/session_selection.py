from __future__ import annotations

from collections.abc import Iterable

from pa.domain.models import AgentSession


def preferred_sessions_by_card(
    sessions: Iterable[AgentSession],
) -> dict[str, AgentSession]:
    """Select the newest open session per card, falling back to the newest closed one."""
    selected: dict[str, AgentSession] = {}
    for session in sessions:
        if not session.card_id:
            continue
        current = selected.get(session.card_id)
        if current is None or (
            current.status == "closed" and session.status != "closed"
        ):
            selected[session.card_id] = session
    return selected
