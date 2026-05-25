from typing import Any

from pa.domain.models import KnowledgeEntry
from pa.domain.store import Store


def _extract_text(update: Any) -> str | None:
    if update is None:
        return None
    if isinstance(update, dict):
        session_update = update.get("sessionUpdate") or update.get("session_update")
        if session_update == "agent_message_chunk":
            content = update.get("content") or {}
            if isinstance(content, dict) and content.get("type") == "text":
                return content.get("text")
        content = update.get("content")
        if isinstance(content, dict) and content.get("type") == "text":
            return content.get("text")
        if isinstance(content, str):
            return content
    content = getattr(update, "content", None)
    if content is not None:
        text = getattr(content, "text", None)
        if text:
            return text
    return None


def capture_from_updates(
    store: Store,
    *,
    session_id: str | None,
    item_id: str | None,
    updates: list[Any],
) -> KnowledgeEntry | None:
    """Summarize agent session output into storable knowledge."""
    chunks = [t for u in updates if (t := _extract_text(u))]
    if not chunks:
        return None

    summary = " ".join(chunks).strip()
    if len(summary) > 2000:
        summary = summary[:1997] + "..."

    entry = KnowledgeEntry(
        session_id=session_id,
        item_id=item_id,
        summary=summary,
        source="acp_session",
        tags=["auto-capture"],
    )
    return store.add_knowledge(entry)
