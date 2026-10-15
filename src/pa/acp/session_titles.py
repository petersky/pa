"""Provider titles are metadata, never authority to rename a user's conversation."""

from datetime import datetime
from typing import Any


def clean_title(value: Any) -> str | None:
    if not isinstance(value, str) or len(value) > 512:
        return None
    value = " ".join(value.split())
    return value if value and not any(ord(c) < 32 for c in value) else None


def apply_provider_title(session, title, *, external_session_id, updated_at=None) -> bool:
    title = clean_title(title)
    if not title or not external_session_id or external_session_id != session.external_session_id:
        return False
    config = dict(session.config_json or {})
    naming = dict(config.get("naming") or {})
    if updated_at is not None:
        try:
            stamp = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            if stamp.tzinfo is None:
                return False
            previous = naming.get("provider_updated_at")
            if previous and stamp <= datetime.fromisoformat(previous):
                return False
        except (TypeError, ValueError, AttributeError):
            return False
        naming["provider_updated_at"] = stamp.isoformat()
    naming["version"] = naming.get("version", 0) + 1
    naming["provider_title"] = title
    naming["provider_session_id"] = external_session_id
    if naming.get("mode") != "manual":
        session.title = title
        naming["mode"] = "automatic"
    config["naming"] = naming
    session.config_json = config
    return True
