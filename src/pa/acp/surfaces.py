"""Stable surface keys for agent invocation contexts.

Surfaces are string keys (not a closed enum) so new invocation sites can
register a key and participate in provider/model selection without schema churn.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# Well-known surfaces (documented; callers may pass any string).
SURFACE_CHAT_DEFAULT = "chat.default"
SURFACE_CHAT_CARD = "chat.card"
SURFACE_PROJECT = "project"
SURFACE_EXECUTION = "execution"

KNOWN_SURFACES: tuple[str, ...] = (
    SURFACE_CHAT_DEFAULT,
    SURFACE_CHAT_CARD,
    SURFACE_PROJECT,
    SURFACE_EXECUTION,
)


@dataclass(frozen=True)
class AgentInvocationContext:
    """Context for resolving which ACP provider (and options) to use."""

    surface: str
    principal_id: str | None = None
    card_id: str | None = None
    project_id: str | None = None
    provider_override: str | None = None
    model_id: str | None = None
    mode_id: str | None = None
    config: dict[str, Any] = field(default_factory=dict)

    def user_id(self) -> str | None:
        if self.principal_id and self.principal_id.startswith("user:"):
            return self.principal_id[5:]
        return None


def surface_for_label(label: str | None, *, project_id: str | None = None) -> str:
    """Map a session label / project to a surface key."""
    if label and label.startswith("card:"):
        return SURFACE_CHAT_CARD
    if project_id:
        return SURFACE_PROJECT
    if label == "default" or label is None:
        return SURFACE_CHAT_DEFAULT
    return SURFACE_CHAT_DEFAULT
