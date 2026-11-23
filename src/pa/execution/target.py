"""Worker target precedence shared by card starts and fleet placement."""


from typing import Any


def worker_target(explicit: str | None, card: Any) -> tuple[str | None, str]:
    """Return the requested worker and its origin; eligibility stays with placement."""
    if explicit:
        return explicit, "explicit_target"
    if card and card.preferred_instance:
        return card.preferred_instance, "card_preferred_instance"
    return None, "placement_policy"
