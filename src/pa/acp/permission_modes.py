"""Defaults for new logical agents; existing native sessions retain authority."""


def default_permission_mode(provider_id: str, purpose: str) -> str | None:
    # Mode IDs are provider contracts, not generic permission levels. In
    # particular Codex's `agent` uses native auto review, not ACP/user review.
    if provider_id == "codex":
        return "agent-full-access" if purpose == "chat" else "workspace-write"
    if provider_id == "openinterpreter" and purpose != "chat":
        return "workspace-write"
    return None
