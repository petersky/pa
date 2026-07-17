"""Agent prompt context from projects and cards."""

from __future__ import annotations

from pa.domain.models import Card, Project
from pa.domain.store import Store


PA_BROWSER_CONTEXT = """## PA browser
PA provides browser tools through the `pa` MCP server. For browser work, use the
`browser_attach`, `browser_open`, `browser_snapshot`, `browser_click`,
`browser_type`, `browser_resize`, and `browser_screenshot` tools from that server.
Prefer these tools over the Codex in-app browser. You may attach and configure a
headless browser yourself; the user does not need to attach one first."""


def resolve_project_for_prompt(
    store: Store,
    *,
    card_id: str | None = None,
    project_id: str | None = None,
    realm_id: str = "default",
) -> Project | None:
    if project_id:
        return store.get_project(project_id, realm_id=realm_id)
    if card_id:
        card = store.get_card(card_id, realm_id=realm_id)
        if card and card.project_id:
            return store.get_project(card.project_id, realm_id=realm_id)
    return None


def build_project_context_prefix(project: Project | None, card: Card | None = None) -> str:
    if not project and not card:
        return ""
    parts: list[str] = []
    if project:
        parts.append(f"# Project: {project.title}")
        if project.description:
            parts.append(project.description)
        if project.agent_prompt:
            parts.append(f"\n## Agent instructions\n{project.agent_prompt}")
        if project.repos:
            repos = ", ".join(r.url for r in project.repos)
            parts.append(f"\n## Repositories\n{repos}")
    if card:
        parts.append(f"\n# Card: {card.title}")
        if card.body:
            parts.append(card.body)
    return "\n".join(parts).strip()


def augment_message_with_context(
    store: Store,
    message: str,
    *,
    card_id: str | None = None,
    project_id: str | None = None,
    realm_id: str = "default",
) -> str:
    project = resolve_project_for_prompt(
        store,
        card_id=card_id,
        project_id=project_id,
        realm_id=realm_id,
    )
    card = store.get_card(card_id, realm_id=realm_id) if card_id else None
    prefix = build_project_context_prefix(project, card)
    prefixes = [PA_BROWSER_CONTEXT]
    if prefix:
        prefixes.insert(0, prefix)
    context = "\n\n".join(prefixes)
    return f"{context}\n\n---\n\n{message}"
