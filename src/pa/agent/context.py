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

PA_DATA_SAFETY_CONTEXT = """## PA data and sync safety
Treat the running PA server as the sole writer for its PA_DATA_DIR. Use PA MCP
tools or the local PA HTTP API for card, project, sync, and conflict-resolution
changes. Never import PA internals to mutate the Store/EventLog from a script,
write pa.db or sync_refs.json directly, or force a ref to a chosen head.

If sync status reports different durable and projection heads, call PA's
sync_reconcile tool/API. Do not restart PA merely to refresh cached state. For
diverged histories, use the conflict-resolution tool/API so PA records a merge
commit; preserve both parents and supply an explicit value for every conflict."""


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


def build_project_context_prefix(
    project: Project | None, card: Card | None = None
) -> str:
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
        pr_policy = dict((project.tool_config or {}).get("pr_policy") or {})
        ready = pr_policy.get("ready_by_default", True)
        notify = pr_policy.get("auto_notify", True)
        merge = pr_policy.get("agent_merge_on_green", True)
        parts.append(
            "\n## Pull request lifecycle\n"
            f"- Open card/project pull requests {'ready for review' if ready else 'according to explicit policy'} by default; "
            "use draft only when the user explicitly requests it.\n"
            "- Register the PR with PA's durable PR supervisor and preserve the "
            "originating session, instance, repository, card, and worktree context.\n"
            f"- Executor notifications are {'enabled' if notify else 'disabled'} for this project.\n"
            f"- Agent merge-on-green is {'enabled' if merge else 'disabled'}; never merge until "
            "the supervisor reports a stable green head and you independently "
            "revalidate every required signal."
        )
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
    prefixes = [PA_DATA_SAFETY_CONTEXT, PA_BROWSER_CONTEXT]
    if prefix:
        prefixes.insert(0, prefix)
    context = "\n\n".join(prefixes)
    return f"{context}\n\n---\n\n{message}"
