"""Render centralized PA prompt context using resolved session values."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from pa.config import Settings
from pa.domain.models import AgentSession, Card, Project
from pa.domain.store import Store
from pa.prompts import PROMPTS, RenderedPrompt


class PromptComposition(BaseModel):
    text: str
    prompts: list[RenderedPrompt] = Field(default_factory=list)

    def audit_records(self) -> list[dict[str, Any]]:
        return [prompt.audit_record() for prompt in self.prompts]


_CONTEXT_TRUNCATION_MARKER = "\n… [truncated to fit provider context]"
_CONTEXT_SEPARATOR = "\n\n---\n\n"


def _bounded_text(value: Any, limit: int) -> str:
    text = str(value)
    if len(text) <= limit:
        return text
    keep = max(0, limit - len(_CONTEXT_TRUNCATION_MARKER))
    return text[:keep].rstrip() + _CONTEXT_TRUNCATION_MARKER


def _fit_context_prompts(
    prompts: list[RenderedPrompt], budget: int
) -> list[RenderedPrompt]:
    if budget < 0:
        return prompts
    result = list(prompts)
    while len("\n\n".join(prompt.text for prompt in result)) > budget:
        candidates = [
            (len(prompt.text), index)
            for index, prompt in enumerate(result)
            if prompt.key in {"agent.context.project", "agent.context.card"}
            and len(prompt.text) > 512 + len(_CONTEXT_TRUNCATION_MARKER)
        ]
        if not candidates:
            break
        _length, index = max(candidates)
        prompt = result[index]
        current_context = "\n\n".join(item.text for item in result)
        overflow = len(current_context) - budget
        keep = max(512, len(prompt.text) - overflow - len(_CONTEXT_TRUNCATION_MARKER))
        text = prompt.text[:keep].rstrip() + _CONTEXT_TRUNCATION_MARKER
        result[index] = prompt.model_copy(
            update={
                "text": text,
                "character_count": len(text),
                "truncated": True,
                "original_character_count": (
                    prompt.original_character_count or prompt.character_count
                ),
            }
        )
    return result


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


def _project_values(project: Project) -> dict[str, Any]:
    policy = dict((project.tool_config or {}).get("pr_policy") or {})
    return {
        "title": _bounded_text(project.title, 4_000),
        "description": _bounded_text(
            project.description or "(no project description)", 32_000
        ),
        "agent_prompt": _bounded_text(
            project.agent_prompt or "(no additional project instructions)", 64_000
        ),
        "repositories": _bounded_text(
            ", ".join(repo.url for repo in project.repos) or "(no linked repositories)",
            16_000,
        ),
        "pr_ready_policy": (
            "ready for review"
            if policy.get("ready_by_default", True)
            else "according to explicit policy"
        ),
        "executor_notifications": (
            "enabled" if policy.get("auto_notify", True) else "disabled"
        ),
        "merge_on_green": (
            "enabled" if policy.get("agent_merge_on_green", True) else "disabled"
        ),
    }


def _card_values(card: Card) -> dict[str, str]:
    return {
        "title": _bounded_text(card.title, 4_000),
        "body": _bounded_text(card.body or "(no card body)", 120_000),
    }


def build_project_context_prefix(
    project: Project | None,
    card: Card | None = None,
    *,
    provider: str = "default",
) -> str:
    rendered: list[str] = []
    if project:
        rendered.append(
            PROMPTS.render(
                "agent.context.project",
                {"project": _project_values(project)},
                provider=provider,
            ).text
        )
    if card:
        rendered.append(
            PROMPTS.render(
                "agent.context.card",
                {"card": _card_values(card)},
                provider=provider,
            ).text
        )
    return "\n\n".join(rendered)


def _execution_values(settings: Settings, session: AgentSession) -> dict[str, Any]:
    config = dict(session.config_json or {})
    execution = dict(config.get("execution_context") or {})
    execution_instance = dict(execution.get("instance") or {})
    execution_instance.setdefault("id", settings.instance_id)
    execution_instance.setdefault("name", settings.instance_name)
    authority = dict(execution.get("authority_instance") or {})
    authority.setdefault("id", execution_instance["id"])
    authority.setdefault("name", execution_instance["name"])
    repositories = list(execution.get("repositories") or [])
    repository = dict(repositories[0]) if repositories else {}
    cwd = str(execution.get("cwd") or session.cwd or "(not materialized)")
    worktree = str(
        repository.get("worktree_path") or repository.get("workspace") or cwd
    )
    checkout = str(repository.get("checkout_path") or worktree)
    return {
        "execution_instance": execution_instance,
        "authority_instance": authority,
        "repository": {
            "id": repository.get("repository_id") or "(not linked)",
            "url": repository.get("repository_url") or "(not linked)",
        },
        "checkout": {"path": checkout},
        "worktree": {"path": worktree},
        "branch": repository.get("branch") or "(not linked)",
        "base_sha": repository.get("base_sha") or "(not linked)",
    }


def compose_session_prompt(
    store: Store,
    settings: Settings,
    session: AgentSession,
    message: str,
    *,
    card_id: str | None = None,
    project_id: str | None = None,
    realm_id: str | None = None,
    seed_prompts: list[RenderedPrompt] | None = None,
) -> PromptComposition:
    provider = session.agent_name or "default"
    realm = realm_id or settings.primary_realm
    effective_card_id = card_id or session.card_id
    effective_project_id = project_id or session.project_id
    card = (
        store.get_card(effective_card_id, realm_id=realm) if effective_card_id else None
    )
    project = resolve_project_for_prompt(
        store,
        card_id=effective_card_id,
        project_id=effective_project_id,
        realm_id=realm,
    )
    prompts = list(seed_prompts or [])
    if project:
        prompts.append(
            PROMPTS.render(
                "agent.context.project",
                {"project": _project_values(project)},
                provider=provider,
            )
        )
    if card:
        prompts.append(
            PROMPTS.render(
                "agent.context.card",
                {"card": _card_values(card)},
                provider=provider,
            )
        )
    prompts.extend(
        [
            PROMPTS.render(
                "agent.context.execution",
                _execution_values(settings, session),
                provider=provider,
            ),
            PROMPTS.render("agent.context.data_safety", provider=provider),
            PROMPTS.render("agent.context.browser", provider=provider),
        ]
    )
    wrapper_limit = PROMPTS.character_limit("agent.message.wrapper", provider=provider)
    prompts = _fit_context_prompts(
        prompts, wrapper_limit - len(message) - len(_CONTEXT_SEPARATOR)
    )
    context = "\n\n".join(prompt.text for prompt in prompts)
    wrapper = PROMPTS.render(
        "agent.message.wrapper",
        {"context": context, "message": message},
        provider=provider,
    )
    prompts.append(wrapper)
    return PromptComposition(text=wrapper.text, prompts=prompts)


def augment_message_with_context(
    store: Store,
    message: str,
    *,
    card_id: str | None = None,
    project_id: str | None = None,
    realm_id: str = "default",
) -> str:
    """Compatibility helper for code without a materialized session.

    Operational paths compose at AgentSessionRuntime._run_prompt so the selected
    execution instance and workspace are exact.
    """
    project = resolve_project_for_prompt(
        store, card_id=card_id, project_id=project_id, realm_id=realm_id
    )
    card = store.get_card(card_id, realm_id=realm_id) if card_id else None
    prefixes = [
        value
        for value in (
            build_project_context_prefix(project, card),
            PROMPTS.render("agent.context.data_safety").text,
            PROMPTS.render("agent.context.browser").text,
        )
        if value
    ]
    return PROMPTS.render(
        "agent.message.wrapper",
        {"context": "\n\n".join(prefixes), "message": message},
    ).text
