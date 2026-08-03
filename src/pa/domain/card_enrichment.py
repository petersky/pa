"""Asynchronous ACP-backed enrichment for newly created cards."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Collection
from typing import Any

from pa.acp.final_message import assemble_final_assistant_message
from pa.domain.models import CardKind, CardUpdate

logger = logging.getLogger(__name__)

_JSON_FENCE = re.compile(
    r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL | re.IGNORECASE
)
_ENRICHABLE = {"body", "kind", "project_id", "preferred_capabilities", "tags"}


def explicit_enrichment_fields(data: Any) -> set[str]:
    """Return fields whose supplied values enrichment must preserve."""
    fields = set(getattr(data, "model_fields_set", set())) & _ENRICHABLE
    for field in ("body", "project_id", "preferred_capabilities", "tags"):
        if not getattr(data, field, None):
            fields.discard(field)
    return fields


def _extract_object(text: str) -> dict[str, Any]:
    match = _JSON_FENCE.search(text)
    candidate = match.group(1) if match else text.strip()
    try:
        value = json.loads(candidate)
    except json.JSONDecodeError:
        start, end = candidate.find("{"), candidate.rfind("}")
        if start < 0 or end <= start:
            raise ValueError("enrichment response did not contain a JSON object")
        value = json.loads(candidate[start : end + 1])
    if not isinstance(value, dict):
        raise TypeError("enrichment response must be a JSON object")
    return value


def build_enrichment_update(
    response: str,
    *,
    explicit_fields: Collection[str],
    project_ids: Collection[str],
) -> CardUpdate:
    """Validate agent output and retain only safe, previously unset metadata."""
    payload = _extract_object(response)
    changes: dict[str, Any] = {}
    locked = set(explicit_fields)
    if "body" not in locked and isinstance(payload.get("description"), str):
        body = payload["description"].strip()
        if body:
            changes["body"] = body[:20_000]
    if "kind" not in locked:
        try:
            changes["kind"] = CardKind(str(payload.get("kind", "")).lower())
        except ValueError:
            pass
    if "project_id" not in locked:
        project_id = payload.get("project_id")
        if isinstance(project_id, str) and project_id in set(project_ids):
            changes["project_id"] = project_id
    for field in ("preferred_capabilities", "tags"):
        value = payload.get(field)
        if field not in locked and isinstance(value, list):
            cleaned = sorted(
                {str(item).strip() for item in value if str(item).strip()}
            )[:20]
            if cleaned:
                changes[field] = cleaned
    return CardUpdate(**changes)


async def enrich_card(
    ctx: Any, card_id: str, realm_id: str, explicit_fields: set[str]
) -> None:
    """Run one isolated ACP turn and apply its validated suggestions."""
    store = ctx.store
    settings = ctx.settings
    card = store.get_card(card_id, realm_id=realm_id)
    if not card:
        return
    projects = store.list_projects(realm_id=card.realm_id)
    project_catalog = [
        {"id": project.id, "title": project.title, "description": project.description}
        for project in projects
    ]
    card_context = {
        "title": card.title,
        "body": card.body,
        "kind": card.kind,
        "project_id": card.project_id,
        "preferred_capabilities": card.preferred_capabilities,
        "tags": card.tags,
    }
    prompt = (
        "Enrich this newly created PA card. Make conservative, useful guesses. "
        "Return only one JSON object with keys description, kind, project_id, "
        "preferred_capabilities, and tags. kind must be goal, task, project, or "
        "concern. project_id must be null or an id from the supplied catalog. "
        "Capabilities and tags must be short string arrays. Do not use tools.\n\n"
        f"Card: {json.dumps(card_context, default=str)}\n"
        f"Projects: {json.dumps(project_catalog)}\n"
        f"Fields that must not change: {json.dumps(sorted(explicit_fields))}"
    )
    manager = ctx.require_service("instance_agent")
    runtime = None
    try:
        runtime = await manager.create_session(
            label=f"card-enrichment:{card.id}",
            title=f"Enrich: {card.title[:80]}",
            principal_id=card.created_by_principal,
        )
        await runtime.prompt(prompt, wait=True)
        final_text = assemble_final_assistant_message(runtime._turn_agent_events)
        current = store.get_card(card.id, realm_id=card.realm_id)
        if not current:
            return
        protected = set(explicit_fields)
        for field in ("body", "project_id", "preferred_capabilities", "tags"):
            if getattr(current, field) != getattr(card, field):
                protected.add(field)
        if current.kind != card.kind:
            protected.add("kind")
        update = build_enrichment_update(
            final_text,
            explicit_fields=protected,
            project_ids=[project.id for project in projects],
        )
        if update.model_fields_set:
            store.update_card(
                current.id,
                update,
                realm_id=current.realm_id,
                principal_id=current.created_by_principal or "user:local",
                instance_id=settings.instance_id,
            )
    except Exception:
        logger.exception("Card auto-enrichment failed for %s", card_id)
    finally:
        if runtime is not None:
            try:
                await runtime.close(reason="card_enrichment_complete")
            except Exception:
                logger.exception("Could not close card enrichment session %s", card_id)
