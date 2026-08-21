"""Asynchronous ACP-backed enrichment for newly created cards."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Collection
from typing import Any
from uuid import uuid4

from pa.acp.final_message import assemble_final_assistant_message
from pa.domain.models import CardKind, CardUpdate

logger = logging.getLogger(__name__)

_JSON_FENCE = re.compile(
    r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL | re.IGNORECASE
)
_ENRICHABLE = {"body", "kind", "project_id", "preferred_capabilities", "tags"}


def advertised_capability_catalog(ctx: Any) -> frozenset[str]:
    """Return the capability vocabulary currently advertised by this fleet."""
    catalog: set[str] = set()

    def _add(values: Any) -> None:
        for item in values or []:
            text = str(item).strip()
            if text:
                catalog.add(text)

    _add(getattr(getattr(ctx, "settings", None), "capabilities", None))
    services = getattr(ctx, "services", None)
    fleet = None
    if isinstance(services, dict):
        fleet = services.get("fleet_registry")
    elif services is not None:
        getter = getattr(services, "get", None)
        fleet = getter("fleet_registry") if callable(getter) else None
    if fleet is not None:
        for instance in fleet.list_instances():
            _add(getattr(instance, "capabilities", None))
    return frozenset(catalog)


async def _close_enrichment_session(
    manager: Any,
    session_id: str,
    runtime: Any | None,
) -> None:
    """Close a one-shot enrichment even when startup failed before returning it."""
    needs_reconciliation = False
    if runtime is not None:
        try:
            needs_reconciliation = await runtime.close(
                reason="card_enrichment_complete",
                reconcile_workspace=False,
            )
        except Exception:
            logger.exception("Could not close card enrichment runtime %s", session_id)

    persisted = await manager._offload(
        "card_enrichment.session_read",
        manager.store.get_session,
        session_id,
    )
    if persisted is not None and persisted.status != "closed":
        closed, prior_status = await manager._offload(
            "card_enrichment.session_close",
            manager.store.close_session,
            session_id,
            reason="card_enrichment_complete",
        )
        needs_reconciliation = bool(closed is not None and prior_status is not None)

    runtimes = getattr(manager, "_runtimes", None)
    if isinstance(runtimes, dict):
        runtimes.pop(session_id, None)
    if needs_reconciliation:
        await manager.reconcile_closed_sessions([session_id])


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
    advertised_capabilities: Collection[str] | None = None,
) -> CardUpdate:
    """Validate agent output and retain only safe, previously unset metadata."""
    payload = _extract_object(response)
    changes: dict[str, Any] = {}
    locked = set(explicit_fields)
    catalog = {str(item).strip() for item in advertised_capabilities or [] if str(item).strip()}
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
    if "preferred_capabilities" not in locked:
        value = payload.get("preferred_capabilities")
        if isinstance(value, list):
            cleaned = sorted(
                {
                    str(item).strip()
                    for item in value
                    if str(item).strip() and str(item).strip() in catalog
                }
            )[:20]
            if cleaned:
                changes["preferred_capabilities"] = cleaned
    if "tags" not in locked:
        value = payload.get("tags")
        if isinstance(value, list):
            cleaned = sorted(
                {str(item).strip() for item in value if str(item).strip()}
            )[:20]
            if cleaned:
                changes["tags"] = cleaned
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
    capability_catalog = sorted(advertised_capability_catalog(ctx))
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
        "preferred_capabilities must be a subset of the advertised capability "
        "catalog; if the catalog is empty or none apply, return []. Do not invent "
        "product-sounding labels. Tags must be short strings. Do not use tools.\n\n"
        f"Card: {json.dumps(card_context, default=str)}\n"
        f"Projects: {json.dumps(project_catalog)}\n"
        f"Advertised capabilities: {json.dumps(capability_catalog)}\n"
        f"Fields that must not change: {json.dumps(sorted(explicit_fields))}"
    )
    manager = ctx.require_service("instance_agent")
    session_id = str(uuid4())
    runtime = None
    try:
        runtime = await manager.create_session(
            session_id=session_id,
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
            advertised_capabilities=capability_catalog,
        )
        if update.model_fields_set:
            store.update_card(
                current.id,
                update,
                realm_id=current.realm_id,
                principal_id=current.created_by_principal or "user:local",
                instance_id=settings.instance_id,
            )
    except Exception as exc:
        from pa.acp.errors import classify_acp_failure, format_acp_error

        classified = classify_acp_failure(
            exc,
            provider_id=getattr(settings, "agent_provider", None),
            stage="card_enrichment",
        )
        logger.exception(
            "Card auto-enrichment failed for %s (%s): %s",
            card_id,
            classified.get("code"),
            format_acp_error(exc),
        )
        if runtime is not None and getattr(runtime, "session", None) is not None:
            try:
                config = dict(runtime.session.config_json or {})
                diagnostics = dict(config.get("diagnostics") or {})
                diagnostics["enrichment_failure"] = classified
                config["diagnostics"] = diagnostics
                runtime.session.config_json = config
                save = getattr(
                    runtime, "_save_session_preserving_external_browser_async", None
                )
                if callable(save):
                    await save()
            except Exception:
                logger.exception(
                    "Could not persist enrichment diagnostics for session %s",
                    getattr(runtime, "session_id", None),
                )
    finally:
        try:
            await _close_enrichment_session(manager, session_id, runtime)
        except Exception:
            logger.exception("Could not close card enrichment session %s", card_id)
