"""Selection binding for fixed-transport, tool-free derived jobs (card summaries)."""

from __future__ import annotations

from datetime import UTC, datetime

from pa.execution.selection import (
    ExecutionCandidate,
    SelectionError,
    TaskAssessment,
    digest,
    validate_reuse,
)
from pa.execution.selection_service import service_for


def summary_selection(
    ctx,
    card,
    configuration,
    *,
    input_hash: str,
    prompt_version: str,
    force=False,
    surface: str = "card_summary",
):
    service = service_for(ctx)
    principal = card.created_by_principal or "user:local"
    binding = digest(
        [card.realm_id, principal, surface, card.id, input_hash, prompt_version]
    )
    prior = service.store.binding(binding, card.realm_id, principal)
    routing = digest(
        [
            configuration.provider,
            configuration.base_url,
            configuration.auth_source,
            digest(configuration.api_key),
        ]
    )
    if prior and not force:
        validate_reuse(prior)
        service.revalidate_attempt(
            prior, realm=card.realm_id, principal=principal, surface=surface
        )
        if prior["selected"]["connection"] != routing:
            raise SelectionError(
                "job_routing_changed",
                "Summary retry is pinned to its original connection. Explicit regeneration is required after routing changes.",
            )
        return prior
    candidate = ExecutionCandidate(
        instance_id=ctx.settings.instance_id,
        harness="pa-summary-http",
        connection=routing,
        model_provider=configuration.provider,
        model=configuration.model,
        model_state="unknown",
        readiness="unknown",
        can_attempt_unverified=configuration.enabled,
        default=True,
        health_evidence=["configuration_only; provider health is unverified"],
        tools=[],
        modalities=["text"],
        catalog_source="explicit_summary_transport_configuration",
        catalog_version=digest([routing, configuration.model, prompt_version]),
        observed_at=datetime.now(UTC),
        freshness="fresh",
    )
    project = (
        ctx.store.get_project(card.project_id, realm_id=card.realm_id)
        if card.project_id
        else None
    )
    receipt = service.resolve(
        candidates=[candidate],
        principal=principal,
        realm=card.realm_id,
        surface=surface,
        card=card,
        project_config=project.tool_config if project else None,
        assessment=TaskAssessment(
            role=surface, complexity="routine", scope="focused", objective="cost"
        ),
    )
    receipt["prompt_identity"] = {
        "input_hash": input_hash,
        "prompt_version": prompt_version,
        "binding": binding,
    }
    receipt["decision_id"] = digest(
        {k: v for k, v in receipt.items() if k != "decision_id"}
    )
    service.store.save_decision(receipt, card.realm_id, principal)
    service.store.bind(binding, receipt["decision_id"], replace=force)
    return service.store.binding(binding, card.realm_id, principal)
