"""Translate adapter evidence into scoped candidates without inventing capability."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

from pa.acp.configuration import (
    ACPConfigurationError,
    advertised_state_values,
    find_option,
    option_current_value,
    option_id,
    option_values,
    parse_model_selector,
    state_current_value,
)
from pa.execution.selection import (
    RESERVED_OPTIONS,
    ExecutionCandidate,
    NativeOption,
    digest,
)


def native_option(option: dict | None) -> NativeOption:
    if option is None:
        return NativeOption()
    values = sorted(option_values(option), key=str)
    if option.get("type") == "boolean":
        values = [False, True]
    mutability = (option.get("_meta") or {}).get("pa.mutableBetweenTurns")
    return NativeOption(
        support="supported" if values else "unknown",
        values=values,
        default=option_current_value(option),
        mutable_between_turns=mutability if isinstance(mutability, bool) else None,
    )


def candidates_from_advertisement(
    *,
    instance_id: str,
    harness: str,
    advertisement: dict[str, Any],
    readiness: str,
    observed_at: datetime,
    source: str,
    freshness: str = "fresh",
    connection: str = "default",
    model_provider: str | None = None,
    health_evidence: list[str] = (),
) -> list[ExecutionCandidate]:
    options = [
        o
        for o in (
            advertisement.get("config_options") or advertisement.get("options") or []
        )
        if isinstance(o, dict)
    ]
    models = advertisement.get("models")
    if isinstance(models, list):
        models = {
            "availableModels": [
                {"modelId": m} if isinstance(m, str) else m for m in models
            ]
        }
    models = models or {}
    ids = advertised_state_values(
        models,
        collection_names=("availableModels", "available_models"),
        id_names=("modelId", "model_id", "id"),
    )
    try:
        model_option = find_option(options, "model")
        effort_option = find_option(options, "reasoning")
    except ACPConfigurationError:
        model_option = effort_option = None
    ids.update(v for v in option_values(model_option or {}) if isinstance(v, str))
    current = option_current_value(model_option or {}) or state_current_value(
        models, ("currentModelId", "current_model_id")
    )
    current_model, current_effort = parse_model_selector(
        str(current) if current else None
    )
    efforts: dict[str, set[str]] = {}
    model_ids = set()
    for value in sorted(ids):
        model, effort = parse_model_selector(value)
        if model:
            model_ids.add(model)
            if effort:
                efforts.setdefault(model, set()).add(effort)
    version = digest(
        {
            "models": models,
            "options": options,
            "source": source,
            "capabilities": advertisement.get("execution_capabilities"),
        }
    )
    candidates = []
    # A missing catalog is represented as unknown; it never proves an explicit model.
    for model in sorted(model_ids) or [None]:
        native = {}
        for option in options:
            key = option_id(option)
            normalized = re.sub(r"[^a-z0-9]", "", (key or "").lower())
            category = str(option.get("category") or "").lower()
            if (
                not key
                or normalized in RESERVED_OPTIONS
                or category
                in {"mode", "permission", "permissions", "sandbox", "collaboration"}
                or re.search(
                    r"secret|token|password|credential|api.?key|authorization",
                    key,
                    re.IGNORECASE,
                )
            ):
                continue
            # Generic live options are evidence only for the current model.
            if model == current_model:
                native[key] = native_option(option)
        reasoning = NativeOption()
        if model in efforts:
            reasoning = NativeOption(
                support="supported",
                values=sorted(efforts[model]),
                default=current_effort if model == current_model else None,
            )
        elif model == current_model:
            reasoning = native_option(effort_option)
        # Optional adapter-normalized capability evidence is explicitly versioned
        # and model-scoped. Never infer context, tools, or modalities from names.
        capability_envelope = advertisement.get("execution_capabilities") or {}
        capabilities = (
            (capability_envelope.get("models") or {}).get(model, {})
            if capability_envelope.get("version") == 1
            else {}
        )

        def string_list(name, capabilities=capabilities):
            value = capabilities.get(name)
            return (
                value
                if isinstance(value, list) and all(isinstance(v, str) for v in value)
                else None
            )

        context = capabilities.get("context_tokens")
        version_id = capabilities.get("model_version")
        candidates.append(
            ExecutionCandidate(
                instance_id=instance_id,
                harness=harness,
                connection=connection,
                connection_revision=advertisement.get("connection_revision"),
                native_model_provider=advertisement.get("native_model_provider"),
                account_label=advertisement.get("account_label"),
                endpoint_label=advertisement.get("endpoint_label"),
                model_provider=model_provider or advertisement.get("model_provider"),
                model=model,
                model_state="known" if model else "provider_default",
                model_version=version_id if isinstance(version_id, str) else None,
                tools=string_list("tools"),
                modalities=string_list("modalities"),
                context_tokens=context
                if isinstance(context, int)
                and not isinstance(context, bool)
                and context >= 0
                else None,
                reasoning=reasoning,
                options=native,
                readiness=readiness,
                default=model == current_model,
                catalog_source=source,
                catalog_version=version,
                observed_at=observed_at,
                freshness=freshness,
                health_evidence=list(health_evidence),
            )
        )
    return candidates


def enrich_provider_catalogs(
    statuses: list[dict], manager: Any | None, data_dir=None
) -> list[dict]:
    """Attach already-held ACP evidence to fleet discovery; never probe here."""
    try:
        runtimes = list(manager.list_runtimes()) if manager is not None else []
    except AttributeError, TypeError:
        runtimes = []
    from pa.execution.selection import SelectionError
    from pa.execution.selection_connections import connection_revision
    from pa.execution.selection_store import SelectionStore

    store = SelectionStore(data_dir) if data_dir is not None else None
    revisions = {}
    for profile in store.connections() if store else []:
        if profile.enabled:
            try:
                revisions[profile.id] = connection_revision(profile, data_dir)
            except SelectionError:
                pass
    result = []
    for status in statuses:
        record = dict(status)
        catalogs = []
        for runtime in runtimes:
            session = getattr(runtime, "session", None)
            if (
                not session
                or session.agent_name != status.get("id")
                or getattr(runtime, "_closed", False)
                or not getattr(runtime, "connected", False)
            ):
                continue
            config = getattr(session, "config_json", None) or {}
            conn = getattr(runtime, "connection", None)
            effective = (config.get("configuration") or {}).get("effective") or {}
            selected = (config.get("execution_selection") or {}).get("selected") or {}
            if selected.get("connection_revision") and selected[
                "connection_revision"
            ] != revisions.get(selected.get("connection")):
                # A live old worker is not evidence for a newly rotated account.
                continue
            catalogs.append(
                {
                    "connection": selected.get("connection", "default"),
                    "connection_revision": selected.get("connection_revision"),
                    "native_model_provider": selected.get("native_model_provider"),
                    "model_provider": selected.get("model_provider")
                    or effective.get("model_provider"),
                    "models": getattr(conn, "models", None) or config.get("models"),
                    "config_options": getattr(conn, "config_options", None)
                    or config.get("options"),
                    "source": "live_acp_session",
                    "observed_at": datetime.now(UTC).isoformat(),
                }
            )
        if catalogs:
            record["execution_catalogs"] = catalogs
        if store is not None:
            cached = []
            for profile in store.connections():
                if profile.harness != status.get("id") or not profile.enabled:
                    continue
                rows, stamp = store.catalog("connection:" + profile.id)
                for row in rows:
                    if row.get("connection_revision") != revisions.get(profile.id):
                        continue
                    if not stamp or (datetime.now(UTC) - stamp).total_seconds() > 300:
                        row = {**row, "freshness": "stale"}
                    cached.append(row)
            record["execution_candidates"] = cached
        result.append(record)
    return result
