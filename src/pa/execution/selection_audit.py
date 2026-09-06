"""Bounded, redacted receipt/attempt evidence shared with confirmed presentation."""

from __future__ import annotations

import re
from contextvars import ContextVar
from datetime import UTC, datetime

from pa.execution.selection import OutcomeEvidence, selection_presentation
from pa.execution.selection_store import SelectionStore

summary_response: ContextVar[dict | None] = ContextVar("summary_response", default=None)


def bind_confirmed_defaults(session):
    """Bind previously unspecified defaults after readback, without rewriting intent."""
    from pa.acp.configuration import confirmed_session_configuration

    config = dict(session.config_json or {})
    receipt = config.get("execution_selection")
    if not receipt:
        return
    prior = config.get("execution_native_binding") or {}
    if prior.get("decision_id") == receipt["decision_id"]:
        return  # Reconnection cannot silently adopt newly changed provider defaults.
    confirmed = confirmed_session_configuration(config)
    config["execution_native_binding"] = {
        "decision_id": receipt["decision_id"],
        "model_id": confirmed["model_id"],
        "reasoning": confirmed["reasoning"],
        "observed_at": datetime.now(UTC).isoformat(),
        "source": "normalized_provider_readback",
    }
    session.config_json = config


def safe_evidence(value):
    if isinstance(value, dict):
        return {
            k: safe_evidence(v)
            for k, v in value.items()
            if not re.search(
                r"secret|password|credential|api.?key|authorization|access.?token|refresh.?token",
                str(k),
                re.IGNORECASE,
            )
            and not (re.search(r"token", str(k), re.IGNORECASE) and isinstance(v, str))
        }
    if isinstance(value, list):
        return [safe_evidence(v) for v in value[:100]]
    return value


def record_configuration(settings, session):
    receipt_config = dict(session.config_json or {})
    pending = receipt_config.get("execution_pending_settings")
    if pending and pending["state"] == "pending":
        receipt_config["execution_selection"] = pending["decision"]
    view = selection_presentation(
        receipt_config, model_id=session.model_id, mode_id=session.mode_id
    )
    if not view:
        return
    receipt = view["decision"]
    configuration = (session.config_json or {}).get("configuration") or {}
    store = SelectionStore(settings.data_dir)
    store.record_attempt(
        f"session:{session.id}:configuration:{configuration.get('attempt', 0)}",
        receipt["decision_id"],
        safe_evidence(
            {
                "session_id": session.id,
                "provider_confirmation": view["provider_confirmation"],
                "requested": receipt["selected"],
                "recorded_at": datetime.now(UTC).isoformat(),
            }
        ),
    )


def begin_prompt(runtime, item):
    from pa.execution.selection import SelectionError, digest

    receipt = (runtime.session.config_json or {}).get("execution_selection")
    if not receipt:
        return None
    view = selection_presentation(
        runtime.session.config_json,
        model_id=runtime.session.model_id,
        mode_id=runtime.session.mode_id,
    )
    confirmation = view["provider_confirmation"]
    mismatches = list(confirmation["mismatches"])
    effective_config = (
        (runtime.session.config_json.get("configuration") or {}).get("effective") or {}
    ).get("config") or {}
    effective_values = {
        **effective_config,
        **confirmation["effective"].get("values", {}),
    }
    mismatches.extend(
        "options." + name
        for name, value in receipt["selected"].get("options", {}).items()
        if effective_values.get(name) != value
    )
    if mismatches or confirmation["state"] in {"failed", "pending", "mismatch"}:
        raise SelectionError(
            "selection_native_drift",
            "The provider no longer confirms this attempt's persisted native selection. Inspect requested versus confirmed settings, submit a verified correction, then retry the same queued prompt.",
            {
                "decision_id": receipt["decision_id"],
                "mismatches": mismatches,
                "provider_confirmation": confirmation,
            },
        )
    service = runtime.manager._selection_service
    service.revalidate_attempt(
        receipt,
        realm=runtime.session.realm_id,
        principal=runtime.session.principal_id,
        surface=receipt.get("context", {}).get("surface", "execution"),
    )
    runtime.session.config_json.pop("execution_selection_block", None)
    images = [
        i.model_dump(mode="json") if hasattr(i, "model_dump") else i
        for i in (getattr(item, "images", None) or [])
    ]
    return service.store.begin_prompt(
        session_id=runtime.session.id,
        prompt_id=item.id,
        prompt_digest=digest({"message": item.message, "images": images}),
        receipt=receipt,
    )


def finish_prompt(
    runtime, attempt_id, *, completed: bool, latency_ms: float, stop_reason
):
    if not attempt_id:
        return
    receipt = runtime.session.config_json["execution_selection"]
    store = runtime.manager._selection_service.store
    previous = store.attempt(attempt_id, receipt["decision_id"]) or {}
    confirmation = selection_presentation(
        runtime.session.config_json,
        model_id=runtime.session.model_id,
        mode_id=runtime.session.mode_id,
    )
    payload = {
        **previous,
        "id": attempt_id,
        "state": "provider_returned" if completed else "provider_failed_or_cancelled",
        "validation_scope": "provider_protocol_only; task completion/tests/review are not inferred",
        "stop_reason": stop_reason,
        "latency_ms": latency_ms,
        "cost_usd": None,
        "provider_confirmation": confirmation["provider_confirmation"],
    }
    store.record_attempt(attempt_id, receipt["decision_id"], safe_evidence(payload))
    store.record_evidence(
        OutcomeEvidence(
            id=attempt_id,
            decision_id=receipt["decision_id"],
            candidate_key=receipt["candidate_key"],
            kind="provider_protocol",
            observed_at=datetime.now(UTC),
            validated=True,
            completed=completed,
            latency_ms=latency_ms,
            references=[attempt_id],
        ),
        runtime.session.realm_id,
        runtime.session.principal_id or "user:local",
    )


def observe_summary(payload):
    target = summary_response.get()
    if target is None or not isinstance(payload, dict):
        return
    model = payload.get("model")
    if isinstance(model, str) and len(model) <= 300:
        target["model_id"] = model
    usage = payload.get("usage")
    if isinstance(usage, dict):
        target["usage"] = {
            k: v
            for k, v in usage.items()
            if isinstance(v, (int, float)) and not isinstance(v, bool) and v >= 0
        }


def record_summary(
    ctx,
    card,
    receipt,
    *,
    attempt: int,
    completed: bool,
    latency_ms: float,
    confirmation: dict,
    attempted_at: datetime,
):
    store = SelectionStore(ctx.settings.data_dir)
    reference = f"card-summary:{card.id}:{receipt['decision_id']}:{attempt}:{attempted_at.isoformat()}"
    requested_model = receipt["selected"].get("model")
    reported_model = confirmation.get("model_id")
    model_matches = reported_model == requested_model if reported_model else None
    store.record_attempt(
        reference,
        receipt["decision_id"],
        {
            "reference": reference,
            "completed": completed,
            "validation_scope": "provider_response_schema_only; not repository/task completion or card application",
            "provider_confirmation": {
                "state": "confirmed"
                if model_matches
                else "reported_identity_differs"
                if reported_model
                else "unknown",
                "requested_model": requested_model,
                "requested_model_confirmed": model_matches,
                "effective": confirmation,
            },
            "latency_ms": latency_ms,
            "cost_usd": None,
            "observed_at": datetime.now(UTC).isoformat(),
        },
    )
    store.record_evidence(
        OutcomeEvidence(
            id=reference,
            decision_id=receipt["decision_id"],
            candidate_key=receipt["candidate_key"],
            kind="provider_protocol",
            observed_at=datetime.now(UTC),
            validated=True,
            completed=completed,
            latency_ms=latency_ms,
            references=[reference],
        ),
        card.realm_id,
        card.created_by_principal or "user:local",
    )
