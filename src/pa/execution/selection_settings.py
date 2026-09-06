"""Durable explicit native-setting changes, applied only at a safe turn boundary."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

from pa.acp.configuration import SessionConfigurationRequest
from pa.execution.selection import (
    ExecutionPreferences,
    Preference,
    SelectionConstraints,
    SelectionError,
    TaskAssessment,
    digest,
)
from pa.execution.selection_catalog import candidates_from_advertisement
from pa.execution.selection_service import SelectionService, selected_configuration


def live_candidates(runtime):
    selected = (
        (runtime.session.config_json or {}).get("execution_selection") or {}
    ).get("selected") or {}
    return candidates_from_advertisement(
        instance_id=runtime.settings.instance_id,
        harness=runtime.session.agent_name,
        connection=selected.get("connection", "default"),
        model_provider=selected.get("model_provider"),
        advertisement={
            "models": runtime.connection.models,
            "config_options": runtime.connection.config_options,
            "connection_revision": selected.get("connection_revision"),
            "native_model_provider": selected.get("native_model_provider"),
        },
        readiness="ready" if runtime.connected else "unavailable",
        observed_at=datetime.now(UTC),
        source="live_acp_session",
    )


async def request_settings(
    runtime,
    preferences: ExecutionPreferences,
    *,
    principal: str,
    key: str,
    expected_version: datetime,
    defer: bool,
):
    lock = getattr(runtime, "_selection_settings_lock", None)
    if lock is None:
        lock = runtime._selection_settings_lock = asyncio.Lock()
    async with lock:
        return await _request_settings(
            runtime,
            preferences,
            principal=principal,
            key=key,
            expected_version=expected_version,
            defer=defer,
        )


async def _request_settings(
    runtime,
    preferences: ExecutionPreferences,
    *,
    principal: str,
    key: str,
    expected_version: datetime,
    defer: bool,
):
    config = runtime.session.config_json or {}
    if getattr(runtime, "_execution_boundary_reserved", False) or config.get(
        "execution_context_boundary"
    ):
        raise SelectionError(
            "context_boundary_fenced",
            "This source has a linked attempt; change that target's settings instead",
        )
    operation = {
        "preferences": preferences.model_dump(mode="json"),
        "principal": principal,
    }
    fingerprint = digest(operation)
    pending = config.get("execution_pending_settings")
    history = config.get("execution_settings_requests") or []
    prior = next((p for p in [pending, *history] if p and p["id"] == key), None)
    if prior:
        if prior["fingerprint"] != fingerprint:
            raise SelectionError(
                "settings_idempotency_conflict",
                "This settings key belongs to another request.",
            )
        return prior
    if runtime.session.updated_at != expected_version:
        raise SelectionError(
            "stale_session_settings",
            "Session changed. Refresh its version before changing settings.",
        )
    if pending and pending["state"] == "pending":
        raise SelectionError(
            "settings_change_pending",
            "A settings change is pending. Inspect or cancel it before replacing it.",
        )
    if runtime.prompting and not defer:
        raise SelectionError(
            "turn_boundary_required",
            "This turn is active. Explicitly defer supported settings until the next turn.",
        )
    original = config.get("execution_selection") or {}
    selected = original.get("selected") or {}
    for field, current in (
        ("harness", runtime.session.agent_name),
        ("connection", selected.get("connection", "default")),
        ("model_provider", selected.get("model_provider")),
    ):
        pref = getattr(preferences, field)
        if pref.value is not None and pref.value != current:
            raise SelectionError(
                "context_boundary_required",
                "Harness/backend/account changes require a linked new attempt and explicit context boundary; the existing runtime is not replaced.",
            )
    # A partial setting request holds unspecified native values. Explicit Auto
    # still invokes policy for that field; it is not the same as leaving it out.
    preferences = preferences.model_copy(deep=True)
    from pa.execution.selection import FIELDS

    for field in FIELDS:
        current = selected.get(field)
        if current is not None and getattr(preferences, field).intent == "inherit":
            setattr(preferences, field, Preference(intent="required", value=current))
    for field, value in selected.get("options", {}).items():
        if (
            field not in preferences.options
            or preferences.options[field].intent == "inherit"
        ):
            preferences.options[field] = Preference(intent="required", value=value)
    service = getattr(runtime.manager, "_selection_service", None) or SelectionService(
        runtime.settings, runtime.store, runtime.manager
    )
    owner = runtime.session.principal_id or "user:local"
    surface = original.get("context", {}).get("surface", "execution")
    if original:
        await runtime._offload(
            "selection.settings_authority",
            service.revalidate_attempt,
            original,
            realm=runtime.session.realm_id,
            principal=owner,
            surface=surface,
        )
    current_constraints = [
        SelectionConstraints.model_validate(c) for c in original.get("constraints", [])
    ]
    candidates = live_candidates(runtime)
    receipt = await runtime._offload(
        "selection.settings_resolve",
        service.resolve,
        candidates=candidates,
        principal=owner,
        realm=runtime.session.realm_id,
        surface=surface,
        overrides=preferences,
        constraints=current_constraints,
        assessment=TaskAssessment.model_validate(original.get("assessment") or {}),
    )
    target = next(c for c in candidates if c.key == receipt["candidate_key"])
    for name, value in [
        ("reasoning", receipt["selected"].get("reasoning")),
        *receipt["selected"].get("options", {}).items(),
    ]:
        option = target.reasoning if name == "reasoning" else target.options.get(name)
        previous = (
            selected.get("reasoning")
            if name == "reasoning"
            else selected.get("options", {}).get(name)
        )
        if value != previous and option and option.mutable_between_turns is False:
            raise SelectionError(
                "context_boundary_required",
                "The provider marks this native option immutable in-session. Use a linked new attempt.",
            )
    # A setting change retains task/card/principal ownership. An administrator's
    # request must not replace the worker's defaults or drop current card policy.
    receipt["context"] = {**receipt["context"], **original.get("context", {})}
    receipt["settings_change"] = {
        "requested_by": principal,
        "previous_decision_id": original.get("decision_id"),
        "id": key,
    }
    receipt["decision_id"] = digest(
        {k: v for k, v in receipt.items() if k != "decision_id"}
    )
    await runtime._offload(
        "selection.settings_receipt",
        service.store.save_decision,
        receipt,
        runtime.session.realm_id,
        owner,
    )
    pending = {
        "id": key,
        "fingerprint": fingerprint,
        "principal": principal,
        "state": "pending",
        "decision": receipt,
        "previous_decision_id": original.get("decision_id"),
        "requested_at": datetime.now(UTC).isoformat(),
    }
    runtime.session.config_json = {**config, "execution_pending_settings": pending}
    runtime.session.updated_at = datetime.now(UTC)
    await runtime._offload(
        "selection.settings_pending", runtime.store.save_session, runtime.session
    )
    if not runtime.prompting:
        async with runtime._prompt_lock:
            await apply_pending(runtime)
    return (runtime.session.config_json or {}).get(
        "execution_pending_settings"
    ) or pending


async def apply_pending(runtime):
    """Caller holds the prompt lock. Failure blocks the original queued prompt."""
    config = runtime.session.config_json or {}
    pending = config.get("execution_pending_settings")
    if pending and pending["state"] == "failed":
        raise SelectionError(
            "settings_confirmation_failed",
            "The deferred setting change failed. Correct the requested settings before continuing this prompt.",
        )
    if not pending or pending["state"] != "pending":
        return
    service = getattr(runtime.manager, "_selection_service", None) or SelectionService(
        runtime.settings, runtime.store, runtime.manager
    )
    try:
        receipt = pending["decision"]
        selected = receipt["selected"]
        await runtime._offload(
            "selection.settings_boundary_authority",
            service.revalidate_attempt,
            receipt,
            realm=runtime.session.realm_id,
            principal=runtime.session.principal_id or "user:local",
            surface=receipt.get("context", {}).get("surface", "execution"),
        )
        prefs = ExecutionPreferences(
            **{
                k: Preference(intent="required", value=selected[k])
                for k in (
                    "harness",
                    "connection",
                    "model_provider",
                    "model",
                    "reasoning",
                )
                if selected.get(k) is not None
            },
            options={
                k: Preference(intent="required", value=v)
                for k, v in selected.get("options", {}).items()
            },
        )
        # Revalidate current policy and live capability against the persisted
        # target. Never reselect to a different candidate at the boundary.
        await runtime._offload(
            "selection.settings_boundary_resolve",
            service.resolve,
            candidates=live_candidates(runtime),
            principal=runtime.session.principal_id or "user:local",
            realm=runtime.session.realm_id,
            surface=receipt.get("context", {}).get("surface", "execution"),
            overrides=prefs,
            constraints=[
                SelectionConstraints.model_validate(c)
                for c in receipt.get("constraints", [])
            ],
        )
        request = selected_configuration(
            receipt, SessionConfigurationRequest(mode_id=runtime.session.mode_id)
        )
        await runtime.connection.configure(request, merge=False, force=True)
        runtime.session = runtime.connection.session or runtime.session
        config = dict(runtime.session.config_json or {})
        previous = config.get("execution_selection")
        config.setdefault("execution_selection_origin", previous)
        config["execution_selection_history"] = [
            *config.get("execution_selection_history", []),
            previous,
        ][-30:]
        config["execution_selection"] = receipt
        pending = {
            **pending,
            "state": "applied",
            "applied_at": datetime.now(UTC).isoformat(),
        }
        config["execution_pending_settings"] = pending
        config["execution_settings_requests"] = [
            *config.get("execution_settings_requests", []),
            pending,
        ][-30:]
        runtime.session.config_json = config
        from pa.execution.selection_audit import bind_confirmed_defaults

        bind_confirmed_defaults(runtime.session)
        await runtime._offload(
            "selection.settings_applied", runtime.store.save_session, runtime.session
        )
    except Exception as exc:
        config = dict(runtime.session.config_json or {})
        pending = {
            **pending,
            "state": "failed",
            "failed_at": datetime.now(UTC).isoformat(),
            "error": "Native settings could not be confirmed. Inspect configuration/selection diagnostics before retrying.",
        }
        from pa.execution.selection_interactions import settings_blocked

        config["execution_pending_settings"] = pending
        config["execution_settings_requests"] = [
            *config.get("execution_settings_requests", []),
            pending,
        ][-30:]
        runtime.session.config_json = config
        await runtime._offload(
            "selection.settings_failed", runtime.store.save_session, runtime.session
        )
        # Failure is durable before notification delivery. A notification outage
        # must never permit the queued prompt or erase partial native state.
        try:
            pending["notification_id"] = await settings_blocked(runtime, pending)
            await runtime._offload(
                "selection.settings_notice", runtime.store.save_session, runtime.session
            )
        except Exception as delivery_error:  # noqa: BLE001 - settings failure is already durable
            logging.getLogger(__name__).warning(
                "Selection settings notification delivery failed (%s); durable failure remains visible",
                type(delivery_error).__name__,
            )
        raise SelectionError("settings_confirmation_failed", pending["error"]) from exc


async def cancel_settings(
    runtime, *, key: str, expected_version: datetime, principal: str
):
    lock = getattr(runtime, "_selection_settings_lock", None)
    if lock is None:
        lock = runtime._selection_settings_lock = asyncio.Lock()
    async with lock, runtime._prompt_lock:
        config = dict(runtime.session.config_json or {})
        pending = config.get("execution_pending_settings")
        if not pending or pending["id"] != key:
            raise SelectionError(
                "settings_request_not_found",
                "The correlated settings request is no longer current",
            )
        if pending["state"] == "cancelled":
            return pending
        if (
            runtime.session.updated_at != expected_version
            or pending["state"] == "applied"
        ):
            raise SelectionError(
                "stale_session_settings",
                "Refresh the session before cancelling; applied settings need a new request",
            )
        # Cancellation does not undo partial native application after a failure.
        # Explicit correction is needed when native confirmation is not ready.
        if pending["state"] == "failed":
            raise SelectionError(
                "settings_confirmation_failed",
                "A failed provider change may have partially applied. Correct settings with a fresh verified request; cancellation cannot invent a rollback.",
            )
        pending = {
            **pending,
            "state": "cancelled",
            "cancelled_at": datetime.now(UTC).isoformat(),
            "cancelled_by": principal,
        }
        config["execution_pending_settings"] = pending
        config["execution_settings_requests"] = [
            *config.get("execution_settings_requests", []),
            pending,
        ][-30:]
        runtime.session.config_json = config
        runtime.session.updated_at = datetime.now(UTC)
        await runtime._offload(
            "selection.settings_cancelled", runtime.store.save_session, runtime.session
        )
        return pending
