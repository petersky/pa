from __future__ import annotations

from collections.abc import Iterable

from pa.collaboration.models import (
    CollaborationMode,
    CollaborationPolicy,
    PolicyDecision,
    PolicyInput,
    PolicyScope,
    PolicyStrategy,
)

_SCOPE_RANK = {
    PolicyScope.FLEET: 10,
    PolicyScope.PROVIDER: 20,
    PolicyScope.INSTANCE: 30,
    PolicyScope.REALM: 40,
    PolicyScope.PROJECT: 50,
}

# Mandatory organization constraints must not be weakened by a narrower default.
_MANDATORY_SCOPE_RANK = {
    PolicyScope.FLEET: 50,
    PolicyScope.REALM: 40,
    PolicyScope.PROJECT: 30,
    PolicyScope.INSTANCE: 20,
    PolicyScope.PROVIDER: 10,
}


def _matches(policy: CollaborationPolicy, value: PolicyInput) -> bool:
    if not policy.enabled:
        return False
    if policy.provider and policy.provider != value.provider:
        return False
    if policy.scope_type == PolicyScope.FLEET:
        return True
    if policy.scope_type == PolicyScope.REALM:
        return policy.scope_id == value.realm_id
    if policy.scope_type == PolicyScope.PROJECT:
        return bool(value.project_id and policy.scope_id == value.project_id)
    if policy.scope_type == PolicyScope.INSTANCE:
        return policy.scope_id == value.instance_id
    if policy.scope_type == PolicyScope.PROVIDER:
        return policy.scope_id == value.provider
    return False


def applicable_policies(
    policies: Iterable[CollaborationPolicy], value: PolicyInput
) -> list[CollaborationPolicy]:
    return sorted(
        (policy for policy in policies if _matches(policy, value)),
        key=lambda policy: (
            _SCOPE_RANK[policy.scope_type],
            policy.version,
            policy.updated_at,
            policy.id,
        ),
        reverse=True,
    )


def _policy_mode(
    policy: CollaborationPolicy, value: PolicyInput
) -> tuple[CollaborationMode, str]:
    if policy.strategy == PolicyStrategy.ALWAYS_PLAN:
        return CollaborationMode.PLAN, "The effective policy requires Plan-first."
    if policy.strategy == PolicyStrategy.ALWAYS_DEFAULT:
        return (
            CollaborationMode.DEFAULT,
            "The effective policy starts sessions in Default mode.",
        )

    signals: list[str] = []
    if value.card_kind and value.card_kind in policy.plan_first_card_kinds:
        signals.append(f"card kind {value.card_kind!r}")
    matching_tags = sorted(set(value.card_tags) & set(policy.plan_first_tags))
    if matching_tags:
        signals.append(f"card tags {matching_tags!r}")
    matching_caps = sorted(
        set(value.capabilities) & set(policy.plan_first_capabilities)
    )
    if matching_caps:
        signals.append(f"capabilities {matching_caps!r}")
    if value.dispatch_intent in policy.plan_first_intents:
        signals.append(f"dispatch intent {value.dispatch_intent!r}")
    if policy.strategy == PolicyStrategy.AUTOMATIC:
        if value.risk in policy.automatic_risk_levels:
            signals.append(f"risk {value.risk!r}")
        if value.ambiguous and policy.automatic_on_ambiguity:
            signals.append("explicit ambiguity signal")
    if signals:
        return CollaborationMode.PLAN, "Plan-first selected from " + ", ".join(
            signals
        ) + "."
    return CollaborationMode.DEFAULT, "No deterministic Plan-first signal matched."


def decide_initial_mode(
    value: PolicyInput,
    policies: Iterable[CollaborationPolicy],
) -> PolicyDecision:
    matched = applicable_policies(policies, value)
    mandatory = max(
        (p for p in matched if p.mandatory_mode is not None),
        key=lambda policy: (
            _MANDATORY_SCOPE_RANK[policy.scope_type],
            policy.version,
            policy.updated_at,
            policy.id,
        ),
        default=None,
    )
    selected_policy = matched[0] if matched else None

    requested = value.dispatch_override or value.user_preference
    if mandatory is not None:
        mode = mandatory.mandatory_mode or CollaborationMode.DEFAULT
        source = f"mandatory:{mandatory.scope_type.value}:{mandatory.scope_id}"
        rationale = (
            f"Mandatory policy {mandatory.id} selected {mode.value}; it overrides "
            "user preference and automatic policy."
        )
        policy = mandatory
        is_mandatory = True
    elif requested is not None:
        mode = requested
        source = (
            "explicit_dispatch_override"
            if value.dispatch_override
            else "explicit_user_preference"
        )
        rationale = f"Explicit user selection requested {mode.value}."
        policy = selected_policy
        is_mandatory = False
    elif selected_policy is not None:
        mode, rationale = _policy_mode(selected_policy, value)
        source = f"policy:{selected_policy.scope_type.value}:{selected_policy.scope_id}"
        policy = selected_policy
        is_mandatory = False
    else:
        mode = CollaborationMode.DEFAULT
        source = "deterministic_fallback"
        rationale = "No collaboration policy matched; PA used the backward-compatible Default mode."
        policy = None
        is_mandatory = False

    supported = list(value.supported_modes)
    if supported and mode not in supported:
        rationale += (
            f" Provider {value.provider!r} does not advertise {mode.value}; "
            "PA fell back to Default without changing execution authority."
        )
        mode = CollaborationMode.DEFAULT
        source += ":provider_fallback"

    inputs = value.model_dump(mode="json")
    inputs["matched_policy_ids"] = [item.id for item in matched]
    values = {
        "effective_mode": mode,
        "requested_mode": requested,
        "source": source,
        "source_policy_id": policy.id if policy else None,
        "source_policy_version": policy.version if policy else None,
        "mandatory": is_mandatory,
        "rationale": rationale,
        "inputs": inputs,
    }
    if policy is not None:
        values["lifecycle"] = policy.lifecycle
    return PolicyDecision(**values)


def transition_allowed(
    policy: CollaborationPolicy | None,
    current: CollaborationMode,
    requested: CollaborationMode,
) -> tuple[bool, str]:
    if requested == current:
        return True, "The requested collaboration mode is already effective."
    if policy is None:
        return (
            True,
            "No denying policy applies; the backward-compatible transition policy allows the request.",
        )
    if not policy.allow_agent_transitions:
        return (
            False,
            f"Policy {policy.id} denies agent-requested collaboration-mode transitions.",
        )
    if requested not in policy.allowed_modes:
        return False, f"Policy {policy.id} does not allow mode {requested.value!r}."
    edge = f"{current.value}:{requested.value}"
    if edge not in policy.allowed_transitions:
        return False, f"Policy {policy.id} does not allow transition {edge}."
    if policy.mandatory_mode is not None and requested != policy.mandatory_mode:
        return (
            False,
            f"Mandatory policy {policy.id} requires {policy.mandatory_mode.value}.",
        )
    return True, f"Policy {policy.id} allows transition {edge}."
