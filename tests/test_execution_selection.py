from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from pa.execution.selection import (
    ExecutionCandidate,
    ExecutionPreferences,
    NativeOption,
    OutcomeEvidence,
    Preference,
    SelectionConstraints,
    SelectionError,
    SelectionPolicy,
    SelectionRule,
    TaskAssessment,
    assess_task,
    authorize_fallback,
    merge_preferences,
    resolve_selection,
    validate_reuse,
)
from pa.execution.selection_catalog import candidates_from_advertisement
from pa.execution.selection_store import SelectionStore

NOW = datetime(2026, 9, 5, tzinfo=UTC)


def test_native_values_and_costs_do_not_coerce_unknown_numeric_types():
    with pytest.raises(ValidationError):
        Preference(intent="required", value=1)
    with pytest.raises(ValidationError):
        candidate(estimated_cost_usd=float("inf"))


def test_model_scoped_capability_evidence_is_versioned_not_inferred():
    advertisement = {
        "models": ["same-model", "another-model"],
        "execution_capabilities": {
            "version": 1,
            "models": {
                "same-model": {
                    "tools": ["terminal"],
                    "modalities": ["text", "image"],
                    "context_tokens": 32000,
                    "model_version": "explicit-version",
                }
            },
        },
    }
    rows = candidates_from_advertisement(
        instance_id="local",
        harness="codex",
        connection="account-a",
        advertisement=advertisement,
        readiness="ready",
        observed_at=NOW,
        source="adapter_fixture",
    )
    result = resolve(
        rows,
        constraints=[
            SelectionConstraints(
                required_tools=["terminal"],
                modalities=["image"],
                min_context_tokens=20000,
            )
        ],
    )
    assert result["selected"]["model"] == "same-model"
    assert result["selected"]["model_version"] == "explicit-version"
    version = rows[0].catalog_version
    advertisement["execution_capabilities"]["version"] = 2
    unknown = candidates_from_advertisement(
        instance_id="local",
        harness="codex",
        connection="account-a",
        advertisement=advertisement,
        readiness="ready",
        observed_at=NOW,
        source="adapter_fixture",
    )
    assert unknown[0].catalog_version != version
    with pytest.raises(SelectionError):
        resolve(
            unknown, constraints=[SelectionConstraints(required_tools=["terminal"])]
        )


def candidate(**values):
    return ExecutionCandidate(
        **dict(
            {
                "instance_id": "local",
                "harness": "codex",
                "connection": "account-a",
                "model_provider": "openai",
                "model": "model-a",
                "model_state": "known",
                "reasoning": NativeOption(support="supported", values=["low", "xhigh"]),
                "readiness": "ready",
                "catalog_source": "fixture",
                "catalog_version": "1",
                "observed_at": NOW,
                "freshness": "fresh",
            },
            **values,
        )
    )


def pref(value, intent="required"):
    return Preference(intent=intent, value=value)


def resolve(candidates=None, prefs=None, **kwargs):
    return resolve_selection(
        candidates=candidates or [candidate()],
        layers=[("dispatch", prefs or ExecutionPreferences())],
        policy=kwargs.pop("policy", SelectionPolicy()),
        assessment=kwargs.pop("assessment", TaskAssessment()),
        now=NOW,
        **kwargs,
    )


def test_field_precedence_and_explicit_auto_terminate_inheritance():
    values, sources = merge_preferences(
        [
            ("dispatch", ExecutionPreferences(model=Preference(intent="automatic"))),
            ("card", ExecutionPreferences(model=pref("card"), reasoning=pref("low"))),
            ("project", ExecutionPreferences(harness=pref("cursor"))),
            (
                "user",
                ExecutionPreferences(
                    harness=pref("codex"), connection=pref("account-b")
                ),
            ),
        ]
    )
    assert values["model"] == {"intent": "automatic", "value": None}
    assert sources == {
        "harness": "project",
        "connection": "user",
        "model_provider": "automatic_policy",
        "model": "dispatch",
        "reasoning": "card",
    }


def test_prompt_identity_and_attempt_record_survive_more_than_one_hundred_turns(
    tmp_path,
):
    store = SelectionStore(tmp_path)
    receipt = resolve()
    for number in range(105):
        key = store.begin_prompt(
            session_id="s",
            prompt_id=str(number),
            prompt_digest="same-content",
            receipt=receipt,
        )
    assert store.attempt(key, receipt["decision_id"])["prompt_digest"] == "same-content"
    with pytest.raises(SelectionError, match="Retry content"):
        store.begin_prompt(
            session_id="s",
            prompt_id="104",
            prompt_digest="changed-content",
            receipt=receipt,
        )
    with pytest.raises(SelectionError, match="another selection"):
        store.record_attempt(key, "different-decision", {})


def test_fallback_reservation_is_idempotent_bounded_and_prompt_scoped(tmp_path):
    store = SelectionStore(tmp_path)
    receipt = resolve(
        policy=SelectionPolicy(
            fallback_max_attempts=1, fallback_connections=["account-a"]
        )
    )
    alternative = receipt["alternatives"][0]
    first = store.reserve_fallback(
        receipt, alternative, key="recovery", prompt_digest="original", limit=1
    )
    assert (
        store.reserve_fallback(
            receipt, alternative, key="recovery", prompt_digest="original", limit=1
        )
        == first
    )
    with pytest.raises(SelectionError, match="different prompt"):
        store.reserve_fallback(
            receipt, alternative, key="recovery", prompt_digest="changed", limit=1
        )
    with pytest.raises(SelectionError, match="exhausted"):
        store.reserve_fallback(
            receipt, alternative, key="more-cost", prompt_digest="original", limit=1
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("harness", "cursor"),
        ("connection", "account-b"),
        ("model", "missing"),
        ("reasoning", "high"),
    ],
)
def test_each_pin_is_respected_with_actionable_combination_failure(field, value):
    with pytest.raises(SelectionError) as error:
        resolve(prefs=ExecutionPreferences(**{field: pref(value)}))
    assert (
        f"required_{field}_incompatible_or_unknown"
        in error.value.receipt["alternatives"][0]["rejections"]
    )


def test_soft_preference_is_visibly_unmet_and_false_is_a_real_option_value():
    c = candidate(
        options={"fast": NativeOption(support="supported", values=[False, True])}
    )
    result = resolve(
        [c],
        ExecutionPreferences(
            model=pref("missing", "preferred"), options={"fast": pref(False)}
        ),
    )
    assert result["selected"]["options"] == {"fast": False}
    assert "preferred_model_unavailable" in result["tradeoffs"]
    assert result["provider_confirmation"]["state"] == "pending"


@pytest.mark.parametrize(
    "option", ["sandbox", "approval_policy", "mode", "collaboration_mode", "api_key"]
)
def test_native_options_cannot_smuggle_authority_or_secrets(option):
    with pytest.raises(ValidationError):
        ExecutionPreferences(options={option: pref("unrestricted")})


def test_permissions_routing_context_tools_modality_and_budget_are_hard_constraints():
    c = candidate(tools=["browser"], modalities=["text"], context_tokens=1000)
    constraints = SelectionConstraints(
        permission_eligible=False,
        ownership_eligible=False,
        connection_ids=["account-b"],
        required_tools=["terminal"],
        modalities=["image"],
        min_context_tokens=2000,
        max_cost_usd=5,
    )
    with pytest.raises(SelectionError) as error:
        resolve([c], constraints=[constraints])
    assert set(error.value.receipt["alternatives"][0]["rejections"]) == {
        "permission_constraint",
        "ownership_constraint",
        "constraint_connection_ids",
        "tools_unsupported_or_unknown",
        "modalities_unsupported_or_unknown",
        "context_unsupported_or_unknown",
        "cost_exceeds_budget_or_unknown",
    }


def test_scoped_models_native_efforts_and_account_boundaries():
    a = candidate()
    b = candidate(
        harness="cursor",
        connection="cursor-account",
        model_provider="cursor/grok",
        model="model-a",
        reasoning=NativeOption(support="supported", values=["think"]),
        default=True,
    )
    result = resolve(
        [a, b], ExecutionPreferences(harness=pref("cursor"), reasoning=pref("think"))
    )
    assert result["selected"]["model_provider"] == "cursor/grok"
    with pytest.raises(SelectionError):
        resolve(
            [a, b],
            ExecutionPreferences(harness=pref("cursor"), reasoning=pref("xhigh")),
        )


def test_catalog_staleness_failed_auth_and_mixed_version_are_not_success():
    with pytest.raises(SelectionError):
        resolve(
            [
                candidate(freshness="stale"),
                candidate(readiness="unavailable"),
                candidate(schema_version=2),
            ]
        )
    result = resolve(
        [candidate(freshness="stale")], policy=SelectionPolicy(allow_stale_catalog=True)
    )
    assert "catalog_stale" in result["tradeoffs"]


def test_editable_rules_match_structured_role_risk_complexity_and_do_not_grant_pins():
    policy = SelectionPolicy(
        rules=[
            SelectionRule(
                id="review",
                description="More native effort for risky reviews",
                match={
                    "role": ["reviewer"],
                    "risk": ["high"],
                    "complexity": ["complex"],
                },
                prefer=ExecutionPreferences(reasoning=pref("xhigh", "preferred")),
            )
        ]
    )
    task = TaskAssessment(role="reviewer", risk="high", complexity="complex")
    result = resolve(policy=policy, assessment=task)
    assert result["selected"]["reasoning"] == "xhigh"
    assert result["matched_rules"] == ["review"]
    pinned = resolve(
        policy=policy,
        assessment=task,
        prefs=ExecutionPreferences(reasoning=pref("low")),
    )
    assert pinned["selected"]["reasoning"] == "low"
    with pytest.raises(ValidationError):
        SelectionRule(
            id="unsafe",
            description="invalid",
            prefer=ExecutionPreferences(reasoning=pref("xhigh")),
        )


def test_cold_start_is_deterministic_and_cost_latency_objectives_use_only_known_estimates():
    a = candidate(model="small", estimated_cost_usd=1, estimated_latency_ms=5000)
    b = candidate(
        model="newest-largest", estimated_cost_usd=10, estimated_latency_ms=10
    )
    assert resolve([a, b]) == resolve([b, a])
    assert (
        resolve([a, b], assessment=TaskAssessment(objective="cost"))["selected"][
            "model"
        ]
        == "small"
    )
    assert (
        resolve([a, b], assessment=TaskAssessment(objective="latency"))["selected"][
            "model"
        ]
        == "newest-largest"
    )
    inferred = assess_task(
        "Investigate authentication architecture", "Migration of the production system"
    )
    assert (inferred.risk, inferred.complexity, inferred.ambiguity) == (
        "high",
        "complex",
        "high",
    )
    assert inferred.tools == []  # task text cannot manufacture tool authority


def test_sparse_old_unvalidated_and_contradictory_feedback_remain_explicit():
    c = candidate()
    evidence = [
        OutcomeEvidence(
            id=str(i),
            candidate_key=c.key,
            decision_id="prior",
            observed_at=NOW,
            validated=True,
            completed=True,
            tests_passed=False,
            references=["tests:failed"],
        )
        for i in range(3)
    ]
    evidence.append(
        OutcomeEvidence(
            id="old",
            candidate_key=c.key,
            decision_id="prior",
            observed_at=NOW - timedelta(days=100),
            validated=True,
            completed=True,
            references=["review:passed"],
        )
    )
    result = resolve([c], evidence=evidence, policy=SelectionPolicy(use_feedback=True))
    summary = result["alternatives"][0]["feedback"]
    assert summary["samples"] == 3 and summary["successes"] == 0
    assert summary["contradictory"] and summary["use"] == "insufficient_or_disabled"
    assert summary["cost_usd_mean"] is None


def test_reuse_and_bounded_fallback_preserve_prompt_connection_and_pins():
    result = resolve(
        policy=SelectionPolicy(
            fallback_max_attempts=1, fallback_connections=["account-a"]
        )
    )
    assert validate_reuse(result) is result
    from copy import deepcopy

    tampered = deepcopy(result)
    tampered["selected"]["reasoning"] = "invented"
    with pytest.raises(SelectionError) as error:
        validate_reuse(tampered)
    assert error.value.code == "selection_receipt_integrity"
    with pytest.raises(SelectionError):
        validate_reuse(result, requested=ExecutionPreferences(model=pref("changed")))
    preferred = ExecutionPreferences(
        model=Preference(intent="preferred", value="unavailable-choice")
    )
    alternative_receipt = resolve(prefs=preferred)
    assert (
        validate_reuse(alternative_receipt, requested=preferred) is alternative_receipt
    )
    with pytest.raises(SelectionError):
        validate_reuse(
            alternative_receipt,
            requested=ExecutionPreferences(
                hard_constraints=SelectionConstraints(models=["not-the-selected-model"])
            ),
        )
    alternative = result["alternatives"][0]
    authorize_fallback(
        result,
        alternative,
        attempt=1,
        prompt_digest="same",
        original_prompt_digest="same",
    )
    for changes in [{"attempt": 2}, {"prompt_digest": "changed"}]:
        with pytest.raises(SelectionError):
            authorize_fallback(
                result,
                alternative,
                **dict(
                    {
                        "attempt": 1,
                        "prompt_digest": "same",
                        "original_prompt_digest": "same",
                    },
                    **changes,
                ),
            )


def test_adapter_catalog_reasoning_is_model_scoped_and_never_copied_from_label():
    options = {
        "models": {
            "availableModels": [{"modelId": "a[low]"}, {"modelId": "b[xhigh]"}],
            "currentModelId": "a[low]",
        }
    }
    result = candidates_from_advertisement(
        instance_id="i",
        harness="codex",
        advertisement=options,
        readiness="ready",
        observed_at=NOW,
        source="acp",
    )
    assert {c.model: c.reasoning.values for c in result} == {
        "a": ["low"],
        "b": ["xhigh"],
    }
    minimax = candidates_from_advertisement(
        instance_id="i",
        harness="openinterpreter",
        model_provider="minimax",
        advertisement={
            "models": {
                "availableModels": [{"modelId": "MiniMax", "name": "high reasoning"}]
            }
        },
        readiness="unknown",
        observed_at=NOW,
        source="configured",
    )
    assert minimax[0].reasoning.support == "unknown"
    assert minimax[0].readiness == "unknown"


def test_durable_policy_revision_decision_idempotence_and_feedback_ownership(tmp_path):
    store = SelectionStore(tmp_path)
    result = resolve()
    assert store.save_decision(result, "realm", "owner") == result
    assert store.save_decision(result, "realm", "owner") == result
    assert store.decision(result["decision_id"], "realm", "other") is None
    policy = store.save_policy("realm", SelectionPolicy(), expected_revision=1)
    assert policy.revision == 2
    with pytest.raises(SelectionError, match="Policy changed"):
        store.save_policy("realm", SelectionPolicy(), expected_revision=1)
    evidence = OutcomeEvidence(
        id="e",
        candidate_key=result["candidate_key"],
        decision_id=result["decision_id"],
        observed_at=NOW,
        validated=True,
        completed=True,
        references=["review:1"],
    )
    with pytest.raises(SelectionError):
        store.record_evidence(evidence, "realm", "other")
    store.record_evidence(evidence, "realm", "owner")
    assert store.evidence("realm", "owner") == [evidence]


def test_all_attempts_remain_accessible_in_bounded_pages(tmp_path):
    store = SelectionStore(tmp_path)
    for index in range(103):
        store.record_attempt(f"attempt-{index}", "decision", {"index": index})
    assert store.attempt_count("decision") == 103
    assert len(store.attempts("decision")) == 100
    assert store.attempts("decision", offset=100) == [
        {"index": i} for i in range(100, 103)
    ]
    with pytest.raises(ValueError):
        store.attempts("decision", limit=101)
