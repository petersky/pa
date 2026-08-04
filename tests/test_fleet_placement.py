from __future__ import annotations

import inspect
import json
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.instance_config import InstanceConfig, save_instance_config
from pa.domain.models import CardCreate, FleetInstance, ProjectCreate, RepositoryCreate
from pa.domain.store import reset_store
from pa.execution.dispatch import (
    CapacityAdmission,
    ConcurrentCardDispatch,
    DispatchCapacityExhausted,
    DispatchIdempotencyConflict,
    DispatchRecord,
    DispatchStore,
)
from pa.fleet.capacity import effective_capacity, workload_counts
from pa.fleet.placement import (
    PlacementCandidate,
    PlacementError,
    PlacementPolicy,
    PlacementRequest,
    PlacementService,
    RoundRobinCursorStore,
)
from pa.instance.agent_session import reset_instance_agent
from pa.modules.fleet import FleetModule
from pa.workloads import CANONICAL_WORKLOAD_PROFILES, LEGACY_CODE_PROFILE_REASON


@pytest.fixture(autouse=True)
def _reset_pa_singletons():
    reset_settings()
    reset_store()
    reset_instance_agent()
    yield
    reset_instance_agent()
    reset_store()
    reset_settings()


def _fresh(value):
    return {
        "state": "fresh",
        "value": value,
        "observed_at": datetime.now(UTC).isoformat(),
    }


def _candidate(
    instance_id: str,
    *,
    active: int = 0,
    queued: int = 0,
    capacity: int = 4,
    local: bool = False,
    authorized: bool = True,
    repositories: list[str] | None = None,
    model_ids: list[str] | None = None,
) -> PlacementCandidate:
    return PlacementCandidate(
        instance_id=instance_id,
        name=instance_id.upper(),
        local=local,
        capabilities=["browser", f"capacity:{capacity}"],
        authorized=authorized,
        authorization_reason=None if authorized else "realm editor role is required",
        reachability=_fresh({"health": "up"}),
        activity=_fresh(
            {
                "state": "idle",
                "active_sessions": active,
                "queued_prompts": queued,
                "quiescing": False,
            }
        ),
        providers=_fresh(
            [
                {
                    "id": "codex",
                    "available": True,
                    "auth_state": "authenticated",
                    "models": model_ids or ["gpt-5"],
                }
            ]
        ),
        repositories=_fresh(
            {
                "observations": [
                    {
                        "state": "fresh",
                        "snapshot": {"repository_id": repository_id},
                    }
                    for repository_id in repositories or []
                ],
                "workspaces": [],
            }
        ),
    )


def _request(
    policy: PlacementPolicy,
    *,
    repository_ids: list[str] | None = None,
) -> PlacementRequest:
    return PlacementRequest(
        realm_id="default",
        fleet_id="fleet",
        policy=policy,
        card_id="card-1",
        provider="codex",
        model_id="gpt-5",
        required_capabilities=["browser"],
        repository_ids=repository_ids or [],
    )


def test_best_match_scores_readiness_locality_capacity_and_breaks_ties() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        service = PlacementService(RoundRobinCursorStore(Path(tmp)))
        decision = service.resolve(
            _request(PlacementPolicy.BEST_MATCH, repository_ids=["repo-1"]),
            [
                _candidate("b", active=2, repositories=[]),
                _candidate("a", active=0, repositories=["repo-1"]),
            ],
        )
        assert decision.chosen_instance_id == "a"
        assert (
            decision.scores["a"]["best_match_total"]
            > decision.scores["b"]["best_match_total"]
        )
        assert "instance ID breaks exact ties" in decision.tie_breaking_reason

        tied = service.resolve(
            _request(PlacementPolicy.BEST_MATCH),
            [_candidate("b"), _candidate("a")],
        )
        assert tied.chosen_instance_id == "a"


def test_least_busy_normalizes_load_and_breaks_ties_deterministically() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        service = PlacementService(RoundRobinCursorStore(Path(tmp)))
        decision = service.resolve(
            _request(PlacementPolicy.LEAST_BUSY),
            [
                _candidate("small", active=1, capacity=2),
                _candidate("large", active=2, capacity=8),
            ],
        )
        assert decision.chosen_instance_id == "large"

        tied = service.resolve(
            _request(PlacementPolicy.LEAST_BUSY),
            [_candidate("b", active=1), _candidate("a", active=1)],
        )
        assert tied.chosen_instance_id == "a"


def test_automatic_provider_records_concrete_target_provider(tmp_path: Path) -> None:
    decision = PlacementService(RoundRobinCursorStore(tmp_path)).resolve(
        PlacementRequest(
            realm_id="default",
            fleet_id="fleet",
            instance_id="codex-only",
            provider=None,
            model_id=None,
        ),
        [_candidate("codex-only")],
    )
    assert decision.eligible_candidates[0]["provider_id"] == "codex"


def test_legacy_none_model_selector_normalizes_to_automatic() -> None:
    from pa.modules.fleet import RemoteAgentStartBody

    body = RemoteAgentStartBody(provider=" codex ", model_id="None")
    assert body.provider == "codex"
    assert body.model_id is None


def test_round_robin_survives_restart_and_membership_changes() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp)
        candidates = [_candidate("a"), _candidate("b"), _candidate("c")]
        first = PlacementService(RoundRobinCursorStore(path))
        assert (
            first.resolve(
                _request(PlacementPolicy.ROUND_ROBIN), candidates
            ).chosen_instance_id
            == "a"
        )
        assert (
            first.resolve(
                _request(PlacementPolicy.ROUND_ROBIN), candidates
            ).chosen_instance_id
            == "b"
        )

        restarted = PlacementService(RoundRobinCursorStore(path))
        changed = [_candidate("a"), _candidate("c"), _candidate("d")]
        assert (
            restarted.resolve(
                _request(PlacementPolicy.ROUND_ROBIN), changed
            ).chosen_instance_id
            == "c"
        )


def test_random_eligible_uses_one_uniform_index_and_records_choice() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        calls: list[int] = []

        def choose(bound: int) -> int:
            calls.append(bound)
            return 1

        service = PlacementService(RoundRobinCursorStore(Path(tmp)), randbelow=choose)
        decision = service.resolve(
            _request(PlacementPolicy.RANDOM_ELIGIBLE),
            [_candidate("c"), _candidate("a"), _candidate("b")],
        )
        assert calls == [3]
        assert decision.chosen_instance_id == "b"
        assert "resolved choice is persisted" in decision.tie_breaking_reason


def test_stale_authorization_capacity_provider_and_empty_sets_fail_explainably() -> (
    None
):
    with tempfile.TemporaryDirectory() as tmp:
        service = PlacementService(RoundRobinCursorStore(Path(tmp)))
        stale = _candidate("stale")
        stale.activity = {"state": "stale", "value": None}
        denied = _candidate("denied", authorized=False)
        full = _candidate("full", active=4, capacity=4)
        wrong_model = _candidate("model", model_ids=["other"])

        with pytest.raises(PlacementError) as raised:
            service.resolve(
                _request(PlacementPolicy.BEST_MATCH),
                [stale, denied, full, wrong_model],
            )
        assert raised.value.code == "no_eligible_instance"
        reasons = " ".join(
            reason
            for item in raised.value.rejected_candidates
            for reason in item["reasons"]
        )
        assert "fresh data is required" in reasons
        assert "realm editor role is required" in reasons
        assert "capacity is exhausted" in reasons
        assert "lacks model" in reasons

        with pytest.raises(PlacementError):
            service.resolve(_request(PlacementPolicy.BEST_MATCH), [])


def test_full_queue_capable_candidate_remains_eligible_until_queue_limit() -> None:
    full = _candidate("full", active=4, capacity=4)
    full.dispatch_queue_capacity = 100
    with tempfile.TemporaryDirectory() as tmp:
        decision = PlacementService(RoundRobinCursorStore(Path(tmp))).resolve(
            _request(PlacementPolicy.BEST_MATCH), [full]
        )
    detail = decision.eligible_candidates[0]
    assert detail["admission_disposition"] == "queued"
    assert detail["execution_slot_available"] is False
    assert detail["queue_capacity"] == 100

    full.activity["value"]["dispatch_waiting"] = 100
    with tempfile.TemporaryDirectory() as tmp, pytest.raises(PlacementError) as raised:
        PlacementService(RoundRobinCursorStore(Path(tmp))).resolve(
            _request(PlacementPolicy.BEST_MATCH), [full]
        )
    assert raised.value.rejected_candidates[0]["rejection_codes"] == [
        "dispatch_queue_full"
    ]


def test_zero_queue_capacity_is_an_explicit_full_queue() -> None:
    full = _candidate("full", active=4, capacity=4)
    full.dispatch_queue_capacity = 0
    with tempfile.TemporaryDirectory() as tmp, pytest.raises(PlacementError) as raised:
        PlacementService(RoundRobinCursorStore(Path(tmp))).resolve(
            _request(PlacementPolicy.BEST_MATCH), [full]
        )

    detail = raised.value.rejected_candidates[0]
    assert detail["rejection_codes"] == ["dispatch_queue_full"]
    assert detail["queue_count"] == 0
    assert detail["queue_capacity"] == 0


def test_named_dispatch_queue_full_returns_structured_actionable_details() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        settings = Settings(
            data_dir=Path(tmp),
            instance_id="local",
            instance_name="Local",
            instance_url="http://pa.test:8080",
            agent_enabled=False,
            subscribed_realms=["default"],
            peers=[],
        )
        app = Kernel.boot(settings=settings).build_app()
        full = _candidate("local", active=4, capacity=4, local=True)
        full.dispatch_queue_capacity = 0
        with (
            patch(
                "pa.modules.fleet._placement_candidates",
                autospec=True,
                return_value=[full],
            ),
            TestClient(app) as client,
        ):
            card = app.state.ctx.store.create_card(CardCreate(title="No queue slot"))
            assert client.get("/").status_code == 200
            response = client.post(
                "/api/fleet/instances/local/agent/start",
                headers={"X-CSRF-Token": client.cookies.get("pa_csrf")},
                json={
                    "card_id": card.id,
                    "provider": "codex",
                    "idempotency_key": "queue-full",
                    "execution_contract": {
                        "version": 1,
                        "profile": "research",
                        "confirmed": True,
                    },
                },
            )

        assert response.status_code == 429, response.text
        detail = response.json()["detail"]
        assert detail["code"] == "dispatch_queue_full"
        assert detail["current_count"] == 0
        assert detail["maximum_count"] == 0
        assert detail["retry_after_seconds"] == 5
        assert "increase dispatch_queue_capacity" in detail["remediation_options"]


def test_preview_routes_dispatch_and_audit_normalize_legacy_code(monkeypatch) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        settings = Settings(
            data_dir=Path(tmp),
            instance_id="local",
            instance_name="Local",
            instance_url="http://pa.test:8080",
            agent_enabled=False,
            subscribed_realms=["default"],
            peers=[],
        )
        app = Kernel.boot(settings=settings).build_app()
        with TestClient(app) as client:
            store = app.state.ctx.store
            project = store.create_project(ProjectCreate(title="Legacy profile"))
            repository = store.create_repository(
                RepositoryCreate(url="https://example.test/owner/repository.git")
            )
            assert store.link_project_repository(project.id, repository.id)
            candidate = _candidate("local", local=True, repositories=[repository.id])
            monkeypatch.setattr(
                "pa.modules.fleet._placement_candidates",
                AsyncMock(return_value=[candidate]),
            )
            assert client.get("/").status_code == 200
            headers = {"X-CSRF-Token": client.cookies.get("pa_csrf")}
            preview_card = store.create_card(
                CardCreate(title="Preview legacy", project_id=project.id)
            )
            for profile in (*CANONICAL_WORKLOAD_PROFILES, "code"):
                placement_preview = client.post(
                    "/api/fleet/placement/preview",
                    headers=headers,
                    json={
                        "card_id": preview_card.id,
                        "placement_policy": "best_match",
                        "execution_contract": {
                            "version": 1,
                            "profile": profile,
                            "confirmed": True,
                            "requirements": {},
                        },
                    },
                )
                group_preview = client.get(
                    "/api/fleet/instance-groups/all-active/preview",
                    params={
                        "project_id": project.id,
                        "workload_profile": profile,
                    },
                )
                assert placement_preview.status_code == 200, placement_preview.text
                assert group_preview.status_code == 200, group_preview.text
                expected = (
                    "repository"
                    if profile in {"automatic", "repository", "code"}
                    else profile
                )
                for response in (placement_preview, group_preview):
                    payload = response.json()
                    assert payload["decision"]["workload_profile"] == expected
                    assert payload["materialization_plan"]["profile"] == expected
                    if profile == "code":
                        assert (
                            payload["decision"]["profile_normalization_reason"]
                            == LEGACY_CODE_PROFILE_REASON
                        )

            for index, profile in enumerate(
                ["automatic", "repository", "research", "operations", "code"]
            ):
                card = store.create_card(
                    CardCreate(title=f"Dispatch {profile}", project_id=project.id)
                )
                response = client.post(
                    "/api/fleet/dispatch",
                    headers=headers,
                    json={
                        "card_id": card.id,
                        "placement_policy": "best_match",
                        "provider": "codex",
                        "idempotency_key": f"profile-{index}",
                        "execution_contract": {
                            "version": 1,
                            "profile": profile,
                            "confirmed": True,
                            "requirements": {},
                        },
                    },
                )
                assert response.status_code == 202, response.text
                decision = response.json()["dispatch"]["placement_decision"]
                expected = (
                    "repository"
                    if profile in {"automatic", "repository", "code"}
                    else profile
                )
                assert decision["workload_profile"] == expected
                if profile == "code":
                    assert (
                        decision["profile_normalization_reason"]
                        == LEGACY_CODE_PROFILE_REASON
                    )
                    legacy_dispatch_id = response.json()["dispatch_id"]

            persisted_legacy = DispatchStore(Path(tmp)).get(legacy_dispatch_id)
            assert persisted_legacy is not None
            persisted_contract = persisted_legacy.request_payload["execution_contract"]
            assert persisted_contract["profile"] == "repository"
            assert (
                persisted_contract["profile_normalization_reason"]
                == LEGACY_CODE_PROFILE_REASON
            )

            named_card = store.create_card(
                CardCreate(title="Named legacy dispatch", project_id=project.id)
            )
            named = client.post(
                "/api/fleet/instances/local/agent/start",
                headers=headers,
                json={
                    "card_id": named_card.id,
                    "provider": "codex",
                    "idempotency_key": "named-legacy-code",
                    "execution_contract": {
                        "version": 1,
                        "profile": "code",
                        "confirmed": True,
                        "requirements": {},
                    },
                },
            )
            assert named.status_code == 202, named.text
            named_decision = named.json()["dispatch"]["placement_decision"]
            assert named_decision["workload_profile"] == "repository"
            assert (
                named_decision["profile_normalization_reason"]
                == LEGACY_CODE_PROFILE_REASON
            )

            audit = client.get(
                "/api/fleet/policy-audit",
                params={"entity_type": "placement_decision"},
            )
            assert audit.status_code == 200, audit.text
            legacy_audit = next(
                item
                for item in audit.json()
                if item["entity_id"] == named.json()["dispatch_id"]
            )
            assert legacy_audit["payload"]["workload_profile"] == "repository"
            assert (
                legacy_audit["payload"]["profile_normalization_reason"]
                == LEGACY_CODE_PROFILE_REASON
            )


def test_preview_routes_return_identical_typed_422_for_unknown_profile() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        app = Kernel.boot(
            settings=Settings(
                data_dir=Path(tmp),
                instance_id="local",
                instance_name="Local",
                instance_url="http://pa.test:8080",
                agent_enabled=False,
                subscribed_realms=["default"],
                peers=[],
            )
        ).build_app()
        with TestClient(app) as client:
            assert client.get("/").status_code == 200
            headers = {"X-CSRF-Token": client.cookies.get("pa_csrf")}
            ui_card = app.state.ctx.store.create_card(CardCreate(title="UI profiles"))
            with patch("pa.fleet.overview.build_overview", return_value={"nodes": []}):
                dispatch_form = client.get(f"/partials/cards/{ui_card.id}/dispatch")
            placement = client.post(
                "/api/fleet/placement/preview",
                headers=headers,
                json={
                    "placement_policy": "best_match",
                    "execution_contract": {
                        "version": 1,
                        "profile": "invalid",
                        "confirmed": True,
                        "requirements": {},
                    },
                },
            )
            group = client.get(
                "/api/fleet/instance-groups/all-active/preview",
                params={"workload_profile": "invalid"},
            )
            dispatch = client.post(
                "/api/fleet/dispatch",
                headers=headers,
                json={
                    "placement_policy": "best_match",
                    "execution_contract": {
                        "version": 1,
                        "profile": "invalid",
                        "confirmed": True,
                        "requirements": {},
                    },
                },
            )
            named_dispatch = client.post(
                "/api/fleet/instances/local/agent/start",
                headers=headers,
                json={
                    "execution_contract": {
                        "version": 1,
                        "profile": "invalid",
                        "confirmed": True,
                        "requirements": {},
                    },
                },
            )
            placement_default = client.put(
                "/api/fleet/placement-defaults",
                headers=headers,
                json={
                    "group_id": "all-active",
                    "workload_profile": "invalid",
                },
            )

        responses = (
            placement,
            group,
            dispatch,
            named_dispatch,
            placement_default,
        )
        assert {response.status_code for response in responses} == {422}
        assert {json.dumps(response.json(), sort_keys=True) for response in responses} == {
            json.dumps(placement.json(), sort_keys=True)
        }
        detail = placement.json()["detail"]
        assert detail["code"] == "invalid_workload_profile"
        assert detail["supported_profiles"] == [
            "automatic",
            "repository",
            "research",
            "operations",
        ]
        assert detail["legacy_aliases"] == {"code": "repository"}
        assert dispatch_form.status_code == 200, dispatch_form.text
        selector = dispatch_form.text.split('name="execution_profile"', 1)[1].split(
            "</select>", 1
        )[0]
        for profile in CANONICAL_WORKLOAD_PROFILES:
            assert f'value="{profile}"' in selector
        assert 'value="code"' not in selector


def test_capacity_precedence_and_documented_default_are_explicit() -> None:
    configured = effective_capacity(configured=9, capabilities=["capacity:3"])
    assert configured.limit == 9
    assert configured.source == "configured"

    legacy = effective_capacity(capabilities=["capacity:7"])
    assert legacy.limit == 7
    assert legacy.source == "legacy_capability"
    assert legacy.legacy_capability == "capacity:7"

    fallback = effective_capacity(capabilities=["capacity:0", "capacity:999"])
    assert fallback.limit == 4
    assert fallback.source == "documented_default"
    assert "Conservative" in fallback.rationale


def test_connected_idle_sessions_do_not_consume_capacity() -> None:
    counts = workload_counts(
        {
            "connected_runtimes": 6,
            "idle_sessions": 3,
            "prompting_turns": 3,
            "active_capacity_consumers": 3,
            "queued_prompts": 0,
            "dispatch_reservations": 0,
        }
    )
    assert counts == {
        "active": 3,
        "queued": 0,
        "reservations": 0,
        "consumed": 3,
        "semantic_source": "capacity_consumers",
    }


def test_provider_specific_limit_applies_with_global_limit() -> None:
    candidate = _candidate("provider-limited", active=0, capacity=8)
    candidate.dispatch_capacity = 8
    candidate.dispatch_provider_capacities = {"codex": 2}
    candidate.activity = _fresh(
        {
            "state": "working",
            "active_capacity_consumers": 1,
            "provider_concurrency": {
                "codex": {
                    "active_capacity_consumers": 1,
                    "queued_prompts": 0,
                    "dispatch_reservations": 1,
                }
            },
        }
    )
    with tempfile.TemporaryDirectory() as tmp:
        service = PlacementService(RoundRobinCursorStore(Path(tmp)))
        with pytest.raises(PlacementError) as raised:
            service.resolve(_request(PlacementPolicy.BEST_MATCH), [candidate])
    rejected = raised.value.rejected_candidates[0]
    assert rejected["capacity"] == 2
    assert rejected["capacity_detail"]["source"] == "configured_provider"
    assert rejected["reserved"] == 1


def _record(
    *,
    key: str,
    fingerprint: str,
    target: str,
    card_id: str = "card-1",
) -> DispatchRecord:
    return DispatchRecord(
        mutation_id=f"mutation-{key}-{target}",
        idempotency_key=key,
        request_fingerprint=fingerprint,
        placement_request_fingerprint=fingerprint,
        card_id=card_id,
        authority_instance_id="authority",
        authority_url="http://authority.test",
        target_instance_id=target,
        placement_policy="best_match",
        placement_decision={"chosen_instance_id": target},
    )


def test_admission_is_atomic_for_idempotency_and_concurrent_card_work() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp))
        first, duplicate = store.admit(
            _record(key="same", fingerprint="fp", target="a")
        )
        assert not duplicate
        repeated, duplicate = store.admit(
            _record(key="same", fingerprint="fp", target="b")
        )
        assert duplicate
        assert repeated.dispatch_id == first.dispatch_id
        assert repeated.target_instance_id == "a"

        with pytest.raises(DispatchIdempotencyConflict):
            store.admit(_record(key="same", fingerprint="different", target="a"))

        def admit(record):
            try:
                return store.admit(record)
            except ConcurrentCardDispatch as exc:
                return exc

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    admit,
                    [
                        _record(
                            key="race-1",
                            fingerprint="race-1",
                            target="a",
                            card_id="card-race",
                        ),
                        _record(
                            key="race-2",
                            fingerprint="race-2",
                            target="b",
                            card_id="card-race",
                        ),
                    ],
                )
            )
        assert sum(isinstance(item, ConcurrentCardDispatch) for item in results) == 1


def test_last_slot_reservation_is_atomic_and_released_on_cancel_and_restart() -> None:
    capacity = CapacityAdmission(limit=1, source="configured")
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp)
        store = DispatchStore(path)

        def admit(index: int):
            try:
                return store.admit(
                    _record(
                        key=f"slot-{index}",
                        fingerprint=f"slot-{index}",
                        target="a",
                        card_id=f"card-{index}",
                    ),
                    capacity=capacity,
                )
            except DispatchCapacityExhausted as exc:
                return exc

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(admit, [1, 2]))
        assert sum(isinstance(item, DispatchCapacityExhausted) for item in results) == 1
        admitted = next(item[0] for item in results if isinstance(item, tuple))

        restarted = DispatchStore(path)
        with pytest.raises(DispatchCapacityExhausted):
            restarted.admit(
                _record(
                    key="after-restart",
                    fingerprint="after-restart",
                    target="a",
                    card_id="card-after-restart",
                ),
                capacity=capacity,
            )

        restarted.transition(admitted, "cancelled", "operator cancelled")
        replacement, duplicate = restarted.admit(
            _record(
                key="replacement",
                fingerprint="replacement",
                target="a",
                card_id="card-replacement",
            ),
            capacity=capacity,
        )
        assert not duplicate
        assert replacement.capacity_reserved_at is not None
        assert admitted.capacity_release_reason == "cancelled"

        replacement.capacity_reservation_expires_at = datetime.now(UTC) - timedelta(
            seconds=1
        )
        restarted.put(replacement)
        assert restarted.runnable() == []
        assert replacement.state == "failed"
        assert replacement.error_code == "capacity_reservation_timeout"
        assert replacement.capacity_release_reason == "timeout"


def test_capacity_override_is_durable_and_auditable() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp))
        record, duplicate = store.admit(
            _record(
                key="override",
                fingerprint="override",
                target="a",
                card_id="card-override",
            ),
            capacity=CapacityAdmission(
                limit=1,
                source="configured",
                observed_active=1,
                override=True,
                override_reason="Incident response approved by operator",
            ),
        )
        assert not duplicate
        assert record.capacity_override is True
        assert (
            record.capacity_override_reason == "Incident response approved by operator"
        )


class _FakeMcp:
    def __init__(self) -> None:
        self.functions = {}

    def tool(self):
        def register(function):
            self.functions[function.__name__] = function
            return function

        return register


def test_mcp_accepts_concrete_target_or_policy_without_client_scheduling() -> None:
    mcp = _FakeMcp()
    ctx = MagicMock()
    with patch("pa.mcp.local_api.request_local_pa") as local:
        FleetModule().register_mcp(mcp, ctx)
        assert "dispatch_card" in mcp.functions
        signature = inspect.signature(mcp.functions["dispatch_card"])
        assert "instance_id" in signature.parameters
        assert "policy" in signature.parameters
        mcp.functions["dispatch_card"](
            "card-1",
            "key-1",
            policy=PlacementPolicy.LEAST_BUSY,
        )
    local.assert_called_once()
    assert local.call_args.args[2] == "/api/fleet/dispatch"
    assert local.call_args.kwargs["json"]["placement_policy"] == "least_busy"


def test_policy_dispatch_endpoint_retries_without_rerunning_placement() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        settings = Settings(
            data_dir=Path(tmp),
            instance_id="local",
            instance_name="Local",
            instance_url="http://pa.test:8080",
            agent_enabled=False,
            subscribed_realms=["default"],
            peers=[],
        )
        app = Kernel.boot(settings=settings).build_app()
        candidate = _candidate("local", local=True)
        with (
            patch(
                "pa.modules.fleet._placement_candidates",
                autospec=True,
                return_value=[candidate],
            ) as candidates,
            TestClient(app) as client,
        ):
            card = app.state.ctx.store.create_card(CardCreate(title="Dispatch me"))
            shell = client.get("/")
            token = client.cookies.get("pa_csrf")
            assert shell.status_code == 200
            headers = {"X-CSRF-Token": token}
            body = {
                "card_id": card.id,
                "placement_policy": "best_match",
                "provider": "codex",
                "model_id": "gpt-5",
                "idempotency_key": "stable-policy-request",
                "execution_contract": {
                    "version": 1,
                    "profile": "research",
                    "confirmed": True,
                },
            }
            first = client.post("/api/fleet/dispatch", headers=headers, json=body)
            second = client.post("/api/fleet/dispatch", headers=headers, json=body)

        assert first.status_code == 202, first.text
        assert second.status_code == 202, second.text
        assert second.json()["duplicate"] is True
        assert (
            second.json()["dispatch"]["target_instance_id"]
            == first.json()["dispatch"]["target_instance_id"]
            == "local"
        )
        assert candidates.call_count == 1


def test_named_dispatch_retry_returns_before_repeating_placement() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        settings = Settings(
            data_dir=Path(tmp),
            instance_id="local",
            instance_name="Local",
            instance_url="http://pa.test:8080",
            agent_enabled=False,
            subscribed_realms=["default"],
            peers=[],
        )
        app = Kernel.boot(settings=settings).build_app()
        candidate = _candidate("local", local=True)
        with (
            patch(
                "pa.modules.fleet._placement_candidates",
                autospec=True,
                return_value=[candidate],
            ) as candidates,
            TestClient(app) as client,
        ):
            card = app.state.ctx.store.create_card(CardCreate(title="Dispatch me"))
            assert client.get("/").status_code == 200
            headers = {"X-CSRF-Token": client.cookies.get("pa_csrf")}
            body = {
                "card_id": card.id,
                "provider": "codex",
                "idempotency_key": "stable-named-request",
                "execution_contract": {
                    "version": 1,
                    "profile": "research",
                    "confirmed": True,
                },
            }
            first = client.post(
                "/api/fleet/instances/local/agent/start", headers=headers, json=body
            )
            second = client.post(
                "/api/fleet/instances/local/agent/start", headers=headers, json=body
            )

        assert first.status_code == 202, first.text
        assert second.status_code == 202, second.text
        assert second.json()["duplicate"] is True
        assert second.json()["dispatch_id"] == first.json()["dispatch_id"]
        assert candidates.call_count == 1


def test_named_dispatch_only_probes_the_requested_instance() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        settings = Settings(
            data_dir=Path(tmp),
            instance_id="local",
            instance_name="Local",
            instance_url="http://pa.test:8080",
            agent_enabled=False,
            subscribed_realms=["default"],
            peers=[],
        )
        app = Kernel.boot(settings=settings).build_app()
        candidate = _candidate("remote")
        with (
            patch(
                "pa.modules.fleet._placement_candidates",
                autospec=True,
                return_value=[candidate],
            ) as candidates,
            TestClient(app) as client,
        ):
            app.state.ctx.require_service("fleet_registry").upsert_instance(
                FleetInstance(
                    instance_id="remote",
                    name="Remote",
                    url="http://remote.test:8080",
                )
            )
            card = app.state.ctx.store.create_card(CardCreate(title="Dispatch me"))
            assert client.get("/").status_code == 200
            response = client.post(
                "/api/fleet/instances/remote/agent/start",
                headers={"X-CSRF-Token": client.cookies.get("pa_csrf")},
                json={
                    "card_id": card.id,
                    "provider": "codex",
                    "idempotency_key": "named-target-only",
                    "execution_contract": {
                        "version": 1,
                        "profile": "research",
                        "confirmed": True,
                    },
                },
            )

        assert response.status_code == 202, response.text
        inspected = candidates.call_args.args[1]
        assert [item.instance_id for item in inspected] == ["remote"]


def test_capacity_config_api_updates_live_fleet_advertisement() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        save_instance_config(
            Path(tmp),
            InstanceConfig(
                data_dir=tmp,
                instance_id="local",
                instance_name="Local",
                instance_url="http://pa.test:8080",
            ),
        )
        settings = Settings(
            data_dir=Path(tmp),
            instance_id="local",
            instance_name="Local",
            instance_url="http://pa.test:8080",
            agent_enabled=False,
            subscribed_realms=["default"],
            peers=[],
        )
        app = Kernel.boot(settings=settings).build_app()
        with TestClient(app) as client:
            shell = client.get("/")
            token = client.cookies.get("pa_csrf")
            assert shell.status_code == 200
            response = client.patch(
                "/api/config/capacity",
                headers={"X-CSRF-Token": token},
                json={
                    "dispatch_capacity": 11,
                    "dispatch_provider_capacities": {"Codex": 3},
                    "dispatch_queue_capacity": 77,
                    "dispatch_provider_queue_capacities": {"Codex": 25},
                    "idempotency_key": "capacity-api-test",
                },
            )
            config = client.get("/api/config").json()
            audit = client.get("/api/configuration/audit").json()
            settings_page = client.get("/settings")
            fleet_page = client.get("/fleet")
            instance = next(
                item
                for item in client.get("/api/fleet/instances").json()
                if item["instance_id"] == "local"
            )

        assert response.status_code == 200, response.text
        assert "Fleet execution capacity" in settings_page.text
        assert 'value="11"' in settings_page.text
        assert "Capacity" in fleet_page.text
        assert "11" in fleet_page.text
        assert response.json()["takes_effect"].startswith("immediately")
        assert config["dispatch_capacity"] == 11
        assert config["dispatch_provider_capacities"] == {"codex": 3}
        assert config["dispatch_queue_capacity"] == 77
        assert config["dispatch_provider_queue_capacities"] == {"codex": 25}
        assert instance["dispatch_capacity"] == 11
        assert instance["dispatch_provider_capacities"] == {"codex": 3}
        assert instance["dispatch_queue_capacity"] == 77
        assert instance["dispatch_provider_queue_capacities"] == {"codex": 25}
        assert audit["events"][-1]["idempotency_key"] == "capacity-api-test"
        assert "dispatch_queue_capacity" in audit["events"][-1]["keys"]
