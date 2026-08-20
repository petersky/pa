from __future__ import annotations

import asyncio
import inspect
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.instance_config import InstanceConfig, save_instance_config
from pa.domain.models import CardCreate, FleetInstance
from pa.domain.store import reset_store
from pa.execution.dispatch import (
    CapacityAdmission,
    ConcurrentCardDispatch,
    DispatchCapacityExhausted,
    DispatchIdempotencyConflict,
    DispatchRecord,
    DispatchStore,
)
from pa.fleet.capacity import (
    effective_capacity,
    normalize_activity_capacity,
    workload_counts,
)
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


def test_preferred_capabilities_score_candidates_without_rejecting_them() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        service = PlacementService(RoundRobinCursorStore(Path(tmp)))
        preferred = _candidate("preferred")
        fallback = _candidate("fallback")
        fallback.capabilities = ["capacity:4"]
        decision = service.resolve(
            PlacementRequest(
                realm_id="default",
                fleet_id="fleet",
                policy=PlacementPolicy.BEST_MATCH,
                provider="codex",
                preferred_capabilities=["browser"],
            ),
            [fallback, preferred],
        )

        assert decision.chosen_instance_id == "preferred"
        assert {item["instance_id"] for item in decision.eligible_candidates} == {
            "fallback",
            "preferred",
        }
        assert decision.scores["preferred"]["capability_match"] == 1.0
        assert decision.scores["fallback"]["capability_match"] == 0.0
        fallback_detail = next(
            item
            for item in decision.eligible_candidates
            if item["instance_id"] == "fallback"
        )
        assert fallback_detail["missing_preferred_capabilities"] == ["browser"]


def test_required_capabilities_remain_hard_admission_requirements() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        service = PlacementService(RoundRobinCursorStore(Path(tmp)))
        candidate = _candidate("fallback")
        candidate.capabilities = ["capacity:4"]
        with pytest.raises(PlacementError) as raised:
            service.resolve(
                PlacementRequest(
                    realm_id="default",
                    fleet_id="fleet",
                    instance_id="fallback",
                    required_capabilities=["browser"],
                ),
                [candidate],
            )

        assert "capability_unavailable" in {
            code
            for item in raised.value.rejected_candidates
            for code in item["rejection_codes"]
        }


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
        assert decision.tie_breaking_reason == (
            "Lowest normalized execution-slot consumption (active capacity "
            "consumers plus durable dispatch reservations); queued prompts are "
            "backlog telemetry. Instance ID breaks exact ties."
        )
        assert "active-plus-queued" not in decision.tie_breaking_reason

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


def test_one_working_session_with_prompt_backlog_consumes_one_slot() -> None:
    counts = workload_counts(
        {
            "connected_runtimes": 6,
            "idle_sessions": 5,
            "prompting_turns": 1,
            "active_capacity_consumers": 1,
            "queued_prompts": 9,
            "dispatch_reservations": 0,
        }
    )
    assert counts == {
        "active": 1,
        "queued": 9,
        "reservations": 0,
        "consumed": 1,
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
                    "queued_prompts": 9,
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


def test_placement_ignores_same_session_backlog_and_deduplicates_consumers(
    tmp_path: Path,
) -> None:
    candidate = _candidate("backlogged", capacity=4)
    candidate.dispatch_capacity = 4
    candidate.dispatch_queue_capacity = 100
    candidate.activity = _fresh(
        {
            "state": "working",
            "active_capacity_consumers": 1,
            "queued_prompts": 9,
            "dispatch_reservations": 0,
            "capacity": {"limit": 4, "consumed": 10},
            "capacity_consumer_links": [
                {
                    "kind": "session",
                    "session_id": "session-1",
                    "state": "working",
                    "slots": 10,
                },
                {
                    "kind": "session",
                    "session_id": "session-1",
                    "state": "working",
                    "slots": 10,
                },
            ],
        }
    )

    decision = PlacementService(RoundRobinCursorStore(tmp_path)).resolve(
        _request(PlacementPolicy.BEST_MATCH), [candidate]
    )

    detail = decision.eligible_candidates[0]
    assert detail["active"] == 1
    assert detail["queued"] == 9
    assert detail["consumed"] == 1
    assert detail["execution_slot_available"] is True
    assert detail["admission_disposition"] == "launchable"
    assert detail["consumer_links"] == [
        {
            "kind": "session",
            "session_id": "session-1",
            "state": "working",
            "slots": 1,
            "consumer_id": "session:session-1",
        }
    ]


def test_placement_projects_working_session_after_queued_legacy_true(
    tmp_path: Path,
) -> None:
    candidate = _candidate("mixed-version", capacity=4)
    candidate.dispatch_capacity = 4
    candidate.activity = _fresh(
        {
            "state": "working",
            "active_capacity_consumers": 1,
            "queued_prompts": 9,
            "dispatch_reservations": 0,
            "sessions": [
                {
                    "id": "queued-backlog",
                    "status": "queued",
                    "capacity_consuming": True,
                },
                {"id": "working-now", "status": "working"},
            ],
        }
    )

    decision = PlacementService(RoundRobinCursorStore(tmp_path)).resolve(
        _request(PlacementPolicy.BEST_MATCH), [candidate]
    )

    detail = decision.eligible_candidates[0]
    assert detail["active"] == 1
    assert detail["queued"] == 9
    assert detail["consumed"] == 1
    assert detail["consumer_links"] == [
        {
            "kind": "session",
            "session_id": "working-now",
            "href": "/agent?session=working-now",
            "state": "working",
            "slots": 1,
            "consumer_id": "session:working-now",
        }
    ]
    assert detail["consumer_links_omitted"] == 0


def test_consumer_links_rank_working_state_over_queued_legacy_true() -> None:
    activity = normalize_activity_capacity(
        {
            "active_capacity_consumers": 1,
            "queued_prompts": 9,
            "sessions": [
                {
                    "id": "queued-backlog",
                    "status": "queued",
                    "capacity_consuming": True,
                },
                {"id": "working-now", "status": "working"},
            ],
        }
    )

    assert activity["capacity_consumer_links"] == [
        {
            "kind": "session",
            "session_id": "working-now",
            "href": "/agent?session=working-now",
            "state": "working",
            "slots": 1,
            "consumer_id": "session:working-now",
        }
    ]
    assert activity["capacity_consumer_links_omitted"] == 0


@pytest.mark.parametrize("state", [None, "unknown"])
def test_consumer_links_allow_explicit_stateless_true_fallback(
    state: str | None,
) -> None:
    session = {"id": "compat-fallback", "capacity_consuming": True}
    if state is not None:
        session["status"] = state
    activity = normalize_activity_capacity(
        {
            "active_capacity_consumers": 1,
            "sessions": [session],
        }
    )

    assert activity["capacity_consumer_links"] == [
        {
            "kind": "session",
            "session_id": "compat-fallback",
            "href": "/agent?session=compat-fallback",
            "state": state,
            "slots": 1,
            "consumer_id": "session:compat-fallback",
        }
    ]
    assert activity["capacity_consumer_links_omitted"] == 0


@pytest.mark.parametrize(
    ("state", "capacity_consuming", "expected_active"),
    [
        ("queued", False, False),
        ("queued", True, False),
        ("idle", True, False),
        ("deferred", True, False),
        ("connected", True, False),
        ("working", False, True),
        ("working", True, True),
        ("prompting", False, True),
        ("unknown", False, False),
    ],
)
def test_explicit_session_state_wins_over_capacity_consuming_flag(
    state: str,
    capacity_consuming: bool,
    expected_active: bool,
) -> None:
    activity = normalize_activity_capacity(
        {
            "active_capacity_consumers": 1,
            "sessions": [
                {
                    "id": "semantic-session",
                    "status": state,
                    "capacity_consuming": capacity_consuming,
                }
            ],
        }
    )

    links = activity["capacity_consumer_links"]
    assert bool(links) is expected_active
    assert activity["capacity_consumer_links_omitted"] == (0 if expected_active else 1)
    if expected_active:
        assert links[0]["consumer_id"] == "session:semantic-session"
        assert links[0]["state"] == state


def test_current_nonconsuming_state_suppresses_same_id_legacy_link() -> None:
    activity = normalize_activity_capacity(
        {
            "active_capacity_consumers": 1,
            "sessions": [
                {
                    "id": "same-session",
                    "status": "queued",
                    "capacity_consuming": True,
                }
            ],
            "capacity_consumer_links": [
                {
                    "kind": "session",
                    "session_id": "same-session",
                    "state": "working",
                    "slots": 10,
                }
            ],
        }
    )

    assert activity["capacity_consumer_links"] == []
    assert activity["capacity_consumer_links_omitted"] == 1


def test_legacy_links_rank_working_state_before_queued_true_fallback() -> None:
    activity = normalize_activity_capacity(
        {
            "active_capacity_consumers": 1,
            "capacity_consumer_links": [
                {
                    "kind": "session",
                    "session_id": "same-session",
                    "state": "queued",
                    "capacity_consuming": True,
                    "slots": 10,
                },
                {
                    "kind": "session",
                    "session_id": "same-session",
                    "state": "working",
                    "slots": 10,
                },
            ],
        }
    )

    assert activity["capacity_consumer_links"] == [
        {
            "kind": "session",
            "session_id": "same-session",
            "state": "working",
            "slots": 1,
            "consumer_id": "session:same-session",
        }
    ]
    assert activity["capacity_consumer_links_omitted"] == 0


def test_consumer_links_prefer_current_working_session_over_stale_legacy() -> None:
    activity = normalize_activity_capacity(
        {
            "active_capacity_consumers": 1,
            "capacity_consumer_links": [
                {
                    "kind": "session",
                    "session_id": "stale-idle",
                    "state": "idle",
                    "slots": 10,
                }
            ],
            "sessions": [{"id": "current-working", "status": "working"}],
        }
    )

    assert activity["capacity_consumer_links"] == [
        {
            "kind": "session",
            "session_id": "current-working",
            "href": "/agent?session=current-working",
            "state": "working",
            "slots": 1,
            "consumer_id": "session:current-working",
        }
    ]
    assert activity["capacity_consumer_links_omitted"] == 0


def test_consumer_links_keep_stateless_legacy_identity_without_sessions_list() -> None:
    activity = normalize_activity_capacity(
        {
            "active_capacity_consumers": 1,
            "capacity_consumer_links": [
                {"kind": "session", "session_id": "legacy-active", "slots": 10}
            ],
        }
    )

    assert activity["capacity_consumer_links"] == [
        {
            "kind": "session",
            "session_id": "legacy-active",
            "slots": 1,
            "consumer_id": "session:legacy-active",
        }
    ]
    assert activity["capacity_consumer_links_omitted"] == 0


def test_consumer_link_omission_counts_unprojectable_active_identity() -> None:
    activity = normalize_activity_capacity(
        {
            "active_capacity_consumers": 2,
            "capacity_consumer_links": [
                {
                    "kind": "session",
                    "session_id": "stale-idle",
                    "state": "idle",
                }
            ],
            "sessions": [
                {"id": "known-working", "status": "working"},
                {"status": "working"},
            ],
        }
    )

    assert [item["consumer_id"] for item in activity["capacity_consumer_links"]] == [
        "session:known-working"
    ]
    assert activity["capacity_consumer_link_count"] == 1
    assert activity["capacity_consumer_links_omitted"] == 1


@pytest.mark.parametrize(
    "state",
    [
        "completed",
        "cancelled",
        "failed",
        "running",
        "waiting_capacity",
        "blocked",
    ],
)
def test_terminal_and_nonreservation_legacy_dispatch_links_are_omitted(
    state: str,
) -> None:
    activity = normalize_activity_capacity(
        {
            "active_capacity_consumers": 0,
            "dispatch_reservations": 1,
            "capacity_consumer_links": [
                {
                    "kind": "dispatch",
                    "dispatch_id": f"legacy-{state}",
                    "state": state,
                    "slots": 10,
                }
            ],
        },
        authority_snapshot={
            "dispatch_reservations": 0,
            "dispatch_waiting": 0,
            "reservation_links": [],
        },
    )

    assert activity["capacity_consumer_links"] == []
    assert activity["capacity_consumer_link_count"] == 0
    assert activity["capacity_consumer_links_omitted"] == 1


@pytest.mark.parametrize(
    "state",
    [
        "queued",
        "checking_sync",
        "materializing",
        "provisioning",
        "starting_session",
        "delivering_prompt",
    ],
)
def test_active_legacy_reservation_link_projects_one_slot(state: str) -> None:
    activity = normalize_activity_capacity(
        {
            "active_capacity_consumers": 0,
            "dispatch_reservations": 1,
            "capacity_consumer_links": [
                {
                    "kind": "dispatch",
                    "dispatch_id": "legacy-reservation",
                    "state": state,
                    "slots": 10,
                }
            ],
        }
    )

    assert activity["capacity_consumer_links"] == [
        {
            "kind": "dispatch",
            "dispatch_id": "legacy-reservation",
            "state": state,
            "slots": 1,
            "consumer_id": "dispatch:legacy-reservation",
        }
    ]
    assert activity["capacity_consumer_links_omitted"] == 0


def test_authoritative_reservation_link_precedes_legacy_projection() -> None:
    activity = normalize_activity_capacity(
        {
            "active_capacity_consumers": 0,
            "dispatch_reservations": 1,
            "capacity_consumer_links": [
                {
                    "kind": "dispatch",
                    "dispatch_id": "reservation-1",
                    "href": "/legacy/reservation-1",
                    "state": "queued",
                    "slots": 10,
                }
            ],
        },
        authority_snapshot={
            "dispatch_reservations": 1,
            "dispatch_waiting": 0,
            "reservation_links": [
                {
                    "kind": "dispatch",
                    "dispatch_id": "reservation-1",
                    "href": "/authority/reservation-1",
                    "state": "materializing",
                    "slots": 1,
                }
            ],
        },
    )

    assert activity["capacity_consumer_links"] == [
        {
            "kind": "dispatch",
            "dispatch_id": "reservation-1",
            "href": "/authority/reservation-1",
            "state": "materializing",
            "slots": 1,
            "consumer_id": "dispatch:reservation-1",
        }
    ]
    assert activity["capacity_consumer_links_omitted"] == 0


def test_reservation_link_omission_counts_unverifiable_identity() -> None:
    activity = normalize_activity_capacity(
        {
            "active_capacity_consumers": 0,
            "dispatch_reservations": 2,
            "capacity_consumer_links": [
                {
                    "kind": "dispatch",
                    "dispatch_id": "known-reservation",
                    "state": "starting_session",
                },
                {
                    "kind": "dispatch",
                    "dispatch_id": "terminal-dispatch",
                    "state": "completed",
                },
                {"kind": "dispatch", "dispatch_id": "missing-state"},
            ],
        }
    )

    assert [item["consumer_id"] for item in activity["capacity_consumer_links"]] == [
        "dispatch:known-reservation"
    ]
    assert activity["capacity_consumer_link_count"] == 1
    assert activity["capacity_consumer_links_omitted"] == 1


def test_authority_overlay_normalizes_session_and_reservation_identities(
    tmp_path: Path,
) -> None:
    from pa.fleet.overview import build_overview
    from pa.fleet.workshop import build_workshop_snapshot
    from pa.modules.fleet import _placement_candidates

    target = FleetInstance(
        instance_id="remote",
        name="Remote",
        url="http://remote.test:8080",
        capabilities=["capacity:2"],
        dispatch_capacity=2,
    )
    legacy_activity = {
        "state": "working",
        "active_capacity_consumers": 1,
        "queued_prompts": 9,
        "dispatch_reservations": 0,
        "capacity": {"limit": 2, "consumed": 10},
        "provider_concurrency": {
            "codex": {
                "active_capacity_consumers": 1,
                "queued_prompts": 9,
                "dispatch_reservations": 0,
            }
        },
        "capacity_consumer_links": [
            {
                "kind": "session",
                "session_id": "session-1",
                "href": "/agent?session=session-1",
                "state": "working",
                "slots": 10,
            },
            {
                "kind": "session",
                "session_id": "session-1",
                "href": "/agent?session=session-1",
                "state": "working",
                "slots": 10,
            },
        ],
        "sessions": [
            {
                "id": "session-1",
                "realm_id": "default",
                "status": "working",
                "provider": "codex",
            }
        ],
        "dispatches": [],
    }
    ledger = DispatchStore(tmp_path)
    reservation = _record(
        key="authority-reservation",
        fingerprint="authority-reservation",
        target="remote",
        card_id="card-reserved",
    )
    reservation.dispatch_id = "dispatch-reserved"
    reservation.capacity_provider = "codex"
    ledger.put(reservation)

    class IndexedReadSentinel(dict):
        def values(self):
            raise AssertionError("capacity snapshot scanned the full dispatch ledger")

    original_records = ledger._records
    ledger._records = IndexedReadSentinel(original_records)
    indexed_snapshot = ledger.capacity_snapshot("remote")
    ledger._records = original_records
    assert indexed_snapshot["dispatch_reservations"] == 1
    assert [item["dispatch_id"] for item in indexed_snapshot["reservation_links"]] == [
        "dispatch-reserved"
    ]

    settings = Settings(
        data_dir=tmp_path,
        instance_id="authority",
        instance_name="Authority",
        instance_url="http://authority.test:8080",
    )
    ctx = MagicMock(settings=settings)
    ctx.services = {"dispatch_store": ledger}
    ctx.store.list_sessions.return_value = []
    ctx.store.list_repositories.return_value = []
    ctx.store.get_projection_head.return_value = "head"
    ctx.store.list_cards.return_value = []
    ctx.store.list_projects.return_value = []
    ctx.store.get_card.return_value = None
    ctx.store.count_cards.return_value = 0

    def cached_dimension(_cache, _instance, dimension):
        values = {
            "reachability": {"health": "up"},
            "activity": legacy_activity,
            "providers": [
                {
                    "id": "codex",
                    "available": True,
                    "auth_state": "authenticated",
                    "models": ["gpt-5"],
                }
            ],
            "mcp_bootstrap": {"classification": "ready"},
            "repositories": {"observations": [], "workspaces": []},
            "sync": {"consistent": True},
        }
        return _fresh(values.get(dimension, {}))

    with patch(
        "pa.fleet.overview._cached_or_default",
        side_effect=cached_dimension,
    ):
        overview = build_overview(ctx, [target], [])

    overview_activity = overview["nodes"][0]["dimensions"]["activity"]["value"]
    expected_links = [
        {
            "kind": "session",
            "session_id": "session-1",
            "href": "/agent?session=session-1",
            "state": "working",
            "slots": 1,
            "consumer_id": "session:session-1",
        },
        {
            "kind": "dispatch",
            "dispatch_id": "dispatch-reserved",
            "card_id": "card-reserved",
            "href": "/?card=card-reserved",
            "state": "queued",
            "slots": 1,
            "consumer_id": "dispatch:dispatch-reserved",
        },
    ]
    assert overview_activity["capacity"]["consumed"] == 2
    assert overview_activity["queued_prompts"] == 9
    assert overview_activity["capacity_consumer_links"] == expected_links
    assert overview_activity["capacity_consumer_links_omitted"] == 0

    workshop = build_workshop_snapshot(ctx, overview)
    assert workshop["bays"][0]["capacity"]["consumer_links"] == expected_links

    async def probe(_ctx, _instance, dimension, *, force=False):
        return cached_dimension(None, _instance, dimension)

    request = MagicMock()
    request.app.state.ctx = ctx
    with patch("pa.modules.fleet.probe_dimension", side_effect=probe):
        candidates = asyncio.run(_placement_candidates(request, [target]))

    candidate_activity = candidates[0].activity["value"]
    assert candidate_activity["capacity"]["consumed"] == 2
    assert candidate_activity["capacity_consumer_links"] == expected_links
    with pytest.raises(PlacementError) as raised:
        PlacementService(RoundRobinCursorStore(tmp_path)).resolve(
            _request(PlacementPolicy.BEST_MATCH), candidates
        )
    rejected = raised.value.rejected_candidates[0]
    assert rejected["consumed"] == 2
    assert rejected["reserved"] == 1
    assert rejected["queued"] == 9
    assert rejected["consumer_links"] == expected_links
    assert rejected["consumer_links_omitted"] == 0


def test_mixed_version_session_states_override_legacy_consumed_total() -> None:
    counts = workload_counts(
        {
            "active_sessions": 10,
            "queued_prompts": 9,
            "capacity": {"limit": 4, "consumed": 10},
            "sessions": [
                {"id": "working", "status": "working"},
                {"id": "idle", "status": "idle"},
            ],
        }
    )
    assert counts == {
        "active": 1,
        "queued": 9,
        "reservations": 0,
        "consumed": 1,
        "semantic_source": "legacy_session_states",
    }


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
        expired = restarted.get(replacement.dispatch_id)
        assert expired.state == "failed"
        assert expired.error_code == "capacity_reservation_timeout"
        assert expired.capacity_release_reason == "timeout"


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
