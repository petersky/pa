from __future__ import annotations

import inspect
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import CardCreate
from pa.domain.store import reset_store
from pa.execution.dispatch import (
    ConcurrentCardDispatch,
    DispatchIdempotencyConflict,
    DispatchRecord,
    DispatchStore,
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
