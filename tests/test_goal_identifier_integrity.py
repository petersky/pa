from __future__ import annotations

import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from pa.domain.models import CardEvent, EventType
from pa.domain.projection import CardProjection
from pa.goals.advanced_models import (
    AllocationDisposition,
    GoalActionApply,
    GoalActionRequest,
    GoalActionReservation,
    GoalAutonomyState,
    GoalGovernancePolicy,
    GoalPortfolioEntry,
    GoalPortfolioReview,
    GoalProposalRequest,
    GoalRateWindow,
    GoalResourceCapacity,
    GoalResourceClaim,
    GoalStrategy,
    GoalStrategyPortfolioUpdate,
    GoalUsage,
    GovernanceMutationContext,
    ProposalKind,
    ProviderGoalAssignment,
    ProviderGoalInvocation,
    ProviderGoalMode,
    ProviderGoalProgress,
    ProviderGoalRun,
    ProviderRunState,
    StandingGoalPolicy,
    normalize_legacy_governance_payload,
)
from pa.goals.governance import GoalGovernanceService
from pa.goals.models import (
    CreateWorkPackageAction,
    EvidenceKind,
    Goal,
    GoalCreate,
    GoalCriterion,
    GoalEventRecord,
    GoalEvidence,
    GoalLease,
    GoalMutationContext,
    GoalPolicy,
    GoalRateLimit,
    GoalState,
    GoalSupervision,
    GoalSupervisionCheckpoint,
    GoalWakeup,
    GoalWorkPackage,
    normalize_legacy_goal_payload,
)
from pa.goals.projection import goal_projection_requires_legacy_id_rebuild
from pa.goals.service import GoalService
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore


def _criterion() -> GoalCriterion:
    return GoalCriterion(
        description="identifier integrity",
        verification_method="strict model validation",
        evidence_requirement="blank references rejected",
    )


def _goal_create() -> GoalCreate:
    return GoalCreate(
        objective="Keep identifiers attributable", criteria=[_criterion()]
    )


def _strategy() -> GoalStrategy:
    return GoalStrategy(
        title="Bound identifiers",
        hypothesis="Every reference is nonblank",
        expected_outcome="Malformed requests fail",
    )


def _invocation() -> ProviderGoalInvocation:
    return ProviderGoalInvocation(
        provider_id="codex",
        mode=ProviderGoalMode.RECOVERABLE_TURN,
        prompt="Do bounded work",
        canonical_goal_id="goal-one",
        policy_revision=1,
    )


@pytest.mark.parametrize(
    "factory",
    [
        lambda: Goal.model_validate(
            {
                "id": " ",
                "objective": "Reject a blank top-level id",
                "criteria": [_criterion().model_dump(mode="json")],
            }
        ),
        lambda: GoalCreate(
            project_id=" ", objective="Reject blank project", criteria=[_criterion()]
        ),
        lambda: GoalPolicy(allowed_provider_ids=[" "]),
        lambda: GoalRateLimit(key=" ", max_actions=1),
        lambda: GoalLease(holder_instance_id=" "),
        lambda: GoalLease(claim_id=" "),
        lambda: GoalLease(eligible_instance_ids=[" "]),
        lambda: GoalWakeup(
            wake_at=datetime.now(UTC), reason="resume", claimed_by_instance_id=" "
        ),
        lambda: GoalMutationContext(
            actor_principal="agent:test",
            authority_instance_id=" ",
            idempotency_key="key",
            expected_version=1,
            policy_revision=1,
        ),
        lambda: GoalMutationContext(
            actor_principal="agent:test",
            authority_instance_id="instance-a",
            idempotency_key=" ",
            expected_version=1,
            policy_revision=1,
        ),
        lambda: GoalEvidence(
            criterion_ids=["criterion-one"],
            kind=EvidenceKind.TEST,
            summary="reject blank producer",
            recorded_by_instance_id=" ",
        ),
        lambda: CreateWorkPackageAction(
            title="package",
            objective="reject blank card",
            criterion_ids=["criterion-one"],
            card_id=" ",
        ),
        lambda: GoalWorkPackage(
            proposal_id="proposal-one",
            title="package",
            objective="reject blank runtime ids",
            criterion_ids=["criterion-one"],
            dispatch_ids=[" "],
        ),
        lambda: GoalSupervision(controller_session_id=" "),
        lambda: GoalSupervisionCheckpoint(
            criteria=[],
            evidence=[],
            proposals=[],
            work_packages=[],
            operator_interactions=[],
            supervision=GoalSupervision(),
            linked_card_ids=[" "],
            linked_dispatch_ids=[],
            assumptions=[],
            risks=[],
            strategy_revision=1,
            state=GoalState.DRAFT,
            reason="reject blank checkpoint references",
        ),
        lambda: GoalEventRecord(
            id="event-one",
            goal_id="goal-one",
            event_type="goal.tested",
            actor_principal="agent:test",
            authority_instance_id=" ",
            policy_revision=1,
            idempotency_key="event-key",
            version=1,
            created_at=datetime.now(UTC),
        ),
    ],
)
def test_core_goal_identifier_surfaces_reject_whitespace(factory) -> None:
    with pytest.raises(ValidationError):
        factory()


@pytest.mark.parametrize(
    "factory",
    [
        lambda: GoalActionApply(reservation_id=" "),
        lambda: GoalActionRequest(action_class=" "),
        lambda: GoalResourceClaim(key=" "),
        lambda: GoalActionRequest(action_class="test", provider_id=" "),
        lambda: GoalStrategy(
            id=" ",
            title="strategy",
            hypothesis="reject blank",
            expected_outcome="validation error",
        ),
        lambda: GoalStrategyPortfolioUpdate(
            strategies=[_strategy()],
            selected_strategy_ids=[" "],
            reason="reject blank selection",
        ),
        lambda: ProviderGoalAssignment(provider_id=" "),
        lambda: ProviderGoalInvocation(
            provider_id="codex",
            mode=ProviderGoalMode.RECOVERABLE_TURN,
            command_name=" ",
            prompt="reject blank command",
            canonical_goal_id="goal-one",
            policy_revision=1,
        ),
        lambda: ProviderGoalProgress(
            run_id=" ", state=ProviderRunState.RUNNING, summary="invalid run"
        ),
        lambda: GoalProposalRequest(
            kind=ProposalKind.DERIVED_SUBGOAL,
            goal=_goal_create(),
            category="maintenance",
            rationale="reject blank parent",
            parent_goal_id=" ",
            parent_risk="low",
        ),
        lambda: GoalPortfolioEntry(
            goal_id=" ",
            priority_score=1,
            disposition=AllocationDisposition.ACTIVE,
            reasons=["invalid reference"],
        ),
        lambda: StandingGoalPolicy(
            id=" ",
            categories=["maintenance"],
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        ),
        lambda: GoalGovernancePolicy(
            provider_rate_limits={" ": [GoalRateLimit(key="calls", max_actions=1)]}
        ),
        lambda: GoalResourceCapacity(key=" ", capacity=1),
        lambda: GoalGovernancePolicy(authored_by=" "),
        lambda: GovernanceMutationContext(
            actor_principal="agent:test",
            authority_instance_id=" ",
            idempotency_key="key",
            expected_version=0,
            policy_revision=1,
        ),
        lambda: GoalRateWindow(
            key=" ", started_at=datetime.now(UTC), usage=GoalUsage()
        ),
        lambda: GoalActionReservation(
            idempotency_key=" ",
            decision_id="decision-one",
            goal_id="goal-one",
            action_class="test",
            actor_principal="agent:test",
            authority_instance_id="instance-a",
            policy_revision=1,
            goal_version=1,
            request=GoalActionRequest(action_class="test"),
        ),
        lambda: ProviderGoalRun(
            goal_id="goal-one",
            provider_id="codex",
            invocation=_invocation(),
            blocker_refs=[" "],
        ),
        lambda: GoalAutonomyState(goal_id=" "),
        lambda: GoalPortfolioReview(
            realm_id="default",
            governance_policy_id="organization",
            governance_policy_version=1,
            reviewer_principal="agent:reviewer",
            independent=True,
            explanation="reject blank pending proposal",
            allocations=[],
            pending_proposal_ids=[" "],
        ),
    ],
)
def test_advanced_goal_identifier_surfaces_reject_whitespace(factory) -> None:
    with pytest.raises(ValidationError):
        factory()


def test_legacy_normalization_does_not_repair_unrelated_malformed_types() -> None:
    criterion = _criterion().model_dump(mode="json")
    criterion["id"] = 123
    with pytest.raises(ValidationError):
        Goal.model_validate(
            {
                "id": "goal-one",
                "objective": "Do not coerce malformed ids",
                "criteria": [criterion],
            }
        )

    with pytest.raises(ValidationError):
        Goal.model_validate(
            {
                "id": "goal-one",
                "objective": "Do not synthesize required action references",
                "criteria": [_criterion().model_dump(mode="json")],
                "proposals": [
                    {
                        "id": "proposal-one",
                        "proposer_principal": "agent:test",
                        "proposer_role": "coordinator",
                        "action": {"kind": "dispatch_work_package"},
                        "rationale": "missing work package",
                        "expected_goal_version": 1,
                        "policy_revision": 1,
                    }
                ],
            }
        )

    with pytest.raises(ValidationError):
        Goal.model_validate(
            {
                "id": "goal-one",
                "objective": "Do not synthesize notification references",
                "criteria": [_criterion().model_dump(mode="json")],
                "proposals": [
                    {
                        "id": "proposal-one",
                        "proposer_principal": "agent:test",
                        "proposer_role": "coordinator",
                        "action": {
                            "kind": "request_operator",
                            "prompt": "Approve?",
                            "allow_freeform": True,
                        },
                        "rationale": "operator decision",
                        "expected_goal_version": 1,
                        "policy_revision": 1,
                    }
                ],
                "operator_interactions": [
                    {"id": "interaction-one", "proposal_id": "proposal-one"}
                ],
            }
        )


def test_new_goal_and_nested_ids_remain_fresh() -> None:
    goals = [Goal(**_goal_create().model_dump(mode="python")) for _ in range(200)]
    assert len({item.id for item in goals}) == 200
    assert len({item.criteria[0].id for item in goals}) == 200


def test_long_provider_identity_remains_round_trippable() -> None:
    provider_id = "p" * 200
    run = ProviderGoalRun(
        goal_id="goal-one",
        provider_id=provider_id,
        invocation=ProviderGoalInvocation(
            provider_id=provider_id,
            mode=ProviderGoalMode.RECOVERABLE_TURN,
            prompt="Exercise the longest accepted provider id",
            canonical_goal_id="goal-one",
            policy_revision=1,
        ),
    )

    assert len(run.executor_principal or "") <= 200
    assert len(run.reservation_id or "") <= 200
    assert ProviderGoalRun.model_validate(run.model_dump()) == run


def test_blank_legacy_governance_singletons_keep_canonical_projection_keys() -> None:
    now = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        log = EventLog(ObjectStore(root / "objects"), root, "instance-a")
        projection = CardProjection(root / "projection.db", log)
        services = GoalGovernanceService(
            projection,
            "instance-a",
            GoalService(projection, "instance-a"),
            clock=lambda: now,
        )
        policy = GoalGovernancePolicy(updated_at=now).model_dump(mode="json")
        policy["id"] = " "
        review = GoalPortfolioReview(
            governance_policy_id="organization",
            governance_policy_version=1,
            reviewer_principal="agent:reviewer",
            independent=True,
            explanation="Legacy singleton key",
            allocations=[],
            created_at=now,
        ).model_dump(mode="json")
        review["id"] = " "

        for index, (entity_type, entity) in enumerate(
            (
                ("goal_governance_policy", policy),
                ("goal_portfolio_review", review),
            ),
            start=1,
        ):
            projection.apply_event(
                CardEvent(
                    id=f"legacy-singleton-{index}",
                    type=EventType.GOAL_GOVERNANCE_UPSERTED,
                    realm_id="default",
                    author_principal="agent:legacy",
                    author_instance="instance-a",
                    timestamp=now,
                    payload={
                        "entity_type": entity_type,
                        "entity_id": " ",
                        "entity": entity,
                        "governance_event": {
                            "event_type": f"{entity_type}.legacy",
                            "actor_principal": "agent:legacy",
                            "authority_instance_id": "instance-a",
                            "policy_revision": 1,
                            "idempotency_key": f"legacy-singleton-{index}",
                            "version": 1,
                        },
                    },
                )
            )

        assert services.get_policy("default") is not None
        assert services.get_latest_review("default") is not None


def test_governance_singletons_ignore_noncanonical_legacy_physical_keys() -> None:
    now = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        projection = CardProjection(
            root / "projection.db",
            EventLog(ObjectStore(root / "objects"), root, "instance-a"),
        )
        policy = GoalGovernancePolicy(updated_at=now).model_dump(mode="json")
        review = GoalPortfolioReview(
            governance_policy_id="organization",
            governance_policy_version=1,
            reviewer_principal="agent:reviewer",
            independent=True,
            explanation="Canonical singleton physical key",
            allocations=[],
            created_at=now,
        ).model_dump(mode="json")
        for index, (entity_type, wrong_key, entity) in enumerate(
            (
                ("goal_governance_policy", "wrong-policy", policy),
                ("goal_portfolio_review", "wrong-current", review),
            ),
            start=1,
        ):
            projection.apply_event(
                CardEvent(
                    id=f"wrong-singleton-{index}",
                    type=EventType.GOAL_GOVERNANCE_UPSERTED,
                    realm_id="default",
                    author_principal="agent:legacy",
                    author_instance="instance-a",
                    timestamp=now,
                    payload={
                        "entity_type": entity_type,
                        "entity_id": wrong_key,
                        "entity": entity,
                        "governance_event": {"version": 1},
                    },
                )
            )

        services = GoalGovernanceService(
            projection,
            "instance-a",
            GoalService(projection, "instance-a"),
            clock=lambda: now,
        )
        assert services.get_policy("default") is not None
        assert services.get_latest_review("default") is not None
        with projection._conn() as conn:
            keys = {
                (row["entity_type"], row["id"])
                for row in conn.execute(
                    """SELECT entity_type, id
                       FROM durable_goal_governance_entities"""
                ).fetchall()
            }
        assert keys == {
            ("goal_governance_policy", "organization"),
            ("goal_portfolio_review", "current"),
        }


def test_governance_entities_use_their_canonical_payload_identity() -> None:
    now = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        projection = CardProjection(
            root / "projection.db",
            EventLog(ObjectStore(root / "objects"), root, "instance-a"),
        )
        proposal_id = "canonical-proposal"
        projection.apply_event(
            CardEvent(
                id="wrong-proposal-key",
                type=EventType.GOAL_GOVERNANCE_UPSERTED,
                realm_id="default",
                author_principal="agent:legacy",
                author_instance="instance-a",
                timestamp=now,
                payload={
                    "entity_type": "goal_proposal",
                    "entity_id": "wrong-proposal",
                    "entity": {
                        "id": proposal_id,
                        "realm_id": "default",
                        "request": {
                            "kind": "top_level",
                            "goal": _goal_create().model_dump(mode="json"),
                            "category": "maintenance",
                            "rationale": "Canonical payload identity",
                            "parent_risk": "low",
                        },
                        "proposed_by": "agent:legacy",
                        "updated_at": now.isoformat(),
                    },
                    "governance_event": {"version": 1},
                },
            )
        )
        services = GoalGovernanceService(
            projection,
            "instance-a",
            GoalService(projection, "instance-a"),
            clock=lambda: now,
        )

        assert services.get_proposal(proposal_id, realm_id="default") is not None
        assert services.get_proposal("wrong-proposal", realm_id="default") is None
        with projection._conn() as conn:
            key = conn.execute(
                """SELECT id FROM durable_goal_governance_entities
                   WHERE entity_type='goal_proposal'"""
            ).fetchone()["id"]
        assert key == proposal_id


def test_projection_rejects_numeric_fallback_identifiers() -> None:
    now = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        projection = CardProjection(
            root / "projection.db",
            EventLog(ObjectStore(root / "objects"), root, "instance-a"),
        )
        raw_goal = Goal(**_goal_create().model_dump(mode="python")).model_dump(
            mode="json"
        )
        raw_goal["id"] = " "
        projection.apply_event(
            CardEvent(
                id="numeric-goal-fallback",
                type=EventType.GOAL_UPSERTED,
                realm_id="default",
                author_principal="agent:legacy",
                author_instance="instance-a",
                timestamp=now,
                payload={
                    "goal": raw_goal,
                    "goal_event": {"goal_id": 123, "version": 1},
                },
            )
        )
        projection.apply_event(
            CardEvent(
                id="numeric-governance-fallback",
                type=EventType.GOAL_GOVERNANCE_UPSERTED,
                realm_id="default",
                author_principal="agent:legacy",
                author_instance="instance-a",
                timestamp=now,
                payload={
                    "entity_type": "goal_autonomy",
                    "entity_id": 456,
                    "entity": {
                        "goal_id": " ",
                        "realm_id": "default",
                        "updated_at": now.isoformat(),
                    },
                },
            )
        )

        with projection._conn() as conn:
            assert conn.execute("SELECT COUNT(*) FROM durable_goals").fetchone()[0] == 0
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM durable_goal_governance_entities"
                ).fetchone()[0]
                == 0
            )


def test_projection_event_identity_distinguishes_blank_legacy_entities() -> None:
    now = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        projection = CardProjection(
            root / "projection.db",
            EventLog(ObjectStore(root / "objects"), root, "instance-a"),
        )
        raw_goal = Goal(**_goal_create().model_dump(mode="python")).model_dump(
            mode="json"
        )
        raw_goal["id"] = " "
        for event_id in ("blank-goal-a", "blank-goal-b"):
            projection.apply_event(
                CardEvent(
                    id=event_id,
                    type=EventType.GOAL_UPSERTED,
                    realm_id="default",
                    author_principal="agent:legacy",
                    author_instance="instance-a",
                    timestamp=now,
                    payload={
                        "goal": raw_goal,
                        "goal_event": {"goal_id": " ", "version": 1},
                    },
                )
            )

        proposals = []
        for event_id in ("blank-proposal-a", "blank-proposal-b"):
            payload = {
                "id": " ",
                "realm_id": "default",
                "request": {
                    "kind": "top_level",
                    "goal": _goal_create().model_dump(mode="json"),
                    "category": "maintenance",
                    "rationale": "distinct legacy proposal",
                    "parent_risk": "low",
                },
                "proposed_by": "agent:legacy",
                "updated_at": now.isoformat(),
            }
            projection.apply_event(
                CardEvent(
                    id=event_id,
                    type=EventType.GOAL_GOVERNANCE_UPSERTED,
                    realm_id="default",
                    author_principal="agent:legacy",
                    author_instance="instance-a",
                    timestamp=now,
                    payload={
                        "entity_type": "goal_proposal",
                        "entity_id": " ",
                        "entity": payload,
                        "governance_event": {"version": 1},
                    },
                )
            )
            proposals.append(event_id)

        goals = GoalService(projection, "instance-a").list()
        assert len(goals) == 2
        assert len({item.id for item in goals}) == 2
        with projection._conn() as conn:
            rows = conn.execute(
                """SELECT id FROM durable_goal_governance_entities
                   WHERE entity_type='goal_proposal'"""
            ).fetchall()
        assert len(rows) == 2
        assert len({row["id"] for row in rows}) == 2


def test_goal_projection_upgrade_detects_keys_and_rebuilds_without_duplicates() -> None:
    now = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        log = EventLog(ObjectStore(root / "objects"), root, "instance-a")
        db_path = root / "projection.db"
        projection = CardProjection(db_path, log)
        canonical_id = "canonical-legacy-goal"
        raw_goal = Goal(**_goal_create().model_dump(mode="python")).model_dump(
            mode="json"
        )
        raw_goal["id"] = " "
        projection.commit_event(
            CardEvent(
                id="legacy-canonical-event",
                type=EventType.GOAL_UPSERTED,
                realm_id="default",
                author_principal="agent:legacy",
                author_instance="instance-a",
                timestamp=now,
                payload={
                    "goal": raw_goal,
                    "goal_event": {"goal_id": canonical_id, "version": 1},
                },
            )
        )
        with projection._conn() as conn:
            canonical_payload = conn.execute(
                "SELECT payload FROM durable_goals WHERE id=?", (canonical_id,)
            ).fetchone()["payload"]
            conn.execute(
                """INSERT INTO durable_goals
                   SELECT ' ', realm_id, project_id, state, owner_principal,
                          revision, version, policy_revision, next_wake_at,
                          updated_at, payload
                   FROM durable_goals WHERE id=?""",
                (canonical_id,),
            )
            conn.execute(
                """INSERT INTO durable_goal_projection_heads
                   SELECT realm_id, entity_type, ' ', version, payload_hash,
                          event_id, event_timestamp
                   FROM durable_goal_projection_heads
                   WHERE entity_type='goal' AND entity_id=?""",
                (canonical_id,),
            )
            assert goal_projection_requires_legacy_id_rebuild(conn)
            # A canonical payload under the wrong physical key must itself be
            # enough to request the rebuild.
            assert canonical_id in canonical_payload

        rebuilt = CardProjection(db_path, log)
        with rebuilt._conn() as conn:
            goal_keys = [
                row["id"]
                for row in conn.execute(
                    "SELECT id FROM durable_goals ORDER BY id"
                ).fetchall()
            ]
            head_keys = [
                row["entity_id"]
                for row in conn.execute(
                    """SELECT entity_id FROM durable_goal_projection_heads
                       WHERE entity_type='goal' ORDER BY entity_id"""
                ).fetchall()
            ]
        assert goal_keys == [canonical_id]
        assert head_keys == [canonical_id]
        assert [item.id for item in GoalService(rebuilt, "instance-a").list()] == [
            canonical_id
        ]


def test_mixed_legacy_identity_graphs_fail_closed() -> None:
    stamp = "2026-01-02T03:04:05Z"
    criterion = _criterion()
    raw_goal = {
        "id": "mixed-goal",
        "objective": "Do not guess mixed proposal references",
        "created_at": stamp,
        "updated_at": stamp,
        "criteria": [criterion.model_dump(mode="json")],
        "proposals": [
            {
                "id": "proposal-known",
                "proposer_principal": "agent:legacy",
                "proposer_role": "coordinator",
                "action": {
                    "kind": "create_work_package",
                    "title": "known",
                    "objective": "known",
                    "criterion_ids": [criterion.id],
                },
                "rationale": "known proposal",
                "expected_goal_version": 1,
                "policy_revision": 1,
            },
            {
                "id": " ",
                "proposer_principal": "agent:legacy",
                "proposer_role": "coordinator",
                "action": {
                    "kind": "create_work_package",
                    "title": "legacy",
                    "objective": "legacy",
                    "criterion_ids": [criterion.id],
                },
                "rationale": "blank proposal",
                "expected_goal_version": 1,
                "policy_revision": 1,
            },
        ],
        "work_packages": [
            {
                "id": "work-a",
                "proposal_id": " ",
                "title": "first",
                "objective": "first",
                "criterion_ids": [criterion.id],
            },
            {
                "id": "work-b",
                "proposal_id": "\t",
                "title": "second",
                "objective": "second",
                "criterion_ids": [criterion.id],
            },
        ],
    }
    with pytest.raises(ValidationError):
        Goal.model_validate(normalize_legacy_goal_payload(raw_goal))

    raw_autonomy = {
        "goal_id": "goal-one",
        "strategies": [
            _strategy()
            .model_copy(update={"id": "strategy-known"})
            .model_dump(mode="json"),
            _strategy().model_copy(update={"id": " "}).model_dump(mode="json"),
        ],
        "selected_strategy_ids": ["", " "],
        "updated_at": stamp,
    }
    with pytest.raises(ValidationError):
        GoalAutonomyState.model_validate(
            normalize_legacy_governance_payload(
                "goal_autonomy", "goal-one", raw_autonomy
            )
        )
    strategy = _strategy()
    with pytest.raises(ValidationError):
        GoalAutonomyState(
            goal_id="goal-one",
            strategies=[strategy],
            selected_strategy_ids=[strategy.id, strategy.id],
        )


def test_legacy_governance_ids_are_repaired_only_by_durable_decoder() -> None:
    stamp = "2026-01-02T03:04:05Z"
    raw = {
        "goal_id": " ",
        "realm_id": " ",
        "strategies": [
            {
                "id": "",
                "title": "first",
                "hypothesis": "first hypothesis",
                "expected_outcome": "first outcome",
            },
            {
                "id": " ",
                "title": "second",
                "hypothesis": "second hypothesis",
                "expected_outcome": "second outcome",
            },
        ],
        "selected_strategy_ids": ["", " "],
        "provider_runs": [
            {
                "id": "",
                "goal_id": " ",
                "provider_id": "codex",
                "invocation": {
                    "provider_id": "codex",
                    "mode": "recoverable_turn",
                    "prompt": "legacy provider work",
                    "canonical_goal_id": " ",
                    "policy_revision": 1,
                },
                "executor_principal": " ",
                "authority_instance_id": " ",
                "reservation_id": " ",
                "blocker_refs": ["", "blocker-one"],
                "created_at": stamp,
                "updated_at": stamp,
            }
        ],
        "recent_decisions": [
            {
                "id": "",
                "goal_id": " ",
                "action_class": "test",
                "disposition": "authorized",
                "reasons": ["legacy decision"],
                "policy_revision": 1,
                "request": {"action_class": "test"},
                "decided_by": "agent:legacy",
                "authority_instance_id": " ",
                "reservation_id": " ",
                "decided_at": stamp,
            },
            {
                "id": " ",
                "goal_id": "\t",
                "action_class": "test",
                "disposition": "authorized",
                "reasons": ["second legacy decision"],
                "policy_revision": 1,
                "request": {"action_class": "test"},
                "decided_by": "agent:legacy",
                "authority_instance_id": "",
                "reservation_id": "",
                "decided_at": stamp,
            },
        ],
        "action_reservations": [
            {
                "id": "",
                "idempotency_key": " ",
                "decision_id": "",
                "goal_id": " ",
                "action_class": "test",
                "actor_principal": "agent:legacy",
                "authority_instance_id": "instance-a",
                "policy_revision": 1,
                "goal_version": 1,
                "request": {"action_class": "test"},
                "created_at": stamp,
            },
            {
                "id": " ",
                "decision_id": " ",
                "goal_id": "\n",
                "action_class": "test",
                "actor_principal": "agent:legacy",
                "authority_instance_id": "instance-a",
                "policy_revision": 1,
                "goal_version": 1,
                "request": {"action_class": "test"},
                "created_at": stamp,
            },
        ],
        "derived_goal_ids": ["", "derived-goal"],
        "updated_at": stamp,
    }

    normalized = normalize_legacy_governance_payload(
        "goal_autonomy", "goal-one", raw, realm_id="default"
    )
    repeated = normalize_legacy_governance_payload(
        "goal_autonomy", "goal-one", raw, realm_id="default"
    )
    assert normalized == repeated
    state = GoalAutonomyState.model_validate(normalized)

    assert state.goal_id == "goal-one"
    assert state.realm_id == "default"
    assert len({item.id for item in state.strategies}) == 2
    assert state.selected_strategy_ids == [item.id for item in state.strategies]
    assert state.derived_goal_ids == ["derived-goal"]
    assert state.provider_runs[0].goal_id == "goal-one"
    assert state.provider_runs[0].authority_instance_id == "legacy"
    assert state.provider_runs[0].executor_principal.strip()
    assert state.provider_runs[0].reservation_id.strip()
    assert state.provider_runs[0].blocker_refs == ["blocker-one"]
    assert len({item.id for item in state.recent_decisions}) == 2
    assert len({item.id for item in state.action_reservations}) == 2
    assert [item.reservation_id for item in state.recent_decisions] == [
        item.id for item in state.action_reservations
    ]
    assert [item.decision_id for item in state.action_reservations] == [
        item.id for item in state.recent_decisions
    ]
    assert all(item.idempotency_key.strip() for item in state.action_reservations)
