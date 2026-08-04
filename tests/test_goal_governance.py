from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pa.domain.projection import CardProjection
from pa.goals.advanced_models import (
    AllocationDisposition,
    GoalActionDisposition,
    GoalActionRequest,
    GoalActionRisk,
    GoalGovernancePolicy,
    GoalPortfolioReviewRequest,
    GoalProposalRequest,
    GoalResourceCapacity,
    GoalResourceClaim,
    GoalStrategy,
    GoalStrategyPortfolioUpdate,
    GoalUsage,
    GovernanceMutationContext,
    ProposalDisposition,
    ProposalKind,
    ProviderGoalAssignment,
    ProviderGoalMode,
    ProviderGoalProgress,
    ProviderRunState,
    ResourceAccess,
    StandingGoalPolicy,
)
from pa.goals.governance import GoalGovernanceConflict, GoalGovernanceService
from pa.goals.models import (
    GoalBudget,
    GoalCreate,
    GoalCriterion,
    GoalMutationContext,
    GoalPolicy,
    GoalRateLimit,
)
from pa.goals.service import GoalService
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore


class GoalGovernanceTests(unittest.TestCase):
    def _services(self, tmp: str, now: datetime | None = None):
        root = Path(tmp)
        objects = ObjectStore(root / "objects")
        log = EventLog(objects, root, "instance-a")
        authority = CardProjection(root / "authority.db", log)
        replica = CardProjection(root / "replica.db", log)
        goals = GoalService(authority, "instance-a")
        governance = GoalGovernanceService(
            authority, "instance-a", goals, clock=lambda: now or datetime.now(UTC)
        )
        return goals, governance, replica

    @staticmethod
    def _goal_data(
        objective: str,
        *,
        project_id: str = "project-a",
        allow_derived: bool = True,
    ) -> GoalCreate:
        return GoalCreate(
            project_id=project_id,
            objective=objective,
            criteria=[
                GoalCriterion(
                    description="verified result",
                    verification_method="independent test",
                    evidence_requirement="test evidence",
                )
            ],
            policy=GoalPolicy(
                autonomy_level=4,
                permitted_actions=["code.*", "provider.goal.assign", "resource.*"],
                prohibited_actions=["code.production.merge"],
                max_action_risk="medium",
                allowed_provider_ids=["codex", "cursor"],
                allow_derived_subgoals=allow_derived,
                auto_activate_derived_subgoals=allow_derived,
                max_subgoal_depth=2,
                max_derived_subgoals=2,
                proposal_cooldown_seconds=0,
                repository_scope=["petersky/pa"],
            ),
            budget=GoalBudget(
                max_cost_usd=10,
                max_tokens=2_000,
                max_api_calls=20,
                max_actions=20,
                max_dispatches=5,
                rate_limits=[
                    GoalRateLimit(
                        key="code.edit",
                        window_seconds=60,
                        max_actions=1,
                    )
                ],
            ),
        )

    @staticmethod
    def _goal_ctx(key: str) -> GoalMutationContext:
        return GoalMutationContext(
            actor_principal="user:operator",
            authority_instance_id="instance-a",
            idempotency_key=key,
            expected_version=0,
            policy_revision=1,
        )

    @staticmethod
    def _ctx(
        version: int,
        key: str,
        *,
        actor: str = "agent:supervisor",
        policy_revision: int = 1,
        goal_version: int | None = 1,
    ) -> GovernanceMutationContext:
        return GovernanceMutationContext(
            actor_principal=actor,
            authority_instance_id="instance-a",
            idempotency_key=key,
            expected_version=version,
            policy_revision=policy_revision,
            goal_version=goal_version,
        )

    def test_action_policy_reserves_budget_and_enforces_rate_and_risk(self) -> None:
        now = datetime(2026, 8, 3, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            goals, governance, _ = self._services(tmp, now)
            goal = goals.create(
                self._goal_data("Govern actions"), self._goal_ctx("create")
            )

            state, allowed = governance.authorize_action(
                goal.id,
                GoalActionRequest(
                    action_class="code.edit",
                    repository="petersky/pa",
                    estimate=GoalUsage(actions=1, cost_usd=2, tokens=100),
                ),
                self._ctx(0, "edit-1"),
            )
            self.assertEqual(allowed.disposition, GoalActionDisposition.AUTHORIZED)
            self.assertEqual(state.usage.cost_usd, 2)

            state, limited = governance.authorize_action(
                goal.id,
                GoalActionRequest(action_class="code.edit"),
                self._ctx(1, "edit-2"),
            )
            self.assertEqual(limited.disposition, GoalActionDisposition.RATE_LIMITED)

            state, denied = governance.authorize_action(
                goal.id,
                GoalActionRequest(action_class="code.production.merge"),
                self._ctx(2, "merge"),
            )
            self.assertEqual(denied.disposition, GoalActionDisposition.DENIED)

            _, approval = governance.authorize_action(
                goal.id,
                GoalActionRequest(
                    action_class="code.deploy",
                    risk=GoalActionRisk.HIGH,
                    reversible=False,
                ),
                self._ctx(3, "deploy"),
            )
            self.assertEqual(
                approval.disposition, GoalActionDisposition.REQUIRES_APPROVAL
            )

    def test_provider_adapter_uses_advertised_native_command_and_ingests_claims(
        self,
    ) -> None:
        now = datetime(2026, 8, 3, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            goals, governance, _ = self._services(tmp, now)
            goal = goals.create(
                self._goal_data("Run natively"), self._goal_ctx("create")
            )

            state, run, decision = governance.assign_provider(
                goal.id,
                ProviderGoalAssignment(
                    provider_id="codex",
                    available_commands=["goal"],
                    estimated_usage=GoalUsage(actions=1, cost_usd=1, tokens=100),
                ),
                self._ctx(0, "assign"),
            )
            self.assertEqual(decision.disposition, GoalActionDisposition.AUTHORIZED)
            assert run is not None
            self.assertEqual(run.invocation.mode, ProviderGoalMode.NATIVE)
            self.assertEqual(run.invocation.canonical_goal_id, goal.id)
            self.assertTrue(
                run.invocation.metadata["goal_packet"][
                    "provider_completion_is_evidence_claim_only"
                ]
                if "provider_completion_is_evidence_claim_only"
                in run.invocation.metadata["goal_packet"]
                else run.invocation.metadata["goal_packet"]["reporting"][
                    "provider_completion_is_evidence_claim_only"
                ]
            )

            state = governance.ingest_provider_progress(
                goal.id,
                ProviderGoalProgress(
                    run_id=run.id,
                    state=ProviderRunState.COMPLETED,
                    summary="Provider reports completion",
                    cumulative_usage=GoalUsage(
                        actions=1, cost_usd=1.25, tokens=120, api_calls=2
                    ),
                    evidence_claims=[{"criterion_id": goal.criteria[0].id}],
                ),
                self._ctx(state.version, "progress"),
            )
            self.assertEqual(state.usage.cost_usd, 1.25)
            self.assertEqual(len(goal.evidence), 0)
            self.assertEqual(
                state.provider_runs[0].evidence_claims[0]["criterion_id"],
                goal.criteria[0].id,
            )

    def test_derived_and_top_level_proposals_obey_activation_policy(self) -> None:
        now = datetime(2026, 8, 3, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            goals, governance, _ = self._services(tmp, now)
            parent = goals.create(self._goal_data("Parent"), self._goal_ctx("parent"))
            child_data = self._goal_data("Derived child")
            proposal = governance.propose_goal(
                GoalProposalRequest(
                    kind=ProposalKind.DERIVED_SUBGOAL,
                    goal=child_data,
                    category="engineering",
                    rationale="The criterion requires a separate workstream",
                    parent_goal_id=parent.id,
                    parent_criterion_id=parent.criteria[0].id,
                ),
                self._ctx(0, "derive"),
            )
            self.assertEqual(proposal.disposition, ProposalDisposition.AUTO_ACTIVATED)
            child = goals.get(proposal.activated_goal_id or "")
            assert child is not None
            self.assertEqual(child.parent_goal_id, parent.id)

            pending = governance.propose_goal(
                GoalProposalRequest(
                    kind=ProposalKind.TOP_LEVEL,
                    goal=self._goal_data("Proactive unmatched"),
                    category="unmatched",
                    rationale="A proactive opportunity was detected",
                ),
                self._ctx(0, "proactive"),
            )
            self.assertEqual(pending.disposition, ProposalDisposition.PENDING_REVIEW)

            policy = GoalGovernancePolicy(
                version=1,
                authored_by="user:operator",
                standing_goal_policies=[
                    StandingGoalPolicy(
                        categories=["engineering"],
                        project_ids=["project-a"],
                        max_cost_usd=10,
                        max_tokens=2_000,
                        expires_at=now + timedelta(days=1),
                    )
                ],
            )
            governance.put_policy(
                policy,
                self._ctx(
                    0,
                    "policy",
                    actor="user:operator",
                    goal_version=None,
                ),
            )
            automatic = governance.propose_goal(
                GoalProposalRequest(
                    kind=ProposalKind.TOP_LEVEL,
                    goal=self._goal_data("Proactive allowed"),
                    category="engineering",
                    rationale="Standing policy covers this bounded opportunity",
                ),
                self._ctx(0, "proactive-allowed", goal_version=None),
            )
            self.assertEqual(automatic.disposition, ProposalDisposition.AUTO_ACTIVATED)

    def test_portfolio_review_prioritizes_and_records_resource_conflicts(self) -> None:
        now = datetime(2026, 8, 3, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            goals, governance, _ = self._services(tmp, now)
            first = goals.create(self._goal_data("First"), self._goal_ctx("first"))
            second = goals.create(self._goal_data("Second"), self._goal_ctx("second"))
            governance.put_policy(
                GoalGovernancePolicy(
                    version=1,
                    authored_by="user:operator",
                    max_active_goals=1,
                    resource_capacities=[
                        GoalResourceCapacity(key="repository:petersky/pa", capacity=1)
                    ],
                ),
                self._ctx(0, "policy", actor="user:operator", goal_version=None),
            )
            first_state = governance.set_priority(
                first.id, 90, "deadline", self._ctx(0, "first-priority")
            )
            governance.set_priority(
                second.id, 10, "backlog", self._ctx(0, "second-priority")
            )
            first_state, reserved = governance.authorize_action(
                first.id,
                GoalActionRequest(
                    action_class="resource.reserve",
                    resource_claims=[
                        GoalResourceClaim(
                            key="repository:petersky/pa",
                            access=ResourceAccess.EXCLUSIVE,
                        )
                    ],
                ),
                self._ctx(first_state.version, "reserve-first"),
            )
            self.assertEqual(reserved.disposition, GoalActionDisposition.AUTHORIZED)
            _, conflict = governance.authorize_action(
                second.id,
                GoalActionRequest(
                    action_class="resource.reserve",
                    resource_claims=[
                        GoalResourceClaim(
                            key="repository:petersky/pa",
                            access=ResourceAccess.EXCLUSIVE,
                        )
                    ],
                ),
                self._ctx(1, "reserve-second"),
            )
            self.assertEqual(
                conflict.disposition, GoalActionDisposition.RESOURCE_CONFLICT
            )
            review = governance.review_portfolio(
                GoalPortfolioReviewRequest(
                    reviewer_principal="agent:critic",
                    explanation="Independent organization allocation review",
                ),
                self._ctx(
                    0,
                    "review",
                    actor="agent:supervisor",
                    goal_version=None,
                ),
            )
            allocations = {item.goal_id: item for item in review.allocations}
            self.assertEqual(
                allocations[first.id].disposition, AllocationDisposition.ACTIVE
            )
            self.assertEqual(
                allocations[second.id].disposition, AllocationDisposition.QUEUED
            )
            self.assertTrue(review.requires_operator_review)

    def test_governance_state_replays_to_replacement_projection(self) -> None:
        now = datetime(2026, 8, 3, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            goals, governance, replica = self._services(tmp, now)
            goal = goals.create(
                self._goal_data("Replay governance"), self._goal_ctx("create")
            )
            strategy = GoalStrategy(
                title="Bounded experiment",
                hypothesis="A focused implementation will satisfy the criterion",
                expected_outcome="Test evidence",
                allocated_cost_usd=2,
                allocated_tokens=200,
            )
            governance.update_strategies(
                goal.id,
                GoalStrategyPortfolioUpdate(
                    strategies=[strategy],
                    selected_strategy_ids=[strategy.id],
                    reason="Prefer the lowest-risk strategy",
                ),
                self._ctx(0, "strategy"),
            )

            replica.rebuild_from_log("default")
            replacement_goals = GoalService(replica, "instance-b")
            replacement = GoalGovernanceService(
                replica, "instance-b", replacement_goals, clock=lambda: now
            )
            restored = replacement.get_state(goal.id)
            self.assertEqual(restored.strategies[0].id, strategy.id)
            self.assertEqual(restored.version, 1)

    def test_subgoal_cannot_expand_parent_authority(self) -> None:
        now = datetime(2026, 8, 3, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            goals, governance, _ = self._services(tmp, now)
            parent = goals.create(self._goal_data("Parent"), self._goal_ctx("parent"))
            child = self._goal_data("Overreaching child")
            child.policy.repository_scope = ["another/repository"]
            with self.assertRaisesRegex(GoalGovernanceConflict, "repository_scope"):
                governance.propose_goal(
                    GoalProposalRequest(
                        kind=ProposalKind.DERIVED_SUBGOAL,
                        goal=child,
                        category="engineering",
                        rationale="Try to exceed the parent scope",
                        parent_goal_id=parent.id,
                        parent_criterion_id=parent.criteria[0].id,
                    ),
                    self._ctx(0, "overreach"),
                )


if __name__ == "__main__":
    unittest.main()
