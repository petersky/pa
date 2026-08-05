from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pa.domain.models import CardEvent, EventType
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
    Goal,
    GoalActorRole,
    GoalBudget,
    GoalCreate,
    GoalCriterion,
    GoalMutationContext,
    GoalPolicy,
    GoalRateLimit,
    GoalRevision,
    GoalWakeup,
)
from pa.goals.service import GoalConflict, GoalService
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
        fence: int | None = None,
    ) -> GovernanceMutationContext:
        return GovernanceMutationContext(
            actor_principal=actor,
            authority_instance_id="instance-a",
            idempotency_key=key,
            expected_version=version,
            policy_revision=policy_revision,
            goal_version=goal_version,
            fencing_token=fence,
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

    def test_repository_data_provider_deadline_budget_and_concurrency_are_hard_gates(
        self,
    ) -> None:
        now = datetime(2026, 8, 3, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            goals, governance, _ = self._services(tmp, now)
            goal = goals.create(
                self._goal_data("Enforce every action envelope"),
                self._goal_ctx("create-envelope-goal"),
            )
            cases = [
                GoalActionRequest(
                    action_class="code.edit", repository="other/repository"
                ),
                GoalActionRequest(
                    action_class="code.edit", data_scope="private-dataset"
                ),
                GoalActionRequest(
                    action_class="provider.goal.assign", provider_id="unapproved"
                ),
            ]
            state = governance.get_state(goal.id)
            for index, request in enumerate(cases):
                state, decision = governance.authorize_action(
                    goal.id,
                    request,
                    self._ctx(state.version, f"scope-denial-{index}"),
                )
                self.assertEqual(decision.disposition, GoalActionDisposition.DENIED)

            state, over_budget = governance.authorize_action(
                goal.id,
                GoalActionRequest(
                    action_class="code.test",
                    estimate=GoalUsage(actions=1, cost_usd=11),
                ),
                self._ctx(state.version, "over-cost-budget"),
            )
            self.assertEqual(
                over_budget.disposition, GoalActionDisposition.BUDGET_EXHAUSTED
            )
            state, held = governance.authorize_action(
                goal.id,
                GoalActionRequest(action_class="code.test"),
                self._ctx(state.version, "hold-concurrency"),
            )
            self.assertEqual(held.disposition, GoalActionDisposition.AUTHORIZED)
            state, concurrent = governance.authorize_action(
                goal.id,
                GoalActionRequest(action_class="code.review"),
                self._ctx(state.version, "exceed-concurrency"),
            )
            self.assertEqual(
                concurrent.disposition, GoalActionDisposition.BUDGET_EXHAUSTED
            )

            expired_data = self._goal_data("Reject actions after deadline")
            expired_data.budget.deadline = now - timedelta(seconds=1)
            expired = goals.create(expired_data, self._goal_ctx("create-expired-goal"))
            _, deadline = governance.authorize_action(
                expired.id,
                GoalActionRequest(action_class="code.test"),
                self._ctx(0, "expired-deadline"),
            )
            self.assertEqual(
                deadline.disposition, GoalActionDisposition.BUDGET_EXHAUSTED
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
            goal = goals.acquire_lease(
                goal.id,
                GoalMutationContext(
                    actor_principal="agent:supervisor",
                    authority_instance_id="instance-a",
                    idempotency_key="provider-lease",
                    expected_version=goal.version,
                    policy_revision=goal.policy.revision,
                ),
                ttl_seconds=120,
            )

            state, run, decision = governance.assign_provider(
                goal.id,
                ProviderGoalAssignment(
                    provider_id="codex",
                    available_commands=["goal"],
                    estimated_usage=GoalUsage(actions=1, cost_usd=1, tokens=100),
                ),
                self._ctx(
                    0,
                    "assign",
                    goal_version=goal.version,
                    fence=goal.lease.fencing_token,
                ),
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

            state, run, launch = governance.launch_provider(
                goal.id,
                run.id,
                self._ctx(
                    state.version,
                    "launch",
                    goal_version=goal.version,
                    fence=goal.lease.fencing_token,
                ),
            )
            self.assertEqual(launch.disposition, GoalActionDisposition.AUTHORIZED)
            self.assertIsNotNone(run.launched_at)

            with self.assertRaisesRegex(
                GoalGovernanceConflict, "assigned service identity"
            ):
                governance.ingest_provider_progress(
                    goal.id,
                    ProviderGoalProgress(
                        run_id=run.id,
                        state=ProviderRunState.RUNNING,
                        summary="Spoofed provider update",
                    ),
                    self._ctx(
                        state.version,
                        "spoofed-progress",
                        actor="service:goal-executor:codex:spoofed",
                        goal_version=goal.version,
                        fence=run.fencing_token,
                    ),
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
                self._ctx(
                    state.version,
                    "progress",
                    actor=run.executor_principal,
                    goal_version=goal.version,
                    fence=run.fencing_token,
                ),
            )
            self.assertEqual(state.usage.cost_usd, 1.25)
            self.assertEqual(len(goal.evidence), 0)
            self.assertEqual(
                state.provider_runs[0].evidence_claims[0]["criterion_id"],
                goal.criteria[0].id,
            )
            reservation = next(
                item
                for item in state.action_reservations
                if item.id == run.reservation_id
            )
            self.assertEqual(reservation.state.value, "released")

    def test_apply_and_release_do_not_double_count_rolling_rate_usage(self) -> None:
        now = datetime(2026, 8, 3, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            goals, governance, _ = self._services(tmp, now)
            goal = goals.create(
                self._goal_data("Account for one action exactly once"),
                self._goal_ctx("create-rate-accounting"),
            )
            request = GoalActionRequest(
                action_class="code.edit",
                repository="petersky/pa",
                estimate=GoalUsage(actions=1, tokens=10),
            )
            state, reserved = governance.authorize_action(
                goal.id, request, self._ctx(0, "rate-reserve-failed-attempt")
            )
            state, applied = governance.apply_action(
                goal.id,
                reserved.reservation_id or "",
                self._ctx(state.version, "rate-apply-failed-attempt"),
            )
            self.assertEqual(applied.disposition, GoalActionDisposition.AUTHORIZED)
            state = governance.release_action(
                goal.id,
                reserved.reservation_id or "",
                self._ctx(state.version, "rate-release-failed-attempt"),
                actual_usage=GoalUsage(),
                reason="prelaunch failure",
            )
            self.assertEqual(state.usage.actions, 0)

            state, retried = governance.authorize_action(
                goal.id, request, self._ctx(state.version, "rate-reserve-retry")
            )
            self.assertEqual(retried.disposition, GoalActionDisposition.AUTHORIZED)
            state, applied = governance.apply_action(
                goal.id,
                retried.reservation_id or "",
                self._ctx(state.version, "rate-apply-retry"),
            )
            self.assertEqual(applied.disposition, GoalActionDisposition.AUTHORIZED)
            state = governance.release_action(
                goal.id,
                retried.reservation_id or "",
                self._ctx(state.version, "rate-release-retry"),
                actual_usage=request.estimate,
                reason="action committed",
            )
            self.assertEqual(state.usage.actions, 1)
            state, limited = governance.authorize_action(
                goal.id, request, self._ctx(state.version, "rate-third-attempt")
            )
            self.assertEqual(limited.disposition, GoalActionDisposition.RATE_LIMITED)

    def test_apply_excludes_its_own_exclusive_resource_claim(self) -> None:
        now = datetime(2026, 8, 3, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            goals, governance, _ = self._services(tmp, now)
            goal = goals.create(
                self._goal_data("Apply one exclusive resource action"),
                self._goal_ctx("create-exclusive-apply"),
            )
            claim = GoalResourceClaim(
                key="repository:petersky/pa",
                access=ResourceAccess.EXCLUSIVE,
            )
            state, reserved = governance.authorize_action(
                goal.id,
                GoalActionRequest(
                    action_class="resource.reserve",
                    resource_claims=[claim],
                ),
                self._ctx(0, "reserve-exclusive-apply"),
            )
            self.assertEqual(reserved.disposition, GoalActionDisposition.AUTHORIZED)

            state, applied = governance.apply_action(
                goal.id,
                reserved.reservation_id or "",
                self._ctx(state.version, "apply-exclusive-resource"),
            )

            self.assertEqual(applied.disposition, GoalActionDisposition.AUTHORIZED)
            self.assertEqual(state.resource_reservations, [claim])

    def test_apply_excludes_only_its_shared_capacity_claim(self) -> None:
        now = datetime(2026, 8, 3, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            goals, governance, _ = self._services(tmp, now)
            goal_data = self._goal_data("Apply one capacity-limited shared action")
            goal_data.budget.max_concurrency = 2
            goal = goals.create(
                goal_data,
                self._goal_ctx("create-shared-capacity-apply"),
            )
            governance.put_policy(
                GoalGovernancePolicy(
                    version=1,
                    authored_by="user:operator",
                    resource_capacities=[
                        GoalResourceCapacity(key="build-pool", capacity=3)
                    ],
                ),
                self._ctx(
                    0,
                    "shared-capacity-policy",
                    actor="user:operator",
                    goal_version=None,
                ),
            )
            applying_claim = GoalResourceClaim(
                key="build-pool",
                access=ResourceAccess.SHARED,
                quantity=2,
            )
            sibling_claim = GoalResourceClaim(
                key="build-pool",
                access=ResourceAccess.SHARED,
                quantity=1,
            )
            state, applying = governance.authorize_action(
                goal.id,
                GoalActionRequest(
                    action_class="resource.reserve",
                    resource_claims=[applying_claim],
                ),
                self._ctx(0, "reserve-shared-applying"),
            )
            state, sibling = governance.authorize_action(
                goal.id,
                GoalActionRequest(
                    action_class="resource.reserve",
                    resource_claims=[sibling_claim],
                ),
                self._ctx(state.version, "reserve-shared-sibling"),
            )
            self.assertEqual(sibling.disposition, GoalActionDisposition.AUTHORIZED)

            state, applied = governance.apply_action(
                goal.id,
                applying.reservation_id or "",
                self._ctx(state.version, "apply-shared-capacity"),
            )

            self.assertEqual(applied.disposition, GoalActionDisposition.AUTHORIZED)
            self.assertEqual(
                sorted(claim.quantity for claim in state.resource_reservations),
                [1, 2],
            )

    def test_provider_launch_revalidates_mutations_after_assignment(self) -> None:
        now = datetime(2026, 8, 3, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            goals, governance, _ = self._services(tmp, now)
            goal = goals.create(
                self._goal_data("Revalidate before provider launch"),
                self._goal_ctx("create-launch-gate"),
            )
            goal = goals.acquire_lease(
                goal.id,
                GoalMutationContext(
                    actor_principal="agent:supervisor",
                    authority_instance_id="instance-a",
                    idempotency_key="launch-lease",
                    expected_version=goal.version,
                    policy_revision=goal.policy.revision,
                ),
                ttl_seconds=120,
            )
            state, run, assigned = governance.assign_provider(
                goal.id,
                ProviderGoalAssignment(provider_id="codex"),
                self._ctx(
                    0,
                    "assign-before-policy-change",
                    goal_version=goal.version,
                    fence=goal.lease.fencing_token,
                ),
            )
            self.assertEqual(assigned.disposition, GoalActionDisposition.AUTHORIZED)
            assert run is not None
            with self.assertRaisesRegex(
                GoalGovernanceConflict, "durably applied launch"
            ):
                governance.ingest_provider_progress(
                    goal.id,
                    ProviderGoalProgress(
                        run_id=run.id,
                        state=ProviderRunState.RUNNING,
                        summary="Execution must not start yet",
                    ),
                    self._ctx(
                        state.version,
                        "progress-before-launch",
                        actor=run.executor_principal,
                        goal_version=goal.version,
                        fence=run.fencing_token,
                    ),
                )

            changed_policy = goal.policy.model_copy(
                update={
                    "revision": 2,
                    "prohibited_actions": [
                        *goal.policy.prohibited_actions,
                        "provider.goal.assign",
                    ],
                }
            )
            goal = goals.revise(
                goal.id,
                GoalRevision(
                    policy=changed_policy,
                    reason="Provider execution is no longer authorized",
                ),
                GoalMutationContext(
                    actor_principal="agent:supervisor",
                    authority_instance_id="instance-a",
                    idempotency_key="revoke-provider-policy",
                    expected_version=goal.version,
                    policy_revision=1,
                    fencing_token=goal.lease.fencing_token,
                ),
            )
            state, gated_run, launch = governance.launch_provider(
                goal.id,
                run.id,
                self._ctx(
                    state.version,
                    "launch-after-policy-change",
                    policy_revision=2,
                    goal_version=goal.version,
                    fence=goal.lease.fencing_token,
                ),
            )
            self.assertEqual(launch.disposition, GoalActionDisposition.DENIED)
            self.assertIsNone(gated_run.launched_at)
            reservation = next(
                item
                for item in state.action_reservations
                if item.id == run.reservation_id
            )
            self.assertEqual(reservation.state.value, "released")
            self.assertTrue(
                any(
                    item["event_type"] == "goal_governance.action_applied"
                    and item["payload"]["disposition"] == "denied"
                    for item in governance.state_events(goal.id)
                )
            )

    def test_provider_retries_are_linear_role_stable_and_goal_budget_bounded(
        self,
    ) -> None:
        now = datetime(2026, 8, 3, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            goals, governance, _ = self._services(tmp, now)
            data = self._goal_data("Bound provider replacement lineage")
            data.budget.retry_limit = 1
            goal = goals.create(data, self._goal_ctx("retry-goal"))
            goal = goals.acquire_lease(
                goal.id,
                GoalMutationContext(
                    actor_principal="agent:supervisor",
                    authority_instance_id="instance-a",
                    idempotency_key="retry-lease",
                    expected_version=goal.version,
                    policy_revision=1,
                ),
                ttl_seconds=120,
            )

            def context(version: int, key: str, *, actor="agent:supervisor"):
                return self._ctx(
                    version,
                    key,
                    actor=actor,
                    goal_version=goal.version,
                    fence=goal.lease.fencing_token,
                )

            state, first, _ = governance.assign_provider(
                goal.id,
                ProviderGoalAssignment(provider_id="codex", max_attempts=20),
                context(0, "retry-first"),
            )
            assert first is not None
            state, first, _ = governance.launch_provider(
                goal.id, first.id, context(state.version, "retry-first-launch")
            )
            state = governance.ingest_provider_progress(
                goal.id,
                ProviderGoalProgress(
                    run_id=first.id,
                    state=ProviderRunState.FAILED,
                    summary="First attempt failed",
                ),
                context(
                    state.version,
                    "retry-first-terminal",
                    actor=first.executor_principal,
                ),
            )
            with self.assertRaisesRegex(
                GoalGovernanceConflict, "cannot change executor/verifier role"
            ):
                governance.assign_provider(
                    goal.id,
                    ProviderGoalAssignment(
                        provider_id="codex",
                        replaces_run_id=first.id,
                        role=GoalActorRole.VERIFIER,
                    ),
                    context(state.version, "retry-role-change"),
                )
            state, second, _ = governance.assign_provider(
                goal.id,
                ProviderGoalAssignment(
                    provider_id="codex",
                    replaces_run_id=first.id,
                    max_attempts=20,
                ),
                context(state.version, "retry-second"),
            )
            assert second is not None
            self.assertEqual(second.attempt, 2)
            self.assertEqual(second.max_attempts, 2)
            with self.assertRaisesRegex(
                GoalGovernanceConflict, "already has a durable replacement"
            ):
                governance.assign_provider(
                    goal.id,
                    ProviderGoalAssignment(
                        provider_id="codex", replaces_run_id=first.id
                    ),
                    context(state.version, "retry-fork"),
                )
            state, second, _ = governance.launch_provider(
                goal.id, second.id, context(state.version, "retry-second-launch")
            )
            state = governance.ingest_provider_progress(
                goal.id,
                ProviderGoalProgress(
                    run_id=second.id,
                    state=ProviderRunState.FAILED,
                    summary="Second attempt failed",
                ),
                context(
                    state.version,
                    "retry-second-terminal",
                    actor=second.executor_principal,
                ),
            )
            with self.assertRaisesRegex(
                GoalGovernanceConflict, "retry limit is exhausted"
            ):
                governance.assign_provider(
                    goal.id,
                    ProviderGoalAssignment(
                        provider_id="codex",
                        replaces_run_id=second.id,
                        max_attempts=20,
                    ),
                    context(state.version, "retry-third"),
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
                    actor="agent:critic",
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

    def test_three_store_takeover_fences_apply_and_recovers_old_reservation(
        self,
    ) -> None:
        clock = [datetime.now(UTC)]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            objects = ObjectStore(root / "objects")
            log = EventLog(objects, root, "instance-a")
            projections = {
                instance: CardProjection(root / f"{instance}.db", log)
                for instance in ("instance-a", "instance-b", "instance-c")
            }
            service_a = GoalService(
                projections["instance-a"], "instance-a", clock=lambda: clock[0]
            )
            governance_a = GoalGovernanceService(
                projections["instance-a"],
                "instance-a",
                service_a,
                clock=lambda: clock[0],
            )
            goal = service_a.create(
                self._goal_data("Fence takeover across three projections"),
                GoalMutationContext(
                    actor_principal="user:operator",
                    authority_instance_id="instance-a",
                    idempotency_key="three-store-create",
                    expected_version=0,
                    policy_revision=1,
                ),
            )
            goal = service_a.schedule_wakeup(
                goal.id,
                GoalWakeup(
                    wake_at=clock[0],
                    reason="eligible fleet takeover",
                    eligible_instance_ids=["instance-a", "instance-b"],
                ),
                GoalMutationContext(
                    actor_principal="user:operator",
                    authority_instance_id="instance-a",
                    idempotency_key="three-store-wakeup",
                    expected_version=goal.version,
                    policy_revision=1,
                ),
            )
            goal = service_a.acquire_lease(
                goal.id,
                GoalMutationContext(
                    actor_principal="service:goal-supervisor:instance-a",
                    authority_instance_id="instance-a",
                    idempotency_key="lease-instance-a",
                    expected_version=goal.version,
                    policy_revision=1,
                ),
                ttl_seconds=60,
            )
            state, decision = governance_a.authorize_action(
                goal.id,
                GoalActionRequest(action_class="code.test"),
                GovernanceMutationContext(
                    actor_principal="service:goal-supervisor:instance-a",
                    authority_instance_id="instance-a",
                    idempotency_key="reserve-before-takeover",
                    expected_version=0,
                    policy_revision=1,
                    goal_version=goal.version,
                    fencing_token=goal.lease.fencing_token,
                ),
            )
            reservation_id = decision.reservation_id or ""

            clock[0] += timedelta(seconds=61)
            projections["instance-b"].rebuild_from_log("default")
            service_b = GoalService(
                projections["instance-b"], "instance-b", clock=lambda: clock[0]
            )
            governance_b = GoalGovernanceService(
                projections["instance-b"],
                "instance-b",
                service_b,
                clock=lambda: clock[0],
            )
            goal_b = service_b.get(goal.id)
            assert goal_b is not None
            goal_b = service_b.acquire_lease(
                goal.id,
                GoalMutationContext(
                    actor_principal="service:goal-supervisor:instance-b",
                    authority_instance_id="instance-b",
                    idempotency_key="lease-instance-b",
                    expected_version=goal_b.version,
                    policy_revision=1,
                ),
                ttl_seconds=60,
            )
            takeover_context = GovernanceMutationContext(
                actor_principal="service:goal-supervisor:instance-b",
                authority_instance_id="instance-b",
                idempotency_key="takeover-apply-must-fail",
                expected_version=state.version,
                policy_revision=1,
                goal_version=goal_b.version,
                fencing_token=goal_b.lease.fencing_token,
            )
            with self.assertRaisesRegex(
                GoalGovernanceConflict, "another authority instance"
            ):
                governance_b.apply_action(goal.id, reservation_id, takeover_context)
            recovered = governance_b.release_action(
                goal.id,
                reservation_id,
                takeover_context.model_copy(
                    update={"idempotency_key": "takeover-release"}
                ),
                actual_usage=GoalUsage(),
                reason="released by the new fenced controller",
            )
            self.assertEqual(recovered.action_reservations[0].state.value, "released")

            projections["instance-a"].rebuild_from_log("default")
            stale_a = GoalService(
                projections["instance-a"], "instance-a", clock=lambda: clock[0]
            )
            current_a = stale_a.get(goal.id)
            assert current_a is not None
            with self.assertRaisesRegex(GoalGovernanceConflict, "fencing token"):
                # Governance writes from the old authority are fenced even though
                # that authority created the original reservation.
                GoalGovernanceService(
                    projections["instance-a"],
                    "instance-a",
                    stale_a,
                    clock=lambda: clock[0],
                ).set_priority(
                    goal.id,
                    99,
                    "stale authority",
                    GovernanceMutationContext(
                        actor_principal="service:goal-supervisor:instance-a",
                        authority_instance_id="instance-a",
                        idempotency_key="stale-after-takeover",
                        expected_version=recovered.version,
                        policy_revision=1,
                        goal_version=current_a.version,
                        fencing_token=1,
                    ),
                )

            clock[0] += timedelta(seconds=61)
            projections["instance-c"].rebuild_from_log("default")
            service_c = GoalService(
                projections["instance-c"], "instance-c", clock=lambda: clock[0]
            )
            goal_c = service_c.get(goal.id)
            assert goal_c is not None
            with self.assertRaisesRegex(GoalConflict, "not eligible"):
                service_c.acquire_lease(
                    goal.id,
                    GoalMutationContext(
                        actor_principal="service:goal-supervisor:instance-c",
                        authority_instance_id="instance-c",
                        idempotency_key="ineligible-instance-c",
                        expected_version=goal_c.version,
                        policy_revision=1,
                    ),
                )

    def test_equal_version_conflicts_choose_the_same_payload_in_any_replay_order(
        self,
    ) -> None:
        now = datetime(2026, 8, 3, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            objects = ObjectStore(root / "objects")
            log = EventLog(objects, root, "instance-a")
            first_projection = CardProjection(root / "first.db", log)
            second_projection = CardProjection(root / "second.db", log)
            goal = Goal(**self._goal_data("Canonical payload A").model_dump())
            competing = goal.model_copy(
                deep=True, update={"objective": "Canonical payload B"}
            )

            def event(event_id: str, value: Goal, offset: int) -> CardEvent:
                return CardEvent(
                    id=event_id,
                    type=EventType.GOAL_UPSERTED,
                    realm_id=value.realm_id,
                    author_principal="agent:replay",
                    author_instance="instance-a",
                    timestamp=now + timedelta(seconds=offset),
                    payload={
                        "goal": value.model_dump(mode="json"),
                        "goal_event": {
                            "goal_id": value.id,
                            "event_type": "goal.replayed",
                            "actor_principal": "agent:replay",
                            "authority_instance_id": "instance-a",
                            "policy_revision": value.policy.revision,
                            "idempotency_key": event_id,
                            "version": value.version,
                        },
                    },
                )

            event_a = event("equal-version-event-a", goal, 1)
            event_b = event("equal-version-event-b", competing, 2)
            first_projection.apply_event(event_a)
            first_projection.apply_event(event_b)
            second_projection.apply_event(event_b)
            second_projection.apply_event(event_a)

            first = GoalService(first_projection, "instance-a")
            second = GoalService(second_projection, "instance-b")
            self.assertEqual(
                first.get(goal.id).model_dump(mode="json"),
                second.get(goal.id).model_dump(mode="json"),
            )
            self.assertEqual(first.conflicts(goal.id), second.conflicts(goal.id))
            self.assertEqual(len(first.conflicts(goal.id)), 1)

    def test_legacy_unfenced_provider_run_migrates_to_cancelled_recoverable_state(
        self,
    ) -> None:
        now = datetime(2026, 8, 3, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            goals, governance, replica = self._services(tmp, now)
            goal = goals.create(
                self._goal_data("Migrate a legacy provider run"),
                self._goal_ctx("legacy-provider-goal"),
            )
            run_id = "legacy-run"
            legacy = {
                "goal_id": goal.id,
                "realm_id": goal.realm_id,
                "version": 1,
                "usage": {"actions": 1, "tokens": 100},
                "provider_runs": [
                    {
                        "id": run_id,
                        "goal_id": goal.id,
                        "provider_id": "codex",
                        "invocation": {
                            "provider_id": "codex",
                            "mode": "recoverable_turn",
                            "prompt": "Legacy unfenced invocation",
                            "canonical_goal_id": goal.id,
                            "policy_revision": 1,
                        },
                        "state": "running",
                        "summary": "Legacy execution",
                        "reserved_usage": {"actions": 1, "tokens": 100},
                        "created_at": now.isoformat(),
                        "updated_at": now.isoformat(),
                    }
                ],
                "updated_at": now.isoformat(),
            }
            goals.store.commit_event(
                CardEvent(
                    id="legacy-provider-event",
                    type=EventType.GOAL_GOVERNANCE_UPSERTED,
                    realm_id=goal.realm_id,
                    author_principal="agent:legacy",
                    author_instance="instance-a",
                    timestamp=now,
                    payload={
                        "entity_type": "goal_autonomy",
                        "entity_id": goal.id,
                        "entity": legacy,
                        "governance_event": {
                            "event_type": "goal_governance.legacy_provider",
                            "actor_principal": "agent:legacy",
                            "authority_instance_id": "instance-a",
                            "policy_revision": 1,
                            "idempotency_key": "legacy-provider-event",
                            "version": 1,
                        },
                    },
                )
            )
            migrated = governance.get_state(goal.id)
            self.assertEqual(migrated.provider_runs[0].state.value, "cancelled")
            self.assertEqual(migrated.provider_runs[0].reserved_usage.actions, 0)
            self.assertEqual(migrated.usage.actions, 0)
            self.assertEqual(
                migrated.action_reservations[0].id,
                f"legacy-provider-run:{run_id}",
            )
            self.assertEqual(migrated.action_reservations[0].state.value, "released")

            replica.rebuild_from_log("default")
            replacement = GoalGovernanceService(
                replica,
                "instance-b",
                GoalService(replica, "instance-b"),
                clock=lambda: now,
            )
            self.assertEqual(
                migrated.model_dump(mode="json"),
                replacement.get_state(goal.id).model_dump(mode="json"),
            )

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
