"""goals tools: authenticated owner API proxies."""

from __future__ import annotations

from pa.core.context import AppContext
from pa.goals.advanced_models import (
    GoalActionApply,
    GoalActionRelease,
    GoalActionRequest,
    GoalGovernancePolicy,
    GoalPortfolioReviewRequest,
    GoalProposalRequest,
    GoalProposalReview,
    GoalStrategyPortfolioUpdate,
    ProviderGoalAssignment,
    ProviderGoalProgress,
)
from pa.goals.models import (
    AssignedServiceGoalAuditCreate,
    AssignedServiceGoalEvidenceCreate,
    AssignedServiceGoalProposalCreate,
    GoalCreate,
    GoalProposalCreate,
    GoalState,
    GoalTransition,
)


def register_mcp(mcp, ctx: AppContext) -> None:
    from pa.mcp.local_api import request_local_pa

    @mcp.tool()
    def list_goals(
        realm: str | None = None, state: GoalState | None = None
    ) -> list[dict]:
        """List durable goals and their criterion coverage."""
        return request_local_pa(
            ctx.settings,
            "GET",
            "/api/goals",
            params={"realm": realm, "state": state},
        )

    @mcp.tool()
    def get_goal(goal_id: str, realm: str | None = None) -> dict:
        """Get a durable goal with its attributable event ledger."""
        return request_local_pa(
            ctx.settings, "GET", f"/api/goals/{goal_id}", params={"realm": realm}
        )

    @mcp.tool()
    def create_goal(
        goal: GoalCreate,
        idempotency_key: str,
        authority_instance_id: str,
        actor_principal: str = "user:local",
    ) -> dict:
        """Create a durable goal under policy revision 1."""
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/goals",
            params={"expected_version": 0, "policy_revision": goal.policy.revision},
            headers={
                "Idempotency-Key": idempotency_key,
                "X-PA-Actor": actor_principal,
                "X-PA-Authority-Instance": authority_instance_id,
            },
            json=goal.model_dump(mode="json"),
        )

    @mcp.tool()
    def transition_goal(
        goal_id: str,
        transition: GoalTransition,
        expected_version: int,
        policy_revision: int,
        idempotency_key: str,
        authority_instance_id: str,
        fencing_token: int | None = None,
        actor_principal: str = "user:local",
    ) -> dict:
        """Apply a lifecycle transition authorized by the active policy revision."""
        headers = {
            "Idempotency-Key": idempotency_key,
            "X-PA-Actor": actor_principal,
            "X-PA-Authority-Instance": authority_instance_id,
        }
        if fencing_token is not None:
            headers["X-PA-Goal-Fencing-Token"] = str(fencing_token)
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/goals/{goal_id}/transition",
            params={
                "expected_version": expected_version,
                "policy_revision": policy_revision,
            },
            headers=headers,
            json=transition.model_dump(mode="json"),
        )

    @mcp.tool()
    def get_goal_portfolio(realm: str = "default") -> dict:
        """Read organization policy, autonomy state, proposals, and review."""
        return request_local_pa(
            ctx.settings,
            "GET",
            "/api/goal-governance/portfolio",
            params={"realm": realm},
        )

    @mcp.tool()
    def get_goal_autonomy(goal_id: str) -> dict:
        """Read priority, strategies, usage, decisions, resources, and runs."""
        return request_local_pa(
            ctx.settings, "GET", f"/api/goals/{goal_id}/autonomy"
        )

    @mcp.tool()
    def authorize_goal_action(
        goal_id: str,
        action: GoalActionRequest,
        expected_autonomy_version: int,
        goal_version: int,
        policy_revision: int,
        idempotency_key: str,
        authority_instance_id: str,
        fencing_token: int | None = None,
        actor_principal: str = "agent:supervisor",
    ) -> dict:
        """Reserve one action only when policy, budgets, rates, and resources allow."""
        headers = {
            "Idempotency-Key": idempotency_key,
            "X-PA-Actor": actor_principal,
            "X-PA-Authority-Instance": authority_instance_id,
        }
        if fencing_token is not None:
            headers["X-PA-Goal-Fencing-Token"] = str(fencing_token)
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/goals/{goal_id}/actions/authorize",
            params={
                "expected_version": expected_autonomy_version,
                "goal_version": goal_version,
                "policy_revision": policy_revision,
            },
            headers=headers,
            json=action.model_dump(mode="json"),
        )

    @mcp.tool()
    def apply_goal_action_reservation(
        goal_id: str,
        apply: GoalActionApply,
        expected_autonomy_version: int,
        goal_version: int,
        policy_revision: int,
        idempotency_key: str,
        authority_instance_id: str,
        fencing_token: int | None = None,
    ) -> dict:
        """Revalidate a durable reservation immediately before its side effect."""
        headers = {
            "Idempotency-Key": idempotency_key,
            "X-PA-Authority-Instance": authority_instance_id,
        }
        if fencing_token is not None:
            headers["X-PA-Goal-Fencing-Token"] = str(fencing_token)
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/goals/{goal_id}/actions/apply",
            params={
                "expected_version": expected_autonomy_version,
                "goal_version": goal_version,
                "policy_revision": policy_revision,
            },
            headers=headers,
            json=apply.model_dump(mode="json"),
        )

    @mcp.tool()
    def release_goal_action_reservation(
        goal_id: str,
        release: GoalActionRelease,
        expected_autonomy_version: int,
        goal_version: int,
        policy_revision: int,
        idempotency_key: str,
        authority_instance_id: str,
        fencing_token: int | None = None,
    ) -> dict:
        """Release a reservation after success, failure, cancellation, or abort."""
        headers = {
            "Idempotency-Key": idempotency_key,
            "X-PA-Authority-Instance": authority_instance_id,
        }
        if fencing_token is not None:
            headers["X-PA-Goal-Fencing-Token"] = str(fencing_token)
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/goals/{goal_id}/actions/release",
            params={
                "expected_version": expected_autonomy_version,
                "goal_version": goal_version,
                "policy_revision": policy_revision,
            },
            headers=headers,
            json=release.model_dump(mode="json"),
        )

    @mcp.tool()
    def assign_provider_goal(
        goal_id: str,
        assignment: ProviderGoalAssignment,
        expected_autonomy_version: int,
        goal_version: int,
        policy_revision: int,
        idempotency_key: str,
        authority_instance_id: str,
        fencing_token: int | None = None,
        actor_principal: str = "agent:supervisor",
    ) -> dict:
        """Translate a bounded PA goal into a provider-native or recoverable run."""
        headers = {
            "Idempotency-Key": idempotency_key,
            "X-PA-Actor": actor_principal,
            "X-PA-Authority-Instance": authority_instance_id,
        }
        if fencing_token is not None:
            headers["X-PA-Goal-Fencing-Token"] = str(fencing_token)
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/goals/{goal_id}/providers/assign",
            params={
                "expected_version": expected_autonomy_version,
                "goal_version": goal_version,
                "policy_revision": policy_revision,
            },
            headers=headers,
            json=assignment.model_dump(mode="json"),
        )

    @mcp.tool()
    def ingest_provider_goal_progress(
        goal_id: str,
        progress: ProviderGoalProgress,
        expected_autonomy_version: int,
        goal_version: int,
        policy_revision: int,
        idempotency_key: str,
        authority_instance_id: str,
        progress_credential: str,
        fencing_token: int | None = None,
    ) -> dict:
        """Ingest provider progress without treating provider claims as proof."""
        headers = {
            "Idempotency-Key": idempotency_key,
            "X-PA-Authority-Instance": authority_instance_id,
            "Authorization": f"GoalRun {progress_credential}",
        }
        if fencing_token is not None:
            headers["X-PA-Goal-Fencing-Token"] = str(fencing_token)
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/goals/{goal_id}/providers/progress",
            params={
                "expected_version": expected_autonomy_version,
                "goal_version": goal_version,
                "policy_revision": policy_revision,
            },
            headers=headers,
            json=progress.model_dump(mode="json"),
        )

    @mcp.tool()
    def launch_provider_goal(
        goal_id: str,
        run_id: str,
        expected_autonomy_version: int,
        goal_version: int,
        policy_revision: int,
        idempotency_key: str,
        authority_instance_id: str,
        fencing_token: int | None = None,
    ) -> dict:
        """Apply the final governance gate and return a runnable invocation."""
        headers = {
            "Idempotency-Key": idempotency_key,
            "X-PA-Authority-Instance": authority_instance_id,
        }
        if fencing_token is not None:
            headers["X-PA-Goal-Fencing-Token"] = str(fencing_token)
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/goals/{goal_id}/providers/{run_id}/launch",
            params={
                "expected_version": expected_autonomy_version,
                "goal_version": goal_version,
                "policy_revision": policy_revision,
            },
            headers=headers,
        )

    @mcp.tool()
    def update_goal_strategies(
        goal_id: str,
        portfolio: GoalStrategyPortfolioUpdate,
        expected_autonomy_version: int,
        goal_version: int,
        policy_revision: int,
        idempotency_key: str,
        authority_instance_id: str,
        fencing_token: int | None = None,
        actor_principal: str = "agent:supervisor",
    ) -> dict:
        """Replace the bounded strategy portfolio under optimistic fencing."""
        headers = {
            "Idempotency-Key": idempotency_key,
            "X-PA-Actor": actor_principal,
            "X-PA-Authority-Instance": authority_instance_id,
        }
        if fencing_token is not None:
            headers["X-PA-Goal-Fencing-Token"] = str(fencing_token)
        return request_local_pa(
            ctx.settings,
            "PUT",
            f"/api/goals/{goal_id}/strategies",
            params={
                "expected_version": expected_autonomy_version,
                "goal_version": goal_version,
                "policy_revision": policy_revision,
            },
            headers=headers,
            json=portfolio.model_dump(mode="json"),
        )

    @mcp.tool()
    def propose_goal(
        proposal: GoalProposalRequest,
        idempotency_key: str,
        authority_instance_id: str,
        policy_revision: int,
        actor_principal: str = "agent:supervisor",
    ) -> dict:
        """Propose a traceable derived or top-level goal under standing policy."""
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/goal-governance/proposals",
            params={"expected_version": 0, "policy_revision": policy_revision},
            headers={
                "Idempotency-Key": idempotency_key,
                "X-PA-Actor": actor_principal,
                "X-PA-Authority-Instance": authority_instance_id,
            },
            json=proposal.model_dump(mode="json"),
        )

    @mcp.tool()
    def review_goal_proposal(
        proposal_id: str,
        review: GoalProposalReview,
        expected_version: int,
        policy_revision: int,
        idempotency_key: str,
        authority_instance_id: str,
        realm: str = "default",
        actor_principal: str = "user:local",
    ) -> dict:
        """Approve or reject one pending proposal with operator attribution."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/goal-governance/proposals/{proposal_id}/review",
            params={
                "realm": realm,
                "expected_version": expected_version,
                "policy_revision": policy_revision,
            },
            headers={
                "Idempotency-Key": idempotency_key,
                "X-PA-Actor": actor_principal,
                "X-PA-Authority-Instance": authority_instance_id,
            },
            json=review.model_dump(mode="json"),
        )

    @mcp.tool()
    def review_goal_portfolio(
        review: GoalPortfolioReviewRequest,
        expected_version: int,
        policy_revision: int,
        idempotency_key: str,
        authority_instance_id: str,
        realm: str = "default",
        actor_principal: str = "agent:supervisor",
    ) -> dict:
        """Record an independent organization-level allocation review."""
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/goal-governance/portfolio/reviews",
            params={
                "realm": realm,
                "expected_version": expected_version,
                "policy_revision": policy_revision,
            },
            headers={
                "Idempotency-Key": idempotency_key,
                "X-PA-Actor": actor_principal,
                "X-PA-Authority-Instance": authority_instance_id,
            },
            json=review.model_dump(mode="json"),
        )

    @mcp.tool()
    def set_goal_governance_policy(
        policy: GoalGovernancePolicy,
        expected_version: int,
        idempotency_key: str,
        authority_instance_id: str,
        actor_principal: str = "user:local",
    ) -> dict:
        """Set the next operator-authored organization governance revision."""
        return request_local_pa(
            ctx.settings,
            "PUT",
            "/api/goal-governance/policy",
            params={
                "expected_version": expected_version,
                "policy_revision": policy.version,
            },
            headers={
                "Idempotency-Key": idempotency_key,
                "X-PA-Actor": actor_principal,
                "X-PA-Authority-Instance": authority_instance_id,
            },
            json=policy.model_dump(mode="json"),
        )

    @mcp.tool()
    def propose_goal_action(
        goal_id: str,
        proposal: GoalProposalCreate,
        expected_version: int,
        policy_revision: int,
        idempotency_key: str,
        authority_instance_id: str,
        fencing_token: int | None = None,
        actor_principal: str = "user:local",
    ) -> dict:
        """Submit a typed proposal for deterministic goal authorization."""
        headers = {
            "Idempotency-Key": idempotency_key,
            "X-PA-Actor": actor_principal,
            "X-PA-Authority-Instance": authority_instance_id,
        }
        if fencing_token is not None:
            headers["X-PA-Goal-Fencing-Token"] = str(fencing_token)
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/goals/{goal_id}/proposals",
            params={
                "expected_version": expected_version,
                "policy_revision": policy_revision,
            },
            headers=headers,
            json=proposal.model_dump(mode="json"),
        )

    @mcp.tool()
    def get_assigned_goal(offset: int = 0, limit: int = 50) -> dict:
        """Read the Goal bound to this assigned PA session."""
        return request_local_pa(
            ctx.settings,
            "GET",
            "/api/goal-assigned-session/goal",
            params={"offset": offset, "limit": limit},
        )

    @mcp.tool()
    def propose_assigned_goal_action(
        proposal: AssignedServiceGoalProposalCreate,
        expected_version: int,
        policy_revision: int,
        idempotency_key: str,
    ) -> dict:
        """Submit a proposal as this bridge's exact assigned Goal service."""
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/goal-assigned-session/proposals",
            params={
                "expected_version": expected_version,
                "policy_revision": policy_revision,
            },
            headers={"Idempotency-Key": idempotency_key},
            json=proposal.model_dump(mode="json"),
        )

    @mcp.tool()
    def record_assigned_goal_evidence(
        change: AssignedServiceGoalEvidenceCreate,
        expected_version: int,
        policy_revision: int,
        idempotency_key: str,
    ) -> dict:
        """Record evidence as this bridge's exact assigned Goal service."""
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/goal-assigned-session/evidence",
            params={
                "expected_version": expected_version,
                "policy_revision": policy_revision,
            },
            headers={"Idempotency-Key": idempotency_key},
            json=change.model_dump(mode="json"),
        )

    @mcp.tool()
    def audit_assigned_goal(
        audit: AssignedServiceGoalAuditCreate,
        expected_version: int,
        policy_revision: int,
        idempotency_key: str,
    ) -> dict:
        """Audit a Goal as this bridge's independently assigned verifier."""
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/goal-assigned-session/audit",
            params={
                "expected_version": expected_version,
                "policy_revision": policy_revision,
            },
            headers={"Idempotency-Key": idempotency_key},
            json=audit.model_dump(mode="json"),
        )

    @mcp.tool()
    def supervise_goal(goal_id: str) -> dict:
        """Run one fenced event-driven supervision cycle for a goal."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/goals/{goal_id}/supervise",
            timeout_seconds=60.0,
        )
