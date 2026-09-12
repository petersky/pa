"""fleet tools: authenticated owner API proxies."""

from __future__ import annotations

from typing import Any, Literal
from pa.collaboration.models import CollaborationMode
from pa.core.context import AppContext
from pa.execution.profiles import ExecutionContract
from pa.execution.progress import PROGRESS_SCHEMA_VERSION, OperatorInputRequestV1
from pa.fleet.placement import PlacementPolicy
from pa.workloads import WorkloadProfileInput


def register_mcp(mcp, ctx: AppContext) -> None:
    from pa.mcp.local_api import request_local_pa

    @mcp.tool()
    def discover_fleet_bootstrap_target(target: str) -> dict[str, Any]:
        """Resolve an SSH target and fingerprint without mutating the host."""
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/fleet/bootstrap/discover",
            json={"target": target},
        )

    @mcp.tool()
    def create_fleet_bootstrap_job(
        target: str,
        instance_name: str,
        instance_url: str,
        idempotency_key: str,
        realm: str = "default",
        worker_profile: str = "manual",
        providers: list[str] | None = None,
        repositories: list[str] | None = None,
        github_transport: str = "none",
        automatic_placement: bool = False,
        dispatch_capacity: int = 1,
        channel: str = "release",
        release_ref: str = "",
        existing_install_action: str = "install",
        smoke_dispatch: bool = False,
        smoke_card_id: str = "",
        start: bool = False,
    ) -> dict[str, Any]:
        """Create a durable, observable machine-onboarding plan."""
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/fleet/bootstrap-jobs",
            json={
                "idempotency_key": idempotency_key,
                "start": start,
                "request": {
                    "target": target,
                    "instance_name": instance_name,
                    "instance_url": instance_url,
                    "realm": realm,
                    "worker_profile": worker_profile,
                    "providers": providers or [],
                    "repositories": repositories or [],
                    "github_transport": github_transport,
                    "automatic_placement": automatic_placement,
                    "dispatch_capacity": dispatch_capacity,
                    "channel": channel,
                    "release_ref": release_ref,
                    "existing_install_action": existing_install_action,
                    "smoke_dispatch": smoke_dispatch,
                    "smoke_card_id": smoke_card_id,
                },
            },
        )

    @mcp.tool()
    def list_fleet_bootstrap_jobs(
        include_terminal: bool = True,
    ) -> list[dict[str, Any]]:
        """List durable onboarding jobs and incomplete machines."""
        return request_local_pa(
            ctx.settings,
            "GET",
            "/api/fleet/bootstrap-jobs",
            params={"include_terminal": include_terminal},
        )

    @mcp.tool()
    def get_fleet_bootstrap_job(job_id: str) -> dict[str, Any] | None:
        """Read phase, logs, required input, evidence, and readiness."""
        return request_local_pa(
            ctx.settings,
            "GET",
            f"/api/fleet/bootstrap-jobs/{job_id}",
            allow_not_found=True,
        )

    @mcp.tool()
    def control_fleet_bootstrap_job(
        job_id: str,
        action: Literal["start", "resume", "retry", "cancel"],
    ) -> dict[str, Any]:
        """Start, safely cancel, resume, or retry a durable onboarding job."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/fleet/bootstrap-jobs/{job_id}/{action}",
        )

    @mcp.tool()
    def submit_fleet_bootstrap_input(
        job_id: str,
        kind: Literal[
            "host_key",
            "ssh_password",
            "key_passphrase",
            "sudo_password",
            "provider_login",
            "github_login",
            "operator_confirmation",
        ],
        value: str = "",
        confirmed: bool = False,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Submit short-lived protected input or explicit phase evidence."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/fleet/bootstrap-jobs/{job_id}/input",
            json={
                "kind": kind,
                "value": value,
                "confirmed": confirmed,
                "details": details or {},
            },
        )

    @mcp.tool()
    def list_instance_groups(
        realm_id: str | None = None, include_archived: bool = False
    ) -> list[dict]:
        """List immutable built-in and operator-defined worker groups."""
        return request_local_pa(
            ctx.settings,
            "GET",
            "/api/fleet/instance-groups",
            params={
                "realm": realm_id,
                "include_archived": include_archived,
            },
        )

    @mcp.tool()
    def get_instance_group(
        group_id: str, realm_id: str | None = None
    ) -> dict | None:
        """Get one worker group with stable-ID membership and exclusions."""
        return request_local_pa(
            ctx.settings,
            "GET",
            f"/api/fleet/instance-groups/{group_id}",
            params={"realm": realm_id},
            allow_not_found=True,
        )

    @mcp.tool()
    def create_instance_group(
        name: str,
        description: str = "",
        realm_id: str = "default",
        included_instance_ids: list[str] | None = None,
        excluded_instance_ids: list[str] | None = None,
        selector: dict[str, Any] | None = None,
        permitted_placement_policies: list[str] | None = None,
        visible_project_ids: list[str] | None = None,
    ) -> dict:
        """Create a synchronized reusable fleet selection scope."""
        payload = {
            "realm_id": realm_id,
            "name": name,
            "description": description,
            "included_instance_ids": included_instance_ids or [],
            "excluded_instance_ids": excluded_instance_ids or [],
            "selector": selector or {},
            "visible_project_ids": visible_project_ids or [],
        }
        if permitted_placement_policies is not None:
            payload["permitted_placement_policies"] = permitted_placement_policies
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/fleet/instance-groups",
            json=payload,
        )

    @mcp.tool()
    def update_instance_group(
        group_id: str,
        changes: dict[str, Any],
        realm_id: str | None = None,
    ) -> dict:
        """Update group rules using the expected_version in changes when supplied."""
        return request_local_pa(
            ctx.settings,
            "PATCH",
            f"/api/fleet/instance-groups/{group_id}",
            params={"realm": realm_id},
            json=changes,
        )

    @mcp.tool()
    def archive_instance_group(group_id: str, realm_id: str | None = None) -> dict:
        """Archive a custom group without allowing defaults to fall back."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/fleet/instance-groups/{group_id}/archive",
            params={"realm": realm_id},
        )

    @mcp.tool()
    def delete_instance_group(
        group_id: str, realm_id: str | None = None
    ) -> dict | None:
        """Delete a custom group; references remain visibly unavailable."""
        return request_local_pa(
            ctx.settings,
            "DELETE",
            f"/api/fleet/instance-groups/{group_id}",
            params={"realm": realm_id},
        )

    @mcp.tool()
    def set_instance_group_member(
        group_id: str,
        instance_id: str,
        *,
        included: bool = True,
        excluded: bool = False,
        realm_id: str | None = None,
    ) -> dict:
        """Add/remove a stable instance ID from explicit membership or exclusions."""
        collection = "exclusions" if excluded else "members"
        return request_local_pa(
            ctx.settings,
            "PUT" if included else "DELETE",
            f"/api/fleet/instance-groups/{group_id}/{collection}/{instance_id}",
            params={"realm": realm_id},
        )

    @mcp.tool()
    def preview_instance_group(
        group_id: str,
        workload_profile: WorkloadProfileInput = "research",
        project_id: str | None = None,
        policy: PlacementPolicy = PlacementPolicy.BEST_MATCH,
    ) -> dict:
        """Preview expanded membership plus policy/readiness rejection reasons."""
        return request_local_pa(
            ctx.settings,
            "GET",
            f"/api/fleet/instance-groups/{group_id}/preview",
            params={
                "workload_profile": workload_profile,
                "project_id": project_id,
                "policy": policy.value,
            },
        )

    @mcp.tool()
    def get_instance_participation_policy(
        instance_id: str, realm_id: str | None = None
    ) -> dict:
        """Get an instance's effective participation policy and summary."""
        return request_local_pa(
            ctx.settings,
            "GET",
            f"/api/fleet/instances/{instance_id}/participation-policy",
            params={"realm": realm_id},
        )

    @mcp.tool()
    def update_instance_participation_policy(
        instance_id: str,
        changes: dict[str, Any],
        realm_id: str | None = None,
    ) -> dict:
        """Update a policy; enabling work requires confirmation fields."""
        return request_local_pa(
            ctx.settings,
            "PUT",
            f"/api/fleet/instances/{instance_id}/participation-policy",
            params={"realm": realm_id},
            json=changes,
        )

    @mcp.tool()
    def set_placement_default_group(
        group_id: str,
        realm_id: str | None = None,
        project_id: str | None = None,
        workload_profile: WorkloadProfileInput | None = None,
    ) -> dict:
        """Set a realm/project/profile default without all-instance fallback."""
        return request_local_pa(
            ctx.settings,
            "PUT",
            "/api/fleet/placement-defaults",
            json={
                "group_id": group_id,
                "realm_id": realm_id,
                "project_id": project_id,
                "workload_profile": workload_profile,
            },
        )

    @mcp.tool()
    def list_placement_default_groups(
        realm_id: str | None = None,
    ) -> list[dict]:
        """List synchronized realm/project/profile group defaults."""
        return request_local_pa(
            ctx.settings,
            "GET",
            "/api/fleet/placement-defaults",
            params={"realm": realm_id},
        )

    @mcp.tool()
    def delete_placement_default_group(
        realm_id: str | None = None,
        project_id: str | None = None,
        workload_profile: WorkloadProfileInput | None = None,
    ) -> None:
        """Delete one exact default scope without silently selecting all peers."""
        request_local_pa(
            ctx.settings,
            "DELETE",
            "/api/fleet/placement-defaults",
            params={
                "realm": realm_id,
                "project_id": project_id,
                "workload_profile": workload_profile,
            },
        )

    @mcp.tool()
    def migrate_instance_participation_policies(
        realm_id: str | None = None, apply: bool = False
    ) -> dict:
        """Preview or deliberately apply the compatibility policy migration."""
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/fleet/participation-migration",
            json={"realm_id": realm_id, "apply": apply},
        )

    @mcp.tool()
    def preview_fleet_placement(
        policy: PlacementPolicy,
        card_id: str | None = None,
        group_id: str | None = None,
        instance_id: str | None = None,
        project_id: str | None = None,
        workload_profile: WorkloadProfileInput = "research",
        provider: str | None = None,
        model_id: str | None = None,
        required_capabilities: list[str] | None = None,
        execution_preferences: dict | None = None,
        task_assessment: dict | None = None,
        expected_card_version: str | None = None,
        context_source_session_id: str | None = None,
    ) -> dict:
        """Resolve and explain candidates without admitting a dispatch."""
        if instance_id and group_id:
            raise ValueError("named preview cannot also specify group_id")
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/fleet/placement/preview",
            json={
                "card_id": card_id,
                "project_id": project_id,
                "target_instance_id": instance_id,
                "placement_policy": None if instance_id else policy.value,
                "group_id": group_id,
                "provider": provider,
                "model_id": model_id,
                "required_capabilities": required_capabilities or [],
                **{
                    key: value
                    for key, value in {
                        "execution_preferences": execution_preferences,
                        "task_assessment": task_assessment,
                        "expected_card_version": expected_card_version,
                        "context_source_session_id": context_source_session_id,
                    }.items()
                    if value is not None
                },
                "execution_contract": {
                    "version": 1,
                    "profile": workload_profile,
                    "confirmed": True,
                    "requirements": {},
                },
            },
        )

    @mcp.tool()
    def list_fleet_policy_audit(
        realm_id: str | None = None,
        entity_type: str | None = None,
        entity_id: str | None = None,
        limit: int = 200,
    ) -> list[dict]:
        """List policy/group/default mutations and resolved placement decisions."""
        return request_local_pa(
            ctx.settings,
            "GET",
            "/api/fleet/policy-audit",
            params={
                "realm": realm_id,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "limit": limit,
            },
        )

    @mcp.tool()
    def dispatch_card(
        card_id: str,
        idempotency_key: str,
        instance_id: str | None = None,
        policy: PlacementPolicy | None = None,
        group_id: str | None = None,
        message: str = "",
        authority_instance_id: str | None = None,
        provider: str | None = None,
        model_id: str | None = None,
        model_provider: str | None = None,
        execution_preferences: dict | None = None,
        task_assessment: dict | None = None,
        expected_card_version: str | None = None,
        context_source_session_id: str | None = None,
        mode_id: str | None = None,
        collaboration_mode: CollaborationMode | None = None,
        collaboration_risk: str = "low",
        collaboration_ambiguous: bool = False,
        collaboration_unattended: bool = False,
        effort: str | None = None,
        allow_concurrent: bool = False,
        concurrent_reason: str | None = None,
        capacity_override: bool = False,
        capacity_override_reason: str | None = None,
        participation_override: bool = False,
        participation_override_reason: str | None = None,
        execution_contract: ExecutionContract | None = None,
        priority: int = 0,
        resume_session_id: str | None = None,
    ) -> dict:
        """Resolve a concrete target or policy and durably dispatch a card."""
        key = idempotency_key.strip()
        if not key:
            raise ValueError("idempotency_key cannot be empty")
        if bool(instance_id) == bool(policy):
            raise ValueError("specify exactly one instance_id or policy")
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/fleet/dispatch",
            json={
                "authority_instance_id": authority_instance_id,
                "card_id": card_id,
                "target_instance_id": instance_id,
                "placement_policy": (
                    policy.value if isinstance(policy, PlacementPolicy) else policy
                ),
                "group_id": group_id,
                "message": message,
                "provider": provider,
                "model_id": model_id,
                **{
                    k: v
                    for k, v in {
                        "model_provider": model_provider,
                        "execution_preferences": execution_preferences,
                        "task_assessment": task_assessment,
                        "expected_card_version": expected_card_version,
                        "context_source_session_id": context_source_session_id,
                    }.items()
                    if v is not None
                },
                "mode_id": mode_id,
                "collaboration_mode": (
                    collaboration_mode.value
                    if isinstance(collaboration_mode, CollaborationMode)
                    else collaboration_mode
                ),
                "collaboration_risk": collaboration_risk,
                "collaboration_ambiguous": collaboration_ambiguous,
                "collaboration_unattended": collaboration_unattended,
                "effort": effort,
                "allow_concurrent": allow_concurrent,
                "concurrent_reason": concurrent_reason,
                "capacity_override": capacity_override,
                "capacity_override_reason": capacity_override_reason,
                "participation_override": participation_override,
                "participation_override_reason": (participation_override_reason),
                "execution_contract": (
                    execution_contract.model_dump(mode="json")
                    if isinstance(execution_contract, ExecutionContract)
                    else execution_contract
                ),
                "priority": priority,
                "resume_session_id": resume_session_id,
                "idempotency_key": key,
            },
            timeout_seconds=30.0,
        )

    @mcp.tool()
    def dispatch_card_to_instance(
        card_id: str,
        instance_id: str,
        idempotency_key: str,
        message: str = "",
        authority_instance_id: str | None = None,
        provider: str | None = None,
        model_id: str | None = None,
        model_provider: str | None = None,
        execution_preferences: dict | None = None,
        task_assessment: dict | None = None,
        expected_card_version: str | None = None,
        context_source_session_id: str | None = None,
        mode_id: str | None = None,
        collaboration_mode: CollaborationMode | None = None,
        collaboration_risk: str = "low",
        collaboration_ambiguous: bool = False,
        collaboration_unattended: bool = False,
        effort: str | None = None,
        cwd: str | None = None,
        config: dict[str, str | bool] | None = None,
        allow_concurrent: bool = False,
        concurrent_reason: str | None = None,
        capacity_override: bool = False,
        capacity_override_reason: str | None = None,
        participation_override: bool = False,
        participation_override_reason: str | None = None,
        execution_contract: ExecutionContract | None = None,
        priority: int = 0,
        resume_session_id: str | None = None,
    ) -> dict:
        """Durably and idempotently dispatch an authoritative card to a fleet instance."""
        key = idempotency_key.strip()
        if not key:
            raise ValueError("idempotency_key cannot be empty")
        payload = {
            "authority_instance_id": authority_instance_id,
            "card_id": card_id,
            "message": message,
            "provider": provider,
            "model_id": model_id,
            **{
                k: v
                for k, v in {
                    "model_provider": model_provider,
                    "execution_preferences": execution_preferences,
                    "task_assessment": task_assessment,
                    "expected_card_version": expected_card_version,
                    "context_source_session_id": context_source_session_id,
                }.items()
                if v is not None
            },
            "mode_id": mode_id,
            "collaboration_mode": (
                collaboration_mode.value
                if isinstance(collaboration_mode, CollaborationMode)
                else collaboration_mode
            ),
            "collaboration_risk": collaboration_risk,
            "collaboration_ambiguous": collaboration_ambiguous,
            "collaboration_unattended": collaboration_unattended,
            "effort": effort,
            "cwd": cwd,
            "config": config or {},
            "idempotency_key": key,
        }
        if priority:
            payload["priority"] = priority
        if execution_contract is not None:
            payload["execution_contract"] = (
                execution_contract.model_dump(mode="json")
                if isinstance(execution_contract, ExecutionContract)
                else execution_contract
            )
        if allow_concurrent:
            payload["allow_concurrent"] = True
        if concurrent_reason is not None:
            payload["concurrent_reason"] = concurrent_reason
        if capacity_override:
            payload["capacity_override"] = True
            payload["capacity_override_reason"] = capacity_override_reason
        if participation_override:
            payload["participation_override"] = True
            payload["participation_override_reason"] = participation_override_reason
        if resume_session_id:
            payload["resume_session_id"] = resume_session_id
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/fleet/instances/{instance_id}/agent/start",
            json=payload,
            timeout_seconds=30.0,
        )

    @mcp.tool()
    def get_dispatch(
        dispatch_id: str, authority_instance_id: str | None = None
    ) -> dict | None:
        """Get normalized durable dispatch, session, authority, target, and card-version state."""
        return request_local_pa(
            ctx.settings,
            "GET",
            (
                f"/api/fleet/instances/{authority_instance_id}/dispatch-jobs/{dispatch_id}"
                if authority_instance_id
                else f"/api/fleet/dispatch-jobs/{dispatch_id}"
            ),
            allow_not_found=True,
        )

    @mcp.tool()
    def get_assigned_dispatch() -> dict:
        """Read the dispatch bound to this assigned PA session."""
        return request_local_pa(
            ctx.settings,
            "GET",
            "/api/goal-assigned-session/dispatch",
        )

    @mcp.tool()
    def get_dispatch_queue() -> dict:
        """Return waiting, blocked, active, and queue-capacity state."""
        return request_local_pa(ctx.settings, "GET", "/api/fleet/dispatch-queue")

    @mcp.tool()
    def set_dispatch_priority(
        dispatch_id: str, priority: int, idempotency_key: str
    ) -> dict:
        """Idempotently reprioritize a waiting dispatch with audit history."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/fleet/dispatch-jobs/{dispatch_id}/priority",
            json={
                "priority": priority,
                "idempotency_key": idempotency_key,
            },
        )

    @mcp.tool()
    def report_dispatch_progress(
        dispatch_id: str,
        phase: Literal[
            "investigating",
            "planning",
            "implementing",
            "testing",
            "opening_pr",
            "waiting_ci",
            "addressing_review",
            "merging",
            "blocked",
            "retrying",
            "turn_ended",
            "completed",
        ],
        summary: str,
        idempotency_key: str,
        branch: str | None = None,
        commit_sha: str | None = None,
        pr_url: str | None = None,
        pr_number: int | None = None,
        changed_file_count: int | None = None,
        blockers: list[str] | None = None,
        retry_reason: str | None = None,
        operator_input: str | OperatorInputRequestV1 | None = None,
    ) -> dict:
        """Report progress. For bounded questions supply operator_input with a stable
        request_id, concise prompt and 2–4 choices (stable id, short label,
        optional description/value). Put explanation in details. Enable
        allow_freeform only when needed; preserve provider permission choices.
        Never substitute quoted JSON or final prose for a rejected request.
        """
        key = idempotency_key.strip()
        if not key:
            raise ValueError("idempotency_key cannot be empty")
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/fleet/dispatch-jobs/{dispatch_id}/checkpoint",
            json={
                "schema_version": PROGRESS_SCHEMA_VERSION,
                "phase": phase,
                "summary": summary,
                "branch": branch,
                "commit_sha": commit_sha,
                "pr_url": pr_url,
                "pr_number": pr_number,
                "changed_file_count": changed_file_count,
                "blockers": blockers or [],
                "retry_reason": retry_reason,
                "operator_input": (operator_input.model_dump(mode="json")
                    if isinstance(operator_input, OperatorInputRequestV1) else operator_input),
                "idempotency_key": key,
            },
        )

    @mcp.tool()
    def report_assigned_dispatch_progress(
        phase: Literal[
            "investigating",
            "planning",
            "implementing",
            "testing",
            "opening_pr",
            "waiting_ci",
            "addressing_review",
            "merging",
            "blocked",
            "retrying",
            "turn_ended",
            "completed",
        ],
        summary: str,
        idempotency_key: str,
        branch: str | None = None,
        commit_sha: str | None = None,
        pr_url: str | None = None,
        pr_number: int | None = None,
        changed_file_count: int | None = None,
        blockers: list[str] | None = None,
        retry_reason: str | None = None,
        operator_input: str | OperatorInputRequestV1 | None = None,
    ) -> dict:
        """Report progress for the dispatch bound to this assigned session."""
        key = idempotency_key.strip()
        if not key:
            raise ValueError("idempotency_key cannot be empty")
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/goal-assigned-session/progress",
            json={
                "schema_version": PROGRESS_SCHEMA_VERSION,
                "phase": phase,
                "summary": summary,
                "branch": branch,
                "commit_sha": commit_sha,
                "pr_url": pr_url,
                "pr_number": pr_number,
                "changed_file_count": changed_file_count,
                "blockers": blockers or [],
                "retry_reason": retry_reason,
                "operator_input": (operator_input.model_dump(mode="json")
                    if isinstance(operator_input, OperatorInputRequestV1) else operator_input),
                "idempotency_key": key,
            },
        )

    @mcp.tool()
    def preview_agent_restart_handoff(
        continuation_prompt: str = "",
    ) -> dict[str, Any]:
        """Preview a service restart and optional exact-session continuation without restarting."""
        import os
        from pa.acp.environment import ASSIGNED_SERVICE_SESSION_ENV

        session_id = os.environ.get(ASSIGNED_SERVICE_SESSION_ENV, "") or os.environ.get(
            "PA_BROWSER_SESSION_ID", ""
        )
        if not session_id:
            raise ValueError("Restart preview requires a managed PA session binding")
        from pa.mcp.server import assigned_service_mcp_mode

        assigned = assigned_service_mcp_mode()
        path = (
            "/api/goal-assigned-session/restart-handoffs" if assigned
            else f"/api/agent/sessions/{session_id}/restart-handoffs"
        )
        pending = [
            item
            for item in request_local_pa(ctx.settings, "GET", path)["handoffs"]
            if item["status"] not in {
                "failed",
                "continuation_delivered",
                "restart_completed",
            }
        ]
        prompt = continuation_prompt.strip()
        return {
            "operation": "service_restart_preview",
            "session_id": session_id,
            "explanation": (
                "PA will wait for the current turn, preserve durable state, restart the service, "
                + ("then queue the displayed continuation once." if prompt else "and will not send a continuation prompt.")
            ),
            "continuation_prompt": prompt or None,
            "will_send_continuation": bool(prompt),
            "pending_handoffs": pending,
            "next_action": "Edit the prompt if needed, then call request_agent_restart_handoff.",
        }

    @mcp.tool()
    def request_agent_restart_handoff(
        idempotency_key: str,
        continuation_prompt: str = "",
    ) -> dict | None:
        """Restart the PA service after preview; an optional continuation is delivered once."""
        import os
        from pa.acp.environment import (
            ASSIGNED_SERVICE_MODE_ENV,
            ASSIGNED_SERVICE_SESSION_ENV,
        )

        assigned = os.environ.get(ASSIGNED_SERVICE_MODE_ENV) == "1"
        bound_session = os.environ.get(ASSIGNED_SERVICE_SESSION_ENV, "")
        session_id = bound_session or os.environ.get("PA_BROWSER_SESSION_ID", "")
        if not session_id:
            raise ValueError("Restart handoff requires a managed PA session binding")
        path = (
            "/api/goal-assigned-session/restart-handoff"
            if assigned
            else f"/api/agent/sessions/{session_id}/restart-handoffs"
        )
        return request_local_pa(
            ctx.settings,
            "POST",
            path,
            json={
                "continuation_prompt": continuation_prompt,
                "idempotency_key": idempotency_key,
            },
            allow_not_found=True,
            timeout_seconds=15.0,
        )

    @mcp.tool()
    def edit_agent_restart_handoff(
        handoff_id: str,
        continuation_prompt: str = "",
    ) -> dict | None:
        """Edit or remove an already-pending restart continuation before quiescing."""
        import os
        from pa.acp.environment import (
            ASSIGNED_SERVICE_MODE_ENV,
            ASSIGNED_SERVICE_SESSION_ENV,
        )

        assigned = os.environ.get(ASSIGNED_SERVICE_MODE_ENV) == "1"
        session_id = os.environ.get(ASSIGNED_SERVICE_SESSION_ENV, "") or os.environ.get(
            "PA_BROWSER_SESSION_ID", ""
        )
        if not session_id:
            raise ValueError("Restart handoff edit requires a managed PA session binding")
        path = (
            "/api/goal-assigned-session/restart-handoff/edit"
            if assigned
            else f"/api/agent/sessions/{session_id}/restart-handoffs/{handoff_id}"
        )
        payload = {"continuation_prompt": continuation_prompt}
        if assigned:
            payload["handoff_id"] = handoff_id
        return request_local_pa(
            ctx.settings,
            "POST" if assigned else "PATCH",
            path,
            json=payload,
            allow_not_found=True,
            timeout_seconds=15.0,
        )

    @mcp.tool()
    def retry_dispatch(
        dispatch_id: str,
        idempotency_key: str,
        authority_instance_id: str | None = None,
    ) -> dict:
        """Idempotently queue a safe retry through the durable dispatch control plane."""
        key = idempotency_key.strip()
        if not key:
            raise ValueError("idempotency_key cannot be empty")
        return request_local_pa(
            ctx.settings,
            "POST",
            (
                f"/api/fleet/instances/{authority_instance_id}/dispatch-jobs/{dispatch_id}/retry"
                if authority_instance_id
                else f"/api/fleet/dispatch-jobs/{dispatch_id}/retry"
            ),
            json={"idempotency_key": key},
        )

    @mcp.tool()
    def cancel_dispatch(
        dispatch_id: str,
        idempotency_key: str,
        authority_instance_id: str | None = None,
    ) -> dict:
        """Idempotently request cancellation at a safe durable dispatch boundary."""
        key = idempotency_key.strip()
        if not key:
            raise ValueError("idempotency_key cannot be empty")
        return request_local_pa(
            ctx.settings,
            "POST",
            (
                f"/api/fleet/instances/{authority_instance_id}/dispatch-jobs/{dispatch_id}/cancel"
                if authority_instance_id
                else f"/api/fleet/dispatch-jobs/{dispatch_id}/cancel"
            ),
            json={"idempotency_key": key},
        )

    @mcp.tool()
    def prompt_dispatch_session(
        dispatch_id: str,
        message: str,
        idempotency_key: str,
        action: Literal["append", "prepend", "interrupt"] = "append",
        authority_instance_id: str | None = None,
    ) -> dict:
        """Durably prompt the live session linked to a dispatch without exposing CSRF state."""
        key = idempotency_key.strip()
        if not key:
            raise ValueError("idempotency_key cannot be empty")
        path = (
            f"/api/fleet/instances/{authority_instance_id}/dispatch-jobs/{dispatch_id}/prompt"
            if authority_instance_id
            else f"/api/fleet/dispatch-jobs/{dispatch_id}/prompt"
        )
        return request_local_pa(
            ctx.settings,
            "POST",
            path,
            json={
                "message": message,
                "action": action,
                "idempotency_key": key,
            },
        )

    @mcp.tool()
    def get_post_turn_action_catalog() -> dict:
        """List versioned follow-up actions, schemas, policy, and loop budgets."""
        return request_local_pa(
            ctx.settings, "GET", "/api/fleet/post-turn/action-catalog"
        )

    @mcp.tool()
    def get_dispatch_turn_end(dispatch_id: str) -> dict | None:
        """Read neutral turn-end snapshots, evaluations, actions, and diagnostics."""
        return request_local_pa(
            ctx.settings,
            "GET",
            f"/api/fleet/dispatch-jobs/{dispatch_id}/turn-end",
            allow_not_found=True,
        )

    @mcp.tool()
    def repair_terminal_dispatch(
        dispatch_id: str,
        idempotency_key: str,
        authority_instance_id: str | None = None,
        mode: Literal[
            "acknowledged_completion",
            "abandoned_without_acknowledgement",
            "closed_session_recovery",
        ] = "acknowledged_completion",
        expected_state: str | None = None,
        reason: str | None = None,
        confirm_no_outcome_inference: bool = False,
    ) -> dict:
        """Audit and normalize one evidence-qualified legacy dispatch row."""
        key = idempotency_key.strip()
        if not key:
            raise ValueError("idempotency_key cannot be empty")
        path = (
            f"/api/fleet/instances/{authority_instance_id}/dispatch-jobs/{dispatch_id}/repair-terminal"
            if authority_instance_id
            else f"/api/fleet/dispatch-jobs/{dispatch_id}/repair-terminal"
        )
        return request_local_pa(
            ctx.settings,
            "POST",
            path,
            json={
                "idempotency_key": key,
                "mode": mode,
                "expected_state": expected_state,
                "reason": reason,
                "confirm_no_outcome_inference": confirm_no_outcome_inference,
            },
        )
