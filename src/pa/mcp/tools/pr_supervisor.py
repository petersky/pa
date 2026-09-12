"""pr_supervisor tools: authenticated owner API proxies."""

from __future__ import annotations

from typing import Any
from pa.core.context import AppContext
from pa.domain.models import Project
from pa.pr_supervisor.models import PRPolicy


def register_mcp(mcp, ctx: AppContext) -> None:
    from pa.mcp.local_api import request_local_pa

    async_runtime = ctx.require_service("async_runtime")

    @mcp.tool()
    def list_pr_watches(
        realm: str = "default",
        card_id: str | None = None,
        include_retired: bool = False,
    ) -> list[dict[str, Any]]:
        """List durable PR watches and their current lifecycle state."""
        return request_local_pa(
            ctx.settings,
            "GET",
            "/api/pr-supervisor/watches",
            params={
                "realm": realm,
                "card_id": card_id,
                "include_retired": include_retired,
            },
        )

    @mcp.tool()
    def get_pr_watch(watch_id: str) -> dict[str, Any] | None:
        """Get a PR watch and its audit history."""
        return request_local_pa(
            ctx.settings,
            "GET",
            f"/api/pr-supervisor/watches/{watch_id}",
            allow_not_found=True,
        )

    @mcp.tool()
    async def create_pr_watch(
        repository: str,
        pr_number: int,
        pr_url: str,
        realm: str = "default",
        project_id: str | None = None,
        card_id: str | None = None,
        originating_session_id: str | None = None,
        originating_agent: str | None = None,
        executor_cwd: str | None = None,
    ) -> dict[str, Any]:
        """Create a durable, fleet-supervised PR watch."""
        return await async_runtime.run_blocking(
            "mcp.pr_watch_create_http",
            request_local_pa,
            ctx.settings,
            "POST",
            "/api/pr-supervisor/watches",
            json={
                "realm_id": realm,
                "project_id": project_id,
                "card_id": card_id,
                "repository": repository,
                "pr_number": pr_number,
                "pr_url": pr_url,
                "originating_session_id": originating_session_id,
                "originating_agent": originating_agent,
                "executor_cwd": executor_cwd,
            },
        )

    @mcp.tool()
    async def refresh_pr_watch(watch_id: str) -> dict[str, Any]:
        """Schedule an immediate refresh for an active PR watch."""
        return await async_runtime.run_blocking(
            "mcp.pr_watch_refresh_http",
            request_local_pa,
            ctx.settings,
            "POST",
            f"/api/pr-supervisor/watches/{watch_id}/refresh",
        )

    @mcp.tool()
    async def retire_pr_watch(watch_id: str) -> dict[str, Any] | None:
        """Retire a PR watch without deleting its audit history."""
        return await async_runtime.run_blocking(
            "mcp.pr_watch_retire_http",
            request_local_pa,
            ctx.settings,
            "DELETE",
            f"/api/pr-supervisor/watches/{watch_id}",
            allow_not_found=True,
        )

    @mcp.tool()
    async def backfill_terminal_pr_watches(
        realm: str = "default",
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Revalidate and archive legacy merged/closed watches idempotently."""
        return await async_runtime.run_blocking(
            "mcp.pr_watch_terminal_backfill_http",
            request_local_pa,
            ctx.settings,
            "POST",
            "/api/pr-supervisor/migrations/terminal-retirements",
            json={"realm_id": realm, "dry_run": dry_run},
        )

    @mcp.tool()
    async def create_supervised_pull_request(
        repository: str,
        title: str,
        head: str,
        base: str = "main",
        body: str = "",
        realm: str = "default",
        project_id: str | None = None,
        card_id: str | None = None,
        originating_session_id: str | None = None,
        executor_cwd: str | None = None,
        draft: bool | None = None,
    ) -> dict[str, Any]:
        """Open a PR ready for review by policy and immediately supervise it."""
        return await async_runtime.run_blocking(
            "mcp.pr_create_http",
            request_local_pa,
            ctx.settings,
            "POST",
            "/api/pr-supervisor/pull-requests",
            json={
                "repository": repository,
                "title": title,
                "head": head,
                "base": base,
                "body": body,
                "realm_id": realm,
                "project_id": project_id,
                "card_id": card_id,
                "originating_session_id": originating_session_id,
                "executor_cwd": executor_cwd,
                "draft": draft,
            },
        )

    @mcp.tool()
    def set_project_pr_policy(
        project_id: str,
        realm: str = "default",
        repository: str | None = None,
        ready_by_default: bool = True,
        auto_notify: bool = True,
        agent_merge_on_green: bool = True,
        repair_failed_checks: bool = True,
        required_checks: list[str] | None = None,
    ) -> dict[str, Any] | None:
        """Set project-wide or repository-specific PR supervision policy."""
        project_data = request_local_pa(
            ctx.settings,
            "GET",
            f"/api/projects/{project_id}",
            params={"realm": realm},
            allow_not_found=True,
        )
        project = Project.model_validate(project_data) if project_data else None
        if not project:
            return None
        config = dict(project.tool_config or {})
        if repository:
            policies = dict(config.get("pr_repository_policies") or {})
            policy_data = dict(
                policies.get(repository) or config.get("pr_policy") or {}
            )
            policy_data.update(
                {
                    "ready_by_default": ready_by_default,
                    "auto_notify": auto_notify,
                    "agent_merge_on_green": agent_merge_on_green,
                    "repair_failed_checks": repair_failed_checks,
                    "required_checks": (
                        required_checks
                        if required_checks is not None
                        else policy_data.get("required_checks", [])
                    ),
                }
            )
            policy = PRPolicy.model_validate(policy_data)
            policies[repository] = policy.model_dump(mode="json")
            config["pr_repository_policies"] = policies
        else:
            policy_data = dict(config.get("pr_policy") or {})
            policy_data.update(
                {
                    "ready_by_default": ready_by_default,
                    "auto_notify": auto_notify,
                    "agent_merge_on_green": agent_merge_on_green,
                    "repair_failed_checks": repair_failed_checks,
                    "required_checks": (
                        required_checks
                        if required_checks is not None
                        else policy_data.get("required_checks", [])
                    ),
                }
            )
            policy = PRPolicy.model_validate(policy_data)
            config["pr_policy"] = policy.model_dump(mode="json")
        updated = request_local_pa(
            ctx.settings,
            "PUT",
            f"/api/pr-supervisor/policies/projects/{project_id}",
            params={"realm": realm},
            json={
                "repository": repository,
                "policy": policy.model_dump(mode="json"),
            },
        )
        return {
            "project_id": project_id,
            "repository": repository,
            "policy": policy.model_dump(mode="json"),
            "tool_config": updated.get("tool_config", config)
            if updated
            else config,
        }

    @mcp.tool()
    async def diagnose_pr_watch_provenance(
        realm: str = "default", include_retired: bool = True
    ) -> dict[str, Any]:
        """Detect malformed, shortened, missing, or mismatched watch provenance."""
        return await async_runtime.run_blocking(
            "mcp.pr_watch_provenance_diagnostics_http",
            request_local_pa,
            ctx.settings,
            "GET",
            "/api/pr-supervisor/provenance/issues",
            params={"realm": realm, "include_retired": include_retired},
        )

    @mcp.tool()
    async def repair_pr_watch_provenance(
        watch_id: str,
        originating_session_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Audited relink of a corrupt watch to one explicit canonical session."""
        return await async_runtime.run_blocking(
            "mcp.pr_watch_provenance_repair_http",
            request_local_pa,
            ctx.settings,
            "POST",
            f"/api/pr-supervisor/watches/{watch_id}/provenance/repair",
            json={
                "originating_session_id": originating_session_id,
                "idempotency_key": idempotency_key,
            },
        )

    @mcp.tool()
    def github_supervision_scope() -> dict[str, Any]:
        """Read this instance's repository scope and CAS revision without credentials."""
        return request_local_pa(ctx.settings, "GET", "/api/github/supervision-scope")

    @mcp.tool()
    def preview_github_supervision_scope(
        allowed_repositories: list[str], expected_revision: str,
    ) -> dict[str, Any]:
        """Validate access and return exact structured operator confirmation choices.

        During a dispatch, pass operator_input unchanged to report_dispatch_progress.
        Await its correlated response before calling update. Never auto-submit a choice.
        """
        return request_local_pa(ctx.settings, "POST", "/api/github/supervision-scope/preview",
            json={"allowed_repositories": allowed_repositories, "expected_revision": expected_revision})

    @mcp.tool()
    def update_github_supervision_scope(
        allowed_repositories: list[str], expected_revision: str, idempotency_key: str,
        confirmed_additions: list[str], confirmation_id: str,
    ) -> dict[str, Any]:
        """Apply the operator's exact confirmed scope using CAS, audit and replay protection.

        Use only the correlated apply_scope response from the preview interaction;
        preserve confirmation_id. Keep/cancel never authorizes this update.
        This never accepts or returns tokens, and never permits an empty wildcard list.
        """
        return request_local_pa(ctx.settings, "PUT", "/api/github/supervision-scope",
            json={"allowed_repositories": allowed_repositories, "expected_revision": expected_revision,
                  "idempotency_key": idempotency_key, "confirmed_additions": confirmed_additions,
                  "confirmation_id": confirmation_id})

    @mcp.tool()
    def github_supervision_scope_audit() -> dict[str, Any]:
        """Read local repository scope changes and confirmation provenance without secrets."""
        return request_local_pa(ctx.settings, "GET", "/api/github/supervision-scope/audit")

    @mcp.tool()
    def github_integration_capability() -> dict[str, Any]:
        """Report local GitHub authentication/webhook capability without secrets."""
        capabilities = request_local_pa(
            ctx.settings,
            "GET",
            "/api/pr-supervisor/capabilities",
        )
        return capabilities["local"]
