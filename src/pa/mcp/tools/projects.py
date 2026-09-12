"""projects tools: authenticated owner API proxies."""

from __future__ import annotations

from pa.core.context import AppContext
from pa.domain.models import RepositoryStatus, RepositoryVisibility


def register_mcp(mcp, ctx: AppContext) -> None:
    from pa.mcp.local_api import request_local_pa

    @mcp.tool()
    def list_projects(realm: str = "default") -> list[dict]:
        """List projects in a realm."""
        return request_local_pa(
            ctx.settings, "GET", "/api/projects", params={"realm": realm}
        )

    @mcp.tool()
    def get_project(project_id: str, realm: str = "default") -> dict | None:
        """Get a project by ID."""
        return request_local_pa(
            ctx.settings,
            "GET",
            f"/api/projects/{project_id}",
            params={"realm": realm},
            allow_not_found=True,
        )

    @mcp.tool()
    def create_project(
        title: str,
        description: str = "",
        realm: str = "default",
        agent_prompt: str = "",
    ) -> dict:
        """Create a new project."""
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/projects",
            json={
                "realm_id": realm,
                "title": title,
                "description": description,
                "agent_prompt": agent_prompt,
            },
        )

    @mcp.tool()
    def update_project(
        project_id: str,
        title: str | None = None,
        description: str | None = None,
        agent_prompt: str | None = None,
        realm: str = "default",
    ) -> dict | None:
        """Update project fields."""
        return request_local_pa(
            ctx.settings,
            "PATCH",
            f"/api/projects/{project_id}",
            params={"realm": realm},
            json={
                key: value
                for key, value in {
                    "title": title,
                    "description": description,
                    "agent_prompt": agent_prompt,
                }.items()
                if value is not None
            },
        )

    @mcp.tool()
    def list_repositories(realm: str = "default") -> list[dict]:
        """List synchronized first-class repositories in a realm."""
        return request_local_pa(
            ctx.settings,
            "GET",
            "/api/realm/repositories",
            params={"realm": realm},
        )

    @mcp.tool()
    def get_repository(repository_id: str, realm: str = "default") -> dict | None:
        """Get repository metadata and per-instance checkouts."""
        return request_local_pa(
            ctx.settings,
            "GET",
            f"/api/repositories/{repository_id}",
            params={"realm": realm},
            allow_not_found=True,
        )

    @mcp.tool()
    def create_repository(
        url: str,
        name: str = "",
        realm: str = "default",
        remotes: list[dict] | None = None,
        default_branch: str | None = None,
        provider: str = "",
        provider_repository_id: str | None = None,
        provider_metadata: dict | None = None,
        visibility: RepositoryVisibility = RepositoryVisibility.REALM,
        status: RepositoryStatus = RepositoryStatus.ACTIVE,
    ) -> dict:
        """Create a synchronized first-class repository."""
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/repositories",
            json={
                "realm_id": realm,
                "url": url,
                "name": name,
                "remotes": remotes or [],
                "default_branch": default_branch,
                "provider": provider,
                "provider_repository_id": provider_repository_id,
                "provider_metadata": provider_metadata or {},
                "visibility": visibility,
                "status": status,
            },
        )

    @mcp.tool()
    def update_repository(
        repository_id: str,
        name: str | None = None,
        remotes: list[dict] | None = None,
        default_branch: str | None = None,
        provider: str | None = None,
        provider_repository_id: str | None = None,
        provider_metadata: dict | None = None,
        visibility: RepositoryVisibility | None = None,
        status: RepositoryStatus | None = None,
        clear_fields: list[str] | None = None,
        realm: str = "default",
    ) -> dict | None:
        """Update metadata or lifecycle; clear nullable fields by name."""
        fields = {
            "name": name,
            "remotes": remotes,
            "default_branch": default_branch,
            "provider": provider,
            "provider_repository_id": provider_repository_id,
            "provider_metadata": provider_metadata,
            "visibility": visibility,
            "status": status,
        }
        nullable_fields = {"default_branch", "provider_repository_id"}
        requested_clears = set(clear_fields or [])
        unsupported = requested_clears - nullable_fields
        if unsupported:
            names = ", ".join(sorted(unsupported))
            raise ValueError(f"Unsupported nullable repository fields: {names}")
        payload = {key: value for key, value in fields.items() if value is not None}
        payload.update({key: None for key in requested_clears})
        return request_local_pa(
            ctx.settings,
            "PATCH",
            f"/api/repositories/{repository_id}",
            params={"realm": realm},
            json=payload,
            allow_not_found=True,
        )

    @mcp.tool()
    def delete_repository(repository_id: str, realm: str = "default") -> None:
        """Delete a repository and its project links and checkout records."""
        return request_local_pa(
            ctx.settings,
            "DELETE",
            f"/api/repositories/{repository_id}",
            params={"realm": realm},
        )

    @mcp.tool()
    def list_project_repositories(
        project_id: str, realm: str = "default"
    ) -> list[dict]:
        """List normalized repositories linked to a project."""
        return request_local_pa(
            ctx.settings,
            "GET",
            f"/api/projects/{project_id}/repositories",
            params={"realm": realm},
        )

    @mcp.tool()
    def link_project_repository(
        project_id: str,
        repository_id: str,
        branch: str | None = None,
        realm: str = "default",
    ) -> dict:
        """Link a repository to a project with an optional requested branch."""
        return request_local_pa(
            ctx.settings,
            "PUT",
            f"/api/projects/{project_id}/repositories/{repository_id}",
            params={"realm": realm},
            json={"branch": branch},
        )

    @mcp.tool()
    def unlink_project_repository(
        project_id: str,
        repository_id: str,
        realm: str = "default",
    ) -> None:
        """Unlink a repository from a project."""
        return request_local_pa(
            ctx.settings,
            "DELETE",
            f"/api/projects/{project_id}/repositories/{repository_id}",
            params={"realm": realm},
        )

    @mcp.tool()
    def set_repository_checkout(
        repository_id: str,
        checkout_instance_id: str,
        path: str,
        branch: str | None = None,
        realm: str = "default",
    ) -> dict:
        """Set a repository checkout for one fleet instance."""
        return request_local_pa(
            ctx.settings,
            "PUT",
            f"/api/repositories/{repository_id}/checkouts/{checkout_instance_id}",
            params={"realm": realm},
            json={"path": path, "branch": branch},
        )

    @mcp.tool()
    def remove_repository_checkout(
        repository_id: str,
        checkout_instance_id: str,
        realm: str = "default",
    ) -> None:
        """Remove a repository checkout for one fleet instance."""
        return request_local_pa(
            ctx.settings,
            "DELETE",
            f"/api/repositories/{repository_id}/checkouts/{checkout_instance_id}",
            params={"realm": realm},
        )

    @mcp.tool()
    def assign_card_to_project(
        card_id: str,
        project_id: str,
        realm: str = "default",
    ) -> dict | None:
        """Assign a card to a project."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/projects/{project_id}/assign/{card_id}",
            params={"realm": realm},
        )
