from __future__ import annotations

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse

from pa.auth.middleware import get_principal_id
from pa.core.contracts import Module
from pa.core.context import AppContext
from pa.core.ui.pages import PageDefinition, PageRegistry
from pa.domain.models import ProjectCreate, ProjectUpdate
from pa.domain.store import get_store

router = APIRouter()
ui_router = APIRouter()


def _active_realm(request: Request) -> str:
    return (
        request.query_params.get("realm")
        or request.app.state.ctx.settings.primary_realm
    )


def _projects_context(request: Request) -> dict:
    store = get_store()
    realm = _active_realm(request)
    project_id = request.query_params.get("project")
    project = store.get_project(project_id, realm_id=realm) if project_id else None
    return {
        "projects": store.list_projects(realm_id=realm),
        "project": project,
        "cards": store.list_cards_for_project(project_id, realm_id=realm)
        if project_id
        else [],
        "active_realm": realm,
        "realms": request.app.state.ctx.settings.subscribed_realms,
    }


@router.get("/projects")
def list_projects_api(request: Request, realm: str | None = None) -> list[dict]:
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    projects = get_store().list_projects(realm_id=realm_id)
    return [p.model_dump(mode="json") for p in projects]


@router.post("/projects", status_code=201)
def create_project_api(request: Request, data: ProjectCreate) -> dict:
    store = get_store()
    project = store.create_project(
        data,
        principal_id=get_principal_id(request),
        instance_id=request.app.state.ctx.settings.instance_id,
    )
    return project.model_dump(mode="json")


@router.get("/projects/{project_id}")
def get_project_api(
    request: Request, project_id: str, realm: str | None = None
) -> dict:
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    project = get_store().get_project(project_id, realm_id=realm_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project.model_dump(mode="json")


@router.patch("/projects/{project_id}")
def update_project_api(
    request: Request,
    project_id: str,
    data: ProjectUpdate,
    realm: str | None = None,
) -> dict:
    settings = request.app.state.ctx.settings
    realm_id = realm or settings.primary_realm
    project = get_store().update_project(
        project_id,
        data,
        realm_id=realm_id,
        principal_id=get_principal_id(request),
        instance_id=settings.instance_id,
    )
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project.model_dump(mode="json")


@router.get("/projects/{project_id}/cards")
def project_cards_api(
    request: Request, project_id: str, realm: str | None = None
) -> list[dict]:
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    cards = get_store().list_cards_for_project(project_id, realm_id=realm_id)
    return [c.model_dump(mode="json") for c in cards]


@router.post("/projects/{project_id}/assign/{card_id}")
def assign_card_api(
    request: Request,
    project_id: str,
    card_id: str,
    realm: str | None = None,
) -> dict:
    settings = request.app.state.ctx.settings
    realm_id = realm or _active_realm(request)
    card = get_store().assign_card_to_project(
        card_id,
        project_id,
        realm_id=realm_id,
        principal_id=get_principal_id(request),
        instance_id=settings.instance_id,
    )
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    return card.model_dump(mode="json")


@ui_router.get("/projects")
def projects_page(request: Request):
    from pa.modules.ui_shell import render_page

    page = request.app.state.ctx.require_service("pages").get_by_path("/projects")
    if not page:
        raise HTTPException(status_code=404)
    return render_page(request, page)


@ui_router.post("/projects", response_model=None)
def create_project_ui(
    request: Request,
    title: str = Form(...),
    description: str = Form(""),
    realm: str | None = None,
) -> HTMLResponse:
    from pa.modules.ui_shell import render_page

    realm_id = realm or _active_realm(request)
    get_store().create_project(
        ProjectCreate(realm_id=realm_id, title=title, description=description),
        principal_id=get_principal_id(request),
        instance_id=request.app.state.ctx.settings.instance_id,
    )
    page = request.app.state.ctx.require_service("pages").get_by_path("/projects")
    if not page:
        raise HTTPException(status_code=404)
    return render_page(request, page)


class ProjectsModule(Module):
    @property
    def name(self) -> str:
        return "projects"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def description(self) -> str:
        return "Projects — card containers with agent context and metadata"

    def on_load(self, ctx: AppContext) -> None:
        pages: PageRegistry = ctx.require_service("pages")
        pages.register(
            PageDefinition(
                id="projects",
                path="/projects",
                label="Projects",
                icon="projects",
                template="pages/projects.html",
                nav_order=15,
                context_builder=_projects_context,
            )
        )

    def api_routers(self):
        return [("/api", router, ["projects"])]

    def ui_routers(self):
        return [ui_router]

    def register_mcp(self, mcp, ctx: AppContext) -> None:
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
