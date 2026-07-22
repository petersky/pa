from __future__ import annotations

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse

from pa.auth.middleware import get_principal_id
from pa.core.contracts import Module
from pa.core.context import AppContext
from pa.core.ui.pages import PageDefinition, PageRegistry
from pa.domain.models import (
    CardLane,
    ProjectCreate,
    ProjectUpdate,
    RepositoryCheckout,
    RepositoryCreate,
    RepositoryUpdate,
)
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
    cards = (
        store.list_cards_for_project(project_id, realm_id=realm) if project_id else []
    )
    card_sessions = {}
    for session in store.list_sessions():
        if session.card_id and session.card_id not in card_sessions:
            card_sessions[session.card_id] = session
    return {
        "projects": store.list_projects(realm_id=realm),
        "repositories": store.list_repositories(realm),
        "project": project,
        "cards": cards,
        "card_projects": {card.id: project for card in cards},
        "card_sessions": card_sessions,
        "lanes": list(CardLane),
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


@router.get("/realm/repositories")
def list_repositories_api(request: Request, realm: str | None = None) -> list[dict]:
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    return [r.model_dump(mode="json") for r in get_store().list_repositories(realm_id)]


@router.post("/repositories", status_code=201)
def create_repository_api(request: Request, data: RepositoryCreate) -> dict:
    settings = request.app.state.ctx.settings
    repository = get_store().create_repository(
        data, principal_id=get_principal_id(request), instance_id=settings.instance_id
    )
    return repository.model_dump(mode="json")


@router.get("/repositories/{repository_id}")
def get_repository_api(
    request: Request, repository_id: str, realm: str | None = None
) -> dict:
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    repository = get_store().get_repository(repository_id, realm_id)
    if not repository:
        raise HTTPException(status_code=404, detail="Repository not found")
    result = repository.model_dump(mode="json")
    result["checkouts"] = [
        c.model_dump(mode="json")
        for c in get_store().list_repository_checkouts(repository_id)
    ]
    return result


@router.patch("/repositories/{repository_id}")
def update_repository_api(
    request: Request,
    repository_id: str,
    data: RepositoryUpdate,
    realm: str | None = None,
) -> dict:
    settings = request.app.state.ctx.settings
    realm_id = realm or settings.primary_realm
    try:
        repository = get_store().update_repository(
            repository_id,
            data,
            realm_id=realm_id,
            principal_id=get_principal_id(request),
            instance_id=settings.instance_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not repository:
        raise HTTPException(status_code=404, detail="Repository not found")
    return repository.model_dump(mode="json")


@router.delete("/repositories/{repository_id}", status_code=204)
def delete_repository_api(
    request: Request, repository_id: str, realm: str | None = None
) -> None:
    settings = request.app.state.ctx.settings
    realm_id = realm or settings.primary_realm
    if not get_store().delete_repository(
        repository_id,
        realm_id=realm_id,
        principal_id=get_principal_id(request),
        instance_id=settings.instance_id,
    ):
        raise HTTPException(status_code=404, detail="Repository not found")


@router.put("/projects/{project_id}/repositories/{repository_id}")
def link_repository_api(
    request: Request,
    project_id: str,
    repository_id: str,
    body: dict | None = None,
    realm: str | None = None,
) -> dict:
    settings = request.app.state.ctx.settings
    realm_id = realm or settings.primary_realm
    if not get_store().link_project_repository(
        project_id,
        repository_id,
        branch=(body or {}).get("branch"),
        realm_id=realm_id,
        principal_id=get_principal_id(request),
        instance_id=settings.instance_id,
    ):
        raise HTTPException(status_code=404, detail="Project or repository not found")
    return get_store().get_project(project_id, realm_id).model_dump(mode="json")


@router.delete("/projects/{project_id}/repositories/{repository_id}", status_code=204)
def unlink_repository_api(
    request: Request, project_id: str, repository_id: str, realm: str | None = None
) -> None:
    settings = request.app.state.ctx.settings
    realm_id = realm or settings.primary_realm
    get_store().unlink_project_repository(
        project_id,
        repository_id,
        realm_id=realm_id,
        principal_id=get_principal_id(request),
        instance_id=settings.instance_id,
    )


@router.put("/repositories/{repository_id}/checkouts/{checkout_instance_id}")
def set_checkout_api(
    request: Request,
    repository_id: str,
    checkout_instance_id: str,
    body: dict,
    realm: str | None = None,
) -> dict:
    settings = request.app.state.ctx.settings
    realm_id = realm or settings.primary_realm
    if not get_store().get_repository(repository_id, realm_id):
        raise HTTPException(status_code=404, detail="Repository not found")
    checkout = RepositoryCheckout(
        repository_id=repository_id,
        instance_id=checkout_instance_id,
        path=body.get("path", ""),
        branch=body.get("branch"),
    )
    get_store().set_repository_checkout(
        checkout,
        realm_id=realm_id,
        principal_id=get_principal_id(request),
        instance_id=settings.instance_id,
    )
    return checkout.model_dump(mode="json")


@router.delete(
    "/repositories/{repository_id}/checkouts/{checkout_instance_id}", status_code=204
)
def remove_checkout_api(
    request: Request,
    repository_id: str,
    checkout_instance_id: str,
    realm: str | None = None,
) -> None:
    settings = request.app.state.ctx.settings
    realm_id = realm or settings.primary_realm
    get_store().remove_repository_checkout(
        repository_id,
        checkout_instance_id,
        realm_id=realm_id,
        principal_id=get_principal_id(request),
        instance_id=settings.instance_id,
    )


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


@ui_router.post("/projects/repositories")
def create_repository_ui(
    request: Request,
    url: str = Form(...),
    name: str = Form(""),
    realm: str | None = None,
) -> HTMLResponse:
    from pa.modules.ui_shell import render_page

    realm_id = realm or _active_realm(request)
    settings = request.app.state.ctx.settings
    get_store().create_repository(
        RepositoryCreate(realm_id=realm_id, url=url, name=name),
        principal_id=get_principal_id(request),
        instance_id=settings.instance_id,
    )
    page = request.app.state.ctx.require_service("pages").get_by_path("/projects")
    return render_page(request, page)


@ui_router.post("/projects/{project_id}/repositories")
def link_repository_ui(
    request: Request,
    project_id: str,
    repository_id: str = Form(...),
    branch: str = Form(""),
    path: str = Form(""),
    realm: str | None = None,
) -> HTMLResponse:
    from pa.modules.ui_shell import render_page

    realm_id = realm or _active_realm(request)
    settings = request.app.state.ctx.settings
    store = get_store()
    if not store.link_project_repository(
        project_id,
        repository_id,
        branch=branch or None,
        realm_id=realm_id,
        principal_id=get_principal_id(request),
        instance_id=settings.instance_id,
    ):
        raise HTTPException(status_code=404, detail="Project or repository not found")
    if path:
        store.set_repository_checkout(
            RepositoryCheckout(
                repository_id=repository_id,
                instance_id=settings.instance_id,
                path=path,
                branch=branch or None,
            ),
            realm_id=realm_id,
            principal_id=get_principal_id(request),
            instance_id=settings.instance_id,
        )
    page = request.app.state.ctx.require_service("pages").get_by_path("/projects")
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
