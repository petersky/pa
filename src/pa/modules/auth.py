from __future__ import annotations

import secrets

from fastapi import APIRouter, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse

from pa.auth.csrf import COOKIE_NAME
from pa.auth.middleware import require_user
from pa.auth.sessions import SessionManager
from pa.auth.users import UserDirectory
from pa.core.contracts import Module
from pa.core.context import AppContext

router = APIRouter()
ui_router = APIRouter()


@router.post("/auth/login")
def login(
    request: Request,
    response: Response,
    username: str = Form(...),
    password: str = Form(...),
) -> dict:
    users: UserDirectory = request.app.state.ctx.require_service("users")
    sessions: SessionManager = request.app.state.ctx.require_service("sessions")
    user = users.authenticate(username, password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = sessions.create_token(user)
    response.set_cookie(
        SessionManager.COOKIE_NAME,
        token,
        httponly=True,
        samesite="lax",
        max_age=86400 * 7,
    )
    return {"user_id": user.id, "username": user.username}


@router.post("/auth/logout")
def logout(response: Response) -> dict:
    response.delete_cookie(
        SessionManager.COOKIE_NAME,
        path="/",
        samesite="lax",
    )
    return {"ok": True}


@router.get("/auth/me")
def me(request: Request) -> dict:
    user = require_user(request)
    return {
        "id": user.id,
        "username": user.username,
        "display_name": user.display_name,
        "role": user.role,
    }


@ui_router.get("/login", response_class=HTMLResponse)
def login_page(request: Request) -> HTMLResponse:
    templates = request.app.state.templates
    assets = request.app.state.ctx.require_service("assets")
    token = request.cookies.get(COOKIE_NAME, "")
    return templates.TemplateResponse(
        request,
        "pages/login.html",
        {
            "csrf_token": token,
            "static_url": assets.url,
            "error": request.query_params.get("error"),
        },
    )


@ui_router.post("/login")
def login_form(
    request: Request,
    response: Response,
    username: str = Form(...),
    password: str = Form(...),
    csrf: str = Form(""),
) -> Response:
    cookie = request.cookies.get(COOKIE_NAME, "")
    if not cookie or not csrf or not secrets.compare_digest(cookie, csrf):
        return RedirectResponse("/login?error=Invalid+session", status_code=303)

    users: UserDirectory = request.app.state.ctx.require_service("users")
    sessions: SessionManager = request.app.state.ctx.require_service("sessions")
    user = users.authenticate(username, password)
    if not user:
        return RedirectResponse("/login?error=Invalid+credentials", status_code=303)

    token = sessions.create_token(user)
    response = RedirectResponse("/", status_code=303)
    response.set_cookie(
        SessionManager.COOKIE_NAME,
        token,
        httponly=True,
        samesite="lax",
        max_age=86400 * 7,
    )
    return response


class AuthModule(Module):
    @property
    def name(self) -> str:
        return "auth"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def description(self) -> str:
        return "Local user authentication and sessions"

    def on_load(self, ctx: AppContext) -> None:
        users = UserDirectory(ctx.settings.data_dir)
        users.ensure_default_user()
        ctx.register_service("users", users)
        ctx.register_service("sessions", SessionManager(ctx.settings.session_secret))

    def api_routers(self):
        return [("/api", router, ["auth"])]

    def ui_routers(self):
        return [ui_router]
