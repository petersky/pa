from __future__ import annotations

from fastapi import APIRouter, Form, HTTPException, Request, Response

from pa.auth.middleware import require_user
from pa.auth.sessions import SessionManager
from pa.auth.users import UserDirectory
from pa.core.contracts import Module
from pa.core.context import AppContext

router = APIRouter()


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
    response.set_cookie(SessionManager.COOKIE_NAME, token, httponly=True, samesite="lax", max_age=86400 * 7)
    return {"user_id": user.id, "username": user.username}


@router.post("/auth/logout")
def logout(response: Response) -> dict:
    response.delete_cookie(SessionManager.COOKIE_NAME)
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
