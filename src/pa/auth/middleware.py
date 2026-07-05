"""Auth middleware and dependencies."""

from __future__ import annotations

from fastapi import HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from pa.auth.sessions import SessionManager
from pa.auth.users import UserDirectory
from pa.config import Settings

PUBLIC_PATHS = {
    "/api/health",
    "/api/auth/login",
    "/api/fleet/join",
    "/login",
}


def _is_public(path: str) -> bool:
    if path in PUBLIC_PATHS:
        return True
    if path.startswith("/static/"):
        return True
    if path.startswith("/partials/") and path.endswith("/public"):
        return True
    return False


class AuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, settings: Settings, users: UserDirectory, sessions: SessionManager):
        super().__init__(app)
        self.settings = settings
        self.users = users
        self.sessions = sessions

    async def dispatch(self, request: Request, call_next) -> Response:
        request.state.principal_id = None
        request.state.user = None

        # Instance auth for sync/fleet API
        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
            if self.settings.sync_token and token == self.settings.sync_token:
                request.state.instance_authenticated = True
            else:
                user = self.users.get_by_cli_token(token)
                if user:
                    request.state.user = user
                    request.state.principal_id = f"user:{user.id}"
            if not request.state.principal_id:
                cli_user = self.users.get_by_cli_token(token)
                if cli_user:
                    request.state.user = cli_user
                    request.state.principal_id = f"user:{cli_user.id}"

        # Session cookie
        session_token = request.cookies.get(self.sessions.COOKIE_NAME)
        if session_token:
            uid = self.sessions.verify_token(session_token)
            if uid:
                user = self.users.get(uid)
                if user:
                    request.state.user = user
                    request.state.principal_id = f"user:{user.id}"

        if not request.state.principal_id:
            default = self.users.ensure_default_user()
            request.state.user = default
            request.state.principal_id = f"user:{default.id}"

        response = await call_next(request)
        return response


def require_user(request: Request):
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user


def get_principal_id(request: Request) -> str:
    return getattr(request.state, "principal_id", "user:local")
