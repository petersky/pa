"""Auth middleware and dependencies."""

from __future__ import annotations

import hmac

from fastapi import HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, Response

from pa.auth.cookies import use_secure_cookies
from pa.auth.csrf import COOKIE_NAME, generate_token, validate_request
from pa.auth.sessions import SessionManager
from pa.auth.users import UserDirectory
from pa.config import Settings

PUBLIC_PATHS = {
    "/api/health",
    "/api/auth/login",
    "/api/fleet/join",
    "/login",
}

SYNC_PATHS = {
    "/api/sync/have",
    "/api/sync/get",
    "/api/sync/push",
    "/api/sync/relay",
    "/api/sync/refs",
}

CSRF_EXEMPT_PATHS = {
    "/api/fleet/join",
    "/api/auth/login",
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


def _is_sync_path(path: str) -> bool:
    return path in SYNC_PATHS


def _needs_csrf(request: Request) -> bool:
    if request.method not in ("POST", "PUT", "PATCH", "DELETE"):
        return False
    path = request.url.path
    if _is_public(path) or path in CSRF_EXEMPT_PATHS:
        return False
    if path.startswith("/api/") and request.headers.get("authorization", "").startswith("Bearer "):
        return False
    return True


class AuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, settings: Settings, users: UserDirectory, sessions: SessionManager):
        super().__init__(app)
        self.settings = settings
        self.users = users
        self.sessions = sessions

    async def dispatch(self, request: Request, call_next) -> Response:
        request.state.principal_id = None
        request.state.user = None
        request.state.instance_authenticated = False

        path = request.url.path
        is_public = _is_public(path)

        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
            if self.settings.sync_token and hmac.compare_digest(
                token, self.settings.sync_token
            ):
                request.state.instance_authenticated = True
            else:
                user = self.users.get_by_cli_token(token)
                if user:
                    request.state.user = user
                    request.state.principal_id = f"user:{user.id}"

        session_token = request.cookies.get(self.sessions.COOKIE_NAME)
        if session_token and not request.state.principal_id:
            uid = self.sessions.verify_token(session_token)
            if uid:
                user = self.users.get(uid)
                if user:
                    request.state.user = user
                    request.state.principal_id = f"user:{user.id}"

        if (
            self.settings.auth_required
            and _is_sync_path(path)
            and not is_public
            and not request.state.instance_authenticated
        ):
            return JSONResponse(
                {"detail": "Instance authentication required"},
                status_code=401,
            )

        if not request.state.principal_id:
            needs_user_auth = (
                self.settings.auth_required
                and path.startswith("/api/")
                and not is_public
                and not _is_sync_path(path)
            )
            if needs_user_auth:
                return JSONResponse(
                    {"detail": "Authentication required"},
                    status_code=401,
                )
            default = self.users.ensure_default_user()
            request.state.user = default
            request.state.principal_id = f"user:{default.id}"

        if _needs_csrf(request) and not validate_request(request):
            return JSONResponse({"detail": "CSRF validation failed"}, status_code=403)

        response = await call_next(request)

        if not request.cookies.get(COOKIE_NAME):
            token = generate_token()
            response.set_cookie(
                COOKIE_NAME,
                token,
                httponly=False,
                samesite="lax",
                secure=use_secure_cookies(request, self.settings),
                max_age=86400 * 30,
            )

        return response


def require_user(request: Request):
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user


def get_principal_id(request: Request) -> str:
    return getattr(request.state, "principal_id", "user:local")
