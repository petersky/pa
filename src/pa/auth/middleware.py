"""Auth middleware and dependencies."""

from __future__ import annotations

import hmac
import re
from urllib.parse import urlsplit

from fastapi import HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, Response

from pa.auth.cookies import use_secure_cookies
from pa.auth.csrf import COOKIE_NAME, generate_token, inspect_token, validate_request
from pa.auth.sessions import SessionManager
from pa.auth.users import UserDirectory
from pa.config import Settings

PUBLIC_PATHS = {
    "/api/health",
    "/api/auth/login",
    "/api/fleet/join",
    "/api/pr-supervisor/webhook/github",
    "/favicon.ico",
    "/api/intake/webhooks/telegram",
    "/api/intake/webhooks/discord",
    "/login",
}

SYNC_PATHS = {
    "/api/sync/have",
    "/api/sync/get",
    "/api/sync/push",
    "/api/sync/relay",
    "/api/sync/refs",
    "/api/fleet/dispatch/materialize",
    "/api/fleet/dispatch/",
}

# Fleet sync credentials are accepted only for the peer operations required by
# native updates. Other API routes continue to require a user session/CLI token.
FLEET_INSTANCE_ROUTES = {
    ("GET", "/api/status"),
    ("GET", "/api/agent/quiesce"),
    ("POST", "/api/agent/quiesce"),
    ("GET", "/api/fleet/peer-update-check"),
    ("POST", "/api/fleet/peer-update"),
    ("POST", "/api/cards/repair-legacy-history"),
    ("GET", "/api/fleet/dispatch-jobs"),
    ("GET", "/api/repositories"),
    ("POST", "/api/repositories/reconcile"),
    ("GET", "/api/fleet/membership"),
    ("POST", "/api/fleet/membership/apply"),
    ("POST", "/api/fleet/credentials/apply"),
    ("POST", "/api/fleet/credentials/revoke"),
}

CSRF_EXEMPT_PATHS = {
    "/api/fleet/join",
    "/api/auth/login",
    "/api/pr-supervisor/webhook/github",
    "/api/intake/webhooks/telegram",
    "/api/intake/webhooks/discord",
    "/login",
}


def _is_public(path: str) -> bool:
    return (
        path in PUBLIC_PATHS
        or path.startswith("/static/")
        or (path.startswith("/partials/") and path.endswith("/public"))
    )


def _is_sync_path(path: str) -> bool:
    return path in SYNC_PATHS or path.startswith("/api/fleet/dispatch/")


def _is_fleet_instance_route(request: Request) -> bool:
    if request.method == "POST" and re.fullmatch(
        r"/api/fleet/dispatch-jobs/[A-Za-z0-9-]{1,80}/assigned-service/"
        r"(?:goal|dispatch|proposals|evidence|audit|progress)",
        request.url.path,
    ):
        return True
    if request.url.path.startswith("/api/telemetry/") and request.method in {
        "GET",
        "POST",
    }:
        return True
    if (request.method, request.url.path) in FLEET_INSTANCE_ROUTES:
        return True
    if request.url.path.startswith("/api/pr-supervisor/") and request.url.path != (
        "/api/pr-supervisor/webhook/github"
    ):
        return True
    if request.method in {"GET", "POST"} and re.fullmatch(
        r"/api/fleet/(?:instances/[A-Za-z0-9-]{1,80}/)?dispatch-jobs/"
        r"[A-Za-z0-9-]{1,80}(?:/(?:retry|cancel|prompt))?",
        request.url.path,
    ):
        return True
    if request.method == "POST" and request.url.path == "/api/fleet/dispatch":
        return True
    if request.method == "POST" and re.fullmatch(
        r"/api/fleet/instances/[A-Za-z0-9-]{1,80}/agent/start", request.url.path
    ):
        return True
    if request.method == "POST" and re.fullmatch(
        r"/api/notifications/[A-Za-z0-9-]{1,80}/respond", request.url.path
    ):
        return True
    if request.method == "POST" and re.fullmatch(
        r"/api/agent/sessions/[A-Za-z0-9-]{1,80}/prompt", request.url.path
    ):
        return True
    if request.method == "GET" and re.fullmatch(
        r"/api/fleet/peer-update/[A-Za-z0-9-]{1,80}", request.url.path
    ):
        return True
    # Fleet provider operations are target-local and already proxied with the
    # shared instance credential. Keep the path character set deliberately narrow.
    return request.method in {"GET", "POST"} and bool(
        re.fullmatch(
            r"/api/agent/providers(?:/[A-Za-z0-9_-]{1,80}(?:/(?:install|update|configure|probe|codex-cli/install|login-jobs(?:/[A-Za-z0-9-]{1,80}(?:/(?:events|cancel))?)?))?)?",
            request.url.path,
        )
    )


def _is_provider_progress_route(request: Request) -> bool:
    return request.method == "POST" and bool(
        re.fullmatch(
            r"/api/goals/[A-Za-z0-9-]{1,80}/providers/progress",
            request.url.path,
        )
    )


def _is_assigned_service_route(request: Request) -> bool:
    return request.method == "POST" and request.url.path in {
        "/api/goal-assigned-service/proposals",
        "/api/goal-assigned-service/evidence",
        "/api/goal-assigned-service/audit",
        "/api/goal-assigned-service/progress",
    }


def _is_assigned_session_route(request: Request) -> bool:
    return (request.method, request.url.path) in {
        ("GET", "/api/goal-assigned-session/goal"),
        ("GET", "/api/goal-assigned-session/dispatch"),
        ("POST", "/api/goal-assigned-session/proposals"),
        ("POST", "/api/goal-assigned-session/evidence"),
        ("POST", "/api/goal-assigned-session/audit"),
        ("POST", "/api/goal-assigned-session/progress"),
    }


def _is_goal_run_credential_route(request: Request) -> bool:
    return (
        _is_provider_progress_route(request)
        or _is_assigned_service_route(request)
    )


def _sync_auth_required(settings: Settings) -> bool:
    """Peer sync endpoints require a bearer when a sync token (or auth_required) is set."""
    return bool(settings.sync_token) or settings.auth_required


def _needs_csrf(request: Request) -> bool:
    if request.method not in ("POST", "PUT", "PATCH", "DELETE"):
        return False
    path = request.url.path
    if _is_public(path) or path in CSRF_EXEMPT_PATHS:
        return False
    if _is_goal_run_credential_route(request) or _is_assigned_session_route(request):
        return False
    return not (
        path.startswith("/api/")
        and request.headers.get("authorization", "").startswith("Bearer ")
    )


def _error(code: str, message: str, status: int) -> JSONResponse:
    return JSONResponse(
        {"detail": {"code": code, "message": message}}, status_code=status
    )


def _origin_allowed(request: Request, settings: Settings) -> bool:
    origin = request.headers.get("origin", "").strip()
    if not origin:
        return True
    parsed = urlsplit(origin)
    if not parsed.scheme or not parsed.netloc:
        return False
    allowed = {f"{request.url.scheme}://{request.url.netloc}".lower()}
    if settings.instance_url:
        advertised = urlsplit(settings.instance_url)
        if advertised.scheme and advertised.netloc:
            allowed.add(f"{advertised.scheme}://{advertised.netloc}".lower())
    return f"{parsed.scheme}://{parsed.netloc}".lower() in allowed


class AuthMiddleware(BaseHTTPMiddleware):
    def __init__(
        self, app, settings: Settings, users: UserDirectory, sessions: SessionManager
    ):
        super().__init__(app)
        self.settings = settings
        self.users = users
        self.sessions = sessions

    async def dispatch(self, request: Request, call_next) -> Response:
        request.state.principal_id = None
        request.state.user = None
        request.state.user_authenticated = False
        request.state.instance_authenticated = False
        request.state.authenticated_instance_id = None
        request.state.provider_run_credential = None
        request.state.assigned_session_capability = None
        cookie_csrf = request.cookies.get(COOKIE_NAME)
        csrf_status = inspect_token(cookie_csrf, self.settings.session_secret)
        rotate_csrf = csrf_status != "valid"
        request.state.csrf_token = (
            generate_token(self.settings.session_secret) if rotate_csrf else cookie_csrf
        )

        path = request.url.path
        is_public = _is_public(path)
        is_fleet_instance_route = _is_fleet_instance_route(request)

        auth_header = request.headers.get("authorization", "")
        provider_run_credential_supplied = auth_header.startswith("GoalRun ")
        if provider_run_credential_supplied:
            request.state.provider_run_credential = auth_header[8:].strip()
        assigned_session_capability_supplied = auth_header.startswith("GoalSession ")
        if assigned_session_capability_supplied:
            request.state.assigned_session_capability = auth_header[12:].strip()
        bearer_supplied = auth_header.startswith("Bearer ")
        bearer_valid = False
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
            accepted = [
                candidate
                for candidate in [
                    self.settings.sync_token,
                    *self.settings.sync_token_previous,
                ]
                if candidate
            ]
            if any(hmac.compare_digest(token, candidate) for candidate in accepted):
                request.state.instance_authenticated = True
                bearer_valid = True
            else:
                user = self.users.get_by_cli_token(token)
                if user:
                    request.state.user = user
                    request.state.principal_id = f"user:{user.id}"
                    request.state.user_authenticated = True
                    bearer_valid = True

        session_token = request.cookies.get(self.sessions.COOKIE_NAME)
        session_status = "missing"
        if session_token and not request.state.principal_id:
            uid, session_status = self.sessions.inspect_token(session_token)
            if uid:
                user = self.users.get(uid)
                if user:
                    request.state.user = user
                    request.state.principal_id = f"user:{user.id}"
                    request.state.user_authenticated = True

        if (
            _sync_auth_required(self.settings)
            and _is_sync_path(path)
            and not is_public
            and not request.state.instance_authenticated
        ):
            return _error(
                "missing_authentication",
                "Fleet instance authentication is missing or invalid.",
                401,
            )

        if not request.state.principal_id:
            # UI/API user login is controlled by auth_required alone — not by sync_token.
            needs_user_auth = (
                self.settings.auth_required
                and path.startswith("/api/")
                and not is_public
                and not _is_sync_path(path)
                and not (
                    _is_goal_run_credential_route(request)
                    and provider_run_credential_supplied
                )
                and not (
                    _is_assigned_session_route(request)
                    and assigned_session_capability_supplied
                )
                and not (
                    is_fleet_instance_route and request.state.instance_authenticated
                )
            )
            if needs_user_auth:
                if request.state.instance_authenticated:
                    return _error(
                        "insufficient_authorization",
                        "Fleet instance credentials do not authorize this user operation.",
                        403,
                    )
                if session_status == "expired":
                    return _error(
                        "expired_session",
                        "The authenticated browser session has expired; sign in again.",
                        401,
                    )
                if bearer_supplied and not bearer_valid:
                    return _error(
                        "invalid_authentication",
                        "The supplied bearer credential is invalid.",
                        401,
                    )
                return _error(
                    "missing_authentication", "Authentication is required.", 401
                )
            default = self.users.ensure_default_user()
            request.state.user = default
            request.state.principal_id = f"user:{default.id}"

        if _needs_csrf(request):
            if not _origin_allowed(request, self.settings):
                return _error(
                    "invalid_origin",
                    "The browser request Origin is not this PA instance or its advertised URL.",
                    403,
                )
            csrf = validate_request(request, self.settings.session_secret)
            if not csrf.ok:
                messages = {
                    "csrf_missing": "The CSRF cookie or matching request header is missing.",
                    "csrf_mismatch": "The CSRF header does not match the CSRF cookie.",
                    "csrf_invalid": "The CSRF token is invalid.",
                    "csrf_expired": "The CSRF token has expired; use the rotated cookie and retry safely.",
                }
                response = _error(csrf.code, messages[csrf.code], 403)
                if csrf.code in {"csrf_expired", "csrf_invalid"}:
                    response.set_cookie(
                        COOKIE_NAME,
                        request.state.csrf_token,
                        httponly=False,
                        samesite="lax",
                        secure=use_secure_cookies(request, self.settings),
                        max_age=86400 * 30,
                    )
                return response

        response = await call_next(request)

        if rotate_csrf:
            response.set_cookie(
                COOKIE_NAME,
                request.state.csrf_token,
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
