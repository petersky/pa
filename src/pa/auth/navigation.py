"""Browser account presentation and strictly local authentication returns."""

from urllib.parse import unquote, urlencode, urlsplit

from starlette.requests import Request


def local_return_path(value: str) -> str:
    """Accept a root-relative location, rejecting browser normalization tricks."""
    candidate = value
    for _ in range(8):
        if (
            not candidate.startswith("/")
            or candidate.startswith("//")
            or "\\" in candidate
            or any(ord(char) < 32 or ord(char) == 127 for char in candidate)
        ):
            return "/"
        try:
            parts = urlsplit(candidate)
        except ValueError:
            return "/"
        if parts.scheme or parts.netloc or any(part in {".", ".."} for part in parts.path.split("/")):
            return "/"
        decoded = unquote(candidate)
        if decoded == candidate:
            return value
        candidate = decoded
    return "/"


def sign_in_url(destination: str) -> str:
    return "/login?" + urlencode({"next": local_return_path(destination)})


def browser_signed_in(request: Request) -> bool:
    return bool(
        getattr(request.state, "user_authenticated", False)
        and getattr(request.state, "authentication_method", None) == "browser_session"
        and not getattr(request.state, "instance_authenticated", False)
        and not any(request.headers.get(name) for name in (
            "Authorization", "X-PA-MCP-Instance-ID", "X-PA-Completion-Producer",
            "X-PA-Assigned-Session-ID", "X-PA-Assigned-Dispatch-ID",
        ))
    )
