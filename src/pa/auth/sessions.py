"""Session management for web and CLI."""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import time
from typing import Any

from pa.auth.users import UserRecord


def _sign(payload: str, secret: str) -> str:
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


class SessionManager:
    COOKIE_NAME = "pa_session"

    def __init__(self, secret: str) -> None:
        self.secret = secret

    def create_token(self, user: UserRecord, *, ttl_seconds: int = 86400 * 7) -> str:
        payload = {
            "uid": user.id,
            "exp": int(time.time()) + ttl_seconds,
            "nonce": secrets.token_hex(8),
        }
        body = json.dumps(payload, separators=(",", ":"))
        sig = _sign(body, self.secret)
        return f"{body}.{sig}"

    def inspect_token(self, token: str) -> tuple[str | None, str]:
        if "." not in token:
            return None, "invalid"
        body, sig = token.rsplit(".", 1)
        if not hmac.compare_digest(_sign(body, self.secret), sig):
            return None, "invalid"
        try:
            payload: dict[str, Any] = json.loads(body)
        except json.JSONDecodeError:
            return None, "invalid"
        if payload.get("exp", 0) < time.time():
            return None, "expired"
        uid = str(payload.get("uid", ""))
        return (uid, "valid") if uid else (None, "invalid")

    def verify_token(self, token: str) -> str | None:
        uid, status = self.inspect_token(token)
        return uid if status == "valid" else None
