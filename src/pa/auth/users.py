"""Local user directory (T1)."""

from __future__ import annotations

import hashlib
import json
import secrets
from pathlib import Path

from pydantic import BaseModel, Field

from pa.core.io import atomic_write_json


class UserRecord(BaseModel):
    id: str
    username: str
    password_hash: str = ""
    display_name: str = ""
    role: str = "editor"
    agent_env: dict[str, str] = Field(default_factory=dict)
    cli_token: str = ""


def _hash_password(password: str, salt: str) -> str:
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode(),
        salt.encode(),
        100_000,
    )
    return f"{salt}${digest.hex()}"


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    return _hash_password(password, salt)


def verify_password(password: str, stored: str) -> bool:
    if "$" not in stored:
        return False
    salt, _ = stored.split("$", 1)
    return secrets.compare_digest(_hash_password(password, salt), stored)


class UserDirectory:
    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir / "users.json"
        self._users: dict[str, UserRecord] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text())
            for item in data.get("users", []):
                user = UserRecord.model_validate(item)
                self._users[user.id] = user
        except (json.JSONDecodeError, ValueError):
            pass

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"users": [u.model_dump() for u in self._users.values()]}
        atomic_write_json(self.path, payload)

    def ensure_default_user(self) -> UserRecord:
        if self._users:
            return next(iter(self._users.values()))
        user = UserRecord(
            id="local",
            username="local",
            display_name="Local User",
            role="admin",
            cli_token=secrets.token_urlsafe(32),
        )
        self._users[user.id] = user
        self._save()
        return user

    def create_user(
        self,
        username: str,
        password: str,
        *,
        display_name: str = "",
        role: str = "editor",
    ) -> UserRecord:
        if any(u.username == username for u in self._users.values()):
            raise ValueError(f"Username already exists: {username}")
        user = UserRecord(
            id=secrets.token_hex(8),
            username=username,
            password_hash=hash_password(password),
            display_name=display_name or username,
            role=role,
            cli_token=secrets.token_urlsafe(32),
        )
        self._users[user.id] = user
        self._save()
        return user

    def authenticate(self, username: str, password: str) -> UserRecord | None:
        for user in self._users.values():
            if user.username == username and verify_password(password, user.password_hash):
                return user
        return None

    def get(self, user_id: str) -> UserRecord | None:
        return self._users.get(user_id)

    def get_by_cli_token(self, token: str) -> UserRecord | None:
        for user in self._users.values():
            if user.cli_token and secrets.compare_digest(user.cli_token, token):
                return user
        return None

    def list_users(self) -> list[UserRecord]:
        return list(self._users.values())

    def update_agent_env(self, user_id: str, env: dict[str, str]) -> UserRecord | None:
        user = self._users.get(user_id)
        if not user:
            return None
        user.agent_env = env
        self._save()
        return user
