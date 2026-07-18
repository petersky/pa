from pathlib import Path
from typing import Annotated
from uuid import uuid4

import json

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from pa.domain.instance_config import (
    config_path,
    ensure_session_secret,
    merge_config_into_settings,
)


def default_data_dir() -> Path:
    return Path.home() / ".pa"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PA_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    instance_id: str = Field(default_factory=lambda: str(uuid4()))
    instance_name: str = "local"
    data_dir: Path = Field(default_factory=default_data_dir)
    host: str = "127.0.0.1"
    port: int = 8080
    peers: Annotated[list[str], NoDecode] = Field(default_factory=list)

    # Fleet / realm
    fleet_id: str = Field(default_factory=lambda: str(uuid4()))
    fleet_owner: str = "local"
    fleet_owner_url: str = ""
    instance_url: str = ""
    subscribed_realms: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["default"]
    )
    zone: str = "default"
    capabilities: Annotated[list[str], NoDecode] = Field(default_factory=list)
    relay_enabled: bool = False

    # Auth (T1)
    sync_token: str = ""
    auth_required: bool = False
    secure_cookies: bool = False
    session_secret: str = Field(default_factory=lambda: str(uuid4()))

    # OIDC hooks (T2+)
    oidc_issuer: str = ""
    oidc_client_id: str = ""
    oidc_client_secret: str = ""

    # Primary ACP agent provider (`cursor` | `codex` | future ids)
    agent_provider: str = "cursor"
    # Optional spawn overrides (None → use selected provider defaults)
    agent_command: str | None = None
    agent_args: Annotated[list[str] | None, NoDecode] = None
    agent_enabled: bool = True

    # Developer / debug
    debug: bool = False
    dev_tools: bool = False
    log_level: str = "INFO"

    # UI defaults (user preferences file overrides appearance at runtime)
    default_theme_id: str = "pa"

    # Install / update
    release_track: str = Field(
        default="release",
        validation_alias=AliasChoices("release_track", "update_channel"),
    )
    update_repo: str = "petersky/pa"
    install_method: str = "uv-tool"

    @field_validator(
        "peers", "subscribed_realms", "capabilities", "agent_args", mode="before"
    )
    @classmethod
    def _parse_env_list(cls, value: object) -> object:
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return []
            if text.startswith("["):
                try:
                    parsed = json.loads(text)
                    if isinstance(parsed, list):
                        return parsed
                except json.JSONDecodeError:
                    pass
            return [part.strip() for part in text.split(",") if part.strip()]
        return value

    @model_validator(mode="after")
    def _normalize_legacy_settings(self) -> Settings:
        track = self.release_track.strip().lower()
        if track in ("github", "stable"):
            self.release_track = "release"
        elif track == "main":
            self.release_track = "dev"
        return self

    @property
    def update_channel(self) -> str:
        """Backward-compatible alias for release_track."""
        return self.release_track

    @property
    def primary_realm(self) -> str:
        return self.subscribed_realms[0] if self.subscribed_realms else "default"

    @model_validator(mode="after")
    def _apply_debug_defaults(self) -> Settings:
        if self.debug and not self.dev_tools:
            self.dev_tools = True
        return self

    @property
    def db_path(self) -> Path:
        return self.data_dir / "pa.db"

    @property
    def knowledge_dir(self) -> Path:
        return self.data_dir / "knowledge"

    @property
    def objects_dir(self) -> Path:
        return self.data_dir / "objects"

    @property
    def users_dir(self) -> Path:
        return self.data_dir / "users"

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.knowledge_dir.mkdir(parents=True, exist_ok=True)
        self.objects_dir.mkdir(parents=True, exist_ok=True)
        self.users_dir.mkdir(parents=True, exist_ok=True)


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        # Resolve settings sources first so instance config is loaded from the
        # directory selected by PA_DATA_DIR (including via .env), not ~/.pa.
        data_dir = Settings().data_dir
        kwargs: dict = {"data_dir": data_dir}
        merge_config_into_settings(data_dir, kwargs)
        if config_path(data_dir).exists():
            kwargs["session_secret"] = ensure_session_secret(data_dir)
        _settings = Settings(**kwargs)
        _settings.ensure_dirs()
        # sync_token protects /api/sync/* peer traffic; it must not force UI login.
        # Set PA_AUTH_REQUIRED=true explicitly when browser/API user auth is desired.
    return _settings


def reset_settings() -> None:
    global _settings
    _settings = None
