from pathlib import Path
from uuid import uuid4

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from pa.domain.instance_config import load_instance_config, merge_config_into_settings


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
    peers: list[str] = Field(default_factory=list)

    # Fleet / realm
    fleet_id: str = Field(default_factory=lambda: str(uuid4()))
    fleet_owner: str = "local"
    subscribed_realms: list[str] = Field(default_factory=lambda: ["default"])
    zone: str = "default"
    capabilities: list[str] = Field(default_factory=list)
    relay_enabled: bool = False

    # Auth (T1)
    sync_token: str = ""
    auth_required: bool = False
    session_secret: str = Field(default_factory=lambda: str(uuid4()))

    # OIDC hooks (T2+)
    oidc_issuer: str = ""
    oidc_client_id: str = ""
    oidc_client_secret: str = ""

    # Primary ACP agent (Cursor: `agent acp`)
    agent_command: str = "agent"
    agent_args: list[str] = Field(default_factory=lambda: ["acp"])
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

    @field_validator("peers", "subscribed_realms", "capabilities", mode="before")
    @classmethod
    def _split_comma_list(cls, value: object) -> object:
        if isinstance(value, str):
            return [part.strip() for part in value.split(",") if part.strip()]
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
        data_dir = default_data_dir()
        kwargs: dict = {}
        merge_config_into_settings(data_dir, kwargs)
        _settings = Settings(**kwargs)
        _settings.ensure_dirs()
        if _settings.sync_token:
            _settings.auth_required = True
    return _settings


def reset_settings() -> None:
    global _settings
    _settings = None
