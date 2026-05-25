from pathlib import Path
from uuid import uuid4

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    update_channel: str = "github"
    update_repo: str = "petersky/pa"
    install_method: str = "uv-tool"

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

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.knowledge_dir.mkdir(parents=True, exist_ok=True)


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
        _settings.ensure_dirs()
    return _settings


def reset_settings() -> None:
    global _settings
    _settings = None
