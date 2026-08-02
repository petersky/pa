import json
import os
from pathlib import Path
from typing import Annotated, Literal
from uuid import uuid4

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from pa.domain.instance_config import (
    config_path,
    ensure_session_secret,
    merge_config_into_settings,
)
from pa.fleet.capacity import MAX_DISPATCH_CAPACITY, DispatchCapacity


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
    # Mutable agent checkouts must never live under PA_DATA_DIR.  None is
    # resolved relative to data_dir so isolated/test instances remain isolated.
    workspace_root: Path | None = None
    host: str = "127.0.0.1"
    # Explicit web binds; empty preserves the legacy single host bind.
    # Entries are HOST or HOST:PORT; bracket IPv6 when specifying a port.
    web_listeners: Annotated[list[str], NoDecode] = Field(default_factory=list)
    port: int = 8080
    peers: Annotated[list[str], NoDecode] = Field(default_factory=list)

    # Fleet / realm
    fleet_id: str = Field(default_factory=lambda: str(uuid4()))
    fleet_owner: str = "local"
    fleet_owner_url: str = ""
    # Single fenced PR-supervisor lease authority. Empty follows fleet_owner_url.
    pr_supervisor_authority_url: str = ""
    instance_url: str = ""
    subscribed_realms: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["default"]
    )
    zone: str = "default"
    capabilities: Annotated[list[str], NoDecode] = Field(default_factory=list)
    # Global execution slots. Four intentionally preserves the historical
    # ceiling and is conservative when PA cannot infer host/provider limits.
    dispatch_capacity: int | None = Field(default=None, ge=1, le=MAX_DISPATCH_CAPACITY)
    dispatch_provider_capacities: dict[str, DispatchCapacity] = Field(
        default_factory=dict
    )
    relay_enabled: bool = False

    # Auth (T1)
    sync_token: str = ""
    sync_token_previous: Annotated[list[str], NoDecode] = Field(default_factory=list)
    auth_required: bool = False
    secure_cookies: bool = False
    session_secret: str = Field(default_factory=lambda: str(uuid4()))

    # OIDC hooks (T2+)
    oidc_issuer: str = ""
    oidc_client_id: str = ""
    oidc_client_secret: str = ""

    # Primary ACP agent provider (`cursor` | `codex` | `openinterpreter`)
    agent_provider: str = "cursor"
    # Optional spawn overrides (None → use selected provider defaults)
    agent_command: str | None = None
    agent_args: Annotated[list[str] | None, NoDecode] = None
    agent_enabled: bool = True
    # Provider processes are comparatively expensive. Keep restart recovery
    # bounded even when many sessions have durable unfinished work.
    agent_recovery_concurrency: int = Field(default=2, ge=1, le=16)
    # Idle sessions remain follow-up capable for one day unless a durable
    # terminal workflow proves that they can close sooner.
    agent_session_idle_retention_hours: float = Field(default=24.0, ge=0.01, le=8760)
    agent_session_sweep_seconds: float = Field(default=30.0, ge=1.0, le=3600)
    # Optional ACP final-fact candidates. Disabled by default; when enabled,
    # only policy-marked candidates enter pending review.
    memory_auto_capture_enabled: bool = False

    # Bounded post-turn evaluation and automatic follow-up policy. Evaluators
    # remain read-only; PA validates and executes eligible catalog actions.
    post_turn_evaluator_max_attempts: int = Field(default=2, ge=1, le=5)
    post_turn_max_automatic_followups: int = Field(default=2, ge=0, le=10)
    post_turn_evaluation_timeout_seconds: float = Field(default=60.0, gt=0, le=600)
    post_turn_retry_seconds: float = Field(default=15.0, gt=0, le=3600)
    post_turn_escalation_threshold: int = Field(default=2, ge=1, le=10)

    # Developer / debug
    debug: bool = False
    dev_tools: bool = False
    log_level: str = "INFO"

    # Bounded compatibility executor and responsiveness telemetry.
    blocking_workers: int = Field(default=8, ge=1, le=64)
    blocking_queue_limit: int = Field(default=64, ge=0, le=4096)
    blocking_default_timeout: float = Field(default=30.0, gt=0, le=3600)
    blocking_slow_call_seconds: float = Field(default=0.5, gt=0, le=60)
    event_loop_probe_interval: float = Field(default=0.1, gt=0, le=10)

    # Resource telemetry. Samples are deliberately isolated from pa.db and realm
    # sync; an empty database path resolves to <data_dir>/telemetry.db.
    telemetry_enabled: bool = True
    telemetry_live_interval_seconds: float = Field(default=5.0, ge=1.0, le=300)
    telemetry_persistence_interval_seconds: float = Field(default=30.0, ge=5.0, le=3600)
    telemetry_raw_retention_hours: float = Field(default=168.0, ge=1.0, le=8760)
    telemetry_rollup_retention_hours: float = Field(default=2160.0, ge=1.0, le=43800)
    telemetry_max_database_bytes: int = Field(
        default=512 * 1024 * 1024, ge=16 * 1024 * 1024, le=64 * 1024 * 1024 * 1024
    )
    telemetry_database_path: Path | None = None
    telemetry_per_session_enabled: bool = True
    telemetry_ui_refresh_seconds: float = Field(default=5.0, ge=2.0, le=300)
    telemetry_default_report_range: str = "1h"

    # Verified authoritative metadata backups. A blank destination resolves to
    # a private sibling of data_dir, never beneath the live data directory.
    backup_enabled: bool = True
    backup_interval_seconds: int = Field(
        default=6 * 60 * 60, ge=60, le=31 * 24 * 60 * 60
    )
    backup_retention_count: int = Field(default=8, ge=1, le=10_000)
    backup_retention_max_age_seconds: int | None = Field(
        default=None, ge=60, le=10 * 365 * 24 * 60 * 60
    )
    backup_retention_max_total_bytes: int | None = Field(default=None, ge=1024)
    backup_destination_dir: Path | None = None
    backup_run_on_startup: bool = False
    backup_startup_min_age_seconds: int = Field(
        default=60 * 60, ge=0, le=31 * 24 * 60 * 60
    )
    backup_verification_level: Literal["quick", "full"] = "full"
    backup_compression: bool = True
    backup_io_limit_mib_per_second: float | None = Field(default=None, gt=0, le=10_000)
    backup_concurrency: Literal[1] = 1
    backup_alert_after_failures: int = Field(default=3, ge=1, le=1000)
    backup_jitter_seconds: int = Field(default=300, ge=0, le=60 * 60)

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
        "peers",
        "subscribed_realms",
        "capabilities",
        "agent_args",
        "web_listeners",
        mode="before",
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
        if self.workspace_root is None:
            self.workspace_root = (
                self.data_dir.parent / f"{self.data_dir.name}-workspaces"
            )
        data_dir = self.data_dir.expanduser().resolve()
        workspace_root = self.workspace_root.expanduser().resolve()
        if (
            workspace_root == data_dir
            or data_dir in workspace_root.parents
            or workspace_root in data_dir.parents
        ):
            raise ValueError("workspace_root must be outside data_dir")
        self.workspace_root = workspace_root
        if self.backup_destination_dir is not None:
            self.backup_destination_dir = (
                self.backup_destination_dir.expanduser().resolve()
            )
        normalized_provider_limits: dict[str, int] = {}
        for provider, limit in self.dispatch_provider_capacities.items():
            key = str(provider).strip().lower()
            if not key:
                raise ValueError(
                    "dispatch_provider_capacities provider names cannot be empty"
                )
            if isinstance(limit, bool) or not 1 <= int(limit) <= MAX_DISPATCH_CAPACITY:
                raise ValueError(
                    "dispatch_provider_capacities values must be integers from "
                    f"1 to {MAX_DISPATCH_CAPACITY}"
                )
            normalized_provider_limits[key] = int(limit)
        self.dispatch_provider_capacities = normalized_provider_limits
        if (
            self.telemetry_persistence_interval_seconds
            < self.telemetry_live_interval_seconds
        ):
            raise ValueError(
                "telemetry_persistence_interval_seconds must be greater than or "
                "equal to telemetry_live_interval_seconds"
            )
        if self.telemetry_rollup_retention_hours < self.telemetry_raw_retention_hours:
            raise ValueError(
                "telemetry_rollup_retention_hours must be greater than or equal "
                "to telemetry_raw_retention_hours"
            )
        if self.telemetry_default_report_range not in {
            "15m",
            "1h",
            "6h",
            "24h",
            "7d",
            "30d",
        }:
            raise ValueError("telemetry_default_report_range is not supported")
        if self.telemetry_database_path is None:
            self.telemetry_database_path = data_dir / "telemetry.db"
        else:
            self.telemetry_database_path = (
                self.telemetry_database_path.expanduser().resolve()
            )
        protected_files = {
            data_dir / "pa.db",
            data_dir / "pa.db-wal",
            data_dir / "pa.db-shm",
            data_dir / "sync_refs.json",
            data_dir / "sync_refs.lock",
        }
        objects_dir = data_dir / "objects"
        if (
            self.telemetry_database_path in protected_files
            or self.telemetry_database_path == objects_dir
            or objects_dir in self.telemetry_database_path.parents
        ):
            raise ValueError(
                "telemetry_database_path must be outside PA metadata and sync authority"
            )
        if self.telemetry_database_path.exists() and not (
            self.telemetry_database_path.is_file()
        ):
            raise ValueError("telemetry_database_path must name a file")
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
        credential_file = os.environ.get("PA_SYNC_TOKEN_FILE", "").strip()
        if not kwargs.get("sync_token") and credential_file:
            try:
                path = Path(credential_file)
                if path.is_file() and path.stat().st_mode & 0o077 == 0:
                    kwargs["sync_token"] = path.read_text().strip()
            except OSError:
                # Doctor reports an unreadable or unsafe managed credential with
                # path-only evidence; settings resolution must never echo it.
                pass
        _settings = Settings(**kwargs)
        _settings.ensure_dirs()
        # sync_token protects /api/sync/* peer traffic; it must not force UI login.
        # Set PA_AUTH_REQUIRED=true explicitly when browser/API user auth is desired.
    return _settings


def reset_settings() -> None:
    global _settings
    _settings = None
