"""Persistent instance configuration (config.json)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from pa.core.io import atomic_write_json
from pa.fleet.capacity import (
    DEFAULT_DISPATCH_QUEUE_CAPACITY,
    MAX_DISPATCH_CAPACITY,
    MAX_DISPATCH_QUEUE_CAPACITY,
    DispatchCapacity,
    DispatchQueueCapacity,
)


class InstanceConfig(BaseModel):
    # Unknown persisted keys are intentionally retained.  The configuration UI
    # reports them and a scoped edit must never silently erase operator data.
    model_config = ConfigDict(extra="allow")

    instance_id: str = Field(default_factory=lambda: str(uuid4()))
    instance_name: str = "local"
    data_dir: str = ""
    workspace_root: str | None = None
    fleet_id: str = Field(default_factory=lambda: str(uuid4()))
    fleet_owner: str = "local"
    fleet_owner_url: str = ""
    pr_supervisor_authority_url: str = ""
    instance_url: str = ""
    host: str = ""
    web_listeners: list[str] = Field(default_factory=list)
    port: int = Field(default=8080, ge=1, le=65535)
    subscribed_realms: list[str] = Field(default_factory=lambda: ["default"])
    zone: str = "default"
    capabilities: list[str] = Field(default_factory=list)
    dispatch_capacity: int | None = Field(default=None, ge=1, le=MAX_DISPATCH_CAPACITY)
    dispatch_provider_capacities: dict[str, DispatchCapacity] = Field(
        default_factory=dict
    )
    dispatch_queue_capacity: int = Field(
        default=DEFAULT_DISPATCH_QUEUE_CAPACITY,
        ge=0,
        le=MAX_DISPATCH_QUEUE_CAPACITY,
    )
    dispatch_provider_queue_capacities: dict[str, DispatchQueueCapacity] = Field(
        default_factory=dict
    )
    relay_enabled: bool = False
    cloud_endpoint: str = ""
    cloud_token: str = ""
    cloud_timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    cloud_lease_fail_open: bool = True
    cloud_publish_queue_capacity: int = Field(default=1000, ge=1, le=100_000)
    cloud_allow_insecure_http: bool = False
    peers: list[str] = Field(default_factory=list)
    release_track: str = "release"
    sync_token: str = ""
    sync_token_previous: list[str] = Field(default_factory=list)
    auth_required: bool = False
    secure_cookies: bool = False
    session_secret: str = ""
    intake_max_event_bytes: int = Field(
        default=2 * 1024 * 1024, ge=1024, le=25 * 1024 * 1024
    )
    intake_max_artifact_bytes: int = Field(
        default=25 * 1024 * 1024, ge=1024, le=25 * 1024 * 1024
    )
    intake_raw_retention_hours: float = Field(default=168.0, ge=1, le=8760)
    intake_canonical_retention_hours: float = Field(default=2160.0, ge=1, le=43800)
    intake_identity_rate_limit: int = Field(default=30, ge=1, le=10_000)
    intake_conversation_rate_limit: int = Field(default=120, ge=1, le=100_000)
    intake_channel_routes: dict[str, dict] = Field(default_factory=dict)
    telegram_bot_token: str = ""
    telegram_webhook_secret: str = ""
    telegram_webhook_url: str = ""
    telegram_allowed_user_ids: list[str] = Field(default_factory=list)
    telegram_allowed_conversation_ids: list[str] = Field(default_factory=list)
    discord_bot_token: str = ""
    discord_application_public_key: str = ""
    discord_allowed_user_ids: list[str] = Field(default_factory=list)
    discord_allowed_conversation_ids: list[str] = Field(default_factory=list)
    oidc_issuer: str = ""
    oidc_client_id: str = ""
    oidc_client_secret: str = ""
    agent_provider: str = "cursor"
    agent_command: str | None = None
    agent_args: list[str] | None = None
    agent_enabled: bool = True
    agent_github_token_enabled: bool = False
    agent_recovery_concurrency: int = Field(default=2, ge=1, le=16)
    agent_session_idle_retention_hours: float = Field(default=24.0, ge=0.01, le=8760)
    agent_session_sweep_seconds: float = Field(default=30.0, ge=1.0, le=3600)
    maintenance_interval_seconds: int = Field(
        default=6 * 60 * 60, ge=60, le=7 * 24 * 60 * 60
    )
    transcript_retention_days: int = Field(default=14, ge=1, le=3650)
    mutation_operation_retention_days: int = Field(default=14, ge=1, le=3650)
    memory_auto_capture_enabled: bool = False
    autonomy_context_by_realm: dict[str, dict] = Field(default_factory=dict)
    card_summary_provider: str = "openai"
    card_summary_model: str = "gpt-5-mini"
    card_summary_base_url: str = "https://api.openai.com/v1"
    card_summary_api_key: str = ""
    card_summary_anthropic_api_key: str = ""
    card_summary_minimax_api_key: str = ""
    card_summary_auth_source: Literal["dedicated", "codex"] = "dedicated"
    card_summary_timeout_seconds: float = Field(default=20.0, gt=0, le=120)
    card_summary_max_concurrency: int = Field(default=2, ge=1, le=16)
    card_summary_max_retries: int = Field(default=2, ge=0, le=4)
    card_summary_retry_base_seconds: float = Field(default=15.0, ge=1, le=3600)
    card_summary_retry_max_seconds: float = Field(default=300.0, ge=1, le=86400)
    card_summary_retry_jitter_ratio: float = Field(default=0.2, ge=0, le=1)
    card_summary_worker_interval_seconds: float = Field(default=10.0, ge=1, le=300)
    card_summary_migration_batch: int = Field(default=20, ge=0, le=500)
    post_turn_evaluator_max_attempts: int = Field(default=2, ge=1, le=5)
    post_turn_max_automatic_followups: int = Field(default=2, ge=0, le=10)
    post_turn_evaluation_timeout_seconds: float = Field(default=60.0, gt=0, le=600)
    post_turn_retry_seconds: float = Field(default=15.0, gt=0, le=3600)
    post_turn_escalation_threshold: int = Field(default=2, ge=1, le=10)
    debug: bool = False
    dev_tools: bool = False
    log_level: str = "INFO"
    log_rotation_max_bytes: int = Field(
        default=25 * 1024 * 1024, ge=1024, le=16 * 1024 * 1024 * 1024
    )
    log_rotation_interval_seconds: float = Field(
        default=24 * 60 * 60, ge=1, le=31 * 24 * 60 * 60
    )
    log_retention_count: int = Field(default=7, ge=1, le=10_000)
    log_retention_max_age_seconds: float = Field(
        default=14 * 24 * 60 * 60, ge=1, le=10 * 365 * 24 * 60 * 60
    )
    log_retention_max_total_bytes: int = Field(
        default=256 * 1024 * 1024, ge=1024, le=64 * 1024 * 1024 * 1024
    )
    log_disk_pressure_free_bytes: int = Field(
        default=512 * 1024 * 1024, ge=0, le=64 * 1024 * 1024 * 1024
    )
    blocking_workers: int = Field(default=8, ge=1, le=64)
    blocking_queue_limit: int = Field(default=64, ge=0, le=4096)
    blocking_default_timeout: float = Field(default=30.0, gt=0, le=3600)
    blocking_slow_call_seconds: float = Field(default=0.5, gt=0, le=60)
    event_loop_probe_interval: float = Field(default=0.1, gt=0, le=10)
    telemetry_enabled: bool = True
    telemetry_live_interval_seconds: float = Field(default=5.0, ge=1.0, le=300)
    telemetry_persistence_interval_seconds: float = Field(default=30.0, ge=5.0, le=3600)
    telemetry_raw_retention_hours: float = Field(default=168.0, ge=1.0, le=8760)
    telemetry_rollup_retention_hours: float = Field(default=2160.0, ge=1.0, le=43800)
    telemetry_max_database_bytes: int = Field(
        default=512 * 1024 * 1024,
        ge=16 * 1024 * 1024,
        le=64 * 1024 * 1024 * 1024,
    )
    telemetry_database_path: str | None = None
    telemetry_per_session_enabled: bool = True
    telemetry_ui_refresh_seconds: float = Field(default=5.0, ge=2.0, le=300)
    telemetry_default_report_range: str = "1h"
    default_theme_id: str = "pa"
    update_repo: str = "petersky/pa"
    install_method: str = "uv-tool"
    backup_enabled: bool = True
    backup_interval_seconds: int = Field(
        default=6 * 60 * 60, ge=60, le=31 * 24 * 60 * 60
    )
    backup_retention_count: int = Field(default=8, ge=1, le=10_000)
    backup_retention_max_age_seconds: int | None = Field(
        default=None, ge=60, le=10 * 365 * 24 * 60 * 60
    )
    backup_retention_max_total_bytes: int | None = Field(default=None, ge=1024)
    backup_destination_dir: str | None = None
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
    backup_scrub_interval_seconds: int = Field(
        default=7 * 24 * 60 * 60, ge=60 * 60, le=365 * 24 * 60 * 60
    )


def config_path(data_dir: Path) -> Path:
    return data_dir / "config.json"


def load_instance_config(data_dir: Path) -> InstanceConfig | None:
    path = config_path(data_dir)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        if (
            isinstance(data, dict)
            and "release_track" not in data
            and "update_channel" in data
        ):
            # Accepted legacy key.  The next managed write materializes the
            # canonical field while preserving the alias for audit/reporting.
            data["release_track"] = data["update_channel"]
        return InstanceConfig.model_validate(data)
    except json.JSONDecodeError, ValueError:
        return None


def save_instance_config(data_dir: Path, config: InstanceConfig) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    path = config_path(data_dir)
    # config.json contains the fleet sync token and session secret.
    atomic_write_json(path, config.model_dump(exclude_unset=True), mode=0o600)
    return path


def ensure_session_secret(data_dir: Path) -> str:
    """Return a stable session secret, persisting to config.json if needed."""
    config = load_instance_config(data_dir)
    if config and config.session_secret:
        return config.session_secret
    secret = str(uuid4())
    update_instance_config(data_dir, session_secret=secret)
    return secret


def merge_config_into_settings(data_dir: Path, settings_dict: dict) -> dict:
    """Overlay config.json values onto settings kwargs."""
    loaded = load_instance_config(data_dir)
    if not loaded:
        return settings_dict
    # Only keys explicitly present in the persisted document are configured.
    # This matters as new registry entries are added to older config files.
    mapping = {
        key: getattr(loaded, key)
        for key in loaded.model_fields_set
        if key in loaded.__class__.model_fields and key != "data_dir"
    }
    for key, value in mapping.items():
        # Empty/None values mean "inherit" for nullable settings.  False, zero,
        # empty lists, and empty dictionaries remain valid explicit values.
        if value is None:
            continue
        if (
            key
            in {
                "host",
                "instance_url",
                "fleet_owner_url",
                "telemetry_database_path",
            }
            and value == ""
        ):
            continue
        if key not in settings_dict:
            settings_dict[key] = value
    if loaded.session_secret:
        settings_dict["session_secret"] = loaded.session_secret
    return settings_dict


def update_instance_config(data_dir: Path, **updates: object) -> InstanceConfig:
    """Merge updates into config.json and return the result."""
    config = load_instance_config(data_dir) or InstanceConfig(data_dir=str(data_dir))
    data = config.model_dump()
    for key, value in updates.items():
        if value is not None:
            data[key] = value
    updated = InstanceConfig.model_validate(data)
    updated.__pydantic_fields_set__ = config.model_fields_set | {
        key for key, value in updates.items() if value is not None
    }
    save_instance_config(data_dir, updated)
    return updated
