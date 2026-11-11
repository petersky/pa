"""Persistent instance configuration (config.json)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from pa.core.operation_budget import default_operation_budgets, validate_operation_budgets
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
    pr_supervisor_reconcile_budget_seconds: float = Field(default=5.0, ge=0.5, le=60)
    pr_supervisor_orphan_card_max_attempts: int = Field(default=8, ge=1, le=100)
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
    # A pre-start capacity reservation older than one hour is renewed while the
    # dispatch is queued or still progressing between stages; a stage hung for a
    # full hour is failed. True restores the old behaviour: fail every
    # reservation that is not running after one hour.
    dispatch_capacity_reservation_hard_expiry: bool = False
    # Re-place a lost remote dispatch automatically (False = leave it failed for manual retry).
    dispatch_reoffer_lost_remote: bool = True
    dispatch_lost_remote_seconds: int = Field(default=21600, ge=0, le=90 * 24 * 3600)
    # Silence before an authority asks the target about a running remote dispatch (0 = never; old behaviour).
    dispatch_stale_running_seconds: int = Field(default=1800, ge=0, le=30 * 24 * 3600)
    # How long completion reconciliation waits on a session that stays unavailable
    # before delivering the completion without it (0 = wait forever; old behaviour).
    reconciliation_session_unavailable_seconds: int = Field(default=3600, ge=0, le=30 * 24 * 3600)
    # Dispatch lease: a running remote dispatch whose target negotiated the lease is
    # live while its lease is fresh; progress, heartbeats and a confirmed probe renew it
    # (0 = grant no leases; older stale-running behaviour only).
    dispatch_lease_ttl_seconds: int = Field(default=300, ge=0, le=7 * 24 * 3600)
    # Extra time an expired lease is given when the target cannot even answer a status
    # probe, before the dispatch is failed recoverably and re-offered.
    dispatch_lease_grace_seconds: int = Field(default=300, ge=0, le=7 * 24 * 3600)
    # Cap of the progress-delivery backoff (old behaviour: flat 3 s).
    progress_delivery_retry_max_seconds: int = Field(default=300, ge=1, le=86400)
    # Retry window for 404/401/unknown_dispatch (0 = permanent after one try, as before).
    completion_unknown_dispatch_grace_seconds: int = Field(default=3600, ge=0, le=30 * 24 * 3600)
    completion_delivery_notify_after_attempts: int = Field(default=12, ge=0, le=1_000_000)
    # 0 retries forever; 12 restores the old give-up-after-12 behaviour.
    completion_delivery_max_attempts: int = Field(default=0, ge=0, le=1_000_000)
    # Cap of the completion-delivery backoff (it retries indefinitely; old cap 300).
    completion_delivery_retry_max_seconds: int = Field(default=600, ge=5, le=86400)
    # Worker re-executions allowed for one dispatch before it fails recoverably;
    # 0 restores the old unlimited behaviour.
    dispatch_max_stage_attempts: int = Field(default=5, ge=0, le=100)
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
    telegram_push_enabled: bool = False
    telegram_owner_chat_id: str = ""
    telegram_owner_user_id: str = ""
    telegram_owner_principal: str = "user:local"
    telegram_push_min_priority: Literal["low", "normal", "high", "urgent"] = "high"
    telegram_renotify_hours: float = Field(default=4.0, ge=0, le=720)
    telegram_renotify_max: int = Field(default=3, ge=0, le=20)
    telegram_push_rate_limit_per_hour: int = Field(default=20, ge=1, le=1000)
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
    agent_reclaim_dead_session_leases: bool = True
    agent_session_idle_retention_hours: float = Field(default=24.0, ge=0.01, le=8760)
    agent_session_sweep_seconds: float = Field(default=30.0, ge=1.0, le=3600)
    # --- Turn liveness and provider-error resilience -------------------------
    # A turn may run for as long as it needs. Only INACTIVITY (no real ACP
    # output: message/thought chunks, tool calls, plans, usage, permission
    # requests) is bounded. Time spent waiting on a pending human permission or
    # elicitation never counts. 0 disables the inactivity deadline.
    agent_turn_inactivity_timeout_seconds: float = Field(default=1800.0, ge=0, le=604800)
    # After the inactivity deadline PA sends session/cancel and waits this long
    # for the provider to end the turn before killing the provider process.
    agent_turn_cancel_grace_seconds: float = Field(default=60.0, ge=0, le=3600)
    # Bound on ACP initialize / session/new / session/load. 0 disables.
    agent_connect_timeout_seconds: float = Field(default=180.0, ge=0, le=3600)
    # A provider error that retrying can cure (rate limit, quota, overload,
    # transient 5xx) re-drains the queue after an exponentially growing delay.
    agent_retry_base_seconds: float = Field(default=30.0, ge=1, le=3600)
    agent_retry_cap_seconds: float = Field(default=900.0, ge=1, le=86400)
    # Attempt cap for retryable errors other than rate limits / quota (those keep
    # waiting with no cap). After the cap the owner is notified.
    agent_retry_max_attempts: int = Field(default=5, ge=1, le=100)
    # Notify the owner once when a rate limit / quota wait outlasts this. 0 = never.
    agent_rate_limit_notify_after_seconds: float = Field(default=1800.0, ge=0, le=604800)
    # The "Agent active." progress heartbeat only counts while the provider
    # produced real output within this window. 0 keeps the heartbeat unconditional.
    agent_heartbeat_activity_window_seconds: float = Field(default=300.0, ge=0, le=86400)
    maintenance_interval_seconds: int = Field(
        default=24 * 60 * 60, ge=60, le=7 * 24 * 60 * 60
    )
    terminal_output_char_limit: int = Field(default=1000, ge=100, le=100000)
    chat_shell_access: Literal["disabled", "signed_in", "anyone"] = "anyone"
    chat_shell_output_to_agent: bool = True
    chat_shell_timeout_seconds: int = Field(default=60, ge=1, le=3600)
    chat_shell_output_char_limit: int = Field(default=20000, ge=1000, le=1000000)
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
    post_turn_max_automatic_followups: int = Field(default=25, ge=0, le=1000)
    post_turn_evaluation_timeout_seconds: float = Field(default=60.0, gt=0, le=600)
    post_turn_retry_seconds: float = Field(default=15.0, gt=0, le=3600)
    post_turn_escalation_threshold: int = Field(default=2, ge=1, le=10)
    post_turn_worker_enabled: bool = True
    post_turn_downgrade_feedback_max: int = Field(default=2, ge=0, le=20)
    completion_missing_disposition_escalates: bool = True
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
    blocking_operation_budgets: dict[str, dict[str, float]] = Field(default_factory=default_operation_budgets)

    @field_validator("blocking_operation_budgets")
    @classmethod
    def _validate_operation_budgets(cls, value):
        return validate_operation_budgets(value)

    blocking_slow_call_seconds: float = Field(default=0.5, gt=0, le=60)
    event_loop_probe_interval: float = Field(default=0.1, gt=0, le=10)
    workshop_snapshot_ttl_seconds: float = Field(default=1.5, ge=0, le=30)
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
    workshop_default_view: Literal["floor", "list"] = "floor"
    workshop_floor_chat_agents: bool = True
    workshop_floor_motion: Literal["auto", "off"] = "auto"
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
    completion_auto_verify_stalled_seconds: int = Field(default=24 * 60 * 60, ge=0, le=365 * 24 * 60 * 60)
    # Durable goals.
    # Seconds without real progress (heartbeat, checkpoint, queue movement) before
    # a goal's in-flight work package is reported as drifting / stalled and blocked.
    # 0 restores the previous behaviour, which counted 30 s supervision cycles
    # (about 90 s / 180 s).
    goal_drifting_after_seconds: int = Field(default=2 * 60 * 60, ge=0, le=30 * 24 * 60 * 60)
    goal_stalled_after_seconds: int = Field(default=8 * 60 * 60, ge=0, le=30 * 24 * 60 * 60)
    # Let the signed-in owner record the completion audit on a single-user instance
    # (never when the audit would be by the executing service).
    goal_owner_may_audit: bool = False
    # `active` is a deliberate milestone, not a default one: a declaration only keeps it when it is
    # explicitly selected (CompletionRequirement.include_active). True restores the old behaviour.
    completion_active_milestone_default: bool = False
    # relaxed: a verifier is independent when it is a different session and a different service
    # principal. strict: it must also run on a different instance and provider (the old rule).
    completion_verifier_independence: Literal["relaxed", "strict"] = "relaxed"
    # How many times a failed verification is fed back to the executor before PA stops and tells
    # the owner. 0 restores the old behaviour (a failed verification is silently terminal).
    completion_verifier_reengage_max: int = Field(default=1, ge=0, le=10)
    # While a verification already ran for this requirement revision, wait until the newest integrated
    # change is this old before verifying again, so a burst of merges is verified once, at its end.
    completion_verification_quiet_seconds: int = Field(default=120, ge=0, le=24 * 60 * 60)
    # Notify the owner when a card waits for their acceptance. False disables the notification.
    completion_notify_acceptance_needed: bool = True
    # Notify the owner when verification stays pending this long with nothing in flight (0 disables).
    completion_verification_pending_notify_seconds: int = Field(default=2 * 60 * 60, ge=0, le=365 * 24 * 60 * 60)
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
