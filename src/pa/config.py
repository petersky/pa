import json
import os
from pathlib import Path
from typing import Annotated, Literal
from uuid import uuid4

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from pa.core.operation_budget import default_operation_budgets, validate_operation_budgets
from pa.domain.instance_config import (
    config_path,
    ensure_session_secret,
    merge_config_into_settings,
)
from pa.fleet.capacity import (
    DEFAULT_DISPATCH_QUEUE_CAPACITY,
    MAX_DISPATCH_CAPACITY,
    MAX_DISPATCH_QUEUE_CAPACITY,
    DispatchCapacity,
    DispatchQueueCapacity,
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
    # Wall-clock budget for one merged-card reconcile pass inside the PR
    # supervisor loop. The pre-budget behaviour was unbounded (up to 100 watches).
    pr_supervisor_reconcile_budget_seconds: float = Field(default=5.0, ge=0.5, le=60)
    # Missing-card completion attempts before a merged watch is marked orphaned
    # and no longer retried. The pre-limit behaviour retried hourly forever.
    pr_supervisor_orphan_card_max_attempts: int = Field(default=8, ge=1, le=100)
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

    # Optional always-on coordination plane. The endpoint implements PA's
    # provider-neutral cloud protocol; local persistence remains authoritative
    # when this is blank. Tokens are deliberately settings-only and are never
    # serialized into shared state.
    cloud_endpoint: str = ""
    cloud_token: str = ""
    cloud_timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    cloud_lease_fail_open: bool = True
    cloud_publish_queue_capacity: int = Field(default=1000, ge=1, le=100_000)
    cloud_allow_insecure_http: bool = False

    # Auth (T1)
    sync_token: str = ""
    sync_token_previous: Annotated[list[str], NoDecode] = Field(default_factory=list)
    auth_required: bool = False
    secure_cookies: bool = False
    session_secret: str = Field(default_factory=lambda: str(uuid4()))

    # Canonical multichannel intake. External adapters remain disabled until
    # their provider credentials and allowlists or identity links are configured.
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
    telegram_allowed_user_ids: Annotated[list[str], NoDecode] = Field(
        default_factory=list
    )
    telegram_allowed_conversation_ids: Annotated[list[str], NoDecode] = Field(
        default_factory=list
    )
    # Outbound Telegram push for high-priority notifications, and the owner-only
    # reply path that answers them. Off until the owner opts in and names the chat.
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
    discord_allowed_user_ids: Annotated[list[str], NoDecode] = Field(
        default_factory=list
    )
    discord_allowed_conversation_ids: Annotated[list[str], NoDecode] = Field(
        default_factory=list
    )

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
    # Explicitly expose the instance-local PR-supervisor credential to ACP
    # providers as GH_TOKEN. Ambient GitHub token variables are always stripped.
    agent_github_token_enabled: bool = False
    # Provider processes are comparatively expensive. Keep restart recovery
    # bounded even when many sessions have durable unfinished work.
    agent_recovery_concurrency: int = Field(default=2, ge=1, le=16)
    # PA may take over a card's worktree lease from a session that is closed or
    # recovery-blocked with no live runtime, after saving its uncommitted work
    # to a rescue ref. Turn off to keep the "resume its owner" refusal.
    agent_reclaim_dead_session_leases: bool = True
    # Idle sessions remain follow-up capable for one day unless a durable
    # terminal workflow proves that they can close sooner.
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
    # Bounded local cruft cleanup. Does not delete cards, sessions, or objects/.
    maintenance_interval_seconds: int = Field(
        default=24 * 60 * 60, ge=60, le=7 * 24 * 60 * 60
    )
    terminal_output_char_limit: int = Field(default=1000, ge=100, le=100000)
    # `!command` in the chat composer runs an OS command in the session's working directory.
    # "anyone" matches the rest of the instance (no extra gate beyond auth_required);
    # "signed_in" additionally needs a real login; "disabled" turns `!` off.
    chat_shell_access: Literal["disabled", "signed_in", "anyone"] = "anyone"
    chat_shell_output_to_agent: bool = True
    chat_shell_timeout_seconds: int = Field(default=60, ge=1, le=3600)
    chat_shell_output_char_limit: int = Field(default=20000, ge=1000, le=1000000)
    transcript_retention_days: int = Field(default=14, ge=1, le=3650)
    mutation_operation_retention_days: int = Field(default=14, ge=1, le=3650)
    # Optional ACP final-fact candidates. Disabled by default; when enabled,
    # only policy-marked candidates enter pending review.
    memory_auto_capture_enabled: bool = False
    # Fail-closed limbic/Memory rollout defaults keyed by realm. Projects may
    # override these through tool_config.autonomy_context.
    autonomy_context_by_realm: dict[str, dict] = Field(default_factory=dict)

    # Card summaries use the selected provider's native HTTP contract.
    # OpenAI and MiniMax are Chat Completions; Anthropic/Claude is Messages.
    # Authentication is the matching write-only Settings key. OpenAI may also
    # reuse a provider-scoped Codex API credential. ChatGPT OAuth remains in
    # Codex's credential store and is diagnosed with a direct-key setup path.
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

    # Bounded post-turn evaluation and automatic follow-up policy. Evaluators
    # remain read-only; PA validates and executes eligible catalog actions.
    # The automatic follow-up budget is a runaway-loop guard, not a work limit:
    # it is high enough that a single card finishes end to end without an
    # operator approval. Set it to 2 to restore the previous behaviour.
    post_turn_evaluator_max_attempts: int = Field(default=2, ge=1, le=5)
    post_turn_max_automatic_followups: int = Field(default=25, ge=0, le=1000)
    post_turn_evaluation_timeout_seconds: float = Field(default=60.0, gt=0, le=600)
    post_turn_retry_seconds: float = Field(default=15.0, gt=0, le=3600)
    post_turn_escalation_threshold: int = Field(default=2, ge=1, le=10)
    # Follow-through: a background worker executes the evaluator's recommended
    # actions that need no approval, within the budget above, and raises a
    # manual-review notification when it cannot. False restores the previous
    # record-only behaviour.
    post_turn_worker_enabled: bool = True
    # Same-session prompts telling an agent why a Done claim was downgraded to
    # Waiting, per card. 0 turns the feedback off.
    post_turn_downgrade_feedback_max: int = Field(default=2, ge=0, le=20)
    # A card turn that never produced a valid disposition after reconciliation is
    # moved to Waiting and the owner is notified. False restores the quiet end.
    completion_missing_disposition_escalates: bool = True

    # Developer / debug
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

    # Bounded compatibility executor and responsiveness telemetry.
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
    # How long the shared Workshop snapshot is reused across live subscribers.
    workshop_snapshot_ttl_seconds: float = Field(default=1.5, ge=0, le=30)

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
    # Workshop page. ``floor`` opens the workshop-floor view (stations with
    # agents walking between them); ``list`` opens the compact list. Each browser
    # can still toggle and remembers its own choice.
    workshop_default_view: Literal["floor", "list"] = "floor"
    # Show chat (non-card) agents on the Workshop floor.
    workshop_floor_chat_agents: bool = True
    # ``auto`` walks agents between stations unless the browser asks for reduced
    # motion; ``off`` always snaps them and only updates the station counts.
    workshop_floor_motion: Literal["auto", "off"] = "auto"

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
    # Stalled, merged cards whose acceptance principal is the local operator are handed to the
    # independent verifier instead of waiting on a worker that has already ended. 0 disables it.
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
        "telegram_allowed_user_ids",
        "telegram_allowed_conversation_ids",
        "discord_allowed_user_ids",
        "discord_allowed_conversation_ids",
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

    @field_validator("telegram_owner_chat_id", "telegram_owner_user_id", mode="before")
    @classmethod
    def _telegram_ids_as_text(cls, value: object) -> object:
        # Provider IDs are numbers in JSON config files but identifiers in PA.
        return str(value).strip() if isinstance(value, int) else value

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
        self.cloud_endpoint = self.cloud_endpoint.strip().rstrip("/")
        if self.cloud_endpoint:
            from urllib.parse import urlparse

            parsed_cloud = urlparse(self.cloud_endpoint)
            if parsed_cloud.scheme not in {"https", "http"} or not parsed_cloud.netloc:
                raise ValueError("cloud_endpoint must be an absolute HTTP(S) URL")
            if parsed_cloud.scheme != "https" and not self.cloud_allow_insecure_http:
                raise ValueError(
                    "cloud_endpoint must use HTTPS unless cloud_allow_insecure_http is true"
                )
            if not self.cloud_token.strip():
                raise ValueError("cloud_token is required when cloud_endpoint is set")
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
        if self.intake_canonical_retention_hours < self.intake_raw_retention_hours:
            raise ValueError(
                "intake_canonical_retention_hours must be greater than or equal "
                "to intake_raw_retention_hours"
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
