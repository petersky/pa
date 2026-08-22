from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import random
import re
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse
from uuid import UUID, uuid4

import httpx

from pa.domain.models import AgentSession, CardLane, CardUpdate
from pa.execution.disposition import (
    decide_card_disposition,
    disposition_for_merged_watch,
)
from pa.pr_supervisor.gating import (
    build_executor_prompt_rendered,
    evaluate_gate,
    redact_external_value,
)
from pa.pr_supervisor.github import GitHubClient, GitHubCredentials
from pa.pr_supervisor.models import (
    GITHUB_TERMINAL_PR_WATCH_STATUSES,
    PR_WATCH_PROTOCOL_VERSION,
    GateResult,
    GitHubCapability,
    LeaseGrant,
    PRPolicy,
    PRSnapshot,
    PRWatch,
    PRWatchEvent,
    PRWatchStatus,
    canonical_repository_name,
    utcnow,
)
from pa.pr_supervisor.store import PRSupervisorStore, StaleFenceError
from pa.prompts import RenderedPrompt
from pa.repository.workspace import WorkspaceProvisioningError

if TYPE_CHECKING:
    from pa.core.async_runtime import AsyncRuntime

logger = logging.getLogger(__name__)

_EXPLICIT_PR_INTENT = re.compile(
    r"(?im)^\s*(?:[-*]\s*)?"
    r"(?P<label>integration\s+pr|pull\s+request|pr\s+watch|"
    r"supervise(?:d)?\s+pr|watched\s+pr)\s*:\s*"
    r"(?P<url>https://github\.com/"
    r"(?P<repository>[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)/pull/(?P<number>\d+))"
    r"\s*$"
)


class ProvenanceValidationError(ValueError):
    """Actionable rejection at a canonical provenance ingestion boundary."""

    def __init__(self, code: str, message: str, **detail: Any) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail = detail

    def http_detail(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, **self.detail}


@dataclass(frozen=True)
class _LocalLease:
    grant: LeaseGrant
    authority: str
    renew_at: float
    expires_at: float


def canonical_uuid(
    value: str | None, field: str, *, required: bool = True
) -> str | None:
    if not value:
        if required:
            raise ProvenanceValidationError(
                "provenance_id_required", f"{field} is required", field=field
            )
        return None
    try:
        parsed = str(UUID(value))
    except (ValueError, AttributeError) as exc:
        raise ProvenanceValidationError(
            "malformed_provenance_id",
            f"{field} must be a full canonical UUID; shortened workspace slugs are not valid",
            field=field,
            value=value,
        ) from exc
    if parsed != value:
        raise ProvenanceValidationError(
            "noncanonical_provenance_id",
            f"{field} must use the canonical lowercase hyphenated UUID form",
            field=field,
            value=value,
        )
    return parsed


class ExecutorDispatcher:
    """Wake/resume an executor, falling back to a card-scoped replacement."""

    def __init__(
        self,
        settings,
        domain_store,
        supervisor_store: PRSupervisorStore,
        *,
        agent_manager=None,
        fleet_registry=None,
        peer_table=None,
        http_client: httpx.AsyncClient | None = None,
        async_runtime: AsyncRuntime | None = None,
    ) -> None:
        self.settings = settings
        self.domain_store = domain_store
        self.store = supervisor_store
        self.agent = agent_manager
        self.fleet = fleet_registry
        self.peer_table = peer_table
        self.http_client = http_client
        self.async_runtime = async_runtime

    async def _offload(self, operation: str, call, *args, **kwargs):
        if self.async_runtime:
            return await self.async_runtime.run_blocking(
                operation, call, *args, **kwargs
            )
        return await asyncio.to_thread(call, *args, **kwargs)

    async def dispatch(
        self,
        watch: PRWatch,
        event_key: str,
        prompt: str | RenderedPrompt,
        *,
        authorization: dict[str, Any] | None = None,
        prompt_audit: list[dict[str, Any]] | None = None,
    ) -> str:
        if (
            not isinstance(authorization, dict)
            or authorization.get("protocol_version") != PR_WATCH_PROTOCOL_VERSION
        ):
            return "rejected"
        prompt_text = prompt.text if isinstance(prompt, RenderedPrompt) else prompt
        prompt_audit = list(prompt_audit or []) or (
            [prompt.audit_record()] if isinstance(prompt, RenderedPrompt) else []
        )
        target = watch.originating_instance_id
        if target and target != self.settings.instance_id:
            url = self._instance_url(target)
            if url:
                try:
                    result = await self._remote_dispatch(
                        url,
                        watch,
                        event_key,
                        prompt_text,
                        prompt_audit,
                        authorization=authorization,
                    )
                    return str(result.get("state") or "queued")
                except (httpx.ConnectError, RuntimeError) as exc:
                    logger.warning(
                        "PR supervisor remote executor unavailable watch=%s target=%s: %s",
                        watch.id,
                        target,
                        exc,
                    )
                    if authorization:
                        raise
                except httpx.HTTPError as exc:
                    # A read/protocol failure after the request left this instance is
                    # ambiguous: the destination may already have queued the prompt.
                    # Retrying the same event key remotely is safe; falling back to a
                    # different instance here is not.
                    logger.warning(
                        "PR supervisor remote dispatch outcome unknown watch=%s "
                        "target=%s: %s",
                        watch.id,
                        target,
                        exc,
                    )
                    raise
        return await self.dispatch_local(
            watch,
            event_key,
            prompt_text,
            prompt_audit=prompt_audit,
            authorization=authorization,
        )

    async def _remote_dispatch(
        self,
        url: str,
        watch: PRWatch,
        event_key: str,
        prompt: str,
        prompt_audit: list[dict[str, Any]],
        *,
        authorization: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if (
            not isinstance(authorization, dict)
            or authorization.get("protocol_version") != PR_WATCH_PROTOCOL_VERSION
        ):
            raise RuntimeError("pr_watch_effect_authorization_required")
        headers: dict[str, str] = {}
        if self.settings.sync_token:
            headers["Authorization"] = f"Bearer {self.settings.sync_token}"
        headers["X-PA-Origin-Instance-ID"] = self.settings.instance_id
        owns = self.http_client is None
        client = self.http_client or httpx.AsyncClient(timeout=30.0)
        try:
            response = await client.post(
                f"{url.rstrip('/')}/api/pr-supervisor/dispatch",
                headers=headers,
                json={
                    "watch": watch.model_dump(mode="json"),
                    "event_key": event_key,
                    "prompt": prompt,
                    "prompt_audit": prompt_audit,
                    "authorization": authorization,
                },
            )
            if response.status_code >= 400:
                raise RuntimeError(
                    f"executor dispatch returned HTTP {response.status_code}"
                )
            return await self._offload("pr_supervisor.response_json", response.json)
        finally:
            if owns:
                await client.aclose()

    async def dispatch_local(
        self,
        watch: PRWatch,
        event_key: str,
        prompt: str,
        *,
        prompt_audit: list[dict[str, Any]] | None = None,
        authorization: dict[str, Any] | None = None,
    ) -> str:
        if (
            not isinstance(authorization, dict)
            or authorization.get("protocol_version") != PR_WATCH_PROTOCOL_VERSION
        ):
            return "rejected"
        try:
            authorization_expires_at = datetime.fromisoformat(
                str(authorization.get("expires_at") or "")
            )
        except ValueError:
            return "rejected"
        if authorization:
            current = await self._offload(
                "sqlite.pr_supervisor_watch_read", self.store.get_watch, watch.id
            )
            current_authorization = (
                (current.state.get("effect_authorizations") or {}).get(event_key)
                if current
                else None
            )
            expected = {
                "realm_id": watch.realm_id,
                "watch_id": watch.id,
                "repository": watch.repository,
                "pr_number": watch.pr_number,
                "head_sha": watch.head_sha,
                "condition_fingerprint": watch.condition_fingerprint,
                "condition_version": watch.condition_version,
                "owner_instance_id": watch.owner_instance_id,
                "fence_token": watch.fence_token,
                "lease_version": watch.lease_version,
                "target_instance_id": self.settings.instance_id,
                "target_session_id": watch.originating_session_id,
                "content_digest": hashlib.sha256(prompt.encode()).hexdigest(),
                "policy_digest": hashlib.sha256(
                    watch.policy.model_dump_json().encode()
                ).hexdigest(),
                "review_hold_version": int(
                    (watch.state or {}).get("review_hold_version") or 0
                ),
            }
            if (
                not current
                or not current.actionable
                or authorization_expires_at <= utcnow()
                or current_authorization != authorization
                or any(
                    authorization.get(key) != value for key, value in expected.items()
                )
            ):
                return "rejected"
        if not await self._offload(
            "pr_supervisor.dispatch_claim",
            self.store.claim_dispatch,
            event_key,
            watch.id,
            target_instance_id=self.settings.instance_id,
            target_session_id=watch.originating_session_id,
        ):
            return "deduplicated"
        if not self.agent:
            await self._offload(
                "pr_supervisor.dispatch_finish",
                self.store.finish_dispatch,
                event_key,
                state="failed",
                detail="instance agent unavailable",
            )
            return "failed"
        runtime = None
        session = None
        delivery = "live"
        failure_reason = None
        try:
            if watch.originating_session_id:
                runtime = self.agent.get(watch.originating_session_id)
                session = await self._offload(
                    "sqlite.agent_session_read",
                    self.domain_store.get_session,
                    watch.originating_session_id,
                )
            if (
                runtime is None
                and session
                and session.status
                not in {"closed", "configuration_failed", "provisioning_failed"}
                and session.external_session_id
            ):
                try:
                    runtime = await self.agent.create_session(
                        existing=session,
                        resume_external_id=session.external_session_id,
                        label=session.label,
                        title=session.title,
                        cwd=watch.executor_cwd or session.cwd,
                        principal_id=session.principal_id,
                        card_id=watch.card_id or session.card_id,
                        project_id=watch.project_id or session.project_id,
                    )
                    delivery = "resumed"
                except WorkspaceProvisioningError as exc:
                    failure_reason = f"resume_workspace_failed:{exc}"
                except Exception:
                    logger.exception("Could not resume executor session %s", session.id)
                    failure_reason = "provider_thread_unrecoverable"
            elif runtime is None:
                if session is None:
                    failure_reason = "durable_session_missing"
                elif session.status == "closed":
                    failure_reason = "durable_session_closed"
                elif session.status in {"configuration_failed", "provisioning_failed"}:
                    failure_reason = f"durable_session_terminal:{session.status}"
                else:
                    failure_reason = "provider_thread_missing"
            if runtime is None:
                if authorization:
                    raise RuntimeError(
                        "fixed authorized destination session is unavailable"
                    )
                card = (
                    await self._offload(
                        "sqlite.card_read",
                        self.domain_store.get_card,
                        watch.card_id,
                        realm_id=watch.realm_id,
                    )
                    if watch.card_id
                    else None
                )
                if not card or card.lane not in {CardLane.ACTIVE, CardLane.WAITING}:
                    raise RuntimeError(
                        f"{failure_reason or 'session_unavailable'}; "
                        "fallback requires a linked active/waiting card"
                    )
                project = (
                    await self._offload(
                        "sqlite.project_read",
                        self.domain_store.get_project,
                        watch.project_id,
                        realm_id=watch.realm_id,
                    )
                    if watch.project_id
                    else None
                )
                runtime = await self.agent.create_session(
                    label=f"card:{watch.card_id}",
                    title=f"PR #{watch.pr_number} executor",
                    cwd=watch.executor_cwd,
                    principal_id="user:local",
                    card_id=watch.card_id,
                    project_id=watch.project_id,
                    project_tool_config=project.tool_config if project else None,
                    surface="execution",
                )
                delivery = "fallback"
            dispatch_state = f"{delivery}_queued"
            valid_dispatch_states = {
                "live_queued",
                "resumed_queued",
                "fallback_queued",
            }
            prompt_id = str(authorization["id"]) if authorization else None
            new_admission = True

            async def persist_enqueue() -> None:
                nonlocal dispatch_state
                admitted = runtime.enqueue(
                    prompt,
                    action="append",
                    card_id=watch.card_id,
                    project_id=watch.project_id,
                    principal_id=runtime.session.principal_id or "user:local",
                    cwd=runtime.session.cwd,
                    source="pr-supervisor",
                    prompt_audit=prompt_audit,
                    prompt_id=prompt_id,
                    acceptance_result=dispatch_state if prompt_id else None,
                )
                if prompt_id:
                    accepted_result = admitted.acceptance_result or dispatch_state
                    if accepted_result not in valid_dispatch_states:
                        raise RuntimeError(
                            f"Prompt id {prompt_id} has an invalid acceptance result"
                        )
                    dispatch_state = accepted_result
                drain = getattr(runtime, "_drain_transcripts", None)
                if drain:
                    drained = drain()
                    if inspect.isawaitable(drained):
                        await drained

            if prompt_id:
                admission_lock = runtime.__dict__.get("_prompt_admission_lock")
                if admission_lock is None:
                    admission_lock = asyncio.Lock()
                    runtime._prompt_admission_lock = admission_lock
                async with admission_lock:
                    accepted = await self._offload(
                        "sqlite.prompt_acceptance",
                        self.domain_store.get_queued_prompt_acceptance,
                        runtime.session_id,
                        prompt_id,
                    )
                    if accepted:
                        payload = accepted.payload or {}
                        if (
                            accepted.event_type != "queue_enqueued"
                            or payload.get("message") != prompt
                            or payload.get("images") not in (None, [])
                            or payload.get("source") not in (None, "pr-supervisor")
                        ):
                            raise RuntimeError(
                                f"Prompt id {prompt_id} was already accepted with "
                                "different content"
                            )
                        accepted_result = str(
                            payload.get("acceptance_result") or dispatch_state
                        )
                        if accepted_result not in valid_dispatch_states:
                            raise RuntimeError(
                                f"Prompt id {prompt_id} has an invalid acceptance result"
                            )
                        dispatch_state = accepted_result
                        new_admission = False
                    else:
                        await persist_enqueue()
            else:
                await persist_enqueue()
            detail = json.dumps(
                {
                    "session_id": runtime.session_id,
                    "originating_session_id": watch.originating_session_id,
                    "originating_agent": watch.originating_agent,
                    "resume_state": dispatch_state.removesuffix("_queued"),
                    "acceptance_replayed": not new_admission,
                    "attempt_resume_state": delivery,
                    "fallback_reason": failure_reason,
                },
                sort_keys=True,
            )
            await self._offload(
                "pr_supervisor.dispatch_finish",
                self.store.finish_dispatch,
                event_key,
                state=dispatch_state,
                detail=detail,
            )
            if new_admission:
                await self._offload(
                    "pr_supervisor.metric",
                    self.store.increment_metric,
                    "executor_prompts",
                )
            return dispatch_state
        except Exception as exc:  # noqa: BLE001
            await self._offload(
                "pr_supervisor.dispatch_finish",
                self.store.finish_dispatch,
                event_key,
                state="failed",
                detail=str(exc),
            )
            return "failed"

    def _instance_url(self, instance_id: str) -> str | None:
        if self.fleet:
            instance = self.fleet.get_instance(instance_id)
            if instance and instance.url:
                return instance.url
        if self.peer_table:
            for route in self.peer_table.all_routes():
                if route.target_instance_id == instance_id:
                    return route.target_url
        return None


class PRSupervisor:
    LEASE_TTL_SECONDS = 45
    # Renew with 8-12 seconds left. The lower bound absorbs one delayed loop or
    # request; per-worker jitter prevents a fleet-wide renewal burst.
    LEASE_RENEWAL_WINDOW_SECONDS = 12
    LEASE_RENEWAL_JITTER_SECONDS = 4
    # The worker discards this much of every authority duration in addition to
    # all request/transport elapsed time. Deadlines always live on monotonic time.
    LEASE_RESPONSE_SAFETY_SECONDS = 2.0
    LEASE_TAKEOVER_JITTER_SECONDS = 4
    LOOP_SECONDS = 2.0
    CAPABILITY_TTL_SECONDS = 120
    CAPABILITY_PROBE_SECONDS = 15 * 60
    CAPABILITY_ERROR_RETRY_SECONDS = 60
    CAPABILITY_HEARTBEAT_SECONDS = 60

    def __init__(
        self,
        settings,
        domain_store,
        *,
        supervisor_store: PRSupervisorStore | None = None,
        github_client: GitHubClient | None = None,
        dispatcher: ExecutorDispatcher | None = None,
        agent_manager=None,
        workspace_manager=None,
        dispatch_store=None,
        fleet_registry=None,
        peer_table=None,
        http_client: httpx.AsyncClient | None = None,
        async_runtime: AsyncRuntime | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self.settings = settings
        self.domain_store = domain_store
        self.store = supervisor_store or PRSupervisorStore(
            settings.data_dir / "pr_supervisor.db"
        )
        self.credentials = (
            github_client.credentials
            if github_client
            else GitHubCredentials.load(settings.data_dir)
        )
        self.async_runtime = async_runtime
        self._owns_http_client = http_client is None
        self.http_client = http_client or httpx.AsyncClient(
            timeout=httpx.Timeout(15.0, connect=5.0),
            limits=httpx.Limits(max_connections=16, max_keepalive_connections=8),
        )
        if github_client and getattr(github_client, "_provided_client", None) is None:
            github_client._provided_client = self.http_client
            if hasattr(github_client, "async_runtime"):
                github_client.async_runtime = async_runtime
        self.github = github_client or GitHubClient(
            self.credentials,
            client=self.http_client,
            async_runtime=async_runtime,
        )
        self.workspace_manager = workspace_manager or getattr(
            agent_manager, "workspace_manager", None
        )
        if self.workspace_manager:
            self.workspace_manager.set_pr_watch_provider(self.store.list_watches)
        self.dispatch_store = dispatch_store
        self.dispatcher = dispatcher or ExecutorDispatcher(
            settings,
            domain_store,
            self.store,
            agent_manager=agent_manager,
            fleet_registry=fleet_registry,
            peer_table=peer_table,
            http_client=self.http_client,
            async_runtime=async_runtime,
        )
        self.rng = rng or random.Random()
        self._task: asyncio.Task | None = None
        self._stopping = False
        self._capability: GitHubCapability | None = None
        self._capability_checked_at = None
        self._capability_heartbeat_at = None
        self._authority_last_success_at = None
        self._authority_last_error: str | None = None
        self._monotonic = time.monotonic
        self._local_leases: dict[str, _LocalLease] = {}
        self._lease_retry_at: dict[str, float] = {}
        self._lease_failure_attempts: dict[str, int] = {}
        self._lease_inflight: dict[str, asyncio.Task[LeaseGrant]] = {}
        self._lease_last_response: dict[str, dict[str, Any]] = {}
        self._lease_suppressed: dict[str, str] = {}
        self._lease_authority = self._lease_authority_key()

    async def _offload(self, operation: str, call, *args, **kwargs):
        if self.async_runtime:
            return await self.async_runtime.run_blocking(
                operation, call, *args, **kwargs
            )
        return await asyncio.to_thread(call, *args, **kwargs)

    async def _observe(self, operation: str, awaitable, *, timeout: float = 15.0):
        if self.async_runtime:
            return await self.async_runtime.observe(
                operation, awaitable, timeout=timeout
            )
        async with asyncio.timeout(timeout):
            return await awaitable

    @property
    def capability(self) -> GitHubCapability:
        return self._capability or self.credentials.capability(
            self.settings.instance_id
        )

    async def start(self) -> None:
        self._stopping = False
        if not self._task or self._task.done():
            self._task = asyncio.create_task(self._run_loop(), name="pa-pr-supervisor")

    async def stop(self) -> None:
        self._stopping = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self._clear_all_lease_state()
        if self._owns_http_client:
            await self.http_client.aclose()

    async def _run_loop(self) -> None:
        try:
            await self.refresh_capability(force=True)
            await self.migrate_discoverable_associations()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("PR supervisor startup reconciliation failed")
            await self._offload(
                "sqlite.pr_supervisor_metric",
                self.store.increment_metric,
                "loop_errors",
            )
        while not self._stopping:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("PR supervisor loop failed")
                await self._offload(
                    "sqlite.pr_supervisor_metric",
                    self.store.increment_metric,
                    "loop_errors",
                )
            await asyncio.sleep(self.LOOP_SECONDS)

    async def refresh_capability(self, *, force: bool = False) -> GitHubCapability:
        now = utcnow()
        credentials = await self._offload(
            "filesystem.github_credentials_read",
            GitHubCredentials.load,
            self.settings.data_dir,
        )
        credentials_changed = credentials != self.credentials
        self.credentials = credentials
        self.github.credentials = credentials
        probe_seconds = (
            self.CAPABILITY_ERROR_RETRY_SECONDS
            if self._capability and self._capability.state == "error"
            else self.CAPABILITY_PROBE_SECONDS
        )
        probe_due = (
            force
            or credentials_changed
            or not self._capability
            or not self._capability_checked_at
            or now - self._capability_checked_at >= timedelta(seconds=probe_seconds)
        )
        if probe_due:
            self._capability = (
                await self.github.probe(self.settings.instance_id)
            ).model_copy(update={"pr_watch_protocol_version": 2})
            self._capability_checked_at = now

        heartbeat_due = (
            probe_due
            or not self._capability_heartbeat_at
            or now - self._capability_heartbeat_at
            >= timedelta(seconds=self.CAPABILITY_HEARTBEAT_SECONDS)
        )
        if heartbeat_due:
            # Heartbeats keep fleet eligibility fresh without treating every
            # heartbeat as a reason to call GitHub's /user endpoint.
            self._capability = self._capability.model_copy(update={"checked_at": now})
            await self._offload(
                "sqlite.pr_supervisor_capability_write",
                self.store.save_capability,
                self._capability,
            )
            await self._heartbeat_authority(self._capability)
            self._capability_heartbeat_at = now
        return self._capability

    async def run_once(self) -> None:
        capability = await self.refresh_capability()
        await self._prune_lease_state(capability)
        await self._renew_due_local_leases(capability)
        await self._reconcile_merged_cards()
        due = await self._offload("sqlite.pr_supervisor_due_read", self.store.list_due)
        if not due:
            return
        for watch in due:
            if not capability.supports(watch.repository):
                self._forget_watch(watch.id)
                eligible = await self._eligible_capabilities(watch.repository)
                if not eligible:
                    next_poll = utcnow() + timedelta(
                        seconds=watch.policy.poll_max_seconds
                    )
                    await self._offload(
                        "sqlite.pr_supervisor_watch_write",
                        self.store.mark_error,
                        watch.id,
                        "No eligible authenticated PA instance can access this repository",
                        next_poll_at=next_poll,
                        visible_state="no_eligible_authenticated_instance",
                    )
                    await self._audit(
                        watch,
                        "capability_missing",
                        f"{watch.id}:capability:none",
                        payload={
                            "required_capabilities": watch.required_capabilities,
                            "action": "Configure instance-local GitHub authentication",
                        },
                    )
                continue
            grant = await self._acquire_lease(watch, capability)
            if not grant.acquired:
                continue
            current = await self._offload(
                "sqlite.pr_supervisor_watch_read", self.store.get_watch, watch.id
            )
            if current and current.actionable:
                await self._process_watch(current, grant)
            else:
                self._forget_watch(watch.id)

    async def register_watch(
        self, watch: PRWatch, *, source: str = "api", replicate: bool = True
    ) -> PRWatch:
        if watch.originating_session_id and watch.provenance_version < 1:
            raise ProvenanceValidationError(
                "unverified_session_provenance",
                "Session-backed watches must be resolved through register_watch_from_session.",
                originating_session_id=watch.originating_session_id,
            )
        if not watch.originating_instance_id and not watch.originating_session_id:
            watch.originating_instance_id = self.settings.instance_id
        if not watch.required_capabilities:
            watch.required_capabilities = [
                "pr-supervisor",
                "github:authenticated",
                f"github:repo:{watch.repository}",
            ]
        stored = await self._offload(
            "sqlite.pr_supervisor_watch_write", self.store.upsert_watch, watch
        )
        await self._audit(
            stored,
            "watch_created",
            f"{stored.id}:created",
            source=source,
            payload={
                "repository": stored.repository,
                "pr_number": stored.pr_number,
                "card_id": stored.card_id,
                "project_id": stored.project_id,
                "repository_id": stored.repository_id,
                "dispatch_id": stored.dispatch_id,
                "originating_instance_id": stored.originating_instance_id,
                "authority_instance_id": stored.authority_instance_id,
                "originating_session_id": stored.originating_session_id,
                "originating_principal_id": stored.originating_principal_id,
                "provenance_version": stored.provenance_version,
                "creation_reason": stored.creation_reason,
                "qualifying_evidence": stored.qualifying_evidence,
                "originating_agent": stored.originating_agent,
                "policy": stored.policy.model_dump(mode="json"),
                "policy_source": stored.policy_source,
                "policy_revision": stored.policy_revision,
                "policy_snapshot_at": stored.policy_snapshot_at.isoformat(),
            },
        )
        if replicate:
            await self._replicate(stored)
        return stored

    async def resolve_session_provenance(self, watch: PRWatch) -> PRWatch:
        """Resolve canonical watch provenance from one durable local session."""
        session_id = canonical_uuid(
            watch.originating_session_id, "originating_session_id"
        )
        session = await self._offload(
            "sqlite.agent_session_read", self.domain_store.get_session, session_id
        )
        if not isinstance(session, AgentSession):
            raise ProvenanceValidationError(
                "originating_session_not_found",
                "The canonical originating session does not exist on this instance.",
                originating_session_id=session_id,
                action="Register from the session's owning instance or relink explicitly.",
            )
        realm_id = session.realm_id or self.settings.primary_realm
        if watch.realm_id != realm_id:
            raise ProvenanceValidationError(
                "provenance_realm_mismatch",
                "The session and requested PR watch belong to different realms.",
                session_realm=realm_id,
                requested_realm=watch.realm_id,
            )
        origin_instance_id = canonical_uuid(
            session.origin_instance_id, "session.origin_instance_id"
        )
        if origin_instance_id != self.settings.instance_id:
            raise ProvenanceValidationError(
                "session_owner_instance_mismatch",
                "The session is not owned by the instance accepting registration.",
                session_instance_id=origin_instance_id,
                accepting_instance_id=self.settings.instance_id,
                action="Register the watch on the session's owning instance.",
            )
        card_id = canonical_uuid(
            session.card_id or session.item_id, "session.card_id", required=False
        )
        project_id = canonical_uuid(
            session.project_id, "session.project_id", required=False
        )
        dispatch_id = canonical_uuid(
            session.dispatch_id, "session.dispatch_id", required=False
        )
        authority_instance_id = canonical_uuid(
            session.authority_instance_id or origin_instance_id,
            "session.authority_instance_id",
            required=False,
        )
        if (
            authority_instance_id
            and authority_instance_id != origin_instance_id
            and not dispatch_id
        ):
            raise ProvenanceValidationError(
                "remote_dispatch_link_missing",
                "A remotely owned session must retain its canonical dispatch linkage.",
                originating_session_id=session_id,
                authority_instance_id=authority_instance_id,
            )
        card = None
        if card_id:
            card = await self._offload(
                "sqlite.card_read",
                self.domain_store.get_card,
                card_id,
                realm_id=realm_id,
            )
            if not card:
                raise ProvenanceValidationError(
                    "provenance_card_not_found",
                    "The session's canonical card does not exist in its realm.",
                    card_id=card_id,
                    realm_id=realm_id,
                )
        project = None
        if project_id:
            project = await self._offload(
                "sqlite.project_read",
                self.domain_store.get_project,
                project_id,
                realm_id=realm_id,
            )
            if not project:
                raise ProvenanceValidationError(
                    "provenance_project_not_found",
                    "The session's canonical project does not exist in its realm.",
                    project_id=project_id,
                    realm_id=realm_id,
                )
        if card and card.project_id != project_id:
            raise ProvenanceValidationError(
                "card_project_provenance_mismatch",
                "The session project does not match the canonical card project.",
                card_project_id=card.project_id,
                session_project_id=project_id,
            )
        principal_id = session.principal_id
        if not principal_id:
            raise ProvenanceValidationError(
                "originating_principal_missing",
                "The durable session has no originating principal.",
                originating_session_id=session_id,
            )

        execution = dict((session.config_json or {}).get("execution_context") or {})
        matches: list[str] = []
        for repository_context in execution.get("repositories") or []:
            if not isinstance(repository_context, dict):
                continue
            repository_id = canonical_uuid(
                repository_context.get("repository_id"),
                "execution_context.repositories[].repository_id",
            )
            repository = await self._offload(
                "sqlite.repository_read",
                self.domain_store.get_repository,
                repository_id,
                realm_id=realm_id,
            )
            if not repository:
                raise ProvenanceValidationError(
                    "provenance_repository_not_found",
                    "A repository in the session execution context no longer exists in its realm.",
                    repository_id=repository_id,
                    realm_id=realm_id,
                )
            try:
                same_repository = canonical_repository_name(
                    repository.url
                ) == canonical_repository_name(watch.repository)
            except ValueError:
                same_repository = False
            if same_repository:
                matches.append(repository_id)
        matches = sorted(set(matches))
        if not matches:
            raise ProvenanceValidationError(
                "repository_not_in_session_context",
                "The PR repository is not one of the session's structured repositories.",
                repository=watch.repository,
                originating_session_id=session_id,
                action="Use the repository linked in PA's execution context; paths and branches are not authoritative.",
            )
        if len(matches) != 1:
            raise ProvenanceValidationError(
                "ambiguous_repository_provenance",
                "Multiple canonical repositories match this PR; operator selection is required.",
                repository=watch.repository,
                repository_ids=matches,
            )
        repository_id = matches[0]

        if dispatch_id:
            if not self.dispatch_store:
                raise ProvenanceValidationError(
                    "dispatch_store_unavailable",
                    "The durable dispatch store is unavailable for provenance verification.",
                    dispatch_id=dispatch_id,
                )
            dispatch = await self._offload(
                "dispatch.record_read", self.dispatch_store.get, dispatch_id
            )
            if not dispatch:
                raise ProvenanceValidationError(
                    "provenance_dispatch_not_found",
                    "The session's canonical dispatch does not exist.",
                    dispatch_id=dispatch_id,
                )
            dispatch_mismatches = {
                field: {"dispatch": expected, "session": actual}
                for field, expected, actual in (
                    ("session_id", dispatch.session_id, session_id),
                    ("card_id", dispatch.card_id, card_id),
                    ("project_id", dispatch.project_id, project_id),
                    ("realm_id", dispatch.realm_id, realm_id),
                    (
                        "target_instance_id",
                        dispatch.target_instance_id,
                        origin_instance_id,
                    ),
                    ("principal_id", dispatch.principal_id, principal_id),
                    (
                        "authority_instance_id",
                        dispatch.authority_instance_id,
                        authority_instance_id,
                    ),
                )
                if expected != actual
            }
            if dispatch_mismatches:
                raise ProvenanceValidationError(
                    "dispatch_session_provenance_mismatch",
                    "The durable dispatch and session provenance do not match.",
                    dispatch_id=dispatch_id,
                    mismatches=dispatch_mismatches,
                )

        expected = {
            "card_id": card_id,
            "project_id": project_id,
            "repository_id": repository_id,
            "dispatch_id": dispatch_id,
            "originating_instance_id": origin_instance_id,
            "authority_instance_id": authority_instance_id,
            "originating_principal_id": principal_id,
        }
        supplied = {
            "card_id": watch.card_id,
            "project_id": watch.project_id,
            "repository_id": watch.repository_id,
            "dispatch_id": watch.dispatch_id,
            "originating_instance_id": watch.originating_instance_id,
            "authority_instance_id": watch.authority_instance_id,
            "originating_principal_id": watch.originating_principal_id,
        }
        for field, value in supplied.items():
            if value is not None and field != "originating_principal_id":
                canonical_uuid(value, field, required=False)
            if value is not None and value != expected[field]:
                raise ProvenanceValidationError(
                    "caller_provenance_mismatch",
                    f"Caller-supplied {field} does not match the durable session context.",
                    field=field,
                    supplied=value,
                    canonical=expected[field],
                )

        watch.card_id = card_id
        watch.project_id = project_id
        watch.repository_id = repository_id
        watch.dispatch_id = dispatch_id
        watch.originating_instance_id = origin_instance_id
        watch.authority_instance_id = authority_instance_id
        watch.originating_session_id = session_id
        watch.originating_principal_id = principal_id
        watch.originating_agent = session.agent_name
        watch.executor_cwd = session.cwd
        watch.provenance_version = 1
        return watch

    async def freeze_canonical_policy(self, watch: PRWatch) -> PRWatch:
        """Freeze policy only after canonical project/repository provenance exists."""
        if not watch.project_id:
            watch.policy = PRPolicy()
            watch.policy_source = "default (no canonical project)"
        else:
            project = await self._offload(
                "sqlite.project_read",
                self.domain_store.get_project,
                watch.project_id,
                realm_id=watch.realm_id,
            )
            if not project:
                raise ProvenanceValidationError(
                    "provenance_project_not_found",
                    "Canonical project disappeared before PR policy resolution.",
                    project_id=watch.project_id,
                )
            config = project.tool_config or {}
            policy_data = dict(config.get("pr_policy") or {})
            source = f"project:{watch.project_id}"
            overrides = config.get("pr_repository_policies") or {}
            repository_override = next(
                (
                    value
                    for key, value in overrides.items()
                    if canonical_repository_name(str(key))
                    == canonical_repository_name(watch.repository)
                ),
                None,
            )
            if repository_override:
                policy_data.update(repository_override)
                source = f"repository:{watch.repository}"
            watch.policy = PRPolicy.model_validate(policy_data)
            watch.policy_source = source
        encoded = json.dumps(
            {"source": watch.policy_source, "policy": watch.policy.model_dump(mode="json")},
            sort_keys=True,
            separators=(",", ":"),
        )
        watch.policy_revision = hashlib.sha256(encoded.encode()).hexdigest()[:16]
        watch.policy_snapshot_at = utcnow()
        return watch

    async def register_watch_from_session(
        self, watch: PRWatch, *, source: str = "api"
    ) -> PRWatch:
        resolved = await self.resolve_session_provenance(watch)
        resolved = await self.freeze_canonical_policy(resolved)
        existing = await self._offload(
            "sqlite.pr_supervisor_watch_read",
            self.store.find_watch,
            resolved.realm_id,
            resolved.repository,
            resolved.pr_number,
        )
        if existing:
            same = all(
                getattr(existing, field) == getattr(resolved, field)
                for field in (
                    "card_id",
                    "project_id",
                    "repository_id",
                    "dispatch_id",
                    "originating_instance_id",
                    "authority_instance_id",
                    "originating_session_id",
                    "originating_principal_id",
                )
            )
            if existing.provenance_version >= 1 and same:
                return existing
            raise ProvenanceValidationError(
                "existing_watch_provenance_conflict",
                "This PR already has different or unverified provenance.",
                watch_id=existing.id,
                action="Use the audited provenance repair endpoint; registration never rewrites an existing relationship.",
            )
        return await self.register_watch(resolved, source=source)

    async def validate_replica_provenance(self, watch: PRWatch) -> PRWatch:
        """Validate server-resolved provenance without requiring a remote session copy."""
        if watch.provenance_version == 0:
            return watch
        if watch.provenance_version != 1:
            raise ProvenanceValidationError(
                "unsupported_provenance_version",
                "Only canonical watch provenance version 1 is supported.",
                provenance_version=watch.provenance_version,
            )
        required_fields = (
            "repository_id",
            "originating_instance_id",
            "authority_instance_id",
            "originating_session_id",
        )
        for field in required_fields:
            canonical_uuid(getattr(watch, field), field)
        if (
            watch.authority_instance_id != watch.originating_instance_id
            and not watch.dispatch_id
        ):
            raise ProvenanceValidationError(
                "remote_dispatch_link_missing",
                "A remote watch must retain its canonical dispatch linkage.",
                originating_instance_id=watch.originating_instance_id,
                authority_instance_id=watch.authority_instance_id,
            )
        for field in ("card_id", "project_id", "dispatch_id"):
            canonical_uuid(getattr(watch, field), field, required=False)
        repository = await self._offload(
            "sqlite.repository_read",
            self.domain_store.get_repository,
            watch.repository_id,
            realm_id=watch.realm_id,
        )
        if not repository:
            raise ProvenanceValidationError(
                "provenance_repository_not_found",
                "The replicated repository does not exist in the watch realm.",
                repository_id=watch.repository_id,
                realm_id=watch.realm_id,
            )
        try:
            repository_matches = canonical_repository_name(
                repository.url
            ) == canonical_repository_name(watch.repository)
        except ValueError:
            repository_matches = False
        if not repository_matches:
            raise ProvenanceValidationError(
                "repository_identity_mismatch",
                "The repository ID and GitHub repository name do not match.",
                repository_id=watch.repository_id,
                repository=watch.repository,
            )
        if watch.card_id:
            card = await self._offload(
                "sqlite.card_read",
                self.domain_store.get_card,
                watch.card_id,
                realm_id=watch.realm_id,
            )
            if not card:
                raise ProvenanceValidationError(
                    "provenance_card_not_found",
                    "The replicated card does not exist in the watch realm.",
                    card_id=watch.card_id,
                    realm_id=watch.realm_id,
                )
            if card.project_id != watch.project_id:
                raise ProvenanceValidationError(
                    "card_project_provenance_mismatch",
                    "The replicated watch project does not match the canonical card.",
                    card_project_id=card.project_id,
                    watch_project_id=watch.project_id,
                )
        if watch.project_id:
            project = await self._offload(
                "sqlite.project_read",
                self.domain_store.get_project,
                watch.project_id,
                realm_id=watch.realm_id,
            )
            if not project:
                raise ProvenanceValidationError(
                    "provenance_project_not_found",
                    "The replicated project does not exist in the watch realm.",
                    project_id=watch.project_id,
                    realm_id=watch.realm_id,
                )
        if watch.dispatch_id and self.dispatch_store:
            dispatch = await self._offload(
                "dispatch.record_read", self.dispatch_store.get, watch.dispatch_id
            )
            if dispatch:
                mismatches = {
                    field: {"dispatch": expected, "watch": actual}
                    for field, expected, actual in (
                        (
                            "session_id",
                            dispatch.session_id,
                            watch.originating_session_id,
                        ),
                        ("card_id", dispatch.card_id, watch.card_id),
                        ("project_id", dispatch.project_id, watch.project_id),
                        ("realm_id", dispatch.realm_id, watch.realm_id),
                        (
                            "target_instance_id",
                            dispatch.target_instance_id,
                            watch.originating_instance_id,
                        ),
                        (
                            "authority_instance_id",
                            dispatch.authority_instance_id,
                            watch.authority_instance_id,
                        ),
                        (
                            "principal_id",
                            dispatch.principal_id,
                            watch.originating_principal_id,
                        ),
                    )
                    if expected != actual
                }
                if mismatches:
                    raise ProvenanceValidationError(
                        "dispatch_watch_provenance_mismatch",
                        "The replicated watch conflicts with the durable dispatch.",
                        dispatch_id=watch.dispatch_id,
                        mismatches=mismatches,
                    )
        return watch

    async def provenance_diagnostics(
        self, *, realm_id: str, include_retired: bool = True
    ) -> list[dict[str, Any]]:
        watches = await self._offload(
            "sqlite.pr_supervisor_watch_read",
            self.store.list_watches,
            realm_id=realm_id,
            include_retired=include_retired,
        )
        diagnostics: list[dict[str, Any]] = []
        for watch in watches:
            issues: list[dict[str, Any]] = []
            linked = bool(
                watch.card_id
                or watch.originating_session_id
                or watch.dispatch_id
                or watch.repository_id
            )
            if linked and watch.provenance_version < 1:
                issues.append(
                    {
                        "code": "unverified_legacy_provenance",
                        "message": "The watch predates server-resolved provenance.",
                    }
                )
            for field in (
                "card_id",
                "project_id",
                "repository_id",
                "dispatch_id",
                "originating_instance_id",
                "authority_instance_id",
                "originating_session_id",
            ):
                value = getattr(watch, field)
                if not value:
                    continue
                try:
                    canonical_uuid(value, field)
                except ProvenanceValidationError as exc:
                    issues.append(exc.http_detail())
            if watch.provenance_version >= 1 and watch.originating_session_id:
                try:
                    await self.resolve_session_provenance(watch.model_copy(deep=True))
                except ProvenanceValidationError as exc:
                    issues.append(exc.http_detail())
            if issues:
                diagnostics.append(
                    {
                        "watch_id": watch.id,
                        "repository": watch.repository,
                        "pr_number": watch.pr_number,
                        "status": watch.status.value,
                        "issues": issues,
                        "repair": {
                            "method": "POST",
                            "path": f"/api/pr-supervisor/watches/{watch.id}/provenance/repair",
                            "required": ["originating_session_id", "idempotency_key"],
                            "guesses": False,
                        },
                    }
                )
        return diagnostics

    async def repair_watch_provenance(
        self,
        watch_id: str,
        *,
        originating_session_id: str,
        idempotency_key: str,
        actor: str,
    ) -> PRWatch:
        if not idempotency_key.strip():
            raise ProvenanceValidationError(
                "repair_idempotency_key_required",
                "An idempotency_key is required for audited provenance repair.",
            )
        existing = await self._offload(
            "sqlite.pr_supervisor_watch_read", self.store.get_watch, watch_id
        )
        if not existing:
            raise ProvenanceValidationError(
                "watch_not_found", "PR watch not found", watch_id=watch_id
            )
        candidate = PRWatch(
            realm_id=existing.realm_id,
            repository=existing.repository,
            pr_number=existing.pr_number,
            pr_url=existing.pr_url,
            originating_session_id=originating_session_id,
            policy=existing.policy,
        )
        resolved = await self.resolve_session_provenance(candidate)
        before = {
            field: getattr(existing, field)
            for field in (
                "card_id",
                "project_id",
                "repository_id",
                "dispatch_id",
                "originating_instance_id",
                "authority_instance_id",
                "originating_session_id",
                "originating_principal_id",
                "provenance_version",
            )
        }
        resolved_values = {field: getattr(resolved, field) for field in before}
        if (
            before == resolved_values
            and existing.originating_agent == resolved.originating_agent
            and existing.executor_cwd == resolved.executor_cwd
        ):
            return existing
        repaired = existing.model_copy(deep=True)
        for field in before:
            setattr(repaired, field, getattr(resolved, field))
        repaired.originating_agent = resolved.originating_agent
        repaired.executor_cwd = resolved.executor_cwd
        stored = await self._offload(
            "sqlite.pr_supervisor_watch_write",
            self.store.upsert_watch,
            repaired,
            preserve_lease=True,
        )
        after = {field: getattr(stored, field) for field in before}
        await self._audit(
            stored,
            "provenance_repaired",
            f"{stored.id}:provenance-repair:{idempotency_key.strip()}",
            source=f"repair:{actor}",
            payload={
                "before": before,
                "after": after,
                "originating_session_id": originating_session_id,
                "guessed": False,
            },
        )
        await self._replicate(stored)
        return stored

    async def refresh_watch(self, watch_id: str) -> PRWatch | None:
        # Operator refresh is an explicit retry: cancel stale shared work and
        # clear cached grants, denial backoff, and failure attempts first.
        self._forget_watch(watch_id)
        if not await self._offload(
            "sqlite.pr_supervisor_watch_write",
            self.store.schedule_now,
            watch_id=watch_id,
        ):
            return None
        watch = await self._offload(
            "sqlite.pr_supervisor_watch_read", self.store.get_watch, watch_id
        )
        await self._replicate(watch)
        return watch

    async def retire_watch(self, watch_id: str) -> PRWatch | None:
        current = await self._offload(
            "sqlite.pr_supervisor_watch_read", self.store.get_watch, watch_id
        )
        if not current:
            return None
        if (
            current.retired_at is not None
            and current.owner_instance_id is None
            and current.lease_expires_at is None
        ):
            return current
        status = (
            current.status
            if current.status in GITHUB_TERMINAL_PR_WATCH_STATUSES
            else PRWatchStatus.RETIRED
        )
        reason = (
            "operator_archived_terminal_watch"
            if status in GITHUB_TERMINAL_PR_WATCH_STATUSES
            else "operator_retired_watch"
        )
        watch = await self._offload(
            "sqlite.pr_supervisor_watch_write",
            self.store.set_terminal,
            watch_id,
            status,
            retirement_reason=reason,
        )
        self._forget_watch(watch_id)
        await self._audit(
            watch,
            "watch_retired",
            f"{watch.id}:retired:{watch.retired_at.isoformat()}",
            source="operator",
            payload={
                "reason": reason,
                "retired_at": watch.retired_at.isoformat(),
                "terminal_status": watch.status.value,
            },
        )
        await self._replicate(watch)
        await self._broadcast_retirement(watch)
        return watch

    async def backfill_terminal_retirements(
        self,
        *,
        realm_id: str,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Archive legacy terminal watches only after GitHub revalidation."""
        watches = await self._offload(
            "sqlite.pr_supervisor_watch_read",
            self.store.list_watches,
            realm_id=realm_id,
            include_retired=True,
        )
        candidates = [
            watch
            for watch in watches
            if watch.status in GITHUB_TERMINAL_PR_WATCH_STATUSES
            and watch.retired_at is None
        ]
        semaphore = asyncio.Semaphore(8)

        async def migrate(watch: PRWatch) -> dict[str, Any]:
            async with semaphore:
                try:
                    snapshot = await self._observe(
                        "http.github_terminal_revalidation",
                        self.github.snapshot(
                            watch.repository,
                            watch.pr_number,
                            policy=watch.policy,
                        ),
                    )
                except Exception as exc:  # noqa: BLE001
                    # A single inaccessible historical PR must not abort the
                    # operator's bounded bulk migration report.
                    return {
                        "watch_id": watch.id,
                        "status": "error",
                        "expected_terminal_status": watch.status.value,
                        "error": type(exc).__name__,
                    }
            observed_status = (
                PRWatchStatus.MERGED
                if snapshot.merged
                else PRWatchStatus.CLOSED
                if snapshot.closed
                else None
            )
            if observed_status != watch.status:
                if not dry_run:
                    await self._audit(
                        watch,
                        "terminal_retirement_backfill_skipped",
                        f"{watch.id}:terminal-retirement-backfill:v1:skipped",
                        source="migration:terminal-retirement-v1",
                        payload={
                            "expected_terminal_status": watch.status.value,
                            "observed_github_state": snapshot.state,
                            "observed_terminal_status": (
                                observed_status.value if observed_status else None
                            ),
                        },
                    )
                return {
                    "watch_id": watch.id,
                    "status": "skipped",
                    "expected_terminal_status": watch.status.value,
                    "observed_github_state": snapshot.state,
                }
            if dry_run:
                return {
                    "watch_id": watch.id,
                    "status": "would_archive",
                    "terminal_status": watch.status.value,
                }
            current = await self._offload(
                "sqlite.pr_supervisor_watch_read",
                self.store.get_watch,
                watch.id,
            )
            if (
                not current
                or current.status != watch.status
                or current.retired_at is not None
            ):
                return {
                    "watch_id": watch.id,
                    "status": "already_converged",
                }
            archived = await self._offload(
                "sqlite.pr_supervisor_watch_write",
                self.store.set_terminal,
                current.id,
                current.status,
                state=current.state,
                retirement_reason="terminal_retirement_backfill_revalidated",
            )
            self._forget_watch(archived.id)
            await self._audit(
                archived,
                "watch_archived",
                f"{archived.id}:terminal-retirement-backfill:v1",
                source="migration:terminal-retirement-v1",
                payload={
                    "reason": "terminal_retirement_backfill_revalidated",
                    "retired_at": archived.retired_at.isoformat(),
                    "terminal_status": archived.status.value,
                    "observed_github_state": snapshot.state,
                    "merge_commit_sha": snapshot.merge_commit_sha,
                },
            )
            await self._replicate(archived)
            await self._broadcast_retirement(archived)
            return {
                "watch_id": archived.id,
                "status": "archived",
                "terminal_status": archived.status.value,
                "retired_at": archived.retired_at.isoformat(),
            }

        results = await asyncio.gather(*(migrate(watch) for watch in candidates))
        counts = dict(Counter(item["status"] for item in results))
        archived_count = counts.get("archived", 0)
        if archived_count:
            await self._offload(
                "sqlite.pr_supervisor_metric",
                self.store.increment_metric,
                "terminal_retirement_backfilled",
                archived_count,
            )
        return {
            "realm_id": realm_id,
            "dry_run": dry_run,
            "scanned": len(watches),
            "candidates": len(candidates),
            "counts": counts,
            "results": results,
        }

    async def _process_watch(self, watch: PRWatch, grant: LeaseGrant) -> None:
        now = utcnow()
        try:
            # Watches do not retain autonomous authority from a stale policy
            # snapshot. Refresh canonical policy before every observation/effect;
            # a change also advances the durable hold version so prepared effect
            # receipts can no longer be accepted.
            canonical_project = None
            if watch.project_id:
                canonical_project = await self._offload(
                    "sqlite.project_read",
                    self.domain_store.get_project,
                    watch.project_id,
                    realm_id=watch.realm_id,
                )
            refreshed = (
                await self.freeze_canonical_policy(watch.model_copy(deep=True))
                if not watch.project_id or canonical_project is not None
                else watch
            )
            if refreshed.policy_revision != watch.policy_revision:
                prior_revision = watch.policy_revision
                refreshed.state = {
                    **(watch.state or {}),
                    "review_hold_version": int(
                        (watch.state or {}).get("review_hold_version") or 0
                    )
                    + 1,
                    "policy_stale": False,
                    "policy_changed_from": prior_revision,
                }
                watch = await self._offload(
                    "sqlite.pr_supervisor_policy_refresh",
                    self.store.upsert_watch,
                    refreshed,
                    preserve_lease=True,
                )
                await self._audit(
                    watch,
                    "watch_policy_reauthorized",
                    f"{watch.id}:policy:{watch.policy_revision}",
                    payload={
                        "previous_revision": prior_revision,
                        "policy_revision": watch.policy_revision,
                        "policy_source": watch.policy_source,
                        "review_hold_version": watch.state["review_hold_version"],
                    },
                )
            snapshot = await self._observe(
                "http.github_snapshot",
                self.github.snapshot(
                    watch.repository, watch.pr_number, policy=watch.policy
                ),
                timeout=60.0,
            )
            current = await self._offload(
                "sqlite.pr_supervisor_watch_read", self.store.get_watch, watch.id
            )
            if not current or not current.actionable:
                self._forget_watch(watch.id)
                return
            confirmed = await self._acquire_lease(current, self.capability)
            if not confirmed.acquired:
                return
            watch = await self._offload(
                "sqlite.pr_supervisor_watch_read", self.store.get_watch, watch.id
            )
            if not watch or not watch.actionable:
                self._forget_watch(current.id)
                return
            grant = confirmed
            await self._offload(
                "sqlite.pr_supervisor_metric",
                self.store.increment_metric,
                "polls",
            )
            if snapshot.stale:
                next_poll = now + timedelta(seconds=watch.policy.poll_min_seconds)
                await self._audit(
                    watch,
                    "stale_head_discarded",
                    f"{watch.id}:stale:{snapshot.head_sha}:{snapshot.confirmed_head_sha}",
                    head_sha=snapshot.head_sha,
                    payload={
                        "observed_head": snapshot.head_sha,
                        "confirmed_head": snapshot.confirmed_head_sha,
                    },
                )
                await self._offload(
                    "sqlite.pr_supervisor_watch_write",
                    self.store.mark_error,
                    watch.id,
                    "Head changed during observation; stale result discarded",
                    next_poll_at=next_poll,
                    owner_instance_id=self.settings.instance_id,
                    fence_token=grant.fence_token,
                    visible_state="stale_head_repoll",
                )
                current = await self._offload(
                    "sqlite.pr_supervisor_watch_read", self.store.get_watch, watch.id
                )
                await self._replicate(current)
                return
            if snapshot.merged:
                await self._handle_merged(watch, snapshot, grant)
                return
            if snapshot.closed:
                state = self._safe_snapshot(snapshot)
                state["supervisor_state"] = "retired_after_close"
                await self._audit(
                    watch,
                    "pull_request_closed",
                    f"{watch.id}:{snapshot.head_sha}:closed",
                    head_sha=snapshot.head_sha,
                    payload={"url": snapshot.url},
                )
                terminal = await self._offload(
                    "sqlite.pr_supervisor_watch_write",
                    self.store.set_terminal,
                    watch.id,
                    PRWatchStatus.CLOSED,
                    state=state,
                    owner_instance_id=self.settings.instance_id,
                    fence_token=grant.fence_token,
                    retirement_reason="github_close_observed",
                )
                self._forget_watch(terminal.id)
                await self._audit(
                    terminal,
                    "watch_archived",
                    f"{terminal.id}:closed:archived",
                    payload={
                        "reason": "github_close_observed",
                        "retired_at": terminal.retired_at.isoformat(),
                        "terminal_status": terminal.status.value,
                    },
                )
                await self._replicate(terminal)
                await self._broadcast_retirement(terminal)
                return

            stable = self._predict_stable(watch, snapshot, now)
            gate = evaluate_gate(snapshot, watch.policy, stable_head=stable)
            changed = gate.fingerprint != watch.condition_fingerprint
            attempt = 0 if changed else min(watch.poll_attempt + 1, 16)
            next_poll = self._next_poll(watch.policy, attempt)
            observation_state = self._safe_snapshot(snapshot, gate)
            prior_hold = dict((watch.state or {}).get("publication_fence") or {})
            hold_active = bool(snapshot.draft)
            hold_version = int((watch.state or {}).get("review_hold_version") or 0)
            if hold_active != bool(prior_hold.get("active")):
                hold_version += 1
            observation_state["review_hold_version"] = hold_version
            observation_state["publication_fence"] = {
                "active": hold_active,
                "reason": "pull_request_draft" if hold_active else None,
                "source": "github_observation",
                "head_sha": snapshot.head_sha,
                "version": hold_version,
                "state": "paused_for_review" if hold_active else "released",
            }
            observation_state["policy_source"] = watch.policy_source
            observation_state["policy_revision"] = watch.policy_revision
            observation_state["policy_stale"] = False
            prior_repair = (watch.state or {}).get("repair_notification") or {}
            repair_epoch = int(prior_repair.get("epoch") or 0)
            repair_active = bool(gate.actionable and gate.repair_fingerprint)
            if repair_active and (
                not prior_repair.get("active")
                or prior_repair.get("fingerprint") != gate.repair_fingerprint
            ):
                repair_epoch += 1
            observation_state["repair_notification"] = {
                "active": repair_active,
                "fingerprint": gate.repair_fingerprint,
                "epoch": repair_epoch,
            }
            updated = await self._offload(
                "sqlite.pr_supervisor_observation_write",
                self.store.update_observation,
                watch.id,
                owner_instance_id=self.settings.instance_id,
                fence_token=grant.fence_token,
                head_sha=snapshot.head_sha,
                base_branch=snapshot.base_branch,
                state=observation_state,
                condition_fingerprint=gate.fingerprint,
                next_poll_at=next_poll,
                poll_attempt=attempt,
                now=now,
            )
            await self._audit(
                updated,
                "observation",
                f"{watch.id}:poll:{uuid4()}",
                head_sha=snapshot.head_sha,
                fingerprint=gate.fingerprint,
                payload={
                    "state": snapshot.state,
                    "draft": snapshot.draft,
                    "stable_head": stable,
                    "green": gate.green,
                    "actionable": gate.actionable,
                    "pending": gate.pending,
                    "reasons": gate.reasons,
                    "checks": [
                        {
                            "name": check.name,
                            "required": check.required,
                            "status": check.status,
                            "conclusion": check.conclusion,
                            "details_url": check.details_url,
                        }
                        for check in snapshot.checks
                    ],
                },
            )
            if gate.actionable and watch.policy.auto_notify:
                await self._notify(updated, snapshot, gate, green=False)
            elif (
                gate.green
                and watch.policy.auto_notify
                and watch.policy.agent_merge_on_green
            ):
                await self._notify(updated, snapshot, gate, green=True)
            await self._replicate(updated)
        except StaleFenceError:
            logger.info("PR supervisor lost fence watch=%s", watch.id)
            await self._offload(
                "sqlite.pr_supervisor_metric",
                self.store.increment_metric,
                "stale_fences",
            )
        except Exception as exc:  # noqa: BLE001
            delay = self._next_poll(watch.policy, watch.poll_attempt + 1)
            message = str(exc)
            logger.warning("PR supervisor poll failed watch=%s: %s", watch.id, message)
            try:
                errored = await self._offload(
                    "sqlite.pr_supervisor_watch_write",
                    self.store.mark_error,
                    watch.id,
                    message,
                    next_poll_at=delay,
                    owner_instance_id=self.settings.instance_id,
                    fence_token=grant.fence_token,
                )
                await self._audit(
                    watch,
                    "poll_error",
                    f"{watch.id}:error:{watch.poll_attempt + 1}:{uuid4()}",
                    payload={
                        "error": message[:1000],
                        "next_poll_at": delay.isoformat(),
                    },
                )
                await self._replicate(errored)
            except StaleFenceError:
                await self._offload(
                    "sqlite.pr_supervisor_metric",
                    self.store.increment_metric,
                    "stale_fences",
                )

    async def _notify(
        self,
        watch: PRWatch,
        snapshot: PRSnapshot,
        gate: GateResult,
        *,
        green: bool,
    ) -> None:
        kind = "green_for_agent_merge" if green else "action_required"
        repair = (watch.state or {}).get("repair_notification") or {}
        effect_fingerprint = gate.fingerprint if green else (
            gate.repair_fingerprint or gate.fingerprint
        )
        effect_version = watch.condition_version if green else int(repair.get("epoch") or 0)
        event_key = (
            f"{watch.id}:{effect_fingerprint}:{effect_version}:"
            f"{watch.originating_session_id or 'no-session'}:{kind}"
        )
        await self._offload(
            "sqlite.pr_supervisor_metric", self.store.increment_metric, "effect_intents"
        )
        await self._audit(
            watch,
            kind,
            event_key,
            head_sha=snapshot.head_sha,
            fingerprint=gate.fingerprint,
            payload={"reasons": gate.reasons},
        )
        prompt = build_executor_prompt_rendered(
            watch,
            snapshot,
            gate,
            green=green,
            provider=watch.originating_agent or "default",
        )
        try:
            state = await self._dispatch_authorized_effect(
                watch,
                event_key,
                prompt,
                effect_kind=kind,
            )
            logger.info(
                "PR supervisor executor dispatch watch=%s event=%s state=%s",
                watch.id,
                kind,
                state,
            )
            metric = (
                "effect_delivery_accepted"
                if state not in {"failed", "rejected"}
                else "effect_delivery_rejected"
            )
            await self._offload(
                "sqlite.pr_supervisor_metric", self.store.increment_metric, metric
            )
            await self._audit(
                watch,
                "effect_delivery_result",
                f"{event_key}:delivery:{state}:{uuid4()}",
                head_sha=snapshot.head_sha,
                fingerprint=effect_fingerprint,
                payload={
                    "effect_kind": kind,
                    "state": state,
                    "event_key": event_key,
                    "target_instance_id": watch.originating_instance_id,
                    "target_session_id": watch.originating_session_id,
                    "retryable": state in {"failed", "rejected"},
                },
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "PR supervisor executor dispatch failed watch=%s event=%s: %s",
                watch.id,
                kind,
                exc,
            )
            await self._offload(
                "sqlite.pr_supervisor_metric",
                self.store.increment_metric,
                "dispatch_errors",
            )
            await self._offload(
                "sqlite.pr_supervisor_metric",
                self.store.increment_metric,
                "effect_delivery_retries",
            )
            await self._audit(
                watch,
                "effect_delivery_result",
                f"{event_key}:delivery:error:{uuid4()}",
                head_sha=snapshot.head_sha,
                fingerprint=effect_fingerprint,
                payload={
                    "effect_kind": kind,
                    "state": "failed",
                    "reason": str(exc)[:500],
                    "event_key": event_key,
                    "target_instance_id": watch.originating_instance_id,
                    "target_session_id": watch.originating_session_id,
                    "retryable": True,
                },
            )

    async def _dispatch_authorized_effect(
        self,
        watch: PRWatch,
        event_key: str,
        prompt: RenderedPrompt,
        *,
        effect_kind: str,
    ) -> str:
        prompt_text = prompt.text
        payload = {
            "protocol_version": 2,
            "realm_id": watch.realm_id,
            "watch_id": watch.id,
            "repository": watch.repository,
            "pr_number": watch.pr_number,
            "head_sha": watch.head_sha,
            "condition_fingerprint": watch.condition_fingerprint,
            "condition_version": watch.condition_version,
            "effect_kind": effect_kind,
            "event_key": event_key,
            "content_digest": hashlib.sha256(prompt_text.encode()).hexdigest(),
            "prompt": prompt_text,
            "prompt_audit": [prompt.audit_record()],
            "owner_instance_id": watch.owner_instance_id,
            "fence_token": watch.fence_token,
            "lease_version": watch.lease_version,
            "target_instance_id": (
                watch.originating_instance_id or self.settings.instance_id
            ),
            "target_session_id": watch.originating_session_id,
            "policy_digest": hashlib.sha256(
                watch.policy.model_dump_json().encode()
            ).hexdigest(),
            "review_hold_version": int(
                (watch.state or {}).get("review_hold_version") or 0
            ),
        }
        authority = self._authority_url()
        if authority:
            result = await self._post_json(
                f"{authority}/api/pr-supervisor/effects/dispatch",
                payload,
            )
            return str(result.get("state") or "failed")
        return await self.authorize_and_dispatch_effect(
            payload, caller_instance_id=self.settings.instance_id
        )

    async def authorize_and_dispatch_effect(
        self, payload: dict[str, Any], *, caller_instance_id: str
    ) -> str:
        if int(payload.get("protocol_version") or 0) != PR_WATCH_PROTOCOL_VERSION:
            raise RuntimeError("pr_watch_effect_upgrade_required")
        prompt = str(payload.get("prompt") or "")
        effect_kind = str(payload.get("effect_kind") or "")
        event_key = str(payload.get("event_key") or "")
        if effect_kind not in {"action_required", "green_for_agent_merge"}:
            raise RuntimeError("unsupported_pr_watch_effect")
        if (
            not prompt
            or payload.get("content_digest")
            != hashlib.sha256(prompt.encode()).hexdigest()
        ):
            raise RuntimeError("pr_watch_effect_content_mismatch")
        watch_id = str(payload.get("watch_id") or "")
        current = await self._offload(
            "sqlite.pr_supervisor_watch_read", self.store.get_watch, watch_id
        )
        if not current or not current.actionable:
            raise StaleFenceError(f"inactive effect watch {watch_id}")
        if caller_instance_id != current.owner_instance_id:
            raise StaleFenceError(f"unauthenticated effect owner for {watch_id}")
        expected_target = current.originating_instance_id or self.settings.instance_id
        if (
            payload.get("target_instance_id") != expected_target
            or payload.get("target_session_id") != current.originating_session_id
            or not event_key.startswith(f"{watch_id}:")
            or not event_key.endswith(f":{effect_kind}")
        ):
            raise StaleFenceError(f"effect destination changed for {watch_id}")
        required_instances = {
            caller_instance_id,
            str(payload.get("target_instance_id") or ""),
        }
        compatible_instances = {
            item.instance_id
            for item in await self._offload(
                "sqlite.pr_supervisor_capability_read",
                self.store.list_capabilities,
                fresh_seconds=self.CAPABILITY_TTL_SECONDS,
            )
            if item.pr_watch_protocol_version >= PR_WATCH_PROTOCOL_VERSION
        }
        compatible_instances.add(self.capability.instance_id)
        if not required_instances <= compatible_instances:
            raise RuntimeError("pr_watch_effect_upgrade_required")
        expected_policy = hashlib.sha256(
            current.policy.model_dump_json().encode()
        ).hexdigest()
        if payload.get("policy_digest") != expected_policy or int(
            payload.get("review_hold_version") or 0
        ) != int((current.state or {}).get("review_hold_version") or 0):
            raise StaleFenceError(f"effect policy changed for {watch_id}")
        bindings = {
            key: payload.get(key)
            for key in (
                "realm_id",
                "watch_id",
                "repository",
                "pr_number",
                "head_sha",
                "condition_fingerprint",
                "condition_version",
                "effect_kind",
                "content_digest",
                "owner_instance_id",
                "fence_token",
                "lease_version",
                "target_instance_id",
                "target_session_id",
                "policy_digest",
                "review_hold_version",
            )
        }
        bindings["issuer_instance_id"] = self.settings.instance_id
        prepared, authorization = await self._offload(
            "sqlite.pr_supervisor_effect_prepare",
            self.store.prepare_effect_authorization,
            watch_id,
            owner_instance_id=current.owner_instance_id,
            fence_token=current.fence_token,
            lease_version=current.lease_version,
            event_key=str(payload.get("event_key") or ""),
            bindings=bindings,
        )
        if authorization.get("state") == "accepted":
            return "deduplicated"
        if not await self._replicate_strict(prepared):
            failed = await self._offload(
                "sqlite.pr_supervisor_effect_finish",
                self.store.finish_effect_authorization,
                watch_id,
                str(payload.get("event_key") or ""),
                str(authorization["id"]),
                accepted=False,
                detail="replication_unavailable",
            )
            await self._replicate(failed)
            raise RuntimeError("effect authorization replication unavailable")
        revalidated = await self._offload(
            "sqlite.pr_supervisor_watch_read", self.store.get_watch, watch_id
        )
        if (
            not revalidated
            or revalidated.owner_instance_id != authorization["owner_instance_id"]
            or revalidated.fence_token != authorization["fence_token"]
            or revalidated.lease_version != authorization["lease_version"]
            or revalidated.head_sha != authorization["head_sha"]
            or revalidated.condition_fingerprint
            != authorization["condition_fingerprint"]
            or revalidated.condition_version != authorization["condition_version"]
            or hashlib.sha256(revalidated.policy.model_dump_json().encode()).hexdigest()
            != authorization["policy_digest"]
            or int((revalidated.state or {}).get("review_hold_version") or 0)
            != authorization["review_hold_version"]
        ):
            raise StaleFenceError(f"effect changed before dispatch for {watch_id}")
        state = await self.dispatcher.dispatch(
            prepared,
            str(payload["event_key"]),
            str(payload["prompt"]),
            authorization=authorization,
            prompt_audit=list(payload.get("prompt_audit") or []),
        )
        accepted = state not in {"failed", "rejected"}
        finished = await self._offload(
            "sqlite.pr_supervisor_effect_finish",
            self.store.finish_effect_authorization,
            watch_id,
            str(payload["event_key"]),
            str(authorization["id"]),
            accepted=accepted,
            detail=state,
        )
        await self._replicate_strict(finished)
        return state

    async def _replicate_strict(self, watch: PRWatch) -> bool:
        # Authorization must be durable at the authority and fixed destination;
        # unrelated configured peers are not participants in this effect. Requiring
        # every peer made a disconnected fleet member veto otherwise safe delivery.
        urls: set[str] = set()
        authority = self._authority_url()
        if authority:
            urls.add(authority)
        target = watch.originating_instance_id
        if target and target != self.settings.instance_id:
            target_url = self.dispatcher._instance_url(target)
            if target_url:
                urls.add(target_url.rstrip("/"))
        if not urls:
            return True
        payload = {"watch": watch.model_dump(mode="json")}
        results = await asyncio.gather(
            *(
                self._post_json(f"{url}/api/pr-supervisor/replicas", payload)
                for url in urls
            ),
            return_exceptions=True,
        )
        return all(not isinstance(result, Exception) for result in results)

    async def _handle_merged(
        self, watch: PRWatch, snapshot: PRSnapshot, grant: LeaseGrant
    ) -> None:
        state = self._safe_snapshot(snapshot)
        prior_state = watch.state or {}
        prior_gate = prior_state.get("gate") or {}
        if (
            prior_state.get("head_sha") == snapshot.head_sha
            and prior_gate.get("green") is True
        ):
            state["stable_green_evidence"] = {
                "green": True,
                "head_sha": snapshot.head_sha,
                "fingerprint": watch.condition_fingerprint,
                "observed_at": prior_state.get("observed_at"),
                "observations": watch.stable_head_observations,
                "stable_head_since": watch.stable_head_since.isoformat()
                if watch.stable_head_since
                else None,
            }
        state["supervisor_state"] = "retired_after_merge"
        state["card_lane"] = "pending" if watch.card_id else None
        event_key = (
            f"{watch.id}:{snapshot.head_sha}:merged:"
            f"{snapshot.merge_commit_sha or 'unknown'}"
        )
        await self._audit(
            watch,
            "merged",
            event_key,
            head_sha=snapshot.head_sha,
            payload={
                "merge_commit_sha": snapshot.merge_commit_sha,
                "card_lane": "pending" if watch.card_id else None,
            },
        )
        terminal = await self._offload(
            "sqlite.pr_supervisor_watch_write",
            self.store.set_terminal,
            watch.id,
            PRWatchStatus.MERGED,
            state=state,
            owner_instance_id=self.settings.instance_id,
            fence_token=grant.fence_token,
            retirement_reason="github_merge_observed",
        )
        self._forget_watch(terminal.id)
        await self._audit(
            terminal,
            "watch_archived",
            f"{terminal.id}:merged:archived",
            payload={
                "reason": "github_merge_observed",
                "retired_at": terminal.retired_at.isoformat(),
                "terminal_status": terminal.status.value,
                "merge_commit_sha": snapshot.merge_commit_sha,
            },
        )
        await self._offload(
            "sqlite.pr_supervisor_metric",
            self.store.increment_metric,
            "merged_watches",
        )
        await self._replicate(terminal)
        await self._broadcast_retirement(terminal)
        await self._complete_merged_card(terminal)

    async def _reconcile_merged_cards(self) -> None:
        watches = await self._offload(
            "sqlite.pr_supervisor_watch_read",
            self.store.list_watches,
            include_retired=True,
        )
        for watch in watches:
            if (
                watch.status == PRWatchStatus.MERGED
                and watch.card_id
                and watch.state.get("card_lane") != "done"
            ):
                await self._complete_merged_card(watch)

    async def _complete_merged_card(self, watch: PRWatch | None) -> None:
        if not watch or not watch.card_id or watch.state.get("card_lane") == "done":
            return
        card = await self._offload(
            "sqlite.card_read",
            self.domain_store.get_card,
            watch.card_id,
            realm_id=watch.realm_id,
        )
        if not card:
            await self._audit(
                watch,
                "card_completion_failed",
                f"{watch.id}:card-completion:missing-card",
                payload={"error": "linked card is missing"},
            )
            await self._offload(
                "sqlite.pr_supervisor_metric",
                self.store.increment_metric,
                "card_completion_errors",
            )
            return
        linked_watches = await self._offload(
            "sqlite.pr_supervisor_watch_read",
            self.store.list_watches,
            realm_id=watch.realm_id,
            card_id=watch.card_id,
            include_retired=True,
        )
        decision = decide_card_disposition(
            disposition_for_merged_watch(watch),
            current_lane=card.lane,
            watches=linked_watches,
        )
        try:
            if decision.applied_lane != card.lane:
                await self._offload(
                    "sqlite.card_write",
                    self.domain_store.update_card,
                    watch.card_id,
                    CardUpdate(lane=decision.applied_lane),
                    realm_id=watch.realm_id,
                    principal_id="instance:pr-supervisor",
                    instance_id=self.settings.instance_id,
                )
        except Exception as exc:  # noqa: BLE001
            await self._audit(
                watch,
                "card_completion_failed",
                f"{watch.id}:card-completion:{watch.updated_at.isoformat()}",
                payload={"error": str(exc)[:1000]},
            )
            await self._offload(
                "sqlite.pr_supervisor_metric",
                self.store.increment_metric,
                "card_completion_errors",
            )
            return
        state = dict(watch.state)
        state["card_lane"] = decision.applied_lane.value
        state["card_disposition"] = {
            "contract": "pa.card-disposition/v1",
            "status": decision.status,
            "reason": decision.reason,
            "requested_lane": decision.requested_lane.value
            if decision.requested_lane
            else None,
            "applied_lane": decision.applied_lane.value,
            "watch_id": decision.watch_id,
        }
        completed = await self._offload(
            "sqlite.pr_supervisor_watch_write",
            self.store.set_terminal,
            watch.id,
            PRWatchStatus.MERGED,
            state=state,
        )
        self._forget_watch(watch.id)
        reason_hash = hashlib.sha256(decision.reason.encode()).hexdigest()[:12]
        event_type = (
            "card_completed"
            if decision.applied_lane == CardLane.DONE
            else "card_completion_blocked"
        )
        await self._audit(
            completed or watch,
            event_type,
            f"{watch.id}:card-disposition:{decision.status}:{reason_hash}",
            payload={
                "card_id": watch.card_id,
                "lane": decision.applied_lane.value,
                "status": decision.status,
                "reason": decision.reason,
            },
        )
        await self._replicate(completed)
        if decision.applied_lane == CardLane.DONE and self.workspace_manager:
            try:
                await self._offload(
                    "filesystem.workspace_completion",
                    self.workspace_manager.mark_card_completed,
                    watch.card_id,
                    merged=True,
                )
            except Exception:
                # Merged-card completion is authoritative; cleanup eligibility is
                # recoverable and must not roll the card back out of Done.
                logger.exception(
                    "Could not mark workspaces completed for card=%s", watch.card_id
                )

    def _predict_stable(self, watch: PRWatch, snapshot: PRSnapshot, now) -> bool:
        if watch.head_sha != snapshot.head_sha:
            since = now
            observations = 1
        else:
            since = watch.stable_head_since or now
            observations = watch.stable_head_observations + 1
        return (
            observations >= watch.policy.stable_observations
            and (now - since).total_seconds() >= watch.policy.stable_head_seconds
        )

    def _next_poll(self, policy: PRPolicy, attempt: int):
        seconds = min(
            policy.poll_max_seconds,
            policy.poll_min_seconds * (2 ** min(attempt, 10)),
        )
        jittered = max(1.0, seconds * self.rng.uniform(0.8, 1.2))
        return utcnow() + timedelta(seconds=jittered)

    def _safe_snapshot(
        self, snapshot: PRSnapshot, gate: GateResult | None = None
    ) -> dict[str, Any]:
        data = snapshot.model_dump(mode="json")
        if gate:
            data["gate"] = gate.model_dump(mode="json")
        return redact_external_value(data)

    async def _audit(
        self,
        watch: PRWatch,
        event_type: str,
        event_key: str,
        *,
        head_sha: str | None = None,
        fingerprint: str | None = None,
        source: str = "supervisor",
        payload: dict[str, Any] | None = None,
    ) -> bool:
        event = PRWatchEvent(
            watch_id=watch.id,
            event_key=event_key,
            event_type=event_type,
            head_sha=head_sha or watch.head_sha,
            condition_fingerprint=fingerprint,
            source=source,
            payload=redact_external_value(payload or {}),
        )
        return await self._offload(
            "sqlite.pr_supervisor_event_write",
            self.store.append_event,
            event,
        )

    async def _acquire_lease(
        self, watch: PRWatch, capability: GitHubCapability
    ) -> LeaseGrant:
        if not watch.actionable:
            self._forget_watch(watch.id)
            return LeaseGrant(
                acquired=False,
                fence_token=watch.fence_token,
                reason="watch_terminal" if watch.terminal else "watch_inactive",
                terminal_status=watch.status if watch.terminal else None,
            )
        if capability.pr_watch_protocol_version < PR_WATCH_PROTOCOL_VERSION:
            self._forget_watch(watch.id)
            reason = "protocol_upgrade_required"
            self._lease_suppressed[watch.id] = reason
            self._lease_last_response[watch.id] = {
                "reason": reason,
                "acquired": False,
                "terminal_status": None,
                "fence_token": watch.fence_token,
                "lease_version": watch.lease_version,
                "received_at": utcnow().isoformat(),
            }
            return LeaseGrant(
                acquired=False,
                fence_token=watch.fence_token,
                lease_version=watch.lease_version,
                reason=reason,
                protocol_version=PR_WATCH_PROTOCOL_VERSION,
            )
        authority = self._lease_authority_key()
        self._handle_authority_change(authority)
        now = self._monotonic()
        if watch.id in self._lease_suppressed:
            return LeaseGrant(acquired=False, reason=self._lease_suppressed[watch.id])
        cached = self._local_leases.get(watch.id)
        if cached and (
            cached.authority != authority
            or cached.grant.owner_instance_id != watch.owner_instance_id
            or cached.grant.fence_token != watch.fence_token
            or cached.grant.lease_version != watch.lease_version
        ):
            self._forget_watch(watch.id)
            cached = None
        if cached and now < cached.renew_at and now < cached.expires_at:
            return cached.grant
        retry_at = self._lease_retry_at.get(watch.id)
        if retry_at is not None and now < retry_at:
            return LeaseGrant(acquired=False, reason="lease_backoff")
        existing = self._lease_inflight.get(watch.id)
        if existing:
            return await self._await_lease_task(existing)
        task = asyncio.create_task(
            self._request_lease(watch, capability, authority),
            name=f"pa-pr-watch-lease:{watch.id}",
        )
        self._lease_inflight[watch.id] = task
        task.add_done_callback(
            lambda completed, watch_id=watch.id: self._lease_task_done(
                watch_id, completed
            )
        )
        return await self._await_lease_task(task)

    def _lease_task_done(self, watch_id: str, task: asyncio.Task[LeaseGrant]) -> None:
        if self._lease_inflight.get(watch_id) is task:
            self._lease_inflight.pop(watch_id, None)

    async def _await_lease_task(self, task: asyncio.Task[LeaseGrant]) -> LeaseGrant:
        try:
            # One cancelled loop/manual caller must not cancel the coalesced
            # request awaited by other callers.
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                raise
            return LeaseGrant(acquired=False, reason="watch_inactive")

    async def _request_lease(
        self,
        watch: PRWatch,
        capability: GitHubCapability,
        authority_key: str,
    ) -> LeaseGrant:
        remote = self._authority_url()
        request_started = self._monotonic()
        try:
            if remote:
                result = await self._post_json(
                    f"{remote}/api/pr-supervisor/watches/{watch.id}/lease",
                    {
                        "instance_id": self.settings.instance_id,
                        "ttl_seconds": self.LEASE_TTL_SECONDS,
                        "renewal_window_seconds": self.LEASE_RENEWAL_WINDOW_SECONDS,
                        "protocol_version": 2,
                        "capability": capability.model_dump(mode="json"),
                        "watch": watch.model_dump(mode="json"),
                    },
                )
                grant = LeaseGrant.model_validate(result)
            else:
                grant = await self._offload(
                    "sqlite.pr_supervisor_lease_write",
                    self.store.try_acquire_lease,
                    watch.id,
                    self.settings.instance_id,
                    ttl_seconds=self.LEASE_TTL_SECONDS,
                    renewal_window_seconds=self.LEASE_RENEWAL_WINDOW_SECONDS,
                    capability=capability,
                )
            self._authority_last_success_at = utcnow()
            self._authority_last_error = None
        except (httpx.HTTPError, RuntimeError, ValueError) as exc:
            self._authority_last_error = str(exc)[:500]
            delay = self._lease_failure_delay(watch)
            self._lease_retry_at[watch.id] = self._monotonic() + delay
            await self._offload(
                "sqlite.pr_supervisor_watch_write",
                self.store.mark_error,
                watch.id,
                f"Fleet lease authority unavailable: {exc}",
                next_poll_at=utcnow() + timedelta(seconds=delay),
                visible_state="lease_authority_unavailable",
            )
            return LeaseGrant(acquired=False, reason="authority_unavailable")

        response_received = self._monotonic()
        self._lease_last_response[watch.id] = {
            "reason": grant.reason,
            "acquired": grant.acquired,
            "terminal_status": (
                grant.terminal_status.value if grant.terminal_status else None
            ),
            "fence_token": grant.fence_token,
            "lease_version": grant.lease_version,
            "received_at": utcnow().isoformat(),
        }

        if (
            grant.reason
            in {
                "watch_terminal",
                "watch_merged",
                "watch_closed",
                "watch_retired",
                "watch_disabled",
                "watch_not_found",
            }
            or grant.terminal_status is not None
        ):
            status = grant.terminal_status or PRWatchStatus.RETIRED
            current = await self._offload(
                "sqlite.pr_supervisor_watch_read", self.store.get_watch, watch.id
            )
            if current and current.actionable:
                await self._offload(
                    "sqlite.pr_supervisor_watch_write",
                    self.store.set_terminal,
                    current.id,
                    status,
                    fence_token_baseline=grant.fence_token,
                    retirement_reason=f"lease_authority_{grant.reason or 'terminal'}",
                )
            self._forget_watch(watch.id)
            return grant

        if grant.acquired:
            if remote and grant.lease_seconds_remaining is None:
                # A legacy authority exposes only cross-host wall time. A skewed
                # clock can extend that grant by hours, so fail closed and never
                # cache or execute work from it.
                self._local_leases.pop(watch.id, None)
                self._lease_retry_at[watch.id] = (
                    response_received + self._lease_failure_delay(watch)
                )
                return LeaseGrant(
                    acquired=False,
                    owner_instance_id=grant.owner_instance_id,
                    fence_token=grant.fence_token,
                    lease_version=grant.lease_version,
                    reason="legacy_grant_uncacheable",
                    protocol_version=grant.protocol_version,
                )
            current = await self._offload(
                "sqlite.pr_supervisor_watch_read", self.store.get_watch, watch.id
            )
            if not current or not current.actionable:
                self._forget_watch(watch.id)
                return LeaseGrant(
                    acquired=False,
                    reason="watch_terminal"
                    if current and current.terminal
                    else "watch_inactive",
                    terminal_status=current.status
                    if current and current.terminal
                    else None,
                )
            remaining = grant.lease_seconds_remaining or 0.0
            conservative_seconds = max(
                0.0,
                remaining
                - (response_received - request_started)
                - self.LEASE_RESPONSE_SAFETY_SECONDS,
            )
            local_expiry = utcnow() + timedelta(seconds=conservative_seconds)
            if conservative_seconds <= 0:
                self._local_leases.pop(watch.id, None)
                return LeaseGrant(
                    acquired=False,
                    owner_instance_id=grant.owner_instance_id,
                    fence_token=grant.fence_token,
                    lease_version=grant.lease_version,
                    reason="grant_expired_in_transit",
                    protocol_version=grant.protocol_version,
                )
            if (
                current.owner_instance_id != grant.owner_instance_id
                or current.fence_token != grant.fence_token
                or current.lease_version != grant.lease_version
                or current.lease_expires_at != local_expiry
            ):
                current.owner_instance_id = grant.owner_instance_id
                current.fence_token = grant.fence_token
                current.lease_version = grant.lease_version
                current.lease_expires_at = local_expiry
                await self._offload(
                    "sqlite.pr_supervisor_watch_write",
                    self.store.upsert_watch,
                    current,
                    preserve_lease=False,
                )
            self._remember_lease(
                watch.id,
                authority_key,
                grant,
                request_started=request_started,
                response_received=response_received,
            )
            return grant

        self._local_leases.pop(watch.id, None)
        if grant.reason in {
            "capability_missing",
            "capability_identity_mismatch",
            "capability_ineligible",
            "protocol_upgrade_required",
            "watch_inactive",
        }:
            reason = grant.reason or "watch_inactive"
            self._forget_watch(watch.id)
            self._lease_suppressed[watch.id] = reason
            return grant
        if grant.reason == "owned":
            remaining = grant.lease_seconds_remaining or self.LOOP_SECONDS
            # A holder with more than the renewal safety window left is healthy.
            # Rechecking it sooner than one TTL only rediscovers each renewal;
            # near expiry we still probe promptly for takeover/failover.
            denial_delay = (
                max(remaining, float(self.LEASE_TTL_SECONDS))
                if remaining > self.LEASE_RENEWAL_WINDOW_SECONDS
                else max(self.LOOP_SECONDS, remaining)
            )
            self._lease_retry_at[watch.id] = (
                self._monotonic()
                + denial_delay
                + self.rng.uniform(0, self.LEASE_TAKEOVER_JITTER_SECONDS)
            )
            self._lease_failure_attempts.pop(watch.id, None)
        else:
            self._lease_retry_at[watch.id] = (
                self._monotonic() + self._lease_failure_delay(watch)
            )
        return grant

    def _remember_lease(
        self,
        watch_id: str,
        authority_key: str,
        grant: LeaseGrant,
        *,
        request_started: float,
        response_received: float,
    ) -> None:
        remaining = grant.lease_seconds_remaining or 0.0
        expires_at = request_started + max(
            0.0, remaining - self.LEASE_RESPONSE_SAFETY_SECONDS
        )
        jitter = self.rng.uniform(0, self.LEASE_RENEWAL_JITTER_SECONDS)
        renew_at = max(
            response_received,
            expires_at - self.LEASE_RENEWAL_WINDOW_SECONDS + jitter,
        )
        self._local_leases[watch_id] = _LocalLease(
            grant=grant,
            authority=authority_key,
            renew_at=renew_at,
            expires_at=expires_at,
        )
        self._lease_retry_at.pop(watch_id, None)
        self._lease_failure_attempts.pop(watch_id, None)

    def _lease_failure_delay(self, watch: PRWatch) -> float:
        attempt = min(self._lease_failure_attempts.get(watch.id, 0) + 1, 8)
        self._lease_failure_attempts[watch.id] = attempt
        base = max(self.LOOP_SECONDS, float(watch.policy.poll_min_seconds))
        cap = max(base, float(watch.policy.poll_max_seconds))
        return min(cap, base * (2 ** (attempt - 1))) + self.rng.uniform(0, base)

    def _lease_authority_key(self) -> str:
        return self._authority_url() or f"local:{self.settings.instance_id}"

    def _handle_authority_change(self, authority: str) -> None:
        if authority == self._lease_authority:
            return
        self._clear_all_lease_state()
        self._lease_authority = authority

    def _forget_watch(self, watch_id: str) -> None:
        self._local_leases.pop(watch_id, None)
        self._lease_retry_at.pop(watch_id, None)
        self._lease_failure_attempts.pop(watch_id, None)
        self._lease_suppressed.pop(watch_id, None)
        task = self._lease_inflight.pop(watch_id, None)
        if task and task is not asyncio.current_task() and not task.done():
            task.cancel()

    def _clear_all_lease_state(self) -> None:
        for watch_id in set(self._lease_inflight) | set(self._local_leases):
            self._forget_watch(watch_id)
        self._lease_retry_at.clear()
        self._lease_failure_attempts.clear()
        self._lease_suppressed.clear()

    def watch_state_changed(self, watch: PRWatch) -> None:
        """Converge scheduler state after a local or synced watch transition."""
        cached = self._local_leases.get(watch.id)
        if not watch.actionable or (
            cached is not None
            and (
                cached.grant.owner_instance_id != watch.owner_instance_id
                or cached.grant.fence_token != watch.fence_token
                or cached.grant.lease_version != watch.lease_version
            )
        ):
            self._forget_watch(watch.id)

    async def _prune_lease_state(self, capability: GitHubCapability) -> None:
        authority = self._lease_authority_key()
        self._handle_authority_change(authority)
        scheduled = (
            set(self._local_leases)
            | set(self._lease_retry_at)
            | set(self._lease_inflight)
        )
        for watch_id in scheduled:
            watch = await self._offload(
                "sqlite.pr_supervisor_watch_read", self.store.get_watch, watch_id
            )
            cached = self._local_leases.get(watch_id)
            if (
                not watch
                or not watch.actionable
                or not capability.supports(watch.repository)
                or (cached is not None and cached.authority != authority)
                or (
                    cached is not None
                    and (
                        cached.grant.owner_instance_id != watch.owner_instance_id
                        or cached.grant.fence_token != watch.fence_token
                        or cached.grant.lease_version != watch.lease_version
                    )
                )
            ):
                self._forget_watch(watch_id)

    async def _renew_due_local_leases(self, capability: GitHubCapability) -> None:
        now = self._monotonic()
        due_ids = [
            watch_id
            for watch_id, state in self._local_leases.items()
            if state.renew_at <= now
        ]
        for watch_id in due_ids:
            watch = await self._offload(
                "sqlite.pr_supervisor_watch_read", self.store.get_watch, watch_id
            )
            if not watch or not watch.actionable:
                self._forget_watch(watch_id)
                continue
            await self._acquire_lease(watch, capability)

    async def _heartbeat_authority(self, capability: GitHubCapability) -> None:
        authority = self._authority_url()
        if not authority:
            return
        try:
            await self._post_json(
                f"{authority}/api/pr-supervisor/instances/heartbeat",
                capability.model_dump(mode="json"),
            )
            self._authority_last_success_at = utcnow()
            self._authority_last_error = None
        except (httpx.HTTPError, RuntimeError) as exc:
            detail = str(exc).strip()
            self._authority_last_error = (
                f"{type(exc).__name__}: {detail}" if detail else type(exc).__name__
            )[:500]
            logger.warning(
                "PR supervisor capability heartbeat failed: %s",
                self._authority_last_error,
            )

    async def _eligible_capabilities(self, repository: str) -> list[GitHubCapability]:
        authority = self._authority_url()
        if authority:
            try:
                data = await self._get_json(
                    f"{authority}/api/pr-supervisor/capabilities"
                )
                capabilities = [
                    GitHubCapability.model_validate(item)
                    for item in data.get("instances", [])
                ]
                return [
                    capability
                    for capability in capabilities
                    if capability.supports(repository)
                ]
            except httpx.HTTPError, RuntimeError, ValueError:
                return []
        capabilities = await self._offload(
            "sqlite.pr_supervisor_capability_read",
            self.store.list_capabilities,
            fresh_seconds=self.CAPABILITY_TTL_SECONDS,
        )
        return [item for item in capabilities if item.supports(repository)]

    async def _replicate(self, watch: PRWatch | None) -> None:
        if not watch:
            return
        urls = self._fleet_urls()
        if not urls:
            return
        payload = {"watch": watch.model_dump(mode="json")}
        results = await asyncio.gather(
            *(
                self._post_json(f"{url}/api/pr-supervisor/replicas", payload)
                for url in urls
            ),
            return_exceptions=True,
        )
        failures = sum(1 for result in results if isinstance(result, Exception))
        if failures:
            await self._offload(
                "sqlite.pr_supervisor_metric",
                self.store.increment_metric,
                "replication_errors",
                failures,
            )

    async def _broadcast_retirement(self, watch: PRWatch) -> None:
        urls = self._fleet_urls()
        if not urls:
            return
        retired_at = watch.retired_at or watch.updated_at
        event_key = f"{watch.id}:retired:{retired_at.isoformat()}"
        results = await asyncio.gather(
            *(
                self._post_json(
                    f"{url}/api/pr-supervisor/retirements",
                    {
                        "watch": watch.model_dump(mode="json"),
                        "event_key": event_key,
                    },
                )
                for url in urls
            ),
            return_exceptions=True,
        )
        failures = sum(1 for result in results if isinstance(result, Exception))
        if failures:
            await self._offload(
                "sqlite.pr_supervisor_metric",
                self.store.increment_metric,
                "replication_errors",
                failures,
            )

    def _fleet_urls(self) -> set[str]:
        urls = set(self.settings.peers)
        authority = self._authority_url()
        if authority:
            urls.add(authority)
        local = (self.settings.instance_url or "").rstrip("/")
        return {url.rstrip("/") for url in urls if url and url.rstrip("/") != local}

    def _authority_url(self) -> str | None:
        authority = (
            self.settings.pr_supervisor_authority_url
            or self.settings.fleet_owner_url
            or ""
        ).rstrip("/")
        if not authority:
            return None
        local = (self.settings.instance_url or "").rstrip("/")
        if local and authority == local:
            return None
        return authority

    def authority_health(self) -> dict[str, Any]:
        """Secret-free control-plane state for operators and fleet health."""
        configured = (
            self.settings.pr_supervisor_authority_url
            or self.settings.fleet_owner_url
            or self.settings.instance_url
            or ""
        ).rstrip("/")
        remote = self._authority_url()
        all_watches = self.store.list_watches(include_retired=True)
        watches = [watch for watch in all_watches if watch.actionable]
        owned = [w for w in watches if w.owner_instance_id == self.settings.instance_id]
        archived = [watch for watch in all_watches if watch.retired_at is not None]
        backlog = [
            watch
            for watch in all_watches
            if watch.status in GITHUB_TERMINAL_PR_WATCH_STATUSES
            and watch.retired_at is None
        ]
        capabilities = self.store.list_capabilities(
            fresh_seconds=self.CAPABILITY_TTL_SECONDS
        )
        if not any(
            item.instance_id == self.settings.instance_id for item in capabilities
        ):
            capabilities.append(self.capability)
        incompatible = sorted(
            item.instance_id
            for item in capabilities
            if item.pr_watch_protocol_version < 2
        )
        if remote and self._authority_last_error:
            state = "authority_unreachable"
        elif remote and not self._authority_last_success_at:
            state = "authority_unverified"
        else:
            state = "ready"
        return {
            "state": state,
            "role": "worker" if remote else "lease_authority",
            "authority_url": configured or None,
            "explicit_authority": bool(self.settings.pr_supervisor_authority_url),
            "lease_ttl_seconds": self.LEASE_TTL_SECONDS,
            "lease_renewal_window_seconds": self.LEASE_RENEWAL_WINDOW_SECONDS,
            "lease_renewal_jitter_seconds": self.LEASE_RENEWAL_JITTER_SECONDS,
            "last_authority_success_at": (
                self._authority_last_success_at.isoformat()
                if self._authority_last_success_at
                else None
            ),
            "last_authority_error": self._authority_last_error,
            "active_watches": len(watches),
            "historical_watches": len(all_watches) - len(watches),
            "archived_watches": len(archived),
            "terminal_retirement_backlog": len(backlog),
            "locally_owned_watches": len(owned),
            "max_fence_token": max((w.fence_token for w in watches), default=0),
            "active_renewers": len(
                set(self._local_leases)
                | set(self._lease_retry_at)
                | set(self._lease_inflight)
            ),
            "retrying_renewers": [
                {
                    "watch_id": watch_id,
                    "retry_in_seconds": max(0.0, retry_at - self._monotonic()),
                    "last_response": self._lease_last_response.get(watch_id),
                }
                for watch_id, retry_at in sorted(self._lease_retry_at.items())
            ],
            "stopped_renewers": [
                {
                    "watch_id": watch_id,
                    "reason": reason,
                    "last_response": self._lease_last_response.get(watch_id),
                }
                for watch_id, reason in sorted(self._lease_suppressed.items())
            ],
            "effect_protocol_version": 2,
            "coordinated_upgrade_ready": not incompatible,
            "incompatible_effect_instances": incompatible,
        }

    async def _post_lease_json(
        self, url: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """Preserve structured terminal/no-op replies from mixed-version peers."""
        headers: dict[str, str] = {"X-PA-Origin-Instance-ID": self.settings.instance_id}
        if self.settings.sync_token:
            headers["Authorization"] = f"Bearer {self.settings.sync_token}"
        response = await self._observe(
            "http.pr_supervisor_lease",
            self.http_client.post(url, headers=headers, json=payload),
        )
        try:
            body = await self._offload("pr_supervisor.response_json", response.json)
        except ValueError:
            body = {}
        if response.status_code < 400:
            return body
        detail = body.get("detail") if isinstance(body, dict) else None
        detail = detail if isinstance(detail, dict) else body
        reason = str((detail or {}).get("reason") or (detail or {}).get("code") or "")
        status_value = (detail or {}).get("terminal_status") or (detail or {}).get(
            "status"
        )
        terminal_status = None
        try:
            terminal_status = PRWatchStatus(status_value) if status_value else None
        except ValueError:
            terminal_status = None
        terminal_reasons = {
            "watch_terminal",
            "watch_merged",
            "watch_closed",
            "watch_retired",
            "watch_disabled",
            "watch_inactive",
            "watch_not_found",
        }
        stop_reasons = {
            "capability_missing",
            "capability_identity_mismatch",
            "capability_ineligible",
            "protocol_upgrade_required",
        }
        if reason in stop_reasons:
            return LeaseGrant(
                acquired=False,
                reason=reason,
                protocol_version=int((detail or {}).get("protocol_version") or 1),
            ).model_dump(mode="json")
        if reason in terminal_reasons or terminal_status is not None:
            if reason == "watch_inactive":
                reason = "watch_retired"
            return LeaseGrant(
                acquired=False,
                fence_token=int((detail or {}).get("fence_token") or 0),
                lease_version=int((detail or {}).get("lease_version") or 0),
                reason=reason or "watch_terminal",
                terminal_status=terminal_status,
                protocol_version=int((detail or {}).get("protocol_version") or 1),
            ).model_dump(mode="json")
        raise RuntimeError(f"HTTP {response.status_code}: {reason or 'lease rejected'}")

    async def _post_json(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        if url.rstrip("/").endswith("/lease"):
            return await self._post_lease_json(url, payload)
        headers: dict[str, str] = {}
        if self.settings.sync_token:
            headers["Authorization"] = f"Bearer {self.settings.sync_token}"
        headers["X-PA-Origin-Instance-ID"] = self.settings.instance_id
        response = await self._observe(
            "http.pr_supervisor_peer",
            self.http_client.post(url, headers=headers, json=payload),
        )
        if response.status_code >= 400:
            raise RuntimeError(f"HTTP {response.status_code}")
        return await self._offload("pr_supervisor.response_json", response.json)

    async def _get_json(self, url: str) -> dict[str, Any]:
        headers: dict[str, str] = {}
        if self.settings.sync_token:
            headers["Authorization"] = f"Bearer {self.settings.sync_token}"
        headers["X-PA-Origin-Instance-ID"] = self.settings.instance_id
        response = await self._observe(
            "http.pr_supervisor_peer",
            self.http_client.get(url, headers=headers),
        )
        if response.status_code >= 400:
            raise RuntimeError(f"HTTP {response.status_code}")
        return await self._offload("pr_supervisor.response_json", response.json)

    async def migrate_discoverable_associations(self) -> int:
        """Migrate only explicit integration declarations, never prose URLs."""
        migrated = 0
        cards = await self._offload("sqlite.card_read", self.domain_store.list_cards)
        for card in cards:
            if card.lane == CardLane.DONE:
                continue
            for match in _EXPLICIT_PR_INTENT.finditer(card.body or ""):
                repository = match.group("repository")
                number = int(match.group("number"))
                if await self._offload(
                    "sqlite.pr_supervisor_watch_read",
                    self.store.find_watch,
                    card.realm_id,
                    repository,
                    number,
                ):
                    continue
                project = (
                    await self._offload(
                        "sqlite.project_read",
                        self.domain_store.get_project,
                        card.project_id,
                        realm_id=card.realm_id,
                    )
                    if card.project_id
                    else None
                )
                policy_data = dict(
                    (project.tool_config or {}).get("pr_policy", {}) if project else {}
                )
                if project:
                    repository_policies = (project.tool_config or {}).get(
                        "pr_repository_policies"
                    ) or {}
                    policy_data.update(repository_policies.get(repository, {}))
                await self.register_watch(
                    PRWatch(
                        realm_id=card.realm_id,
                        project_id=card.project_id,
                        card_id=card.id,
                        repository=repository,
                        pr_number=number,
                        pr_url=match.group("url"),
                        originating_instance_id=(
                            card.created_by_instance or self.settings.instance_id
                        ),
                        authority_instance_id=self.settings.instance_id,
                        provenance_version=1,
                        creation_reason="legacy_explicit_integration_intent",
                        qualifying_evidence=match.group(0).strip(),
                        policy=PRPolicy.model_validate(policy_data),
                    ),
                    source="legacy_explicit_integration_discovery",
                )
                migrated += 1
        if migrated:
            await self._offload(
                "sqlite.pr_supervisor_metric",
                self.store.increment_metric,
                "migrated_watches",
                migrated,
            )
        return migrated

    async def handle_webhook(
        self, event_name: str, delivery_id: str, payload: dict[str, Any]
    ) -> int:
        repository = str((payload.get("repository") or {}).get("full_name") or "")
        pr = payload.get("pull_request") or {}
        candidates = (
            (payload.get("check_run") or {}).get("pull_requests")
            or (payload.get("check_suite") or {}).get("pull_requests")
            or (payload.get("workflow_run") or {}).get("pull_requests")
            or []
        )
        if not pr and candidates:
            pr = candidates[0] or {}
        number = int(pr.get("number") or payload.get("number") or 0)
        if not repository or not number:
            return 0
        supported = {
            "pull_request",
            "pull_request_review",
            "pull_request_review_comment",
            "check_run",
            "check_suite",
            "status",
            "workflow_run",
        }
        if event_name not in supported:
            return 0
        watches = await self._offload(
            "sqlite.pr_supervisor_watch_read",
            self.store.find_watches,
            repository,
            number,
        )
        count = 0
        for watch in watches:
            count += await self._offload(
                "sqlite.pr_supervisor_watch_write",
                self.store.schedule_now,
                watch_id=watch.id,
            )
            await self._audit(
                watch,
                "webhook_received",
                f"{watch.id}:webhook:{delivery_id}",
                source="github_webhook",
                payload={"event": event_name, "action": payload.get("action")},
            )
            current = await self._offload(
                "sqlite.pr_supervisor_watch_read", self.store.get_watch, watch.id
            )
            await self._replicate(current)
        await self._offload(
            "sqlite.pr_supervisor_metric",
            self.store.increment_metric,
            "webhooks",
        )
        return count


def repository_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    if parsed.hostname != "github.com":
        return None
    parts = parsed.path.strip("/").removesuffix(".git").split("/")
    return "/".join(parts[:2]) if len(parts) >= 2 else None
