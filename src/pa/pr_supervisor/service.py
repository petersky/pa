from __future__ import annotations

import asyncio
import logging
import random
import re
from datetime import timedelta
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

import httpx

from pa.agent.context import augment_message_with_context
from pa.domain.models import CardLane, CardUpdate
from pa.pr_supervisor.gating import (
    build_executor_prompt,
    evaluate_gate,
    redact_external_value,
)
from pa.pr_supervisor.github import GitHubClient, GitHubCredentials
from pa.pr_supervisor.models import (
    GateResult,
    GitHubCapability,
    LeaseGrant,
    PRPolicy,
    PRSnapshot,
    PRWatch,
    PRWatchEvent,
    PRWatchStatus,
    utcnow,
)
from pa.pr_supervisor.store import PRSupervisorStore, StaleFenceError

logger = logging.getLogger(__name__)

_PR_URL = re.compile(
    r"https://github\.com/(?P<repository>[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)/pull/(?P<number>\d+)"
)


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
    ) -> None:
        self.settings = settings
        self.domain_store = domain_store
        self.store = supervisor_store
        self.agent = agent_manager
        self.fleet = fleet_registry
        self.peer_table = peer_table
        self.http_client = http_client

    async def dispatch(self, watch: PRWatch, event_key: str, prompt: str) -> str:
        target = watch.originating_instance_id
        if target and target != self.settings.instance_id:
            url = self._instance_url(target)
            if url:
                try:
                    result = await self._remote_dispatch(
                        url, watch, event_key, prompt
                    )
                    return str(result.get("state") or "queued")
                except (httpx.HTTPError, RuntimeError) as exc:
                    logger.warning(
                        "PR supervisor remote executor unavailable watch=%s target=%s: %s",
                        watch.id,
                        target,
                        exc,
                    )
        return await self.dispatch_local(watch, event_key, prompt)

    async def _remote_dispatch(
        self, url: str, watch: PRWatch, event_key: str, prompt: str
    ) -> dict[str, Any]:
        headers: dict[str, str] = {}
        if self.settings.sync_token:
            headers["Authorization"] = f"Bearer {self.settings.sync_token}"
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
                },
            )
            if response.status_code >= 400:
                raise RuntimeError(
                    f"executor dispatch returned HTTP {response.status_code}"
                )
            return response.json()
        finally:
            if owns:
                await client.aclose()

    async def dispatch_local(
        self, watch: PRWatch, event_key: str, prompt: str
    ) -> str:
        if not self.store.claim_dispatch(
            event_key,
            watch.id,
            target_instance_id=self.settings.instance_id,
            target_session_id=watch.originating_session_id,
        ):
            return "deduplicated"
        if not self.agent:
            self.store.finish_dispatch(
                event_key, state="failed", detail="instance agent unavailable"
            )
            raise RuntimeError("instance agent unavailable")
        try:
            runtime = None
            session = None
            if watch.originating_session_id:
                runtime = self.agent.get(watch.originating_session_id)
                session = self.domain_store.get_session(
                    watch.originating_session_id
                )
            if runtime is None and watch.card_id:
                for candidate in self.agent.list_runtimes():
                    if candidate.session.card_id == watch.card_id:
                        runtime = candidate
                        break
                if session is None:
                    session = self.domain_store.get_session_by_label(
                        f"card:{watch.card_id}"
                    )
            if runtime is None and session and session.status != "closed":
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
                except Exception:
                    logger.exception(
                        "Could not resume executor session %s; creating replacement",
                        session.id,
                    )
            if runtime is None:
                project = (
                    self.domain_store.get_project(
                        watch.project_id, realm_id=watch.realm_id
                    )
                    if watch.project_id
                    else None
                )
                runtime = await self.agent.create_session(
                    label=f"card:{watch.card_id}" if watch.card_id else "pr-supervisor",
                    title=f"PR #{watch.pr_number} executor",
                    cwd=watch.executor_cwd,
                    principal_id="user:local",
                    card_id=watch.card_id,
                    project_id=watch.project_id,
                    project_tool_config=project.tool_config if project else None,
                    surface="execution",
                )
            message = augment_message_with_context(
                self.domain_store,
                prompt,
                card_id=watch.card_id,
                project_id=watch.project_id,
                realm_id=watch.realm_id,
            )
            runtime.enqueue(
                message,
                action="append",
                card_id=watch.card_id,
                project_id=watch.project_id,
                principal_id=runtime.session.principal_id or "user:local",
                cwd=watch.executor_cwd or runtime.session.cwd,
                source="pr-supervisor",
            )
            self.store.finish_dispatch(
                event_key, state="queued", detail=runtime.session_id
            )
            self.store.increment_metric("executor_prompts")
            return "queued"
        except Exception as exc:
            self.store.finish_dispatch(event_key, state="failed", detail=str(exc))
            raise

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
    LOOP_SECONDS = 2.0
    CAPABILITY_TTL_SECONDS = 120

    def __init__(
        self,
        settings,
        domain_store,
        *,
        supervisor_store: PRSupervisorStore | None = None,
        github_client: GitHubClient | None = None,
        dispatcher: ExecutorDispatcher | None = None,
        agent_manager=None,
        fleet_registry=None,
        peer_table=None,
        http_client: httpx.AsyncClient | None = None,
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
        self.github = github_client or GitHubClient(self.credentials)
        self.http_client = http_client
        self.dispatcher = dispatcher or ExecutorDispatcher(
            settings,
            domain_store,
            self.store,
            agent_manager=agent_manager,
            fleet_registry=fleet_registry,
            peer_table=peer_table,
            http_client=http_client,
        )
        self.rng = rng or random.Random()
        self._task: asyncio.Task | None = None
        self._stopping = False
        self._capability: GitHubCapability | None = None
        self._capability_checked_at = None

    @property
    def capability(self) -> GitHubCapability:
        return self._capability or self.credentials.capability(
            self.settings.instance_id
        )

    async def start(self) -> None:
        self._stopping = False
        await self.refresh_capability(force=True)
        await self.migrate_discoverable_associations()
        if not self._task or self._task.done():
            self._task = asyncio.create_task(
                self._run_loop(), name="pa-pr-supervisor"
            )

    async def stop(self) -> None:
        self._stopping = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run_loop(self) -> None:
        while not self._stopping:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("PR supervisor loop failed")
                self.store.increment_metric("loop_errors")
            await asyncio.sleep(self.LOOP_SECONDS)

    async def refresh_capability(self, *, force: bool = False) -> GitHubCapability:
        now = utcnow()
        if (
            not force
            and self._capability
            and self._capability_checked_at
            and now - self._capability_checked_at < timedelta(seconds=60)
        ):
            return self._capability
        self.credentials = GitHubCredentials.load(self.settings.data_dir)
        self.github.credentials = self.credentials
        self._capability = await self.github.probe(self.settings.instance_id)
        self._capability_checked_at = now
        self.store.save_capability(self._capability)
        await self._heartbeat_authority(self._capability)
        return self._capability

    async def run_once(self) -> None:
        capability = await self.refresh_capability()
        due = self.store.list_due()
        if not due:
            return
        for watch in due:
            if not capability.supports(watch.repository):
                eligible = await self._eligible_capabilities(watch.repository)
                if not eligible:
                    next_poll = utcnow() + timedelta(
                        seconds=watch.policy.poll_max_seconds
                    )
                    self.store.mark_error(
                        watch.id,
                        "No eligible authenticated PA instance can access this repository",
                        next_poll_at=next_poll,
                        visible_state="no_eligible_authenticated_instance",
                    )
                    self._audit(
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
            current = self.store.get_watch(watch.id)
            if current:
                await self._process_watch(current, grant)

    async def register_watch(
        self, watch: PRWatch, *, source: str = "api", replicate: bool = True
    ) -> PRWatch:
        if not watch.originating_instance_id:
            watch.originating_instance_id = self.settings.instance_id
        if not watch.required_capabilities:
            watch.required_capabilities = [
                "pr-supervisor",
                "github:authenticated",
                f"github:repo:{watch.repository}",
            ]
        stored = self.store.upsert_watch(watch)
        self._audit(
            stored,
            "watch_created",
            f"{stored.id}:created",
            source=source,
            payload={
                "repository": stored.repository,
                "pr_number": stored.pr_number,
                "card_id": stored.card_id,
                "project_id": stored.project_id,
                "originating_instance_id": stored.originating_instance_id,
                "originating_session_id": stored.originating_session_id,
                "policy": stored.policy.model_dump(mode="json"),
            },
        )
        if replicate:
            await self._replicate(stored)
        return stored

    async def _process_watch(self, watch: PRWatch, grant: LeaseGrant) -> None:
        now = utcnow()
        try:
            snapshot = await self.github.snapshot(
                watch.repository, watch.pr_number, policy=watch.policy
            )
            self.store.increment_metric("polls")
            if snapshot.stale:
                next_poll = now + timedelta(seconds=watch.policy.poll_min_seconds)
                self._audit(
                    watch,
                    "stale_head_discarded",
                    f"{watch.id}:stale:{snapshot.head_sha}:{snapshot.confirmed_head_sha}",
                    head_sha=snapshot.head_sha,
                    payload={
                        "observed_head": snapshot.head_sha,
                        "confirmed_head": snapshot.confirmed_head_sha,
                    },
                )
                self.store.mark_error(
                    watch.id,
                    "Head changed during observation; stale result discarded",
                    next_poll_at=next_poll,
                    owner_instance_id=self.settings.instance_id,
                    fence_token=grant.fence_token,
                    visible_state="stale_head_repoll",
                )
                await self._replicate(self.store.get_watch(watch.id))
                return
            if snapshot.merged:
                await self._handle_merged(watch, snapshot, grant)
                return
            if snapshot.closed:
                state = self._safe_snapshot(snapshot)
                self._audit(
                    watch,
                    "pull_request_closed",
                    f"{watch.id}:{snapshot.head_sha}:closed",
                    head_sha=snapshot.head_sha,
                    payload={"url": snapshot.url},
                )
                terminal = self.store.set_terminal(
                    watch.id,
                    PRWatchStatus.CLOSED,
                    state=state,
                    owner_instance_id=self.settings.instance_id,
                    fence_token=grant.fence_token,
                )
                await self._replicate(terminal)
                return

            stable = self._predict_stable(watch, snapshot, now)
            gate = evaluate_gate(snapshot, watch.policy, stable_head=stable)
            changed = gate.fingerprint != watch.condition_fingerprint
            attempt = 0 if changed else min(watch.poll_attempt + 1, 16)
            next_poll = self._next_poll(watch.policy, attempt)
            updated = self.store.update_observation(
                watch.id,
                owner_instance_id=self.settings.instance_id,
                fence_token=grant.fence_token,
                head_sha=snapshot.head_sha,
                base_branch=snapshot.base_branch,
                state=self._safe_snapshot(snapshot, gate),
                condition_fingerprint=gate.fingerprint,
                next_poll_at=next_poll,
                poll_attempt=attempt,
                now=now,
            )
            self._audit(
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
            self.store.increment_metric("stale_fences")
        except Exception as exc:
            delay = self._next_poll(watch.policy, watch.poll_attempt + 1)
            message = str(exc)
            logger.warning("PR supervisor poll failed watch=%s: %s", watch.id, message)
            try:
                errored = self.store.mark_error(
                    watch.id,
                    message,
                    next_poll_at=delay,
                    owner_instance_id=self.settings.instance_id,
                    fence_token=grant.fence_token,
                )
                self._audit(
                    watch,
                    "poll_error",
                    f"{watch.id}:error:{watch.poll_attempt + 1}:{uuid4()}",
                    payload={"error": message[:1000], "next_poll_at": delay.isoformat()},
                )
                await self._replicate(errored)
            except StaleFenceError:
                self.store.increment_metric("stale_fences")

    async def _notify(
        self,
        watch: PRWatch,
        snapshot: PRSnapshot,
        gate: GateResult,
        *,
        green: bool,
    ) -> None:
        kind = "green_for_agent_merge" if green else "action_required"
        event_key = (
            f"{watch.id}:{snapshot.head_sha}:{gate.fingerprint}:"
            f"{watch.condition_version}:{kind}"
        )
        self._audit(
            watch,
            kind,
            event_key,
            head_sha=snapshot.head_sha,
            fingerprint=gate.fingerprint,
            payload={"reasons": gate.reasons},
        )
        prompt = build_executor_prompt(
            watch, snapshot, gate, green=green
        )
        try:
            state = await self.dispatcher.dispatch(watch, event_key, prompt)
            logger.info(
                "PR supervisor executor dispatch watch=%s event=%s state=%s",
                watch.id,
                kind,
                state,
            )
        except Exception as exc:
            logger.warning(
                "PR supervisor executor dispatch failed watch=%s event=%s: %s",
                watch.id,
                kind,
                exc,
            )
            self.store.increment_metric("dispatch_errors")

    async def _handle_merged(
        self, watch: PRWatch, snapshot: PRSnapshot, grant: LeaseGrant
    ) -> None:
        state = self._safe_snapshot(snapshot)
        state["supervisor_state"] = "retired_after_merge"
        if watch.card_id:
            self.domain_store.update_card(
                watch.card_id,
                CardUpdate(lane=CardLane.DONE),
                realm_id=watch.realm_id,
                principal_id="instance:pr-supervisor",
                instance_id=self.settings.instance_id,
            )
        gate = evaluate_gate(snapshot, watch.policy, stable_head=True)
        event_key = (
            f"{watch.id}:{snapshot.head_sha}:merged:"
            f"{snapshot.merge_commit_sha or 'unknown'}"
        )
        self._audit(
            watch,
            "merged",
            event_key,
            head_sha=snapshot.head_sha,
            payload={
                "merge_commit_sha": snapshot.merge_commit_sha,
                "card_lane": "done" if watch.card_id else None,
            },
        )
        terminal = self.store.set_terminal(
            watch.id,
            PRWatchStatus.MERGED,
            state=state,
            owner_instance_id=self.settings.instance_id,
            fence_token=grant.fence_token,
        )
        self.store.increment_metric("merged_watches")
        await self._replicate(terminal)
        prompt = build_executor_prompt(
            watch, snapshot, gate, green=False, merged=True
        )
        try:
            await self.dispatcher.dispatch(watch, event_key, prompt)
        except Exception:
            logger.exception("Could not notify executor after merge watch=%s", watch.id)

    def _predict_stable(
        self, watch: PRWatch, snapshot: PRSnapshot, now
    ) -> bool:
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

    def _audit(
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
        return self.store.append_event(
            PRWatchEvent(
                watch_id=watch.id,
                event_key=event_key,
                event_type=event_type,
                head_sha=head_sha or watch.head_sha,
                condition_fingerprint=fingerprint,
                source=source,
                payload=redact_external_value(payload or {}),
            )
        )

    async def _acquire_lease(
        self, watch: PRWatch, capability: GitHubCapability
    ) -> LeaseGrant:
        authority = self._authority_url()
        if not authority:
            return self.store.try_acquire_lease(
                watch.id,
                self.settings.instance_id,
                ttl_seconds=self.LEASE_TTL_SECONDS,
                capability=capability,
            )
        try:
            result = await self._post_json(
                f"{authority}/api/pr-supervisor/watches/{watch.id}/lease",
                {
                    "instance_id": self.settings.instance_id,
                    "ttl_seconds": self.LEASE_TTL_SECONDS,
                    "capability": capability.model_dump(mode="json"),
                    "watch": watch.model_dump(mode="json"),
                },
            )
            grant = LeaseGrant.model_validate(result)
            if grant.acquired:
                watch.owner_instance_id = grant.owner_instance_id
                watch.fence_token = grant.fence_token
                watch.lease_expires_at = grant.expires_at
                self.store.upsert_watch(watch, preserve_lease=False)
            return grant
        except (httpx.HTTPError, RuntimeError, ValueError) as exc:
            self.store.mark_error(
                watch.id,
                f"Fleet lease authority unavailable: {exc}",
                next_poll_at=utcnow()
                + timedelta(seconds=watch.policy.poll_min_seconds),
                visible_state="lease_authority_unavailable",
            )
            return LeaseGrant(acquired=False, reason="authority_unavailable")

    async def _heartbeat_authority(
        self, capability: GitHubCapability
    ) -> None:
        authority = self._authority_url()
        if not authority:
            return
        try:
            await self._post_json(
                f"{authority}/api/pr-supervisor/instances/heartbeat",
                capability.model_dump(mode="json"),
            )
        except (httpx.HTTPError, RuntimeError):
            logger.warning("PR supervisor capability heartbeat failed")

    async def _eligible_capabilities(
        self, repository: str
    ) -> list[GitHubCapability]:
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
            except (httpx.HTTPError, RuntimeError, ValueError):
                return []
        return [
            capability
            for capability in self.store.list_capabilities(
                fresh_seconds=self.CAPABILITY_TTL_SECONDS
            )
            if capability.supports(repository)
        ]

    async def _replicate(self, watch: PRWatch | None) -> None:
        if not watch:
            return
        urls = set(self.settings.peers)
        authority = self._authority_url()
        if authority:
            urls.add(authority)
        local = (self.settings.instance_url or "").rstrip("/")
        urls = {url.rstrip("/") for url in urls if url and url.rstrip("/") != local}
        if not urls:
            return
        payload = {"watch": watch.model_dump(mode="json")}
        results = await asyncio.gather(
            *(
                self._post_json(
                    f"{url}/api/pr-supervisor/replicas", payload
                )
                for url in urls
            ),
            return_exceptions=True,
        )
        failures = sum(1 for result in results if isinstance(result, Exception))
        if failures:
            self.store.increment_metric("replication_errors", failures)

    def _authority_url(self) -> str | None:
        authority = (self.settings.fleet_owner_url or "").rstrip("/")
        if not authority:
            return None
        local = (self.settings.instance_url or "").rstrip("/")
        if local and authority == local:
            return None
        return authority

    async def _post_json(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        headers: dict[str, str] = {}
        if self.settings.sync_token:
            headers["Authorization"] = f"Bearer {self.settings.sync_token}"
        owns = self.http_client is None
        client = self.http_client or httpx.AsyncClient(timeout=15.0)
        try:
            response = await client.post(url, headers=headers, json=payload)
            if response.status_code >= 400:
                raise RuntimeError(f"HTTP {response.status_code}")
            return response.json()
        finally:
            if owns:
                await client.aclose()

    async def _get_json(self, url: str) -> dict[str, Any]:
        headers: dict[str, str] = {}
        if self.settings.sync_token:
            headers["Authorization"] = f"Bearer {self.settings.sync_token}"
        owns = self.http_client is None
        client = self.http_client or httpx.AsyncClient(timeout=15.0)
        try:
            response = await client.get(url, headers=headers)
            if response.status_code >= 400:
                raise RuntimeError(f"HTTP {response.status_code}")
            return response.json()
        finally:
            if owns:
                await client.aclose()

    async def migrate_discoverable_associations(self) -> int:
        migrated = 0
        for card in self.domain_store.list_cards():
            if card.lane == CardLane.DONE:
                continue
            for match in _PR_URL.finditer(card.body or ""):
                repository = match.group("repository")
                number = int(match.group("number"))
                if self.store.find_watch(card.realm_id, repository, number):
                    continue
                project = (
                    self.domain_store.get_project(
                        card.project_id, realm_id=card.realm_id
                    )
                    if card.project_id
                    else None
                )
                policy_data = (
                    (project.tool_config or {}).get("pr_policy", {})
                    if project
                    else {}
                )
                await self.register_watch(
                    PRWatch(
                        realm_id=card.realm_id,
                        project_id=card.project_id,
                        card_id=card.id,
                        repository=repository,
                        pr_number=number,
                        pr_url=match.group(0),
                        originating_instance_id=card.created_by_instance,
                        policy=PRPolicy.model_validate(policy_data),
                    ),
                    source="migration",
                )
                migrated += 1
        if migrated:
            self.store.increment_metric("migrated_watches", migrated)
        return migrated

    async def handle_webhook(
        self, event_name: str, delivery_id: str, payload: dict[str, Any]
    ) -> int:
        repository = str(
            (payload.get("repository") or {}).get("full_name") or ""
        )
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
        count = self.store.schedule_now(
            repository=repository, pr_number=number
        )
        watch = self.store.find_watch(
            self.settings.primary_realm, repository, number
        )
        if watch:
            self._audit(
                watch,
                "webhook_received",
                f"{watch.id}:webhook:{delivery_id}",
                source="github_webhook",
                payload={"event": event_name, "action": payload.get("action")},
            )
        self.store.increment_metric("webhooks")
        return count


def repository_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    if parsed.hostname != "github.com":
        return None
    parts = parsed.path.strip("/").removesuffix(".git").split("/")
    return "/".join(parts[:2]) if len(parts) >= 2 else None
