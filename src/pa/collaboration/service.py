from __future__ import annotations

import inspect
import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from pa.acp.configuration import SessionConfigurationRequest
from pa.collaboration.commands import (
    advertised_collaboration_modes,
    build_catalog,
)
from pa.collaboration.models import (
    CollaborationMode,
    CollaborationPolicy,
    CollaborationState,
    CommandAvailability,
    CommandCatalog,
    CommandResult,
    CommandResultStatus,
    ExecuteCommandRequest,
    ModeTransitionRequest,
    ModeTransitionResult,
    PolicyDecision,
    PolicyInput,
    TransitionStatus,
)
from pa.collaboration.policy import (
    applicable_policies,
    decide_initial_mode,
    transition_allowed,
)
from pa.collaboration.store import (
    CollaborationStore,
    IdempotencyConflict,
    request_fingerprint,
)


def _current_mode(session: Any) -> CollaborationMode:
    config = dict(getattr(session, "config_json", {}) or {})
    collaboration = dict(config.get("collaboration") or {})
    # ``values`` is the provider-confirmed configuration projection.  The
    # collaboration block is PA's policy/audit metadata and may lag a direct,
    # authenticated user configuration change across a restart.
    value = dict(config.get("values") or {}).get("collaboration_mode")
    if value is None:
        value = collaboration.get("current_mode")
    try:
        return CollaborationMode(str(value).lower())
    except ValueError, TypeError:
        return CollaborationMode.DEFAULT


class CollaborationService:
    def __init__(self, settings: Any, domain_store: Any) -> None:
        self.settings = settings
        self.domain_store = domain_store
        self.store = CollaborationStore(settings.data_dir)
        self.manager: Any | None = None
        self.notifier: Any | None = None

    def bind_runtime(self, manager: Any, *, notifier: Any | None = None) -> None:
        self.manager = manager
        self.notifier = notifier

    def resolve_dispatch_policy(
        self,
        value: PolicyInput,
        *,
        session_id: str | None = None,
        dispatch_id: str | None = None,
        card_id: str | None = None,
    ) -> PolicyDecision:
        decision = decide_initial_mode(value, self.store.list_policies())
        self.store.record_decision(
            decision,
            session_id=session_id,
            dispatch_id=dispatch_id,
            card_id=card_id,
        )
        return decision

    def save_policy(
        self, policy: CollaborationPolicy, *, expected_version: int | None = None
    ) -> CollaborationPolicy:
        return self.store.save_policy(policy, expected_version=expected_version)

    def effective_policy(self, session: Any) -> CollaborationPolicy | None:
        card = (
            self.domain_store.get_card(session.card_id, realm_id=session.realm_id)
            if session.card_id
            else None
        )
        supported = self.supported_modes(session)
        value = PolicyInput(
            realm_id=session.realm_id,
            project_id=session.project_id,
            instance_id=self.settings.instance_id,
            provider=session.agent_name,
            card_id=session.card_id,
            card_kind=(
                card.kind.value
                if card and hasattr(card.kind, "value")
                else str(card.kind)
                if card
                else None
            ),
            card_tags=list(card.tags if card else []),
            dispatch_intent="automatic" if session.dispatch_id else "manual",
            risk="high" if card and "high-risk" in card.tags else "low",
            ambiguous=bool(card and "ambiguous" in card.tags),
            supported_modes=supported,
        )
        matched = applicable_policies(self.store.list_policies(), value)
        return matched[0] if matched else None

    def capture_available_commands(
        self,
        session: Any,
        commands: list[Any],
        *,
        connection_generation: int,
    ) -> CommandCatalog:
        catalog = build_catalog(
            session_id=session.id,
            provider=session.agent_name,
            generation=self.store.next_catalog_generation(session.id),
            connection_generation=connection_generation,
            provider_commands=commands,
        )
        return self.store.save_catalog(catalog)

    def catalog(self, session_id: str) -> CommandCatalog | None:
        return self.store.active_catalog(session_id)

    def ensure_catalog(self, session: Any) -> CommandCatalog:
        catalog = self.store.active_catalog(session.id)
        if catalog is not None:
            return catalog
        return self.store.save_catalog(
            build_catalog(
                session_id=session.id,
                provider=session.agent_name,
                generation=self.store.next_catalog_generation(session.id),
                connection_generation=1,
                provider_commands=[],
            )
        )

    def supported_modes(self, session: Any) -> list[CollaborationMode]:
        runtime = self.manager.get(session.id) if self.manager else None
        options = None
        if runtime and runtime.connection:
            options = runtime.connection.config_options
        if options is None:
            options = list((session.config_json or {}).get("options") or [])
        result = advertised_collaboration_modes(
            config_options=options,
            catalog=self.store.active_catalog(session.id),
        )
        if not result:
            # Legacy and non-Codex providers remain compatible and do not gain
            # Plan support merely because PA itself knows that mode name.
            result = [CollaborationMode.DEFAULT]
        current = _current_mode(session)
        if current not in result:
            result.insert(0, current)
        return result

    def state(self, session: Any) -> CollaborationState:
        pending = self.store.pending_mode_request(session.id)
        decision = self.store.latest_decision(
            session_id=session.id, dispatch_id=session.dispatch_id
        )
        catalog = self.store.active_catalog(session.id)
        return CollaborationState(
            session_id=session.id,
            supported_modes=self.supported_modes(session),
            current_mode=_current_mode(session),
            pending_transition=pending[1] if pending else None,
            effective_policy=self.effective_policy(session),
            policy_decision=decision,
            command_catalog_generation=catalog.generation if catalog else None,
            provider=session.agent_name,
            execution_mode_id=session.mode_id,
        )

    def _validate_provenance(
        self, session: Any, request: ModeTransitionRequest
    ) -> tuple[TransitionStatus | None, str | None, str | None]:
        if request.session_id != session.id:
            return (
                TransitionStatus.STALE,
                "The request session provenance does not match.",
                None,
            )
        if session.dispatch_id and request.dispatch_id != session.dispatch_id:
            return (
                TransitionStatus.STALE,
                "The dispatch provenance is missing or stale.",
                None,
            )
        if session.card_id and request.card_id != session.card_id:
            return (
                TransitionStatus.STALE,
                "The card provenance is missing or stale.",
                None,
            )
        if request.authority_instance_id and request.authority_instance_id != (
            session.authority_instance_id or self.settings.instance_id
        ):
            return (
                TransitionStatus.STALE,
                "The authority instance changed before this request was evaluated.",
                None,
            )
        authority_version = None
        if session.card_id:
            card = self.domain_store.get_card(
                session.card_id, realm_id=session.realm_id
            )
            if not card:
                return (
                    TransitionStatus.STALE,
                    "The authoritative card is no longer available.",
                    None,
                )
            authority_version = card.updated_at.isoformat()
            if request.authority_version != authority_version:
                return (
                    TransitionStatus.STALE,
                    "The card authority version changed; refresh provenance and submit a new request.",
                    authority_version,
                )
        return None, None, authority_version

    async def _notify(self, kind: str, session: Any, result: Any) -> Any | None:
        if not self.notifier:
            return
        result_payload = (
            result.model_dump(mode="json") if hasattr(result, "model_dump") else result
        )
        payload = {
            "kind": kind,
            "session_id": session.id,
            "dispatch_id": session.dispatch_id,
            "card_id": session.card_id,
            "result": result_payload,
        }
        create = getattr(self.notifier, "create", None)
        if callable(create):
            # NotificationService is supplied by the fleet interactions card.
            # Keep the import local so rolling upgrades remain compatible while
            # that module is not installed on every instance yet.
            try:
                from pa.domain.notifications import (
                    InteractionChoice,
                    InteractionKind,
                    InteractionRequest,
                    NotificationAction,
                    NotificationCreate,
                    NotificationPriority,
                    NotificationSeverity,
                    NotificationType,
                    NotificationVisibility,
                )

                titles = {
                    "collaboration_mode_approval": "Approve collaboration-mode change",
                    "collaboration_mode_pending": "Collaboration-mode change pending",
                    "collaboration_mode_applied": "Collaboration mode changed",
                    "collaboration_mode_rejected": "Collaboration-mode change not applied",
                    "collaboration_plan_escalation": "Plan-first session needs attention",
                    "command_result": "Agent command result",
                }
                reason = (
                    str(result_payload.get("reason") or "")
                    if isinstance(result_payload, dict)
                    else str(result_payload)
                )
                principal = session.principal_id or None
                approval = kind == "collaboration_mode_approval"
                notice = create(
                    NotificationCreate(
                        id=(
                            result_payload.get("approval_notification_id")
                            if approval
                            else None
                        ),
                        realm_id=session.realm_id,
                        visibility=(
                            NotificationVisibility.PRINCIPAL
                            if principal
                            else NotificationVisibility.REALM
                        ),
                        principal_id=principal,
                        type=(
                            NotificationType.INTERACTION
                            if approval
                            else NotificationType.GENERAL
                        ),
                        severity=(
                            NotificationSeverity.WARNING
                            if approval or kind.endswith(("rejected", "escalation"))
                            else NotificationSeverity.SUCCESS
                            if kind.endswith("applied")
                            else NotificationSeverity.INFO
                        ),
                        priority=(
                            NotificationPriority.HIGH
                            if approval or kind.endswith("escalation")
                            else NotificationPriority.NORMAL
                        ),
                        title=titles.get(kind, "Collaboration workflow update"),
                        body=reason[:16_000],
                        summary=reason[:1_000],
                        card_id=session.card_id,
                        session_id=session.id,
                        dispatch_id=session.dispatch_id,
                        project_id=session.project_id,
                        owner_instance_id=self.settings.instance_id,
                        capability="pa.collaboration-mode.v1",
                        actions=(
                            [
                                NotificationAction(
                                    id="respond",
                                    kind="respond",
                                    label="Review decision",
                                    enabled=True,
                                )
                            ]
                            if approval
                            else []
                        ),
                        interaction=(
                            InteractionRequest(
                                request_id=str(result_payload.get("request_id")),
                                kind=InteractionKind.APPROVAL,
                                prompt=(
                                    f"Approve changing this session from Plan to {result_payload.get('requested_mode', 'default')}? "
                                    f"{reason}"
                                )[:8000],
                                choices=[
                                    InteractionChoice(
                                        id="approve",
                                        label="Approve and continue",
                                        value="approve",
                                    ),
                                    InteractionChoice(
                                        id="decline",
                                        label="Keep Plan mode",
                                        value="decline",
                                    ),
                                ],
                                allow_cancel=False,
                                protocol_method="pa/collaboration_mode_approval",
                                protocol_request_id=str(result_payload.get("request_id")),
                                continuation_mode="protocol",
                            )
                            if approval
                            else None
                        ),
                        deduplication_key=(
                            f"collaboration:{kind}:"
                            f"{result_payload.get('request_id') or result_payload.get('id') or request_fingerprint(payload)}"
                            if isinstance(result_payload, dict)
                            else f"collaboration:{kind}:{request_fingerprint(payload)}"
                        ),
                    ),
                    principal_id=principal or "system:collaboration",
                    instance_id=self.settings.instance_id,
                )
                return notice
            except (ImportError, TypeError, ValueError):
                # Older peers may expose a different notification surface.
                if kind == "collaboration_mode_approval":
                    # Approval-required work must never degrade into a passive
                    # general notification with no way for the user to answer.
                    raise
                pass
        for name in ("publish", "emit", "create_notification", "notify"):
            call = getattr(self.notifier, name, None)
            if not callable(call):
                continue
            try:
                outcome = call(payload)
                if inspect.isawaitable(outcome):
                    await outcome
            except TypeError:
                continue
            return

    def _needs_user_approval(
        self,
        policy: CollaborationPolicy | None,
        current: CollaborationMode,
        request: ModeTransitionRequest,
    ) -> bool:
        return bool(
            policy is not None
            and current == CollaborationMode.PLAN
            and request.requested_mode == CollaborationMode.DEFAULT
            and policy.lifecycle.require_user_approval
            and request.actor == "agent"
        )

    def _record_pending_approval(self, session: Any, result: Any) -> None:
        config = dict(session.config_json or {})
        durable = dict(config.get("durable_runtime") or {})
        durable["pending_interaction"] = {
            "kind": "approval",
            "count": 1,
            "request_ids": [result.request_id],
            "notification_id": result.approval_notification_id,
            "action": "Approve or decline the requested collaboration-mode change.",
        }
        config["durable_runtime"] = durable
        session.config_json = config
        save = getattr(self.domain_store, "save_session", None)
        if callable(save):
            save(session)

    def _result(
        self,
        request: ModeTransitionRequest,
        status: TransitionStatus,
        current: CollaborationMode,
        reason: str,
        *,
        authority_version: str | None = None,
        policy_decision_id: str | None = None,
        pending: bool = False,
    ) -> ModeTransitionResult:
        return ModeTransitionResult(
            status=status,
            requested_mode=request.requested_mode,
            effective_mode=current,
            reason=reason,
            pending=pending,
            policy_decision_id=policy_decision_id,
            authority_version=authority_version,
            applied_at=(
                datetime.now(UTC)
                if status == TransitionStatus.APPROVED_APPLIED
                else None
            ),
        )

    def _transition_authorized(
        self,
        policy: CollaborationPolicy | None,
        current: CollaborationMode,
        request: ModeTransitionRequest,
    ) -> tuple[bool, str]:
        allowed, reason = transition_allowed(policy, current, request.requested_mode)
        if not allowed:
            return allowed, reason
        return allowed, reason

    async def request_transition(
        self, session: Any, request: ModeTransitionRequest
    ) -> ModeTransitionResult:
        prior = self.store.get_mode_request(session.id, request.idempotency_key)
        fingerprint = request_fingerprint(request)
        if prior:
            if prior[0] != fingerprint:
                raise IdempotencyConflict(
                    "mode-transition idempotency key was reused for a different request"
                )
            duplicate = prior[2].model_copy(update={"duplicate": True})
            if duplicate.status == TransitionStatus.APPROVAL_REQUIRED:
                await self._notify(
                    "collaboration_mode_approval", session, duplicate
                )
                self._record_pending_approval(session, duplicate)
            return duplicate

        current = _current_mode(session)
        stale, reason, authority_version = self._validate_provenance(session, request)
        if stale:
            result = self._result(
                request,
                stale,
                current,
                reason or "Stale request.",
                authority_version=authority_version,
            )
            self.store.save_mode_request(request, result)
            await self._notify("collaboration_mode_rejected", session, result)
            return result

        supported = self.supported_modes(session)
        if request.requested_mode not in supported:
            result = self._result(
                request,
                TransitionStatus.UNSUPPORTED,
                current,
                f"Provider {session.agent_name!r} does not advertise collaboration mode {request.requested_mode.value!r} for this session.",
                authority_version=authority_version,
            )
            self.store.save_mode_request(request, result)
            await self._notify("collaboration_mode_rejected", session, result)
            return result

        policy = self.effective_policy(session)
        allowed, policy_reason = self._transition_authorized(policy, current, request)
        value = PolicyInput(
            realm_id=session.realm_id,
            project_id=session.project_id,
            instance_id=self.settings.instance_id,
            provider=session.agent_name,
            card_id=session.card_id,
            supported_modes=supported,
            dispatch_intent="automatic" if session.dispatch_id else "manual",
        )
        decision = decide_initial_mode(value, [policy] if policy else [])
        self.store.record_decision(
            decision,
            session_id=session.id,
            dispatch_id=session.dispatch_id,
            card_id=session.card_id,
        )
        if allowed and self._needs_user_approval(policy, current, request):
            result = self._result(
                request,
                TransitionStatus.APPROVAL_REQUIRED,
                current,
                (
                    f"Policy {policy.id} requires trusted user approval before "
                    "leaving Plan mode. The original request and pending work are preserved."
                ),
                authority_version=authority_version,
                policy_decision_id=decision.id,
                pending=True,
            )
            result.approval_notification_id = str(
                uuid5(
                    NAMESPACE_URL,
                    f"pa-collaboration-approval:{session.id}:{result.request_id}",
                )
            )
            # Persist the request before publishing the actionable interaction;
            # an immediate response must always find its correlated request.
            self.store.save_mode_request(request, result)
            await self._notify("collaboration_mode_approval", session, result)
            self._record_pending_approval(session, result)
            self.store.update_mode_result(request, result)
            return result
        if not allowed:
            result = self._result(
                request,
                TransitionStatus.REJECTED,
                current,
                policy_reason,
                authority_version=authority_version,
                policy_decision_id=decision.id,
            )
            self.store.save_mode_request(request, result)
            await self._notify("collaboration_mode_rejected", session, result)
            return result

        runtime = self.manager.get(session.id) if self.manager else None
        if current == request.requested_mode:
            result = self._result(
                request,
                TransitionStatus.APPROVED_APPLIED,
                current,
                policy_reason,
                authority_version=authority_version,
                policy_decision_id=decision.id,
            )
            self.store.save_mode_request(request, result)
            await self._notify("collaboration_mode_applied", session, result)
            return result
        if not runtime or not runtime.connected or runtime.prompting:
            result = self._result(
                request,
                TransitionStatus.APPROVED_PENDING,
                current,
                (
                    "A turn is active; PA recorded the approved request and will apply it before the next turn."
                    if runtime and runtime.prompting
                    else "The session is not connected; PA will apply the approved request at the next recovered turn boundary."
                ),
                authority_version=authority_version,
                policy_decision_id=decision.id,
                pending=True,
            )
            self.store.save_mode_request(request, result)
            await self._notify("collaboration_mode_pending", session, result)
            return result

        result = await self._apply(runtime, request, decision, authority_version)
        self.store.save_mode_request(request, result)
        return result

    async def _apply(
        self,
        runtime: Any,
        request: ModeTransitionRequest,
        decision: PolicyDecision | None,
        authority_version: str | None,
    ) -> ModeTransitionResult:
        session = runtime.session
        before_execution = dict(
            (session.config_json or {}).get("execution_context") or {}
        )
        before_execution_mode = session.mode_id
        try:
            effective = await runtime.configure(
                SessionConfigurationRequest.from_values(
                    config={"collaboration_mode": request.requested_mode.value}
                )
            )
            confirmed = dict(effective.get("config") or {}).get("collaboration_mode")
            if confirmed != request.requested_mode.value:
                raise RuntimeError(
                    f"provider confirmed collaboration_mode={confirmed!r}, not {request.requested_mode.value!r}"
                )
            if (
                session.mode_id != before_execution_mode
                or dict((session.config_json or {}).get("execution_context") or {})
                != before_execution
            ):
                raise RuntimeError(
                    "collaboration-mode application changed execution/permission authority"
                )
            config = dict(session.config_json or {})
            config["collaboration"] = {
                "current_mode": request.requested_mode.value,
                "pending": None,
                "last_request_id": request.idempotency_key,
                "policy_decision_id": decision.id if decision else None,
                "authority_version": authority_version,
                "applied_at": datetime.now(UTC).isoformat(),
            }
            session.config_json = config
            await runtime._save_session_preserving_external_browser_async()
            result = self._result(
                request,
                TransitionStatus.APPROVED_APPLIED,
                request.requested_mode,
                "PA applied the policy-approved collaboration mode between turns.",
                authority_version=authority_version,
                policy_decision_id=decision.id if decision else None,
            )
            runtime._append_transcript(
                "collaboration_mode_transition",
                {
                    "request": request.model_dump(mode="json"),
                    "result": result.model_dump(mode="json"),
                    "execution_mode_id": before_execution_mode,
                },
            )
            await runtime._drain_transcripts()
            await self._notify("collaboration_mode_applied", session, result)
            return result
        except Exception as exc:  # noqa: BLE001 - provider/runtime boundary
            result = self._result(
                request,
                TransitionStatus.FAILED,
                _current_mode(session),
                f"PA could not apply the approved transition: {str(exc)[:1000]}",
                authority_version=authority_version,
                policy_decision_id=decision.id if decision else None,
            )
            await self._notify("collaboration_mode_rejected", session, result)
            return result

    async def apply_pending(self, runtime: Any) -> ModeTransitionResult | None:
        pending = self.store.pending_mode_request(runtime.session.id)
        if not pending:
            return None
        request, prior = pending
        if prior.status == TransitionStatus.APPROVAL_REQUIRED:
            return prior
        stale, reason, authority_version = self._validate_provenance(
            runtime.session, request
        )
        if stale:
            result = prior.model_copy(
                update={
                    "status": stale,
                    "reason": reason or "Pending request became stale.",
                    "pending": False,
                    "authority_version": authority_version,
                }
            )
        else:
            policy = self.effective_policy(runtime.session)
            allowed, policy_reason = self._transition_authorized(
                policy, _current_mode(runtime.session), request
            )
            if not allowed:
                result = prior.model_copy(
                    update={
                        "status": TransitionStatus.REJECTED,
                        "reason": "Authority changed before application: "
                        + policy_reason,
                        "pending": False,
                    }
                )
            else:
                decision = self.store.latest_decision(
                    session_id=runtime.session.id,
                    dispatch_id=runtime.session.dispatch_id,
                )
                result = await self._apply(
                    runtime, request, decision, authority_version
                )
        self.store.update_mode_result(request, result)
        return result

    async def handle_mode_approval(self, notification: Any, response: Any) -> None:
        """Consume one authenticated notification decision and resume exactly once."""
        if not notification.interaction or not notification.session_id:
            raise RuntimeError("Collaboration approval is missing session provenance")
        pending = self.store.pending_mode_request(notification.session_id)
        if not pending:
            return
        request, prior = pending
        if (
            prior.status != TransitionStatus.APPROVAL_REQUIRED
            or prior.request_id != notification.interaction.protocol_request_id
            or prior.approval_notification_id != notification.id
        ):
            raise RuntimeError("Collaboration approval no longer matches pending work")
        principal = str(notification.interaction.response_principal or "")
        if not principal or principal.startswith(("agent", "system:")):
            raise RuntimeError("Collaboration approval requires an authenticated user")
        approved = response.choice_id == "approve" and not response.cancel
        runtime = self.manager.get(notification.session_id) if self.manager else None
        if not approved:
            result = prior.model_copy(
                update={
                    "status": TransitionStatus.REJECTED,
                    "reason": "The user declined the collaboration-mode change.",
                    "pending": False,
                    "approved_by": principal,
                }
            )
            self.store.update_mode_result(request, result)
        else:
            if not runtime or not runtime.connected or runtime.prompting:
                result = prior.model_copy(
                    update={
                        "status": TransitionStatus.APPROVED_PENDING,
                        "reason": "Trusted user approval is recorded; PA will apply it at the next turn boundary.",
                        "pending": True,
                        "approved_by": principal,
                    }
                )
            else:
                decision = self.store.latest_decision(
                    session_id=notification.session_id,
                    dispatch_id=runtime.session.dispatch_id,
                )
                result = await self._apply(
                    runtime, request, decision, prior.authority_version
                )
                result.approved_by = principal
            self.store.update_mode_result(request, result)
            if (
                result.status == TransitionStatus.APPROVED_PENDING
                and self.manager is not None
                and (not runtime or not runtime.connected)
            ):
                try:
                    runtime = await self.manager.recover_session(
                        notification.session_id
                    )
                    if runtime.connected and not runtime.prompting:
                        applied = await self.apply_pending(runtime)
                        if applied is not None:
                            result = applied.model_copy(update={"approved_by": principal})
                            self.store.update_mode_result(request, result)
                except Exception:
                    # Approval is already durable. The normal recovery
                    # coordinator will retry without replaying the prompt.
                    runtime = None
        get_session = getattr(self.domain_store, "get_session", None)
        session = (
            runtime.session
            if runtime
            else get_session(notification.session_id)
            if callable(get_session)
            else None
        )
        if session is not None:
            config = dict(session.config_json or {})
            durable = dict(config.get("durable_runtime") or {})
            pending_interaction = dict(durable.get("pending_interaction") or {})
            if pending_interaction.get("notification_id") == notification.id:
                durable.pop("pending_interaction", None)
                config["durable_runtime"] = durable
                session.config_json = config
                save = getattr(self.domain_store, "save_session", None)
                if callable(save):
                    save(session)
        if runtime and result.status == TransitionStatus.APPROVED_APPLIED:
            start_drain = getattr(runtime, "_start_drain", None)
            if callable(start_drain):
                start_drain()

    async def reconcile_provider_mode(
        self, session: Any, confirmed_mode: str
    ) -> ModeTransitionResult | None:
        """Settle stale PA approval state from authenticated provider evidence."""
        try:
            mode = CollaborationMode(confirmed_mode)
        except ValueError:
            return None
        pending = self.store.pending_mode_request(session.id)
        if not pending:
            return None
        request, prior = pending
        if request.requested_mode != mode:
            return None
        result = prior.model_copy(
            update={
                "status": TransitionStatus.APPROVED_APPLIED,
                "effective_mode": mode,
                "reason": (
                    "The provider confirmed the requested mode through its "
                    "authenticated configuration channel; no repeated approval is required."
                ),
                "pending": False,
                "applied_at": datetime.now(UTC),
            }
        )
        self.store.update_mode_result(request, result)
        config = dict(session.config_json or {})
        durable = dict(config.get("durable_runtime") or {})
        pending_interaction = dict(durable.get("pending_interaction") or {})
        if pending_interaction.get("notification_id") == prior.approval_notification_id:
            durable.pop("pending_interaction", None)
            config["durable_runtime"] = durable
            session.config_json = config
        await self._notify("collaboration_mode_applied", session, result)
        return result

    async def prepare_turn(self, runtime: Any) -> ModeTransitionResult | None:
        """Apply pending work and enforce bounded Plan-first lifecycle."""
        applied = await self.apply_pending(runtime)
        session = runtime.session
        if _current_mode(session) != CollaborationMode.PLAN:
            return applied
        policy = self.effective_policy(session)
        lifecycle = policy.lifecycle if policy else None
        if lifecycle is None:
            return applied
        config = dict(session.config_json or {})
        collaboration = dict(config.get("collaboration") or {})
        started_raw = collaboration.get("plan_started_at")
        try:
            started = (
                datetime.fromisoformat(started_raw)
                if started_raw
                else datetime.now(UTC)
            )
        except ValueError, TypeError:
            started = datetime.now(UTC)
        turns = int(collaboration.get("plan_turns") or 0)
        expired = datetime.now(UTC) >= started + timedelta(
            minutes=lifecycle.expires_minutes
        )
        automatic_implementation = lifecycle.unattended_auto_approve and turns >= 1
        exhausted = turns >= lifecycle.max_turns or expired
        if automatic_implementation or (
            exhausted and lifecycle.unavailable_user_fallback.value == "default"
        ):
            card = (
                self.domain_store.get_card(session.card_id, realm_id=session.realm_id)
                if session.card_id
                else None
            )
            request = ModeTransitionRequest(
                requested_mode=CollaborationMode.DEFAULT,
                purpose=(
                    "Plan-first automatic approval criteria were satisfied."
                    if automatic_implementation
                    else "Plan-first budget expired and policy selected Default fallback."
                ),
                intended_next_action="Continue the bounded session in implementation mode.",
                session_id=session.id,
                dispatch_id=session.dispatch_id,
                card_id=session.card_id,
                authority_instance_id=session.authority_instance_id
                or self.settings.instance_id,
                authority_version=card.updated_at.isoformat() if card else None,
                idempotency_key=f"plan-lifecycle:{session.id}:{turns}:default",
                actor="pa-policy",
            )
            return await self.request_transition(session, request)
        if exhausted:
            reason = (
                f"Plan-first stopped after {turns} turns"
                + (" and expiry" if expired else "")
                + f"; fallback is {lifecycle.unavailable_user_fallback.value}."
            )
            if (
                lifecycle.require_user_approval
                and lifecycle.unavailable_user_fallback.value == "escalate"
            ):
                card = (
                    self.domain_store.get_card(
                        session.card_id, realm_id=session.realm_id
                    )
                    if session.card_id
                    else None
                )
                approval = await self.request_transition(
                    session,
                    ModeTransitionRequest(
                        requested_mode=CollaborationMode.DEFAULT,
                        purpose=reason,
                        intended_next_action=(
                            "Continue the original accepted prompt exactly once after approval."
                        ),
                        session_id=session.id,
                        dispatch_id=session.dispatch_id,
                        card_id=session.card_id,
                        authority_instance_id=session.authority_instance_id
                        or self.settings.instance_id,
                        authority_version=card.updated_at.isoformat() if card else None,
                        idempotency_key=f"plan-lifecycle:{session.id}:{turns}:approval",
                        actor="agent",
                    ),
                )
                if approval.status == TransitionStatus.APPROVAL_REQUIRED:
                    raise RuntimeError(approval.reason)
            await self._notify(
                "collaboration_plan_escalation",
                session,
                {"reason": reason, "lifecycle": lifecycle.model_dump(mode="json")},
            )
            raise RuntimeError(reason)
        collaboration.update(
            current_mode=CollaborationMode.PLAN.value,
            plan_started_at=started.isoformat(),
            plan_turns=turns + 1,
            plan_expires_at=(
                started + timedelta(minutes=lifecycle.expires_minutes)
            ).isoformat(),
            plan_max_turns=lifecycle.max_turns,
        )
        config["collaboration"] = collaboration
        session.config_json = config
        await runtime._save_session_preserving_external_browser_async()
        return applied

    async def execute_command(
        self, session: Any, request: ExecuteCommandRequest
    ) -> CommandResult:
        prior = self.store.get_command_result(session.id, request.idempotency_key)
        payload = request.model_dump(mode="json", exclude={"idempotency_key"})
        fingerprint = request_fingerprint(payload)
        if prior:
            if prior[0] != fingerprint:
                raise IdempotencyConflict(
                    "command idempotency key was reused for a different request"
                )
            return prior[1].model_copy(update={"duplicate": True})
        provenance_error = self._validate_command_provenance(session, request)
        if provenance_error:
            return await self._save_command_failure(
                session,
                request,
                CommandResultStatus.STALE,
                provenance_error,
            )
        catalog = self.ensure_catalog(session)
        if (
            request.catalog_generation is not None
            and request.catalog_generation != catalog.generation
        ):
            return await self._save_command_failure(
                session,
                request,
                CommandResultStatus.STALE,
                f"Command catalog generation {request.catalog_generation} is stale; current generation is {catalog.generation}.",
            )
        command = next(
            (
                item
                for item in catalog.commands
                if item.name == request.name.removeprefix("/")
            ),
            None,
        )
        if not command:
            return await self._save_command_failure(
                session,
                request,
                CommandResultStatus.UNSUPPORTED,
                "The command is not recognized in the active catalog.",
            )
        if command.availability != CommandAvailability.AVAILABLE:
            return await self._save_command_failure(
                session,
                request,
                CommandResultStatus.REJECTED,
                command.disabled_reason
                or "The command is disabled in the current context.",
            )
        if command.input_required and (
            request.arguments is None
            or request.arguments == ""
            or request.arguments == {}
        ):
            return await self._save_command_failure(
                session,
                request,
                CommandResultStatus.REJECTED,
                "This command requires input.",
            )
        runtime = self.manager.get(session.id) if self.manager else None
        if not runtime or not runtime.connected:
            return await self._save_command_failure(
                session,
                request,
                CommandResultStatus.FAILED,
                "The session owner is temporarily offline.",
            )
        action = command.action or {"type": "forward_prompt"}
        action_type = action.get("type")
        try:
            if action_type == "collaboration_status":
                state = self.state(session).model_dump(mode="json")
                result = CommandResult(
                    session_id=session.id,
                    command_name=command.name,
                    status=CommandResultStatus.APPLIED,
                    reason="Collaboration state inspected.",
                    effective_configuration=state,
                )
            elif action_type in {
                "request_collaboration_mode",
                "set_config_option",
            } and (
                action_type == "request_collaboration_mode"
                or action.get("config_id")
                in {"collaboration_mode", "collaborationMode"}
            ):
                raw_mode = (
                    action.get("mode")
                    if action_type == "request_collaboration_mode"
                    else action.get("value")
                )
                if raw_mode == "$argument":
                    raw_mode = request.arguments
                mode = CollaborationMode(str(raw_mode).strip().lower())
                card = (
                    self.domain_store.get_card(
                        session.card_id, realm_id=session.realm_id
                    )
                    if session.card_id
                    else None
                )
                mode_result = await self.request_transition(
                    session,
                    ModeTransitionRequest(
                        requested_mode=mode,
                        purpose=f"Execute /{command.name} from the PA command catalog.",
                        intended_next_action=(
                            str(request.arguments)[:2000]
                            if request.arguments
                            else "Continue with the selected collaboration workflow."
                        ),
                        session_id=session.id,
                        dispatch_id=session.dispatch_id,
                        card_id=session.card_id,
                        authority_instance_id=session.authority_instance_id
                        or self.settings.instance_id,
                        authority_version=(
                            card.updated_at.isoformat() if card else None
                        ),
                        idempotency_key=f"command:{request.idempotency_key}",
                        actor=request.actor,
                    ),
                )
                result = CommandResult(
                    session_id=session.id,
                    command_name=command.name,
                    status=(
                        CommandResultStatus.APPLIED
                        if mode_result.status
                        in {
                            TransitionStatus.APPROVED_APPLIED,
                            TransitionStatus.APPROVED_PENDING,
                        }
                        else CommandResultStatus.FORWARDED
                        if mode_result.status == TransitionStatus.APPROVAL_REQUIRED
                        else CommandResultStatus.UNSUPPORTED
                        if mode_result.status == TransitionStatus.UNSUPPORTED
                        else CommandResultStatus.STALE
                        if mode_result.status == TransitionStatus.STALE
                        else CommandResultStatus.FAILED
                        if mode_result.status == TransitionStatus.FAILED
                        else CommandResultStatus.REJECTED
                    ),
                    reason=mode_result.reason,
                    mode_result=mode_result,
                    effective_configuration=self.state(session).model_dump(mode="json"),
                )
            elif action_type == "set_config_option":
                if runtime.prompting:
                    return await self._save_command_failure(
                        session,
                        request,
                        CommandResultStatus.REJECTED,
                        "Wait for the active turn to finish before changing provider configuration.",
                    )
                effective = await runtime.configure(
                    SessionConfigurationRequest.from_values(
                        config={str(action.get("config_id")): action.get("value")}
                    )
                )
                result = CommandResult(
                    session_id=session.id,
                    command_name=command.name,
                    status=CommandResultStatus.APPLIED,
                    reason="The advertised provider configuration action was applied and verified.",
                    effective_configuration=effective,
                )
            elif action_type == "forward_prompt":
                argument_text = (
                    json.dumps(request.arguments, sort_keys=True)
                    if isinstance(request.arguments, dict)
                    else str(request.arguments or "")
                )
                text = f"/{command.name}" + (
                    f" {argument_text}" if argument_text else ""
                )
                stop_reason = await runtime.prompt(
                    text,
                    item_id=session.card_id,
                    principal_id=session.principal_id,
                    project_id=session.project_id,
                    action="append",
                    prompt_id=f"command:{request.idempotency_key}",
                    wait=False,
                )
                result = CommandResult(
                    session_id=session.id,
                    command_name=command.name,
                    status=CommandResultStatus.FORWARDED,
                    reason=f"The provider-advertised command was durably {stop_reason}.",
                )
            else:
                result = CommandResult(
                    session_id=session.id,
                    command_name=command.name,
                    status=CommandResultStatus.UNSUPPORTED,
                    reason=f"The provider advertised unsupported command action {action_type!r}; PA did not forward it as prompt text.",
                )
        except Exception as exc:  # noqa: BLE001 - provider/runtime boundary
            result = CommandResult(
                session_id=session.id,
                command_name=command.name,
                status=CommandResultStatus.FAILED,
                reason=f"Recognized command execution failed: {str(exc)[:1000]}",
            )
        saved = self.store.save_command_result(
            payload, result, idempotency_key=request.idempotency_key
        )
        runtime._append_transcript("command_result", saved.model_dump(mode="json"))
        await runtime._drain_transcripts()
        await self._notify("command_result", session, saved)
        return saved

    def _validate_command_provenance(
        self, session: Any, request: ExecuteCommandRequest
    ) -> str | None:
        if request.session_id != session.id:
            return "The command session provenance does not match."
        if session.dispatch_id and request.dispatch_id != session.dispatch_id:
            return "The command dispatch provenance is missing or stale."
        if session.card_id and request.card_id != session.card_id:
            return "The command card provenance is missing or stale."
        current_authority = session.authority_instance_id or self.settings.instance_id
        if request.authority_instance_id != current_authority:
            return "The command authority instance is missing or stale."
        if session.card_id:
            card = self.domain_store.get_card(
                session.card_id, realm_id=session.realm_id
            )
            if not card:
                return "The authoritative card is no longer available."
            if request.authority_version != card.updated_at.isoformat():
                return (
                    "The command card authority version changed; refresh the catalog."
                )
        return None

    async def _save_command_failure(
        self,
        session: Any,
        request: ExecuteCommandRequest,
        status: CommandResultStatus,
        reason: str,
    ) -> CommandResult:
        result = CommandResult(
            session_id=request.session_id,
            command_name=request.name.removeprefix("/"),
            status=status,
            reason=reason,
        )
        saved = self.store.save_command_result(
            request.model_dump(mode="json", exclude={"idempotency_key"}),
            result,
            idempotency_key=request.idempotency_key,
        )
        runtime = self.manager.get(session.id) if self.manager else None
        if runtime is not None:
            runtime._append_transcript("command_result", saved.model_dump(mode="json"))
            await runtime._drain_transcripts()
        await self._notify("command_result", session, saved)
        return saved
