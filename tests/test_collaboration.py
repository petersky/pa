from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase, TestCase

from pa.acp.client import normalize_session_update
from pa.collaboration.commands import build_catalog
from pa.collaboration.models import (
    CollaborationMode,
    CollaborationPolicy,
    ExecuteCommandRequest,
    ModeTransitionRequest,
    PlanFallback,
    PlanLifecycle,
    PolicyInput,
    PolicyScope,
    PolicyStrategy,
    TransitionStatus,
)
from pa.collaboration.policy import decide_initial_mode
from pa.collaboration.service import CollaborationService
from pa.domain.models import AgentSession


class _Card:
    def __init__(self) -> None:
        self.id = "card-1"
        self.project_id = "project-1"
        self.kind = SimpleNamespace(value="task")
        self.tags = ["ambiguous"]
        self.updated_at = datetime.now(UTC)


class _DomainStore:
    def __init__(self) -> None:
        self.card = _Card()
        self.saved = []

    def get_card(self, card_id, *, realm_id="default"):
        return self.card if card_id == self.card.id else None


class _Connection:
    def __init__(self, values=("default", "plan")) -> None:
        self.config_options = [
            {
                "id": "collaboration_mode",
                "currentValue": "default",
                "options": [{"value": value} for value in values],
            }
        ]


class _Runtime:
    def __init__(self, session: AgentSession, values=("default", "plan")) -> None:
        self.session = session
        self.connection = _Connection(values)
        self.connected = True
        self.prompting = False
        self.events = []
        self.configure_calls = []

    async def configure(self, request):
        self.configure_calls.append(request.as_dict())
        value = request.config["collaboration_mode"]
        config = dict(self.session.config_json)
        config["values"] = {
            **dict(config.get("values") or {}),
            "collaboration_mode": value,
        }
        self.session.config_json = config
        return {
            "model_id": None,
            "mode_id": self.session.mode_id,
            "reasoning": None,
            "config": {"collaboration_mode": value},
        }

    async def _save_session_preserving_external_browser_async(self):
        return None

    def _append_transcript(self, event_type, payload):
        self.events.append((event_type, payload))

    async def _drain_transcripts(self):
        return None


class _Manager:
    def __init__(self, runtime: _Runtime | None) -> None:
        self.runtime = runtime

    def get(self, session_id):
        if self.runtime and self.runtime.session.id == session_id:
            return self.runtime
        return None


def _session() -> AgentSession:
    return AgentSession(
        id="session-1",
        agent_name="codex",
        authority_instance_id="instance-1",
        dispatch_id="dispatch-1",
        realm_id="default",
        card_id="card-1",
        project_id="project-1",
        principal_id="user:local",
        mode_id="agent",
        config_json={
            "values": {"collaboration_mode": "default"},
            "execution_context": {"sandbox": "workspace-write", "token": 7},
        },
    )


class CollaborationPolicyTests(TestCase):
    def test_explicit_selection_overrides_automatic(self):
        policy = CollaborationPolicy(
            id="project-policy",
            scope_type=PolicyScope.PROJECT,
            scope_id="project-1",
            strategy=PolicyStrategy.AUTOMATIC,
        )
        decision = decide_initial_mode(
            PolicyInput(
                instance_id="instance-1",
                provider="codex",
                project_id="project-1",
                ambiguous=True,
                dispatch_override=CollaborationMode.DEFAULT,
                supported_modes=[CollaborationMode.DEFAULT, CollaborationMode.PLAN],
            ),
            [policy],
        )
        self.assertEqual(decision.effective_mode, CollaborationMode.DEFAULT)
        self.assertEqual(decision.source, "explicit_dispatch_override")

    def test_mandatory_constraint_overrides_explicit_selection(self):
        policy = CollaborationPolicy(
            id="mandatory",
            scope_type=PolicyScope.PROJECT,
            scope_id="project-1",
            strategy=PolicyStrategy.ALWAYS_PLAN,
            mandatory_mode=CollaborationMode.PLAN,
        )
        decision = decide_initial_mode(
            PolicyInput(
                instance_id="instance-1",
                provider="codex",
                project_id="project-1",
                dispatch_override=CollaborationMode.DEFAULT,
                supported_modes=[CollaborationMode.DEFAULT, CollaborationMode.PLAN],
            ),
            [policy],
        )
        self.assertEqual(decision.effective_mode, CollaborationMode.PLAN)
        self.assertTrue(decision.mandatory)
        self.assertIn("Mandatory", decision.rationale)

    def test_automatic_plan_first_records_deterministic_inputs(self):
        policy = CollaborationPolicy(
            id="auto",
            scope_type=PolicyScope.INSTANCE,
            scope_id="instance-1",
            strategy=PolicyStrategy.AUTOMATIC,
        )
        decision = decide_initial_mode(
            PolicyInput(
                instance_id="instance-1",
                provider="codex",
                ambiguous=True,
                risk="high",
                supported_modes=[CollaborationMode.DEFAULT, CollaborationMode.PLAN],
            ),
            [policy],
        )
        self.assertEqual(decision.effective_mode, CollaborationMode.PLAN)
        self.assertIn("risk", decision.rationale)
        self.assertTrue(decision.inputs["ambiguous"])
        self.assertEqual(decision.inputs["matched_policy_ids"], ["auto"])

    def test_provider_without_plan_falls_back_to_default(self):
        policy = CollaborationPolicy(
            id="always-plan",
            scope_type=PolicyScope.PROVIDER,
            scope_id="cursor",
            strategy=PolicyStrategy.ALWAYS_PLAN,
        )
        decision = decide_initial_mode(
            PolicyInput(
                instance_id="instance-1",
                provider="cursor",
                supported_modes=[CollaborationMode.DEFAULT],
            ),
            [policy],
        )
        self.assertEqual(decision.effective_mode, CollaborationMode.DEFAULT)
        self.assertIn("does not advertise", decision.rationale)

    def test_project_policy_precedes_instance_and_provider_defaults(self):
        decision = decide_initial_mode(
            PolicyInput(
                instance_id="instance-1",
                provider="codex",
                project_id="project-1",
                supported_modes=[CollaborationMode.DEFAULT, CollaborationMode.PLAN],
            ),
            [
                CollaborationPolicy(
                    id="provider-default",
                    scope_type=PolicyScope.PROVIDER,
                    scope_id="codex",
                    strategy=PolicyStrategy.ALWAYS_DEFAULT,
                ),
                CollaborationPolicy(
                    id="instance-default",
                    scope_type=PolicyScope.INSTANCE,
                    scope_id="instance-1",
                    strategy=PolicyStrategy.ALWAYS_DEFAULT,
                ),
                CollaborationPolicy(
                    id="project-plan",
                    scope_type=PolicyScope.PROJECT,
                    scope_id="project-1",
                    strategy=PolicyStrategy.ALWAYS_PLAN,
                ),
            ],
        )
        self.assertEqual(decision.effective_mode, CollaborationMode.PLAN)
        self.assertEqual(decision.source_policy_id, "project-plan")

    def test_available_commands_update_retains_actions(self):
        update = normalize_session_update(
            {
                "sessionUpdate": "available_commands_update",
                "availableCommands": [
                    {
                        "name": "plan",
                        "description": "Plan first",
                        "commandAction": {
                            "setConfigOption": {
                                "configId": "collaboration_mode",
                                "value": "plan",
                            }
                        },
                    }
                ],
            }
        )
        self.assertEqual(update["type"], "available_commands_update")
        self.assertEqual(update["available_commands"][0]["name"], "plan")

    def test_provider_and_pa_commands_have_deterministic_names(self):
        catalog = build_catalog(
            session_id="session-1",
            provider="codex",
            generation=1,
            connection_generation=1,
            provider_commands=[{"name": "plan", "description": "provider plan"}],
        )
        names = [command.name for command in catalog.commands]
        self.assertIn("plan", names)
        self.assertIn("pa:plan", names)
        self.assertEqual(names.count("plan"), 1)


class CollaborationServiceTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = TemporaryDirectory()
        self.settings = SimpleNamespace(
            data_dir=Path(self.tmp.name), instance_id="instance-1"
        )
        self.domain = _DomainStore()
        self.session = _session()
        self.runtime = _Runtime(self.session)
        self.manager = _Manager(self.runtime)
        self.service = CollaborationService(self.settings, self.domain)
        self.service.bind_runtime(self.manager)

    async def asyncTearDown(self):
        self.tmp.cleanup()

    def request(self, **updates):
        values = {
            "requested_mode": CollaborationMode.PLAN,
            "purpose": "Investigate ambiguous requirements before editing.",
            "intended_next_action": "Produce a durable implementation plan.",
            "session_id": self.session.id,
            "dispatch_id": self.session.dispatch_id,
            "card_id": self.session.card_id,
            "authority_instance_id": "instance-1",
            "authority_version": self.domain.card.updated_at.isoformat(),
            "idempotency_key": "transition-1",
        }
        values.update(updates)
        return ModeTransitionRequest(**values)

    async def test_allowed_transition_applies_without_broadening_authority(self):
        before = dict(self.session.config_json["execution_context"])
        result = await self.service.request_transition(self.session, self.request())
        self.assertEqual(result.status, TransitionStatus.APPROVED_APPLIED)
        self.assertEqual(result.effective_mode, CollaborationMode.PLAN)
        self.assertEqual(self.session.mode_id, "agent")
        self.assertEqual(self.session.config_json["execution_context"], before)
        self.assertEqual(len(self.runtime.configure_calls), 1)

    async def test_duplicate_returns_same_durable_result(self):
        first = await self.service.request_transition(self.session, self.request())
        second = await self.service.request_transition(self.session, self.request())
        self.assertEqual(first.request_id, second.request_id)
        self.assertTrue(second.duplicate)
        self.assertEqual(len(self.runtime.configure_calls), 1)

    async def test_denying_policy_returns_reason(self):
        self.service.save_policy(
            CollaborationPolicy(
                id="deny",
                scope_type=PolicyScope.INSTANCE,
                scope_id="instance-1",
                allow_agent_transitions=False,
            )
        )
        result = await self.service.request_transition(self.session, self.request())
        self.assertEqual(result.status, TransitionStatus.REJECTED)
        self.assertIn("denies", result.reason)

    async def test_agent_cannot_self_approve_leaving_plan_mode(self):
        self.service.save_policy(
            CollaborationPolicy(
                id="approval-required",
                scope_type=PolicyScope.INSTANCE,
                scope_id="instance-1",
                lifecycle=PlanLifecycle(require_user_approval=True),
            )
        )
        self.session.config_json["values"]["collaboration_mode"] = "plan"
        result = await self.service.request_transition(
            self.session,
            self.request(
                requested_mode=CollaborationMode.DEFAULT,
                purpose="The plan is complete and ready for approval.",
                intended_next_action="Implement only after the user approves.",
                actor="agent",
            ),
        )
        self.assertEqual(result.status, TransitionStatus.REJECTED)
        self.assertIn("user approval", result.reason)

    async def test_user_can_approve_leaving_plan_mode(self):
        self.service.save_policy(
            CollaborationPolicy(
                id="approval-required",
                scope_type=PolicyScope.INSTANCE,
                scope_id="instance-1",
                lifecycle=PlanLifecycle(require_user_approval=True),
            )
        )
        self.session.config_json["values"]["collaboration_mode"] = "plan"
        result = await self.service.request_transition(
            self.session,
            self.request(
                requested_mode=CollaborationMode.DEFAULT,
                purpose="The user approved the durable plan.",
                intended_next_action="Begin implementation within existing authority.",
                actor="user:local",
            ),
        )
        self.assertEqual(result.status, TransitionStatus.APPROVED_APPLIED)

    async def test_in_flight_request_is_applied_at_next_turn_boundary(self):
        self.runtime.prompting = True
        pending = await self.service.request_transition(self.session, self.request())
        self.assertEqual(pending.status, TransitionStatus.APPROVED_PENDING)
        self.assertTrue(pending.pending)
        self.assertEqual(self.runtime.configure_calls, [])
        self.runtime.prompting = False
        applied = await self.service.apply_pending(self.runtime)
        self.assertEqual(applied.status, TransitionStatus.APPROVED_APPLIED)
        self.assertFalse(self.service.store.pending_mode_request(self.session.id))

    async def test_stale_authority_version_is_deterministic(self):
        result = await self.service.request_transition(
            self.session, self.request(authority_version="old")
        )
        self.assertEqual(result.status, TransitionStatus.STALE)
        self.assertIn("authority version", result.reason)

    async def test_unsupported_provider_mode_is_not_applied(self):
        self.runtime.connection = _Connection(("default",))
        result = await self.service.request_transition(self.session, self.request())
        self.assertEqual(result.status, TransitionStatus.UNSUPPORTED)
        self.assertEqual(self.runtime.configure_calls, [])

    async def test_recovered_disconnected_session_retains_pending_request(self):
        self.runtime.connected = False
        result = await self.service.request_transition(self.session, self.request())
        self.assertEqual(result.status, TransitionStatus.APPROVED_PENDING)
        self.assertTrue(self.service.store.pending_mode_request(self.session.id))

    async def test_provider_plan_command_uses_configuration_action(self):
        catalog = self.service.capture_available_commands(
            self.session,
            [
                {
                    "name": "plan",
                    "description": "Plan first",
                    "commandAction": {
                        "setConfigOption": {
                            "configId": "collaboration_mode",
                            "value": "plan",
                        }
                    },
                }
            ],
            connection_generation=3,
        )
        result = await self.service.execute_command(
            self.session,
            ExecuteCommandRequest(
                session_id=self.session.id,
                name="plan",
                catalog_generation=catalog.generation,
                dispatch_id=self.session.dispatch_id,
                card_id=self.session.card_id,
                authority_instance_id=self.session.authority_instance_id,
                authority_version=self.domain.card.updated_at.isoformat(),
                idempotency_key="command-1",
            ),
        )
        self.assertEqual(result.status.value, "applied")
        self.assertEqual(result.mode_result.status, TransitionStatus.APPROVED_APPLIED)
        self.assertEqual(
            self.runtime.configure_calls[-1]["config"]["collaboration_mode"], "plan"
        )

    async def test_unknown_provider_action_is_not_forwarded_as_prompt(self):
        catalog = self.service.capture_available_commands(
            self.session,
            [
                {
                    "name": "mystery",
                    "description": "Unknown action",
                    "commandAction": {"opaque": True},
                }
            ],
            connection_generation=3,
        )
        result = await self.service.execute_command(
            self.session,
            ExecuteCommandRequest(
                session_id=self.session.id,
                name="mystery",
                catalog_generation=catalog.generation,
                dispatch_id=self.session.dispatch_id,
                card_id=self.session.card_id,
                authority_instance_id=self.session.authority_instance_id,
                authority_version=self.domain.card.updated_at.isoformat(),
                idempotency_key="command-unknown-action",
            ),
        )
        self.assertEqual(result.status.value, "unsupported")
        self.assertIn("did not forward", result.reason)

    async def test_plan_lifecycle_budget_falls_back_without_authority_change(self):
        self.service.save_policy(
            CollaborationPolicy(
                id="plan-budget",
                scope_type=PolicyScope.INSTANCE,
                scope_id="instance-1",
                strategy=PolicyStrategy.ALWAYS_PLAN,
                lifecycle=PlanLifecycle(
                    max_turns=1,
                    expires_minutes=1,
                    unavailable_user_fallback=PlanFallback.DEFAULT,
                ),
            )
        )
        self.session.config_json["values"]["collaboration_mode"] = "plan"
        self.session.config_json["collaboration"] = {
            "current_mode": "plan",
            "plan_turns": 1,
            "plan_started_at": (datetime.now(UTC) - timedelta(minutes=2)).isoformat(),
        }
        before_mode = self.session.mode_id
        result = await self.service.prepare_turn(self.runtime)
        self.assertEqual(result.status, TransitionStatus.APPROVED_APPLIED)
        self.assertEqual(result.effective_mode, CollaborationMode.DEFAULT)
        self.assertEqual(self.session.mode_id, before_mode)
