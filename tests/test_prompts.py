"""Regression coverage for PA's centralized operational prompt registry."""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from pa.agent.context import compose_session_prompt
from pa.config import Settings
from pa.domain.models import AgentSession, Card, Project, ProjectRepo
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime
from pa.instance.quiesce import QueuedPrompt
from pa.prompts import (
    PROMPTS,
    PromptDefinition,
    PromptRegistry,
    PromptRenderError,
    PromptVariable,
)

EXPECTED_PROMPT_KEYS = {
    "card.reconciliation.disposition",
    "agent.context.browser",
    "agent.context.card",
    "agent.context.collaboration",
    "agent.context.data_safety",
    "agent.context.execution",
    "agent.context.interactions",
    "agent.context.memory",
    "agent.context.project",
    "agent.context.workspace_catalog",
    "agent.message.wrapper",
    "dispatch.remote.default",
    "pr_supervisor.action.green",
    "pr_supervisor.action.merged",
    "pr_supervisor.action.required",
    "pr_supervisor.executor",
    "release.notes.generate",
    "session.recovery.resume",
}


class PromptRegistryTests(unittest.TestCase):
    def test_catalog_is_complete_versioned_typed_and_read_only(self) -> None:
        definitions = PROMPTS.all()
        self.assertEqual({item.key for item in definitions}, EXPECTED_PROMPT_KEYS)
        for item in definitions:
            with self.subTest(key=item.key):
                self.assertTrue(item.purpose)
                self.assertTrue(item.scope)
                self.assertGreaterEqual(item.version, 1)
                self.assertEqual(item.source, "pa:builtin")
                self.assertTrue(item.read_only)
                self.assertEqual(
                    {variable.name for variable in item.variables},
                    set(
                        __import__("re").findall(
                            r"{{\s*([A-Za-z_][A-Za-z0-9_.]*)\s*}}",
                            item.template,
                        )
                    ),
                )

    def test_card_reconciliation_distinguishes_no_integration_outcomes(self) -> None:
        prompt = PROMPTS.render("card.reconciliation.disposition").text

        self.assertIn("set `integration_required` to false", prompt)
        self.assertIn("do not invent or inspect pull-request work", prompt)
        self.assertIn("A verified no-integration task may use `done`", prompt)

    def test_unbound_session_receives_canonical_pa_catalog_and_link_guidance(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            from pa.domain.models import CardCreate, ProjectCreate, RepositoryCreate
            from pa.domain.projection import CardProjection

            store = CardProjection(Path(tmp) / "pa.db")
            project = store.create_project(ProjectCreate(title="PA Core"))
            card = store.create_card(
                CardCreate(title="Capture standalone work", project_id=project.id)
            )
            repository = store.create_repository(
                RepositoryCreate(url="https://example.invalid/acme/pa", name="PA")
            )
            session = AgentSession(id="session-unbound", agent_name="codex")
            store.save_session(session)
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="executor",
                instance_name="Executor",
            )

            result = compose_session_prompt(store, settings, session, "Find the work.")

            self.assertIn("Agent session: session-unbound", result.text)
            self.assertIn(f"PA Core ({project.id})", result.text)
            self.assertIn(f"Capture standalone work ({card.id})", result.text)
            self.assertIn(repository.url, result.text)
            self.assertIn("associate_agent_session_card", result.text)

    def test_all_registered_prompts_have_an_operational_call_site(self) -> None:
        root = Path(__file__).parents[1] / "src" / "pa"
        source = "\n".join(
            path.read_text() for path in root.rglob("*.py") if path.name != "catalog.py"
        )
        for key in EXPECTED_PROMPT_KEYS:
            with self.subTest(key=key):
                self.assertIn(f'"{key}"', source)

    def test_operational_prompt_phrases_exist_only_in_catalog(self) -> None:
        root = Path(__file__).parents[1] / "src" / "pa"
        phrases = (
            "Work on this card autonomously",
            "# PA pull-request supervisor",
            "PA provides browser tools through",
            "PA data and sync safety",
            "You are writing release notes",
            "PA recovered this queued turn",
        )
        offenders: list[str] = []
        for path in root.rglob("*.py"):
            if path.name == "catalog.py":
                continue
            text = path.read_text()
            offenders.extend(
                f"{path.relative_to(root)}: {phrase}"
                for phrase in phrases
                if phrase in text
            )
        self.assertEqual(offenders, [])

    def test_required_variables_and_invalid_placeholders_fail(self) -> None:
        with self.assertRaisesRegex(PromptRenderError, "required prompt variable"):
            PROMPTS.render("agent.context.card", {"card": {"title": "Only title"}})
        with self.assertRaisesRegex(ValueError, "invalid placeholder"):
            PromptDefinition(
                key="test.invalid",
                purpose="test",
                scope="global",
                version=1,
                template="Hello {{ invalid-name }}",
            )

    def test_secret_values_are_redacted_and_secret_variables_forbidden(self) -> None:
        registry = PromptRegistry()
        registry.register(
            PromptDefinition(
                key="test.payload",
                purpose="test redaction",
                scope="global",
                version=1,
                template="Payload: {{ payload }}",
                variables=(
                    PromptVariable(
                        name="payload",
                        type="object",
                        description="Safe wrapper around external data.",
                        example={},
                    ),
                ),
            )
        )
        rendered = registry.render(
            "test.payload",
            {"payload": {"title": "safe", "api_token": "gho_abcdefghijklmnop"}},
        )
        self.assertIn('"api_token": "[REDACTED]"', rendered.text)
        self.assertNotIn("gho_abcdefghijklmnop", rendered.text)
        self.assertEqual(
            rendered.resolved_context["payload"]["api_token"], "[REDACTED]"
        )
        with self.assertRaisesRegex(ValueError, "secret-like"):
            PromptDefinition(
                key="test.secret",
                purpose="test",
                scope="global",
                version=1,
                template="{{ api_token }}",
                variables=(
                    PromptVariable(
                        name="api_token",
                        description="Forbidden.",
                        example="synthetic",
                    ),
                ),
            )

    def test_literal_braces_in_values_are_not_unresolved_placeholders(self) -> None:
        rendered = PROMPTS.render(
            "agent.context.card",
            {"card": {"title": "Template", "body": "Keep {{ user.value }} literal."}},
        )
        self.assertIn("{{ user.value }}", rendered.text)

    def test_provider_and_definition_size_limits_are_enforced(self) -> None:
        registry = PromptRegistry()
        registry.register(
            PromptDefinition(
                key="test.limit",
                purpose="test",
                scope="global",
                version=1,
                max_characters=5,
                template="{{ value }}",
                variables=(
                    PromptVariable(
                        name="value",
                        description="Large test value.",
                        example="small",
                    ),
                ),
            )
        )
        with self.assertRaisesRegex(PromptRenderError, "limit is 5"):
            registry.render("test.limit", {"value": "123456"}, provider="codex")

    def test_provider_context_reserve_is_enforced_for_nested_prompts(self) -> None:
        registry = PromptRegistry()
        registry.register(
            PromptDefinition(
                key="test.nested",
                purpose="test reserved provider context",
                scope="global",
                version=1,
                template="{{ value }}",
                variables=(
                    PromptVariable(
                        name="value",
                        description="Nested prompt payload.",
                        example="small",
                    ),
                ),
            )
        )
        payload = {"value": "x" * 70_000}

        self.assertEqual(len(registry.render("test.nested", payload).text), 70_000)
        with self.assertRaisesRegex(PromptRenderError, "limit is 65536"):
            registry.render("test.nested", payload, reserve_context=True)

    def test_registry_has_no_host_assumptions(self) -> None:
        templates = "\n".join(item.template for item in PROMPTS.all())
        for forbidden in ("Mac mini", "/Users/", "/home/", "localhost:"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, templates)


class PromptCompositionTests(unittest.TestCase):
    @patch("pa.agent.context.MemoryService")
    @patch("pa.agent.context.LimbicService")
    def test_shadow_rollout_observes_without_prompt_effects(
        self, limbic_service: MagicMock, memory_service: MagicMock
    ) -> None:
        store = MagicMock()
        project = Project(
            id="project-shadow",
            title="Shadow",
            tool_config={"autonomy_context": {"mode": "shadow"}},
        )
        store.get_project.return_value = project
        store.get_card.return_value = None
        session = AgentSession(
            id="session-shadow",
            agent_name="codex",
            project_id=project.id,
            principal_id="agent:one",
        )
        settings = Settings(data_dir=Path(tempfile.mkdtemp()), instance_id="executor")
        limbic_service.return_value.appraise.return_value = MagicMock()
        memory_service.return_value.working_packet.return_value.memories = []

        result = compose_session_prompt(store, settings, session, "secret-token=bad")

        self.assertNotIn("Authorized Memory", result.text)
        limbic_service.return_value.appraise.assert_called_once()
        self.assertTrue(limbic_service.return_value.appraise.call_args.kwargs["shadow_mode"])
        query = memory_service.return_value.working_packet.call_args.args[0]
        self.assertEqual(query.requester_principal, "agent:one")

    @patch("pa.agent.context.MemoryService")
    @patch("pa.agent.context.LimbicService")
    def test_memory_promotion_requires_all_gates_and_keeps_citations(
        self, limbic_service: MagicMock, memory_service: MagicMock
    ) -> None:
        from pa.limbic.models import (
            MemoryProvenance,
            MemoryRecord,
            MemoryTier,
            RetrievedMemory,
        )

        store = MagicMock()
        project = Project(
            id="project-live",
            title="Live",
            tool_config={
                "autonomy_context": {
                    "mode": "promoted",
                    "policy_gate": True,
                    "max_characters": 1000,
                }
            },
        )
        store.get_project.return_value = project
        store.get_card.return_value = None
        session = AgentSession(
            id="session-live", agent_name="codex", project_id=project.id,
            principal_id="agent:one",
        )
        settings = Settings(data_dir=Path(tempfile.mkdtemp()), instance_id="executor")
        signal = MagicMock()
        appraisal = MagicMock(deterministic_bypass=None)
        route = MagicMock(preliminary=False)
        limbic_service.return_value.appraise.return_value = MagicMock(
            signal=signal, appraisal=appraisal, route=route
        )
        record = MemoryRecord(
            id="memory-safe", tier=MemoryTier.PROCEDURAL, subject="repo",
            predicate="workflow", value="run tests", summary="Run scoped tests.",
            owner_principal="agent:one",
            provenance=MemoryProvenance(
                source_type="operator", source_id="decision-1",
                actor_principal="user:one", verified=True,
            ),
        )
        memory_service.return_value.working_packet.return_value.memories = [
            RetrievedMemory(
                record=record, relevance=1, instruction_trusted=True,
                retrieval_reason="authorized",
            )
        ]

        result = compose_session_prompt(store, settings, session, "Do the work")

        self.assertIn("[memory:memory-safe]", result.text)
        self.assertIn("operator:decision-1; trusted", result.text)

    def test_composition_uses_selected_instance_and_materialized_workspace(
        self,
    ) -> None:
        store = MagicMock()
        project = Project(
            id="project-actual",
            title="Actual Project",
            description="Project context",
            agent_prompt="Use project conventions.",
            repos=[ProjectRepo(url="https://example.invalid/acme/actual")],
        )
        card = Card(
            id="card-actual",
            title="Actual Card",
            body="Deliver it.",
            project_id=project.id,
        )
        store.get_card.return_value = card
        store.get_project.return_value = project
        settings = Settings(
            data_dir=Path(tempfile.mkdtemp()),
            instance_id="executor-actual",
            instance_name="Actual Runner",
        )
        session = AgentSession(
            id="session-actual",
            agent_name="codex",
            card_id=card.id,
            project_id=project.id,
            cwd="/resolved/worktrees/actual",
            config_json={
                "execution_context": {
                    "instance": {
                        "id": "executor-actual",
                        "name": "Actual Runner",
                    },
                    "authority_instance": {
                        "id": "authority-actual",
                        "name": "Actual Authority",
                    },
                    "cwd": "/resolved/worktrees/actual",
                    "repositories": [
                        {
                            "repository_id": "repository-actual",
                            "repository_url": "https://example.invalid/acme/actual",
                            "checkout_path": "/resolved/checkouts/actual",
                            "worktree_path": "/resolved/worktrees/actual",
                            "branch": "pa/card-actual",
                            "base_sha": "abc123",
                        }
                    ],
                }
            },
        )

        result = compose_session_prompt(store, settings, session, "Do the work.")

        self.assertIn("Actual Runner (executor-actual)", result.text)
        self.assertIn("Actual Authority (authority-actual)", result.text)
        self.assertIn("/resolved/checkouts/actual", result.text)
        self.assertIn("/resolved/worktrees/actual", result.text)
        self.assertIn("pa/card-actual", result.text)
        self.assertIn("abc123", result.text)
        self.assertNotIn("Mac mini", result.text)
        audit = {item.key: item.audit_record() for item in result.prompts}
        self.assertEqual(
            audit["agent.context.execution"]["resolved_context"]["execution_instance"][
                "id"
            ],
            "executor-actual",
        )
        self.assertNotIn(
            "body",
            audit["agent.context.card"]["resolved_context"]["card"],
        )

    def test_large_card_context_is_trimmed_to_the_exact_wrapper_budget(self) -> None:
        store = MagicMock()
        card = Card(
            id="card-large",
            title="Large card",
            body="x" * 90_000,
        )
        store.get_card.return_value = card
        store.get_project.return_value = None
        settings = Settings(
            data_dir=Path(tempfile.mkdtemp()),
            instance_id="executor",
            instance_name="Executor",
        )
        session = AgentSession(
            id="session-large",
            agent_name="cursor",
            card_id=card.id,
            cwd="/resolved/worktree",
            config_json={
                "execution_context": {
                    "instance": {"id": "executor", "name": "Executor"},
                    "cwd": "/resolved/worktree",
                    "repositories": [],
                }
            },
        )
        message = "PA pull-request supervisor\n\n" + ("y" * 48_000)

        result = compose_session_prompt(store, settings, session, message)

        self.assertLessEqual(
            len(result.text),
            PROMPTS.character_limit("agent.message.wrapper", provider="cursor"),
        )
        card_prompt = next(
            prompt for prompt in result.prompts if prompt.key == "agent.context.card"
        )
        self.assertTrue(card_prompt.truncated)
        self.assertGreater(
            card_prompt.original_character_count, card_prompt.character_count
        )
        self.assertIn("truncated to fit provider context", card_prompt.text)

    def test_composition_failure_does_not_leave_session_prompting(self) -> None:
        async def run() -> tuple[bool, object, list[tuple[bool, str | None]]]:
            with tempfile.TemporaryDirectory() as tmp:
                store = MagicMock()
                store.get_card.return_value = None
                store.get_project.return_value = None
                settings = Settings(data_dir=Path(tmp), instance_id="executor")
                session = AgentSession(
                    id="session-limit",
                    agent_name="codex",
                    cwd=str(Path(tmp) / "workspace"),
                    config_json={
                        "execution_context": {
                            "instance": {"id": "executor", "name": "Executor"},
                            "cwd": str(Path(tmp) / "workspace"),
                            "repositories": [],
                        }
                    },
                )
                manager = AgentSessionManager(settings, store)
                manager.workspace_manager.renew_session = MagicMock()
                runtime = AgentSessionRuntime(manager, session)
                connection = MagicMock()
                connection.prompt = AsyncMock(return_value="end_turn")
                runtime.connection = connection

                observed: list[tuple[bool, str | None]] = []

                def inspect_composition(*args, **kwargs):
                    snapshot = runtime.to_session_snapshot()
                    observed.append(
                        (
                            runtime.prompting,
                            snapshot.in_flight.id if snapshot.in_flight else None,
                        )
                    )
                    return compose_session_prompt(*args, **kwargs)

                with (
                    patch(
                        "pa.instance.agent_session.compose_session_prompt",
                        side_effect=inspect_composition,
                    ),
                    self.assertRaises(PromptRenderError),
                ):
                    await runtime._run_prompt(
                        QueuedPrompt(
                            message="x" * 300_000,
                            session_id=session.id,
                            cwd=session.cwd,
                        )
                    )
                return runtime.prompting, connection.prompt, observed

        prompting, prompt, observed = asyncio.run(run())
        self.assertEqual(len(observed), 1)
        self.assertTrue(observed[0][0])
        self.assertIsNotNone(observed[0][1])
        self.assertFalse(prompting)
        prompt.assert_not_awaited()

    def test_provider_retry_does_not_duplicate_audit_or_user_events(self) -> None:
        async def run() -> tuple[AgentSession, list]:
            with tempfile.TemporaryDirectory() as tmp:
                store = MagicMock()
                store.next_transcript_seq.return_value = 1
                store.get_card.return_value = None
                store.get_project.return_value = None
                settings = Settings(data_dir=Path(tmp), instance_id="executor")
                session = AgentSession(
                    id="session-retry",
                    agent_name="codex",
                    cwd=str(Path(tmp) / "workspace"),
                    config_json={
                        "execution_context": {
                            "instance": {"id": "executor", "name": "Executor"},
                            "cwd": str(Path(tmp) / "workspace"),
                            "repositories": [],
                        }
                    },
                )
                manager = AgentSessionManager(settings, store)
                manager.workspace_manager.renew_session = MagicMock()
                runtime = AgentSessionRuntime(manager, session)
                connection = MagicMock()
                connection.prompt = AsyncMock(
                    side_effect=[RuntimeError("provider failed"), "end_turn"]
                )
                connection.last_usage = None
                runtime.connection = connection
                item = QueuedPrompt(
                    id="prompt-retry",
                    message="Retry this exact turn.",
                    session_id=session.id,
                    cwd=session.cwd,
                )

                with self.assertRaisesRegex(RuntimeError, "provider failed"):
                    await runtime._run_prompt(item)
                await runtime._run_prompt(item)
                events = [
                    event
                    for call in store.append_transcript_events.call_args_list
                    for event in call.args[0]
                ]
                return session, events

        session, events = asyncio.run(run())
        history = [
            entry
            for entry in session.config_json["prompt_audit"]
            if entry["prompt_id"] == "prompt-retry"
        ]
        self.assertEqual(len(history), 1)
        self.assertEqual(
            sum(event.event_type == "prompt_rendered" for event in events), 1
        )
        self.assertEqual(sum(event.event_type == "user_message" for event in events), 1)

    def test_runtime_sends_composed_prompt_and_persists_exact_versions(self) -> None:
        async def run() -> tuple[str, AgentSession, list]:
            with tempfile.TemporaryDirectory() as tmp:
                store = MagicMock()
                store.next_transcript_seq.return_value = 1
                store.get_card.return_value = None
                store.get_project.return_value = None
                settings = Settings(
                    data_dir=Path(tmp) / "data",
                    workspace_root=Path(tmp) / "workspaces",
                    instance_id="executor-real",
                    instance_name="Real Executor",
                )
                session = AgentSession(
                    id="session-real",
                    agent_name="codex",
                    cwd=str(Path(tmp) / "workspaces" / "session-real"),
                    config_json={
                        "execution_context": {
                            "instance": {
                                "id": "executor-real",
                                "name": "Real Executor",
                            },
                            "cwd": str(Path(tmp) / "workspaces" / "session-real"),
                            "repositories": [],
                        }
                    },
                )
                store.get_session.return_value = session
                manager = AgentSessionManager(settings, store)
                manager.workspace_manager.renew_session = MagicMock()
                runtime = AgentSessionRuntime(manager, session)
                connection = MagicMock()
                connection.prompt = AsyncMock(return_value="end_turn")
                connection.last_usage = None
                runtime.connection = connection

                await runtime._run_prompt(
                    QueuedPrompt(
                        message="Inspect {{ literal }} safely.",
                        session_id=session.id,
                        cwd=session.cwd,
                    )
                )
                sent = connection.prompt.await_args.args[0]
                events = [
                    event
                    for call in store.append_transcript_events.call_args_list
                    for event in call.args[0]
                ]
                return sent, session, events

        sent, session, events = asyncio.run(run())
        self.assertIn("Real Executor (executor-real)", sent)
        self.assertIn("Inspect {{ literal }} safely.", sent)
        history = session.config_json["prompt_audit"]
        keys = {item["key"] for item in history[-1]["prompts"]}
        self.assertIn("agent.context.execution", keys)
        self.assertIn("agent.context.data_safety", keys)
        self.assertIn("agent.context.browser", keys)
        self.assertIn("agent.message.wrapper", keys)
        versions = {item["key"]: item["version"] for item in history[-1]["prompts"]}
        self.assertEqual(versions["agent.context.execution"], 1)
        self.assertTrue(any(event.event_type == "prompt_rendered" for event in events))


class PromptSettingsAccessibilityTests(unittest.TestCase):
    def test_settings_exposes_search_scope_copy_and_synthetic_preview(self) -> None:
        template = (
            Path(__file__).parents[1]
            / "src"
            / "pa"
            / "server"
            / "templates"
            / "pages"
            / "settings.html"
        ).read_text()
        for marker in (
            "('prompts', 'Prompts')",
            'role="search"',
            'aria-label="Filter prompts"',
            'id="pa-prompt-search"',
            'id="pa-prompt-scope"',
            "data-copy-prompt=",
            'aria-live="polite"',
            "synthetic context",
            "Read-only; no override is configured",
            "reserved for context",
        ):
            with self.subTest(marker=marker):
                self.assertIn(marker, template)

    def test_catalog_previews_are_renderable_and_secret_free(self) -> None:
        rows = PROMPTS.catalog(provider="codex")
        self.assertEqual(len(rows), len(EXPECTED_PROMPT_KEYS))
        encoded = __import__("json").dumps(rows)
        self.assertNotIn("sk_", encoded)
        self.assertNotIn("github_pat_", encoded)
        self.assertTrue(all(row["preview"] for row in rows))


if __name__ == "__main__":
    unittest.main()
