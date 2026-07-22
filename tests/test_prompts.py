"""Regression coverage for PA's centralized operational prompt registry."""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

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
    "agent.context.browser",
    "agent.context.card",
    "agent.context.data_safety",
    "agent.context.execution",
    "agent.context.project",
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

    def test_composition_failure_does_not_leave_session_prompting(self) -> None:
        async def run() -> tuple[bool, object]:
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

                with self.assertRaises(PromptRenderError):
                    await runtime._run_prompt(
                        QueuedPrompt(
                            message="x" * 300_000,
                            session_id=session.id,
                            cwd=session.cwd,
                        )
                    )
                return runtime.prompting, connection.prompt

        prompting, prompt = asyncio.run(run())
        self.assertFalse(prompting)
        prompt.assert_not_awaited()

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
