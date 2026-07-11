"""Unit tests for ACP provider registry and selection cascade."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pa.acp.providers.base import ProviderConfigureBody
from pa.acp.providers.registry import DEFAULT_PROVIDER_ID, get_provider, list_provider_ids
from pa.acp.providers.resolve import resolve_agent_provider, resolve_provider_id
from pa.acp.surfaces import (
    SURFACE_CHAT_CARD,
    SURFACE_CHAT_DEFAULT,
    AgentInvocationContext,
    surface_for_label,
)
from pa.config import Settings
from pa.core.preferences import SurfaceAgentPrefs, get_preferences_store


class AcpProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self._tmp.name)
        self.settings = Settings(data_dir=self.data_dir, agent_provider="cursor")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_builtin_providers_registered(self) -> None:
        ids = set(list_provider_ids())
        self.assertIn("cursor", ids)
        self.assertIn("codex", ids)
        self.assertEqual(get_provider("cursor").display_name, "Cursor")

    def test_surface_for_label(self) -> None:
        self.assertEqual(surface_for_label("default"), SURFACE_CHAT_DEFAULT)
        self.assertEqual(surface_for_label("card:abc"), SURFACE_CHAT_CARD)
        self.assertEqual(surface_for_label(None, project_id="p1"), "project")

    def test_resolve_defaults_to_cursor(self) -> None:
        ctx = AgentInvocationContext(surface=SURFACE_CHAT_DEFAULT)
        pid, source = resolve_provider_id(self.settings, ctx)
        self.assertEqual(pid, "cursor")
        self.assertIn(source, {"instance", "default"})

    def test_resolve_user_overrides_instance(self) -> None:
        get_preferences_store(self.data_dir, user_id="alice").update(
            agent_provider="codex"
        )
        ctx = AgentInvocationContext(
            surface=SURFACE_CHAT_DEFAULT, principal_id="user:alice"
        )
        pid, source = resolve_provider_id(self.settings, ctx)
        self.assertEqual(pid, "codex")
        self.assertEqual(source, "user")

    def test_resolve_surface_overrides_user(self) -> None:
        get_preferences_store(self.data_dir, user_id="alice").update(
            agent_provider="cursor",
            agent_surfaces={
                SURFACE_CHAT_CARD: SurfaceAgentPrefs(provider="codex"),
            },
        )
        ctx = AgentInvocationContext(
            surface=SURFACE_CHAT_CARD, principal_id="user:alice"
        )
        pid, source = resolve_provider_id(self.settings, ctx)
        self.assertEqual(pid, "codex")
        self.assertEqual(source, "surface")

    def test_resolve_explicit_override_wins(self) -> None:
        ctx = AgentInvocationContext(
            surface=SURFACE_CHAT_DEFAULT,
            principal_id="user:alice",
            provider_override="codex",
        )
        pid, source = resolve_provider_id(self.settings, ctx)
        self.assertEqual(pid, "codex")
        self.assertEqual(source, "override")

    def test_resolve_project_tool_config(self) -> None:
        ctx = AgentInvocationContext(surface="project", project_id="p1")
        pid, source = resolve_provider_id(
            self.settings, ctx, project_tool_config={"agent_provider": "codex"}
        )
        self.assertEqual(pid, "codex")
        self.assertEqual(source, "project")

    def test_cursor_spawn_defaults(self) -> None:
        resolved = resolve_agent_provider(
            self.settings, AgentInvocationContext(surface=SURFACE_CHAT_DEFAULT)
        )
        self.assertEqual(resolved.provider_id, DEFAULT_PROVIDER_ID)
        self.assertEqual(resolved.spec.command, "agent")
        self.assertEqual(resolved.spec.args, ["acp"])

    def test_codex_spawn_without_override(self) -> None:
        settings = Settings(data_dir=self.data_dir, agent_provider="codex")
        resolved = resolve_agent_provider(
            settings, AgentInvocationContext(surface=SURFACE_CHAT_DEFAULT)
        )
        self.assertEqual(resolved.provider_id, "codex")
        cmd = resolved.spec.command
        self.assertTrue(
            cmd in {"codex-acp", "npx"}
            or cmd.endswith("codex-acp")
            or cmd.endswith("/npx")
            or cmd.endswith("\\npx"),
            f"unexpected command: {cmd!r}",
        )

    def test_command_override(self) -> None:
        settings = Settings(
            data_dir=self.data_dir,
            agent_provider="codex",
            agent_command="custom-acp",
            agent_args=["--flag"],
        )
        resolved = resolve_agent_provider(
            settings, AgentInvocationContext(surface=SURFACE_CHAT_DEFAULT)
        )
        self.assertEqual(resolved.spec.command, "custom-acp")
        self.assertEqual(resolved.spec.args, ["--flag"])

    def test_codex_configure_persists_meta(self) -> None:
        provider = get_provider("codex")
        status = provider.configure(
            self.data_dir,
            ProviderConfigureBody(
                env={"INITIAL_AGENT_MODE": "agent"},
                secrets={"CODEX_API_KEY": "sk-test"},
                no_browser=True,
            ),
        )
        self.assertEqual(status.id, "codex")
        meta = json.loads((self.data_dir / "agent_providers" / "codex.json").read_text())
        self.assertEqual(meta["env"]["NO_BROWSER"], "1")
        creds = json.loads((self.data_dir / "integrations" / "codex.json").read_text())
        self.assertEqual(creds["CODEX_API_KEY"], "sk-test")

    def test_unknown_provider_raises(self) -> None:
        with self.assertRaises(KeyError):
            get_provider("nope")


if __name__ == "__main__":
    unittest.main()
