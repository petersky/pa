"""Unit tests for ACP provider registry and selection cascade."""

from __future__ import annotations

import asyncio
import json
import os
import queue
import subprocess
import tempfile
import threading
import time
import tomllib
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException

from pa.acp.providers.base import (
    AgentProviderSpec,
    ProviderConfigureBody,
    ProviderStatus,
)
from pa.acp.providers.codex import (
    NPM_PACKAGE,
    CodexProvider,
    _codex_auth_state,
    _codex_auth_status,
)
from pa.acp.providers.cursor import CursorProvider, _cursor_auth_status
from pa.acp.providers.openinterpreter import (
    OpenInterpreterProvider,
    _run_official_installer,
    _repair_managed_config_if_needed,
    _spawn_args,
    preflight_session_start,
    provider_options_snapshot,
)
from pa.acp.errors import ProviderStartError, classify_acp_failure
from pa.acp.providers.metadata import ProviderMetadata, save_metadata
from pa.acp.providers.codex_auth import (
    CodexLoginJob,
    CodexLoginJobStore,
    LoginState,
    MAX_EVENTS,
    normalize_terminal_output,
    redact_login_output,
)
from pa.acp.providers.registry import (
    DEFAULT_PROVIDER_ID,
    get_provider,
    list_provider_ids,
    provider_catalog,
)
from pa.acp.providers.resolve import (
    list_provider_summaries_bounded,
    resolve_agent_provider,
    resolve_provider_id,
    resolve_surface_preferences,
)
from pa.acp.surfaces import (
    SURFACE_CHAT_CARD,
    SURFACE_CHAT_DEFAULT,
    AgentInvocationContext,
    surface_for_label,
)
from pa.config import Settings
from pa.core.async_runtime import BlockingQueueFull
from pa.core.preferences import SurfaceAgentPrefs, get_preferences_store
from pa.core.subprocesses import ProcessResult
from pa.modules.agent_providers import (
    LoginBody,
    ProviderActionGate,
    _run_provider_action,
    list_provider_catalog,
    start_provider_login,
)


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
        self.assertIn("openinterpreter", ids)
        self.assertEqual(get_provider("cursor").display_name, "Cursor")
        self.assertEqual(
            get_provider("openinterpreter").display_name, "OpenInterpreter"
        )

    def test_provider_catalog_lists_runtimes_without_status_probes(self) -> None:
        with patch.object(
            CursorProvider, "status", side_effect=AssertionError("status probed")
        ):
            catalog = provider_catalog()
        self.assertEqual(
            {item["id"] for item in catalog},
            {"cursor", "codex", "openinterpreter"},
        )
        self.assertEqual(
            {item["display_name"] for item in catalog},
            {"Cursor", "Codex", "OpenInterpreter"},
        )
        self.assertEqual(list_provider_catalog(), catalog)

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

    def test_surface_session_defaults_merge_global_and_user_fields(self) -> None:
        get_preferences_store(self.data_dir).update(
            agent_surfaces={
                SURFACE_CHAT_DEFAULT: SurfaceAgentPrefs(
                    provider="cursor",
                    model_id="global-model",
                    effort="medium",
                    config={"sandbox": "workspace", "shared": "global"},
                )
            }
        )
        get_preferences_store(self.data_dir, user_id="alice").update(
            agent_surfaces={
                SURFACE_CHAT_DEFAULT: SurfaceAgentPrefs(
                    provider="codex",
                    mode_id="code",
                    config={"shared": "user"},
                )
            }
        )

        resolved = resolve_surface_preferences(
            self.settings,
            AgentInvocationContext(
                surface=SURFACE_CHAT_DEFAULT, principal_id="user:alice"
            ),
        )

        self.assertEqual(resolved.provider, "codex")
        self.assertEqual(resolved.model_id, "global-model")
        self.assertEqual(resolved.mode_id, "code")
        self.assertEqual(resolved.effort, "medium")
        self.assertEqual(
            resolved.config, {"sandbox": "workspace", "shared": "user"}
        )

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
        self.assertEqual(Path(resolved.spec.command).name, "agent")
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

    def test_codex_spawn_resolves_npx_on_service_path(self) -> None:
        npx = Path("/opt/homebrew/bin/npx")

        def fake_resolve(name: str, **_kwargs: object) -> Path | None:
            if name == "npx":
                return npx
            return None

        with (
            patch(
                "pa.acp.providers.codex.resolve_executable",
                side_effect=fake_resolve,
            ),
            patch("pa.acp.providers.codex.shutil.which", return_value=None),
        ):
            spec = CodexProvider().resolve_spawn(data_dir=self.data_dir)
        self.assertEqual(spec.command, str(npx))
        self.assertEqual(spec.args[:2], ["-y", NPM_PACKAGE])

    def test_missing_provider_executable_is_classified(self) -> None:
        classified = classify_acp_failure(
            FileNotFoundError(2, "No such file or directory", "codex-acp"),
            provider_id="codex",
            stage="provider_spawn",
        )
        self.assertEqual(classified["code"], "provider_not_installed")
        self.assertIn("codex-acp", classified["message"])
        self.assertIn("PA_AGENT_COMMAND", classified["message"])
        self.assertTrue(classified["recoverable"])

    def test_openinterpreter_spawn_uses_acp_and_managed_home(self) -> None:
        settings = Settings(data_dir=self.data_dir, agent_provider="openinterpreter")
        resolved = resolve_agent_provider(
            settings, AgentInvocationContext(surface=SURFACE_CHAT_DEFAULT)
        )
        self.assertEqual(resolved.provider_id, "openinterpreter")
        self.assertEqual(resolved.spec.args, ["acp"])
        self.assertEqual(
            resolved.spec.env["INTERPRETER_HOME"],
            str(
                self.data_dir
                / "agent_providers"
                / "openinterpreter"
                / "home"
            ),
        )
        self.assertEqual(
            get_provider("openinterpreter")
            .resolve_spawn(
                data_dir=self.data_dir,
                extra_env={"INTERPRETER_HOME": "/tmp/not-allowed"},
            )
            .env["INTERPRETER_HOME"],
            str(
                self.data_dir
                / "agent_providers"
                / "openinterpreter"
                / "home"
            ),
        )

    def test_openinterpreter_configure_writes_model_config_and_isolates_secret(
        self,
    ) -> None:
        secret = "secret-never-return"
        provider = OpenInterpreterProvider()
        status = provider.configure(
            self.data_dir,
            ProviderConfigureBody(
                model="acme-coder",
                model_provider="acme",
                model_provider_name="Acme",
                model_provider_base_url="https://api.acme.example/v1",
                model_provider_env_key="ACME_API_KEY",
                model_provider_wire_api="chat",
                secrets={"ACME_API_KEY": secret},
            ),
        )

        config_path = (
            self.data_dir
            / "agent_providers"
            / "openinterpreter"
            / "home"
            / "config.toml"
        )
        config = tomllib.loads(config_path.read_text())
        self.assertEqual(config["model"], "acme-coder")
        self.assertEqual(config["model_provider"], "acme")
        self.assertEqual(
            config["model_providers"]["acme"],
            {
                "name": "Acme",
                "base_url": "https://api.acme.example/v1",
                "env_key": "ACME_API_KEY",
                "wire_api": "chat",
            },
        )
        credentials = json.loads(
            (self.data_dir / "integrations" / "openinterpreter.json").read_text()
        )
        self.assertEqual(credentials["ACME_API_KEY"], secret)
        public_status = status.model_dump_json()
        self.assertNotIn(secret, public_status)
        self.assertEqual(status.meta["credential_keys"], ["ACME_API_KEY"])
        self.assertEqual(
            provider.resolve_spawn(data_dir=self.data_dir).env["ACME_API_KEY"],
            secret,
        )

    def test_openinterpreter_builtin_provider_does_not_write_incomplete_override(
        self,
    ) -> None:
        provider = OpenInterpreterProvider()
        provider.configure(
            self.data_dir,
            ProviderConfigureBody(
                model="MiniMax-M2.5",
                model_provider="minimax-coding-plan",
                model_provider_env_key="MINIMAX_API_KEY",
                model_provider_wire_api="messages",
                secrets={"MINIMAX_API_KEY": "secret"},
            ),
        )
        config = tomllib.loads(
            (
                self.data_dir
                / "agent_providers"
                / "openinterpreter"
                / "home"
                / "config.toml"
            ).read_text()
        )
        self.assertEqual(
            config,
            {"model": "MiniMax-M2.5", "model_provider": "minimax-coding-plan"},
        )
        self.assertNotIn("model_providers", config)

    def test_openinterpreter_repairs_incomplete_model_provider_override(self) -> None:
        home = self.data_dir / "agent_providers" / "openinterpreter" / "home"
        home.mkdir(parents=True)
        (home / "config.toml").write_text(
            "# Managed by PA for the OpenInterpreter ACP provider.\n"
            'model = "MiniMax-M2.5"\n'
            'model_provider = "minimax-coding-plan"\n'
            "\n"
            '[model_providers."minimax-coding-plan"]\n'
            'env_key = "MINIMAX_API_KEY"\n'
            'wire_api = "messages"\n',
            encoding="utf-8",
        )
        save_metadata(
            self.data_dir,
            ProviderMetadata(
                provider_id="openinterpreter",
                configuration={
                    "model": "MiniMax-M2.5",
                    "model_provider": "minimax-coding-plan",
                    "model_provider_env_key": "MINIMAX_API_KEY",
                    "model_provider_wire_api": "messages",
                },
            ),
        )
        self.assertTrue(_repair_managed_config_if_needed(self.data_dir))
        config = tomllib.loads((home / "config.toml").read_text())
        self.assertEqual(
            config,
            {"model": "MiniMax-M2.5", "model_provider": "minimax-coding-plan"},
        )

    def test_openinterpreter_options_and_preflight_after_configure(self) -> None:
        provider = OpenInterpreterProvider()
        provider.configure(
            self.data_dir,
            ProviderConfigureBody(
                model="MiniMax-M2.5",
                model_provider="minimax-coding-plan",
                secrets={"MINIMAX_API_KEY": "secret"},
            ),
        )
        snap = provider_options_snapshot(self.data_dir)
        self.assertTrue(snap["supports_model_provider"])
        self.assertEqual(snap["model_provider"], "minimax-coding-plan")
        self.assertTrue(snap["models"]["availableModels"])
        self.assertTrue(snap["modes"]["availableModes"])
        self.assertTrue(snap["config_options"])
        self.assertTrue(
            any(item["id"] == "minimax-coding-plan" for item in snap["model_providers"])
        )
        with patch(
            "pa.acp.providers.openinterpreter.resolve_executable",
            return_value=Path("/test/openinterpreter-acp"),
        ):
            self.assertIsNone(preflight_session_start(self.data_dir))
        missing = preflight_session_start(
            Path(tempfile.mkdtemp()), model_provider="openai"
        )
        self.assertEqual(missing["code"], "auth_missing")
        classified = classify_acp_failure(
            ProviderStartError(missing), provider_id="openinterpreter"
        )
        self.assertEqual(classified["code"], "auth_missing")
        self.assertIn("-c", _spawn_args(model_provider="minimax-coding-plan"))

    def test_openinterpreter_rejects_unsafe_model_configuration(self) -> None:
        provider = OpenInterpreterProvider()
        with self.assertRaisesRegex(ValueError, "model_provider"):
            provider.configure(
                self.data_dir,
                ProviderConfigureBody(
                    model_provider='bad"]\nmalicious = "value',
                ),
            )
        with self.assertRaisesRegex(ValueError, "INTERPRETER_HOME"):
            provider.configure(
                self.data_dir,
                ProviderConfigureBody(env={"INTERPRETER_HOME": "/tmp/escape"}),
            )
        self.assertFalse(
            (
                self.data_dir
                / "agent_providers"
                / "openinterpreter"
                / "home"
                / "config.toml"
            ).exists()
        )

    def test_openinterpreter_install_uses_official_installer(self) -> None:
        provider = OpenInterpreterProvider()
        completed = subprocess.CompletedProcess(["sh", "install.sh"], 0, "ok", "")
        installed = Path("/opt/openinterpreter/interpreter")
        with (
            patch(
                "pa.acp.providers.openinterpreter.resolve_executable",
                side_effect=[None, installed],
            ),
            patch("pa.acp.providers.openinterpreter.shutil.which", return_value=None),
            patch(
                "pa.acp.providers.openinterpreter._run_official_installer",
                return_value=completed,
            ) as run_installer,
            patch(
                "pa.acp.providers.openinterpreter._version",
                return_value="interpreter 1.2.3",
            ),
        ):
            result = provider.install(self.data_dir)
        self.assertTrue(result.ok)
        self.assertEqual(result.command, str(installed))
        run_installer.assert_called_once_with(
            self.data_dir / "agent_providers" / "openinterpreter" / "home"
        )

    @unittest.skipIf(__import__("os").name == "nt", "Unix installer path")
    def test_openinterpreter_official_installer_is_bounded_and_noninteractive(
        self,
    ) -> None:
        response = MagicMock()
        response.geturl.return_value = "https://www.openinterpreter.com/install"
        response.read.return_value = b"#!/bin/sh\nexit 0\n"
        response.__enter__.return_value = response
        completed = subprocess.CompletedProcess(["sh", "install.sh"], 0, "", "")
        with (
            patch(
                "pa.acp.providers.openinterpreter.urllib.request.urlopen",
                return_value=response,
            ),
            patch(
                "pa.acp.providers.openinterpreter.subprocess.run",
                return_value=completed,
            ) as run,
        ):
            result = _run_official_installer(self.data_dir / "interpreter-home")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(run.call_args.args[0][0], "sh")
        self.assertEqual(
            run.call_args.kwargs["env"]["INTERPRETER_HOME"],
            str(self.data_dir / "interpreter-home"),
        )
        self.assertEqual(
            run.call_args.kwargs["env"]["OPEN_INTERPRETER_NONINTERACTIVE"], "1"
        )

    def test_openinterpreter_update_uses_self_update_command(self) -> None:
        provider = OpenInterpreterProvider()
        installed = Path("/opt/openinterpreter/interpreter")
        completed = subprocess.CompletedProcess(
            [str(installed), "update"], 0, "updated", ""
        )
        with (
            patch(
                "pa.acp.providers.openinterpreter.resolve_executable",
                return_value=installed,
            ),
            patch(
                "pa.acp.providers.openinterpreter.subprocess.run",
                return_value=completed,
            ) as run,
            patch(
                "pa.acp.providers.openinterpreter._version",
                return_value="interpreter 1.2.4",
            ),
        ):
            result = provider.update(self.data_dir)
        self.assertTrue(result.ok)
        self.assertEqual(result.version, "interpreter 1.2.4")
        self.assertEqual(run.call_args.args[0], [str(installed), "update"])

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
        meta = json.loads(
            (self.data_dir / "agent_providers" / "codex.json").read_text()
        )
        self.assertEqual(meta["env"]["NO_BROWSER"], "1")
        creds = json.loads((self.data_dir / "integrations" / "codex.json").read_text())
        self.assertEqual(creds["CODEX_API_KEY"], "sk-test")

    def test_codex_status_recognizes_chatgpt_login(self) -> None:
        completed = __import__("subprocess").CompletedProcess(
            ["codex", "login", "status"], 0, "Logged in using ChatGPT\n", ""
        )
        with patch("pa.acp.providers.codex.subprocess.run", return_value=completed):
            configured, method, message, error = _codex_auth_status(
                "/usr/bin/codex", creds={}, env={}
            )
        self.assertTrue(configured)
        self.assertEqual(method, "chatgpt_oauth")
        self.assertIn("ChatGPT", message)
        self.assertIsNone(error)

    def test_codex_status_prefers_target_api_key_without_exposing_it(self) -> None:
        secret = "sk-test-never-return"
        configured, method, message, error = _codex_auth_status(
            None, creds={"CODEX_API_KEY": secret}, env={}
        )
        self.assertTrue(configured)
        self.assertEqual(method, "api_key")
        self.assertNotIn(secret, message)
        self.assertIsNone(error)

    def test_codex_status_handles_timeout(self) -> None:
        import subprocess

        with patch(
            "pa.acp.providers.codex.subprocess.run",
            side_effect=subprocess.TimeoutExpired(["codex", "login", "status"], 10),
        ):
            configured, method, message, error = _codex_auth_status(
                "/usr/bin/codex", creds={}, env={}
            )
        self.assertFalse(configured)
        self.assertEqual(method, "unknown")
        self.assertIn("timed out", message)
        self.assertIn("timed out", error or "")

    def test_codex_status_handles_logout_and_malformed_credentials(self) -> None:
        import subprocess

        logged_out = subprocess.CompletedProcess(
            ["codex", "login", "status"], 1, "Not logged in\n", ""
        )
        malformed_secret = "refresh_token=must-not-leak"
        malformed = subprocess.CompletedProcess(
            ["codex", "login", "status"], 1, "", malformed_secret
        )
        with patch(
            "pa.acp.providers.codex.subprocess.run",
            side_effect=[logged_out, malformed],
        ):
            logged_out_status = _codex_auth_status("/usr/bin/codex", creds={}, env={})
            malformed_status = _codex_auth_status("/usr/bin/codex", creds={}, env={})
        self.assertEqual(logged_out_status[:2], (False, "none"))
        self.assertEqual(malformed_status[:2], (False, "unknown"))
        self.assertNotIn(malformed_secret, " ".join(str(v) for v in malformed_status))

    def test_codex_status_unknown_success_is_not_marked_configured(self) -> None:
        import subprocess

        unknown = subprocess.CompletedProcess(
            ["codex", "login", "status"], 0, "Future login method\n", ""
        )
        with patch("pa.acp.providers.codex.subprocess.run", return_value=unknown):
            configured, method, message, error = _codex_auth_status(
                "/usr/bin/codex", creds={}, env={}
            )
        self.assertFalse(configured)
        self.assertEqual(method, "unknown")
        self.assertIn("unknown", message)
        self.assertIsNone(error)

    def test_unscoped_process_api_key_does_not_mask_chatgpt_login(self) -> None:
        import subprocess

        chatgpt = subprocess.CompletedProcess(
            ["codex", "login", "status"], 0, "Logged in using ChatGPT\n", ""
        )
        with (
            patch.dict("os.environ", {"OPENAI_API_KEY": "unrelated-service-key"}),
            patch("pa.acp.providers.codex.subprocess.run", return_value=chatgpt),
        ):
            configured, method, _, _ = _codex_auth_status(
                "/usr/bin/codex", creds={}, env={}
            )
        self.assertTrue(configured)
        self.assertEqual(method, "chatgpt_oauth")

    def test_codex_chatgpt_login_normalizes_as_authenticated(self) -> None:
        auth = (True, "chatgpt_oauth", "Signed in with ChatGPT on the target.", None)
        self.assertEqual(
            _codex_auth_state(auth, codex_cli="/usr/bin/codex"),
            "authenticated",
        )

    def test_cursor_status_uses_supported_cli_and_api_key_paths(self) -> None:
        secret = "cursor-key-never-return"
        api_key = _cursor_auth_status(None, env={"CURSOR_API_KEY": secret})
        self.assertEqual(api_key[:3], ("authenticated", True, "api_key"))
        self.assertNotIn(secret, " ".join(str(value) for value in api_key))

        signed_in = subprocess.CompletedProcess(
            ["agent", "status"], 0, "Authenticated as operator@example.test", ""
        )
        signed_out = subprocess.CompletedProcess(
            ["agent", "status"], 1, "Not logged in", ""
        )
        malformed = subprocess.CompletedProcess(
            ["agent", "status"], 0, "future status payload", ""
        )
        with patch(
            "pa.acp.providers.cursor.subprocess.run",
            side_effect=[signed_in, signed_out, malformed],
        ) as run:
            authenticated = _cursor_auth_status("/usr/bin/agent", env={})
            logged_out = _cursor_auth_status("/usr/bin/agent", env={})
            unknown = _cursor_auth_status("/usr/bin/agent", env={})
        self.assertEqual(authenticated[:3], ("authenticated", True, "cursor_account"))
        self.assertEqual(logged_out[0], "signed_out")
        self.assertEqual(unknown[0], "unknown")
        self.assertEqual(run.call_args_list[0].args[0], ["/usr/bin/agent", "status"])

    def test_cursor_status_distinguishes_timeout_permission_and_unavailable(self) -> None:
        with patch(
            "pa.acp.providers.cursor.subprocess.run",
            side_effect=subprocess.TimeoutExpired(["agent", "status"], 3),
        ):
            timed_out = _cursor_auth_status("/usr/bin/agent", env={})
        with patch(
            "pa.acp.providers.cursor.subprocess.run",
            side_effect=PermissionError("secret credential path"),
        ):
            denied = _cursor_auth_status("/usr/bin/agent", env={})
        unavailable = _cursor_auth_status(None, env={})
        self.assertEqual(timed_out[0], "timed_out")
        self.assertEqual(denied[0], "probe_failed")
        self.assertNotIn("secret credential path", " ".join(str(v) for v in denied))
        self.assertEqual(unavailable[0], "unavailable")

    def test_provider_summaries_correlate_active_sessions_and_redact_failures(self) -> None:
        active = [
            SimpleNamespace(
                _closed=False,
                connected=True,
                session=SimpleNamespace(agent_name=provider_id),
            )
            for provider_id in ("codex", "cursor")
        ]
        manager = SimpleNamespace(list_runtimes=lambda: active)
        codex = SimpleNamespace(
            id="codex",
            display_name="Codex",
            status=lambda _data_dir: ProviderStatus(
                id="codex",
                display_name="Codex",
                installed=True,
                available=True,
                auth_configured=True,
                auth_state="authenticated",
                auth_method="chatgpt_oauth",
                auth_status="Signed in with ChatGPT on the target.",
                auth_evidence=["codex_cli_status"],
            ),
        )
        leaked = "refresh_token=must-never-leak"
        broken = SimpleNamespace(
            id="cursor",
            display_name="Cursor",
            status=lambda _data_dir: (_ for _ in ()).throw(PermissionError(leaked)),
        )
        with patch(
            "pa.acp.providers.resolve.list_providers", return_value=[codex, broken]
        ):
            summaries = asyncio.run(
                list_provider_summaries_bounded(
                    self.data_dir, manager=manager, timeout=0.1
                )
            )
        self.assertEqual(summaries[0]["auth_state"], "authenticated")
        self.assertEqual(summaries[0]["auth_method"], "chatgpt_oauth")
        self.assertEqual(summaries[0]["active_session_count"], 1)
        self.assertIn("active_acp_session", summaries[0]["auth_evidence"])
        self.assertEqual(summaries[1]["auth_state"], "authenticated")
        self.assertEqual(summaries[1]["direct_auth_state"], "probe_failed")
        self.assertEqual(summaries[1]["auth_method"], "active_acp_session")
        self.assertTrue(summaries[1]["available"])
        self.assertNotIn(leaked, json.dumps(summaries))

    def test_provider_action_failures_do_not_return_sensitive_command_output(self) -> None:
        secret = "refresh_token=provider-action-secret"
        failed = subprocess.CompletedProcess(["provider"], 7, secret, secret)

        with (
            patch(
                "pa.acp.providers.cursor.resolve_executable",
                return_value=Path("/usr/bin/agent"),
            ),
            patch("pa.acp.providers.cursor.subprocess.run", return_value=failed),
            patch("pa.acp.providers.cursor._version", return_value=None),
        ):
            cursor = CursorProvider().update(self.data_dir)

        def codex_which(command):
            return "/usr/bin/npm" if command == "npm" else None

        with (
            patch("pa.acp.providers.codex.shutil.which", side_effect=codex_which),
            patch("pa.acp.providers.codex.subprocess.run", return_value=failed),
        ):
            codex = CodexProvider().install(self.data_dir)

        with (
            patch(
                "pa.acp.providers.openinterpreter.resolve_executable",
                return_value=None,
            ),
            patch("pa.acp.providers.openinterpreter.shutil.which", return_value=None),
            patch(
                "pa.acp.providers.openinterpreter._run_official_installer",
                return_value=failed,
            ),
        ):
            interpreter = OpenInterpreterProvider().install(self.data_dir)

        payload = json.dumps(
            [
                cursor.model_dump(mode="json"),
                codex.model_dump(mode="json"),
                interpreter.model_dump(mode="json"),
            ]
        )
        self.assertNotIn(secret, payload)
        self.assertIn("exit 7", payload)

    def test_acp_probe_failure_redacts_exception_and_log(self) -> None:
        from pa.acp.providers.probe import probe_acp_initialize

        secret = "cookie=probe-secret"
        spec = AgentProviderSpec(
            id="test", display_name="Test", command="test-agent"
        )
        with (
            patch(
                "pa.acp.providers.probe._probe_async",
                new=AsyncMock(side_effect=RuntimeError(secret)),
            ),
            self.assertLogs("pa.acp.providers.probe", level="WARNING") as logs,
        ):
            result = probe_acp_initialize(spec)
        self.assertNotIn(secret, json.dumps(result))
        self.assertNotIn(secret, " ".join(logs.output))
        self.assertEqual(result["ok"], False)

    def test_provider_summaries_timeout_one_provider_without_blocking_another(self) -> None:
        def slow_status(_data_dir):
            time.sleep(0.05)
            return ProviderStatus(id="cursor", display_name="Cursor")

        slow = SimpleNamespace(id="cursor", display_name="Cursor", status=slow_status)
        ready = SimpleNamespace(
            id="codex",
            display_name="Codex",
            status=lambda _data_dir: ProviderStatus(
                id="codex",
                display_name="Codex",
                installed=True,
                available=True,
                auth_configured=True,
                auth_state="authenticated",
                auth_method="chatgpt_oauth",
            ),
        )
        with patch(
            "pa.acp.providers.resolve.list_providers", return_value=[slow, ready]
        ):
            summaries = asyncio.run(
                list_provider_summaries_bounded(self.data_dir, timeout=0.01)
            )
        self.assertEqual(summaries[0]["auth_state"], "timed_out")
        self.assertEqual(summaries[1]["auth_state"], "authenticated")

    def test_login_output_redacts_credentials_but_keeps_device_instructions(
        self,
    ) -> None:
        self.assertEqual(
            redact_login_output("access_token=very-secret"), "access_token=[redacted]"
        )
        self.assertIn(
            "[redacted]",
            redact_login_output("Bearer abcdefghijklmnopqrstuvwxyz0123456789"),
        )
        instructions = redact_login_output(
            "Open https://auth.openai.com/device and enter ABCD-EFGH"
        )
        self.assertIn("https://auth.openai.com/device", instructions)
        self.assertIn("ABCD-EFGH", instructions)
        authorization_instructions = redact_login_output(
            "Open the authorization page https://auth.openai.com/device and enter ABCD-EFGH"
        )
        self.assertIn("https://auth.openai.com/device", authorization_instructions)
        self.assertIn("ABCD-EFGH", authorization_instructions)

    def test_terminal_normalization_and_parser_handle_ansi_chunk_boundaries(self) -> None:
        store = CodexLoginJobStore(self.data_dir)
        job = store.create()
        job.state = LoginState.RUNNING
        capture = "\x1b[90mOpen https://auth.openai.com/device\x1b[0m\r\nenter ABCD-"
        store._consume_output(job, capture, capture)
        capture += "EFGH\x1b[94m\x1b[0m"
        store._consume_output(job, capture, "EFGH\x1b[94m\x1b[0m")
        self.assertEqual(job.verification_url, "https://auth.openai.com/device")
        self.assertEqual(job.user_code, "ABCD-EFGH")
        self.assertEqual(job.state, LoginState.WAITING_FOR_USER)
        self.assertNotIn("[90m", normalize_terminal_output(capture))

    def test_parser_accepts_chatgpt_four_five_device_codes(self) -> None:
        store = CodexLoginJobStore(self.data_dir)
        job = store.create()
        job.state = LoginState.RUNNING
        capture = (
            "Follow these steps to sign in with ChatGPT using device code authorization:\n"
            "1. Open this link in your browser and sign in to your account\n"
            "https://auth.openai.com/codex/device\n"
            "2. Enter this one-time code (expires in 15 minutes)\n"
            "DUKP-DTG49\n"
        )
        store._consume_output(job, capture, capture)
        self.assertEqual(job.verification_url, "https://auth.openai.com/codex/device")
        self.assertEqual(job.user_code, "DUKP-DTG49")
        self.assertEqual(job.state, LoginState.WAITING_FOR_USER)

    @unittest.skipIf(__import__("os").name == "nt", "Unix PTY capture")
    def test_login_uses_pty_to_surface_unterminated_device_instructions(self) -> None:
        script = self.data_dir / "fake-codex"
        script.write_text(
            "#!/bin/sh\nprintf '\\033[90mOpen https://auth.openai.com/device\\033[0m enter ABCD-EFGH'\n"
        )
        script.chmod(0o700)
        store = CodexLoginJobStore(self.data_dir)
        job = store.create()
        store.start(job, str(script))
        deadline = time.monotonic() + 3
        while not job.terminal and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(job.state, LoginState.SUCCEEDED)
        self.assertEqual(job.verification_url, "https://auth.openai.com/device")
        self.assertEqual(job.user_code, "ABCD-EFGH")

    @unittest.skipIf(__import__("os").name == "nt", "Unix process-group cleanup")
    def test_login_with_no_output_fails_actionably_and_reaps(self) -> None:
        script = self.data_dir / "silent-codex"
        script.write_text("#!/bin/sh\nsleep 5\n")
        script.chmod(0o700)
        store = CodexLoginJobStore(self.data_dir)
        job = store.create()
        with patch("pa.acp.providers.codex_auth.NO_OUTPUT_TIMEOUT_S", 0.1):
            store.start(job, str(script))
            deadline = time.monotonic() + 3
            while (
                not job.terminal or job.job_id in store._processes
            ) and time.monotonic() < deadline:
                time.sleep(0.01)
        self.assertEqual(job.state, LoginState.FAILED)
        self.assertIn("no device-login instructions", job.error or "")
        self.assertNotIn(job.job_id, store._processes)

    @unittest.skipIf(__import__("os").name == "nt", "Unix process-group cleanup")
    def test_login_with_non_actionable_output_times_out_and_reaps(self) -> None:
        script = self.data_dir / "spinner-codex"
        script.write_text("#!/bin/sh\nprintf 'Starting device login'\nsleep 5\n")
        script.chmod(0o700)
        store = CodexLoginJobStore(self.data_dir)
        job = store.create()
        with (
            patch("pa.acp.providers.codex_auth.NO_OUTPUT_TIMEOUT_S", 5),
            patch("pa.acp.providers.codex_auth.NO_INSTRUCTIONS_TIMEOUT_S", 0.1),
        ):
            store.start(job, str(script))
            deadline = time.monotonic() + 3
            while (
                not job.terminal or job.job_id in store._processes
            ) and time.monotonic() < deadline:
                time.sleep(0.01)
        self.assertEqual(job.state, LoginState.FAILED)
        self.assertIn("verification URL and code", job.error or "")
        self.assertNotIn(job.job_id, store._processes)

    def test_persisted_events_are_bounded_and_never_store_cli_output(self) -> None:
        secret = "access_token=never-persist-this"
        store = CodexLoginJobStore(self.data_dir)
        job = store.create()
        capture = "Open https://auth.openai.com/device and enter ABCD-EFGH " + secret
        for _ in range(MAX_EVENTS + 20):
            store._consume_output(job, capture, secret)
            store._event(job, "progress", secret)
        store._persist(job)
        persisted = (store.directory / f"{job.job_id}.json").read_text()
        self.assertLessEqual(len(job.events), MAX_EVENTS)
        self.assertNotIn("never-persist-this", persisted)

    def test_login_store_atomically_allows_only_one_active_job(self) -> None:
        store = CodexLoginJobStore(self.data_dir)
        barrier = threading.Barrier(2)

        def create_job():
            barrier.wait()
            try:
                return store.create()
            except ValueError:
                return None

        with ThreadPoolExecutor(max_workers=2) as executor:
            jobs = list(executor.map(lambda _: create_job(), range(2)))
        self.assertEqual(sum(job is not None for job in jobs), 1)

    def test_separate_worker_store_respects_active_disk_lease(self) -> None:
        first = CodexLoginJobStore(self.data_dir)
        job = first.create()
        second = CodexLoginJobStore(self.data_dir)
        self.assertEqual(second.latest_active().job_id, job.job_id)
        with self.assertRaisesRegex(ValueError, "already active"):
            second.create()

    def test_separate_worker_cancellation_reaches_process_owner(self) -> None:
        owner = CodexLoginJobStore(self.data_dir)
        job = owner.create()
        other_worker = CodexLoginJobStore(self.data_dir)
        other_worker.cancel(job.job_id)
        owner._refresh_cancelled(job)
        self.assertEqual(job.state, LoginState.CANCELLED)

    def test_completion_does_not_overwrite_cross_worker_cancellation(self) -> None:
        owner = CodexLoginJobStore(self.data_dir)
        job = owner.create()
        other_worker = CodexLoginJobStore(self.data_dir)
        other_worker.cancel(job.job_id)
        owner._finish(job, LoginState.SUCCEEDED, "complete")
        self.assertEqual(job.state, LoginState.CANCELLED)
        reloaded = CodexLoginJobStore(self.data_dir).get(job.job_id)
        self.assertEqual(reloaded.state, LoginState.CANCELLED)

    def test_dead_owner_job_is_interrupted_before_expiry(self) -> None:
        first = CodexLoginJobStore(self.data_dir)
        job = first.create()
        job.owner_pid = 999_999_999
        first._persist(job)
        recovered = CodexLoginJobStore(self.data_dir).get(job.job_id)
        self.assertEqual(recovered.state, LoginState.INTERRUPTED)

    def test_cancel_before_process_registration_still_terminates_and_reaps(
        self,
    ) -> None:
        store = CodexLoginJobStore(self.data_dir)
        job = store.create()
        constructing = threading.Event()
        release = threading.Event()
        reaped = threading.Event()

        class FakeStdout:
            def readline(self):
                return ""

            def __iter__(self):
                return iter(())

        class FakeProcess:
            stdout = FakeStdout()
            pid = 12345
            returncode = None

            def poll(self):
                return self.returncode

            def wait(self, timeout=None):
                reaped.set()
                return self.returncode

        class FakeSelector:
            def register(self, *args):
                return None

            def select(self, timeout=None):
                return []

            def close(self):
                return None

        process = FakeProcess()

        def construct(*args, **kwargs):
            constructing.set()
            release.wait(timeout=2)
            return process

        def terminate(proc):
            proc.returncode = -15
            proc.wait(timeout=3)

        with (
            patch(
                "pa.acp.providers.codex_auth.subprocess.Popen", side_effect=construct
            ),
            patch(
                "pa.acp.providers.codex_auth.selectors.DefaultSelector", FakeSelector
            ),
            patch(
                "pa.acp.providers.codex_auth._terminate_process", side_effect=terminate
            ) as terminate_mock,
        ):
            store.start(job, "/custom/codex")
            self.assertTrue(constructing.wait(timeout=2))
            store.cancel(job.job_id)
            release.set()
            self.assertTrue(reaped.wait(timeout=2))
            deadline = time.monotonic() + 2
            while job.job_id in store._processes and time.monotonic() < deadline:
                time.sleep(0.01)
        terminate_mock.assert_called()
        self.assertEqual(job.state, LoginState.CANCELLED)

    def test_cancel_before_worker_start_never_launches_codex(self) -> None:
        store = CodexLoginJobStore(self.data_dir)
        job = store.create()
        store.cancel(job.job_id)
        with patch("pa.acp.providers.codex_auth.subprocess.Popen") as popen:
            store._run(job.job_id, "/custom/codex")
        popen.assert_not_called()
        self.assertEqual(job.state, LoginState.CANCELLED)

    def test_sigkill_fallback_reaps_login_process(self) -> None:
        import subprocess

        from pa.acp.providers.codex_auth import _terminate_process

        process = MagicMock()
        process.pid = 12345
        process.poll.return_value = None
        process.wait.side_effect = [
            subprocess.TimeoutExpired(["codex"], 3),
            -9,
        ]
        with patch("pa.acp.providers.codex_auth.os.killpg") as killpg:
            _terminate_process(process)
        self.assertEqual(process.wait.call_count, 2)
        self.assertEqual(killpg.call_count, 2)

    def test_windows_termination_uses_process_methods_and_reaps(self) -> None:
        import subprocess

        from pa.acp.providers.codex_auth import _terminate_process

        process = MagicMock()
        process.poll.return_value = None
        process.wait.side_effect = [subprocess.TimeoutExpired(["codex"], 3), -9]
        with patch("pa.acp.providers.codex_auth.os.name", "nt"):
            _terminate_process(process)
        process.terminate.assert_called_once()
        process.kill.assert_called_once()
        self.assertEqual(process.wait.call_count, 2)

    def test_pipe_read_timeout_does_not_block_lifecycle_checks(self) -> None:
        store = CodexLoginJobStore(self.data_dir)
        output: queue.Queue[str] = queue.Queue()
        started = time.monotonic()
        chunk = store._read_ready(
            MagicMock(), None, None, output, timeout=0.01
        )
        self.assertEqual(chunk, "")
        self.assertLess(time.monotonic() - started, 0.5)

    def test_queued_pipe_instructions_are_consumed_before_timeouts(self) -> None:
        store = CodexLoginJobStore(self.data_dir)
        job = store.create()

        stream = MagicMock()
        stream.read1.side_effect = [
            b"Open https://auth.openai.com/device enter ABCD-EFGH",
            b"",
        ]
        process = MagicMock()
        process.stdout = stream
        process.pid = 12345
        process.poll.side_effect = [None, 0]
        process.wait.return_value = 0
        with (
            patch("pa.acp.providers.codex_auth.os.name", "nt"),
            patch(
                "pa.acp.providers.codex_auth.subprocess.CREATE_NEW_PROCESS_GROUP",
                0,
                create=True,
            ),
            patch("pa.acp.providers.codex_auth.subprocess.Popen", return_value=process),
            patch("pa.acp.providers.codex_auth.NO_OUTPUT_TIMEOUT_S", 0),
            patch("pa.acp.providers.codex_auth.NO_INSTRUCTIONS_TIMEOUT_S", 0),
        ):
            store._run(job.job_id, "/custom/codex")

        self.assertEqual(job.verification_url, "https://auth.openai.com/device")
        self.assertEqual(job.user_code, "ABCD-EFGH")
        self.assertEqual(job.state, LoginState.SUCCEEDED)

    @unittest.skipIf(__import__("os").name == "nt", "Unix PTY setup")
    def test_runner_deadline_refreshes_persisted_expiry(self) -> None:
        store = CodexLoginJobStore(self.data_dir)
        job = store.create()
        original_expiry = datetime.fromisoformat(job.expires_at)
        job.expires_at = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()

        process = MagicMock()
        process.pid = 12345
        process.poll.return_value = 0
        process.wait.return_value = 0
        with patch("pa.acp.providers.codex_auth.subprocess.Popen", return_value=process):
            store._run(job.job_id, "/custom/codex")

        self.assertGreater(datetime.fromisoformat(job.expires_at), original_expiry)
        self.assertEqual(job.state, LoginState.SUCCEEDED)

    def test_resolve_codex_cli_honors_configured_executable(self) -> None:
        from pa.acp.providers.codex_auth import resolve_codex_cli

        configured = self.data_dir / "custom-codex"
        configured.write_text("#!/bin/sh\n")
        configured.chmod(0o700)
        self.assertEqual(resolve_codex_cli(str(configured)), str(configured))

    def test_login_store_marks_active_snapshot_interrupted_on_restart(self) -> None:
        directory = self.data_dir / "agent_provider_jobs" / "codex"
        directory.mkdir(parents=True)
        job = CodexLoginJob(
            job_id="job-1",
            state=LoginState.WAITING_FOR_USER,
            created_at="2026-01-01T00:00:00+00:00",
            updated_at="2026-01-01T00:00:00+00:00",
            expires_at="2026-01-01T00:10:00+00:00",
            timeout_seconds=600,
        )
        (directory / "job-1.json").write_text(job.model_dump_json())
        loaded = CodexLoginJobStore(self.data_dir).get("job-1")
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.state, LoginState.INTERRUPTED)

    def test_restart_recovery_cleans_recorded_login_process_group(self) -> None:
        directory = self.data_dir / "agent_provider_jobs" / "codex"
        directory.mkdir(parents=True)
        now = datetime.now(UTC)
        job = CodexLoginJob(
            job_id="job-orphan",
            state=LoginState.RUNNING,
            created_at=now.isoformat(),
            updated_at=now.isoformat(),
            expires_at=(now + timedelta(minutes=10)).isoformat(),
            timeout_seconds=600,
            owner_pid=999_999_999,
            process_pid=4242,
        )
        (directory / "job-orphan.json").write_text(job.model_dump_json())
        with patch("pa.acp.providers.codex_auth._terminate_orphan_group") as cleanup:
            loaded = CodexLoginJobStore(self.data_dir).get(job.job_id)
        cleanup.assert_called_once_with(4242)
        self.assertEqual(loaded.state, LoginState.INTERRUPTED)

    def test_login_api_requires_explicit_consent_without_starting_process(self) -> None:
        request = MagicMock()
        request.app.state.ctx.settings.data_dir = self.data_dir
        with (
            patch("pa.modules.agent_providers.resolve_codex_cli") as resolve,
            self.assertRaises(HTTPException) as raised,
        ):
            asyncio.run(
                start_provider_login(request, "codex", LoginBody(consent=False))
            )
        self.assertEqual(raised.exception.status_code, 400)
        resolve.assert_not_called()

    def test_login_api_missing_cli_is_actionable(self) -> None:
        request = MagicMock()
        request.app.state.ctx.settings.data_dir = self.data_dir
        runtime = MagicMock()

        def run_blocking(_operation, call, *args, **kwargs):
            kwargs.pop("timeout", None)
            return call(*args, **kwargs)

        runtime.run_blocking = AsyncMock(side_effect=run_blocking)
        request.app.state.ctx.require_service.return_value = runtime
        with (
            patch("pa.modules.agent_providers.resolve_codex_cli", return_value=None),
            self.assertRaises(HTTPException) as raised,
        ):
            asyncio.run(
                start_provider_login(request, "codex", LoginBody(consent=True))
            )
        self.assertEqual(raised.exception.status_code, 409)
        self.assertIn("not installed", str(raised.exception.detail))

    def test_provider_action_uses_observed_process_result(self) -> None:
        async def observe(_operation, awaitable, *, timeout):
            self.assertEqual(timeout, 65.0)
            return await awaitable

        runtime = SimpleNamespace(observe=observe)
        process_result = ProcessResult(
            args=("python",),
            returncode=0,
            stdout='PA_PROVIDER_RESULT={"ok":true,"provider_id":"codex"}\n',
            stderr="",
        )
        with patch(
            "pa.modules.agent_providers.run_process",
            new=AsyncMock(return_value=process_result),
        ) as process:
            result = asyncio.run(
                _run_provider_action(
                    self.data_dir,
                    "codex",
                    "probe",
                    timeout=60.0,
                    async_runtime=runtime,
                )
            )
        self.assertTrue(result["ok"])
        self.assertEqual(process.await_count, 1)

    def test_provider_action_gate_bounds_active_and_queued_work(self) -> None:
        async def exercise() -> None:
            gate = ProviderActionGate(max_active=1, max_queue=1)
            release = asyncio.Event()
            entered = asyncio.Event()

            async def hold() -> None:
                async with gate.slot():
                    entered.set()
                    await release.wait()

            async def wait() -> None:
                async with gate.slot():
                    return

            active = asyncio.create_task(hold())
            await entered.wait()
            queued = asyncio.create_task(wait())
            await asyncio.sleep(0)
            self.assertEqual(gate.snapshot()["queued"], 1)
            with self.assertRaises(BlockingQueueFull):
                async with gate.slot():
                    pass
            release.set()
            await asyncio.gather(active, queued)
            self.assertEqual(gate.snapshot()["active"], 0)

        asyncio.run(exercise())

    def test_probe_passes_isolated_child_environment(self) -> None:
        from pa.acp.providers.probe import _probe_async

        key = "PA_TEST_PROBE_ENV_ISOLATION"
        spec = AgentProviderSpec(
            id="test",
            display_name="Test",
            command="test-agent",
            env={key: "child-only"},
        )
        connection = SimpleNamespace(
            initialize=AsyncMock(
                return_value=SimpleNamespace(
                    agent_capabilities={},
                    auth_methods=[],
                )
            )
        )
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=(connection, object()))
        context.__aexit__ = AsyncMock(return_value=None)
        captured: dict = {}

        def spawn(_client, _command, *_args, **kwargs):
            captured.update(kwargs)
            return context

        with (
            patch.dict(os.environ, {}, clear=False),
            patch("pa.acp.transport.spawn_agent", side_effect=spawn),
            patch("pa.packaging.paths.resolve_executable", return_value=None),
        ):
            os.environ.pop(key, None)
            result = asyncio.run(_probe_async(spec, timeout=1.0))
            self.assertNotIn(key, os.environ)

        self.assertTrue(result["ok"])
        self.assertEqual(captured["env"][key], "child-only")

    def test_runtime_lifecycle_invalidates_local_provider_snapshot(self) -> None:
        from pa.fleet.overview import cache_for, field
        from pa.instance.agent_session import AgentSessionManager

        cache = cache_for(self.data_dir)
        cache.put(
            self.settings.instance_id,
            "providers",
            field(
                "fresh",
                [{"id": "codex", "auth_state": "authenticated"}],
                observed_at="2026-07-25T12:00:00+00:00",
            ),
        )
        manager = AgentSessionManager(self.settings, MagicMock())
        manager._invalidate_provider_overview()
        self.assertIsNone(cache.get(self.settings.instance_id, "providers"))

    def test_unknown_provider_raises(self) -> None:
        with self.assertRaises(KeyError):
            get_provider("nope")

    def test_put_agent_preferences_merges_global_surfaces(self) -> None:
        from pa.modules.agent_chat import PreferencesBody, put_agent_preferences

        get_preferences_store(self.data_dir).update(
            agent_surfaces={
                "chat.card": SurfaceAgentPrefs(provider="cursor"),
                "execution": SurfaceAgentPrefs(provider="codex"),
            }
        )
        request = MagicMock()
        request.app.state.ctx.settings.data_dir = self.data_dir
        with patch("pa.modules.agent_chat._user_id", return_value="alice"):
            put_agent_preferences(
                request,
                PreferencesBody(
                    agent_surfaces={
                        "chat.default": {
                            "provider": "codex",
                            "model_id": "gpt-5",
                        }
                    },
                    scope="global",
                ),
            )
        prefs = get_preferences_store(self.data_dir).load()
        self.assertEqual(prefs.agent_surfaces["chat.card"].provider, "cursor")
        self.assertEqual(prefs.agent_surfaces["execution"].provider, "codex")
        self.assertEqual(prefs.agent_surfaces["chat.default"].provider, "codex")
        self.assertEqual(prefs.agent_surfaces["chat.default"].model_id, "gpt-5")


if __name__ == "__main__":
    unittest.main()
