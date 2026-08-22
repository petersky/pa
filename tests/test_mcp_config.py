import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from pa.acp.environment import (
    ASSIGNED_SERVICE_AUTHORITY_INSTANCE_ENV,
    ASSIGNED_SERVICE_AUTHORITY_URL_ENV,
    ASSIGNED_SERVICE_CREDENTIAL_ENV,
    ASSIGNED_SERVICE_DISPATCH_ENV,
    ASSIGNED_SERVICE_MODE_ENV,
    ASSIGNED_SERVICE_SESSION_ENV,
    assigned_service_mcp_environment,
    inject_agent_github_environment,
    sanitize_provider_environment,
)
from pa.acp.mcp_config import (
    OwnerChannelError,
    apply_codex_owner_sandbox_environment,
    merge_codex_owner_sandbox_config,
    owner_endpoint,
    owner_sandbox_directories,
    pa_mcp_servers,
    probe_owner_channel,
    probe_pa_mcp_stdio,
)
from pa.auth.users import UserDirectory
from pa.config import Settings
from pa.server.listeners import owner_socket_path


class PaMcpServersTests(unittest.TestCase):
    def _settings(self, root: str) -> Settings:
        return Settings(
            data_dir=Path(root),
            instance_id="owner-instance",
            host="127.0.0.1",
            port=9123,
            agent_enabled=False,
        )

    def _owner_env(self, settings: Settings) -> dict[str, str]:
        return {
            "PA_DATA_DIR": str(settings.data_dir),
            "PA_LOCAL_API_URL": "http://pa-owner",
            "PA_LOCAL_API_ENDPOINT_TYPE": "unix",
            "PA_LOCAL_API_SOCKET": str(owner_socket_path(settings)),
            "PA_LOCAL_API_TOKEN": (
                UserDirectory(settings.data_dir).ensure_default_user().cli_token
            ),
            "PA_INSTANCE_ID": "owner-instance",
        }

    def test_forwards_attached_browser_environment(self):
        browser_env = {
            "PA_BROWSER_CDP_URL": "http://127.0.0.1:9222",
            "PA_BROWSER_TARGET_ID": "target-1",
            "PA_BROWSER_ATTACHMENT_ID": "attachment-1",
            "PA_BROWSER_SESSION_ID": "session-1",
        }

        with tempfile.TemporaryDirectory() as tmp:
            settings = self._settings(tmp)
            with patch.dict(
                os.environ, {**browser_env, "PA_OWNER_API_URL": ""}, clear=False
            ):
                server = pa_mcp_servers(settings)[0]
                expected = {**self._owner_env(settings), **browser_env}

            self.assertEqual(
                {item.name: item.value for item in server.env},
                expected,
            )
            self.assertEqual(server.command, sys.executable)
            self.assertEqual(server.args, ["-m", "pa", "mcp"])

    def test_pins_owner_environment_when_browser_is_detached(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = self._settings(tmp)
            with patch.dict(os.environ, {}, clear=True):
                server = pa_mcp_servers(settings)[0]
                expected = self._owner_env(settings)

            self.assertEqual(
                {item.name: item.value for item in server.env},
                expected,
            )

    def test_forwards_session_id_without_attached_browser(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = self._settings(tmp)
            with patch.dict(
                os.environ, {"PA_BROWSER_SESSION_ID": "session-1"}, clear=True
            ):
                server = pa_mcp_servers(settings)[0]
                expected = {
                    **self._owner_env(settings),
                    "PA_BROWSER_SESSION_ID": "session-1",
                }

            self.assertEqual(
                {item.name: item.value for item in server.env},
                expected,
            )

    def test_assigned_session_descriptor_contains_only_nonsecret_binding(self):
        private = assigned_service_mcp_environment(
            dispatch_id="dispatch-1",
            session_id="session-1",
        )
        browser_env = {
            "PA_BROWSER_CDP_URL": "http://127.0.0.1:9222",
            "PA_BROWSER_TARGET_ID": "target-1",
            "PA_BROWSER_ATTACHMENT_ID": "attachment-1",
            "PA_BROWSER_SESSION_ID": "browser-session-1",
        }
        with tempfile.TemporaryDirectory() as tmp:
            settings = self._settings(tmp)
            with (
                patch.dict(os.environ, {"PA_OWNER_API_URL": ""}, clear=True),
                patch("pa.acp.mcp_config.UserDirectory") as user_directory,
            ):
                server = pa_mcp_servers(
                    settings,
                    session_environment=browser_env,
                    private_environment=private,
                )[0]
                expected_socket = str(owner_socket_path(settings, os.environ))

        environment = {item.name: item.value for item in server.env}
        self.assertEqual(
            environment,
            {
                "PA_DATA_DIR": str(settings.data_dir),
                "PA_LOCAL_API_URL": "http://pa-owner",
                "PA_LOCAL_API_ENDPOINT_TYPE": "unix",
                "PA_LOCAL_API_SOCKET": expected_socket,
                "PA_INSTANCE_ID": "owner-instance",
                **private,
            },
        )
        user_directory.assert_not_called()
        self.assertNotIn("PA_LOCAL_API_TOKEN", environment)
        for name in browser_env:
            self.assertNotIn(name, environment)
        for name in (
            ASSIGNED_SERVICE_CREDENTIAL_ENV,
            ASSIGNED_SERVICE_AUTHORITY_URL_ENV,
            ASSIGNED_SERVICE_AUTHORITY_INSTANCE_ENV,
        ):
            self.assertNotIn(name, environment)

    def test_private_environment_rejects_legacy_assigned_secrets(self):
        binding = assigned_service_mcp_environment(
            dispatch_id="dispatch-1",
            session_id="session-1",
        )
        forbidden = {
            ASSIGNED_SERVICE_CREDENTIAL_ENV: "paas1.must-not-serialize",
            ASSIGNED_SERVICE_AUTHORITY_URL_ENV: "https://authority.test",
            ASSIGNED_SERVICE_AUTHORITY_INSTANCE_ENV: "authority-instance",
        }
        with tempfile.TemporaryDirectory() as tmp:
            settings = self._settings(tmp)
            for name, value in forbidden.items():
                with (
                    self.subTest(name=name),
                    self.assertRaisesRegex(
                        ValueError,
                        "must not serialize credentials or authority data",
                    ),
                ):
                    pa_mcp_servers(
                        settings,
                        private_environment={**binding, name: value},
                    )

    def test_session_environment_cannot_smuggle_assigned_binding_or_secrets(self):
        binding = assigned_service_mcp_environment(
            dispatch_id="dispatch-1",
            session_id="session-1",
        )
        with tempfile.TemporaryDirectory() as tmp:
            settings = self._settings(tmp)
            server = pa_mcp_servers(
                settings,
                session_environment={
                    ASSIGNED_SERVICE_CREDENTIAL_ENV: "must-not-forward",
                    ASSIGNED_SERVICE_AUTHORITY_URL_ENV: "https://forged.test",
                    ASSIGNED_SERVICE_AUTHORITY_INSTANCE_ENV: "forged",
                    ASSIGNED_SERVICE_MODE_ENV: "0",
                    ASSIGNED_SERVICE_DISPATCH_ENV: "forged-dispatch",
                    ASSIGNED_SERVICE_SESSION_ENV: "forged-session",
                },
                private_environment=binding,
            )[0]
        environment = {item.name: item.value for item in server.env}
        self.assertEqual(
            {name: environment[name] for name in binding},
            binding,
        )
        for name in (
            ASSIGNED_SERVICE_CREDENTIAL_ENV,
            ASSIGNED_SERVICE_AUTHORITY_URL_ENV,
            ASSIGNED_SERVICE_AUTHORITY_INSTANCE_ENV,
        ):
            self.assertNotIn(name, environment)

    def test_provider_sanitizer_strips_new_and_legacy_assignment_fields(self):
        assignment_environment = {
            **assigned_service_mcp_environment(
                dispatch_id="dispatch-1",
                session_id="session-1",
            ),
            ASSIGNED_SERVICE_CREDENTIAL_ENV: "paas1.private",
            ASSIGNED_SERVICE_AUTHORITY_URL_ENV: "https://authority.test",
            ASSIGNED_SERVICE_AUTHORITY_INSTANCE_ENV: "authority-instance",
        }

        self.assertEqual(
            sanitize_provider_environment(
                {**assignment_environment, "PUBLIC_PROVIDER_VALUE": "visible"}
            ),
            {"PUBLIC_PROVIDER_VALUE": "visible"},
        )

    def test_provider_sanitizer_strips_all_ambient_github_tokens(self):
        environment = sanitize_provider_environment(
            {
                "PA_GITHUB_TOKEN": "pa-secret",
                "PA_GITHUB_WEBHOOK_SECRET": "webhook-secret",
                "GH_TOKEN": "ambient-gh-secret",
                "GITHUB_TOKEN": "ambient-github-secret",
                "SAFE": "yes",
            },
            {"PA_GITHUB_TOKEN": "override", "GH_TOKEN": "override"},
        )
        self.assertEqual(environment, {"SAFE": "yes"})

    def test_managed_github_token_injection_is_explicit_and_non_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            integration = data_dir / "integrations"
            integration.mkdir()
            (integration / "github.json").write_text(
                json.dumps(
                    {
                        "token": "managed-secret",
                        "allowed_repositories": ["petersky/pa"],
                    }
                )
            )
            disabled, source = inject_agent_github_environment(
                {"SAFE": "yes"}, Settings(data_dir=data_dir)
            )
            self.assertEqual(disabled, {"SAFE": "yes"})
            self.assertEqual(source, "disabled")

            enabled, source = inject_agent_github_environment(
                {"SAFE": "yes"},
                Settings(data_dir=data_dir, agent_github_token_enabled=True),
            )
            self.assertEqual(enabled["GH_TOKEN"], "managed-secret")
            self.assertNotIn("PA_GITHUB_TOKEN", enabled)
            self.assertEqual(source, "instance_file")

    def test_enabled_injection_without_credential_preserves_oauth_environment(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(
            os.environ, {}, clear=True
        ):
            environment, source = inject_agent_github_environment(
                {"PATH": "/bin"},
                Settings(data_dir=Path(tmp), agent_github_token_enabled=True),
            )
        self.assertEqual(environment, {"PATH": "/bin"})
        self.assertEqual(source, "missing")

    def test_owner_endpoint_is_independent_of_web_binds(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = self._settings(tmp)
            paths = set()
            for host in ("0.0.0.0", "::", "100.78.2.112", "localhost"):
                settings.host = host
                with patch.dict(os.environ, {}, clear=True):
                    endpoint = owner_endpoint(settings)
                self.assertEqual(
                    (endpoint.url, endpoint.kind), ("http://pa-owner", "unix")
                )
                paths.add(endpoint.uds)
            self.assertEqual(len(paths), 1)

    def test_probe_classifies_owner_failures(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = self._settings(tmp)
            request = httpx.Request("GET", "http://127.0.0.1:9123/api/ready")
            cases = (
                (
                    401,
                    {},
                    "authentication_rejected",
                ),
                (404, {}, "api_incompatible"),
                (503, {}, "api_not_ready"),
                (500, {}, "api_error"),
                (200, {}, "identity_missing"),
                (200, {"X-PA-Instance-ID": "other-instance"}, "instance_mismatch"),
            )
            for status, headers, classification in cases:
                response = httpx.Response(
                    status, headers=headers, request=request, json={}
                )
                with (
                    patch("pa.acp.mcp_config._get_ready", return_value=response),
                    self.assertRaises(OwnerChannelError) as raised,
                ):
                    probe_owner_channel(settings, timeout=0)
                self.assertEqual(raised.exception.classification, classification)

    def test_probe_accepts_ready_identity_verified_owner(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = self._settings(tmp)
            response = httpx.Response(
                200,
                headers={"X-PA-Instance-ID": "owner-instance"},
                request=httpx.Request("GET", "http://pa-owner/api/ready"),
                json={"status": "ready"},
            )
            with (
                patch.dict(os.environ, {"PA_OWNER_API_URL": ""}, clear=False),
                patch("pa.acp.mcp_config._get_ready", return_value=response),
            ):
                result = probe_owner_channel(settings, timeout=0)
            self.assertEqual(result, {"state": "connected", "endpoint_type": "unix"})

    def test_stdio_mcp_smoke_initializes_lists_tools_and_shuts_down(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = self._settings(tmp)
            with patch.dict(
                os.environ,
                {
                    "PA_OWNER_API_URL": "",
                    "PA_OWNER_SOCKET": str(Path(tmp) / "owner.sock"),
                },
                clear=False,
            ):
                result = probe_pa_mcp_stdio(settings, timeout=20)

            self.assertEqual(result["state"], "connected")
            self.assertEqual(result["classification"], "ok")
            self.assertGreater(result["tool_count"], 0)


class CodexOwnerSandboxConfigTests(unittest.TestCase):
    def test_owner_sandbox_directories_use_socket_parent(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="owner-instance",
                host="127.0.0.1",
                port=9123,
                agent_enabled=False,
            )
            socket = Path(tmp) / "runtime" / "owner.sock"
            with patch.dict(
                os.environ,
                {"PA_OWNER_API_URL": "", "PA_OWNER_SOCKET": str(socket)},
                clear=False,
            ):
                self.assertEqual(
                    owner_sandbox_directories(settings),
                    [str(socket.parent)],
                )

    def test_http_owner_endpoint_has_no_sandbox_directories(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="owner-instance",
                host="127.0.0.1",
                port=9123,
                agent_enabled=False,
            )
            with patch.dict(
                os.environ,
                {"PA_OWNER_API_URL": "http://127.0.0.1:8081"},
                clear=False,
            ):
                self.assertEqual(owner_sandbox_directories(settings), [])
                self.assertIsNone(owner_endpoint(settings).uds)

    def test_codex_config_grants_owner_socket_without_replacing_existing_roots(self):
        socket = "/tmp/pa-501/abcd/owner.sock"
        merged = json.loads(
            merge_codex_owner_sandbox_config(
                json.dumps(
                    {
                        "sandbox_workspace_write": {
                            "writable_roots": ["/workspace"],
                            "network_access": True,
                        }
                    }
                ),
                socket_path=socket,
            )
        )
        self.assertEqual(
            merged["sandbox_workspace_write"]["writable_roots"],
            ["/workspace", "/tmp/pa-501/abcd"],
        )
        self.assertTrue(merged["sandbox_workspace_write"]["network_access"])
        self.assertEqual(merged["default_permissions"], "pa-owner")
        self.assertEqual(merged["permissions"]["pa-owner"]["extends"], ":workspace")
        self.assertEqual(
            merged["permissions"]["pa-owner"]["network"]["unix_sockets"][socket],
            "allow",
        )
        self.assertNotIn("network", merged["permissions"])
        self.assertEqual(
            merged["features"]["network_proxy"]["unix_sockets"][socket],
            "allow",
        )

    def test_codex_config_migrates_legacy_network_grant_into_named_profile(self):
        socket = "/tmp/pa-501/abcd/owner.sock"
        merged = json.loads(
            merge_codex_owner_sandbox_config(
                json.dumps(
                    {
                        "permissions": {
                            "network": {
                                "unix_sockets": {
                                    "/tmp/old.sock": "allow",
                                }
                            }
                        }
                    }
                ),
                socket_path=socket,
            )
        )
        self.assertEqual(merged["default_permissions"], "pa-owner")
        sockets = merged["permissions"]["pa-owner"]["network"]["unix_sockets"]
        self.assertEqual(sockets["/tmp/old.sock"], "allow")
        self.assertEqual(sockets[socket], "allow")
        self.assertNotIn("network", merged["permissions"])

    def test_codex_config_grants_socket_on_existing_named_profile(self):
        socket = "/tmp/pa-501/abcd/owner.sock"
        merged = json.loads(
            merge_codex_owner_sandbox_config(
                json.dumps(
                    {
                        "default_permissions": "project-edit",
                        "permissions": {
                            "project-edit": {
                                "extends": ":workspace",
                            }
                        },
                    }
                ),
                socket_path=socket,
            )
        )
        self.assertEqual(merged["default_permissions"], "project-edit")
        self.assertEqual(
            merged["permissions"]["project-edit"]["network"]["unix_sockets"][socket],
            "allow",
        )
        self.assertNotIn("pa-owner", merged["permissions"])

    def test_codex_config_wraps_builtin_default_permissions(self):
        socket = "/tmp/pa-501/abcd/owner.sock"
        merged = json.loads(
            merge_codex_owner_sandbox_config(
                json.dumps({"default_permissions": ":read-only"}),
                socket_path=socket,
            )
        )
        self.assertEqual(merged["default_permissions"], "pa-owner")
        self.assertEqual(merged["permissions"]["pa-owner"]["extends"], ":read-only")
        self.assertEqual(
            merged["permissions"]["pa-owner"]["network"]["unix_sockets"][socket],
            "allow",
        )

    def test_codex_spawn_environment_pins_owner_sandbox_and_keeps_pa_mcp(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="owner-instance",
                host="127.0.0.1",
                port=9123,
                agent_enabled=False,
            )
            socket = Path(tmp) / "runtime" / "owner.sock"
            with patch.dict(
                os.environ,
                {"PA_OWNER_API_URL": "", "PA_OWNER_SOCKET": str(socket)},
                clear=False,
            ):
                environment = apply_codex_owner_sandbox_environment(
                    {
                        "PATH": "/bin",
                        "CODEX_CONFIG": json.dumps({"model": "gpt-5.4"}),
                    },
                    settings,
                )
            config = json.loads(environment["CODEX_CONFIG"])
            self.assertEqual(environment["DISABLE_MCP_CONFIG_FILTERING"], "true")
            self.assertEqual(config["model"], "gpt-5.4")
            self.assertIn(str(socket.parent), config["sandbox_workspace_write"]["writable_roots"])
            self.assertEqual(config["default_permissions"], "pa-owner")
            self.assertEqual(
                config["permissions"]["pa-owner"]["network"]["unix_sockets"][str(socket)],
                "allow",
            )
