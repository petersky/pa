import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from pa.acp.mcp_config import (
    OwnerChannelError,
    owner_endpoint,
    pa_mcp_servers,
    probe_owner_channel,
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
                    {"X-PA-Instance-ID": "owner-instance"},
                    "authentication_rejected",
                ),
                (404, {"X-PA-Instance-ID": "owner-instance"}, "api_incompatible"),
                (503, {"X-PA-Instance-ID": "owner-instance"}, "api_not_ready"),
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
