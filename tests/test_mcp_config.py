import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pa.acp.mcp_config import pa_mcp_servers
from pa.auth.users import UserDirectory
from pa.config import Settings


class PaMcpServersTests(unittest.TestCase):
    def _settings(self, root: str) -> Settings:
        return Settings(
            data_dir=Path(root),
            instance_id="owner-instance",
            port=9123,
            agent_enabled=False,
        )

    def _owner_env(self, settings: Settings) -> dict[str, str]:
        return {
            "PA_DATA_DIR": str(settings.data_dir),
            "PA_LOCAL_API_URL": "http://127.0.0.1:9123",
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
            with patch.dict(os.environ, browser_env, clear=False):
                server = pa_mcp_servers(settings)[0]

            self.assertEqual(
                {item.name: item.value for item in server.env},
                {
                    **self._owner_env(settings),
                    **browser_env,
                },
            )
            self.assertEqual(server.command, sys.executable)
            self.assertEqual(server.args, ["-m", "pa", "mcp"])

    def test_pins_owner_environment_when_browser_is_detached(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = self._settings(tmp)
            with patch.dict(os.environ, {}, clear=True):
                server = pa_mcp_servers(settings)[0]

            self.assertEqual(
                {item.name: item.value for item in server.env},
                self._owner_env(settings),
            )

    def test_forwards_session_id_without_attached_browser(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = self._settings(tmp)
            with patch.dict(
                os.environ, {"PA_BROWSER_SESSION_ID": "session-1"}, clear=True
            ):
                server = pa_mcp_servers(settings)[0]

            self.assertEqual(
                {item.name: item.value for item in server.env},
                {
                    **self._owner_env(settings),
                    "PA_BROWSER_SESSION_ID": "session-1",
                },
            )
