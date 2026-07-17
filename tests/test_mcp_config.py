import os
import unittest
from unittest.mock import Mock, patch

from pa.acp.mcp_config import pa_mcp_servers


class PaMcpServersTests(unittest.TestCase):
    def test_forwards_attached_browser_environment(self):
        browser_env = {
            "PA_BROWSER_CDP_URL": "http://127.0.0.1:9222",
            "PA_BROWSER_TARGET_ID": "target-1",
            "PA_BROWSER_ATTACHMENT_ID": "attachment-1",
            "PA_BROWSER_SESSION_ID": "session-1",
        }

        with patch.dict(os.environ, browser_env, clear=False):
            server = pa_mcp_servers(Mock())[0]

        self.assertEqual(
            {item.name: item.value for item in server.env},
            browser_env,
        )

    def test_omits_browser_environment_when_detached(self):
        with patch.dict(os.environ, {}, clear=True):
            server = pa_mcp_servers(Mock())[0]

        self.assertEqual(server.env, [])
