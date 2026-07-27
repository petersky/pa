import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pa.acp.mcp_config import pa_mcp_servers
from pa.config import Settings
from pa.mcp.local_api import local_pa_url
from pa.mcp.owner_channel import owner_channel


class OwnerChannelTests(unittest.TestCase):
    def _settings(self, root: str, host: str) -> Settings:
        return Settings(
            data_dir=Path(root),
            workspace_root=Path(root).parent / "pa-owner-channel-workspaces",
            instance_id="owner",
            host=host,
            port=9123,
            agent_enabled=False,
        )

    def test_supported_bind_targets(self):
        with tempfile.TemporaryDirectory() as tmp:
            for host, expected, endpoint_type in (
                ("localhost", "http://localhost:9123", "loopback"),
                ("127.0.0.1", "http://127.0.0.1:9123", "loopback"),
                ("0.0.0.0", "http://127.0.0.1:9123", "wildcard_ipv4"),
                ("::", "http://[::1]:9123", "wildcard_ipv6"),
                ("100.78.2.112", "http://100.78.2.112:9123", "concrete"),
                ("2001:db8::24", "http://[2001:db8::24]:9123", "concrete_ipv6"),
            ):
                with self.subTest(host=host), patch.dict("os.environ", {}, clear=True):
                    settings = self._settings(tmp, host)
                    channel = owner_channel(settings)
                    self.assertEqual(channel.url, expected)
                    self.assertEqual(channel.endpoint_type, endpoint_type)
                    self.assertEqual(local_pa_url(settings), expected)
                    env = {
                        item.name: item.value for item in pa_mcp_servers(settings)[0].env
                    }
                    self.assertEqual(env["PA_LOCAL_API_URL"], expected)
                    self.assertEqual(env["PA_LOCAL_API_ENDPOINT_TYPE"], endpoint_type)

    def test_explicit_child_endpoint_still_wins(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = self._settings(tmp, "100.78.2.112")
            with patch.dict(
                "os.environ",
                {"PA_LOCAL_API_URL": "http://owner.internal:9999/"},
                clear=True,
            ):
                self.assertEqual(local_pa_url(settings), "http://owner.internal:9999")
