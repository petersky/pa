import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import httpx

from pa.acp.owner_channel import owner_endpoint, probe_owner_channel
from pa.config import Settings


class OwnerEndpointTests(unittest.TestCase):
    def settings(self, root: str, host: str) -> Settings:
        return Settings(
            data_dir=Path(root),
            instance_id="owner",
            host=host,
            port=9123,
            agent_enabled=False,
        )

    def test_supported_bind_classes(self):
        cases = {
            "127.0.0.1": ("http://127.0.0.1:9123", "loopback_ipv4"),
            "localhost": ("http://localhost:9123", "loopback_hostname"),
            "0.0.0.0": ("http://127.0.0.1:9123", "wildcard_ipv4"),
            "::": ("http://[::1]:9123", "wildcard_ipv6"),
            "192.168.1.8": ("http://192.168.1.8:9123", "concrete_ipv4"),
            "100.78.2.112": ("http://100.78.2.112:9123", "concrete_ipv4"),
            "2001:db8::8": ("http://[2001:db8::8]:9123", "concrete_ipv6"),
        }
        with tempfile.TemporaryDirectory() as tmp:
            for host, expected in cases.items():
                with self.subTest(host=host):
                    result = owner_endpoint(self.settings(tmp, host))
                    self.assertEqual((result.url, result.endpoint_type), expected)

    def test_probe_verifies_identity_and_readiness(self):
        with tempfile.TemporaryDirectory() as tmp:
            response = Mock(
                status_code=200,
                headers={"X-PA-Instance-ID": "owner"},
            )
            response.json.return_value = {"status": "ready"}
            with patch("pa.acp.owner_channel.httpx.get", return_value=response):
                result = probe_owner_channel(
                    self.settings(tmp, "100.78.2.112"), attempts=1
                )
            self.assertEqual(result["state"], "connected_identity_verified")
            self.assertEqual(result["endpoint_type"], "concrete_ipv4")

    def test_probe_classifies_auth_identity_readiness_and_transport(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = self.settings(tmp, "127.0.0.1")
            cases = [
                (
                    Mock(status_code=401, headers={"X-PA-Instance-ID": "owner"}),
                    "authentication_rejected",
                ),
                (
                    Mock(status_code=200, headers={"X-PA-Instance-ID": "other"}),
                    "instance_mismatch",
                ),
                (
                    Mock(status_code=503, headers={"X-PA-Instance-ID": "owner"}),
                    "api_not_ready",
                ),
                (httpx.ConnectError("no owner"), "connection_refused_or_unreachable"),
            ]
            for result, classification in cases:
                with self.subTest(classification=classification):
                    effect = result if isinstance(result, Exception) else None
                    value = None if effect else result
                    with patch(
                        "pa.acp.owner_channel.httpx.get",
                        return_value=value,
                        side_effect=effect,
                    ), self.assertRaisesRegex(
                        RuntimeError, f"classification={classification}"
                    ):
                        probe_owner_channel(settings, attempts=1)
