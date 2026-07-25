from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from pa.config import Settings
from pa.core.kernel import Kernel
from pa.domain.store import reset_store
from pa.fleet.control_plane import build_control_plane_status
from pa.instance.agent_session import reset_instance_agent


class ControlPlaneStatusTests(unittest.TestCase):
    def test_legacy_seed_is_never_reported_as_consensus_authority(self) -> None:
        settings = Settings(
            instance_id="mac-mini",
            instance_name="local",
            fleet_id="fleet-1",
            data_dir=Path("/tmp/pa-control-plane-status"),
            instance_url="http://mac-mini:8080",
            pr_supervisor_authority_url="http://bootstrap:8080",
        )

        status = build_control_plane_status(
            settings,
            pr_supervisor_health={
                "role": "worker",
                "state": "ready",
                "authority_url": "http://bootstrap:8080",
                "max_fence_token": 12,
            },
        )

        self.assertEqual(status["mode"], "legacy_static")
        self.assertEqual(
            status["discovery"]["legacy_seed_url"], "http://bootstrap:8080"
        )
        self.assertFalse(status["discovery"]["seed_is_consensus_authority"])
        self.assertIsNone(status["consensus"]["leader_instance_id"])
        self.assertIsNone(
            status["service_authorities"]["pr-supervisor"]["authority_instance_id"]
        )
        self.assertEqual(
            status["service_authorities"]["pr-supervisor"][
                "max_observed_resource_fence"
            ],
            12,
        )

    def test_empty_static_configuration_does_not_claim_consensus_leadership(
        self,
    ) -> None:
        settings = Settings(
            instance_id="bootstrap",
            instance_name="local",
            fleet_id="fleet-1",
            data_dir=Path("/tmp/pa-control-plane-empty"),
            fleet_owner_url="",
            pr_supervisor_authority_url="",
        )

        status = build_control_plane_status(settings)

        self.assertFalse(status["consensus"]["available"])
        self.assertIsNone(status["consensus"]["leader_instance_id"])
        self.assertFalse(status["migration"]["automatic_failover_enabled"])
        self.assertTrue(
            any("treats itself" in warning for warning in status["warnings"])
        )


class ControlPlaneStatusAPITests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.settings = Settings(
            instance_id="instance-a",
            instance_name="Mac mini",
            fleet_id="fleet-1",
            data_dir=Path(self.tmp.name),
            instance_url="http://instance-a:8080",
            fleet_owner_url="http://legacy-owner:8080",
            sync_token="fleet-secret",
            agent_enabled=False,
            peers=[],
        )

    def tearDown(self) -> None:
        reset_instance_agent()
        reset_store()
        self.tmp.cleanup()

    def test_api_is_secret_free_and_labels_legacy_routing(self) -> None:
        app = Kernel.boot(settings=self.settings).build_app()
        with TestClient(app) as client:
            response = client.get(
                "/api/fleet/control-plane/status",
                headers={"Authorization": "Bearer fleet-secret"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status_version"], 1)
        self.assertEqual(payload["mode"], "legacy_static")
        self.assertEqual(payload["consensus"]["term"], None)
        self.assertNotIn("fleet-secret", json.dumps(payload))


if __name__ == "__main__":
    unittest.main()
