from __future__ import annotations

import tempfile
from pathlib import Path
from unittest import TestCase

from pa.config import Settings
from pa.core.kernel import Kernel


class OpenAPIContractTests(TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        settings = Settings(
            data_dir=Path(self.tempdir.name),
            auth_required=True,
            instance_url="http://pa.test:8080",
        )
        self.app = Kernel.boot(settings=settings).build_app()
        self.schema = self.app.openapi()

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_security_schemes_describe_session_csrf_and_bearers(self) -> None:
        schemes = self.schema["components"]["securitySchemes"]
        self.assertEqual(schemes["paSession"]["name"], "pa_session")
        self.assertEqual(schemes["paSession"]["in"], "cookie")
        self.assertEqual(schemes["paCsrfCookie"]["name"], "pa_csrf")
        self.assertEqual(schemes["paCsrfHeader"]["name"], "X-CSRF-Token")
        self.assertEqual(schemes["userBearer"]["scheme"], "bearer")
        self.assertEqual(schemes["instanceBearer"]["scheme"], "bearer")

        mutation = self.schema["paths"]["/api/cards/{card_id}"]["patch"]
        self.assertIn(
            {
                "paSession": [],
                "paCsrfCookie": [],
                "paCsrfHeader": [],
            },
            mutation["security"],
        )
        self.assertIn("401", mutation["responses"])
        self.assertIn("403", mutation["responses"])

    def test_public_and_instance_operations_have_accurate_security(self) -> None:
        login = self.schema["paths"]["/api/auth/login"]["post"]
        self.assertEqual(login["security"], [])

        sync = self.schema["paths"]["/api/sync/refs"]["get"]
        self.assertEqual(sync["security"], [{"instanceBearer": []}])
        self.assertNotIn("403", sync["responses"])

    def test_remote_dispatch_documents_linkage_idempotency_and_examples(self) -> None:
        operation = self.schema["paths"][
            "/api/fleet/instances/{instance_id}/agent/start"
        ]["post"]
        idempotency = next(
            parameter
            for parameter in operation["parameters"]
            if parameter["name"] == "Idempotency-Key"
        )
        self.assertEqual(idempotency["in"], "header")
        self.assertIn("target fleet instance", operation["description"])
        self.assertIn("inherits the card's project", operation["description"])
        self.assertIn("duplicate: true", operation["description"])
        self.assertIn(
            "cardLinked",
            operation["requestBody"]["content"]["application/json"]["examples"],
        )
        response = operation["responses"]["202"]["content"]["application/json"]
        self.assertEqual(
            response["schema"]["$ref"],
            "#/components/schemas/DispatchAdmission",
        )
        self.assertIn("409", operation["responses"])
        self.assertIn({"instanceBearer": []}, operation["security"])
        prompt = self.schema["paths"]["/api/fleet/dispatch-jobs/{dispatch_id}/prompt"][
            "post"
        ]
        self.assertIn({"instanceBearer": []}, prompt["security"])

    def test_documented_http_helper_is_redacted_and_avoids_token_scraping(self) -> None:
        docs = (Path(__file__).parents[1] / "docs" / "API.md").read_text()
        self.assertIn("from pa.http_client import PAClient", docs)
        self.assertIn('idempotency_key="dispatch-2026-07-24-001"', docs)
        self.assertIn('pa.login("operator", "REDACTED")', docs)
        self.assertIn("direct cookie/token scraping is unsupported", docs)
        self.assertNotIn("awk '$6", docs)
        self.assertNotIn("PA_SYNC_TOKEN", docs)
