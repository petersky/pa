"""Auth middleware: sync_token must not force UI login."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from pa.auth.middleware import AuthMiddleware
from pa.auth.sessions import SessionManager
from pa.auth.users import UserDirectory
from pa.config import Settings, reset_settings


async def _ok(_request: Request) -> JSONResponse:
    return JSONResponse({"ok": True})


class SyncTokenAuthSeparationTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_settings()
        self._tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self._tmp.name)
        self.settings = Settings(
            data_dir=self.data_dir,
            sync_token="shared-secret",
            auth_required=False,
            session_secret="test-secret",
        )
        self.users = UserDirectory(self.data_dir)
        self.sessions = SessionManager(self.settings)

        app = Starlette(
            routes=[
                Route("/api/fleet/join-token", _ok, methods=["POST"]),
                Route("/api/sync/push", _ok, methods=["POST"]),
                Route("/api/health", _ok, methods=["GET"]),
            ]
        )
        app.add_middleware(
            AuthMiddleware,
            settings=self.settings,
            users=self.users,
            sessions=self.sessions,
        )
        self.client = TestClient(app)

    def tearDown(self) -> None:
        self._tmp.cleanup()
        reset_settings()

    def test_join_token_works_without_login_when_only_sync_token_set(self) -> None:
        # Prime CSRF cookie
        self.client.get("/api/health")
        csrf = self.client.cookies.get("pa_csrf")
        self.assertTrue(csrf)
        resp = self.client.post(
            "/api/fleet/join-token",
            json={},
            headers={"X-CSRF-Token": csrf},
        )
        self.assertEqual(resp.status_code, 200, resp.text)

    def test_sync_push_requires_bearer_when_sync_token_set(self) -> None:
        self.client.get("/api/health")
        csrf = self.client.cookies.get("pa_csrf")
        resp = self.client.post(
            "/api/sync/push",
            json={},
            headers={"X-CSRF-Token": csrf},
        )
        self.assertEqual(resp.status_code, 401)
        self.assertIn("Instance authentication", resp.json()["detail"])

        resp_ok = self.client.post(
            "/api/sync/push",
            json={},
            headers={"Authorization": "Bearer shared-secret"},
        )
        self.assertEqual(resp_ok.status_code, 200)

    def test_explicit_auth_required_blocks_join_token(self) -> None:
        self.settings.auth_required = True
        self.client.get("/api/health")
        csrf = self.client.cookies.get("pa_csrf")
        resp = self.client.post(
            "/api/fleet/join-token",
            json={},
            headers={"X-CSRF-Token": csrf},
        )
        self.assertEqual(resp.status_code, 401)
        self.assertEqual(resp.json()["detail"], "Authentication required")


if __name__ == "__main__":
    unittest.main()
