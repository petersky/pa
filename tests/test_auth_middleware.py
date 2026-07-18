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
                Route("/api/status", _ok, methods=["GET"]),
                Route("/api/agent/quiesce", _ok, methods=["GET", "POST"]),
                Route("/api/fleet/peer-update-check", _ok, methods=["GET"]),
                Route("/api/fleet/peer-update", _ok, methods=["POST"]),
                Route("/api/fleet/peer-update/{operation_id}", _ok, methods=["GET"]),
                Route("/api/config", _ok, methods=["GET"]),
                Route("/api/agent/prompt", _ok, methods=["POST"]),
                Route("/api/agent/providers", _ok, methods=["GET"]),
                Route("/api/agent/providers/codex", _ok, methods=["GET"]),
                Route(
                    "/api/agent/providers/codex/login-jobs",
                    _ok,
                    methods=["POST"],
                ),
                Route(
                    "/api/agent/providers/codex/login-jobs/{job_id}",
                    _ok,
                    methods=["GET"],
                ),
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

    def test_hardened_peer_accepts_sync_token_only_for_update_routes(self) -> None:
        self.settings.auth_required = True
        headers = {"Authorization": "Bearer shared-secret"}
        routes = [
            ("GET", "/api/status"),
            ("GET", "/api/agent/quiesce"),
            ("POST", "/api/agent/quiesce"),
            ("GET", "/api/fleet/peer-update-check"),
            ("POST", "/api/fleet/peer-update"),
            ("GET", "/api/fleet/peer-update/job-123"),
        ]
        for method, path in routes:
            with self.subTest(method=method, path=path):
                response = self.client.request(method, path, headers=headers, json={})
                self.assertEqual(response.status_code, 200, response.text)

    def test_hardened_peer_does_not_grant_sync_token_user_api_access(self) -> None:
        self.settings.auth_required = True
        headers = {"Authorization": "Bearer shared-secret"}
        for method, path in [
            ("GET", "/api/config"),
            ("POST", "/api/agent/prompt"),
            ("POST", "/api/fleet/join-token"),
        ]:
            with self.subTest(method=method, path=path):
                response = self.client.request(method, path, headers=headers, json={})
                self.assertEqual(response.status_code, 401, response.text)

        user = self.users.ensure_default_user()
        response = self.client.get(
            "/api/config",
            headers={"Authorization": f"Bearer {user.cli_token}"},
        )
        self.assertEqual(response.status_code, 200, response.text)

    def test_hardened_peer_accepts_sync_token_for_provider_login_proxy(self) -> None:
        self.settings.auth_required = True
        headers = {"Authorization": "Bearer shared-secret"}
        for method, path in [
            ("GET", "/api/agent/providers"),
            ("GET", "/api/agent/providers/codex"),
            ("POST", "/api/agent/providers/codex/login-jobs"),
            ("GET", "/api/agent/providers/codex/login-jobs/job-123"),
        ]:
            with self.subTest(method=method, path=path):
                response = self.client.request(method, path, headers=headers, json={})
                self.assertEqual(response.status_code, 200, response.text)


if __name__ == "__main__":
    unittest.main()
