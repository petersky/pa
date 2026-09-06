"""GitHub Add Repo boundaries, recovery, and real dialog behavior."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent
from pa.pr_supervisor.github import GitHubAPIError, GitHubClient, GitHubCredentials
from pa.repository.github import GitHubRepositories, validate_name


REMOTE = {
    "id": 42, "name": "fresh", "full_name": "octocat/fresh",
    "clone_url": "https://github.com/octocat/fresh.git", "default_branch": "main",
    "private": True, "archived": False, "fork": False, "description": None,
}


class GitHubRepositoryClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_api_contract_and_pagination(self):
        calls = []
        def handler(request):
            calls.append(request)
            if request.url.path == "/user":
                return httpx.Response(200, json={"login": "octocat", "private_token": "never-return"})
            if request.method == "POST":
                self.assertEqual(json.loads(request.content), {"name": "fresh", "private": True, "auto_init": True})
                return httpx.Response(201, json={**REMOTE, "token": "never-return"})
            if request.url.path == "/user/repos":
                self.assertEqual(request.url.params["page"], "2")
                self.assertEqual(request.url.params["visibility"], "private")
                self.assertEqual(request.url.params["affiliation"], "owner,collaborator,organization_member")
                return httpx.Response(200, json=[REMOTE] * 100)
            return httpx.Response(404, json={"message": "Not Found"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            service = GitHubRepositories(GitHubClient(GitHubCredentials(token="secret"), client=client))
            self.assertEqual(await service.identity(), {"login": "octocat"})
            listed = await service.list(page=2, visibility="private", sort="full_name")
            self.assertEqual(listed["next_page"], 3)
            self.assertTrue(await service.availability("fresh", "octocat"))
            self.assertEqual(await service.create(" fresh "), REMOTE)
        self.assertTrue(all(call.headers["Authorization"] == "Bearer secret" for call in calls))

    async def test_auth_permission_and_rate_errors_are_not_availability(self):
        for status in [401, 403, 429, 500]:
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(status, json={"message": "secret"}))) as client:
                service = GitHubRepositories(GitHubClient(GitHubCredentials(token="secret"), client=client))
                with self.assertRaises(GitHubAPIError):
                    await service.availability("fresh", "octocat")

    def test_names_cannot_change_api_path(self):
        for name in ["", ".", "..", "../other", "owner/repo", "x?private=false", "a" * 101, "a b"]:
            with self.subTest(name=name), self.assertRaises(ValueError):
                validate_name(name)


class GitHubRepositoryRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        reset_settings(); reset_store(); reset_instance_agent()
        cls.tmp = tempfile.TemporaryDirectory()
        cls.app = Kernel.boot(settings=Settings(data_dir=Path(cls.tmp.name), instance_id="repo-test", agent_enabled=False)).build_app()
        cls.client = TestClient(cls.app)
        cls.client.__enter__()
        cls.client.get("/projects?view=repos")
        cls.csrf = cls.client.cookies.get("pa_csrf")

    @classmethod
    def tearDownClass(cls):
        cls.client.__exit__(None, None, None)
        reset_instance_agent(); reset_store(); reset_settings()
        cls.tmp.cleanup()

    def setUp(self):
        self.service = AsyncMock(spec=GitHubRepositories)
        self.service.identity.return_value = {"login": "octocat"}
        self.service.availability.return_value = True
        self.service.create.return_value = REMOTE.copy()
        self.patch = patch("pa.modules.github_repositories._service", new=AsyncMock(return_value=self.service))
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def post(self, key, **overrides):
        return self.client.post("/api/github/repositories", json={"name": "fresh", "confirmed_login": "octocat", **overrides}, headers={"X-CSRF-Token": self.csrf, "Idempotency-Key": key})

    def test_create_and_replay_register_once_remotely(self):
        first = self.post("successful-create")
        second = self.post("successful-create")
        self.assertEqual(first.status_code, 201, first.text)
        self.assertEqual(second.status_code, 201, second.text)
        self.assertEqual(first.json()["id"], second.json()["id"])
        self.assertEqual(first.json()["provider_repository_id"], "42")
        self.service.create.assert_awaited_once_with("fresh")
        self.service.availability.assert_awaited_once()
        conflict = self.post("successful-create", name="other")
        self.assertEqual(conflict.status_code, 409)

    def test_catalog_failure_replays_remote_receipt(self):
        with patch.object(self.app.state.ctx.store, "create_repository", side_effect=RuntimeError("disk failed")):
            response = self.post("catalog-recovery")
        self.assertEqual(response.status_code, 503)
        self.assertIn("GitHub created octocat/fresh", response.text)
        self.assertEqual(self.post("catalog-recovery").status_code, 201)
        self.service.create.assert_awaited_once()

    def test_collision_or_account_change_never_creates(self):
        self.service.availability.return_value = False
        self.assertEqual(self.post("collision").status_code, 409)
        self.assertEqual(self.post("account-change", confirmed_login="other").status_code, 409)
        self.service.create.assert_not_awaited()

    def test_timeout_does_not_automatically_repeat_post(self):
        self.service.create.side_effect = httpx.ReadTimeout("secret request info")
        first = self.post("uncertain")
        second = self.post("uncertain")
        self.assertEqual(first.status_code, 502)
        self.assertNotIn("secret", first.text)
        self.assertEqual(second.status_code, 409)
        self.service.create.assert_awaited_once()

    def test_rejected_creation_is_safe_and_does_not_register(self):
        self.service.create.side_effect = GitHubAPIError(422, "create", "secret")
        response = self.post("rejected")
        self.assertEqual(response.status_code, 409)
        self.assertNotIn("secret", response.text)

    def test_auth_and_input_validation(self):
        for status in [401, 403, 429, 500]:
            self.service.identity.side_effect = GitHubAPIError(status, "identity", "secret")
            response = self.client.get("/api/github/identity")
            self.assertEqual(response.status_code, {401: 401, 403: 403, 429: 403, 500: 502}[status])
            self.assertNotIn("secret", response.text)
        for query in ["page=0", "visibility=internal", "sort=unsafe"]:
            self.assertEqual(self.client.get("/api/github/repositories?" + query).status_code, 422)
        self.assertEqual(self.post("bad-name", name="../other").status_code, 422)
        self.service.create.assert_not_awaited()

    def test_missing_credentials_report_unauthenticated(self):
        self.patch.stop()
        with patch("pa.modules.github_repositories.GitHubCredentials.load", return_value=GitHubCredentials()):
            response = self.client.get("/api/github/identity")
        self.assertEqual(response.status_code, 401)
        self.assertIn("not authenticated", response.text)

    def test_csrf_and_idempotency_are_required(self):
        data = {"name": "fresh", "confirmed_login": "octocat"}
        self.assertEqual(self.client.post("/api/github/repositories", json=data).status_code, 403)
        self.assertEqual(self.client.post("/api/github/repositories", json=data, headers={"X-CSRF-Token": self.csrf}).status_code, 422)
        self.service.create.assert_not_awaited()

    def test_dialog_markup_and_existing_manual_url_flow(self):
        page = self.client.get("/projects?view=repos")
        self.assertIn('role="tab" aria-selected="true"', page.text)
        self.assertIn('data-repository-name', page.text)
        self.assertIn('data-github-identity', page.text)
        self.assertIn('js/repository-dialog.js', page.text)
        response = self.client.post("/projects/repositories?view=repos", data={"url": "https://example.test/manual.git"}, headers={"X-CSRF-Token": self.csrf, "HX-Request": "true"})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertIn("https://example.test/manual.git", response.text)
