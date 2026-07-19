from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from fastapi import HTTPException
from fastapi.templating import Jinja2Templates
from starlette.applications import Starlette
from starlette.requests import Request

from pa.config import Settings
from pa.core.context import AppContext
from pa.core.ui.pages import PageRegistry
from pa.modules.files import (
    _file_context,
    _file_diff,
    _inline_diff,
    _resolve_authorized_path,
    _side_by_side_diff,
    browse_files,
    raw_file,
)


class FileBrowserTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.store = MagicMock()
        self.store.list_sessions.return_value = []
        self.store.list_projects.return_value = []
        app = Starlette()
        app.state.ctx = SimpleNamespace(
            settings=SimpleNamespace(data_dir=self.root, auth_required=False),
            store=self.store,
        )
        self.request = Request(
            {"type": "http", "method": "GET", "path": "/browse", "headers": [], "app": app}
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_authorizes_data_and_session_roots_but_blocks_other_paths(self) -> None:
        data_file = self.root / "notes.md"
        data_file.write_text("# Notes")
        resolved, trusted = _resolve_authorized_path(self.request, str(data_file))
        self.assertEqual(resolved, data_file.resolve())
        self.assertEqual(trusted, self.root.resolve())

        with tempfile.TemporaryDirectory() as other:
            other_file = Path(other) / "secret.txt"
            other_file.write_text("secret")
            with self.assertRaises(HTTPException) as raised:
                _resolve_authorized_path(self.request, str(other_file))
            self.assertEqual(raised.exception.status_code, 403)

            self.store.list_sessions.return_value = [SimpleNamespace(cwd=other)]
            resolved, trusted = _resolve_authorized_path(self.request, str(other_file))
            self.assertEqual(resolved, other_file.resolve())
            self.assertEqual(trusted, Path(other).resolve())

    def test_auth_required_rejects_default_unauthenticated_ui_user(self) -> None:
        path = self.root / "private.txt"
        path.write_text("private")
        self.request.app.state.ctx.settings.auth_required = True
        with self.assertRaises(HTTPException) as raised:
            _resolve_authorized_path(self.request, str(path))
        self.assertEqual(raised.exception.status_code, 401)

        self.request.state.user_authenticated = True
        resolved, _ = _resolve_authorized_path(self.request, str(path))
        self.assertEqual(resolved, path.resolve())

    def test_symlink_cannot_escape_a_trusted_root(self) -> None:
        with tempfile.TemporaryDirectory() as other:
            target = Path(other) / "outside.txt"
            target.write_text("outside")
            link = self.root / "escape.txt"
            link.symlink_to(target)
            with self.assertRaises(HTTPException) as raised:
                _resolve_authorized_path(self.request, str(link))
            self.assertEqual(raised.exception.status_code, 403)

    def test_markdown_context_has_render_source_and_safe_raw_routes(self) -> None:
        path = self.root / "README.md"
        path.write_text("# Hello\n")
        context = _file_context(self.request, path, self.root, 1, None)
        self.assertTrue(context["is_markdown"])
        self.assertTrue(context["is_text"])
        self.assertEqual(context["text"], "# Hello\n")
        self.assertIn("path=", context["breadcrumbs"][-1]["url"])

        response = raw_file(self.request, str(path))
        self.assertIn("sandbox", response.headers["content-security-policy"])
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")

    def test_browse_route_renders_markdown_and_directory_pages(self) -> None:
        path = self.root / "README.md"
        path.write_text("# Hello\n")
        settings = Settings(data_dir=self.root, agent_enabled=False)
        ctx = AppContext(settings=settings, hooks=MagicMock(), store=self.store)
        ctx.register_service("instance_agent", SimpleNamespace(connected=False))
        ctx.register_service("pages", PageRegistry())
        ctx.register_service(
            "assets",
            SimpleNamespace(version="test", url=lambda name: f"/static/{name}"),
        )
        self.request.app.state.ctx = ctx
        templates_root = Path(__file__).parents[1] / "src" / "pa" / "server" / "templates"
        self.request.app.state.templates = Jinja2Templates(directory=str(templates_root))

        file_response = browse_files(self.request, str(path))
        file_html = file_response.body.decode()
        self.assertIn("data-file-markdown", file_html)
        self.assertIn("file-browser.js", file_html)

        directory_response = browse_files(self.request, str(self.root))
        directory_html = directory_response.body.decode()
        self.assertIn("README.md", directory_html)
        self.assertIn("file-entry", directory_html)

    def test_git_diff_supports_inline_and_side_by_side_views(self) -> None:
        repo = self.root / "repo"
        repo.mkdir()
        subprocess.run(["git", "init", "-q", str(repo)], check=True)
        subprocess.run(["git", "-C", str(repo), "config", "user.email", "pa@example.test"], check=True)
        subprocess.run(["git", "-C", str(repo), "config", "user.name", "PA Tests"], check=True)
        path = repo / "app.py"
        path.write_text("first\nold\nlast\n")
        subprocess.run(["git", "-C", str(repo), "add", "app.py"], check=True)
        subprocess.run(["git", "-C", str(repo), "commit", "-qm", "base"], check=True)
        path.write_text("first\nnew\nlast\n")

        raw, base = _file_diff(path, "HEAD")
        inline = _inline_diff(raw)
        side = _side_by_side_diff(raw)

        self.assertEqual(base, "HEAD")
        self.assertTrue(any(row["kind"] == "removed" for row in inline))
        self.assertTrue(any(row["kind"] == "added" for row in inline))
        changed = next(row for row in side if row["kind"] == "changed")
        self.assertEqual(changed["left"], "old")
        self.assertEqual(changed["right"], "new")


if __name__ == "__main__":
    unittest.main()
