from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from pa.config import Settings
from pa.core.kernel import Kernel


ROOT = Path(__file__).parents[1]
STATIC = ROOT / "src" / "pa" / "server" / "static"
TEMPLATES = STATIC.parent / "templates"


class FaviconTests(unittest.TestCase):
    def test_favicon_route_serves_packaged_icon_with_cache_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app = Kernel.boot(
                settings=Settings(data_dir=Path(tmp), auth_required=True),
                load_modules=False,
            ).build_app()
            response = TestClient(app).get("/favicon.ico")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "image/x-icon")
        self.assertEqual(
            response.headers["cache-control"], "public, max-age=604800"
        )
        self.assertTrue(response.content.startswith(b"\x00\x00\x01\x00"))

    def test_all_full_page_layouts_declare_svg_and_ico_icons(self) -> None:
        expected = (
            '<link rel="icon" href="{{ static_url(\'favicon.svg\') }}" '
            'type="image/svg+xml">'
        )
        for template in ("shell.html", "pages/login.html"):
            source = (TEMPLATES / template).read_text()
            with self.subTest(template=template):
                self.assertIn(expected, source)
                self.assertIn('<link rel="icon" href="/favicon.ico" sizes="any">', source)

    def test_favicon_assets_are_inside_the_pa_package(self) -> None:
        self.assertTrue((STATIC / "favicon.svg").is_file())
        self.assertTrue((STATIC / "favicon.ico").is_file())


if __name__ == "__main__":
    unittest.main()
