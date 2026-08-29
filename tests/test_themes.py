from __future__ import annotations

import json
import re
import unittest
from pathlib import Path

from pa.modules.theme import get_theme_catalog


THEMES_DIR = (
    Path(__file__).resolve().parents[1] / "src" / "pa" / "server" / "static" / "themes"
)
REQUIRED_VARIABLES = {
    "--pa-bg",
    "--pa-surface",
    "--pa-surface-raised",
    "--pa-border",
    "--pa-text",
    "--pa-text-muted",
    "--pa-accent",
    "--pa-accent-hover",
    "--pa-accent-subtle",
    "--pa-ok",
    "--pa-warn",
    "--pa-danger",
    "--pa-shadow",
    "--pa-focus",
    "--pa-chat-thought-bg",
    "--pa-chat-tool-bg",
}
EXPECTED_THEMES = {
    "ayu": "ayu",
    "feelin-pretty": "Feelin' Pretty",
    "go-hawks": "Go Hawks",
    "monokai-pro": "Monokai Pro",
    "nord": "Nord",
    "osu": "OSU",
    "solarized": "Solarized",
    "tokyo-night": "Tokyo Night",
}


def _css_variables(css: str) -> dict[str, str]:
    return dict(re.findall(r"(--pa-[\w-]+):\s*([^;]+);", css))


def _relative_luminance(color: str) -> float:
    channels = [int(color[index : index + 2], 16) / 255 for index in (1, 3, 5)]
    linear = [
        channel / 12.92
        if channel <= 0.04045
        else ((channel + 0.055) / 1.055) ** 2.4
        for channel in channels
    ]
    return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]


def _contrast(first: str, second: str) -> float:
    lighter, darker = sorted(
        (_relative_luminance(first), _relative_luminance(second)), reverse=True
    )
    return (lighter + 0.05) / (darker + 0.05)


class ThemeCatalogTests(unittest.TestCase):
    def test_shell_applies_resolved_theme_before_loading_stylesheets(self) -> None:
        shell = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "pa"
            / "server"
            / "templates"
            / "shell.html"
        ).read_text()

        resolver = shell.index('localStorage.getItem("pa.appearance")')
        structural_styles = shell.index("static_url('style.css')")
        variant_link = shell.index('id="pa-theme-variant"')
        variant_assignment = shell.index('link.href = "/static/themes/"')
        self.assertLess(resolver, structural_styles)
        self.assertLess(resolver, variant_link)
        self.assertLess(variant_link, variant_assignment)
        self.assertIn("root.dataset.appearance = resolved", shell)
        self.assertIn("root.style.colorScheme = resolved", shell)
        self.assertNotIn("/light.css') }}", shell)

    def test_login_loads_matching_pa_theme_variables(self) -> None:
        login = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "pa"
            / "server"
            / "templates"
            / "pages"
            / "login.html"
        ).read_text()

        self.assertIn('data-theme="pa" data-appearance="light"', login)
        self.assertIn("document.documentElement.dataset.appearance", login)
        self.assertIn("static_url('themes/pa/light.css')", login)
        self.assertIn("static_url('themes/pa/dark.css')", login)

    def test_requested_themes_are_discoverable(self) -> None:
        catalog = {theme["id"]: theme for theme in get_theme_catalog()}

        for theme_id, name in EXPECTED_THEMES.items():
            with self.subTest(theme=theme_id):
                self.assertIn(theme_id, catalog)
                self.assertEqual(catalog[theme_id]["name"], name)
                self.assertEqual(
                    catalog[theme_id]["variants"],
                    [
                        {"id": "light", "label": "Light"},
                        {"id": "dark", "label": "Dark"},
                    ],
                )

    def test_every_manifest_has_complete_variant_assets(self) -> None:
        for manifest_path in THEMES_DIR.glob("*/manifest.json"):
            manifest = json.loads(manifest_path.read_text())
            theme_dir = manifest_path.parent

            with self.subTest(theme=manifest["id"]):
                self.assertEqual(manifest["id"], theme_dir.name)

            for variant in manifest["variants"]:
                appearance = variant["id"]
                css_path = theme_dir / f"{appearance}.css"
                with self.subTest(theme=manifest["id"], appearance=appearance):
                    self.assertTrue(css_path.is_file())
                    css = css_path.read_text()
                    self.assertIn(
                        f'[data-theme="{manifest["id"]}"]'
                        f'[data-appearance="{appearance}"]',
                        css,
                    )
                    variables = _css_variables(css)
                    self.assertEqual(set(variables), REQUIRED_VARIABLES)
                    self.assertGreaterEqual(
                        _contrast(variables["--pa-text"], variables["--pa-bg"]),
                        4.5,
                    )
                    self.assertGreaterEqual(
                        _contrast(variables["--pa-text"], variables["--pa-surface"]),
                        4.5,
                    )
                    self.assertGreaterEqual(
                        _contrast(variables["--pa-accent"], variables["--pa-surface"]),
                        4.5,
                    )
