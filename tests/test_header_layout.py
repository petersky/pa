"""Browser layout regressions for the shared two-line PA brand."""

from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).parents[1]
MACOS_CHROME = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
CHROME = next(
    (
        executable
        for name in ("google-chrome", "chromium", "chromium-browser")
        if (executable := shutil.which(name))
    ),
    str(MACOS_CHROME) if MACOS_CHROME.is_file() else None,
)


@unittest.skipUnless(CHROME, "Chrome or Chromium is required for browser layout coverage")
class SharedHeaderBrowserLayoutTests(unittest.TestCase):
    def test_brand_identity_stays_visible_compact_and_aligned_in_both_themes(
        self,
    ) -> None:
        stylesheet = (ROOT / "src/pa/server/static/style.css").as_uri()
        themes = ROOT / "src/pa/server/static/themes/pa"
        results = []

        for appearance in ("light", "dark"):
            for width in (360, 1280):
                theme = (themes / f"{appearance}.css").as_uri()
                fixture = f"""<!doctype html>
<html lang="en" data-theme="pa" data-appearance="{appearance}">
<head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="{stylesheet}">
<link rel="stylesheet" href="{theme}">
</head><body>
<header class="site-header">
  <div class="header-start">
    <div class="brand-block">
      <a href="/" class="brand-link">PA</a>
      <span class="brand-instance" data-pa-instance-name="macmini">
        <span class="sr-only">Instance: </span>macmini
      </span>
    </div>
    <nav class="top-nav">
      <a class="nav-btn active" href="#"><span>Home</span></a>
      <a class="nav-btn" href="#"><span>Work</span></a>
      <a class="nav-btn" href="#"><span>Projects</span></a>
      <a class="nav-btn" href="#"><span>Agent</span></a>
    </nav>
  </div>
  <div class="chrome-actions">
    <button class="new-card-button" type="button"><span>+</span> New</button>
    <button class="status-btn online" type="button">Agent</button>
    <button class="icon-btn" type="button" aria-label="Toggle theme">◐</button>
    <a class="icon-btn" href="#" aria-label="Settings">⚙</a>
  </div>
</header>
<script>
  function luminance(color) {{
    var channels = color.match(/[\\d.]+/g).slice(0, 3).map(function (value) {{
      var channel = Number(value) / 255;
      return channel <= 0.04045
        ? channel / 12.92
        : Math.pow((channel + 0.055) / 1.055, 2.4);
    }});
    return 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2];
  }}

  function contrast(foreground, background) {{
    var lighter = Math.max(luminance(foreground), luminance(background));
    var darker = Math.min(luminance(foreground), luminance(background));
    return (lighter + 0.05) / (darker + 0.05);
  }}

  function inspect() {{
    var header = document.querySelector(".site-header");
    var brand = document.querySelector(".brand-block");
    var instance = document.querySelector(".brand-instance");
    var nav = document.querySelector(".top-nav");
    var actions = document.querySelector(".chrome-actions");
    var headerRect = header.getBoundingClientRect();
    var brandRect = brand.getBoundingClientRect();
    var instanceRect = instance.getBoundingClientRect();
    var style = getComputedStyle(instance);
    var backgroundColor = getComputedStyle(header).backgroundColor;
    return {{
      width: window.innerWidth,
      headerHeight: headerRect.height,
      instanceVisible: instanceRect.width > 0 && instanceRect.height > 0 &&
        style.visibility === "visible" && style.display !== "none",
      instanceText: instance.textContent.trim(),
      brandInsideHeader: brandRect.top >= headerRect.top &&
        brandRect.bottom <= headerRect.bottom,
      actionsSingleLine: actions.getBoundingClientRect().height < 40,
      navSingleLine: nav.getBoundingClientRect().height < 40,
      noPageOverflow: document.documentElement.scrollWidth <= window.innerWidth,
      instanceColor: style.color,
      backgroundColor: backgroundColor,
      contrast: contrast(style.color, backgroundColor)
    }};
  }}
  var result = document.createElement("pre");
  result.id = "result";
  result.textContent = JSON.stringify(inspect());
  document.body.append(result);
</script>
</body></html>"""
                with tempfile.TemporaryDirectory() as tmp:
                    page = Path(tmp) / "header.html"
                    page.write_text(fixture)
                    completed = subprocess.run(
                        [
                            str(CHROME),
                            "--headless=new",
                            "--disable-gpu",
                            "--no-sandbox",
                            "--allow-file-access-from-files",
                            f"--window-size={width},800",
                            "--dump-dom",
                            page.as_uri(),
                        ],
                        check=True,
                        capture_output=True,
                        text=True,
                        timeout=30,
                    )
                marker = '<pre id="result">'
                payload = completed.stdout.split(marker, 1)[1].split("</pre>", 1)[0]
                results.append(
                    (appearance, json.loads(payload.replace("&quot;", '"')))
                )

        for appearance, measurement in results:
            with self.subTest(
                appearance=appearance,
                width=measurement["width"],
            ):
                self.assertTrue(measurement["instanceVisible"])
                self.assertEqual(measurement["instanceText"], "Instance: macmini")
                self.assertTrue(measurement["brandInsideHeader"])
                self.assertTrue(measurement["actionsSingleLine"])
                self.assertTrue(measurement["navSingleLine"])
                self.assertTrue(measurement["noPageOverflow"])
                self.assertLessEqual(measurement["headerHeight"], 52)
                self.assertNotEqual(
                    measurement["instanceColor"],
                    measurement["backgroundColor"],
                )
                self.assertGreaterEqual(measurement["contrast"], 4.5)
