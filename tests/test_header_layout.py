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
            for width in (390, 720, 820, 1440):
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
    <details class="responsive-nav"><summary class="nav-menu-button">Menu</summary>
      <nav class="responsive-nav-menu" aria-label="Main navigation menu">
        <a class="nav-btn" href="#"><span>Home</span></a>
        <a class="nav-btn" href="#"><span>Work</span></a>
        <a class="nav-btn" href="#"><span>Projects</span></a>
        <a class="nav-btn" href="#"><span>Fleet</span></a>
        <a class="nav-btn" href="#"><span>Sessions</span></a>
        <a class="nav-btn" href="#"><span>Settings</span></a>
      </nav>
    </details>
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
    var responsive = document.querySelector(".responsive-nav");
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
      responsiveMenuVisible: getComputedStyle(responsive).display !== "none",
      menuLabels: Array.from(responsive.querySelectorAll("a")).map(function (link) {{
        return link.textContent.trim();
      }}),
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
                if measurement["width"] <= 1180:
                    self.assertTrue(measurement["responsiveMenuVisible"])
                    self.assertEqual(
                        measurement["menuLabels"],
                        ["Home", "Work", "Projects", "Fleet", "Sessions", "Settings"],
                    )
                else:
                    self.assertTrue(measurement["navSingleLine"])
                self.assertTrue(measurement["noPageOverflow"])
                self.assertLessEqual(measurement["headerHeight"], 52)
                self.assertNotEqual(
                    measurement["instanceColor"],
                    measurement["backgroundColor"],
                )
                self.assertGreaterEqual(measurement["contrast"], 4.5)

    def test_card_modal_keeps_long_errors_actions_and_focus_viewport_safe(self) -> None:
        stylesheet = (ROOT / "src/pa/server/static/style.css").as_uri()
        for width, height in ((390, 844), (820, 900), (1440, 900)):
            fixture = f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="{stylesheet}">
<style>:root {{--pa-bg:#fff;--pa-surface:#f7f8fa;--pa-surface-raised:#fff;--pa-text:#172033;--pa-text-muted:#536079;--pa-border:#73809a;--pa-accent:#315fc7;--pa-danger:#a11}}</style>
</head><body><button id="opener">New card</button>
<dialog id="new-card-dialog" class="new-card-dialog" aria-labelledby="new-card-title">
<div id="new-card-dialog-content"><form class="new-card-form">
<header class="new-card-header"><h2 id="new-card-title">Create new card</h2><button type="button">Close</button></header>
<div class="new-card-scroll-body"><div class="new-card-fields">
<label class="new-card-field-span-2"><span>Title</span><input id="title" required></label>
<label class="new-card-field-span-2"><span>Description</span><textarea rows="40">{('Long content ' * 300)}</textarea></label>
</div><p class="new-card-error" role="alert" tabindex="-1">Validation failed: correct the long content and try again.</p></div>
<footer class="new-card-footer"><span>Keyboard shortcut</span><div class="form-actions"><button type="button">Cancel</button><button id="create">Create card</button></div></footer>
</form></div></dialog><button id="outside">Outside</button>
<script>
var dialog=document.querySelector('dialog');dialog.showModal();document.querySelector('#title').focus();
document.querySelector('#outside').focus();
var d=dialog.getBoundingClientRect(), footer=document.querySelector('.new-card-footer').getBoundingClientRect();
var result=document.createElement('pre');result.id='result';result.textContent=JSON.stringify({{
  width:innerWidth, noOverflow:document.documentElement.scrollWidth<=innerWidth,
  dialogInside:d.top>=0&&d.bottom<=innerHeight, footerReachable:footer.top>=d.top&&footer.bottom<=d.bottom,
  bodyScrollable:document.querySelector('.new-card-scroll-body').scrollHeight>document.querySelector('.new-card-scroll-body').clientHeight,
  focusContained:dialog.contains(document.activeElement), errorNamed:document.querySelector('[role=alert]').textContent.includes('Validation failed')
}});document.body.append(result);
</script></body></html>"""
            with tempfile.TemporaryDirectory() as tmp:
                page = Path(tmp) / "modal.html"
                page.write_text(fixture)
                completed = subprocess.run(
                    [str(CHROME), "--headless=new", "--disable-gpu", "--no-sandbox",
                     "--allow-file-access-from-files", f"--window-size={width},{height}",
                     "--dump-dom", page.as_uri()],
                    check=True, capture_output=True, text=True, timeout=30,
                )
            payload = completed.stdout.split('<pre id="result">', 1)[1].split("</pre>", 1)[0]
            measurement = json.loads(payload.replace("&quot;", '"').replace("&amp;", "&"))
            with self.subTest(width=width):
                self.assertTrue(measurement["noOverflow"], measurement)
                self.assertTrue(measurement["dialogInside"], measurement)
                self.assertTrue(measurement["footerReachable"], measurement)
                self.assertTrue(measurement["bodyScrollable"], measurement)
                self.assertTrue(measurement["focusContained"], measurement)
                self.assertTrue(measurement["errorNamed"], measurement)
