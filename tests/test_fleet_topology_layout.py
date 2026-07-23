"""Browser layout regressions for the responsive Fleet topology."""

from __future__ import annotations

import html
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).parents[1]
CHROME = next(
    (
        executable
        for name in ("google-chrome", "chromium", "chromium-browser")
        if (executable := shutil.which(name))
    ),
    None,
)


@unittest.skipUnless(CHROME, "Chrome or Chromium is required for browser layout coverage")
class FleetTopologyBrowserLayoutTests(unittest.TestCase):
    def test_phone_tablet_and_desktop_layouts_keep_every_node_operable(self) -> None:
        fleet_script = (ROOT / "src/pa/server/static/js/fleet.js").as_uri()
        stylesheet = (ROOT / "src/pa/server/static/style.css").as_uri()
        overview = json.dumps(
            {
                "nodes": [
                    {
                        "id": "mac-mini",
                        "name": "Mac mini",
                        "url": "http://mini.test",
                        "local": False,
                        "dimensions": {},
                    },
                    {
                        "id": "local",
                        "name": "Local",
                        "url": "http://local.test",
                        "local": True,
                        "dimensions": {},
                    },
                    {
                        "id": "monica",
                        "name": "Monica",
                        "url": "http://monica.test",
                        "local": False,
                        "dimensions": {},
                    },
                ],
                "edges": [],
            }
        ).replace("</", "<\\/")
        fixture = f"""<!doctype html>
<html><head><meta charset="utf-8"><link rel="stylesheet" href="{stylesheet}"></head>
<body>
<div id="pa-fleet-root">
  <script type="application/json" id="pa-fleet-overview-data">{overview}</script>
  <section id="fixture-panel" style="width: 358px">
    <div id="pa-fleet-topology" class="fleet-topology">
      <svg viewBox="0 0 960 420" role="img" aria-label="Fleet instance and activity topology"></svg>
    </div>
    <details class="fleet-route-equivalent">
      <summary>Route and placement list</summary>
      <ul id="pa-fleet-edge-list"></ul>
    </details>
    <aside id="pa-fleet-detail" tabindex="0"><h3>Inspect activity</h3></aside>
  </section>
</div>
<script>window.PA_TEST = true;</script>
<script src="{fleet_script}"></script>
<script>
  window.addEventListener("DOMContentLoaded", function () {{
    var api = window.__paFleetTopology;
    var panel = document.querySelector("#fixture-panel");
    var host = document.querySelector("#pa-fleet-topology");
    var svg = host.querySelector("svg");

    function inspect(name, width) {{
      panel.style.width = width + "px";
      api.render();
      var hostRect = host.getBoundingClientRect();
      var svgRect = svg.getBoundingClientRect();
      var nodes = Array.from(svg.querySelectorAll("[data-fleet-node]"));
      var epsilon = 1;
      return {{
        name: name,
        mode: svg.dataset.layout,
        viewBox: svg.getAttribute("viewBox"),
        allVisible: nodes.every(function (node) {{
          var bounds = node.querySelector("rect").getBoundingClientRect();
          return bounds.left >= hostRect.left - epsilon &&
            bounds.right <= hostRect.right + epsilon &&
            bounds.top >= svgRect.top - epsilon &&
            bounds.bottom <= svgRect.bottom + epsilon;
        }}),
        noHorizontalOverflow: host.scrollWidth <= host.clientWidth,
        tabStops: svg.querySelectorAll('[role="button"][tabindex="0"]').length,
        labelPixels: 12 * svg.getScreenCTM().a,
        routesAfterGraph: document.querySelector(".fleet-route-equivalent")
          .getBoundingClientRect().top >= svgRect.bottom - epsilon,
        detailReachable: document.querySelector("#pa-fleet-detail") !== null
      }};
    }}

    var phone = inspect("phone", 358);
    var monica = svg.querySelector('[data-fleet-node="monica"]');
    monica.dispatchEvent(new MouseEvent("click", {{ bubbles: true }}));
    phone.pointerDetail = document.querySelector("#pa-fleet-detail h3").textContent;
    var mini = svg.querySelector('[data-fleet-node="mac-mini"]');
    mini.focus();
    mini.dispatchEvent(new KeyboardEvent("keydown", {{ key: "Enter", bubbles: true }}));
    phone.keyboardDetail = document.querySelector("#pa-fleet-detail h3").textContent;

    var tablet = inspect("tablet", 700);
    tablet.focusedNode = document.activeElement.dataset.fleetNode;
    var desktop = inspect("desktop", 960);
    desktop.focusedNode = document.activeElement.dataset.fleetNode;

    var output = document.createElement("pre");
    output.id = "result";
    output.textContent = JSON.stringify([phone, tablet, desktop]);
    document.body.append(output);
  }});
</script>
</body></html>"""

        with tempfile.TemporaryDirectory() as tmp:
            fixture_path = Path(tmp) / "fleet-topology-layout.html"
            fixture_path.write_text(fixture)
            completed = subprocess.run(
                [
                    CHROME,
                    "--headless=new",
                    "--no-sandbox",
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--allow-file-access-from-files",
                    "--window-size=1200,1000",
                    "--dump-dom",
                    fixture_path.as_uri(),
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            )

        match = re.search(r'<pre id="result">(.*?)</pre>', completed.stdout, re.S)
        self.assertIsNotNone(match, completed.stderr or completed.stdout)
        layouts = json.loads(html.unescape(match.group(1)))
        by_name = {item["name"]: item for item in layouts}

        self.assertEqual(by_name["phone"]["mode"], "stacked")
        self.assertEqual(by_name["phone"]["viewBox"], "0 0 320 524")
        self.assertEqual(by_name["phone"]["pointerDetail"], "Monica")
        self.assertEqual(by_name["phone"]["keyboardDetail"], "Mac mini")
        self.assertEqual(by_name["tablet"]["mode"], "grid")
        self.assertEqual(by_name["tablet"]["viewBox"], "0 0 640 384")
        self.assertEqual(by_name["tablet"]["focusedNode"], "mac-mini")
        self.assertEqual(by_name["desktop"]["mode"], "radial")
        self.assertEqual(by_name["desktop"]["viewBox"], "0 0 960 420")
        self.assertEqual(by_name["desktop"]["focusedNode"], "mac-mini")

        for layout in layouts:
            with self.subTest(viewport=layout["name"]):
                self.assertTrue(layout["allVisible"])
                self.assertTrue(layout["noHorizontalOverflow"])
                self.assertEqual(layout["tabStops"], 3)
                self.assertGreaterEqual(layout["labelPixels"], 11)
                self.assertTrue(layout["routesAfterGraph"])
                self.assertTrue(layout["detailReachable"])
