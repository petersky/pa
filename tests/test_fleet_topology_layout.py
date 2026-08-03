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
        for candidate in (
            "google-chrome",
            "chromium",
            "chromium-browser",
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        )
        if (
            executable := (
                shutil.which(candidate)
                if not candidate.startswith("/")
                else (candidate if Path(candidate).is_file() else None)
            )
        )
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
                "edges": [
                    {
                        "id": "sync-local-monica",
                        "kind": "sync",
                        "source": "local",
                        "target": "monica",
                        "status": "healthy",
                        "label": "default",
                    },
                    {
                        "id": "dispatch-monica-local",
                        "kind": "dispatch",
                        "source": "monica",
                        "target": "local",
                        "status": "degraded",
                        "label": "running",
                    },
                    {
                        "id": "repository-mini",
                        "kind": "repository",
                        "source": "mac-mini",
                        "target": "mac-mini",
                        "status": "healthy",
                        "label": "petersky/pa",
                    },
                    {
                        "id": "supervisor-mini-local",
                        "kind": "supervisor",
                        "source": "mac-mini",
                        "target": "local",
                        "status": "healthy",
                        "label": "PR petersky/pa#1",
                    },
                ],
            }
        ).replace("</", "<\\/")
        fixture = f"""<!doctype html>
<html><head><meta charset="utf-8"><link rel="stylesheet" href="{stylesheet}">
<style>:root {{--pa-surface:#fff;--pa-text:#172033;--pa-text-muted:#536079;--pa-border:#73809a;--pa-accent:#315fc7;--pa-ok:#167447;--pa-danger:#b42318;--pa-focus:#7047eb}}</style></head>
<body>
<div id="pa-fleet-root">
  <script type="application/json" id="pa-fleet-overview-data">{overview}</script>
  <section id="fixture-panel" style="width: 358px">
    <div class="fleet-topology-controls" role="group" aria-label="Topology viewport controls">
      <button type="button" data-fleet-topology-action="zoom-out">−</button>
      <button type="button" data-fleet-topology-action="zoom-in">+</button>
      <button type="button" data-fleet-topology-action="reset">Reset <span data-fleet-topology-scale>100%</span></button>
      <button type="button" data-fleet-topology-action="fit">Fit topology</button>
    </div>
    <div id="pa-fleet-topology" class="fleet-topology">
      <svg viewBox="0 0 960 420" role="img" aria-label="Fleet instance and activity topology"></svg>
      <p data-fleet-topology-state>Loading cached topology…</p>
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
    var autoNodeCount = svg.querySelectorAll("[data-fleet-node]").length;

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
        tabStops: svg.querySelectorAll('[data-fleet-node][role="button"][tabindex="0"]').length,
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
    desktop.autoNodeCount = autoNodeCount;

    var controller = api.controller();
    var firstEdge = svg.querySelector('[data-fleet-edge="sync-local-monica"]');
    var firstEdgePath = firstEdge.querySelector(".fleet-edge-visual");
    firstEdge.dispatchEvent(new MouseEvent("click", {{ bubbles: true }}));
    firstEdge = svg.querySelector('[data-fleet-edge="sync-local-monica"]');
    desktop.edgeDomStable = firstEdge.querySelector(".fleet-edge-visual") === firstEdgePath;
    firstEdge.dispatchEvent(new PointerEvent("pointerover", {{ bubbles: true }}));
    var nodeElsewhere = svg.querySelector('[data-fleet-node="mac-mini"]');
    nodeElsewhere.dispatchEvent(new PointerEvent("pointerover", {{ bubbles: true }}));
    desktop.selectedSurvivesHover = firstEdge.classList.contains("fleet-selected");
    var layers = Array.from(svg.querySelectorAll("[data-fleet-layer]"));
    desktop.layerOrder = layers.map(function (layer) {{ return layer.dataset.fleetLayer; }});
    desktop.nodeBeforeEdge = layers.findIndex(function (layer) {{ return layer.dataset.fleetLayer === "nodes"; }}) <
      layers.findIndex(function (layer) {{ return layer.dataset.fleetLayer === "edges"; }});
    desktop.edgeHitWidth = Number(getComputedStyle(
      svg.querySelector('[data-fleet-layer="interactions"] .fleet-edge-hit')
    ).strokeWidth.replace("px", ""));
    desktop.edgeSelectedHaloVisible = getComputedStyle(
      svg.querySelector('[data-fleet-layer="edges"] .fleet-selected .fleet-edge-halo')
    ).stroke !== "none";
    var selectedNode = svg.querySelector('[data-fleet-node="mac-mini"]');
    selectedNode.dispatchEvent(new MouseEvent("click", {{ bubbles: true }}));
    selectedNode = svg.querySelector('[data-fleet-node="mac-mini"]');
    desktop.nodeSelectedHaloVisible = getComputedStyle(
      selectedNode.querySelector(".fleet-node-halo")
    ).stroke !== "none";
    api.render();
    desktop.nodeSelectionSurvivesRerender = svg.querySelector(
      '[data-fleet-node="mac-mini"]'
    ).classList.contains("fleet-selected");
    desktop.edgesAfterHover = svg.querySelectorAll(
      '[data-fleet-layer="interactions"] [data-fleet-edge]'
    ).length;
    desktop.visibleEdgePaths = Array.from(svg.querySelectorAll(".fleet-edge-visual"))
      .every(function (path) {{
        var style = getComputedStyle(path);
        return style.display !== "none" && style.visibility !== "hidden" &&
          Number(style.opacity || 1) > 0;
      }});
    svg.dispatchEvent(new PointerEvent("pointerout", {{ bubbles: true }}));
    desktop.edgesAfterExit = svg.querySelectorAll(
      '[data-fleet-layer="interactions"] [data-fleet-edge]'
    ).length;

    document.querySelector('[data-fleet-topology-action="zoom-in"]').click();
    var zoomed = Object.assign({{}}, controller.viewport);
    api.render();
    desktop.viewportPreserved = controller.viewport.scale === zoomed.scale &&
      controller.viewport.x === zoomed.x && controller.viewport.y === zoomed.y;
    desktop.wheelAccepted = !svg.dispatchEvent(new WheelEvent("wheel", {{
      bubbles: true, cancelable: true, deltaY: -100, clientX: 480, clientY: 210
    }}));
    desktop.wheelScale = controller.viewport.scale;

    var surface = svg.querySelector("[data-fleet-pan-surface]");
    surface.dispatchEvent(new PointerEvent("pointerdown", {{
      bubbles: true, cancelable: true, pointerId: 41, button: 0, clientX: 200, clientY: 200
    }}));
    svg.dispatchEvent(new PointerEvent("pointermove", {{
      bubbles: true, pointerId: 41, clientX: 250, clientY: 230
    }}));
    desktop.panMoved = controller.viewport.x !== zoomed.x || controller.viewport.y !== zoomed.y;
    svg.dispatchEvent(new PointerEvent("pointercancel", {{
      bubbles: true, pointerId: 41
    }}));
    desktop.panCancelled = !host.classList.contains("is-panning") &&
      controller.pointerId === null;

    document.querySelector('[data-fleet-topology-action="reset"]').click();
    desktop.resetScale = controller.viewport.scale;
    desktop.resetX = controller.viewport.x;
    desktop.resetY = controller.viewport.y;
    document.querySelector('[data-fleet-topology-action="fit"]').click();
    desktop.fitWithinBounds = controller.viewport.scale >= 0.5 &&
      controller.viewport.scale <= 3;
    var zoomIn = document.querySelector('[data-fleet-topology-action="zoom-in"]');
    var zoomOut = document.querySelector('[data-fleet-topology-action="zoom-out"]');
    for (var zoomIndex = 0; zoomIndex < 20; zoomIndex += 1) zoomIn.click();
    desktop.maxScale = controller.viewport.scale;
    desktop.zoomInDisabled = zoomIn.disabled;
    for (var zoomOutIndex = 0; zoomOutIndex < 40; zoomOutIndex += 1) zoomOut.click();
    desktop.minScale = controller.viewport.scale;
    desktop.zoomOutDisabled = zoomOut.disabled;

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
        self.assertEqual(by_name["desktop"]["autoNodeCount"], 3)
        self.assertTrue(by_name["desktop"]["selectedSurvivesHover"])
        self.assertEqual(
            by_name["desktop"]["layerOrder"],
            ["nodes", "edges", "labels", "interactions"],
        )
        self.assertTrue(by_name["desktop"]["nodeBeforeEdge"])
        self.assertGreaterEqual(by_name["desktop"]["edgeHitWidth"], 18)
        self.assertTrue(by_name["desktop"]["edgeSelectedHaloVisible"])
        self.assertTrue(by_name["desktop"]["nodeSelectedHaloVisible"])
        self.assertTrue(by_name["desktop"]["nodeSelectionSurvivesRerender"])
        self.assertTrue(by_name["desktop"]["edgeDomStable"])
        self.assertEqual(by_name["desktop"]["edgesAfterHover"], 4)
        self.assertTrue(by_name["desktop"]["visibleEdgePaths"])
        self.assertEqual(by_name["desktop"]["edgesAfterExit"], 4)
        self.assertTrue(by_name["desktop"]["viewportPreserved"])
        self.assertTrue(by_name["desktop"]["wheelAccepted"])
        self.assertGreater(by_name["desktop"]["wheelScale"], 1)
        self.assertTrue(by_name["desktop"]["panMoved"])
        self.assertTrue(by_name["desktop"]["panCancelled"])
        self.assertEqual(by_name["desktop"]["resetScale"], 1)
        self.assertEqual(by_name["desktop"]["resetX"], 0)
        self.assertEqual(by_name["desktop"]["resetY"], 0)
        self.assertTrue(by_name["desktop"]["fitWithinBounds"])
        self.assertEqual(by_name["desktop"]["maxScale"], 3)
        self.assertTrue(by_name["desktop"]["zoomInDisabled"])
        self.assertEqual(by_name["desktop"]["minScale"], 0.5)
        self.assertTrue(by_name["desktop"]["zoomOutDisabled"])

        for layout in layouts:
            with self.subTest(viewport=layout["name"]):
                self.assertTrue(layout["allVisible"])
                self.assertTrue(layout["noHorizontalOverflow"])
                self.assertEqual(layout["tabStops"], 3)
                self.assertGreaterEqual(layout["labelPixels"], 11)
                self.assertTrue(layout["routesAfterGraph"])
                self.assertTrue(layout["detailReachable"])
