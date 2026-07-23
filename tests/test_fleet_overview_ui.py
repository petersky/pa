"""Deterministic browser-state regressions for incremental Fleet refresh."""

from __future__ import annotations

import shutil
import subprocess
import unittest
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).parents[1] / "src" / "pa" / "server" / "static" / "js" / "fleet.js"
)

NODE_HARNESS = r"""
const fs = require("fs");
const vm = require("vm");
const assert = require("assert");
global.window = {
  innerWidth: 1200,
  addEventListener: function () {},
};
global.document = {
  querySelector: function () { return null; },
  querySelectorAll: function () { return []; },
  addEventListener: function () {},
  body: { addEventListener: function () {} },
};
global.CSS = { escape: function (value) { return String(value); } };
vm.runInThisContext(fs.readFileSync(process.argv[1], "utf8"));
const model = window.PAFleetOverview;
const dimensions = [
  "reachability", "status", "providers", "update",
  "activity", "sync", "repositories", "supervisor"
];
function valueFor(dimension) {
  if (dimension === "reachability") return { health: "up" };
  if (dimension === "status") return { version: "1.2.3" };
  if (dimension === "providers") return [];
  if (dimension === "update") return { upgrade_available: false };
  if (dimension === "activity") return { state: "idle", sessions: [], dispatches: [] };
  if (dimension === "sync") return { consistent: true };
  return {};
}
function field(dimension, state, duration) {
  return {
    state: state || "fresh",
    value: valueFor(dimension),
    observed_at: "2026-07-22T12:00:00Z",
    duration_ms: duration == null ? 1 : duration,
    error: null,
  };
}
function node(id) {
  const fields = {};
  dimensions.forEach(function (dimension) { fields[dimension] = field(dimension); });
  return {
    id: id,
    name: id === "local" ? "Local" : id,
    url: "http://" + id + ":8080",
    zone: "default",
    local: id === "local",
    capabilities: [],
    dimensions: fields,
  };
}
function overview(ids) {
  return { dimensions: dimensions.slice(), nodes: ids.map(node), edges: [] };
}
function apply(state, generation, nodeId, dimension, value, elapsedMs) {
  return model.applyDimensionUpdate(state.overview, state.refresh, {
    generation: generation,
    nodeId: nodeId,
    dimension: dimension,
    value: value,
    elapsedMs: elapsedMs,
  });
}
"""


@unittest.skipUnless(shutil.which("node"), "node is required for Fleet UI behavior tests")
class FleetOverviewUiStateTests(unittest.TestCase):
    def run_node(self, body: str) -> None:
        subprocess.run(
            [shutil.which("node"), "-e", NODE_HARNESS + body, str(SCRIPT_PATH)],
            check=True,
            capture_output=True,
            text=True,
        )

    def test_timeout_followed_by_late_success_is_fresh_once(self) -> None:
        self.run_node(
            r"""
let state = model.beginRefresh(overview(["local"]), ["local"], ["providers"], 7, 0);
state = apply(state, 7, "local", "providers", {
  state: "timeout", value: [], error: "browser deadline", duration_ms: 5500
}, 5500);
let snapshot = model.createSnapshot(state.overview, state.refresh, { kind: "node", id: "local" });
assert.strictEqual(state.refresh.completed, 1);
assert.strictEqual(snapshot.nodesById.local.freshness, "timeout");
assert.strictEqual(snapshot.selectedNode.freshness, "timeout");
assert.ok(snapshot.nodesById.local.accessibleLabel.includes("freshness timeout"));

state = apply(state, 7, "local", "providers", field("providers", "fresh", 6001), 6001);
snapshot = model.createSnapshot(state.overview, state.refresh, { kind: "node", id: "local" });
assert.strictEqual(state.refresh.completed, 1, "late success must not double-count progress");
assert.strictEqual(snapshot.nodesById.local.freshness, "fresh");
assert.strictEqual(snapshot.nodesById.local.topologyStatus, "fresh");
assert.strictEqual(snapshot.selectedNode.freshness, "fresh");
assert.ok(snapshot.nodesById.local.accessibleLabel.includes("freshness fresh"));
assert.ok(!snapshot.nodesById.local.accessibleLabel.includes("timeout"));
"""
        )

    def test_out_of_order_dimension_completion_uses_current_snapshot(self) -> None:
        self.run_node(
            r"""
let state = model.beginRefresh(overview(["local"]), ["local"], dimensions, 11, 0);
dimensions.slice().reverse().forEach(function (dimension, index) {
  state = apply(state, 11, "local", dimension, field(dimension, "fresh", index + 1), index + 1);
  assert.strictEqual(state.refresh.completed, index + 1);
  assert.strictEqual(
    state.overview.nodes[0].dimensions[dimension].refreshing,
    false,
    "completed field must be terminal regardless of arrival order"
  );
});
const snapshot = model.createSnapshot(state.overview, state.refresh, null);
assert.strictEqual(snapshot.nodesById.local.freshness, "fresh");
assert.strictEqual(snapshot.nodesById.local.refreshing, false);
assert.strictEqual(state.refresh.completed, dimensions.length);
"""
        )

    def test_selected_detail_tracks_refresh_snapshot(self) -> None:
        self.run_node(
            r"""
let state = model.beginRefresh(overview(["local"]), ["local"], dimensions, 13, 0);
state = apply(state, 13, "local", "activity", {
  state: "error", value: valueFor("activity"), error: "probe failed"
}, 10);
let snapshot = model.createSnapshot(state.overview, state.refresh, { kind: "node", id: "local" });
assert.strictEqual(snapshot.selectedNode, snapshot.nodesById.local);
assert.strictEqual(snapshot.selectedNode.freshness, "error");
assert.strictEqual(snapshot.selectedNode.node.dimensions.activity.state, "error");

state = apply(state, 13, "local", "activity", field("activity", "fresh", 12), 12);
snapshot = model.createSnapshot(state.overview, state.refresh, { kind: "node", id: "local" });
assert.strictEqual(snapshot.selectedNode, snapshot.nodesById.local);
assert.strictEqual(snapshot.selectedNode.freshness, "fresh");
assert.strictEqual(snapshot.selectedNode.node.dimensions.activity.state, "fresh");
assert.strictEqual(state.refresh.completed, 1);
"""
        )

    def test_selected_watch_tracks_group_across_relationship_refresh(self) -> None:
        self.run_node(
            r"""
const groupId = "edge-supervisor-stable";
const selection = {
  kind: "edge-item",
  id: "watch-watch-b",
  edgeId: groupId,
};
let current = overview(["local", "peer-a"]);
current.edges = [{
  id: groupId,
  kind: "supervisor",
  source: "peer-a",
  target: "local",
  details: {
    items: [
      { id: "watch-watch-a", status: "healthy", details: { id: "watch-a" } },
      { id: "watch-watch-b", status: "degraded", details: { id: "watch-b" } },
    ],
  },
}];
let snapshot = model.createSnapshot(current, null, selection);
assert.strictEqual(snapshot.selection.edgeId, groupId);
assert.strictEqual(snapshot.selectedEdge.id, groupId);
assert.strictEqual(snapshot.selectedEdgeItem.details.id, "watch-b");

const incoming = overview(["local", "peer-a"]);
incoming.edges = [{
  id: groupId,
  kind: "supervisor",
  source: "peer-a",
  target: "local",
  details: {
    items: [
      { id: "watch-watch-b", status: "healthy", details: { id: "watch-b" } },
      { id: "watch-watch-a", status: "healthy", details: { id: "watch-a" } },
    ],
  },
}];
current = model.mergeMetadata(current, incoming);
snapshot = model.createSnapshot(current, null, selection);
assert.strictEqual(snapshot.selection.id, "watch-watch-b");
assert.strictEqual(snapshot.selection.edgeId, groupId);
assert.strictEqual(snapshot.selectedEdge.id, groupId);
assert.strictEqual(snapshot.selectedEdgeItem.details.id, "watch-b");
assert.strictEqual(snapshot.selectedEdgeItem.status, "healthy");
"""
        )

    def test_final_24_of_24_has_consistent_fresh_derived_state(self) -> None:
        self.run_node(
            r"""
const ids = ["local", "peer-a", "peer-b"];
let state = model.beginRefresh(overview(ids), ids, dimensions, 17, 0);
const work = [];
ids.forEach(function (id) {
  dimensions.forEach(function (dimension) { work.push([id, dimension]); });
});
const ordered = work.filter(function (_, index) { return index % 2 === 1; }).reverse()
  .concat(work.filter(function (_, index) { return index % 2 === 0; }));
ordered.forEach(function (item, index) {
  const value = item[0] === "local" && item[1] === "providers"
    ? { state: "timeout", value: [], error: "browser deadline", duration_ms: 5500 }
    : field(item[1], "fresh", index + 1);
  state = apply(state, 17, item[0], item[1], value, index + 1);
});
assert.strictEqual(state.refresh.completed, 24);
let snapshot = model.createSnapshot(state.overview, state.refresh, { kind: "node", id: "local" });
assert.strictEqual(snapshot.nodesById.local.freshness, "timeout");

state = apply(state, 17, "local", "providers", field("providers", "fresh", 6001), 6001);
snapshot = model.createSnapshot(state.overview, state.refresh, { kind: "node", id: "local" });
assert.strictEqual(snapshot.refresh.completed, 24);
assert.strictEqual(snapshot.refresh.total, 24);
assert.strictEqual(snapshot.selectedNode.freshness, "fresh");
snapshot.nodes.forEach(function (item) {
  assert.strictEqual(item.freshness, "fresh");
  assert.strictEqual(item.topologyStatus, "fresh");
  assert.strictEqual(item.refreshing, false);
  assert.ok(item.accessibleLabel.includes("freshness fresh"));
  assert.ok(!item.accessibleLabel.includes("timeout"));
});
"""
        )
