"""Fleet instance identity presentation regressions."""

from __future__ import annotations

import shutil
import subprocess
import unittest
from pathlib import Path
from types import SimpleNamespace

from pa.core.ui.instance_identity import (
    canonical_instance_identities,
    present_instance_references,
    resolve_instance_identity,
)


ROOT = Path(__file__).parents[1]
SCRIPT = ROOT / "src" / "pa" / "server" / "static" / "js" / "instance-identity.js"


class _Registry:
    def __init__(self, instances):
        self.instances = instances

    def list_instances(self):
        return self.instances


class InstanceIdentityResolutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.first_id = "11111111-1111-4111-8111-111111111111"
        self.second_id = "22222222-2222-4222-8222-222222222222"
        self.registry = _Registry(
            [
                SimpleNamespace(instance_id=self.first_id, name="Monica"),
                SimpleNamespace(instance_id=self.second_id, name="Monica"),
            ]
        )
        self.ctx = SimpleNamespace(
            settings=SimpleNamespace(instance_id=self.first_id, instance_name="stale-local"),
            services={"fleet_registry": self.registry},
        )

    def test_canonical_names_win_and_duplicates_have_stable_suffixes(self) -> None:
        directory = canonical_instance_identities(self.ctx)
        by_id = {item["id"]: item for item in directory}
        self.assertEqual(by_id[self.first_id]["name"], "Monica")
        self.assertEqual(by_id[self.first_id]["display_name"], "Monica · 11111111")
        self.assertEqual(by_id[self.second_id]["display_name"], "Monica · 22222222")

    def test_rename_is_observed_without_reusing_stale_aliases(self) -> None:
        self.registry.instances[0].name = "Monica renamed"
        resolved = resolve_instance_identity(self.ctx, self.first_id)
        self.assertTrue(resolved["known"])
        self.assertEqual(resolved["display_name"], "Monica renamed")

    def test_local_settings_are_not_used_as_an_inferred_alias(self) -> None:
        self.registry.instances = []
        resolved = resolve_instance_identity(self.ctx, self.first_id)
        self.assertFalse(resolved["known"])
        self.assertEqual(resolved["display_name"], "Unknown instance · 11111111")

    def test_durable_messages_use_the_current_name_not_id_or_stale_name(self) -> None:
        message = present_instance_references(
            self.ctx,
            f"Dispatched to stale-local ({self.first_id})",
            self.first_id,
            "stale-local",
        )
        self.assertEqual(message, "Dispatched to Monica · 11111111 (Monica · 11111111)")

    def test_unknown_removed_instance_keeps_short_and_full_id(self) -> None:
        removed_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        resolved = resolve_instance_identity(self.ctx, removed_id)
        self.assertFalse(resolved["known"])
        self.assertEqual(resolved["id"], removed_id)
        self.assertEqual(resolved["display_name"], "Unknown instance · aaaaaaaa")


@unittest.skipUnless(shutil.which("node"), "node is required for identity UI tests")
class InstanceIdentityBrowserTests(unittest.TestCase):
    def run_node(self, body: str) -> None:
        harness = r'''
const fs = require("fs");
const vm = require("vm");
const assert = require("assert");
let copied = "";
Object.defineProperty(global, "navigator", {
  value: { clipboard: { writeText: value => { copied = value; return Promise.resolve(); } } },
  configurable: true,
});
global.document = {
  hidden: false,
  documentElement: { dataset: {} },
  getElementById: () => null,
  querySelector: () => null,
  querySelectorAll: () => [],
  addEventListener: () => {},
};
global.window = { addEventListener: () => {} };
vm.runInThisContext(fs.readFileSync(process.argv[1], "utf8"));
const identity = window.PAInstanceIdentity;
'''
        subprocess.run(
            [shutil.which("node"), "-e", harness + body, str(SCRIPT)],
            check=True,
            capture_output=True,
            text=True,
        )

    def test_directory_rename_duplicate_fallback_and_exact_copy(self) -> None:
        self.run_node(
            r'''
identity.setDirectory([
  { instance_id: "id-one", name: "Worker" },
  { instance_id: "id-two", name: "Worker" },
]);
assert.strictEqual(identity.resolve("id-one").displayName, "Worker · id-one");
assert.strictEqual(identity.resolve("id-two").displayName, "Worker · id-two");
identity.setDirectory([{ instance_id: "id-one", name: "Renamed worker" }]);
assert.strictEqual(identity.resolve("id-one").displayName, "Renamed worker");
assert.strictEqual(identity.resolve("id-two").displayName, "Unknown instance · id-two");
assert.ok(identity.html("id-two").includes('instance-id="id-two"'));
(async () => {
  await identity.copyText("full-canonical-instance-id");
  assert.strictEqual(copied, "full-canonical-instance-id");
  navigator.clipboard.writeText = () => Promise.reject(new Error("denied"));
  await assert.rejects(identity.copyText("full-canonical-instance-id"), /denied/);
})().catch(error => { throw error; });
'''
        )

    def test_accessible_copy_and_keyboard_contract_is_shared(self) -> None:
        source = SCRIPT.read_text()
        style = (ROOT / "src" / "pa" / "server" / "static" / "style.css").read_text()
        self.assertIn('aria-label="Copy instance ID"', source)
        self.assertIn('role="status" aria-live="polite"', source)
        self.assertIn('element.setFeedback("Copied")', source)
        self.assertIn('element.setFeedback("Copy failed")', source)
        self.assertIn('button.addEventListener("keydown"', source)
        self.assertIn(".instance-identity-copy:focus-visible", style)
        self.assertIn("max-width: 32ch", style)
        self.assertIn(
            ".agent-session-metadata .instance-identity-name",
            style,
        )


class InstanceIdentitySurfaceAuditTests(unittest.TestCase):
    def test_major_surfaces_use_shared_identity_component(self) -> None:
        templates = ROOT / "src" / "pa" / "server" / "templates"
        expected = {
            "macros/cards.html": "instance_identity(card.preferred_instance)",
            "pages/agent.html": "instance_identity(s.origin_instance_id)",
            "pages/fleet.html": "instance_identity(node.id)",
            "pages/pr-supervisor.html": "instance_identity(watch.owner_instance_id)",
            "pages/projects.html": "instance_identity(checkout.instance_id)",
            "pages/settings.html": "instance_identity(status.instance_id)",
            "partials/card-detail.html": "instance_identity(card.preferred_instance)",
            "partials/card-detail-activity.html": "instance_identity(entry.instance_id)",
            "partials/card-detail-agent.html": "instance_identity(instance.get('id') or session.origin_instance_id)",
        }
        for relative, marker in expected.items():
            with self.subTest(surface=relative):
                self.assertIn(marker, (templates / relative).read_text())

        fleet_js = (ROOT / "src" / "pa" / "server" / "static" / "js" / "fleet.js").read_text()
        self.assertIn("identityHtml(item.instance_id)", fleet_js)
        self.assertIn("identityHtml(local.instance_id)", fleet_js)
        self.assertIn("endpointIdentityHtml(edge.source)", fleet_js)
        self.assertNotIn("<code>{{ checkout.instance_id }}</code>", (templates / "pages/projects.html").read_text())
        self.assertNotIn("owner {{ watch.owner_instance_id", (templates / "pages/pr-supervisor.html").read_text())

        for relative in ("pages/work.html", "pages/fleet.html", "partials/card-new.html"):
            with self.subTest(instance_selector=relative):
                source = (templates / relative).read_text()
                self.assertIn("data-instance-identity-select", source)
                self.assertIn("data-instance-identity-selection", source)


if __name__ == "__main__":
    unittest.main()
