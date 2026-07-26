import asyncio
import tempfile
import unittest
from pathlib import Path
from urllib.parse import quote

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionError, BrowserSessionManager


@unittest.skipUnless(_browser_executable(), "managed Chromium is not installed")
class ManagedBrowserInteractionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.browser = BrowserManager(Path(self.temp_dir.name))
        self.manager = BrowserSessionManager(
            self.browser, instance_id="e2e-instance", idle_ttl_seconds=60
        )
        self.scope = BrowserScope("user:e2e", "session-e2e", "e2e-instance")
        html = """
        <!doctype html>
        <style>
          #scroll { width: 200px; height: 80px; overflow: auto }
          #spacer { height: 500px }
          #source, #target { width: 80px; height: 40px; margin: 12px }
        </style>
        <button id="button">Button</button>
        <form id="form"><input id="input"><button>Submit</button></form>
        <div id="scroll"><div id="spacer"></div></div>
        <div id="source">Source</div><div id="target">Target</div>
        <script>
          const button = document.querySelector('#button');
          button.addEventListener('mouseover', () => document.body.dataset.hover = 'yes');
          button.addEventListener('contextmenu', event => {
            event.preventDefault(); document.body.dataset.context = 'yes';
          });
          button.addEventListener('dblclick', () => document.body.dataset.double = 'yes');
          document.addEventListener('keydown', event => {
            if (event.key === 'k')
              document.body.dataset.key = `${event.ctrlKey}:${event.shiftKey}:${event.key}`;
          });
          document.querySelector('#form').addEventListener('submit', event => {
            event.preventDefault(); document.body.dataset.submit = 'yes';
          });
          let held = false;
          document.querySelector('#source').addEventListener('pointerdown', () => held = true);
          document.querySelector('#target').addEventListener('pointerup', () => {
            if (held) document.body.dataset.drag = 'yes';
          });
        </script>
        """
        await self.manager.attach(
            self.scope, url=f"data:text/html,{quote(html)}", width=900, height=700
        )

    async def asyncTearDown(self):
        await self.manager.close()
        await self.browser.close()
        self.temp_dir.cleanup()

    async def test_semantic_and_compound_interactions_in_managed_chromium(self):
        await self.manager.hover(self.scope, selector="#button")
        await self.manager.click(self.scope, selector="#button", button="right")
        await self.manager.click(self.scope, selector="#button", click_count=2)
        await self.manager.press(self.scope, key="k", modifiers=["Control", "Shift"])
        await self.manager.scroll(self.scope, selector="#scroll", delta_y=120)
        await self.manager.drag(
            self.scope, source_selector="#source", target_selector="#target", steps=4
        )
        await self.manager.type_text(
            self.scope, selector="#input", text="hello", submit=True, delay_ms=1
        )
        await self.manager.actions(
            self.scope,
            actions=[
                {"type": "pointer_move", "x": 10, "y": 10},
                {"type": "pointer_down", "button": "middle"},
                {"type": "key_down", "key": "Alt"},
                {"type": "pause", "duration_ms": 10},
                {"type": "key_up", "key": "Alt"},
                {"type": "pointer_up", "button": "middle"},
            ],
        )
        await asyncio.sleep(0.05)

        session = self.manager.resolve(self.scope)
        state = await session.page.evaluate(
            """({
              hover: document.body.dataset.hover,
              context: document.body.dataset.context,
              double: document.body.dataset.double,
              key: document.body.dataset.key,
              submit: document.body.dataset.submit,
              drag: document.body.dataset.drag,
              scroll: document.querySelector('#scroll').scrollTop,
              value: document.querySelector('#input').value
            })"""
        )
        self.assertEqual(state["hover"], "yes")
        self.assertEqual(state["context"], "yes")
        self.assertEqual(state["double"], "yes")
        self.assertEqual(state["key"], "true:true:k")
        self.assertEqual(state["submit"], "yes")
        self.assertEqual(state["drag"], "yes")
        self.assertGreater(state["scroll"], 0)
        self.assertEqual(state["value"], "hello")
        self.assertFalse(session.held_buttons)
        self.assertFalse(session.held_keys)

        with self.assertRaises(BrowserSessionError) as invalid_selector:
            await self.manager.click(self.scope, selector="[")
        self.assertEqual(invalid_selector.exception.code, "invalid_selector")

        snapshot = await self.manager.snapshot(self.scope)
        ref = snapshot["elements"][0]["ref"]
        await session.page.evaluate(
            "document.body.setAttribute('data-revision', String(Date.now()))"
        )
        await asyncio.sleep(0)
        with self.assertRaises(BrowserSessionError) as raised:
            await self.manager.click(self.scope, ref=ref)
        self.assertEqual(raised.exception.code, "stale_snapshot_reference")
