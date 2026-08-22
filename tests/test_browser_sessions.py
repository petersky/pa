import asyncio
import unittest
from types import SimpleNamespace

from pa.browser.session import (
    MAX_ACTIONS,
    BrowserScope,
    BrowserSessionError,
    BrowserSessionManager,
)


class FakePage:
    def __init__(self, target_id: str):
        self.target_id = target_id
        self.commands = []
        self.document_id = f"doc-{target_id}"
        self.revision = 0
        self.url = "about:blank"

    async def metadata(self):
        return {"target_id": self.target_id, "title": "Test", "url": self.url}

    async def viewport(self):
        return {"width": 800, "height": 600, "device_scale_factor": 1}

    async def command(self, method, params=None):
        self.commands.append((method, params or {}))
        return {}

    async def navigate_and_wait(self, url):
        self.url = url
        self.document_id += "-next"
        self.revision = 0
        return {"ready_state": "complete", "url": url, "title": "Test"}

    async def evaluate(self, expression):
        if "const elements =" in expression:
            return {
                "state": {
                    "document_id": self.document_id,
                    "revision": self.revision,
                    "url": self.url,
                },
                "document": {
                    "ready_state": "complete",
                    "url": self.url,
                    "title": "Test",
                    "body_text": "button",
                },
                "elements": [
                    {
                        "index": 0,
                        "locator": "#button",
                        "tag": "button",
                        "role": "",
                        "text": "Button",
                        "href": "",
                        "disabled": False,
                    }
                ],
            }
        if "document_id:" in expression and "__paBrowserDocumentId" in expression:
            return {
                "document_id": self.document_id,
                "revision": self.revision,
                "url": self.url,
            }
        if "document.querySelector" in expression:
            return {"ok": True, "x": 25, "y": 30}
        return "requestSubmit" not in expression

    async def screenshot(self):
        return b"png"


class FakeAttachment:
    def __init__(self, session_id: str):
        self.id = f"attachment-{session_id}"
        self.session_id = session_id
        self.target_id = f"target-{session_id}"
        self.process = SimpleNamespace(returncode=None)
        self._page = FakePage(self.target_id)
        self.width = 800
        self.height = 600
        self.device_scale_factor = 1

    @property
    def page(self):
        return self._page

    async def resize(self, width, height, *, device_scale_factor=1):
        self.width = width
        self.height = height
        self.device_scale_factor = device_scale_factor


class FakeBrowserManager:
    def __init__(self):
        self.attachments = {}
        self.detached = []

    async def attach(self, session_id, **_kwargs):
        attachment = FakeAttachment(session_id)
        self.attachments[session_id] = attachment
        return attachment

    async def detach(self, session_id):
        self.detached.append(session_id)
        self.attachments.pop(session_id, None)


def scope(session: str, principal: str = "user:one") -> BrowserScope:
    return BrowserScope(principal, session, "instance-1")


class BrowserSessionIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.browser = FakeBrowserManager()
        self.manager = BrowserSessionManager(
            self.browser, instance_id="instance-1", idle_ttl_seconds=60
        )

    async def test_default_sessions_are_isolated_and_cross_session_handles_fail(self):
        first = await self.manager.attach(scope("session-1"))
        second = await self.manager.attach(scope("session-2"))
        self.assertNotEqual(first["browser_handle"], second["browser_handle"])
        self.assertNotEqual(
            self.manager.resolve(scope("session-1")).attachment.profile_dir
            if hasattr(
                self.manager.resolve(scope("session-1")).attachment, "profile_dir"
            )
            else self.manager.resolve(scope("session-1")).attachment.session_id,
            self.manager.resolve(scope("session-2")).attachment.session_id,
        )
        with self.assertRaisesRegex(BrowserSessionError, "another principal or agent"):
            self.manager.resolve(scope("session-2"), first["browser_handle"])
        with self.assertRaisesRegex(BrowserSessionError, "another principal or agent"):
            self.manager.resolve(
                scope("session-1", "user:two"), first["browser_handle"]
            )
        with self.assertRaisesRegex(BrowserSessionError, "unknown or expired"):
            self.manager.resolve(scope("session-1"), "br_" + "A" * 43)

    async def test_context_quota_is_enforced_before_browser_launch(self):
        browser = FakeBrowserManager()
        manager = BrowserSessionManager(
            browser, instance_id="instance-1", max_contexts=1
        )
        await manager.attach(scope("session-1"))
        with self.assertRaises(BrowserSessionError) as raised:
            await manager.attach(scope("session-2"))
        self.assertEqual(raised.exception.code, "quota_exceeded")
        self.assertNotIn("automation-session-2", browser.attachments)

    async def test_sharing_is_explicit_scoped_single_use_and_audited(self):
        owner = await self.manager.attach(scope("session-owner"))
        grant = await self.manager.share(
            scope("session-owner"), authorized_session_id="session-guest"
        )
        shared = await self.manager.attach(
            scope("session-guest"), share_handle=grant["share_handle"]
        )
        self.assertEqual(shared["browser_handle"], owner["browser_handle"])
        self.assertEqual(shared["ownership"], "intentionally_shared")
        with self.assertRaises(BrowserSessionError):
            await self.manager.attach(
                scope("session-other"), share_handle=grant["share_handle"]
            )
        detached = await self.manager.detach(scope("session-guest"))
        self.assertTrue(detached["preserved"])
        self.assertIn("share", [record.action_class for record in self.manager.audit])

    async def test_user_owned_browser_is_preserved(self):
        attached = FakeAttachment("session-user")
        manager = BrowserSessionManager(
            self.browser,
            instance_id="instance-1",
            attached_lookup=lambda session_id: (
                attached if session_id == "session-user" else None
            ),
        )
        state = await manager.attach(scope("session-user"))
        self.assertEqual(state["ownership"], "user_owned")
        result = await manager.detach(scope("session-user"))
        self.assertTrue(result["preserved"])
        self.assertEqual(self.browser.detached, [])


class BrowserInputTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.browser = FakeBrowserManager()
        self.manager = BrowserSessionManager(
            self.browser, instance_id="instance-1", idle_ttl_seconds=60
        )
        await self.manager.attach(scope("session-1"))

    async def test_right_double_click_modifiers_and_deduplication(self):
        first = await self.manager.click(
            scope("session-1"),
            selector="#button",
            button="right",
            click_count=2,
            modifiers=["Control", "Shift"],
            operation_id="click-1",
        )
        second = await self.manager.click(
            scope("session-1"),
            selector="#button",
            button="right",
            click_count=2,
            modifiers=["Control", "Shift"],
            operation_id="click-1",
        )
        commands = self.manager.resolve(scope("session-1")).page.commands
        self.assertEqual(len(commands), 2)
        self.assertEqual(commands[0][1]["button"], "right")
        self.assertEqual(commands[0][1]["clickCount"], 2)
        self.assertEqual(commands[0][1]["modifiers"], 10)
        self.assertEqual(first["operation_id"], "click-1")
        self.assertTrue(second["deduplicated"])

    async def test_hover_press_scroll_drag_and_mixed_holds(self):
        await self.manager.hover(scope("session-1"), selector="#button")
        await self.manager.press(scope("session-1"), key="k", modifiers=["Control"])
        await self.manager.scroll(scope("session-1"), delta_y=250, selector="#button")
        await self.manager.drag(
            scope("session-1"),
            source_selector="#source",
            target_selector="#target",
            steps=3,
        )
        await self.manager.actions(
            scope("session-1"),
            actions=[
                {"type": "pointer_move", "x": 10, "y": 20},
                {"type": "pointer_down", "button": "left"},
                {"type": "key_down", "key": "Shift"},
                {"type": "wheel", "delta_y": 5},
                {"type": "key_up", "key": "Shift"},
                {"type": "pointer_up", "button": "left"},
            ],
        )
        session = self.manager.resolve(scope("session-1"))
        self.assertFalse(session.held_buttons)
        self.assertFalse(session.held_keys)
        methods = [method for method, _ in session.page.commands]
        self.assertIn("Input.dispatchKeyEvent", methods)
        self.assertIn("Input.dispatchMouseEvent", methods)

    async def test_type_supports_clear_slow_key_events_modifiers_and_submit(self):
        result = await self.manager.type_text(
            scope("session-1"),
            selector="#input",
            text="ab",
            clear=True,
            submit=True,
            delay_ms=1,
            modifiers=["Shift"],
        )
        self.assertEqual(result["characters"], 2)
        commands = self.manager.resolve(scope("session-1")).page.commands
        key_events = [
            payload
            for method, payload in commands
            if method == "Input.dispatchKeyEvent"
        ]
        self.assertTrue(any(item["key"] == "Enter" for item in key_events))
        self.assertTrue(any(item["key"] == "Shift" for item in key_events))

    async def test_snapshot_reference_survives_unrelated_dom_revision(self):
        snapshot = await self.manager.snapshot(scope("session-1"))
        ref = snapshot["elements"][0]["ref"]
        self.manager.resolve(scope("session-1")).page.revision += 1
        result = await self.manager.click(scope("session-1"), ref=ref)
        self.assertTrue(result["ok"])

    async def test_snapshot_reference_is_stale_after_navigation(self):
        snapshot = await self.manager.snapshot(scope("session-1"))
        ref = snapshot["elements"][0]["ref"]
        self.manager.resolve(scope("session-1")).page.document_id += "-next"
        with self.assertRaises(BrowserSessionError) as raised:
            await self.manager.click(scope("session-1"), ref=ref)
        self.assertEqual(raised.exception.code, "stale_snapshot_reference")
        self.assertTrue(raised.exception.retryable)

    async def test_operation_outcome_reports_running_completed_and_not_started(self):
        started = asyncio.Event()
        release = asyncio.Event()

        async def work(_session):
            started.set()
            await release.wait()
            return {"action": "test"}

        task = asyncio.create_task(
            self.manager.execute(
                scope("session-1"), "test", work, operation_id="lookup-1"
            )
        )
        await started.wait()
        running = await self.manager.operation_outcome(
            scope("session-1"), operation_id="lookup-1"
        )
        self.assertEqual(running["state"], "running")
        release.set()
        await task
        completed = await self.manager.operation_outcome(
            scope("session-1"), operation_id="lookup-1"
        )
        self.assertEqual(completed["state"], "completed")
        missing = await self.manager.operation_outcome(
            scope("session-1"), operation_id="lookup-missing"
        )
        self.assertEqual(missing["state"], "not_started")

    async def test_sequences_serialize_on_one_target_and_isolated_targets_parallelize(
        self,
    ):
        await self.manager.attach(scope("session-2"))
        running = 0
        peak = 0

        async def work(_session):
            nonlocal running, peak
            running += 1
            peak = max(peak, running)
            await asyncio.sleep(0.02)
            running -= 1
            return {"done": True}

        await asyncio.gather(
            self.manager.execute(scope("session-1"), "test", work),
            self.manager.execute(scope("session-1"), "test", work),
        )
        self.assertEqual(peak, 1)
        peak = 0
        await asyncio.gather(
            self.manager.execute(scope("session-1"), "test", work),
            self.manager.execute(scope("session-2"), "test", work),
        )
        self.assertEqual(peak, 2)


class BrowserSecurityAndRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.browser = FakeBrowserManager()
        self.manager = BrowserSessionManager(
            self.browser, instance_id="instance-1", idle_ttl_seconds=60
        )
        await self.manager.attach(scope("session-1"))

    def test_action_limits_keys_buttons_coordinates_and_pauses(self):
        invalid = [
            ([{"type": "pause", "duration_ms": 2001}], "invalid_pause"),
            ([{"type": "pointer_move", "x": 100001, "y": 0}], "invalid_coordinates"),
            ([{"type": "key_press", "key": "NotARealKey"}], "unsupported_key"),
            ([{"type": "pointer_down", "button": "back"}], "unsupported_button"),
            (
                [{"type": "pointer_move", "x": float("nan"), "y": 0}],
                "invalid_coordinates",
            ),
            ([{"type": "wheel", "delta_y": float("inf")}], "invalid_scroll_delta"),
            (
                [{"type": "key_press", "key": "a", "secret": True}],
                "invalid_action_sequence",
            ),
            ([{"type": "wheel", "delta_y": "many"}], "invalid_scroll_delta"),
            ([{"type": "pause", "duration_ms": "long"}], "invalid_pause"),
            (
                [{"type": "pause", "duration_ms": 2000}] * 6,
                "invalid_pause",
            ),
            (
                [{"type": "pause", "duration_ms": 1}] * (MAX_ACTIONS + 1),
                "quota_exceeded",
            ),
        ]
        for actions, code in invalid:
            with (
                self.subTest(code=code),
                self.assertRaises(BrowserSessionError) as raised,
            ):
                self.manager.validate_actions(actions)
            self.assertEqual(raised.exception.code, code)

    async def test_failed_action_releases_held_input_before_unlocking(self):
        session = self.manager.resolve(scope("session-1"))
        original = session.page.command

        async def fail_after_key(method, params=None):
            if method == "Input.dispatchMouseEvent":
                raise BrowserSessionError("browser_protocol_error", "failed")
            return await original(method, params)

        session.page.command = fail_after_key
        with self.assertRaises(BrowserSessionError):
            await self.manager.actions(
                scope("session-1"),
                actions=[
                    {"type": "key_down", "key": "Shift"},
                    {"type": "wheel", "delta_y": 1},
                ],
            )
        self.assertFalse(session.held_keys)

    async def test_concurrent_attach_mints_one_default_context(self):
        first, second = await asyncio.gather(
            self.manager.attach(scope("session-race")),
            self.manager.attach(scope("session-race")),
        )
        self.assertEqual(first["browser_handle"], second["browser_handle"])
        self.assertEqual(
            list(self.browser.attachments).count("automation-session-race"), 1
        )

    async def test_attach_retry_with_same_operation_id_is_deduplicated(self):
        first = await self.manager.attach(
            scope("session-attach"),
            url="https://example.test/one",
            operation_id="attach-1",
        )
        document_id = self.manager.resolve(scope("session-attach")).page.document_id
        second = await self.manager.attach(
            scope("session-attach"),
            url="https://example.test/two",
            operation_id="attach-1",
        )
        self.assertEqual(first["operation_id"], "attach-1")
        self.assertTrue(second["deduplicated"])
        self.assertEqual(
            self.manager.resolve(scope("session-attach")).page.document_id,
            document_id,
        )

    async def test_timeout_cancels_work_and_outcome_is_deterministic(self):
        cancelled = asyncio.Event()

        async def slow(_session):
            try:
                await asyncio.sleep(10)
            finally:
                cancelled.set()

        with self.assertRaises(BrowserSessionError) as raised:
            await self.manager.execute(
                scope("session-1"),
                "click",
                slow,
                operation_id="timeout-1",
                timeout=0.01,
            )
        self.assertEqual(raised.exception.code, "timeout")
        self.assertTrue(cancelled.is_set())
        outcome = await self.manager.operation_outcome(
            scope("session-1"), operation_id="timeout-1"
        )
        self.assertEqual(outcome["state"], "completed")
        self.assertEqual(outcome["result"]["error"]["code"], "timeout")
        replay = await self.manager.execute(
            scope("session-1"), "click", slow, operation_id="timeout-1"
        )
        self.assertTrue(replay["deduplicated"])

    async def test_simultaneous_transport_retry_executes_operation_once(self):
        calls = 0
        started = asyncio.Event()
        release = asyncio.Event()

        async def work(_session):
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return {"action": "test"}

        first = asyncio.create_task(
            self.manager.execute(
                scope("session-1"), "test", work, operation_id="retry-1"
            )
        )
        await started.wait()
        second = asyncio.create_task(
            self.manager.execute(
                scope("session-1"), "test", work, operation_id="retry-1"
            )
        )
        await asyncio.sleep(0)
        release.set()
        results = await asyncio.gather(first, second)
        self.assertEqual(calls, 1)
        self.assertEqual(sum(bool(item.get("deduplicated")) for item in results), 1)

    async def test_cancelled_sequence_is_released_and_retry_is_not_replayed(self):
        task = asyncio.create_task(
            self.manager.actions(
                scope("session-1"),
                actions=[
                    {"type": "key_down", "key": "Shift"},
                    {"type": "pause", "duration_ms": 1000},
                ],
                operation_id="cancel-1",
            )
        )
        await asyncio.sleep(0.02)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        session = self.manager.resolve(scope("session-1"))
        self.assertFalse(session.held_keys)
        retry = await self.manager.actions(
            scope("session-1"), actions=[{"type": "pause"}], operation_id="cancel-1"
        )
        self.assertTrue(retry["deduplicated"])
        self.assertEqual(retry["error"]["code"], "interrupted_sequence")

    async def test_ambiguous_failure_is_cached_and_never_replayed(self):
        calls = 0

        async def ambiguous(_session):
            nonlocal calls
            calls += 1
            raise BrowserSessionError(
                "navigation_invalidation", "navigated", ambiguous=True
            )

        with self.assertRaises(BrowserSessionError):
            await self.manager.execute(
                scope("session-1"),
                "click",
                ambiguous,
                operation_id="ambiguous-1",
            )
        replay = await self.manager.execute(
            scope("session-1"),
            "click",
            ambiguous,
            operation_id="ambiguous-1",
        )
        self.assertEqual(calls, 1)
        self.assertTrue(replay["deduplicated"])
        self.assertFalse(replay["ok"])

    async def test_restart_invalidates_old_handle_and_default_attach_recovers(self):
        old = self.manager.resolve(scope("session-1")).handle
        restarted = BrowserSessionManager(
            FakeBrowserManager(), instance_id="instance-1"
        )
        with self.assertRaises(BrowserSessionError) as raised:
            restarted.resolve(scope("session-1"), old)
        self.assertEqual(raised.exception.code, "invalid_browser_handle")
        new = await restarted.attach(scope("session-1"))
        self.assertNotEqual(new["browser_handle"], old)

    async def test_orphan_cleanup_and_audit_redaction(self):
        session = self.manager.resolve(scope("session-1"))
        session.expires_at = 0
        await self.manager.cleanup()
        self.assertNotIn(session.handle, self.manager.sessions)
        await self.manager.attach(scope("credential-session"))
        await self.manager.type_text(
            scope("credential-session"),
            selector="#password",
            text="super-secret-password",
        )
        rendered = repr(self.manager.audit)
        self.assertNotIn("super-secret-password", rendered)
        self.assertNotIn("#password", rendered)
