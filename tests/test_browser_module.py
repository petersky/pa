import inspect
import unittest

from pa.modules.browser import BrowserModule, browser_capabilities


class FakeMcp:
    def __init__(self):
        self.functions = {}

    def tool(self):
        def register(function):
            self.functions[function.__name__] = function
            return function

        return register


class BrowserMcpSchemaTests(unittest.IsolatedAsyncioTestCase):
    async def test_capability_schema_is_compact_and_complete(self):
        capabilities = await browser_capabilities()
        self.assertEqual(capabilities["schema"], "pa.browser-capabilities/v1")
        self.assertIn("click", capabilities["common_actions"])
        self.assertIn("pointer_down", capabilities["advanced_actions"])
        self.assertEqual(capabilities["buttons"]["numbers"], [0, 1, 2])
        self.assertEqual(capabilities["limits"]["max_actions"], 100)
        self.assertEqual(capabilities["cli"], "pa browser --help")

    async def test_common_legacy_and_semantic_tools_are_registered(self):
        mcp = FakeMcp()
        BrowserModule().register_mcp(mcp, object())
        expected = {
            "browser_attach",
            "browser_open",
            "browser_snapshot",
            "browser_click",
            "browser_type",
            "browser_resize",
            "browser_screenshot",
            "browser_state",
            "browser_back",
            "browser_detach",
            "browser_hover",
            "browser_press",
            "browser_press_key",
            "browser_scroll",
            "browser_drag",
            "browser_actions",
            "browser_share",
            "browser_capabilities",
            "browser_operation_outcome",
        }
        self.assertEqual(set(mcp.functions), expected)

        click = inspect.signature(mcp.functions["browser_click"])
        self.assertEqual(next(iter(click.parameters)), "selector")
        browser_type = inspect.signature(mcp.functions["browser_type"])
        self.assertEqual(
            list(browser_type.parameters)[:3], ["selector", "text", "clear"]
        )
