import os
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from pa.browser.cdp import CdpError, CdpPage, validate_browser_url
from pa.browser.manager import BrowserAttachment, _browser_executable


class BrowserUrlTests(unittest.TestCase):
    def test_allows_web_and_blank_urls(self):
        for url in ("https://example.com", "http://127.0.0.1:8080", "about:blank"):
            self.assertEqual(validate_browser_url(url), url)

    def test_rejects_privileged_and_script_urls(self):
        for url in ("file:///etc/passwd", "javascript:alert(1)", "chrome://settings"):
            with self.assertRaises(CdpError):
                validate_browser_url(url)

    def test_executable_override(self):
        with patch.dict(os.environ, {"PA_BROWSER_EXECUTABLE": __file__}):
            self.assertEqual(_browser_executable(), __file__)


class BrowserAttachmentTests(unittest.IsolatedAsyncioTestCase):
    async def test_environment_and_public_state(self):
        process = AsyncMock()
        process.returncode = None
        attachment = BrowserAttachment(
            id="attachment-1",
            session_id="session-1",
            endpoint="http://127.0.0.1:9222",
            target_id="target-1",
            process=process,
            profile_dir=Path("/tmp/profile"),
        )
        with patch.object(CdpPage, "metadata", AsyncMock(return_value={"target_id": "target-1", "title": "PA", "url": "https://example.com"})):
            self.assertEqual((await attachment.state())["url"], "https://example.com")
        self.assertEqual(attachment.environment()["PA_BROWSER_TARGET_ID"], "target-1")
