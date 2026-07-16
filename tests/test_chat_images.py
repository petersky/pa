"""Image attachment validation and ACP prompt transport tests."""

from __future__ import annotations

import asyncio
import base64
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from jinja2 import Environment, FileSystemLoader
from pydantic import ValidationError

from pa.acp.client import AgentConnection
from pa.instance.quiesce import ImageAttachment, QueuedPrompt
from pa.modules.agent_chat import PromptBody, session_prompt


def _image(name: str = "pixel.png") -> ImageAttachment:
    return ImageAttachment(
        name=name,
        mime_type="image/png",
        data=base64.b64encode(b"png bytes").decode(),
    )


class ImageAttachmentTests(unittest.TestCase):
    def test_rejects_unsupported_or_invalid_images(self) -> None:
        with self.assertRaises(ValidationError):
            ImageAttachment(name="vector.svg", mime_type="image/svg+xml", data="YWJj")
        with self.assertRaises(ValidationError):
            ImageAttachment(name="broken.png", mime_type="image/png", data="not base64")

    def test_prompt_can_contain_only_images(self) -> None:
        body = PromptBody(images=[_image()])
        queued = QueuedPrompt(message="", images=body.images)

        self.assertEqual(body.message, "")
        self.assertEqual(queued.public_dict()["images"], [
            {"name": "pixel.png", "mime_type": "image/png"}
        ])
        self.assertNotIn("data", queued.public_dict()["images"][0])


class AcpImagePromptTests(unittest.TestCase):
    def test_sends_text_and_image_content_blocks(self) -> None:
        store = MagicMock()
        connection = AgentConnection(MagicMock(), store)
        connection.session = MagicMock(
            id="pa-session",
            external_session_id="acp-session",
            status="idle",
            metrics_json={},
        )
        connection._conn = MagicMock()
        connection._conn.prompt = AsyncMock(
            return_value=SimpleNamespace(stop_reason="end_turn", usage=None)
        )

        async def run() -> None:
            with patch("pa.acp.client.capture_from_updates"):
                await connection.prompt("What is shown?", images=[_image()])

        asyncio.run(run())
        prompt = connection._conn.prompt.await_args.kwargs["prompt"]
        self.assertEqual([block.type for block in prompt], ["text", "image"])
        self.assertEqual(prompt[1].mime_type, "image/png")
        self.assertEqual(prompt[1].data, _image().data)


class ChatPromptEndpointTests(unittest.TestCase):
    def test_forwards_image_only_prompt_to_runtime(self) -> None:
        runtime = MagicMock()
        runtime.prompt = AsyncMock(return_value="started")
        runtime._queue = []

        async def run() -> dict:
            with (
                patch("pa.modules.agent_chat._runtime_or_404", return_value=runtime),
                patch("pa.modules.agent_chat.get_principal_id", return_value="user:test"),
            ):
                return await session_prompt(
                    MagicMock(),
                    "session-1",
                    PromptBody(images=[_image()]),
                )

        result = asyncio.run(run())
        self.assertTrue(result["started"])
        runtime.prompt.assert_awaited_once()
        self.assertEqual(runtime.prompt.await_args.kwargs["images"][0].name, "pixel.png")


class ChatWidgetTemplateTests(unittest.TestCase):
    def test_shared_widget_exposes_drop_target_and_attach_control(self) -> None:
        template_root = Path(__file__).parents[1] / "src" / "pa" / "server" / "templates"
        env = Environment(loader=FileSystemLoader(template_root), autoescape=True)
        html = env.get_template("partials/agent/chat-widget.html").render()

        self.assertIn("data-acw-input", html)
        self.assertIn("drop images here", html)
        self.assertIn("data-acw-file-input", html)
        self.assertIn("data-acw-attach", html)
        self.assertIn("multiple hidden", html)


if __name__ == "__main__":
    unittest.main()
