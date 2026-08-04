from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import re
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import quote, urlsplit

import httpx
import websockets

from pa.intake.security import validate_discord_attachment_url

logger = logging.getLogger(__name__)
MAX_ARTIFACT_BYTES = 25 * 1024 * 1024
_TELEGRAM_FILE_PATH = re.compile(r"^[A-Za-z0-9_./-]{1,2048}$")


class ChannelTransportError(RuntimeError):
    pass


class ChannelTransport:
    def __init__(
        self,
        *,
        telegram_bot_token: str = "",
        discord_bot_token: str = "",
        client: httpx.Client | None = None,
    ) -> None:
        self.telegram_bot_token = telegram_bot_token
        self.discord_bot_token = discord_bot_token
        self._client = client or httpx.Client(
            timeout=httpx.Timeout(15.0, connect=5.0), follow_redirects=False
        )
        self._owns_client = client is None

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def _download(
        self,
        url: str,
        *,
        expected_size: int | None,
        headers: dict[str, str] | None = None,
    ) -> bytes:
        chunks: list[bytes] = []
        size = 0
        with self._client.stream("GET", url, headers=headers) as response:
            if response.status_code >= 400:
                raise ChannelTransportError(
                    f"artifact download failed with HTTP {response.status_code}"
                )
            declared = int(response.headers.get("content-length") or 0)
            if declared > MAX_ARTIFACT_BYTES or (
                expected_size is not None and declared and declared != expected_size
            ):
                raise ChannelTransportError(
                    "artifact size exceeds or contradicts declared limits"
                )
            for chunk in response.iter_bytes():
                size += len(chunk)
                if size > MAX_ARTIFACT_BYTES:
                    raise ChannelTransportError("artifact exceeds the 25 MB limit")
                chunks.append(chunk)
        if expected_size is not None and size != expected_size:
            raise ChannelTransportError(
                "downloaded artifact size does not match provider metadata"
            )
        return b"".join(chunks)

    def fetch_telegram_file(self, file_id: str, *, expected_size: int | None) -> bytes:
        if not self.telegram_bot_token:
            raise ChannelTransportError("Telegram bot token is not configured")
        response = self._client.post(
            f"https://api.telegram.org/bot{self.telegram_bot_token}/getFile",
            json={"file_id": file_id},
        )
        if response.status_code >= 400:
            raise ChannelTransportError(
                f"Telegram getFile failed with HTTP {response.status_code}"
            )
        payload = response.json()
        path = str((payload.get("result") or {}).get("file_path") or "")
        if (
            not path
            or ".." in path.split("/")
            or not _TELEGRAM_FILE_PATH.fullmatch(path)
        ):
            raise ChannelTransportError("Telegram returned an invalid file path")
        return self._download(
            f"https://api.telegram.org/file/bot{self.telegram_bot_token}/{quote(path, safe='/')}",
            expected_size=expected_size,
        )

    def fetch_discord_file(self, url: str, *, expected_size: int | None) -> bytes:
        if not validate_discord_attachment_url(url):
            raise ChannelTransportError(
                "Discord attachment URL is not an approved CDN origin"
            )
        return self._download(url, expected_size=expected_size)

    def configure_telegram_webhook(self, url: str, secret_token: str) -> None:
        if not self.telegram_bot_token or not url:
            return
        if urlsplit(url).scheme != "https":
            raise ChannelTransportError("Telegram webhook URL must use HTTPS")
        response = self._client.post(
            f"https://api.telegram.org/bot{self.telegram_bot_token}/setWebhook",
            json={
                "url": url,
                "secret_token": secret_token,
                "allowed_updates": [
                    "message",
                    "edited_message",
                    "channel_post",
                    "edited_channel_post",
                    "message_reaction",
                ],
                "drop_pending_updates": False,
            },
        )
        if response.status_code >= 400 or not response.json().get("ok"):
            raise ChannelTransportError("Telegram setWebhook failed")

    def send_telegram(
        self,
        *,
        conversation_id: str,
        thread_id: str | None,
        reply_to_message_id: str | None,
        text: str,
    ) -> dict[str, Any]:
        if not self.telegram_bot_token:
            raise ChannelTransportError("Telegram bot token is not configured")
        base = f"https://api.telegram.org/bot{self.telegram_bot_token}"
        progress: dict[str, Any] = {"chat_id": conversation_id, "action": "typing"}
        if thread_id:
            progress["message_thread_id"] = thread_id
        with contextlib.suppress(httpx.HTTPError):
            self._client.post(f"{base}/sendChatAction", json=progress)
        payload: dict[str, Any] = {
            "chat_id": conversation_id,
            "text": text[:4096],
            "disable_web_page_preview": True,
        }
        if thread_id:
            payload["message_thread_id"] = thread_id
        if reply_to_message_id:
            payload["reply_parameters"] = {
                "message_id": int(reply_to_message_id),
                "allow_sending_without_reply": True,
            }
        response = self._client.post(f"{base}/sendMessage", json=payload)
        if response.status_code >= 400:
            raise ChannelTransportError(
                f"Telegram sendMessage failed with HTTP {response.status_code}"
            )
        result = response.json().get("result") or {}
        return {
            "provider_message_id": str(result.get("message_id") or "") or None,
            "provider_delivery_id": None,
        }

    def send_discord(
        self,
        *,
        conversation_id: str,
        reply_to_message_id: str | None,
        text: str,
        nonce: str,
    ) -> dict[str, Any]:
        if not self.discord_bot_token:
            raise ChannelTransportError("Discord bot token is not configured")
        url = f"https://discord.com/api/v10/channels/{quote(conversation_id, safe='')}"
        headers = {"Authorization": f"Bot {self.discord_bot_token}"}
        with contextlib.suppress(httpx.HTTPError):
            self._client.post(f"{url}/typing", headers=headers)
        payload: dict[str, Any] = {
            "content": text[:2000],
            "allowed_mentions": {"parse": []},
            "nonce": nonce[:25],
            "enforce_nonce": True,
        }
        if reply_to_message_id:
            payload["message_reference"] = {
                "message_id": reply_to_message_id,
                "fail_if_not_exists": False,
            }
        response = self._client.post(f"{url}/messages", headers=headers, json=payload)
        if response.status_code >= 400:
            raise ChannelTransportError(
                f"Discord create message failed with HTTP {response.status_code}"
            )
        result = response.json()
        return {
            "provider_message_id": str(result.get("id") or "") or None,
            "provider_delivery_id": str(result.get("nonce") or "") or None,
        }


class DiscordGateway:
    """Reconnecting Discord Gateway consumer for messages, edits, reactions, and threads."""

    INTENTS = (1 << 0) | (1 << 9) | (1 << 10) | (1 << 12) | (1 << 13) | (1 << 15)
    EVENTS = {
        "MESSAGE_CREATE",
        "MESSAGE_UPDATE",
        "MESSAGE_REACTION_ADD",
        "MESSAGE_REACTION_REMOVE",
    }

    def __init__(
        self,
        token: str,
        handler: Callable[[str, dict[str, Any], int | None], Awaitable[None]],
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.token = token
        self.handler = handler
        self._client = http_client or httpx.AsyncClient(timeout=10.0)
        self._owns_client = http_client is None
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._threads: dict[str, str] = {}

    def start(self) -> None:
        if self.token and (not self._task or self._task.done()):
            self._task = asyncio.create_task(self._run(), name="pa-discord-gateway")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        if self._owns_client:
            await self._client.aclose()

    async def _gateway_url(self) -> str:
        response = await self._client.get(
            "https://discord.com/api/v10/gateway/bot",
            headers={"Authorization": f"Bot {self.token}"},
        )
        response.raise_for_status()
        value = str(response.json().get("url") or "")
        if not value.startswith("wss://"):
            raise ChannelTransportError("Discord returned an invalid Gateway URL")
        return value + "?v=10&encoding=json"

    async def _run(self) -> None:
        delay = 1.0
        while not self._stop.is_set():
            try:
                await self._connect_once(await self._gateway_url())
                delay = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Discord Gateway disconnected",
                    extra={"error_type": type(exc).__name__},
                )
                try:
                    await asyncio.wait_for(
                        self._stop.wait(), timeout=delay + random.random()
                    )
                except TimeoutError:
                    pass
                delay = min(delay * 2, 60.0)

    async def _connect_once(self, url: str) -> None:
        async with websockets.connect(
            url,
            open_timeout=10,
            close_timeout=5,
            max_size=2 * 1024 * 1024,
            ping_interval=None,
        ) as socket:
            hello = json.loads(await asyncio.wait_for(socket.recv(), timeout=15))
            if int(hello.get("op", -1)) != 10:
                raise ChannelTransportError("Discord Gateway did not send HELLO")
            interval = (
                float((hello.get("d") or {}).get("heartbeat_interval") or 45000) / 1000
            )
            sequence: int | None = None

            async def heartbeat() -> None:
                while True:
                    await asyncio.sleep(interval)
                    await socket.send(json.dumps({"op": 1, "d": sequence}))

            heartbeat_task = asyncio.create_task(heartbeat())
            try:
                await socket.send(
                    json.dumps(
                        {
                            "op": 2,
                            "d": {
                                "token": self.token,
                                "intents": self.INTENTS,
                                "properties": {
                                    "os": "linux",
                                    "browser": "pa",
                                    "device": "pa",
                                },
                            },
                        }
                    )
                )
                async for raw in socket:
                    frame = json.loads(raw)
                    if frame.get("s") is not None:
                        sequence = int(frame["s"])
                    op = int(frame.get("op", -1))
                    if op == 7:
                        return
                    if op == 9:
                        await asyncio.sleep(random.uniform(1, 5))
                        return
                    if op != 0:
                        continue
                    event_name = str(frame.get("t") or "")
                    data = dict(frame.get("d") or {})
                    if event_name in {"THREAD_CREATE", "THREAD_UPDATE"}:
                        if data.get("id") and data.get("parent_id"):
                            self._threads[str(data["id"])] = str(data["parent_id"])
                        continue
                    if event_name == "THREAD_DELETE":
                        self._threads.pop(str(data.get("id") or ""), None)
                        continue
                    channel_id = str(data.get("channel_id") or "")
                    if channel_id in self._threads:
                        data["_thread_parent_id"] = self._threads[channel_id]
                    if event_name in self.EVENTS:
                        await self.handler(event_name, data, sequence)
            finally:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task
