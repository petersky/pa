"""Bounded transport encoding of complete Fleet representations."""
from __future__ import annotations

import asyncio
import gzip
import io
import re

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

_PATHS = frozenset({"/fleet", "/api/fleet/overview"})
_QUALITY = re.compile(r"(?:0(?:\.\d{0,3})?|1(?:\.0{0,3})?)\Z")
_MIN_BYTES = 4096
# Bound total buffering; oversized responses keep their original representation.
_MAX_BYTES = 4 * 1024 * 1024
_SLICE_BYTES = 256 * 1024


async def _compress_complete(body: bytes) -> bytes:
    # A complete Response already owns these bytes. Yield between bounded CPU
    # slices without buffering any streaming response or emitting partial gzip.
    with io.BytesIO() as output:
        with gzip.GzipFile(fileobj=output, mode="wb", compresslevel=1, mtime=0) as encoded:
            view = memoryview(body)
            for offset in range(0, len(body), _SLICE_BYTES):
                encoded.write(view[offset:offset + _SLICE_BYTES])
                if offset + _SLICE_BYTES < len(body):
                    await asyncio.sleep(0)
        return output.getvalue()


def _accepts_gzip(headers: Headers) -> bool:
    qualities = []
    for field in headers.getlist("accept-encoding"):
        for item in field.split(","):
            coding, *parameters = item.strip().lower().split(";")
            if coding.strip() != "gzip":
                continue
            quality = 1.0
            if parameters:
                parameter = parameters[0].strip()
                if len(parameters) != 1 or not parameter.startswith("q="):
                    quality = 0.0
                else:
                    value = parameter[2:].strip()
                    quality = float(value) if _QUALITY.fullmatch(value) else 0.0
            qualities.append(quality)
    # Decline ambiguous duplicates and malformed qualities conservatively.
    # Wildcards can still receive identity, as before this optional encoding.
    return bool(qualities) and min(qualities) > 0


class FleetCompressionMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope.get("method") != "GET" or scope.get("path") not in _PATHS:
            await self.app(scope, receive, send)
            return
        request_headers = Headers(scope=scope)
        accepts = _accepts_gzip(request_headers)
        pending: Message | None = None

        async def encoded_send(message: Message) -> None:
            nonlocal pending
            if message["type"] == "http.response.start":
                pending = {**message, "headers": list(message.get("headers", []))}
                return
            if pending is not None:
                headers = MutableHeaders(raw=pending["headers"])
                body = message.get("body", b"")
                cache_directives = {v.strip().lower() for field in headers.getlist("cache-control") for v in field.split(",")}
                eligible = (
                    message["type"] == "http.response.body"
                    and not message.get("more_body", False)
                    and pending["status"] == 200
                    and _MIN_BYTES <= len(body) <= _MAX_BYTES
                    and headers.get("content-type", "").split(";", 1)[0].strip().lower()
                    in {"text/html", "application/json"}
                    and not any(name in headers for name in ("content-encoding", "content-range", "etag"))
                    and "range" not in request_headers
                    and "no-transform" not in cache_directives
                )
                if eligible:
                    vary = [v.strip() for field in headers.getlist("vary") for v in field.split(",") if v.strip()]
                    if not {"*", "accept-encoding"}.intersection(v.lower() for v in vary):
                        headers["Vary"] = ", ".join([*vary, "Accept-Encoding"])
                    if accepts:
                        compressed = await _compress_complete(body)
                        if len(compressed) < len(body):
                            headers["Content-Encoding"] = "gzip"
                            headers["Content-Length"] = str(len(compressed))
                            message = {**message, "body": compressed}
                # Never accumulate streaming chunks or delay their first body.
                # pathsend and every other non-buffered message pass unchanged.
                await send(pending)
                pending = None
            await send(message)

        await self.app(scope, receive, encoded_send)
