import asyncio
import gzip
import random
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from starlette.datastructures import Headers

from pa.server.fleet_compression import FleetCompressionMiddleware


class FleetCompressionTests(unittest.IsolatedAsyncioTestCase):
    async def response(self, *, accept="gzip", path="/api/fleet/overview", method="GET",
                       body=b'{"detail":"' + b"full metadata " * 1000 + b'"}',
                       status=200, headers=None, request_headers=None, streaming=False, content_type=b"application/json"):
        sent = []
        original_headers = [(b"content-type", content_type), (b"content-length", str(len(body)).encode()),
                            (b"set-cookie", b"unchanged=value; HttpOnly"), (b"cache-control", b"no-store")]
        original_headers.extend(headers or [])
        original_start = {"type": "http.response.start", "status": status, "headers": original_headers}
        async def app(scope, receive, send):
            await send(original_start)
            await send({"type": "http.response.body", "body": body, "more_body": streaming})
            if streaming:
                # The first chunk must have reached the client before the
                # application produces any subsequent chunk.
                self.assertEqual(len(sent), 2)
                self.assertEqual(sent[-1]["body"], body)
                await send({"type": "http.response.body", "body": b"tail", "more_body": False})
        raw_headers = [] if accept is None else [(b"accept-encoding", accept.encode())]
        raw_headers.extend(request_headers or [])
        await FleetCompressionMiddleware(app)({"type":"http","method":method,"path":path,"headers":raw_headers}, None, self.sender(sent))
        return sent, original_start

    @staticmethod
    def sender(sent):
        async def send(message): sent.append(message)
        return send

    async def test_complete_bytes_and_headers_survive_negotiated_encoding(self):
        sent, original = await self.response(headers=[(b"vary", b"Origin"), (b"vary", b"Cookie")])
        headers = Headers(raw=sent[0]["headers"])
        body = sent[1]["body"]
        self.assertEqual(headers["content-encoding"], "gzip")
        self.assertEqual(int(headers["content-length"]), len(body))
        self.assertEqual(gzip.decompress(body), b'{"detail":"' + b"full metadata " * 1000 + b'"}')
        self.assertEqual(headers["set-cookie"], "unchanged=value; HttpOnly")
        self.assertEqual(headers["cache-control"], "no-store")
        self.assertEqual(headers["vary"], "Origin, Cookie, Accept-Encoding")
        self.assertNotIn(b"content-encoding", dict(original["headers"]))

    async def test_identity_quality_and_duplicate_negotiation(self):
        for accept, compressed in [(None,False),("identity",False),("*",False),("gzip;q=0",False),
            ("gzip;q=0.000",False),("gzip;q=1.001",False),("gzip;q=nan",False),
            ("gzip;q=0, gzip",False),("GZIP; q=0.5",True),("br, gzip;q=1.000",True)]:
            with self.subTest(accept=accept):
                sent,_ = await self.response(accept=accept)
                self.assertEqual("content-encoding" in Headers(raw=sent[0]["headers"]), compressed)
                self.assertIn("Accept-Encoding", Headers(raw=sent[0]["headers"])["vary"])

    async def test_exclusions_do_not_invoke_compressor(self):
        for options in [dict(path="/api/cards"),dict(method="POST"),dict(method="HEAD"),dict(status=503),
            dict(body=b"small"),dict(body=b"x"*(4*1024*1024+1)),dict(headers=[(b"content-encoding",b"br")]),
            dict(headers=[(b"content-range",b"bytes 0-4/5")]),dict(headers=[(b"etag",b'"strong"')]),
            dict(request_headers=[(b"range",b"bytes=0-4")]),dict(headers=[(b"cache-control",b"no-transform")]),
            dict(content_type=b"text/event-stream; charset=utf-8"),dict(content_type=b"application/octet-stream")]:
            with self.subTest(options={k:len(v) if k=="body" else v for k,v in options.items()}):
                with patch("pa.server.fleet_compression.gzip.GzipFile",side_effect=AssertionError("excluded response")):
                    await self.response(**options)

    async def test_streams_flush_first_chunk_without_compression(self):
        with patch("pa.server.fleet_compression.gzip.GzipFile",side_effect=AssertionError("stream encoded")):
            sent,_ = await self.response(streaming=True)
        self.assertEqual(len(sent),3)
        self.assertNotIn("content-encoding",Headers(raw=sent[0]["headers"]))

    async def test_html_and_existing_vary(self):
        sent,_ = await self.response(path="/fleet",content_type=b"text/html; charset=utf-8",
                                    headers=[(b"vary",b"Accept-Encoding")])
        self.assertEqual(Headers(raw=sent[0]["headers"])["vary"],"Accept-Encoding")
        self.assertEqual(Headers(raw=sent[0]["headers"])["content-encoding"],"gzip")

    async def test_incompressible_body_is_not_expanded(self):
        body = random.Random(20261025).randbytes(5000)
        sent,_ = await self.response(body=body)
        self.assertEqual(sent[1]["body"],body)
        self.assertNotIn("content-encoding",Headers(raw=sent[0]["headers"]))

    async def test_request_metrics_count_encoded_bytes_and_full_elapsed_path(self):
        from pa.core.kernel import _ResponsivenessMiddleware
        class Runtime:
            def __init__(self): self.calls=[]
            def record_request(self,*args,**kwargs): self.calls.append((args,kwargs))
        runtime=Runtime()
        body=b'{"detail":"'+b"full metadata "*1000+b'"}'
        async def app(scope,receive,send):
            await send({"type":"http.response.start","status":200,
                        "headers":[(b"content-type",b"application/json")]})
            await send({"type":"http.response.body","body":body})
        sent=[]
        wrapped=_ResponsivenessMiddleware(FleetCompressionMiddleware(app),lambda:runtime)
        await wrapped({"type":"http","method":"GET","path":"/api/fleet/overview",
                       "headers":[(b"accept-encoding",b"gzip")],
                       "route":SimpleNamespace(path="/api/fleet/overview")},None,self.sender(sent))
        self.assertEqual(gzip.decompress(sent[1]["body"]),body)
        self.assertEqual(runtime.calls[0][1]["response_bytes"],len(sent[1]["body"]))
        self.assertFalse(runtime.calls[0][1]["streaming"])

    async def test_pathsend_passes_through_without_encoding(self):
        sent=[]
        start={"type":"http.response.start","status":200,"headers":[(b"content-type",b"application/json")]}
        pathsend={"type":"http.response.pathsend","path":"/private/tmp/not-opened"}
        async def app(scope,receive,send):
            await send(start)
            await send(pathsend)
        await FleetCompressionMiddleware(app)({"type":"http","method":"GET","path":"/fleet",
            "headers":[(b"accept-encoding",b"gzip")]},None,self.sender(sent))
        self.assertEqual(sent,[start,pathsend])

    async def test_non_http_scope_passes_through(self):
        seen=[]
        async def app(scope,receive,send): seen.append(scope)
        scope={"type":"websocket","path":"/fleet"}
        await FleetCompressionMiddleware(app)(scope,None,None)
        self.assertEqual(seen,[scope])

    async def test_large_buffer_yields_to_ready_work_before_sending(self):
        progressed=False
        body=b"full cached metadata "*30000
        async def ready():
            nonlocal progressed
            progressed=True
        async def app(scope,receive,send):
            await send({"type":"http.response.start","status":200,
                        "headers":[(b"content-type",b"application/json")]})
            await send({"type":"http.response.body","body":body})
        sent=[]
        async def send(message):
            self.assertTrue(progressed)
            sent.append(message)
        task=asyncio.create_task(ready())
        try:
            await FleetCompressionMiddleware(app)({"type":"http","method":"GET","path":"/fleet",
                "headers":[(b"accept-encoding",b"gzip")]},None,send)
            self.assertEqual(gzip.decompress(sent[1]["body"]),body)
        finally:
            await task

    async def test_cancellation_does_not_emit_partial_encoded_response(self):
        sliced=asyncio.Event()
        resume=asyncio.Event()
        async def pause(_):
            sliced.set()
            await resume.wait()
        sent=[]
        async def app(scope,receive,send):
            await send({"type":"http.response.start","status":200,
                        "headers":[(b"content-type",b"application/json")]})
            await send({"type":"http.response.body","body":b"metadata "*100000})
        with patch("pa.server.fleet_compression.asyncio.sleep",side_effect=pause):
            task=asyncio.create_task(FleetCompressionMiddleware(app)({"type":"http","method":"GET",
                "path":"/fleet","headers":[(b"accept-encoding",b"gzip")]},None,self.sender(sent)))
            try:
                await asyncio.wait_for(sliced.wait(),2)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertEqual(sent,[])
            finally:
                task.cancel()
                await asyncio.gather(task,return_exceptions=True)

    async def test_kernel_installs_encoding_inside_request_byte_accounting(self):
        import httpx
        from fastapi.responses import Response
        from pa.config import Settings
        from pa.core.kernel import Kernel
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            kernel=Kernel.boot(settings=Settings(data_dir=root/'data',workspace_root=root/'workspaces',
                auth_required=False,agent_enabled=False),load_modules=False)
            app=kernel.build_app()
            body=b"<html>"+b"full cached inspection data "*1000+b"</html>"
            @app.get('/fleet')
            async def fleet():
                return Response(body,media_type='text/html')
            try:
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://fixture') as client:
                    result=await client.get('/fleet',headers={'Accept-Encoding':'gzip'})
                self.assertEqual(result.content,body)
                self.assertEqual(result.headers['content-encoding'],'gzip')
                encoded=int(result.headers['content-length'])
                self.assertLess(encoded,len(body))
                metric=kernel.ctx.services['async_runtime'].snapshot()['requests']['routes']['/fleet']
                self.assertEqual(metric['response_bytes'],encoded)
            finally:
                await kernel.ctx.services['async_runtime'].close()
