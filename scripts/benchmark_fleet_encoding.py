"""Measure Fleet transport encoding with synthetic data and private roots.

Run with ``uv run python scripts/benchmark_fleet_encoding.py --output NEW_PATH``.
This measures ASGI body bytes and middleware CPU, not live network throughput.
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import html
import json
import math
import os
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path


def distribution(values):
    ordered = sorted(values)
    return {"samples": len(ordered), **{
        name: ordered[math.ceil(q * len(ordered)) - 1]
        for name, q in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))
    }}


async def measure():
    from pa.server import fleet_compression

    payload = json.dumps({"repositories": [
        {"id": f"synthetic-{i}", "name": f"Repository {i}",
         "workspaces": [{"branch": f"card/{i}/{j}", "state": "ready",
                         "lease": "synthetic-only", "detail": "retained inspection metadata" * 8}
                        for j in range(4)]}
        for i in range(600)
    ]}, separators=(",", ":")).encode()
    results = {}
    for path, body, content_type in (
        ("/api/fleet/overview", payload, b"application/json"),
        ("/fleet", b"<html><pre>" + html.escape(payload.decode()).encode() + b"</pre></html>", b"text/html"),
    ):
        assert 4096 <= len(body) <= 4 * 1024 * 1024
        async def app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200,
                        "headers": [(b"content-type", content_type), (b"content-length", str(len(body)).encode())]})
            await send({"type": "http.response.body", "body": body})
        wrapped = fleet_compression.FleetCompressionMiddleware(app)
        samples = []
        for pair in range(20):
            for encoding in (("identity", "gzip") if pair % 2 == 0 else ("gzip", "identity")):
                sent = []
                async def send(message):
                    sent.append(message)
                started = time.perf_counter_ns()
                cpu = time.thread_time_ns()
                await wrapped({"type": "http", "method": "GET", "path": path,
                               "headers": [(b"accept-encoding", encoding.encode())]}, None, send)
                elapsed = (time.perf_counter_ns() - started) / 1e6
                cpu_ms = (time.thread_time_ns() - cpu) / 1e6
                encoded = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
                headers = dict(sent[0]["headers"])
                # Identity and exact decoding checks are outside the measured interval.
                assert int(headers[b"content-length"]) == len(encoded)
                assert (gzip.decompress(encoded) if encoding == "gzip" else encoded) == body
                samples.append(dict(pair=pair, accept_encoding=encoding, elapsed_ms=elapsed,
                                    thread_cpu_ms=cpu_ms, encoded_body_bytes=len(encoded),
                                    content_encoding=headers.get(b"content-encoding", b"identity").decode()))
        results[path] = dict(decoded_body_bytes=len(body), samples=samples,
            by_encoding={encoding: dict(
                elapsed_ms=distribution([s["elapsed_ms"] for s in samples if s["accept_encoding"] == encoding]),
                thread_cpu_ms=distribution([s["thread_cpu_ms"] for s in samples if s["accept_encoding"] == encoding]),
                encoded_body_bytes=sorted({s["encoded_body_bytes"] for s in samples if s["accept_encoding"] == encoding}))
                for encoding in ("identity", "gzip")})
    return dict(results=results, module_sha256=hashlib.sha256(Path(fleet_compression.__file__).read_bytes()).hexdigest())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists(), "Preserve prior results; choose a new output path"
    started = datetime.now(UTC).isoformat()
    with tempfile.TemporaryDirectory(prefix="pa-fleet-encoding-") as tmp:
        # These must be private before any PA import, even though this helper
        # never constructs a Store or calls a running service.
        os.environ.update(PA_DATA_DIR=str(Path(tmp) / "data"),
                          PA_WORKSPACE_ROOT=str(Path(tmp) / "workspaces"), PA_AGENT_ENABLED="false")
        result = asyncio.run(measure())
    result.update(contract="pa.fleet-encoding-benchmark/v1", started_at=started,
                  finished_at=datetime.now(UTC).isoformat(),
                  scope="Synthetic complete buffered responses, 20 alternating pairs per path. Exact decoded equality; no live data, service request, network throughput, provider, deployment or whole-path acceptance claim.")
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps({"output": str(args.output), "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest()}))


if __name__ == "__main__":
    main()
