"""Measure durable human enqueue writes using disposable state and no provider.

Run the same script with PYTHONPATH pointing to another source for comparison.
This paused-queue scenario does not accept provider execution or mixed-load tails.
"""
import argparse
import asyncio
import hashlib
import json
import math
import tempfile
import time
from pathlib import Path

from pa.config import Settings
from pa.core.kernel import Kernel
from pa.domain.models import AgentSession
from pa.instance import agent_session
from pa.instance.agent_session import AgentSessionRuntime


def distribution(values):
    values = sorted(values)
    return {"samples": len(values), "max_ms": max(values),
            **{f"p{p}_ms": values[math.ceil(len(values) * p / 100) - 1]
               for p in (50, 95, 99)}}


async def run(root):
    settings = Settings(data_dir=root / "data", workspace_root=root / "workspaces",
                        agent_enabled=False, telemetry_enabled=False, auth_required=False)
    app = Kernel.boot(settings=settings).build_app()
    async with app.router.lifespan_context(app):
        ctx = app.state.ctx
        manager = ctx.require_service("instance_agent")
        session = AgentSession(id="normal-enqueue", agent_name="codex", purpose="chat", status="idle")
        await asyncio.to_thread(ctx.store.save_session, session)
        worker = AgentSessionRuntime(manager, session, initial_transcript_seq=0)
        worker._queue_paused = True
        await asyncio.sleep(0.1)
        lag, latency, checkpoints = [], [], []
        done = False
        original = worker._checkpoint_runtime

        def checkpoint(**kwargs):
            started = time.perf_counter()
            try:
                return original(**kwargs)
            finally:
                checkpoints.append((time.perf_counter() - started) * 1000)

        worker._checkpoint_runtime = checkpoint

        async def ticker():
            while not done:
                started = time.perf_counter()
                await asyncio.sleep(0.01)
                lag.append(max(0, (time.perf_counter() - started - 0.01) * 1000))

        task = asyncio.create_task(ticker())
        await asyncio.sleep(0.02)
        for index in range(100):
            started = time.perf_counter()
            worker.enqueue("fixture " * 100, prompt_id=f"normal-{index}", source="ui")
            latency.append((time.perf_counter() - started) * 1000)
            await asyncio.sleep(0.005)
        await worker._drain_transcripts(raise_on_timeout=True)
        done = True
        await task
        persisted = await asyncio.to_thread(ctx.store.get_session, session.id)
        queued = persisted.config_json["durable_runtime"]["queued_prompts"]
        assert [item["id"] for item in queued] == [f"normal-{index}" for index in range(100)]
        assert persisted.config_json["durable_runtime"]["last_event_cursor"] == 100
        return {
            "scope": "100 paused human enqueues; real durable Store/transcript; no provider/peers",
            "agent_session_source_sha256": hashlib.sha256(Path(agent_session.__file__).read_bytes()).hexdigest(),
            "latency": distribution(latency), "checkpoint": distribution(checkpoints),
            "loop_lag": distribution(lag), "checkpoints": len(checkpoints), "durable_queued": len(queued),
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="pa-normal-enqueue-") as tmp:
        result = asyncio.run(run(Path(tmp)))
    args.output.write_text(json.dumps(result, indent=2) + "\n")
