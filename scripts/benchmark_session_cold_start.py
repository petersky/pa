"""Cold PA admission and ACP initialization with tool-free fixture subprocesses.

No installed PA state, model account, external endpoint, or real agent is used.
"""
import argparse
import asyncio
import json
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

from pa.acp.configuration import SessionConfigurationRequest
from pa.acp.providers import registry
from pa.acp.startup_trace import SessionStartupTrace
from pa.config import Settings
from pa.core.async_runtime import AsyncRuntime
from pa.core.performance import distribution
from pa.domain.projection import CardProjection
from pa.instance.agent_session import AgentSessionManager

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tests.selection_browser_app import FixtureProvider


async def run(root, samples):
    settings = Settings(data_dir=root / "data", workspace_root=root / "workspaces", agent_provider="codex",
                        agent_enabled=True, agent_model="fixture-balanced", telemetry_enabled=False)
    settings.data_dir.mkdir()
    store = CardProjection(settings.db_path)
    executor = AsyncRuntime()
    manager = AgentSessionManager(settings, store)
    manager.async_runtime = executor
    timings, phases = [], []
    try:
        with patch.dict(registry._PROVIDERS, {h: FixtureProvider(h) for h in ("codex", "cursor", "openinterpreter")}, clear=True), patch(
            "pa.acp.client.pa_mcp_servers", return_value=[]
        ):
            for i in range(samples):
                trace = SessionStartupTrace()
                start = time.perf_counter()
                runtime = await manager.create_session(label=f"cold-{i}", principal_id="user:fixture", provider_override="codex",
                    initial_configuration=SessionConfigurationRequest(model_id="fixture-balanced", mode_id="workspace-write", reasoning="low"),
                    purpose="chat", startup_trace=trace)
                assert runtime.connected
                await runtime._drain_transcripts(raise_on_timeout=True)
                trace.mark("response_readiness")
                timings.append((time.perf_counter() - start) * 1000)
                phases.append((runtime.session.config_json.get("startup_trace") or {}).get("phases", []))
                assert store.get_session(runtime.session_id).execution_binding
                await runtime.close(reason="fixture_complete", reconcile_workspace=False)
        return {"fixture": "tool-free ACP process, real admission/configuration/workspace/binding/transcript paths",
                "latency_ms": distribution(timings), "first_start_ms": round(timings[0], 3),
                "subsequent_ms": distribution(timings[1:]), "samples_ms": timings,
                "phases": phases, "runtime": executor.snapshot()}
    finally:
        await manager.stop()
        await executor.close()
        store.close_thread_connection()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=10)
    args = parser.parse_args()
    if args.samples < 1:
        parser.error("samples must be positive")
    with tempfile.TemporaryDirectory(prefix="pa-session-cold-") as tmp:
        result = asyncio.run(run(Path(tmp), args.samples))
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: result[key] for key in ("latency_ms", "first_start_ms", "subsequent_ms")}, indent=2))
