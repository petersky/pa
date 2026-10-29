"""Compare complete display-label responses with exact pre-change source."""
import argparse
import ast
import hashlib
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

BASELINE = "f103e237bd17cec59a29ef0c2083106c1af1177d"


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--origins", type=int, default=1)
    p.add_argument("--members", type=int, default=100)
    a = p.parse_args()
    assert 1 <= a.origins <= a.members <= 500 and not a.output.exists()
    with tempfile.TemporaryDirectory(prefix="pa-session-origin-names-") as tmp:
        root = Path(tmp)
        os.environ.update(PA_DATA_DIR=str(root / "data"), PA_WORKSPACE_ROOT=str(root / "workspaces"), PA_AGENT_ENABLED="false")
        from pa.config import Settings
        from pa.core.sqlite_metrics import sqlite_connection_scope
        from pa.domain.models import AgentSession, FleetInstance
        from pa.domain.projection import CardProjection
        from pa.fleet.registry import FleetRegistry
        from pa.modules import agent_chat as module

        store = CardProjection(root / "fixture.db")
        fleet = FleetRegistry(root, "fixture")
        for i in range(a.members):
            fleet._instances[f"origin-{i}"] = FleetInstance(instance_id=f"origin-{i}", name=f"Member {i}", url="")
        fleet._save_instances()
        now = datetime.now(UTC)
        for i in range(250):
            store.save_session(AgentSession(id=f"s-{i:04}", agent_name="fixture", status="idle",
                origin_instance_id=f"origin-{i % a.origins}", origin_instance_name="Snapshot", created_at=now, updated_at=now))
        manager = SimpleNamespace(store=store, list_runtimes=lambda: [], get=lambda _: None, startup_complete=True, quiescing=False)
        settings = Settings(data_dir=root / "data", workspace_root=root / "workspaces", agent_enabled=False, instance_id="fixture", peers=[])
        ctx = SimpleNamespace(store=store, settings=settings, services={"fleet_registry": fleet}, require_service=lambda _: manager)
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)))
        source = subprocess.check_output(["git", "show", BASELINE + ":src/pa/modules/agent_chat.py"], text=True)
        namespace = dict(vars(module))
        tree = ast.parse(source)
        for name in ["_session_list_item", "list_agent_sessions"]:
            node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
            exec(compile(ast.get_source_segment(source, node), "<exact-origin-baseline>", "exec"), namespace)
        present = module.build_session_presentation

        def presentation(*args, **kwargs):
            return present(*args, **kwargs, now=now)

        namespace["build_session_presentation"] = presentation
        calls = {"baseline": lambda: namespace["list_agent_sessions"](request, view="all"),
                 "candidate": lambda: module.list_agent_sessions(request, view="all")}
        samples, counts, expected = [], {}, None
        with patch.object(module, "build_session_presentation", side_effect=presentation):
            for pair in range(5):
                for variant in (["baseline", "candidate"] if pair % 2 == 0 else ["candidate", "baseline"]):
                    with sqlite_connection_scope():
                        start, cpu = time.perf_counter_ns(), time.thread_time_ns()
                        result = calls[variant]()
                        samples.append(dict(pair=pair, variant=variant, wall_ms=(time.perf_counter_ns()-start)/1e6,
                                            thread_cpu_ms=(time.thread_time_ns()-cpu)/1e6))
                    body = json.dumps(result, default=str, sort_keys=True).encode()
                    if expected is None:
                        expected = body
                    assert body == expected
            codes = {getattr(FleetRegistry, n).__code__: n for n in ["list_instances", "_instances_disk_key"]}
            for variant, run in calls.items():
                counter = {name: 0 for name in codes.values()}

                def profile(frame, event, arg):
                    if event == "call" and frame.f_code in codes:
                        counter[codes[frame.f_code]] += 1

                previous = sys.getprofile()
                sys.setprofile(profile)
                try:
                    with sqlite_connection_scope():
                        result = run()
                finally:
                    sys.setprofile(previous)
                assert json.dumps(result, default=str, sort_keys=True).encode() == expected
                counts[variant] = counter
        summary = {}
        for variant in calls:
            values = sorted(s["wall_ms"] for s in samples if s["variant"] == variant)
            summary[variant] = dict(samples=len(values), p50=statistics.median(values), p95=values[-1], p99=values[-1], max=values[-1])
        store.close_thread_connection()
        value = dict(contract="pa.session-origin-name-comparison/v1", baseline_source=BASELINE,
            source_sha256=hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest(), sessions=250, origins=a.origins,
            fleet_members=a.members, samples=samples, latency_ms=summary, untimed_amplification=counts,
            complete_response_bytes_equal=True, response_bytes=len(expected), response_sha256=hashlib.sha256(expected).hexdigest(),
            scope="Five alternating complete list calls; both roots private before PA imports. Timers exclude serialization/profiling/byte checks; normal GC/host load unchanged. Same response bodies, first native display-name read per origin/snapshot in one response; next request fresh. Custom readers remain per row; authority paths unchanged. No production workload, physical-I/O or historical attribution, installed/full HTTP/mixed/whole-card acceptance.")
    a.output.write_text(json.dumps(value, indent=2)+"\n")
    print(json.dumps({k: value[k] for k in ["latency_ms", "untimed_amplification", "response_bytes"]}))


if __name__ == "__main__":
    main()
