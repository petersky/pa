"""Compare complete session lists with exact pre-change source in private roots."""
import argparse
import ast
import hashlib
import json
import os
import statistics
import subprocess
import sys
import tempfile
import textwrap
import time
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


BASELINE = "aa14cd1ede6115940b8a25402017804d49ea4d4f"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sessions", type=int, default=250)
    parser.add_argument("--projects", type=int, default=8)
    args = parser.parse_args()
    assert 1 <= args.projects <= args.sessions <= 500 and not args.output.exists()
    with tempfile.TemporaryDirectory(prefix="pa-session-project-labels-") as tmp:
        root = Path(tmp)
        os.environ.update(PA_DATA_DIR=str(root / "data"),
                          PA_WORKSPACE_ROOT=str(root / "workspaces"), PA_AGENT_ENABLED="false")
        from pa.config import Settings
        from pa.core.sqlite_metrics import sqlite_connection_scope
        from pa.domain.models import AgentSession, Project
        from pa.domain.projection import CardProjection
        from pa.modules import agent_chat as module

        store = CardProjection(root / "fixture.db")
        for i in range(args.projects):
            store._upsert_project(Project(id=f"p-{i}", title=f"Project {i}",
                                          description="x" * 65536,
                                          tool_config={"fixture": "y" * 65536}))
        now = datetime.now(UTC)
        for i in range(args.sessions):
            store.save_session(AgentSession(id=f"s-{i:04}", agent_name="fixture", status="idle",
                                            project_id=f"p-{i % args.projects}", created_at=now, updated_at=now))
        manager = SimpleNamespace(store=store, list_runtimes=lambda: [], get=lambda _: None,
                                  startup_complete=True, quiescing=False)
        settings = Settings(data_dir=root / "data", workspace_root=root / "workspaces",
                            agent_enabled=False, instance_id="fixture", peers=[])
        ctx = SimpleNamespace(store=store, settings=settings, services={}, require_service=lambda _: manager)
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)))
        source = subprocess.check_output(["git", "show", BASELINE + ":src/pa/modules/agent_chat.py"], text=True)
        namespace = dict(vars(module))
        tree = ast.parse(source)
        for name in ["_session_list_item", "list_agent_sessions"]:
            node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
            exec(compile(textwrap.dedent(ast.get_source_segment(source, node)), "<exact-label-baseline>", "exec"), namespace)
        present = module.build_session_presentation

        def presentation(*args, **kwargs):
            # The same explicit observation instant keeps age fields comparable;
            # wall/CPU timers and normal GC are unchanged.
            return present(*args, **kwargs, now=now)

        namespace["build_session_presentation"] = presentation
        calls = {"baseline": lambda: namespace["list_agent_sessions"](request, view="all", limit=args.sessions),
                 "candidate": lambda: module.list_agent_sessions(request, view="all", limit=args.sessions)}
        samples, counts = [], {}
        expected = None
        with patch.object(module, "build_session_presentation", side_effect=presentation):
            for pair in range(5):
                for variant in (["baseline", "candidate"] if pair % 2 == 0 else ["candidate", "baseline"]):
                    with sqlite_connection_scope():
                        start, cpu = time.perf_counter_ns(), time.thread_time_ns()
                        result = calls[variant]()
                        samples.append(dict(pair=pair, variant=variant,
                                            wall_ms=(time.perf_counter_ns() - start) / 1e6,
                                            thread_cpu_ms=(time.thread_time_ns() - cpu) / 1e6))
                    body = json.dumps(result, default=str, sort_keys=True).encode()
                    if expected is None:
                        expected = body
                    assert body == expected
            code = CardProjection._row_to_project.__code__
            with store._conn() as conn:
                connection = conn
            for variant, run in calls.items():
                counter = dict(project_models=0, materialized_project_text_bytes=0)
                statements = []

                def profile(frame, event, arg):
                    if event == "call" and frame.f_code is code:
                        row = frame.f_locals["row"]
                        counter["project_models"] += 1
                        counter["materialized_project_text_bytes"] += sum(
                            len(row[k].encode()) for k in ["description", "tool_config", "agent_prompt", "memberships", "repos", "tags"]
                        )

                previous = sys.getprofile()
                connection.set_trace_callback(statements.append)
                sys.setprofile(profile)
                try:
                    with sqlite_connection_scope():
                        result = run()
                finally:
                    sys.setprofile(previous)
                    connection.set_trace_callback(None)
                assert json.dumps(result, default=str, sort_keys=True).encode() == expected
                counter["project_selects"] = sum(s.startswith("SELECT * FROM projects WHERE id") for s in statements)
                counter["sqlite_statements"] = len(statements)
                counts[variant] = counter
        summary = {}
        for variant in calls:
            values = sorted(s["wall_ms"] for s in samples if s["variant"] == variant)
            summary[variant] = dict(samples=len(values), p50=statistics.median(values),
                                    p95=values[-1], p99=values[-1], max=values[-1])
        store.close_thread_connection()
        value = dict(contract="pa.session-project-label-comparison/v1", baseline_source=BASELINE,
                     source_sha256=hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest(),
                     sessions=args.sessions, projects=args.projects, project_fixture_text_bytes=131072,
                     samples=samples, latency_ms=summary, untimed_amplification=counts,
                     complete_response_bytes_equal=True, response_bytes=len(expected),
                     response_sha256=hashlib.sha256(expected).hexdigest(),
                     scope="Five alternating complete list calls. Both roots private before imports; no production state or provider. All comparisons preserve the same response body. Timers exclude serialization, profiling, SQL tracing and byte checks; normal GC and uncontrolled host load remain. Untimed profiling counts native full Project hydration without replacing the getter/converter or bypassing its native guard. First-read display-label consistency is per project per invocation; custom readers retain per-row behavior. Not installed, full HTTP/mixed-load, physical-write or historical-cause acceptance.")
    args.output.write_text(json.dumps(value, indent=2) + "\n")
    print(json.dumps({k: value[k] for k in ["latency_ms", "untimed_amplification", "response_bytes"]}))


if __name__ == "__main__":
    main()
