"""Exact baseline/candidate full session watch reads in private roots."""
import argparse, ast, hashlib, json, os, statistics, subprocess, tempfile, time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--all-related", action="store_true")
    a = p.parse_args()
    assert not a.output.exists()
    baseline = "d783f8f78caff10f85811dab4b713493030fda98"
    with tempfile.TemporaryDirectory(prefix="pa-full-session-watches-") as tmp:
        root = Path(tmp)
        os.environ.update(PA_DATA_DIR=str(root / "data"), PA_WORKSPACE_ROOT=str(root / "workspaces"), PA_AGENT_ENABLED="false")
        from pa.modules import agent_chat as module
        from pa.pr_supervisor.models import PRWatch
        from pa.pr_supervisor.store import PRSupervisorStore
        store = PRSupervisorStore(root / "watches.db")
        for i in range(100):
            store.upsert_watch(PRWatch(id=f"watch-{i}", repository="owner/repo", pr_number=i + 1,
                pr_url=f"https://github.com/owner/repo/pull/{i+1}", originating_instance_id="fixture",
                originating_session_id="shared" if a.all_related else f"session-{i}", state={"proof": "x" * 65536}), preserve_lease=False)
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(services={"pr_supervisor_store": store}))))
        sessions = [SimpleNamespace(id="shared" if a.all_related else f"session-{i}", card_id=None) for i in range(10)]
        source = subprocess.check_output(["git", "show", baseline + ":src/pa/modules/agent_chat.py"], text=True)
        node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == "_session_pr_watches")
        namespace = dict(vars(module))
        exec(compile(ast.get_source_segment(source, node), "<exact-full-watch-baseline>", "exec"), namespace)
        variants = {"baseline": namespace["_session_pr_watches"], "candidate": module._session_pr_watches}
        expected, samples, counts = None, [], {}
        for pair in range(5):
            for name in (["baseline", "candidate"] if pair % 2 == 0 else ["candidate", "baseline"]):
                start, cpu = time.perf_counter_ns(), time.thread_time_ns()
                result = [variants[name](request, session) for session in sessions]
                samples.append(dict(pair=pair, variant=name, wall_ms=(time.perf_counter_ns()-start)/1e6,
                                    thread_cpu_ms=(time.thread_time_ns()-cpu)/1e6))
                body = json.dumps(result, sort_keys=True).encode()
                if expected is None: expected = body
                assert body == expected
        # Profile conversion without overriding the reader or its native guard.
        import sys
        native_code = PRSupervisorStore._row_to_watch.__code__
        for name, call in variants.items():
            count = {"models": 0, "state_json_bytes": 0}
            def profile(frame, event, arg):
                if event == "call" and frame.f_code is native_code:
                    count["models"] += 1
                    count["state_json_bytes"] += len(frame.f_locals["row"]["state_json"].encode())
            previous = sys.getprofile()
            sys.setprofile(profile)
            try: result = [call(request, session) for session in sessions]
            finally: sys.setprofile(previous)
            assert json.dumps(result, sort_keys=True).encode() == expected
            counts[name] = count
        latencies = {}
        for name in variants:
            xs = sorted(s["wall_ms"] for s in samples if s["variant"] == name)
            latencies[name] = dict(samples=5, p50=statistics.median(xs), p95=xs[-1], p99=xs[-1], max=xs[-1])
        value = dict(contract="pa.full-session-watch-comparison/v1", baseline_source=baseline,
            source_hashes={m.__name__: hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest() for m in [module, __import__("pa.pr_supervisor.store", fromlist=["store"])]},
            watches=100, sessions=10, all_related=a.all_related, proof_bytes_each=65536, samples=samples, latency_ms=latencies,
            untimed_amplification=counts, complete_response_bytes_equal=True, response_bytes=len(expected),
            scope="Five alternating private batches of ten full session watch reads. Exact original helper baseline; complete response bytes match. Timers exclude serialization/profile/checks; normal GC unchanged. No production state, network, real provider or historical/physical-I/O claim. Returned matching proof bytes deliberately remain complete.")
    a.output.write_text(json.dumps(value, indent=2)+"\n")
    print(json.dumps({k: value[k] for k in ["latency_ms", "untimed_amplification", "response_bytes"]}))


if __name__ == "__main__": main()
