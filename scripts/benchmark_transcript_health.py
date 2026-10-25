"""Compare fresh transcript health on private identical retained objects.

Only the selected archived method differs between stages. Both stages
retain the current native integrity check and read fresh event references.
"""
from __future__ import annotations

import argparse
import ast
import asyncio
import hashlib
import inspect
import json
import os
import re
import subprocess
import tempfile
import textwrap
import time
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch


def run(output: Path, *, baseline: str, count: int, pairs: int,
        method_name: str = "_missing_referenced_objects") -> dict:
    assert re.fullmatch(r"[0-9a-f]{40}", baseline)
    assert 1 <= count <= 15000 and 1 <= pairs <= 20
    assert not output.exists(), "Preserve earlier measurements"
    assert method_name in {"_missing_referenced_objects", "referenced_hashes"}
    with tempfile.TemporaryDirectory(prefix="pa-fresh-health-") as temporary:
        root = Path(temporary)
        os.environ.update(PA_DATA_DIR=str(root / "data"),
                          PA_WORKSPACE_ROOT=str(root / "workspaces"), PA_AGENT_ENABLED="false")
        from pa.core.async_runtime import AsyncRuntime
        from pa.core.performance import distribution
        from pa.domain.models import TranscriptEvent
        import pa.domain.transcript_storage as module

        source = subprocess.check_output([
            "git", "show", baseline + ":src/pa/domain/transcript_storage.py",
        ], text=True)
        cls = next(n for n in ast.parse(source).body
                   if isinstance(n, ast.ClassDef) and n.name == "TranscriptStorage")
        method = next(n for n in cls.body if isinstance(n, ast.FunctionDef)
                      and n.name == method_name)
        baseline_code = textwrap.dedent("\n".join(source.splitlines()[method.lineno - 1:method.end_lineno]))
        namespace = vars(module).copy()
        exec(compile(baseline_code, "<archived-health-baseline>", "exec"), namespace)
        functions = {"baseline": namespace[method_name],
                     "candidate": getattr(module.TranscriptStorage, method_name)}
        storage = module.TranscriptStorage(root / "fixture.db")
        for start in range(0, count, 256):
            storage.append([
                TranscriptEvent(session_id="retained", seq=i + 1, event_type="output",
                                payload={"text": f"{i}:" + "retained fixture " * 400})
                for i in range(start, min(count, start + 256))
            ])

        def contents():
            return {str(p.relative_to(storage.objects_dir)): hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in storage.objects_dir.glob("*/*.zlib")}

        before = contents()
        assert len(before) == count
        expected = storage.metrics()
        assert expected["missing_objects"] == 0 and expected["integrity"] == "ok"
        samples = []

        async def compare():
            runtime = AsyncRuntime(max_workers=1)
            try:
                for pair in range(pairs):
                    for label in (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")):
                        def measured():
                            wall, cpu = time.perf_counter_ns(), time.thread_time_ns()
                            offset = time.time() - time.monotonic()
                            with patch.object(storage, method_name, lambda: functions[label](storage)):
                                answer = storage.metrics()
                            assert answer == expected
                            return {"stage": label, "pair": pair,
                                    "wall_ms": (time.perf_counter_ns() - wall) / 1e6,
                                    "thread_cpu_ms": (time.thread_time_ns() - cpu) / 1e6,
                                    "wall_minus_monotonic_before": offset,
                                    "wall_minus_monotonic_after": time.time() - time.monotonic()}

                        samples.append(await runtime.run_blocking("benchmark.fresh_health." + label,
                                                                  measured, wait_for_completion=True))
                return runtime.snapshot()
            finally:
                await runtime.close()

        runtime = asyncio.run(compare())
        assert contents() == before
        value = {"contract": "pa.transcript-fresh-health-comparison/v1",
                 "observed_at": datetime.now(UTC).isoformat(), "baseline": baseline,
                 "compared_method": method_name,
                 "source_sha256": hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest(),
                 "baseline_method_sha256": hashlib.sha256(baseline_code.encode()).hexdigest(),
                 "candidate_method_sha256": hashlib.sha256(inspect.getsource(functions["candidate"]).encode()).hexdigest(),
                 "retained_objects": count, "pairs": pairs, "samples": samples, "runtime": runtime,
                 "cold_file_manifest_sha256": hashlib.sha256(json.dumps(before, sort_keys=True).encode()).hexdigest(),
                 "all_file_bytes_unchanged": True, "identical_fresh_health": expected,
                 "summary": {label: {key: distribution([r[key] for r in samples if r["stage"] == label])
                                     for key in ("wall_ms", "thread_cpu_ms")} for label in functions},
                 "scope": "Alternating same-input fresh-health reads, one worker, private roots. "
                          "Every native quick_check, event-reference read and filesystem diagnostic remains. "
                          "No production state/provider/peer/install/restart/fault workload. "
                          "Not mixed-load, deployed or whole-PA acceptance."}
    output.write_text(json.dumps(value, indent=2) + "\n")
    return value


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline", default="d244274ba50a5c057040471403bfbb7dedade9f6")
    parser.add_argument("--objects", type=int, default=12511)
    parser.add_argument("--pairs", type=int, default=5)
    parser.add_argument("--method", choices=["_missing_referenced_objects", "referenced_hashes"],
                        default="_missing_referenced_objects")
    args = parser.parse_args()
    value = run(args.output, baseline=args.baseline, count=args.objects, pairs=args.pairs,
                method_name=args.method)
    print(json.dumps({"output": str(args.output), "summary": value["summary"]}))
