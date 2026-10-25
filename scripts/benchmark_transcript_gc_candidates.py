"""Compare GC row materialization in disposable storage, never live PA data.

The baseline is one method from the specified immutable repository revision;
the candidate is the imported source. Every other fixture/input is shared.
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


def run(output: Path, *, baseline: str, count: int, pairs: int) -> dict:
    assert re.fullmatch(r"[0-9a-f]{40}", baseline)
    assert 1 <= count <= 15000 and 1 <= pairs <= 20
    assert not output.exists(), "Preserve earlier measurements"
    with tempfile.TemporaryDirectory(prefix="pa-gc-candidates-") as temporary:
        root = Path(temporary)
        # Set both defaults before PA imports, in addition to explicit fixture paths.
        os.environ.update(PA_DATA_DIR=str(root / "data"),
                          PA_WORKSPACE_ROOT=str(root / "workspaces"), PA_AGENT_ENABLED="false")
        from pa.core.async_runtime import AsyncRuntime
        from pa.core.performance import capture_retention_phases, distribution, retention_phase
        from pa.domain.models import TranscriptEvent
        import pa.domain.transcript_storage as module

        source = subprocess.check_output([
            "git", "show", baseline + ":src/pa/domain/transcript_storage.py",
        ], text=True)
        cls = next(n for n in ast.parse(source).body
                   if isinstance(n, ast.ClassDef) and n.name == "TranscriptStorage")
        method = next(n for n in cls.body if isinstance(n, ast.FunctionDef)
                      and n.name == "collect_unreferenced_objects")
        baseline_code = textwrap.dedent("\n".join(source.splitlines()[method.lineno - 1:method.end_lineno]))
        namespace = vars(module).copy()
        exec(compile(baseline_code, "<archived-gc-baseline>", "exec"), namespace)
        functions = {"baseline": namespace["collect_unreferenced_objects"],
                     "candidate": module.TranscriptStorage.collect_unreferenced_objects}
        storage = module.TranscriptStorage(root / "fixture.db")
        for start in range(0, count, 256):
            storage.append([
                TranscriptEvent(session_id="retained", seq=i + 1, event_type="chunk",
                                payload={"text": f"{i}:" + "retained cold fixture " * 300})
                for i in range(start, min(count, start + 256))
            ])

        def contents():
            return {str(p.relative_to(storage.objects_dir)): hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in storage.objects_dir.glob("*/*.zlib")}

        before = contents()
        assert len(before) == count
        samples = []

        async def compare():
            runtime = AsyncRuntime(max_workers=1)
            try:
                for pair in range(pairs):
                    for label in (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")):
                        def measured():
                            phases = {}
                            wall, cpu = time.perf_counter_ns(), time.thread_time_ns()
                            offset = time.time() - time.monotonic()
                            with capture_retention_phases(phases), retention_phase("retention_object_collection"):
                                removed = functions[label](storage)
                            return {"stage": label, "pair": pair, "removed": removed,
                                    "wall_ms": (time.perf_counter_ns() - wall) / 1e6,
                                    "thread_cpu_ms": (time.thread_time_ns() - cpu) / 1e6,
                                    "wall_minus_monotonic_before": offset,
                                    "wall_minus_monotonic_after": time.time() - time.monotonic(),
                                    "phase": phases["retention_object_collection"]}

                        sample = await runtime.run_blocking("benchmark.gc_candidates." + label,
                                                            measured, wait_for_completion=True)
                        assert sample["removed"] == 0
                        samples.append(sample)
                return runtime.snapshot()
            finally:
                await runtime.close()

        runtime = asyncio.run(compare())
        assert contents() == before
        value = {"contract": "pa.transcript-gc-candidate-comparison/v1",
                 "observed_at": datetime.now(UTC).isoformat(), "baseline": baseline,
                 "source_sha256": hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest(),
                 "baseline_method_sha256": hashlib.sha256(baseline_code.encode()).hexdigest(),
                 "candidate_method_sha256": hashlib.sha256(inspect.getsource(functions["candidate"]).encode()).hexdigest(),
                 "retained_objects": count, "pairs": pairs, "samples": samples, "runtime": runtime,
                 "cold_file_manifest_sha256": hashlib.sha256(json.dumps(before, sort_keys=True).encode()).hexdigest(),
                 "all_file_bytes_unchanged": True,
                 "summary": {label: {
                     "wall_ms": distribution([r["wall_ms"] for r in samples if r["stage"] == label]),
                     "thread_cpu_ms": distribution([r["thread_cpu_ms"] for r in samples if r["stage"] == label]),
                     "sqlite_rows": [r["phase"]["sqlite_rows"] for r in samples if r["stage"] == label],
                     "sqlite_calls": [r["phase"]["sqlite_calls"] for r in samples if r["stage"] == label],
                 } for label in functions},
                 "scope": "Alternating same-input all-live collector sweeps, one worker, private roots. "
                          "No production state, external provider/peer, source edits, install, restart or faults. "
                          "Validation is outside timers. Clock offsets retained; physical IO/busy/scheduling "
                          "are not independently separated. This is not mixed-load or deployed acceptance."}
    output.write_text(json.dumps(value, indent=2) + "\n")
    return value


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline", default="87e49ca8e256d4ef3641e10b5b0a2de06fb3a17f")
    parser.add_argument("--objects", type=int, default=12511)
    parser.add_argument("--pairs", type=int, default=5)
    args = parser.parse_args()
    value = run(args.output, baseline=args.baseline, count=args.objects, pairs=args.pairs)
    print(json.dumps({"output": str(args.output), "summary": value["summary"]}))
