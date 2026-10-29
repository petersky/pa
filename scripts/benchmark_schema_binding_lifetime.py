"""Compare complete private PA schemas and temporary binding retention.

Native GC runs unchanged. No forced collection, policy changes, listener,
provider, network or production state. This does not reproduce a Linux CI pause.
"""
import argparse
import ast
import gc
import hashlib
import json
import os
import subprocess
import tempfile
import threading
import time
import weakref
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

BASELINE = "8e1b0cd3b46318e108153de2d82cd6ab275992e0"
DISCOVERY = {"get_flat_models_from_model", "get_flat_models_from_annotation",
             "get_flat_models_from_field", "get_flat_models_from_fields"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists()
    with tempfile.TemporaryDirectory(prefix="pa-schema-binding-benchmark-") as tmp:
        os.environ.update(PA_DATA_DIR=str(Path(tmp)/"data"),
                          PA_WORKSPACE_ROOT=str(Path(tmp)/"workspaces"), PA_AGENT_ENABLED="false")
        from pa.config import Settings
        from pa.core.kernel import Kernel
        from pa.server import schema_generation as sg

        source = subprocess.check_output(["git", "show", BASELINE + ":src/pa/server/schema_generation.py"], text=True)
        namespace = dict(vars(sg))
        node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == "get_openapi")
        exec(compile(ast.get_source_segment(source, node), "<exact-schema-lifetime-baseline>", "exec"), namespace)
        app = Kernel.boot(settings=Settings(data_dir=Path(tmp)/"data", workspace_root=Path(tmp)/"workspaces",
                                            agent_enabled=False, telemetry_enabled=False, auth_required=False, peers=[])).build_app()
        options = dict(title="Complete PA lifetime fixture", version="1", routes=app.routes,
                       webhooks=app.webhooks.routes)
        result = dict(baseline=BASELINE, source_sha256=hashlib.sha256(Path(sg.__file__).read_bytes()).hexdigest(),
                      started_at=datetime.now(UTC).isoformat(), rows=[], scope=__doc__,
                      native_thresholds=gc.get_threshold(), private_roots_before_imports=True)
        args.output.write_text(json.dumps(result, indent=2)+"\n")
        original_bind = sg._bind
        expected = None
        for pair in range(3):
            for variant in (["baseline", "candidate"] if pair % 2 == 0 else ["candidate", "baseline"]):
                refs, pauses, pending = [], [], {}
                def tracked(function, globals_dict):
                    bound = original_bind(function, globals_dict)
                    if function.__name__ in DISCOVERY:
                        refs.append(weakref.ref(bound))
                    return bound
                def observe(phase, info):
                    key = (threading.get_ident(), info["generation"])
                    if phase == "start":
                        pending[key] = (time.perf_counter(), time.thread_time())
                    elif key in pending:
                        start, cpu = pending.pop(key)
                        pauses.append(dict(generation=key[1], wall_ms=(time.perf_counter()-start)*1000,
                                           thread_cpu_ms=(time.thread_time()-cpu)*1000,
                                           collected=info["collected"], uncollectable=info["uncollectable"]))
                namespace["_bind"] = tracked
                gc.callbacks.append(observe)
                start, cpu = time.perf_counter(), time.thread_time()
                try:
                    with patch.object(sg, "_bind", tracked):
                        value = (namespace["get_openapi"] if variant == "baseline" else sg.get_openapi)(**options)
                    wall_ms = (time.perf_counter()-start)*1000
                    cpu_ms = (time.thread_time()-cpu)*1000
                    live = sum(ref() is not None for ref in refs)
                finally:
                    gc.callbacks.remove(observe)
                assert len(refs) == 4
                assert live == (4 if variant == "baseline" else 0)
                canonical = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
                if expected is None:
                    expected = canonical
                assert canonical == expected, "Complete schema output changed"
                assert gc.get_threshold() == tuple(result["native_thresholds"])
                result["rows"].append(dict(pair=pair, variant=variant, schema_wall_ms=wall_ms,
                    schema_thread_cpu_ms=cpu_ms, temporary_discovery_functions=4, retained_after_return=live,
                    schema_bytes=len(canonical), schema_sha256=hashlib.sha256(canonical).hexdigest(),
                    paths=len(value["paths"]), natural_gc_pauses=pauses))
                args.output.write_text(json.dumps(result, indent=2)+"\n")
        result.update(finished_at=datetime.now(UTC).isoformat(), complete_schema_bytes_equal=True,
                      gc_policy_unchanged=True,
                      limits="Temporary discovery functions release4to0 without explicit collection. Other Pydantic/FastAPI cycles may remain. Native Darwin3.14.2 fixture is not Linux3.14.10 CI or live activation; no whole-card or CI-failure resolution claim.")
        args.output.write_text(json.dumps(result, indent=2)+"\n")
        print(json.dumps(dict(rows=len(result["rows"]), complete_schema_bytes_equal=True,
                             retained_after_return=[r["retained_after_return"] for r in result["rows"]])))


if __name__ == "__main__":
    main()
