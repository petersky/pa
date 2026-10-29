"""Complete private PA schemas with native generator lifetime observations.

Compare exact archived source with candidate using three alternating pairs.
Both PA roots are private before imports; no listener/provider/network/service
state, forced collection, GC policy or deadline changes. Observer cost included.
"""
import argparse
import gc
import hashlib
import json
import os
import subprocess
import tempfile
import time
import weakref
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

BASELINE = "f46364419707e63cb36561fddf86c7d81280f7fe"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists()
    with tempfile.TemporaryDirectory(prefix="pa-generator-lifetime-benchmark-") as tmp:
        os.environ.update(PA_DATA_DIR=str(Path(tmp)/"data"),
                          PA_WORKSPACE_ROOT=str(Path(tmp)/"workspaces"), PA_AGENT_ENABLED="false")
        from pydantic import json_schema
        from pa.config import Settings
        from pa.core.kernel import Kernel
        from pa.server import schema_generation as sg

        source = subprocess.check_output(["git", "show", BASELINE+":src/pa/server/schema_generation.py"], text=True)
        baseline = dict(vars(sg))
        exec(compile(source, "<exact-generator-lifetime-baseline>", "exec"), baseline)
        app = Kernel.boot(settings=Settings(data_dir=Path(tmp)/"data", workspace_root=Path(tmp)/"workspaces",
                                            agent_enabled=False, telemetry_enabled=False, auth_required=False, peers=[])).build_app()
        options = dict(title="Complete PA generator lifetime", version="1", routes=app.routes,
                       webhooks=app.webhooks.routes)
        result = dict(baseline=BASELINE, source_sha256=hashlib.sha256(Path(sg.__file__).read_bytes()).hexdigest(),
                      started_at=datetime.now(UTC).isoformat(), native_thresholds=gc.get_threshold(), rows=[],
                      scope=__doc__, private_roots_before_imports=True)
        args.output.write_text(json.dumps(result, indent=2)+"\n")
        native_init = json_schema.GenerateJsonSchema.__init__
        expected = None
        for pair in range(3):
            for variant in (["baseline", "candidate"] if pair % 2 == 0 else ["candidate", "baseline"]):
                refs = []
                def record(self, *a, **k):
                    native_init(self, *a, **k)
                    refs.append(weakref.ref(self))
                start, cpu = time.perf_counter(), time.thread_time()
                with patch.object(json_schema.GenerateJsonSchema, "__init__", record):
                    schema = (baseline["get_openapi"] if variant == "baseline" else sg.get_openapi)(**options)
                wall_ms = (time.perf_counter()-start)*1000
                cpu_ms = (time.thread_time()-cpu)*1000
                assert len(refs) == 1
                owner = refs[0]()
                alive = owner is not None
                definitions = len(owner.definitions) if alive else 0
                method_refs = sum(getattr(m, "__self__", None) is owner for m in owner._schema_type_to_method.values()) if alive else 0
                del owner
                canonical = json.dumps(schema, sort_keys=True, separators=(",", ":")).encode()
                if expected is None:
                    expected = canonical
                assert canonical == expected, "Complete generator output changed"
                assert alive == (variant == "baseline"), "Lifetime contract failed; retain partial stage"
                assert gc.get_threshold() == tuple(result["native_thresholds"])
                result["rows"].append(dict(pair=pair, variant=variant, schema_wall_ms=wall_ms,
                    schema_thread_cpu_ms=cpu_ms, generators_created=1, alive_after_return=int(alive),
                    retained_definitions=definitions, self_bound_methods=method_refs,
                    schema_bytes=len(canonical), schema_sha256=hashlib.sha256(canonical).hexdigest(), paths=len(schema["paths"])))
                args.output.write_text(json.dumps(result, indent=2)+"\n")
        result.update(finished_at=datetime.now(UTC).isoformat(), complete_schema_bytes_equal=True, gc_policy_unchanged=True,
                      limits="Only exact version-verified Pydantic/FastAPI generator classes use the weak internal dispatch map; arbitrary custom bases/subclasses retain native dicts and may retain cycles. Escaped bound methods and public handlers retain their owner normally. Native instance after-return lifetime is measured; other framework cycles, historical CI GC allocation ownership, live activation and whole-card acceptance remain unverified.")
        args.output.write_text(json.dumps(result, indent=2)+"\n")
        print(json.dumps(dict(rows=len(result["rows"]), complete_schema_bytes_equal=True,
                             retained=[r["alive_after_return"] for r in result["rows"]],
                             definitions=[r["retained_definitions"] for r in result["rows"]])))


if __name__ == "__main__":
    main()
