"""Complete enrichment context parity on a private native tag catalog.

Logical hydrated body bytes are not physical disk I/O. No provider is invoked;
this compares request preparation, not deployed/historical performance.
"""
import argparse
import ast
import hashlib
import json
import os
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

BASELINE = "dd56ac85d4707631a9d4612d71659ffda55edab4"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists()
    with tempfile.TemporaryDirectory(prefix="pa-enrichment-tag-catalog-") as root:
        os.environ.update(PA_DATA_DIR=str(Path(root) / "data"), PA_WORKSPACE_ROOT=str(Path(root) / "workspaces"), PA_AGENT_ENABLED="false")
        from pa.domain import card_enrichment as module
        from pa.domain.models import CardCreate, ProjectCreate, RepositoryCreate
        from pa.domain.projection import CardProjection

        source = subprocess.check_output(["git", "show", BASELINE + ":src/pa/domain/card_enrichment.py"], text=True)
        node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == "enrichment_request")
        baseline = dict(vars(module))
        exec(compile(ast.get_source_segment(source, node), "<exact-enrichment-tag-baseline>", "exec"), baseline)
        store = CardProjection(Path(root) / "fixture.db")
        project = store.create_project(ProjectCreate(title="Fixture project"))
        repo = store.create_repository(RepositoryCreate(name="Fixture", url="https://example.test/fixture"))
        store.link_project_repository(project.id, repo.id)
        for index in range(100):
            store.create_card(CardCreate(title=f"Fixture {index}", body="x" * 65536, tags=["shared", f"tag-{index % 9}", "é", "", "shared"]))
        store.create_card(CardCreate(realm_id="other", title="Foreign", tags=["foreign"]))
        card = store.list_cards(realm_id="default")[0]
        ctx = SimpleNamespace(store=store, settings=SimpleNamespace(capabilities=["browser"]), services={})
        value = dict(contract="pa.enrichment-tag-catalog-comparison/v1", started_at=datetime.now(UTC).isoformat(), baseline=BASELINE, source_hashes={str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(module.__file__), Path("src/pa/domain/projection.py")]}, cards=100, body_bytes_per_card=65536, pairs=30, private_roots_before_imports=True, rows=[], scope=__doc__)
        args.output.write_text(json.dumps(value, indent=2) + "\n")
        original = store._row_to_card
        expected = {}
        try:
            for pair in range(30):
                locked = {"title", "body"} | ({"tags"} if pair % 2 else set())
                for variant in (["baseline", "candidate"] if pair % 2 else ["candidate", "baseline"]):
                    counts = dict(models=0, logical_body_bytes=0)
                    def hydrate(row):
                        model = original(row)
                        counts["models"] += 1
                        counts["logical_body_bytes"] += len(model.body.encode("utf-8"))
                        return model
                    with patch.object(store, "_row_to_card", side_effect=hydrate):
                        start = time.perf_counter()
                        request, projects, capabilities = (baseline["enrichment_request"] if variant == "baseline" else module.enrichment_request)(ctx, card, locked)
                        elapsed = (time.perf_counter() - start) * 1000
                    encoded = json.dumps(dict(messages=request.messages, schema=request.schema, projects=[p.model_dump(mode="json") for p in projects], capabilities=capabilities), sort_keys=True, separators=(",", ":")).encode()
                    key = "tags_locked" if "tags" in locked else "tags_offered"
                    expected.setdefault(key, encoded)
                    assert encoded == expected[key]
                    assert counts["models"] == (100 if variant == "baseline" else 0)
                    value["rows"].append(dict(pair=pair, variant=variant, fields=key, request_ms=elapsed, **counts, context_bytes=len(encoded), context_sha256=hashlib.sha256(encoded).hexdigest()))
                    args.output.write_text(json.dumps(value, indent=2) + "\n")
        finally:
            store.close_thread_connection()
        def distribution(items):
            items = sorted(items)
            return {name: items[min(len(items)-1, int((len(items)-1)*percentile))] for name, percentile in [("p50_ms", .5), ("p95_ms", .95), ("p99_ms", .99), ("max_ms", 1)]}
        value.update(finished_at=datetime.now(UTC).isoformat(), complete_context_bytes_equal=True, summary={variant: dict(samples=30, models=sum(r["models"] for r in value["rows"] if r["variant"] == variant), logical_body_bytes=sum(r["logical_body_bytes"] for r in value["rows"] if r["variant"] == variant), **distribution([r["request_ms"] for r in value["rows"] if r["variant"] == variant])) for variant in ["baseline", "candidate"]})
        args.output.write_text(json.dumps(value, indent=2) + "\n")
        print(json.dumps(value["summary"]))


if __name__ == "__main__":
    main()
