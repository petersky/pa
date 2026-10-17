"""Disposable status-count benchmark; no installed PA state or provider work."""
import argparse
import hashlib
import json
import tempfile
import time
from pathlib import Path

from pa.core.performance import distribution
from pa.domain.models import AgentSession, CardCreate, KnowledgeEntry
from pa.domain.projection import CardProjection
from pa.status import info


def run(root, *, aggregate=False):
    store = CardProjection(root / "fixture.db")
    for index in range(331):
        store.create_card(CardCreate(title=f"fixture {index}", body="x" * 65536))
    for index in range(359):
        store.save_session(AgentSession(id=f"session-{index}", agent_name="fixture",
            metrics_json={"fixture": "x" * 65536}))
    for index in range(10):
        store.add_knowledge(KnowledgeEntry(summary=f"fixture {index}"))
    samples, hydrated = [], {"cards": 0, "sessions": 0, "knowledge": 0}
    for kind, method in (("cards", "_row_to_card"), ("sessions", "_row_to_session"), ("knowledge", "_row_to_knowledge")):
        original = getattr(store, method)
        def observe(row, original=original, kind=kind):
            hydrated[kind] += 1
            return original(row)
        setattr(store, method, observe)
    for _ in range(30):
        start = time.perf_counter()
        result = store.status_inventory() if aggregate else {
            "item_count": len(store.list_items()), "session_count": len(store.list_sessions()),
            "knowledge_count": len(store.list_knowledge(limit=5)),
        }
        samples.append((time.perf_counter() - start) * 1000)
        assert result == {"item_count": 331, "session_count": 359, "knowledge_count": 5}
    return {"scope": "331 cards and 359 sessions with 64KiB synthetic body/metrics each, 10 active knowledge entries, 30 count reads; isolated projection only",
        "mode": "aggregate" if aggregate else "full_hydration", "latency_ms": distribution(samples),
        "hydrated": hydrated, "result": result,
        "source_projection_sha256": hashlib.sha256(Path(__import__('pa.domain.projection', fromlist=['x']).__file__).read_bytes()).hexdigest(),
        "source_status_sha256": hashlib.sha256(Path(info.__file__).read_bytes()).hexdigest()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aggregate", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="pa-status-inventory-") as temp:
        result = run(Path(temp), aggregate=args.aggregate)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
