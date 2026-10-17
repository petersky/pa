"""Disposable complete-history read amplification; no service or peer traffic."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys
import tempfile
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pa.core.performance import distribution
from pa.domain.models import CardEvent, EventType, SyncCommit
from tests.test_realm_convergence import _Node


def run(root: Path, *, commits: int, iterations: int) -> dict:
    node = _Node(root, "collector", "Collector")
    parents, expected = [], {}
    stamp = datetime(2026, 1, 1, tzinfo=UTC)
    for index in range(commits):
        event = CardEvent(id=f"event-{index}", timestamp=stamp,
            author_instance="collector", author_principal="user:local",
            type=EventType.CARD_CREATED, realm_id="default", card_id=f"card-{index}",
            payload={"title": f"fixture-{index}"})
        event_hash = node.objects.put_json(event.model_dump(mode="json"))
        commit = SyncCommit(hash="", realm_id="default", instance_id="collector",
            parent_hashes=parents, event_hashes=[event_hash],
            author_principal="user:local", timestamp=stamp)
        head = node.objects.put_json(commit.model_dump(mode="json"))
        parents = [head]
        for object_hash in (event_hash, head):
            expected[object_hash] = base64.b64encode(node.objects.get(object_hash)).decode()
    calls, samples = Counter(), []
    original = node.objects.get

    def counted(object_hash):
        calls[object_hash] += 1
        return original(object_hash)

    node.objects.get = counted
    for _ in range(iterations):
        started = time.perf_counter()
        exported = node.engine._collect_objects(head)
        samples.append((time.perf_counter() - started) * 1000)
        assert exported == expected
    return {
        "scope": "Disposable immutable chain; identical complete exported bytes; no network, projection publication or production mutation",
        "commits": commits, "samples": iterations, "objects_per_call": len(expected),
        "read_calls_total": sum(calls.values()),
        "read_calls_per_collection": sum(calls.values()) / iterations,
        "max_reads_per_object_per_collection": max(calls.values()) / iterations,
        "latency_ms": distribution(samples),
        "canonical_payload_sha256": hashlib.sha256(json.dumps(expected, sort_keys=True).encode()).hexdigest(),
        "source_sha256": {name: hashlib.sha256(Path(sys.modules[name].__file__).read_bytes()).hexdigest()
                          for name in ("pa.sync.engine", "pa.sync.event_log")},
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--commits", type=int, default=1000, choices=range(1, 10001), metavar="1..10000")
    parser.add_argument("--iterations", type=int, default=10, choices=range(1, 101), metavar="1..100")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="pa-collect-read-") as tmp:
        result = run(Path(tmp), commits=args.commits, iterations=args.iterations)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
