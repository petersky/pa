"""Real-fsync backup mixed load in a new, preserved disposable instance.

uv run python scripts/benchmark_backup_responsiveness.py --output .dev/backup.json
No production requests, mocked fsyncs, retention deletion, or existing data.
Use the same script with PYTHONPATH set to an extracted baseline src to compare.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import logging
import math
import sqlite3
import tempfile
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

from starlette.testclient import TestClient

from pa.backup import service as backup_module
from pa.config import Settings
from pa.core.kernel import Kernel
from pa.domain.models import TranscriptEvent


def distribution(values):
    ordered = sorted(values)
    return {f"p{p}_seconds": round(ordered[math.ceil(len(ordered) * p / 100) - 1], 6)
            for p in (50, 95, 99, 100)} if ordered else {}


def run(args):
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="backup-real-fsync-", dir=output.parent))
    destination = root / "backups"
    destination.mkdir(mode=0o700)
    settings = Settings(
        instance_id=str(uuid4()), instance_name="isolated-backup-responsiveness",
        data_dir=root / "data", workspace_root=root / "workspaces",
        backup_destination_dir=destination, backup_run_on_startup=False,
        agent_enabled=False,
    )
    kernel = Kernel.boot(settings=settings)
    app = kernel.build_app()
    store = kernel.ctx.store
    for index in range(args.objects):
        # Fixture setup only: populate immutable bytes directly, avoiding a
        # derived-catalog transaction per file before the measured workload.
        payload = f"isolated backup object {index}".encode()
        digest = hashlib.sha256(payload).hexdigest()
        path = settings.objects_dir / digest[:2] / digest[2:]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    for offset in range(0, args.cold_transcripts, 128):
        store.append_transcript_events([TranscriptEvent(
            session_id="isolated-backup-transcripts", seq=index + 1,
            event_type="turn_completed", payload={"text": f"payload {index} " * 2000},
        ) for index in range(offset, min(offset + 128, args.cold_transcripts))])
    if not 0 <= args.missing_cold_transcripts <= args.cold_transcripts:
        raise ValueError("missing cold payload count must fit the disposable fixture")
    missing = sorted(store.transcripts.referenced_hashes())[:args.missing_cold_transcripts]
    for digest in missing:
        # Pre-existing historical absence in this newly created fixture only.
        # The real capture must preserve its database reference and disclose it.
        store.transcripts._object_path(digest).unlink()
    database_bytes = {}
    for database, minimum in (
        (settings.db_path, args.projection_bytes),
        (store.transcripts.db_path, args.transcript_bytes),
    ):
        with contextlib.closing(sqlite3.connect(database)) as conn:
            size = conn.execute("PRAGMA page_count").fetchone()[0] * conn.execute("PRAGMA page_size").fetchone()[0]
            if minimum > size:
                conn.execute("CREATE TABLE backup_fixture_padding (value BLOB)")
                for _ in range(math.ceil((minimum - size) / (1024 * 1024))):
                    conn.execute("INSERT INTO backup_fixture_padding VALUES(zeroblob(1048576))")
                conn.commit()
            database_bytes[database.name] = conn.execute("PRAGMA page_count").fetchone()[0] * conn.execute("PRAGMA page_size").fetchone()[0]
    result = {
        "schema": "pa.backup-responsiveness/v1", "fixture_root": str(root),
        "source_revision": args.source_revision,
        "backup_module_sha256": hashlib.sha256(Path(backup_module.__file__).read_bytes()).hexdigest(),
        "backup_module": str(Path(backup_module.__file__).resolve()),
        "objects": args.objects, "cold_transcripts": args.cold_transcripts, "real_fsync": True,
        "expected_missing_payloads": missing,
        "database_logical_bytes": database_bytes,
        "production_requests": 0, "archives_preserved": True, "samples": [], "operations": [],
        "manual_only": args.manual_only,
    }

    def save():
        output.write_text(json.dumps(result, indent=2) + "\n")

    with TestClient(app) as client, ThreadPoolExecutor(max_workers=1) as worker:
        service = app.state.ctx.require_service("backup_service")
        logging.getLogger("pa.modules.ui_shell").setLevel(logging.WARNING)
        client.get("/api/backups/status")
        headers = {"X-CSRF-Token": client.cookies.get("pa_csrf")}

        def request(method, path, *, scenario, **kwargs):
            started = time.perf_counter()
            response = client.request(method, path, **kwargs)
            sample = {"scenario": scenario, "method": method, "path": path,
                      "seconds": round(time.perf_counter() - started, 6), "status": response.status_code}
            result["samples"].append(sample)
            if response.status_code >= 400:
                raise RuntimeError(f"fixture request failed: {sample}: {response.text[:300]}")
            return response

        def interactive(scenario):
            request("POST", "/api/cards", scenario=scenario,
                    json={"title": f"isolated {scenario} {uuid4()}"},
                    headers={**headers, "Idempotency-Key": str(uuid4())})
            for path in ("/api/backups/status", "/api/backups?limit=50&offset=0",
                         "/api/cards/summaries?limit=10", "/settings?section=backups"):
                request("GET", path, scenario=scenario)

        for _ in range(3):
            interactive("baseline")

        def mixed(scenario, operation):
            future = worker.submit(operation)
            while not future.done():
                interactive(scenario)
                time.sleep(0.25)
            value = future.result()
            result["operations"].append({"scenario": scenario, "result": value})
            save()
            return value

        manual = mixed("manual", lambda: request(
            "POST", "/api/backups", scenario="manual-operation", json={},
            headers={**headers, "Idempotency-Key": str(uuid4())}).json())

        # Exercise the actual dedicated scheduled-worker entry point. Timer
        # admission itself is separately covered by scheduler regression tests.
        async def scheduled():
            value = await service._run_maintenance(
                "backup.scheduled", service.run_backup, trigger="scheduled",
                idempotency_key=str(uuid4()),
            )
            return value.model_dump(mode="json")

        if not args.manual_only:
            mixed("scheduled-worker", lambda: client.portal.call(scheduled))
            mixed("explicit-verify", lambda: request(
                "POST", f"/api/backups/{manual['backup_id']}/verify",
                scenario="verify-operation", headers=headers).json())
        result["runs"] = [value.model_dump(mode="json") for value in service._state.runs]
        result["metrics"] = dict(service._state.metrics)
        result["archives"] = []
        for record in service.list_backups():
            with tarfile.open(record.path, "r:*") as archive:
                member = archive.next()
                assert member.name == "manifest.json"
                manifest = json.load(archive.extractfile(member))
                result["archives"].append({
                    "backup_id": record.backup_id, "manifest_bytes": member.size,
                    "manifest_files": len(manifest["files"]), "verified": record.verified,
                    "integrity_verified": record.integrity_verified,
                    "data_complete": record.data_complete,
                    "missing_transcript_objects": manifest.get("missing_transcript_objects", []),
                })
                assert {item["sha256"] for item in manifest.get("missing_transcript_objects", [])} == set(missing)
                if args.objects >= 12_000:
                    assert member.size > 2 * 1024 * 1024
        assert result["archives"]
        assert all(item["integrity_verified"] and item["data_complete"] == (not missing)
                   and item["verified"] == (not missing) for item in result["archives"])
        result["health"] = {key: service.status()[key] for key in (
            "backup_count", "verified_backup_count", "last_success", "last_known_good",
            "consecutive_failures", "limited_backup_count", "historical_data_warning", "last_completed",
        )}
        assert store.transcripts.referenced_hashes() >= set(missing)
        assert all(not store.transcripts._object_path(digest).exists() for digest in missing)
        assert all(value["status"] == "success" for value in result["runs"])
        assert not any(value["pruned_backup_ids"] for value in result["runs"])
        result["distributions"] = {
            scenario: {kind: distribution([sample["seconds"] for sample in result["samples"]
                       if sample["scenario"] == scenario and sample["method"] == kind])
                       for kind in ("GET", "POST")}
            for scenario in ("baseline", "manual", "scheduled-worker", "explicit-verify")
        }
        save()
        print(json.dumps({"output": str(output), "distributions": result["distributions"],
                          "metrics": result["metrics"]}, indent=2), flush=True)
        # Keep the independently established two-second page guard, also apply
        # it to writes; never turn a successful backup into latency acceptance.
        assert all(sample["seconds"] < 2.0 for sample in result["samples"]
                   if sample["scenario"] in result["distributions"]), "interactive latency exceeds two seconds"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--objects", type=int, default=12_000)
    parser.add_argument("--cold-transcripts", type=int, default=128)
    parser.add_argument("--missing-cold-transcripts", type=int, default=0,
                        help="Pre-existing absent payloads in the new disposable fixture only")
    parser.add_argument("--projection-bytes", type=int, default=0,
                        help="Minimum disposable projection size, including inert fixture padding")
    parser.add_argument("--transcript-bytes", type=int, default=0,
                        help="Minimum disposable transcript database size, including inert fixture padding")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-revision", default="working-tree")
    parser.add_argument("--manual-only", action="store_true")
    run(parser.parse_args())
