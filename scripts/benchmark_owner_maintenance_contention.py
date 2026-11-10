"""Bounded private paired probe of two databases belonging to one PA owner."""
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import shutil
import math
from collections import Counter
import sqlite3
import tempfile
import threading
import time
from unittest.mock import patch

from pa.core.sqlite_maintenance import DatabaseMaintenanceGate
from pa.core.sqlite_metrics import ObservedConnection
from pa.instance.maintenance import _maintain_database


def benchmark(*, pairs=2):
    if not 1 <= pairs <= 3:
        raise ValueError("pairs must be1..3")
    samples = []
    with tempfile.TemporaryDirectory(prefix="pa-owner-maintenance-") as temporary:
        root = Path(temporary)
        if shutil.disk_usage(root).free < 4*1024**3:
            raise RuntimeError("At least4GiB free required before seeding")
        for pair in range(pairs):
            for shared in ((False,True) if pair % 2 == 0 else (True,False)):
                metadata, transcript = root / "metadata.db", root / "transcript.db"
                rows = 32768
                for path in (metadata,transcript):
                    with closing(sqlite3.connect(path)) as seed, seed:
                        seed.execute("PRAGMA journal_mode=WAL")
                        seed.execute("PRAGMA synchronous=FULL")
                        seed.execute("CREATE TABLE retained(id INTEGER PRIMARY KEY,body BLOB)")
                        seed.executemany("INSERT INTO retained VALUES(?,?)",((i,b"x"*4096) for i in range(rows)))
                        seed.execute("CREATE TABLE foreground(value)")
                        seed.execute("INSERT INTO foreground VALUES(0)")
                        seed.execute("DELETE FROM retained WHERE id<64")
                with (closing(sqlite3.connect(metadata)) as reader,
                      closing(sqlite3.connect(metadata)) as producer,
                      closing(sqlite3.connect(metadata,timeout=10)) as writer):
                    reader.execute("BEGIN")
                    reader.execute("SELECT value FROM foreground").fetchone()
                    # This fixture producer alone keeps a large WAL for the
                    # target writer. Both target modes retain FULL + their
                    # original automatic-checkpoint threshold.
                    producer.execute("PRAGMA synchronous=FULL")
                    producer.execute("PRAGMA wal_autocheckpoint=0")
                    producer.execute("UPDATE retained SET body=?",(b"y"*4096,))
                    producer.commit()
                    reader.rollback()
                    writer.execute("PRAGMA synchronous=FULL")
                    threshold = writer.execute("PRAGMA wal_autocheckpoint").fetchone()[0]
                    writer.execute("SELECT value FROM foreground").fetchone()
                    entered = threading.Event()
                    owner = DatabaseMaintenanceGate()
                    maintenance_gate = owner if shared else DatabaseMaintenanceGate()
                    local = threading.local()
                    execute, progress = ObservedConnection.execute, ObservedConnection.set_progress_handler
                    def observed(connection, sql, *args):
                        local.vacuum = sql == "VACUUM"
                        try:
                            return execute(connection,sql,*args)
                        finally:
                            local.vacuum = False
                    def observed_progress(connection,callback,interval):
                        def observed_callback():
                            if getattr(local,"vacuum",False):
                                entered.set()
                            return callback()
                        return progress(connection,observed_callback,interval)
                    with (ThreadPoolExecutor(max_workers=1) as pool,
                          patch.object(ObservedConnection,"execute",observed),
                          patch.object(ObservedConnection,"set_progress_handler",observed_progress)):
                        future = pool.submit(_maintain_database,transcript,writers=maintenance_gate)
                        assert entered.wait(10), "Actual native VACUUM progress not observed"
                        start, cpu = time.perf_counter(), time.thread_time()
                        with owner.writer():
                            writer.execute("UPDATE foreground SET value=value+1")
                            commit_start = time.perf_counter()
                            writer.commit()
                            commit_ms = (time.perf_counter()-commit_start)*1000
                        writer_ms, writer_cpu = (time.perf_counter()-start)*1000,(time.thread_time()-cpu)*1000
                        result = future.result(timeout=20)
                    retries = [_maintain_database(path) for path in (metadata,transcript)]
                    assert all(report["status"]=="ok" and report["quick_check"]=="ok" and report["after"]["free_bytes"]==0 for report in retries)
                    assert result["compacted"] or retries[1]["compacted"]
                    assert retries[0]["compacted"]
                    assert writer.execute("SELECT COUNT(*),MIN(id),MAX(id),SUM(body!=?) FROM retained",(b"y"*4096,)).fetchone()==(rows-64,64,rows-1,0)
                    assert writer.execute("SELECT value FROM foreground").fetchone()==(1,)
                    with closing(sqlite3.connect(transcript)) as verify:
                        assert verify.execute("SELECT COUNT(*),MIN(id),MAX(id),SUM(body!=?) FROM retained",(b"x"*4096,)).fetchone()==(rows-64,64,rows-1,0)
                    assert writer.execute("PRAGMA wal_autocheckpoint").fetchone()[0] == threshold
                    samples.append({"shared_owner":shared,"writer_ms":round(writer_ms,3),"commit_ms":round(commit_ms,3),"writer_cpu_ms":round(writer_cpu,3),"maintenance_ms":result["phases"]["total"]["wall_ms"],"reason":result.get("reason"),"compacted_before_quiet":result["compacted"],"quiet_total_ms":[r["phases"]["total"]["wall_ms"] for r in retries],"target_wal_autocheckpoint":threshold})
                for file in root.iterdir():
                    file.unlink()  # Only files generated in this private root.
    def summary(values):
        values = sorted(values)
        return {"samples":len(values),"max_ms":max(values),
                **{f"p{p}_ms":values[math.ceil(len(values)*p/100)-1] for p in (50,95,99)}}
    return {"pairs":pairs,"rows_per_database":32768,
            "modes":{str(shared):{
                **{field:summary([r[field] for r in samples if r["shared_owner"]==shared])
                   for field in ("writer_ms","commit_ms","writer_cpu_ms","maintenance_ms")},
                "quiet_total_ms":summary([value for r in samples if r["shared_owner"]==shared for value in r["quiet_total_ms"]]),
                "outcomes":dict(Counter(r["reason"] or "complete" for r in samples if r["shared_owner"]==shared)),
                "target_wal_autocheckpoint":sorted({r["target_wal_autocheckpoint"] for r in samples if r["shared_owner"]==shared})}
                for shared in (False,True)},
            "temporary_fixture_removed":not root.exists(),
            "scope":"Private FULL-synchronous checkpoint-heavy writer concurrent with actual other-database VACUUM. Only owner-gate sharing differs. All retained bytes/IDs/update and actual quiet full reclamation/integrity verified. Target pragmas unchanged; producer disables its automatic checkpoint solely to construct a pinned-reader WAL. Not deployed/physical-I/O/whole-card acceptance."}


if __name__ == "__main__":
    print(json.dumps(benchmark(),separators=(",",":")))
