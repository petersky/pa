"""Private-root SQLite observer cost and successful-wait demonstration.

Run with --output NEW_PATH. No live PA state or production lock is used.
"""
from __future__ import annotations

import argparse
import ast
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import threading
import time
from contextlib import closing
from unittest.mock import patch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists(), 'Preserve previous evidence'
    with tempfile.TemporaryDirectory(prefix='pa-sqlite-cpu-timing-') as temporary:
        root = Path(temporary)
        os.environ.update(PA_DATA_DIR=str(root / 'data'),
                          PA_WORKSPACE_ROOT=str(root / 'workspaces'), PA_AGENT_ENABLED='false')
        # Both roots precede all PA imports, even module metadata reads.
        from pa.core.performance import OperationObservation, current_observation, distribution
        from pa.core.sqlite_metrics import ObservedConnection

        archived_source = subprocess.check_output([
            'git', 'show', '056b8e3e8dd64cde0558ff6a4c7b96cbf359397a:src/pa/core/sqlite_metrics.py'
        ], text=True)
        tree = ast.parse(archived_source)
        method = next(n for c in tree.body if isinstance(c, ast.ClassDef)
                      and c.name == 'ObservedConnection' for n in c.body
                      if isinstance(n, ast.FunctionDef) and n.name == '_observe')
        namespace = dict(time=time, sqlite3=sqlite3, current_observation=current_observation)
        exec(compile(ast.Module(body=[method], type_ignores=[]), '<archived-observer>', 'exec'), namespace)
        archived, candidate = namespace['_observe'], ObservedConnection._observe
        samples = []
        for iteration in range(5):
            for mode in (('archived', 'candidate') if iteration % 2 == 0 else ('candidate', 'archived')):
                with closing(sqlite3.connect(':memory:', factory=ObservedConnection)) as conn:
                    conn.execute('CREATE TABLE fixture(value INTEGER)')
                    observation = OperationObservation()
                    token = current_observation.set(observation)
                    start, cpu = time.perf_counter(), time.thread_time()
                    try:
                        with patch.object(ObservedConnection, '_observe', archived if mode == 'archived' else candidate):
                            for i in range(5000):
                                assert conn.execute('SELECT ?+1', (i,)).fetchone() == (i + 1,)
                            conn.execute('INSERT INTO fixture VALUES(42)')
                            conn.commit()
                    finally:
                        wall_ms = (time.perf_counter() - start) * 1000
                        cpu_ms = (time.thread_time() - cpu) * 1000
                        current_observation.reset(token)
                    assert observation.sqlite_calls == 10001 and observation.sqlite_rows == 5000
                    assert conn.execute('SELECT * FROM fixture').fetchall() == [(42,)]
                    samples.append(dict(iteration=iteration, mode=mode, wall_ms=wall_ms,
                                        thread_cpu_ms=cpu_ms, observation=asdict(observation)))

        # One bounded real writer wait in a disposable DB, with release even on failure.
        path = root / 'wait.db'
        with closing(sqlite3.connect(path, check_same_thread=False)) as holder:
            holder.execute('CREATE TABLE fixture(value INTEGER)')
            holder.execute('INSERT INTO fixture VALUES(0)')
            holder.commit()
            holder.execute('BEGIN IMMEDIATE')
            holder.execute('UPDATE fixture SET value=999')
            timer = threading.Timer(0.060, holder.rollback)
            timer.start()
            observation = OperationObservation()
            token = current_observation.set(observation)
            try:
                with closing(sqlite3.connect(path, timeout=1, factory=ObservedConnection)) as contender:
                    contender.execute('UPDATE fixture SET value=value+1')
                    contender.commit()
                    assert contender.execute('SELECT * FROM fixture').fetchall() == [(1,)]
            finally:
                current_observation.reset(token)
                timer.join(timeout=2)
                assert not timer.is_alive()
                holder.rollback()
            wait = asdict(observation)
            assert observation.sqlite_busy_failures == 0

        out = dict(contract='pa.sqlite-call-cpu-observation/v1',
                   source_sha256=hashlib.sha256(Path('src/pa/core/sqlite_metrics.py').read_bytes()).hexdigest(),
                   archived_source='056b8e3e8dd64cde0558ff6a4c7b96cbf359397a',
                   sqlite_version=sqlite3.sqlite_version, samples=samples,
                   latency_ms={mode: distribution([s['wall_ms'] for s in samples if s['mode'] == mode])
                               for mode in ('archived', 'candidate')},
                   successful_private_writer_wait=wait,
                   scope='Five alternating pairs of 5000 in-memory reads plus one insert/commit each; exact values, 10001 statements/fetches and 5000 rows retained. Setup/assertion outside measured call loop except per-result parity assertions. One separately bounded 60 ms private writer release demonstrates a successful native wait with zero busy exceptions. All numbers include observer overhead. Wall minus thread CPU remains unclassified off-CPU time and is not a general lock/disk attribution. No SQL/parameters/results, live state, providers or credentials are retained; no production fault injection or whole-path speedup claim.')
        args.output.write_text(json.dumps(out, indent=2) + '\n')
        print(json.dumps(dict(output=str(args.output), latency_ms=out['latency_ms'],
                              successful_wait=wait, sha256=hashlib.sha256(args.output.read_bytes()).hexdigest())))


if __name__ == '__main__':
    main()
