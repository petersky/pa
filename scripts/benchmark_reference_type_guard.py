"""Compare a full reference scan with indexed type endpoints in private stores.

Run with --output NEW_PATH. The archived method is read from Git; no checkout,
installation or live PA state is changed. Complete reference paging is retained.
"""
import argparse
import ast
from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, datetime
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import time
from types import MethodType
from unittest.mock import patch


BASELINE = '24c85fc7546336efc52c6074d7c2853771e2ed48'


def distribution(values):
    ordered = sorted(values)
    return dict(samples=len(ordered), **{
        key: ordered[math.ceil(q * len(ordered)) - 1]
        for key, q in [('p50_ms', .5), ('p95_ms', .95), ('p99_ms', .99), ('max_ms', 1)]})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists(), 'Choose a new path; preserve previous evidence'
    out = dict(contract='pa.reference-type-guard-benchmark/v1', baseline=BASELINE,
               started_at=datetime.now(UTC).isoformat(), populations=[])
    with tempfile.TemporaryDirectory(prefix='pa-reference-type-guard-') as temporary:
        root = Path(temporary)
        os.environ.update(PA_DATA_DIR=str(root / 'data'), PA_WORKSPACE_ROOT=str(root / 'workspaces'), PA_AGENT_ENABLED='false')
        from pa.core.performance import OperationObservation, current_observation
        from pa.domain import transcript_storage
        source = subprocess.check_output(['git', 'show', BASELINE + ':src/pa/domain/transcript_storage.py'], text=True)
        cls = next(n for n in ast.parse(source).body if isinstance(n, ast.ClassDef) and n.name == 'TranscriptStorage')
        method = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == 'referenced_hashes')
        leaf = textwrap.dedent(ast.get_source_segment(source, method))
        namespace = dict(vars(transcript_storage))
        exec(compile(leaf, '<archived-reference-read>', 'exec'), namespace)
        archived = namespace['referenced_hashes']
        out['archived_method_sha256'] = hashlib.sha256(leaf.encode()).hexdigest()
        out['loaded_module_path'] = transcript_storage.__file__
        out['loaded_module_sha256'] = hashlib.sha256(Path(transcript_storage.__file__).read_bytes()).hexdigest()
        for count in [100_000, 500_000]:
            folder = root / str(count)
            folder.mkdir()
            store = transcript_storage.TranscriptStorage(folder / 'fixture.db')
            expected = {f'{i:064x}' for i in range(1000)}
            with store._conn() as conn:
                conn.executemany(
                    "INSERT INTO transcript_events(id,session_id,seq,event_type,payload_hash,cold_hash,created_at) "
                    "VALUES(?,'fixture',?,'output','payload',?,'2026-10-25')",
                    ((str(i), i, f'{i % 1000:064x}') for i in range(count)))
            candidate = store.referenced_hashes
            variants = dict(archived=MethodType(archived, store), candidate=candidate)
            population = dict(rows=count, distinct_references=1000, samples=[], first_select_vm_steps={})
            for pair in range(5):
                for label in (['archived', 'candidate'] if pair % 2 == 0 else ['candidate', 'archived']):
                    observation = OperationObservation()
                    token = current_observation.set(observation)
                    start, cpu = time.perf_counter_ns(), time.thread_time_ns()
                    try:
                        actual = variants[label]()
                        timing = dict(wall_ms=(time.perf_counter_ns()-start)/1e6,
                                      thread_cpu_ms=(time.thread_time_ns()-cpu)/1e6)
                    finally:
                        current_observation.reset(token)
                    assert actual == expected
                    population['samples'].append(dict(pair=pair, variant=label, **timing, observation=asdict(observation)))
            original = store._conn
            for label, read in variants.items():
                counts = dict(selects=0, guard_vm_steps=0)

                @contextmanager
                def measured_connection():
                    with original() as conn:
                        def trace(sql):
                            if sql.lstrip().upper().startswith('SELECT'):
                                counts['selects'] += 1

                        def step():
                            if counts['selects'] == 1:
                                counts['guard_vm_steps'] += 1
                            return 0

                        conn.set_trace_callback(trace)
                        conn.set_progress_handler(step, 1)
                        try:
                            yield conn
                        finally:
                            conn.set_progress_handler(None, 0)
                            conn.set_trace_callback(None)

                with patch.object(store, '_conn', measured_connection):
                    assert read() == expected
                population['first_select_vm_steps'][label] = counts
            with store._conn() as conn:
                assert conn.execute('SELECT COUNT(*) FROM transcript_events').fetchone()[0] == count
                assert not conn.execute('SELECT 1 FROM transcript_operations').fetchone()
                # Publication must be visible on the next call, without caching.
                conn.execute("UPDATE transcript_events SET cold_hash='new' WHERE seq=0")
            for read in variants.values():
                assert read() == expected | {'new'}
            population['latency_ms'] = {label: distribution([s['wall_ms'] for s in population['samples'] if s['variant'] == label]) for label in variants}
            out['populations'].append(population)
    out.update(finished_at=datetime.now(UTC).isoformat(), scope='Five alternating pairs per private synthetic population, identical 1000 distinct references with repeated retained events. Complete fresh reference sets and every event retained; a subsequent publication is visible to both methods. No object files are seeded or health file checks skipped by product code; this benchmark measures referenced_hashes only, not full health/maintenance or production load. SQL VM work is a separate instrumented pass excluded from latency samples. Source/archive methods differ only in type validation; no schema, policy, warning or deadline change. Small samples do not prove statistical whole-path speedup or deployed acceptance.', ordering_source='https://www.sqlite.org/datatype3.html#sorting_grouping_and_compound_selects')
    args.output.write_text(json.dumps(out, indent=2) + '\n')
    print(json.dumps(dict(output=str(args.output), sha256=hashlib.sha256(args.output.read_bytes()).hexdigest(), populations=[{k:v for k,v in p.items() if k!='samples'} for p in out['populations']])))


if __name__ == '__main__':
    main()
