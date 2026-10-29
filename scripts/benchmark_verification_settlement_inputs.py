"""Exact archived/native private settlement comparison; no verifier admission.

Nested steps and followup entries count entries in full dispatch snapshots, not
bytes written or physical I/O. The required followup helper still inspects its
turn/operation inputs on the native path; that scan is not this copy counter.
Each scenario retains its own raw samples and maxima.
"""
import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from unittest.mock import patch

BASELINE = 'fcabfdc75e2f3d27a3babe07b85e124a3cf85e3d'


def distribution(values):
    ordered = sorted(values)
    return {name: ordered[int((len(ordered)-1)*p)] for name, p in
            [('p50_ms', .50), ('p95_ms', .95), ('p99_ms', .99)]} | {
                'samples': len(ordered), 'max_ms': max(ordered)}


def check_scope():
    hashes = {}
    archived_method = None
    for path in ['src/pa/execution/verification.py', 'src/pa/execution/dispatch.py',
                 'src/pa/execution/followup.py']:
        before = subprocess.check_output(['git', 'show', BASELINE+':'+path], text=True)
        after = Path(path).read_text()
        hashes[path] = dict(baseline=hashlib.sha256(before.encode()).hexdigest(),
                            candidate=hashlib.sha256(after.encode()).hexdigest())
        trees = [ast.parse(s) for s in [before, after]]
        if path.endswith('/verification.py'):
            for index, tree in enumerate(trees):
                cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'VerificationService')
                if index == 0:
                    archived_method = next(n for n in cls.body if getattr(n, 'name', None) == 'origins_settled')
                cls.body = [n for n in cls.body if getattr(n, 'name', None) != 'origins_settled']
                tree.body = [n for n in tree.body if not (isinstance(n, ast.ImportFrom) and n.module == 'inspect')]
                for n in tree.body:
                    if isinstance(n, ast.ImportFrom) and n.module == 'pa.execution.dispatch':
                        n.names = [alias for alias in n.names if alias.name != 'DispatchStore']
        elif path.endswith('/dispatch.py'):
            for tree in trees:
                tree.body = [n for n in tree.body if getattr(n, 'name', None) != 'VerificationSettlement']
                cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'DispatchStore')
                cls.body = [n for n in cls.body if getattr(n, 'name', None) not in {
                    '_verification_settlement', 'verification_settlement'}]
        assert ast.dump(trees[0]) == ast.dump(trees[1]), 'Unexpected application change: '+path
    return hashes, archived_method


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists()
    hashes, method = check_scope()
    value = dict(contract='pa.verification-settlement-input-comparison/v1',
        started_at=datetime.now(UTC).isoformat(), baseline=BASELINE, source_hashes=hashes,
        other_application_ast_equal_after_explicit_scope=True,
        percentile_method='Lower empirical floor((n-1)*p); all raw samples/maxima retained.', scenarios={})
    def save():
        args.output.write_text(json.dumps(value, indent=2)+'\n')
    with tempfile.TemporaryDirectory(prefix='pa-settlement-native-') as tmp:
        root = Path(tmp)
        os.environ.update(PA_DATA_DIR=str(root/'unused-data'), PA_WORKSPACE_ROOT=str(root/'unused-workspaces'), PA_AGENT_ENABLED='false')
        sys.path.insert(0, str(Path.cwd()))
        from pa.execution import verification
        from pa.execution.dispatch import DispatchRecord
        from tests.test_autonomous_verification import system, ordinary, origin
        assert Path(verification.__file__).resolve() == Path('src/pa/execution/verification.py').resolve()
        namespace = dict(vars(verification))
        exec(compile(ast.Module(body=[method], type_ignores=[]), 'exact611-origins-settled', 'exec'), namespace)
        for shape, count in [('ordinary100', 100), ('settled_followups50', 50)]:
            directory = root/shape
            directory.mkdir()
            fixture = system.__wrapped__(directory)
            target = next(fixture)
            ledger = target.ctx.services['dispatch_store']
            try:
                card, first = ordinary(target)
                if shape == 'settled_followups50':
                    first.followup_turns = [{'state': 'ended', 'delivery_state': 'acknowledged', 'prompt_id': f'p-{j}'} for j in range(10)]
                    first.followup_operations = {f'k-{j}': {'state': 'accepted', 'response': {'prompt_id': f'p-{j}', 'accepted': True}, 'retained_proof': 'x'*4096} for j in range(10)}
                    ledger.put(first)
                for index in range(count-1):
                    extra = dict(followup_turns=first.followup_turns, followup_operations=first.followup_operations) if shape == 'settled_followups50' else {}
                    origin(target, card.id, session_id=f'origin-{index}',
                           materialization_plan={'steps': [{'proof': ['x'*64], 'index': j} for j in range(1000)]}, **extra)
                packet = target.service.packet(card)
                assert len(packet['origin_dispatches']) == count
                rows = []
                value['scenarios'][shape] = dict(records=count, calls_per_variant=30, rows=rows)
                original_copy = DispatchRecord.model_copy
                for pair in range(30):
                    for variant in (['baseline', 'candidate'] if pair % 2 == 0 else ['candidate', 'baseline']):
                        copies = []
                        def counted(record, **kwargs):
                            copies.append((len((record.materialization_plan or {}).get('steps', [])),
                                           len(record.followup_turns)+len(record.followup_operations)))
                            return original_copy(record, **kwargs)
                        call = namespace['origins_settled'] if variant == 'baseline' else verification.VerificationService.origins_settled
                        with patch.object(DispatchRecord, 'model_copy', counted):
                            wall, cpu = time.perf_counter(), time.thread_time()
                            settled = call(target.service, packet)
                            ms, cpu_ms = (time.perf_counter()-wall)*1000, (time.thread_time()-cpu)*1000
                        assert settled is True and not target.calls
                        rows.append(dict(pair=pair, variant=variant, wall_ms=ms, thread_cpu_ms=cpu_ms,
                            settled=settled, full_dispatch_snapshots=len(copies),
                            logical_nested_plan_steps=sum(c[0] for c in copies),
                            logical_followup_entries=sum(c[1] for c in copies)))
                        save()
                value['scenarios'][shape].update(complete_results_equal=True, no_admission=True,
                    variants={variant: dict(latency_ms=distribution([r['wall_ms'] for r in rows if r['variant'] == variant]),
                        full_dispatch_snapshots=sum(r['full_dispatch_snapshots'] for r in rows if r['variant'] == variant),
                        logical_nested_plan_steps=sum(r['logical_nested_plan_steps'] for r in rows if r['variant'] == variant),
                        logical_followup_entries=sum(r['logical_followup_entries'] for r in rows if r['variant'] == variant))
                        for variant in ['baseline', 'candidate']})
            finally:
                target.ctx.store.close_thread_connection()
                try:
                    next(fixture)
                except StopIteration:
                    pass
    value.update(finished_at=datetime.now(UTC).isoformat(), scope=__doc__ +
        'Thirty alternating pairs per native ordinary100/settled_followups50 shape; complete True results equal, no admission. '
        'Only settlement projection/reader and origins_settled accessor selection differ in application AST. '
        'Followup helper byte-identical. Branch contract matrix is separate; named path/hash equality is not everybranch acceptance. '
        'No productiondata/provider/network/restart/policy/GC/deadline changes; no physicalIO/historicalcause/hardwarebound/installed/wholePA acceptance.')
    save()
    print(json.dumps({k: v['variants'] for k, v in value['scenarios'].items()}))


if __name__ == '__main__':
    main()
