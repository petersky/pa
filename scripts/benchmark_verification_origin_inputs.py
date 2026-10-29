"""Compare complete private packet planning with archived and scalar origin reads.

No verifier is admitted. Full watch proof validation, packet digests, origin
bindings and retry-control decisions remain authoritative in both variants.
Copied plan steps are logical work, not physical disk I/O or write amplification.
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

BASELINE = '4bc5b353603813a79adbc4c2360644d6b3b3308f'


def distribution(values):
    values = sorted(values)
    return dict(samples=len(values), p50_ms=values[int((len(values)-1)*.50)],
                p95_ms=values[int((len(values)-1)*.95)],
                p99_ms=values[int((len(values)-1)*.99)], max_ms=max(values))


def check_scope():
    hashes = {}
    archived_packet = None
    for path in ['src/pa/execution/verification.py', 'src/pa/execution/dispatch.py',
                 'src/pa/health_journal/service.py']:
        before = subprocess.check_output(['git', 'show', BASELINE+':'+path], text=True)
        after = Path(path).read_text()
        trees = [ast.parse(text) for text in [before, after]]
        hashes[path] = dict(baseline=hashlib.sha256(before.encode()).hexdigest(),
                            candidate=hashlib.sha256(after.encode()).hexdigest())
        if path.endswith('/verification.py'):
            for index, tree in enumerate(trees):
                cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'VerificationService')
                if index == 0:
                    archived_packet = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == 'packet')
                cls.body = [n for n in cls.body if getattr(n, 'name', None) != 'packet']
                # Only fixed diagnostic keywords differ outside packet planning.
                for n in ast.walk(tree):
                    if isinstance(n, ast.Call) and ast.unparse(n.func) == 'self.call':
                        n.keywords = [kw for kw in n.keywords if kw.arg != 'diagnostic_phase']
        elif path.endswith('/dispatch.py'):
            for tree in trees:
                tree.body = [n for n in tree.body if getattr(n, 'name', None) != 'VerificationOrigin']
                cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'DispatchStore')
                cls.body = [n for n in cls.body if getattr(n, 'name', None) not in {
                    '_verification_origin', 'verification_origin', 'verification_card_origins'}]
        else:
            for tree in trees:
                tree.body = [n for n in tree.body if not (isinstance(n, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id == 'CANONICAL_PHASES' for t in n.targets))]
        assert ast.dump(trees[0]) == ast.dump(trees[1]), 'Unexpected shared application change: '+path
    return hashes, archived_packet


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists()
    hashes, node = check_scope()
    value = dict(contract='pa.verification-origin-input-comparison/v1',
                 started_at=datetime.now(UTC).isoformat(), baseline=BASELINE,
                 source_hashes=hashes, other_application_ast_equal_after_explicit_scope=True,
                 percentile_method='Lower empirical floor((n-1)*p); all maxima/samples retained.',
                 private_roots_before_imports=True, scenarios={})
    def save():
        args.output.write_text(json.dumps(value, indent=2)+'\n')

    with tempfile.TemporaryDirectory(prefix='pa-verification-origins-') as tmp:
        root = Path(tmp)
        os.environ.update(PA_DATA_DIR=str(root/'unused-data'),
                          PA_WORKSPACE_ROOT=str(root/'unused-workspaces'), PA_AGENT_ENABLED='false')
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from pa.execution import verification
        from tests.test_autonomous_verification import system, ordinary, origin, health_repair
        assert Path(verification.__file__).resolve() == Path('src/pa/execution/verification.py').resolve()
        namespace = dict(vars(verification))
        exec(compile(ast.Module(body=[node], type_ignores=[]), 'exact609-packet', 'exec'), namespace)
        for shape, count in [('ordinary100', 100), ('health50', 50)]:
            directory = root/shape
            directory.mkdir()
            fixture = system.__wrapped__(directory)
            target = next(fixture)
            ledger = target.ctx.services['dispatch_store']
            try:
                card = ordinary(target)[0] if shape == 'ordinary100' else health_repair(target)[0]
                for index in range(count-1):
                    origin(target, card.id, session_id=f'origin-{index}',
                           materialization_plan={'steps': [{'proof': ['x'*64], 'index': j} for j in range(1000)]})
                rows = []
                value['scenarios'][shape] = dict(records=count, nested_plan_steps_per_added_record=1000,
                                                 calls_per_variant=30, rows=rows)
                snapshot = ledger._snapshot
                for pair in range(30):
                    expected = None
                    for variant in (['baseline', 'candidate'] if pair % 2 == 0 else ['candidate', 'baseline']):
                        copies = []
                        def counted(record):
                            copies.append(len((record.materialization_plan or {}).get('steps', [])))
                            return snapshot(record)
                        call = namespace['packet'] if variant == 'baseline' else verification.VerificationService.packet
                        with patch.object(ledger, '_snapshot', side_effect=counted):
                            wall, cpu = time.perf_counter(), time.thread_time()
                            packet = call(target.service, card)
                            elapsed, cpu_ms = (time.perf_counter()-wall)*1000, (time.thread_time()-cpu)*1000
                        expected = packet if expected is None else expected
                        assert packet == expected and not target.calls
                        rows.append(dict(pair=pair, variant=variant, wall_ms=elapsed, thread_cpu_ms=cpu_ms,
                                         full_dispatch_snapshots=len(copies), copied_nested_plan_steps=sum(copies),
                                         packet_sha256=hashlib.sha256(json.dumps(packet, sort_keys=True).encode()).hexdigest()))
                        save()
                value['scenarios'][shape].update(complete_packets_equal=True, no_admission=True,
                    variants={variant: dict(latency_ms=distribution([r['wall_ms'] for r in rows if r['variant'] == variant]),
                        full_dispatch_snapshots=sum(r['full_dispatch_snapshots'] for r in rows if r['variant'] == variant),
                        copied_nested_plan_steps=sum(r['copied_nested_plan_steps'] for r in rows if r['variant'] == variant))
                        for variant in ['baseline', 'candidate']})
            finally:
                target.ctx.store.close_thread_connection()
                try:
                    next(fixture)
                except StopIteration:
                    pass
    value.update(finished_at=datetime.now(UTC).isoformat(), scope=__doc__ +
        'Thirty alternating pairs per ordinary100/health50 native private shape, retained1000nested steps per added origin. '
        'Only packet/scalar-origin projections/fixed diagnostic labels differ in application AST. All complete packets match; '
        'full watch proofs stay on the original path, no verifier admission or real provider/network. No GC/deadline/policy change. '
        'Separate profile/prototype and historical541ms health observations remain distinct; no physicalIO, historicalcause, '
        'statistical hardware bound, everybranch, deployment or wholePA acceptance.')
    save()
    print(json.dumps({name: row['variants'] for name, row in value['scenarios'].items()}))


if __name__ == '__main__':
    main()
