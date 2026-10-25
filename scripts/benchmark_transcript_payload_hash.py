"""Private comparison of one repeated digest, preserving complete payload reads."""
import argparse, hashlib, inspect, json, os, random, tempfile, textwrap, time, zlib
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

p = argparse.ArgumentParser()
p.add_argument('--output', type=Path, required=True)
args = p.parse_args()
assert not args.output.exists()
started = datetime.now(UTC).isoformat()
with tempfile.TemporaryDirectory(prefix='pa-payload-hash-work-') as tmp:
    os.environ.update(PA_DATA_DIR=str(Path(tmp)/'data'), PA_WORKSPACE_ROOT=str(Path(tmp)/'workspaces'), PA_AGENT_ENABLED='false')
    import pa.domain.transcript_storage as module
    from pa.domain.transcript_storage import TranscriptStorage
    source = textwrap.dedent(inspect.getsource(TranscriptStorage._payload))
    old = 'if hashlib.sha256(raw).hexdigest() != digest or hashlib.sha256(raw).hexdigest() != row["payload_hash"]:'
    new = 'content_hash = hashlib.sha256(raw).hexdigest()\n    if content_hash != digest or content_hash != row["payload_hash"]:'
    assert source.count(new) == 1
    baseline_source = source.replace(new, old)
    namespace = dict(vars(module))
    exec(baseline_source, namespace)
    baseline = namespace['_payload']
    candidate = TranscriptStorage._payload
    store = TranscriptStorage(Path(tmp)/'transcripts.db')
    rng = random.Random(20261025)
    pairs = []
    original_sha = hashlib.sha256
    for size in (8192, 131072):
        payload = {'text': ''.join(rng.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=size))}
        raw = json.dumps(payload, separators=(',', ':')).encode()
        digest = original_sha(raw).hexdigest()
        path = store._object_path(digest)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(zlib.compress(raw))
        row = {'hot_payload': '{}', 'cold_hash': digest, 'payload_hash': digest}
        for pair in range(5):
            times = {}
            for label in (('baseline', 'candidate') if pair % 2 == 0 else ('candidate', 'baseline')):
                method = baseline if label == 'baseline' else candidate
                calls = total = 0
                def counted(data):
                    global calls, total
                    calls += 1
                    total += len(data)
                    return original_sha(data)
                with patch.object(module.hashlib, 'sha256', counted):
                    wall, cpu = time.perf_counter_ns(), time.thread_time_ns()
                    for _ in range(200):
                        assert method(store, row) == payload
                    times[label] = dict(wall_ms=(time.perf_counter_ns()-wall)/1e6,
                        thread_cpu_ms=(time.thread_time_ns()-cpu)/1e6, hash_calls=calls, hashed_bytes=total)
            pairs.append(dict(payload_bytes=len(raw), reads=200, pair=pair, measurements=times))
    # Both independent digest identities, missing bytes, decompression failure,
    # valid hash with invalid JSON and hot-only reads keep the original result.
    cases = []
    for label, raw_bytes, cold, expected_hash in [
        ('valid', b'{"text":"valid"}', None, None),
        ('payload_hash_mismatch', b'{"text":"valid"}', None, '0'*64),
        ('object_hash_mismatch', b'{"text":"valid"}', '1'*64, None),
        ('invalid_json', b'not json', None, None),
    ]:
        h = original_sha(raw_bytes).hexdigest()
        cold = cold or h
        path = store._object_path(cold); path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(zlib.compress(raw_bytes))
        row = {'hot_payload':'{}', 'cold_hash':cold, 'payload_hash':expected_hash or h}
        def outcome(method):
            try: return {'result': method(store, row)}
            except Exception as exc: return {'error_type':type(exc).__name__}
        a, b = outcome(baseline), outcome(candidate)
        assert a == b
        cases.append(dict(case=label, equal=True, outcome=a))
    for label, row in [('bad_zlib', row), ('missing', dict(row, cold_hash='2'*64)), ('hot', dict(row, cold_hash=None))]:
        if label == 'bad_zlib': store._object_path(row['cold_hash']).write_bytes(b'bad zlib')
        a, b = outcome(baseline), outcome(candidate)
        assert a == b
        cases.append(dict(case=label, equal=True, outcome=a))
out = dict(contract='pa.transcript-payload-digest-work/v1', started_at=started,
    finished_at=datetime.now(UTC).isoformat(), baseline_source_sha256=original_sha(baseline_source.encode()).hexdigest(),
    baseline_source=baseline_source, candidate_source=source, pairs=pairs, parity=cases,
    scope='Private synthetic cached-file reads with both roots private before imports. Five alternating pairs per size, 200 reads per arm. Full decompression, hash comparisons, JSON parsing and result equality retained. Counts prove removed duplicate digest work; timing includes observer overhead and is not production attribution or statistical whole-path acceptance.')
args.output.write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps(dict(output=str(args.output), pairs=len(pairs), parity=len(cases))))
