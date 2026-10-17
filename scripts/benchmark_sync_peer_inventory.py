"""Exact protocol-v3 one-delta negotiation against an in-memory peer."""
import argparse, asyncio, base64, json, sys, tempfile, time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
import httpx
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pa.core.performance import distribution
from pa.domain.models import CardEvent, EventType, SyncCommit
from pa.sync.object_store import object_hash
from tests.test_realm_convergence import _Node

async def run(root):
    n = _Node(root, 'probe', 'Probe')
    parents = []
    expected = {}
    stamp = datetime(2026, 1, 1, tzinfo=UTC)
    for i in range(1000):
        e = n.objects.put_json(CardEvent(id=f'event-{i}', timestamp=stamp, type=EventType.CARD_CREATED, realm_id='default', author_instance='probe', author_principal='user:local', payload={'fixture': i}).model_dump(mode='json'))
        h = n.objects.put_json(SyncCommit(hash='', realm_id='default', instance_id='probe', parent_hashes=parents, event_hashes=[e], timestamp=stamp).model_dump(mode='json'))
        parents = [h]
        expected.update({e: n.objects.get(e), h: n.objects.get(h)})
    new = {e, h}
    initial = set(expected) - new
    remote = set(initial)
    sent = set()
    requests = Counter()
    reads = Counter()
    original = n.objects.get
    latencies = []
    advertised = []

    def counted(key):
        reads[key] += 1
        return original(key)
    n.objects.get = counted

    async def request(method, url, *, payload=None, **kwargs):
        requests['need' if url.endswith('/need') else 'push'] += 1
        body = payload or {}
        if url.endswith('/need'):
            advertised.append(len(body['hashes']))
            data = {'protocol': 3, 'missing': [v for v in body['hashes'] if v not in remote], 'present_commits': [v for v in body['commit_hashes'] if v in remote]}
        else:
            for (key, value) in body.get('objects', {}).items():
                assert object_hash(base64.b64decode(value)) == key
                assert base64.b64decode(value) == expected[key]
                remote.add(key)
                sent.add(key)
            if body.get('head_hash'):
                assert remote == set(expected)
            data = {'head': body.get('head_hash') or None}
        return httpx.Response(200, json=data, request=httpx.Request(method, url))
    n.engine._request = request
    for _ in range(20):
        remote = set(initial)
        sent.clear()
        start = time.perf_counter()
        result = await n.engine._push_peer_v3('default', SimpleNamespace(target_url='http://synthetic-peer'), h, {})
        latencies.append((time.perf_counter() - start) * 1000)
        assert result['objects_sent'] == 2 and result['head'] == h and (sent == new)
    return {'scope': '20 exact protocol-v3 missing-one-commit exchanges over1000commits; in-memory peer with hash-checked imported bytes and complete closure; no external traffic or production writes', 'latency_ms': distribution(latencies), 'file_reads_total': sum(reads.values()), 'reads_per_exchange': sum(reads.values()) / 20, 'http_counts': dict(requests), 'advertised_object_counts': advertised, 'objects_sent_per_exchange': 2}
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='pa-peer-probe-') as temporary:
        result = asyncio.run(run(Path(temporary)))
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))
