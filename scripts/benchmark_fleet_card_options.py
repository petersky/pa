"""Compare Fleet selector reads without touching live state or peer services."""
from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
import time
from pathlib import Path

from pa.core.performance import distribution
from pa.domain.models import CardCreate, CardLane
from pa.domain.projection import CardProjection


def fields(cards):
    return [(c.id, c.realm_id, c.kind.value, c.title, c.lane.value, c.project_id,
             c.created_at.isoformat(), c.updated_at.isoformat()) for c in cards]


def run(root: Path, count: int, iterations: int) -> dict:
    store = CardProjection(root / 'cards.db')
    for i in range(count):
        store.create_card(CardCreate(title=f'fixture {i:04d}', realm_id='default',
            lane=list(CardLane)[i % len(CardLane)], body='x' * 65536))
    samples = {'full': [], 'selector': []}
    body_sizes = {}
    expected = fields(store.list_cards(realm_id='default'))
    for i in range(iterations):
        for mode in (['full', 'selector'] if i % 2 else ['selector', 'full']):
            start = time.perf_counter()
            cards = (store.list_cards(realm_id='default') if mode == 'full'
                     else store.list_card_options(realm_id='default', recent_first=True))
            samples[mode].append((time.perf_counter() - start) * 1000)
            assert fields(cards) == expected
            body_sizes[mode] = sum(len(c.body) for c in cards)
    return {'scope': 'Disposable cards with64KiB bodies, alternating read order, exact '
            'identity/realm/kind/title/lane/project/timestamp/order parity. Isolates the '
            'selector phase; does not time complete Fleet context, template, peers or '
            'production. Normal GC retained; no durability/threshold changes.',
            'cards': count, 'iterations': iterations,
            'latency_ms': {k: distribution(v) for k, v in samples.items()},
            'samples_ms': samples, 'body_characters_materialized_per_call': body_sizes,
            'projection_source_sha256': hashlib.sha256(Path(__import__(
                'pa.domain.projection', fromlist=['']).__file__).read_bytes()).hexdigest()}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--cards', type=int, default=331)
    parser.add_argument('--iterations', type=int, default=30)
    args = parser.parse_args()
    if args.cards < 1 or args.iterations < 1:
        parser.error('cards and iterations must be positive')
    with tempfile.TemporaryDirectory(prefix='pa-fleet-card-options-') as tmp:
        result = run(Path(tmp), args.cards, args.iterations)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: v for k, v in result.items() if k != 'samples_ms'}))
