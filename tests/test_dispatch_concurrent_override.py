from __future__ import annotations

import tempfile
from pathlib import Path

from pa.execution.dispatch import DispatchRecord, DispatchStore


def _record(key: str, target: str, *, allow: bool) -> DispatchRecord:
    return DispatchRecord(
        mutation_id=f"mutation-{key}",
        idempotency_key=key,
        request_fingerprint=key,
        card_id="card-1",
        authority_instance_id="authority",
        authority_url="http://authority.test",
        target_instance_id=target,
        allow_concurrent=allow,
    )


def test_explicit_override_allows_a_second_concurrent_card_dispatch() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp))
        first, first_duplicate = store.admit(
            _record("first", "one", allow=False)
        )
        second, second_duplicate = store.admit(
            _record("second", "two", allow=True)
        )
    assert not first_duplicate
    assert not second_duplicate
    assert first.dispatch_id != second.dispatch_id
