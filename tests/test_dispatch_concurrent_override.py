from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from pa.execution.dispatch import ConcurrentCardDispatch, DispatchRecord, DispatchStore


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


def test_same_session_resume_does_not_require_allow_concurrent() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp))
        first, _duplicate = store.admit(
            DispatchRecord(
                mutation_id="mutation-first",
                idempotency_key="first",
                request_fingerprint="first",
                card_id="card-1",
                authority_instance_id="authority",
                authority_url="http://authority.test",
                target_instance_id="one",
                session_id="session-live",
            )
        )
        second, duplicate = store.admit(
            DispatchRecord(
                mutation_id="mutation-resume",
                idempotency_key="resume",
                request_fingerprint="resume",
                card_id="card-1",
                authority_instance_id="authority",
                authority_url="http://authority.test",
                target_instance_id="one",
                session_id="session-live",
                resume_requested=True,
                resume_session_id="session-live",
            )
        )
    assert not duplicate
    assert first.dispatch_id != second.dispatch_id
    assert second.session_id == "session-live"


def test_second_session_on_the_same_card_still_requires_allow_concurrent() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp))
        store.admit(
            DispatchRecord(
                mutation_id="mutation-first",
                idempotency_key="first",
                request_fingerprint="first",
                card_id="card-1",
                authority_instance_id="authority",
                authority_url="http://authority.test",
                target_instance_id="one",
                session_id="session-a",
            )
        )
        with pytest.raises(ConcurrentCardDispatch):
            store.admit(
                DispatchRecord(
                    mutation_id="mutation-resume",
                    idempotency_key="resume",
                    request_fingerprint="resume",
                    card_id="card-1",
                    authority_instance_id="authority",
                    authority_url="http://authority.test",
                    target_instance_id="one",
                    resume_requested=True,
                    resume_session_id="session-b",
                    session_id="session-b",
                )
            )
