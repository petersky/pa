from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from pa.config import Settings
from pa.domain.card_summary_service import CardSummaryService, sanitize_summary
from pa.domain.models import CardCreate, CardUpdate
from pa.domain.projection import CardProjection


def context(tmp: str, provider):
    settings = Settings(
        data_dir=Path(tmp),
        card_summary_api_key="test",
        card_summary_max_retries=1,
    )
    return SimpleNamespace(
        settings=settings,
        store=CardProjection(Path(tmp) / "pa.db"),
    ), provider


async def _semantic_summary_uses_full_input_and_persists_provenance() -> None:
    seen = {}

    async def provider(title, body):
        seen.update(title=title, body=body)
        return "Replace mechanical excerpts with a concise semantic summary while keeping card writes responsive."

    with tempfile.TemporaryDirectory() as tmp:
        ctx, call = context(tmp, provider)
        body = "setup " * 500 + "The outcome is at the end."
        card = ctx.store.create_card(CardCreate(title="Summaries", body=body))
        await CardSummaryService(ctx, provider_call=call).generate(
            card.id, card.realm_id
        )
        ready = ctx.store.get_card(card.id)
        assert ready is not None
        assert seen == {"title": "Summaries", "body": body}
        assert ready.summary_status.value == "ready"
        assert ready.summary_source.value == "agent"
        assert (
            ready.summary_input_hash
            and ready.summary_prompt_version == "card-summary-v1"
        )
        assert not ready.summary.startswith(body[:40])


def test_semantic_summary_uses_full_input_and_persists_provenance() -> None:
    asyncio.run(_semantic_summary_uses_full_input_and_persists_provenance())


async def _old_completion_cannot_overwrite_edited_card() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def provider(title, body):
        started.set()
        await release.wait()
        return "This result belongs to the old description."

    with tempfile.TemporaryDirectory() as tmp:
        ctx, call = context(tmp, provider)
        card = ctx.store.create_card(CardCreate(title="Race", body="old"))
        service = CardSummaryService(ctx, provider_call=call)
        task = asyncio.create_task(service.generate(card.id, card.realm_id))
        await started.wait()
        ctx.store.update_card(card.id, CardUpdate(body="new"))
        release.set()
        await task
        current = ctx.store.get_card(card.id)
        assert current is not None
        assert current.body == "new"
        assert current.summary != "This result belongs to the old description."
        assert current.summary_status.value == "stale"


def test_old_completion_cannot_overwrite_edited_card() -> None:
    asyncio.run(_old_completion_cannot_overwrite_edited_card())


async def _failure_is_truthful_and_retries_are_bounded() -> None:
    calls = 0

    async def provider(title, body):
        nonlocal calls
        calls += 1
        raise TimeoutError("slow provider")

    with tempfile.TemporaryDirectory() as tmp:
        ctx, call = context(tmp, provider)
        card = ctx.store.create_card(CardCreate(title="Failure", body="details"))
        await CardSummaryService(ctx, provider_call=call).generate(
            card.id, card.realm_id
        )
        failed = ctx.store.get_card(card.id)
        assert failed is not None
        assert calls == 2
        assert failed.summary == ""
        assert failed.summary_status.value == "failed"
        assert "TimeoutError" in (failed.summary_failure or "")


def test_failure_is_truthful_and_retries_are_bounded() -> None:
    asyncio.run(_failure_is_truthful_and_retries_are_bounded())


def test_contract_rejects_enumeration_and_overlong_output() -> None:
    assert sanitize_summary("A concise result. A key constraint remains.")
    with pytest.raises(ValueError):
        sanitize_summary("One. Two. Three. Four.")
    with pytest.raises(ValueError):
        sanitize_summary("x" * 601)


async def _fleet_member_does_not_run_legacy_migration() -> None:
    async def provider(title, body):
        raise AssertionError("fleet member must not schedule migration summaries")

    with tempfile.TemporaryDirectory() as tmp:
        ctx, call = context(tmp, provider)
        ctx.settings.fleet_owner_url = "http://fleet-owner.example"
        card = ctx.store.create_card(CardCreate(title="Legacy", body="details"))
        service = CardSummaryService(ctx, provider_call=call)

        migrated = await service.migrate_legacy()

        current = ctx.store.get_card(card.id)
        assert migrated == 0
        assert current is not None
        assert current.summary_source.value == "fallback"
        assert not service._tasks


def test_fleet_member_does_not_run_legacy_migration() -> None:
    asyncio.run(_fleet_member_does_not_run_legacy_migration())
