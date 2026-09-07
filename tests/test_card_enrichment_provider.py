"""Enrichment uses the same configured HTTP adapters as card summaries."""

import asyncio
import json
import tempfile
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from pa.domain.card_enrichment import enrichment_request
from pa.domain.card_summary_service import (
    CardSummaryService,
    SummaryConfiguration,
    SummaryFailureCode,
    SummaryProviderError,
    parse_chat_completion_summary,
)
from pa.domain.models import CardCreate
from tests.test_card_summary_service import _RecordingAsyncClient, context


def request_for(ctx):
    card = ctx.store.create_card(CardCreate(body="Investigate deployment failures"))
    request, _, _ = enrichment_request(
        ctx, card, {"body", "kind", "project_id", "preferred_capabilities", "tags"}
    )
    return card, request


@pytest.mark.parametrize("provider", ["openai", "anthropic", "minimax"])
def test_enrichment_uses_summary_transport_and_schema(provider):
    async def run():
        with tempfile.TemporaryDirectory() as tmp:
            ctx, _ = context(tmp, None)
            card, request = request_for(ctx)
            result = {"title": "Stabilize deployments"}
            if provider == "anthropic":
                response = {
                    "content": [
                        {"type": "tool_use", "name": request.tool_name, "input": result}
                    ]
                }
            elif provider == "minimax":
                response = {
                    "choices": [
                        {
                            "message": {
                                "tool_calls": [
                                    {
                                        "function": {
                                            "name": request.tool_name,
                                            "arguments": json.dumps(result),
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            else:
                response = {"choices": [{"message": {"content": json.dumps(result)}}]}
            calls = []
            config = SummaryConfiguration(
                enabled=True,
                provider=provider,
                model="configured-model",
                auth_source="dedicated_api_key",
                state="configured",
                base_url="https://provider.example/v1",
                api_key="test-key",
            )
            service = CardSummaryService(ctx)
            with patch(
                "pa.domain.card_summary_service.httpx.AsyncClient",
                return_value=_RecordingAsyncClient(
                    [httpx.Response(200, json=response)], calls
                ),
            ):
                output = await service._call_provider(
                    card.title, card.body, config, structured=request
                )
            assert json.loads(output) == result
            payload = calls[0]["json"]
            assert payload["model"] == config.model
            assert payload["messages"][-1] == request.messages[-1]
            assert "submit_summary" not in json.dumps(payload)
            if provider == "anthropic":
                assert payload["tools"][0]["input_schema"] == request.schema
                assert calls[0]["headers"]["x-api-key"] == "test-key"
            elif provider == "minimax":
                assert payload["tools"][0]["function"]["parameters"] == request.schema
            else:
                assert (
                    payload["response_format"]["json_schema"]["schema"]
                    == request.schema
                )

    asyncio.run(run())


def test_enrichment_retries_and_records_separate_selection():
    async def run():
        with tempfile.TemporaryDirectory() as tmp:
            ctx, _ = context(tmp, None)
            ctx.settings.card_summary_retry_base_seconds = 0
            card, request = request_for(ctx)
            service = CardSummaryService(ctx)
            call = AsyncMock(
                side_effect=[httpx.ConnectError("test"), '{"title":"Deployments"}']
            )
            with patch.object(service, "_call_provider", call):
                assert (
                    await service.suggest_card_fields(card, request)
                    == '{"title":"Deployments"}'
                )
            assert call.await_count == 2
            assert call.await_args.kwargs["structured"] is request
            from pa.execution.selection_jobs import summary_selection

            config = await service._configuration()
            first = summary_selection(
                ctx, card, config, input_hash="identical", prompt_version="same"
            )
            second = summary_selection(
                ctx,
                card,
                config,
                input_hash="identical",
                prompt_version="same",
                surface="card_enrichment",
            )
            assert first["decision_id"] != second["decision_id"]

    asyncio.run(run())


@pytest.mark.parametrize(
    "content", ['{"summary":"wrong schema"}', "plain text", '{"title":42}']
)
def test_enrichment_rejects_invalid_output(content):
    with tempfile.TemporaryDirectory() as tmp:
        ctx, _ = context(tmp, None)
        _, request = request_for(ctx)
        with pytest.raises(SummaryProviderError) as error:
            parse_chat_completion_summary(
                {"choices": [{"message": {"content": content}}]}, structured=request
            )
        assert error.value.code == SummaryFailureCode.SCHEMA_VIOLATION


def test_enrichment_tasks_are_deduplicated_and_closed_with_summary_service():
    async def run():
        with tempfile.TemporaryDirectory() as tmp:
            ctx, _ = context(tmp, None)
            card, _ = request_for(ctx)
            service = CardSummaryService(ctx)
            started = asyncio.Event()

            async def block(*args, **kwargs):
                started.set()
                await asyncio.Event().wait()

            with patch("pa.domain.card_enrichment.enrich_card", block):
                assert service.enqueue_enrichment(card, {"body"})
                assert not service.enqueue_enrichment(card, {"body"})
                await started.wait()
                task = next(iter(service._enrichment_tasks.values()))
                await service.close()
                assert task.cancelled()

    asyncio.run(run())
