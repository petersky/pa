"""Canonical web, Telegram, and Discord goal-intake surfaces."""

from __future__ import annotations

import asyncio
import json
from typing import Annotated, Any
from uuid import uuid4

from fastapi import APIRouter, Header, HTTPException, Request, Response

from pa.auth.middleware import get_principal_id
from pa.core.context import AppContext
from pa.core.contracts import Module
from pa.intake.adapters import AdapterError, DiscordAdapter, TelegramAdapter
from pa.intake.models import (
    Channel,
    CorrelatedResponseCreate,
    IntakeMutationContext,
    LinkChallenge,
    LinkVerification,
    ReceiptCreate,
    RedactionCreate,
    RepresentationCreate,
    WebIntakeCreate,
)
from pa.intake.security import verify_discord_signature, verify_telegram_secret
from pa.intake.service import IntakeConflict, IntakeRejected, IntakeService
from pa.intake.transports import ChannelTransportError, DiscordGateway

router = APIRouter(prefix="/intake")
MAX_WEBHOOK_BYTES = 2 * 1024 * 1024


def _service(request: Request) -> IntakeService:
    return request.app.state.ctx.require_service("intake_service")


def _context(
    request: Request,
    idempotency_key: str,
    *,
    expected_version: int | None = None,
    actor: str | None = None,
) -> IntakeMutationContext:
    return IntakeMutationContext(
        actor_principal=actor or get_principal_id(request),
        authority_instance_id=request.app.state.ctx.settings.instance_id,
        idempotency_key=idempotency_key,
        expected_version=expected_version,
    )


def _run(call):
    try:
        return call()
    except KeyError as exc:
        raise HTTPException(
            status_code=404, detail="intake envelope not found"
        ) from exc
    except IntakeRejected as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail={"code": exc.code, "message": str(exc)},
        ) from exc
    except IntakeConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ChannelTransportError as exc:
        raise HTTPException(
            status_code=502,
            detail={"code": "channel_delivery_failed", "message": str(exc)},
        ) from exc
    except (AdapterError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("")
def list_intake(
    request: Request,
    realm: str | None = None,
    channel: Channel | None = None,
    correlation_id: str | None = None,
    limit: int = 100,
):
    return [
        item.model_dump(mode="json")
        for item in _service(request).list(
            realm_id=realm,
            channel=channel,
            correlation_id=correlation_id,
            limit=limit,
        )
    ]


@router.get("/capabilities")
def intake_capabilities(request: Request) -> dict[str, Any]:
    settings = request.app.state.ctx.settings
    return {
        "schema_version": 1,
        "channels": {
            "web": {"enabled": True, "transport": "native"},
            "telegram": {
                "enabled": bool(
                    settings.telegram_bot_token
                    and settings.telegram_webhook_secret
                    and settings.telegram_webhook_url
                ),
                "transport": "signed_webhook",
            },
            "discord": {
                "enabled": bool(settings.discord_bot_token),
                "transport": "gateway",
                "signed_event_webhook": bool(settings.discord_application_public_key),
            },
        },
        "modalities": ["text", "image", "voice", "audio", "video", "file", "reaction"],
        "maximum_event_bytes": settings.intake_max_event_bytes,
        "maximum_artifact_bytes": settings.intake_max_artifact_bytes,
        "identity_linking": "one_time_code",
        "delivery_receipts": True,
        "correlated_responses": True,
    }


@router.get("/{envelope_id}")
def get_intake(request: Request, envelope_id: str):
    item = _service(request).get(envelope_id)
    if not item:
        raise HTTPException(status_code=404, detail="intake envelope not found")
    return item.model_dump(mode="json")


@router.post("/web", status_code=201)
def create_web_intake(
    request: Request,
    body: WebIntakeCreate,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
):
    if not body.message.strip():
        raise HTTPException(status_code=400, detail="message is required")
    principal = get_principal_id(request)
    message_id = body.channel_message_id or idempotency_key
    item = _service(request).ingest_web_prompt(
        principal_id=principal,
        session_id=body.conversation_id,
        message=body.message.strip(),
        images=[],
        realm_id=body.realm_id,
        project_id=body.project_id,
        goal_ids=body.goal_ids,
        channel_message_id=message_id,
        context=_context(request, idempotency_key),
    )
    return item.model_dump(mode="json")


@router.post("/webhooks/telegram")
async def telegram_webhook(request: Request):
    raw = await request.body()
    settings = request.app.state.ctx.settings
    if len(raw) > min(MAX_WEBHOOK_BYTES, settings.intake_max_event_bytes):
        raise HTTPException(
            status_code=413, detail="Telegram webhook payload too large"
        )
    if not verify_telegram_secret(
        settings.telegram_webhook_secret,
        request.headers.get("x-telegram-bot-api-secret-token"),
    ):
        raise HTTPException(status_code=401, detail="Invalid Telegram webhook secret")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON") from exc
    try:
        envelope = TelegramAdapter().normalize(payload, realm_id=settings.primary_realm)
    except AdapterError:
        return {"accepted": False, "ignored": True}
    key = f"telegram:{payload.get('update_id') or envelope.id}"
    link_code = _link_code(envelope.text)
    if link_code:
        binding = _run(
            lambda: _service(request).verify_link(
                channel=Channel.TELEGRAM,
                code=link_code,
                channel_user_id=envelope.sender.channel_user_id,
                conversation_id=envelope.thread.conversation_id,
                context=_context(request, key, actor="channel:telegram"),
            )
        )
        return {
            "accepted": True,
            "identity_linked": True,
            "principal_id": binding.principal_id,
        }
    item = await asyncio.to_thread(
        _run,
        lambda: _service(request).ingest(
            envelope,
            _context(request, key, actor="channel:telegram"),
            raw_payload=raw,
        ),
    )
    return {
        "accepted": True,
        "intake_id": item.id,
        "correlation_id": item.correlation_id,
        "disposition": item.security.disposition,
    }


@router.post("/webhooks/discord")
async def discord_event_webhook(request: Request):
    raw = await request.body()
    settings = request.app.state.ctx.settings
    if len(raw) > min(MAX_WEBHOOK_BYTES, settings.intake_max_event_bytes):
        raise HTTPException(status_code=413, detail="Discord webhook payload too large")
    if not verify_discord_signature(
        settings.discord_application_public_key,
        request.headers.get("x-signature-timestamp"),
        raw,
        request.headers.get("x-signature-ed25519"),
    ):
        raise HTTPException(status_code=401, detail="Invalid Discord webhook signature")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON") from exc
    if payload.get("type") == 0:
        return Response(status_code=204, media_type="application/json")
    # Discord's Event Webhook currently carries application/Social SDK events.
    # Ordinary messages and reactions arrive through the managed Gateway below.
    return Response(status_code=204, media_type="application/json")


@router.post("/links")
def create_link(
    request: Request,
    body: LinkChallenge,
):
    result = _service(request).begin_link(
        principal_id=get_principal_id(request),
        channel=body.channel,
        realm_id=body.realm_id,
        expires_in_seconds=body.expires_in_seconds,
    )
    return result.model_dump(mode="json")


@router.post("/links/verify")
def verify_link(
    request: Request,
    body: LinkVerification,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
):
    binding = _run(
        lambda: _service(request).verify_link(
            channel=body.channel,
            code=body.code,
            channel_user_id=body.channel_user_id,
            conversation_id=body.conversation_id,
            context=_context(request, idempotency_key),
        )
    )
    return binding.model_dump(mode="json")


@router.post("/{envelope_id}/responses", status_code=201)
def respond_to_intake(
    request: Request,
    envelope_id: str,
    body: CorrelatedResponseCreate,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
):
    item = _run(
        lambda: _service(request).send_response(
            envelope_id, body, _context(request, idempotency_key)
        )
    )
    return item.model_dump(mode="json")


@router.post("/{envelope_id}/receipts")
def add_receipt(
    request: Request,
    envelope_id: str,
    body: ReceiptCreate,
    expected_version: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
):
    item = _run(
        lambda: _service(request).record_receipt(
            envelope_id,
            body,
            _context(request, idempotency_key, expected_version=expected_version),
        )
    )
    return item.model_dump(mode="json")


@router.post("/{envelope_id}/representations")
def add_representation(
    request: Request,
    envelope_id: str,
    body: RepresentationCreate,
    expected_version: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
):
    item = _run(
        lambda: _service(request).add_representation(
            envelope_id,
            body.representation,
            _context(request, idempotency_key, expected_version=expected_version),
        )
    )
    return item.model_dump(mode="json")


@router.post("/{envelope_id}/redact")
def redact_intake(
    request: Request,
    envelope_id: str,
    body: RedactionCreate,
    expected_version: int,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
):
    item = _run(
        lambda: _service(request).redact(
            envelope_id,
            body,
            _context(request, idempotency_key, expected_version=expected_version),
        )
    )
    return item.model_dump(mode="json")


@router.post("/retention/run")
def run_retention(request: Request, limit: int = 500):
    return _service(request).retention_sweep(limit=limit)


def _link_code(text: str | None) -> str | None:
    parts = (text or "").strip().split()
    if len(parts) == 2 and parts[0].split("@", 1)[0].lower() == "/link":
        return parts[1]
    return None


class IntakeModule(Module):
    def __init__(self) -> None:
        self._gateway: DiscordGateway | None = None

    @property
    def name(self) -> str:
        return "intake"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Canonical multichannel multimodal intake, identity, and delivery"

    def on_load(self, ctx: AppContext) -> None:
        ctx.register_service("intake_service", IntakeService(ctx.store, ctx.settings))

    async def on_startup(self, app, ctx: AppContext) -> None:
        service: IntakeService = ctx.require_service("intake_service")
        if (
            ctx.settings.telegram_bot_token
            and ctx.settings.telegram_webhook_secret
            and ctx.settings.telegram_webhook_url
        ):
            await asyncio.to_thread(
                service.transport.configure_telegram_webhook,
                ctx.settings.telegram_webhook_url,
                ctx.settings.telegram_webhook_secret,
            )
        if not ctx.settings.discord_bot_token:
            return
        adapter = DiscordAdapter()

        async def handle(
            event_name: str, payload: dict[str, Any], sequence: int | None
        ) -> None:
            try:
                envelope = adapter.normalize_gateway(
                    event_name,
                    payload,
                    sequence=sequence,
                    realm_id=ctx.settings.primary_realm,
                )
                key = f"discord:{sequence if sequence is not None else envelope.id}:{event_name}"
                link_code = _link_code(envelope.text)
                if link_code:
                    await asyncio.to_thread(
                        service.verify_link,
                        channel=Channel.DISCORD,
                        code=link_code,
                        channel_user_id=envelope.sender.channel_user_id,
                        conversation_id=envelope.thread.conversation_id,
                        context=IntakeMutationContext(
                            actor_principal="channel:discord",
                            authority_instance_id=ctx.settings.instance_id,
                            idempotency_key=key,
                        ),
                    )
                    return
                raw = json.dumps(
                    {"event": event_name, "sequence": sequence, "data": payload},
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
                await asyncio.to_thread(
                    service.ingest,
                    envelope,
                    IntakeMutationContext(
                        actor_principal="channel:discord",
                        authority_instance_id=ctx.settings.instance_id,
                        idempotency_key=key,
                    ),
                    raw_payload=raw,
                )
            except (
                AdapterError,
                IntakeRejected,
                IntakeConflict,
                ChannelTransportError,
            ):
                # Rejected provider input remains outside the durable domain.
                return

        self._gateway = DiscordGateway(ctx.settings.discord_bot_token, handle)
        self._gateway.start()

    async def on_shutdown(self, app, ctx: AppContext) -> None:
        if self._gateway:
            await self._gateway.stop()
        ctx.require_service("intake_service").close()

    def api_routers(self):
        return [("/api", router, ["intake"])]

    def register_mcp(self, mcp, ctx: AppContext) -> None:
        from pa.mcp.local_api import request_local_pa

        @mcp.tool()
        def intake_capability() -> dict:
            """Report configured canonical channel capabilities without secrets."""
            return request_local_pa(ctx.settings, "GET", "/api/intake/capabilities")

        @mcp.tool()
        def list_intake(
            realm: str | None = None,
            channel: Channel | None = None,
            correlation_id: str | None = None,
            limit: int = 100,
        ) -> list[dict]:
            """List bounded canonical intake envelopes."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/intake",
                params={
                    "realm": realm,
                    "channel": channel.value if channel else None,
                    "correlation_id": correlation_id,
                    "limit": limit,
                },
            )

        @mcp.tool()
        def get_intake(envelope_id: str) -> dict:
            """Get one canonical intake envelope and delivery history."""
            return request_local_pa(ctx.settings, "GET", f"/api/intake/{envelope_id}")

        @mcp.tool()
        def create_intake_link(
            channel: Channel,
            realm_id: str = "default",
            expires_in_seconds: int = 600,
        ) -> dict:
            """Create a short-lived one-time channel identity link code."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/intake/links",
                json={
                    "channel": channel.value,
                    "realm_id": realm_id,
                    "expires_in_seconds": expires_in_seconds,
                },
                idempotency_key=f"intake-link:{uuid4()}",
            )
