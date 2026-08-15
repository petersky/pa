from __future__ import annotations

import asyncio
import logging
import time
from contextlib import suppress
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from pa.auth.middleware import get_principal_id, require_user
from pa.core.context import AppContext
from pa.core.contracts import Module
from pa.domain.notifications import (
    InteractionResponse,
    Notification,
    NotificationPriority,
    NotificationType,
)
from pa.notifications import NotificationConflict, NotificationService

router = APIRouter()
logger = logging.getLogger(__name__)


class IdempotentMutation(BaseModel):
    idempotency_key: str = Field(min_length=1, max_length=300)


def _service(request: Request) -> NotificationService:
    return request.app.state.ctx.require_service("notifications")


def _realms(request: Request) -> set[str]:
    ctx = request.app.state.ctx
    subscribed = set(ctx.settings.subscribed_realms)
    membership = ctx.services.get("membership")
    if not membership:
        return subscribed
    principal = _principal(request)
    membership_principal = (
        principal.removeprefix("user:") if principal.startswith("user:") else principal
    )
    return {
        realm_id
        for realm_id in subscribed
        if membership.has_role(realm_id, membership_principal)
    }


def _principal(request: Request) -> str:
    if getattr(request.state, "instance_authenticated", False):
        return request.headers.get("X-PA-Acting-Principal", "instance:fleet")[:300]
    require_user(request)
    return get_principal_id(request)


def _authorized_notice(request: Request, notification_id: str) -> Notification:
    try:
        return _service(request).get_authorized(
            notification_id, principal_id=_principal(request), realms=_realms(request)
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Notification not found") from exc


def _route_metadata(request: Request, item: Notification) -> dict[str, Any]:
    settings = request.app.state.ctx.settings
    local = not item.owner_instance_id or item.owner_instance_id == settings.instance_id
    destination = item.destination_url or item.source_url
    if not destination and item.card_id:
        destination = f"/work?card={item.card_id}"
    elif not destination and item.session_id:
        destination = f"/agent?session={item.session_id}"
    elif not destination and item.pr_number:
        destination = item.source_url
    elif not destination and item.project_id:
        destination = f"/projects?project={item.project_id}"
    elif not destination and item.type == NotificationType.SYNC_CONFLICT:
        destination = "/fleet"
    elif not destination and item.type in {
        NotificationType.SECURITY,
        NotificationType.UPGRADE,
        NotificationType.SERVICE_HEALTH,
    }:
        destination = "/settings"
    owner_url = item.owner_url
    if not local and not owner_url:
        fleet = request.app.state.ctx.services.get("fleet_registry")
        member = fleet.get_instance(item.owner_instance_id) if fleet else None
        owner_url = member.url if member else None
    exact_remote = (
        f"{owner_url.rstrip('/')}{destination}"
        if owner_url and destination and destination.startswith("/")
        else destination or owner_url
    )
    return {
        "local_authority": local,
        "distributable": item.distributable,
        "destination": destination if local else exact_remote,
        "owner_instance_id": item.owner_instance_id,
        "owner_url": owner_url,
        "response_mode": "local"
        if local
        else ("proxy" if item.distributable else "remote"),
    }


@router.get("/notifications")
async def list_notifications(
    request: Request,
    realm: str | None = None,
    type: NotificationType | None = None,
    priority: NotificationPriority | None = None,
    unread: bool | None = None,
    outstanding: bool | None = None,
    resolved: bool | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    principal = _principal(request)
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    service = _service(request)
    authorized_realms = _realms(request)
    now = time.monotonic()
    last_expire = service._expire_mono.get(realm_id, 0.0)
    if realm_id in authorized_realms and now - last_expire >= 5.0:
        await service.expire_due(realm_id=realm_id)
        service._expire_mono[realm_id] = now
    runtime = request.app.state.ctx.services.get("async_runtime")
    list_kwargs = dict(
        principal_id=principal,
        realms=authorized_realms,
        realm_id=realm_id,
        notification_type=type.value if type else None,
        priority=priority.value if priority else None,
        unread=unread,
        outstanding=outstanding,
        resolved=resolved,
        limit=limit,
        offset=offset,
    )
    if runtime:
        records, outstanding_count = await runtime.run_blocking(
            "notifications.list_page", service.list_inbox, **list_kwargs
        )
    else:
        records, outstanding_count = await asyncio.to_thread(
            service.list_inbox, **list_kwargs
        )
    return {
        "items": [
            {**item.public_dict(), "routing": _route_metadata(request, item)}
            for item in records
        ],
        "offset": offset,
        "limit": limit,
        "next_offset": offset + len(records) if len(records) == limit else None,
        "outstanding_count": outstanding_count,
    }


@router.get("/notifications/{notification_id}")
def get_notification(request: Request, notification_id: str) -> dict[str, Any]:
    item = _authorized_notice(request, notification_id)
    return {
        **item.public_dict(),
        "routing": _route_metadata(request, item),
        "audit": request.app.state.ctx.store.list_notification_audit(item.id),
    }


@router.post("/notifications/{notification_id}/read")
def read_notification(
    request: Request, notification_id: str, body: IdempotentMutation
) -> dict[str, Any]:
    item = _authorized_notice(request, notification_id)
    result = _service(request).mark_read(
        item, principal_id=_principal(request), idempotency_key=body.idempotency_key
    )
    return result.public_dict()


@router.post("/notifications/{notification_id}/acknowledge")
def acknowledge_notification(
    request: Request, notification_id: str, body: IdempotentMutation
) -> dict[str, Any]:
    item = _authorized_notice(request, notification_id)
    result = _service(request).acknowledge(
        item, principal_id=_principal(request), idempotency_key=body.idempotency_key
    )
    return result.public_dict()


@router.post("/notifications/{notification_id}/resolve")
def resolve_notification(
    request: Request, notification_id: str, body: IdempotentMutation
) -> dict[str, Any]:
    item = _authorized_notice(request, notification_id)
    try:
        result = _service(request).resolve(
            item,
            principal_id=_principal(request),
            idempotency_key=body.idempotency_key,
        )
    except NotificationConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": str(exc)},
        ) from exc
    return result.public_dict()


async def _proxy_response(
    request: Request, item: Notification, body: InteractionResponse
) -> dict[str, Any]:
    routing = _route_metadata(request, item)
    owner_url = routing.get("owner_url")
    if not item.distributable:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "remote_authority_required",
                "message": "This response must be completed on the owning instance.",
                "destination": routing.get("destination"),
                "owner_instance_id": item.owner_instance_id,
            },
        )
    if not owner_url:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "owner_unreachable",
                "message": "The owning instance has no advertised reachable URL.",
                "owner_instance_id": item.owner_instance_id,
            },
        )
    settings = request.app.state.ctx.settings
    headers = {
        "Accept": "application/json",
        "X-PA-Origin-Instance-ID": settings.instance_id,
        "X-PA-Acting-Principal": _principal(request),
    }
    if settings.sync_token:
        headers["Authorization"] = f"Bearer {settings.sync_token}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                f"{owner_url.rstrip('/')}/api/notifications/{item.id}/respond",
                json=body.model_dump(mode="json"),
                headers=headers,
            )
        if response.status_code >= 400:
            try:
                detail = response.json().get("detail")
            except ValueError:
                detail = "Owning instance rejected the response"
            raise HTTPException(status_code=response.status_code, detail=detail)
        return response.json()
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "code": "owner_unreachable",
                "message": "The owning instance is currently unreachable.",
                "destination": routing.get("destination"),
                "owner_instance_id": item.owner_instance_id,
            },
        ) from exc


@router.post("/notifications/{notification_id}/respond")
async def respond_notification(
    request: Request, notification_id: str, body: InteractionResponse
) -> dict[str, Any]:
    item = _authorized_notice(request, notification_id)
    if (
        item.owner_instance_id
        and item.owner_instance_id != request.app.state.ctx.settings.instance_id
    ):
        return await _proxy_response(request, item, body)
    try:
        result = await _service(request).respond(
            item, body, principal_id=_principal(request)
        )
    except NotificationConflict as exc:
        status = (
            422
            if exc.code
            in {"invalid_choice", "response_validation_failed", "freeform_not_allowed"}
            else 409
        )
        raise HTTPException(
            status_code=status,
            detail={
                "code": exc.code,
                "message": str(exc),
                "notification": exc.notification.public_dict()
                if exc.notification
                else None,
            },
        ) from exc
    return result.public_dict()


class NotificationsModule(Module):
    @property
    def name(self) -> str:
        return "notifications"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Durable fleet notifications and correlated user interactions"

    def on_load(self, ctx: AppContext) -> None:
        ctx.register_service("notifications", NotificationService(ctx))
        self._maintenance_task: asyncio.Task[None] | None = None
        self._maintenance_stop: asyncio.Event | None = None

    async def on_startup(self, app, ctx: AppContext) -> None:
        service: NotificationService = ctx.require_service("notifications")
        self._maintenance_stop = asyncio.Event()

        async def maintain() -> None:
            iteration = 0
            while self._maintenance_stop and not self._maintenance_stop.is_set():
                for realm_id in ctx.settings.subscribed_realms:
                    try:
                        await service.expire_due(realm_id=realm_id)
                        if iteration % 120 == 0:
                            await asyncio.to_thread(service.prune, realm_id=realm_id)
                    except Exception:
                        logger.exception(
                            "Notification maintenance failed for realm %s", realm_id
                        )
                iteration += 1
                try:
                    await asyncio.wait_for(self._maintenance_stop.wait(), timeout=30)
                except TimeoutError:
                    pass

        self._maintenance_task = asyncio.create_task(
            maintain(), name="notification-maintenance"
        )

    async def on_shutdown(self, app, ctx: AppContext) -> None:
        if self._maintenance_stop:
            self._maintenance_stop.set()
        if self._maintenance_task:
            self._maintenance_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._maintenance_task

    def api_routers(self):
        return [("/api", router, ["notifications", "interactions"])]

    def register_mcp(self, mcp, ctx: AppContext) -> None:
        from pa.mcp.local_api import request_local_pa

        @mcp.tool()
        def list_notifications(
            realm: str | None = None,
            type: str | None = None,
            priority: str | None = None,
            unread: bool | None = None,
            outstanding: bool | None = None,
            resolved: bool | None = None,
            limit: int = 50,
            offset: int = 0,
        ) -> dict:
            """List authorized fleet notifications with filters and pagination."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/notifications",
                params={
                    "realm": realm,
                    "type": type,
                    "priority": priority,
                    "unread": unread,
                    "outstanding": outstanding,
                    "resolved": resolved,
                    "limit": limit,
                    "offset": offset,
                },
            )

        @mcp.tool()
        def get_notification(notification_id: str) -> dict | None:
            """View one authorized notification, routing metadata, and audit trail."""
            return request_local_pa(
                ctx.settings,
                "GET",
                f"/api/notifications/{notification_id}",
                allow_not_found=True,
            )

        @mcp.tool()
        def acknowledge_notification(
            notification_id: str, idempotency_key: str
        ) -> dict:
            """Idempotently acknowledge a notification."""
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/notifications/{notification_id}/acknowledge",
                json={"idempotency_key": idempotency_key},
            )

        @mcp.tool()
        def resolve_notification(notification_id: str, idempotency_key: str) -> dict:
            """Idempotently resolve a notification when authorized."""
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/notifications/{notification_id}/resolve",
                json={"idempotency_key": idempotency_key},
            )

        @mcp.tool()
        def respond_notification(
            notification_id: str,
            idempotency_key: str,
            choice_id: str | None = None,
            value: str | None = None,
            fields: dict[str, Any] | None = None,
            cancel: bool = False,
        ) -> dict:
            """Answer a durable interaction by choice, freeform value, fields, or cancellation."""
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/notifications/{notification_id}/respond",
                json={
                    "idempotency_key": idempotency_key,
                    "choice_id": choice_id,
                    "value": value,
                    "fields": fields,
                    "cancel": cancel,
                },
            )
