"""Phase 3 limbic appraisal and tiered-memory public surfaces."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from pa.core.context import AppContext
from pa.core.contracts import Module
from pa.limbic.appraisal import LimbicService
from pa.limbic.memory import MemoryConflict, MemoryService
from pa.limbic.models import (
    AppraisalResult,
    MemoryMutationContext,
    MemoryQuery,
    MemoryRecord,
    ReplayCase,
    ReplayReport,
    RetrievedMemory,
    SignalEnvelope,
    WorkingMemoryPacket,
)

router = APIRouter()


class AppraiseRequest(BaseModel):
    signal: SignalEnvelope
    shadow_mode: bool = False


class ReplayRequest(BaseModel):
    cases: list[ReplayCase]


class MemoryWriteRequest(BaseModel):
    record: MemoryRecord


def _limbic(request: Request) -> LimbicService:
    return request.app.state.ctx.require_service("limbic_service")


def _memory(request: Request) -> MemoryService:
    return request.app.state.ctx.require_service("memory_service")


@router.post("/limbic/appraise", response_model=AppraisalResult)
def appraise_signal(request: Request, body: AppraiseRequest) -> AppraisalResult:
    return _limbic(request).appraise(body.signal, shadow_mode=body.shadow_mode)


@router.post("/limbic/replay", response_model=ReplayReport)
def evaluate_replay(request: Request, body: ReplayRequest) -> ReplayReport:
    return _limbic(request).evaluate(body.cases)


@router.post("/memory", response_model=MemoryRecord, status_code=201)
def remember(
    request: Request,
    body: MemoryWriteRequest,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    actor: Annotated[str, Header(alias="X-PA-Actor")] = "user:local",
    authority: Annotated[
        str | None, Header(alias="X-PA-Authority-Instance")
    ] = None,
) -> MemoryRecord:
    context = MemoryMutationContext(
        actor_principal=actor,
        authority_instance_id=authority or request.app.state.ctx.settings.instance_id,
        idempotency_key=idempotency_key,
    )
    try:
        return _memory(request).remember(body.record, context)
    except MemoryConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/memory/{record_id}", response_model=MemoryRecord)
def get_memory(request: Request, record_id: str) -> MemoryRecord:
    record = _memory(request).get(record_id)
    if not record:
        raise HTTPException(status_code=404, detail="memory record not found")
    return record


@router.post("/memory/retrieve", response_model=list[RetrievedMemory])
def retrieve_memory(request: Request, body: MemoryQuery) -> list[RetrievedMemory]:
    return _memory(request).retrieve(body)


@router.post("/memory/working-packet", response_model=WorkingMemoryPacket)
def working_memory_packet(
    request: Request, body: MemoryQuery
) -> WorkingMemoryPacket:
    return _memory(request).working_packet(body)


class LimbicModule(Module):
    @property
    def name(self) -> str:
        return "limbic"

    @property
    def description(self) -> str:
        return "Canonical appraisal, fast/slow routing, and scoped tiered memory"

    def on_load(self, ctx: AppContext) -> None:
        ctx.register_service(
            "limbic_service", LimbicService(ctx.store, ctx.settings.instance_id)
        )
        ctx.register_service(
            "memory_service", MemoryService(ctx.store, ctx.settings.instance_id)
        )

    def api_routers(self):
        return [("/api", router, ["limbic", "memory"])]

    def register_mcp(self, mcp, ctx: AppContext) -> None:
        from pa.mcp.local_api import request_local_pa

        @mcp.tool()
        def appraise_signal(signal: SignalEnvelope, shadow_mode: bool = False) -> dict:
            """Appraise one canonical signal and return its policy-gated route."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/limbic/appraise",
                json={
                    "signal": signal.model_dump(mode="json"),
                    "shadow_mode": shadow_mode,
                },
            )

        @mcp.tool()
        def evaluate_limbic_replay(cases: list[ReplayCase]) -> dict:
            """Replay redacted fixtures against the deterministic appraisal contract."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/limbic/replay",
                json={"cases": [case.model_dump(mode="json") for case in cases]},
            )

        @mcp.tool()
        def record_memory(
            record: MemoryRecord,
            idempotency_key: str,
            authority_instance_id: str,
            actor_principal: str = "user:local",
        ) -> dict:
            """Record attributable memory without overwriting prior facts."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/memory",
                headers={
                    "Idempotency-Key": idempotency_key,
                    "X-PA-Actor": actor_principal,
                    "X-PA-Authority-Instance": authority_instance_id,
                },
                json={"record": record.model_dump(mode="json")},
            )

        @mcp.tool()
        def retrieve_memory(query: MemoryQuery) -> list[dict]:
            """Retrieve memory within explicit realm, goal, principal, and sensitivity scope."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/memory/retrieve",
                json=query.model_dump(mode="json"),
            )

        @mcp.tool()
        def build_working_memory(query: MemoryQuery) -> dict:
            """Build a bounded reproducible working-memory packet."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/memory/working-packet",
                json=query.model_dump(mode="json"),
            )
