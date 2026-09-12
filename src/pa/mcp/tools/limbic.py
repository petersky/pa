"""limbic tools: authenticated owner API proxies."""

from __future__ import annotations

from pa.core.context import AppContext
from pa.limbic.models import MemoryQuery, MemoryRecord, ReplayCase, SignalEnvelope


def register_mcp(mcp, ctx: AppContext) -> None:
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
    def inspect_limbic_operations(realm_id: str = "default", limit: int = 500) -> dict:
        """Inspect redacted rollout quality, latency, fallback, and promotion metrics."""
        return request_local_pa(
            ctx.settings,
            "GET",
            "/api/limbic/operations",
            params={"realm_id": realm_id, "limit": limit},
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
