"""Bounded proxies to the server-owned operational journal."""
from __future__ import annotations

from typing import Annotated
from pydantic import Field
from pa.health_journal.models import Observation, Assessment


def register_mcp(mcp, ctx):
    from pa.mcp.local_api import request_local_pa

    @mcp.tool()
    def report_pa_problem(idempotency_key: Annotated[str, Field(min_length=1, max_length=160)], observation: Observation) -> dict:
        """Record a sanitized PA failure. Reuse occurrence/correlation identity for updates.

        Exclude secrets, private answers and transcript dumps. Reporting failure
        must not recursively report itself or block the primary task.
        """
        from pa.health_journal.models import Observation
        body = Observation.model_validate(observation).model_dump()
        import json
        import os
        headers = {'Idempotency-Key': idempotency_key}
        execution = json.loads(os.environ.get('PA_EXECUTION_CONTEXT') or '{}')
        if execution.get('session_id') and not os.environ.get('PA_ASSIGNED_SERVICE_MODE'):
            headers['X-PA-Health-Session-ID'] = str(execution['session_id'])
        return request_local_pa(ctx.settings, 'POST', '/api/health-journal/reports',
                                json=body, headers=headers,
                                timeout_seconds=5)

    @mcp.tool()
    def list_pa_problems(cursor: str | None = None, limit: int = 32) -> dict:
        """Read a bounded journal page; gathered is delivery, not resolution."""
        return request_local_pa(ctx.settings, 'GET', '/api/health-journal/reports',
                                params={'cursor': cursor, 'limit': limit}, timeout_seconds=5)

    @mcp.tool()
    def get_pa_problem(report_id: str, before: int | None = None) -> dict:
        """Read immutable report history, custody and fix backlinks without repair."""
        from uuid import UUID
        return request_local_pa(ctx.settings, 'GET', f'/api/health-journal/reports/{UUID(report_id)}',
                                params={'before': before}, timeout_seconds=5)

    @mcp.tool()
    def update_pa_problem(group_id: str, assessment: Assessment) -> dict:
        """CAS a reasoned triage disposition. Production resolution requires authoritative acceptance."""
        from uuid import UUID
        from pa.health_journal.models import Assessment
        body = Assessment.model_validate(assessment).model_dump()
        return request_local_pa(ctx.settings, 'PATCH', f'/api/health-journal/groups/{UUID(group_id)}',
                                json=body, timeout_seconds=5)

    @mcp.tool()
    def health_journal_status() -> dict:
        """Read configured authority, lease, pending reports and collection failures."""
        return request_local_pa(ctx.settings, 'GET', '/api/health-journal/status', timeout_seconds=5)


    @mcp.tool()
    def get_pa_problem_group(group_id: str) -> dict:
        """Read current CAS version, untrusted evidence and auditable triage history."""
        from uuid import UUID
        return request_local_pa(ctx.settings, 'GET', f'/api/health-journal/groups/{UUID(group_id)}', timeout_seconds=5)
