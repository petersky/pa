"""All scheduling tools proxy the owning server; stdio never opens the ledger."""
from typing import Any
from pa.scheduling.store import Schedule, Trigger


def register_mcp(mcp, ctx):
    from pa.mcp.local_api import request_local_pa

    @mcp.tool()
    def list_schedules(cursor: str | None = None, limit: int = 50) -> dict[str, Any]:
        """List one bounded page of schedules/status; pass next_cursor to continue."""
        return request_local_pa(ctx.settings, 'GET', '/api/schedules', params={'limit':limit, **({'cursor':cursor} if cursor else {})})

    @mcp.tool()
    def schedule_history(schedule_id: str) -> list[dict[str, Any]]:
        """Inspect bounded occurrence history, failures and retry times."""
        from urllib.parse import quote
        return request_local_pa(ctx.settings, 'GET', f'/api/schedules/{quote(schedule_id, safe="")}/history')

    @mcp.tool()
    def preview_schedule(trigger: Trigger) -> dict[str, Any]:
        """Preview the next ten times in the trigger timezone."""
        return request_local_pa(ctx.settings, 'POST', '/api/schedules/preview', json=trigger.model_dump(mode='json'))

    @mcp.tool()
    def schedule_command(action: str, idempotency_key: str, schedule_id: str | None = None,
                         occurrence_id: str | None = None, version: int | None = None,
                         schedule: Schedule | None = None) -> dict[str, Any]:
        """Create/update/pause/resume/cancel/delete/fire/retry/webhook. Delivered means accepted, never task completion.

        Update/pause/resume/delete require the current version. Reuse the command
        key after an unknown outcome. Busy agents queue unless interrupt is explicit.
        """
        if action == 'create' and schedule is not None:
            import os
            session_id = os.environ.get('PA_BROWSER_SESSION_ID', '').strip()
            if session_id and not (schedule.context_refs.get('session') or schedule.context_refs.get('session_id')):
                schedule = schedule.model_copy(update={'context_refs':{**schedule.context_refs, 'session':session_id}})
        return request_local_pa(ctx.settings, 'POST', '/api/schedules', json={
            'action':action, 'idempotency_key':idempotency_key, 'schedule_id':schedule_id,
            'occurrence_id':occurrence_id, 'version':version,
            'schedule':schedule.model_dump(mode='json') if schedule else None})
