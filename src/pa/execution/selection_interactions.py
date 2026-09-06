"""Selection recovery uses the existing durable notification/interaction contract."""

from __future__ import annotations

from pa.domain.notifications import (
    InteractionChoice,
    InteractionRequest,
    NotificationAction,
    NotificationCreate,
)


async def settings_blocked(runtime, pending):
    service = getattr(runtime.manager, "notification_service", None)
    if service is None:
        return None
    session = runtime.session
    correlation = f"selection-settings:{session.id}:{pending['id']}"
    notice = await runtime._offload(
        "selection.operator_interaction",
        service.create,
        NotificationCreate(
            realm_id=session.realm_id,
            principal_id=session.principal_id,
            visibility="principal" if session.principal_id else "realm",
            type="interaction",
            severity="warning",
            priority="high",
            title="Execution selection needs attention"
            if pending.get("code")
            else "Execution settings need correction",
            body=(
                pending.get(
                    "message", "The provider did not confirm the requested settings."
                )
                + " The queued prompt retains its identity and will not run with silently substituted settings. Inspect this session's selection controls and current routing policy, then submit a compatible correction or coordinate an explicit linked attempt. Cancellation cannot undo partial native application. Acknowledging this notice does not apply settings or resume the prompt."
            ),
            source_instance_id=runtime.settings.instance_id,
            owner_instance_id=runtime.settings.instance_id,
            card_id=session.card_id,
            session_id=session.id,
            dispatch_id=session.dispatch_id,
            project_id=session.project_id,
            deduplication_key=correlation,
            actions=[
                NotificationAction(
                    id="settings",
                    kind="navigate",
                    label="Inspect session settings",
                    href=f"/agent?session={session.id}",
                )
            ],
            interaction=InteractionRequest(
                id=correlation,
                kind="choice",
                protocol_method="pa.execution-settings/v1",
                protocol_request_id=correlation,
                continuation_mode="none",
                prompt="Inspect this session's failed settings and submit a corrected request before retrying the queued prompt.",
                choices=[
                    InteractionChoice(
                        id="acknowledged", label="I will inspect the settings"
                    )
                ],
            ),
        ),
        principal_id=session.principal_id or "user:local",
        instance_id=runtime.settings.instance_id,
    )
    return notice.id
