"""Explicit linked attempts; native context is never represented as continuous.

The source is durably fenced before another provider starts. A stable dispatch or
session label identifies the replacement across transport retries and crashes.
"""

from __future__ import annotations

from contextlib import AsyncExitStack
from datetime import UTC, datetime
from uuid import uuid4

from pa.execution.selection import ExecutionPreferences, SelectionError, digest


async def create_linked_session(manager, source_id: str, options: dict):
    key = options.get("dispatch_id") or options.get("label")
    if not key or options.get("existing") or options.get("resume_external_id"):
        raise SelectionError(
            "context_boundary_identity_required",
            "A linked new attempt needs a stable new dispatch ID or session label, not an in-place resume",
        )
    identity = digest(
        {
            "source": source_id,
            "key": key,
            "principal": options.get("principal_id"),
            "card": options.get("card_id"),
            "project": options.get("project_id"),
            "preferences": ExecutionPreferences.model_validate(
                options.get("execution_preferences") or {}
            ).model_dump(mode="json"),
            "provider": options.get("provider_override"),
            "configuration": options["initial_configuration"].as_dict()
            if options.get("initial_configuration")
            else None,
        }
    )
    async with (
        manager.label_lock("execution-boundary:" + source_id),
        AsyncExitStack() as stack,
    ):
        live = manager.get(source_id)
        if live and not live._closed:
            await stack.enter_async_context(live._prompt_admission_lock)
        source = (
            live.session
            if live
            else await manager._offload(
                "selection.boundary_source", manager.store.get_session, source_id
            )
        )
        if not source:
            raise SelectionError(
                "context_source_missing",
                "The source session must be available on the selected execution instance",
            )
        if (
            source.origin_instance_id
            and source.origin_instance_id != manager.settings.instance_id
        ):
            raise SelectionError(
                "context_source_instance_mismatch",
                "Create the linked attempt on the source's owning instance; no context is copied from an unverified peer",
            )
        principal = options.get("principal_id") or "user:local"
        if source.principal_id not in {None, principal}:
            raise SelectionError(
                "context_source_not_owned",
                "Only the source session's principal may authorize transferring its saved context",
            )
        if source.card_id != options.get("card_id") or source.project_id != options.get(
            "project_id"
        ):
            raise SelectionError(
                "context_source_scope_mismatch",
                "A linked attempt must preserve the source card and project",
            )
        if options.get("realm_id") and source.realm_id != options["realm_id"]:
            raise SelectionError(
                "context_source_scope_mismatch",
                "A linked attempt must preserve the source realm",
            )
        prior = (source.config_json or {}).get("execution_context_boundary")
        if prior and prior["fingerprint"] != identity:
            raise SelectionError(
                "context_boundary_conflict",
                "This source already has a linked attempt. Inspect its durable target instead of starting a competing runtime",
            )
        if source.dispatch_id:
            from pa.execution.dispatch import DispatchStore

            ledger = DispatchStore(manager.settings.data_dir, read_only=True)
            old_dispatch = await manager._offload(
                "selection.boundary_dispatch", ledger.get, source.dispatch_id
            )
            if (
                not old_dispatch
                or old_dispatch.state
                not in {"completed", "failed", "cancelled", "acknowledged"}
                or not options.get("dispatch_id")
                or options["dispatch_id"] == source.dispatch_id
            ):
                raise SelectionError(
                    "dispatch_context_boundary_required",
                    "Wait for the originating dispatch to become terminal, then submit a new durable dispatch with context_source_session_id and a named source instance",
                )
        durable = (source.config_json or {}).get("durable_runtime") or {}
        if (
            (
                live
                and (
                    live.prompting
                    or live._queue
                    or live._in_flight
                    or live._prompt_lock.locked()
                )
            )
            or durable.get("queued_prompts")
            or durable.get("in_flight")
        ):
            raise SelectionError(
                "context_boundary_busy",
                "The source still owns active or queued prompts. Finish or explicitly cancel them before changing harness/context",
            )
        pending = (source.config_json or {}).get("execution_pending_settings") or {}
        if pending.get("state") in {"pending", "failed"}:
            raise SelectionError(
                "context_boundary_pending_settings",
                "Resolve the source's pending native settings before creating a linked attempt",
            )
        if live:
            live._execution_boundary_reserved = True
            stack.callback(
                lambda: setattr(
                    live,
                    "_execution_boundary_reserved",
                    bool(
                        (live.session.config_json or {}).get(
                            "execution_context_boundary"
                        )
                    ),
                )
            )
        boundary = prior or {
            "contract": "pa.execution-context-boundary/v1",
            "id": str(uuid4()),
            "target_session_id": str(uuid4()),
            "source_session_id": source.id,
            "source_dispatch_id": source.dispatch_id,
            "fingerprint": identity,
            "requested_by": principal,
            "state": "reserved",
            "created_at": datetime.now(UTC).isoformat(),
        }
        if not prior:
            events = await manager._offload(
                "selection.boundary_transcript",
                manager.store.list_transcript_events_before,
                source.id,
                limit=100,
            )
            excerpts = []
            for event in events:
                if event.event_type in {
                    "user_message",
                    "agent_message",
                    "agent_message_chunk",
                }:
                    payload = event.payload or {}
                    text = (
                        payload.get("message")
                        or payload.get("text")
                        or payload.get("content")
                    )
                    if isinstance(text, str):
                        excerpts.append(event.event_type + ": " + text)
            boundary["saved_excerpt"] = "\n\n".join(excerpts[-12:])[-8000:]
            boundary["saved_excerpt_digest"] = digest(boundary["saved_excerpt"])
        target = await manager._offload(
            "selection.boundary_target",
            manager.store.get_session,
            boundary["target_session_id"],
        )
        if target:
            runtime = manager.get(target.id)
            if runtime and not runtime._closed:
                return runtime
            options.update(
                existing=target, resume_external_id=target.external_session_id
            )
        options.update(
            session_id=boundary["target_session_id"], realm_id=source.realm_id
        )
        if prior:
            options["execution_selection"] = prior["selection"]
        workflow = dict(options.get("initiating_workflow") or {})
        workflow["context_boundary"] = {
            k: v for k, v in boundary.items() if k != "selection"
        }
        options["initiating_workflow"] = workflow
        runtime = await manager.create_session(
            **options, _linked_boundary={"source": source, "receipt": boundary}
        )
        runtime._append_transcript(
            "context_boundary",
            {
                "boundary_id": boundary["id"],
                "source_session_id": source.id,
                "source_dispatch_id": source.dispatch_id,
                "reason": "explicit_execution_selection_change",
                "message": "New native context. The former transcript and workspace remain linked, not silently copied or treated as continuous.",
            },
        )
        config = dict(source.config_json or {})
        config["execution_context_boundary"] = {
            **boundary,
            "state": "linked",
            "linked_at": datetime.now(UTC).isoformat(),
        }
        source.config_json = config
        await manager._offload(
            "selection.boundary_linked", manager.store.save_session, source
        )
        return runtime


async def fence_source(manager, boundary, selection):
    source, receipt = boundary["source"], boundary["receipt"]
    receipt["selection"] = selection
    source.config_json = {
        **(source.config_json or {}),
        "execution_context_boundary": receipt,
    }
    source.updated_at = datetime.now(UTC)
    await manager._offload(
        "selection.boundary_fence", manager.store.save_session, source
    )
    runtime = manager.get(source.id)
    if runtime and not runtime._closed:
        runtime.session = source
        # The source admission lock is held and no prompt/queue is owned here.
        # Retain the old worktree and transcript as durable context evidence.
        await runtime.close(
            reason="explicit_execution_context_boundary", reconcile_workspace=False
        )
    else:
        source.status = "closed"
        await manager._offload(
            "selection.boundary_closed", manager.store.save_session, source
        )
