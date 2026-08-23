"""Optional cloud coordination module."""

from __future__ import annotations

from pa.cloud import CloudCoordinator
from pa.core.context import AppContext
from pa.core.contracts import Module


class CloudModule(Module):
    @property
    def name(self) -> str:
        return "cloud"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def description(self) -> str:
        return "Optional cloud leasing, dispatch, and shared-state coordination"

    def on_load(self, ctx: AppContext) -> None:
        if ctx.settings.cloud_endpoint:
            ctx.register_service("cloud_coordinator", CloudCoordinator(ctx.settings))

    async def on_startup(self, app, ctx: AppContext) -> None:
        coordinator: CloudCoordinator | None = ctx.services.get("cloud_coordinator")
        if coordinator is None:
            return
        coordinator.start()

        event_log = ctx.services.get("event_log")
        if event_log is not None:
            original_append = event_log.append_event

            def append_with_cloud(event, on_commit=None):
                def combined(commit):
                    if on_commit:
                        on_commit(commit)
                    coordinator.publish_event(
                        {
                            "realm_id": commit.realm_id,
                            "commit_hash": commit.hash,
                            "parent_hashes": list(commit.parent_hashes),
                            "event": event.model_dump(mode="json"),
                        }
                    )

                return original_append(event, on_commit=combined)

            event_log.append_event = append_with_cloud  # type: ignore[method-assign]

        dispatch_store = ctx.services.get("dispatch_store")
        if dispatch_store is not None:
            original_put = dispatch_store.put

            def put_with_cloud(record):
                saved = original_put(record)
                coordinator.publish_dispatch(saved.model_dump(mode="json"))
                return saved

            dispatch_store.put = put_with_cloud  # type: ignore[method-assign]

    async def on_shutdown(self, app, ctx: AppContext) -> None:
        coordinator: CloudCoordinator | None = ctx.services.get("cloud_coordinator")
        if coordinator is not None:
            coordinator.close()
