"""Notion connector stub."""

from __future__ import annotations

from pa.integrations.base import ExternalRef, ExternalSystem, SyncBinding


class NotionConnector:
    system = ExternalSystem.NOTION

    def configure(self, config: dict) -> None:
        pass

    async def pull(self, binding: SyncBinding) -> dict:
        return {}

    async def push(self, binding: SyncBinding, pa_snapshot: dict) -> ExternalRef:
        raise NotImplementedError("Notion sync not implemented")
