"""Jira connector stub."""

from __future__ import annotations

from pa.integrations.base import ExternalRef, ExternalSystem, SyncBinding


class JiraConnector:
    system = ExternalSystem.JIRA

    def configure(self, config: dict) -> None:
        pass

    async def pull(self, binding: SyncBinding) -> dict:
        return {}

    async def push(self, binding: SyncBinding, pa_snapshot: dict) -> ExternalRef:
        raise NotImplementedError("Jira sync not implemented")
