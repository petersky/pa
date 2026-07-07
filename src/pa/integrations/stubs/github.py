"""GitHub Issues connector stub."""

from __future__ import annotations

from pa.integrations.base import Connector, ExternalRef, ExternalSystem, SyncBinding


class GitHubIssuesConnector:
    system = ExternalSystem.GITHUB_ISSUES
    STUB = True

    def configure(self, config: dict) -> None:
        pass

    async def pull(self, binding: SyncBinding) -> dict:
        return {"_stub": True, "items": []}

    async def push(self, binding: SyncBinding, pa_snapshot: dict) -> ExternalRef:
        raise NotImplementedError("GitHub Issues sync not implemented")
