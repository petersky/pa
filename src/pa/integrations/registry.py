"""Integration connector registry and binding store."""

from __future__ import annotations

import json
from pathlib import Path

from pa.integrations.base import Connector, ExternalSystem, SyncBinding
from pa.integrations.stubs.github import GitHubIssuesConnector
from pa.integrations.stubs.jira import JiraConnector
from pa.integrations.stubs.notion import NotionConnector


class IntegrationsRegistry:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.bindings_path = data_dir / "integrations.json"
        self._connectors: dict[ExternalSystem, Connector] = {
            ExternalSystem.GITHUB_ISSUES: GitHubIssuesConnector(),
            ExternalSystem.NOTION: NotionConnector(),
            ExternalSystem.JIRA: JiraConnector(),
        }
        self._bindings: list[SyncBinding] = []
        self._load()

    def _load(self) -> None:
        if not self.bindings_path.exists():
            return
        try:
            data = json.loads(self.bindings_path.read_text())
            self._bindings = [SyncBinding.model_validate(b) for b in data.get("bindings", [])]
        except (json.JSONDecodeError, ValueError):
            self._bindings = []

    def _save(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        payload = {"bindings": [b.model_dump(mode="json") for b in self._bindings]}
        self.bindings_path.write_text(json.dumps(payload, indent=2) + "\n")

    def list_systems(self) -> list[str]:
        return [s.value for s in self._connectors]

    def get_connector(self, system: ExternalSystem) -> Connector | None:
        return self._connectors.get(system)

    def list_bindings(self, realm_id: str | None = None) -> list[SyncBinding]:
        if realm_id:
            return [b for b in self._bindings if b.realm_id == realm_id]
        return list(self._bindings)

    def add_binding(self, binding: SyncBinding) -> SyncBinding:
        self._bindings.append(binding)
        self._save()
        return binding

    def get_binding(self, binding_id: str) -> SyncBinding | None:
        for b in self._bindings:
            if b.id == binding_id:
                return b
        return None
