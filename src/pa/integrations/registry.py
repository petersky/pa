"""Integration connector registry and binding store."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from pa.core.io import atomic_write_json
from pa.integrations.base import Connector, ExternalSystem, SyncBinding
from pa.integrations.stubs.github import GitHubIssuesConnector
from pa.integrations.stubs.jira import JiraConnector
from pa.integrations.stubs.notion import NotionConnector

logger = logging.getLogger(__name__)


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
        atomic_write_json(self.bindings_path, payload)

    def list_systems(self) -> list[dict]:
        return [
            {
                "id": system.value,
                "stub": bool(getattr(connector, "STUB", False)),
            }
            for system, connector in self._connectors.items()
        ]

    def get_connector(self, system: ExternalSystem) -> Connector | None:
        return self._connectors.get(system)

    def is_stub(self, system: ExternalSystem) -> bool:
        connector = self.get_connector(system)
        return bool(connector and getattr(connector, "STUB", False))

    def configure(self, system: ExternalSystem, config: dict) -> None:
        connector = self.get_connector(system)
        if not connector:
            raise ValueError(f"Unknown system: {system}")
        if getattr(connector, "STUB", False):
            logger.warning(
                "Connector %s is a stub; configuration is stored but sync is not implemented",
                system.value,
            )
        connector.configure(config)

    async def pull_binding(self, binding: SyncBinding) -> dict:
        connector = self.get_connector(binding.external_ref.system)
        if not connector:
            raise ValueError(f"Unknown system: {binding.external_ref.system}")
        if getattr(connector, "STUB", False):
            logger.warning(
                "Connector %s is a stub; pull for binding %s returned no data",
                binding.external_ref.system.value,
                binding.id,
            )
        return await connector.pull(binding)

    def list_bindings(self, realm_id: str | None = None) -> list[SyncBinding]:
        if realm_id:
            return [b for b in self._bindings if b.realm_id == realm_id]
        return list(self._bindings)

    def add_binding(self, binding: SyncBinding) -> SyncBinding:
        if self.is_stub(binding.external_ref.system):
            logger.warning(
                "Adding binding for stub connector %s; sync will not fetch external data",
                binding.external_ref.system.value,
            )
        self._bindings.append(binding)
        self._save()
        return binding

    def get_binding(self, binding_id: str) -> SyncBinding | None:
        for b in self._bindings:
            if b.id == binding_id:
                return b
        return None
