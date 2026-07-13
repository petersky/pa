"""Fleet registry — instances owned by this fleet."""

from __future__ import annotations

import json
import secrets
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pa.core.io import atomic_write_json
from pa.domain.models import FleetInstance, FleetJoinToken


class FleetRegistry:
    def __init__(self, data_dir: Path, fleet_id: str) -> None:
        self.fleet_id = fleet_id
        self.instances_path = data_dir / "fleet_instances.json"
        self.tokens_path = data_dir / "fleet_join_tokens.json"
        self._instances: dict[str, FleetInstance] = {}
        self._tokens: dict[str, FleetJoinToken] = {}
        self._load()

    def _load(self) -> None:
        self._reload_instances()
        self._reload_tokens()

    def _reload_instances(self) -> None:
        """Merge instances from disk so CLI and server stay consistent."""
        if not self.instances_path.exists():
            return
        try:
            data = json.loads(self.instances_path.read_text())
            for item in data.get("instances", []):
                inst = FleetInstance.model_validate(item)
                self._instances[inst.instance_id] = inst
        except (json.JSONDecodeError, ValueError):
            pass

    def _reload_tokens(self) -> None:
        """Merge tokens from disk so CLI-minted tokens work with a live server."""
        if not self.tokens_path.exists():
            return
        try:
            data = json.loads(self.tokens_path.read_text())
            now = datetime.now(UTC)
            for item in data.get("tokens", []):
                tok = FleetJoinToken.model_validate(item)
                if tok.expires_at > now:
                    self._tokens[tok.token] = tok
        except (json.JSONDecodeError, ValueError):
            pass

    def _save_instances(self) -> None:
        self.instances_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"instances": [i.model_dump(mode="json") for i in self._instances.values()]}
        atomic_write_json(self.instances_path, payload)

    def _save_tokens(self) -> None:
        # Drop expired before save
        now = datetime.now(UTC)
        self._tokens = {k: v for k, v in self._tokens.items() if v.expires_at > now}
        payload = {"tokens": [t.model_dump(mode="json") for t in self._tokens.values()]}
        atomic_write_json(self.tokens_path, payload)

    def register_self(
        self,
        instance_id: str,
        name: str,
        url: str,
        *,
        zone: str = "default",
        capabilities: list[str] | None = None,
        relay_enabled: bool = False,
    ) -> FleetInstance:
        self._reload_instances()
        inst = FleetInstance(
            instance_id=instance_id,
            name=name,
            url=url,
            zone=zone,
            capabilities=capabilities or [],
            relay_enabled=relay_enabled,
            last_seen=datetime.now(UTC),
            healthy=True,
        )
        self._instances[instance_id] = inst
        self._save_instances()
        return inst

    def upsert_instance(self, inst: FleetInstance) -> FleetInstance:
        self._reload_instances()
        inst.last_seen = datetime.now(UTC)
        self._instances[inst.instance_id] = inst
        self._save_instances()
        return inst

    def list_instances(self) -> list[FleetInstance]:
        self._reload_instances()
        return list(self._instances.values())

    def get_instance(self, instance_id: str) -> FleetInstance | None:
        self._reload_instances()
        return self._instances.get(instance_id)

    def remove_instance(self, instance_id: str) -> bool:
        self._reload_instances()
        if instance_id not in self._instances:
            return False
        del self._instances[instance_id]
        self._save_instances()
        return True

    def create_join_token(self, *, ttl_hours: int = 24, created_by: str = "") -> FleetJoinToken:
        self._reload_tokens()
        token = secrets.token_urlsafe(32)
        join = FleetJoinToken(
            token=token,
            fleet_id=self.fleet_id,
            expires_at=datetime.now(UTC) + timedelta(hours=ttl_hours),
            created_by=created_by,
        )
        self._tokens[token] = join
        self._save_tokens()
        return join

    def consume_join_token(self, token: str) -> FleetJoinToken | None:
        self._reload_tokens()
        join = self._tokens.get(token)
        if not join:
            return None
        if join.expires_at < datetime.now(UTC):
            del self._tokens[token]
            self._save_tokens()
            return None
        del self._tokens[token]
        self._save_tokens()
        return join
