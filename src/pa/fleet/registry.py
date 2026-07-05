"""Fleet registry — instances owned by this fleet."""

from __future__ import annotations

import json
import secrets
from datetime import UTC, datetime, timedelta
from pathlib import Path

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
        if self.instances_path.exists():
            try:
                data = json.loads(self.instances_path.read_text())
                for item in data.get("instances", []):
                    inst = FleetInstance.model_validate(item)
                    self._instances[inst.instance_id] = inst
            except (json.JSONDecodeError, ValueError):
                pass
        if self.tokens_path.exists():
            try:
                data = json.loads(self.tokens_path.read_text())
                for item in data.get("tokens", []):
                    tok = FleetJoinToken.model_validate(item)
                    if tok.expires_at > datetime.now(UTC):
                        self._tokens[tok.token] = tok
            except (json.JSONDecodeError, ValueError):
                pass

    def _save_instances(self) -> None:
        self.instances_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"instances": [i.model_dump(mode="json") for i in self._instances.values()]}
        self.instances_path.write_text(json.dumps(payload, indent=2) + "\n")

    def _save_tokens(self) -> None:
        payload = {"tokens": [t.model_dump(mode="json") for t in self._tokens.values()]}
        self.tokens_path.write_text(json.dumps(payload, indent=2) + "\n")

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
        inst.last_seen = datetime.now(UTC)
        self._instances[inst.instance_id] = inst
        self._save_instances()
        return inst

    def list_instances(self) -> list[FleetInstance]:
        return list(self._instances.values())

    def get_instance(self, instance_id: str) -> FleetInstance | None:
        return self._instances.get(instance_id)

    def remove_instance(self, instance_id: str) -> bool:
        if instance_id not in self._instances:
            return False
        del self._instances[instance_id]
        self._save_instances()
        return True

    def create_join_token(self, *, ttl_hours: int = 24, created_by: str = "") -> FleetJoinToken:
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
