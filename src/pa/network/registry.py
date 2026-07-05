import httpx

from pa.config import Settings
from pa.domain.models import InstanceInfo


class PeerRegistry:
    """Tracks peer PA instances across the network."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    @property
    def self_info(self) -> InstanceInfo:
        return InstanceInfo(
            id=self.settings.instance_id,
            name=self.settings.instance_name,
            host=self.settings.host,
            port=self.settings.port,
            peers=list(self.settings.peers),
            fleet_id=self.settings.fleet_id,
            subscribed_realms=list(self.settings.subscribed_realms),
            zone=self.settings.zone,
            capabilities=list(self.settings.capabilities),
            relay_enabled=self.settings.relay_enabled,
            agent_enabled=self.settings.agent_enabled,
        )

    async def discover_peers(self) -> list[InstanceInfo]:
        peers: list[InstanceInfo] = []
        headers = {}
        if self.settings.sync_token:
            headers["Authorization"] = f"Bearer {self.settings.sync_token}"
        async with httpx.AsyncClient(timeout=5.0) as client:
            for peer_url in self.settings.peers:
                try:
                    resp = await client.get(
                        f"{peer_url.rstrip('/')}/api/instance",
                        headers=headers,
                    )
                    resp.raise_for_status()
                    peers.append(InstanceInfo.model_validate(resp.json()))
                except httpx.HTTPError:
                    continue
        return peers

    async def broadcast(self, path: str, payload: dict) -> None:
        headers = {"Content-Type": "application/json"}
        if self.settings.sync_token:
            headers["Authorization"] = f"Bearer {self.settings.sync_token}"
        async with httpx.AsyncClient(timeout=5.0) as client:
            for peer_url in self.settings.peers:
                try:
                    await client.post(
                        f"{peer_url.rstrip('/')}{path}",
                        json=payload,
                        headers=headers,
                    )
                except httpx.HTTPError:
                    continue
