"""Per-realm peer routing table."""

from __future__ import annotations

import json
from pathlib import Path

from pa.domain.models import FleetInstance, PeerRoute, PeerRouteMode


class PeerTable:
    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir / "peer_table.json"
        self._routes: list[PeerRoute] = []
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text())
            self._routes = [PeerRoute.model_validate(r) for r in data.get("routes", [])]
        except json.JSONDecodeError, ValueError:
            self._routes = []

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"routes": [r.model_dump() for r in self._routes]}
        self.path.write_text(json.dumps(payload, indent=2) + "\n")

    def set_routes(self, routes: list[PeerRoute]) -> None:
        self._routes = routes
        self._save()

    def add_route(self, route: PeerRoute) -> None:
        self._routes = [
            r
            for r in self._routes
            if not (r.realm_id == route.realm_id and r.target_url == route.target_url)
        ]
        self._routes.append(route)
        self._save()

    def routes_for_realm(self, realm_id: str) -> list[PeerRoute]:
        return [r for r in self._routes if r.realm_id == realm_id]

    def all_routes(self) -> list[PeerRoute]:
        return list(self._routes)

    def sync_from_settings_peers(
        self, realm_id: str, peer_urls: list[str], zone: str = "default"
    ) -> None:
        for url in peer_urls:
            self.add_route(
                PeerRoute(
                    realm_id=realm_id,
                    target_url=url.rstrip("/"),
                    zone=zone,
                    mode=PeerRouteMode.DIRECT,
                )
            )

    def prefer_same_zone(self, realm_id: str, local_zone: str) -> list[PeerRoute]:
        routes = self.routes_for_realm(realm_id)
        same = [r for r in routes if r.zone == local_zone]
        other = [r for r in routes if r.zone != local_zone]
        return same + other

    def reconcile_membership(
        self,
        members: list[FleetInstance],
        *,
        realms: list[str],
        local_instance_id: str,
    ) -> dict[str, int]:
        """Derive direct routes from canonical members, preserving explicit relays."""
        before = list(self._routes)
        explicit_relays = [
            route for route in before if route.mode == PeerRouteMode.RELAY
        ]
        routes = list(explicit_relays)
        for realm_id in realms:
            for member in members:
                if (
                    member.instance_id == local_instance_id
                    or member.lifecycle_state != "active"
                    or not member.url
                ):
                    continue
                routes.append(
                    PeerRoute(
                        realm_id=realm_id,
                        target_url=member.url,
                        target_instance_id=member.instance_id,
                        zone=member.zone,
                        mode=PeerRouteMode.DIRECT,
                    )
                )
        unique: dict[tuple[str, str, str], PeerRoute] = {}
        for route in routes:
            key = (
                route.realm_id,
                route.target_instance_id or "",
                route.target_url.rstrip("/"),
            )
            unique[key] = route
        self.set_routes(list(unique.values()))
        return {"before": len(before), "after": len(unique)}
