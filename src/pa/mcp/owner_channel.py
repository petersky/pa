"""Canonical process-local endpoint for PA-owned child processes."""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass

from pa.config import Settings


_LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1", "[::1]"}


@dataclass(frozen=True)
class OwnerChannel:
    """A locally reachable view of the owning server's configured listener."""

    url: str
    endpoint_type: str


def _url_host(host: str) -> str:
    """Format a bind host for use as an HTTP URL authority."""
    normalized = host.strip()
    if normalized.startswith("[") and normalized.endswith("]"):
        return normalized
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        return normalized
    return f"[{normalized}]" if address.version == 6 else normalized


def owner_channel(settings: Settings) -> OwnerChannel:
    """Resolve the owner listener without consulting advertised/fleet URLs.

    Wildcard listeners accept loopback traffic, so they use a loopback address
    of the same family. Concrete binds use that exact local interface;
    substituting an advertised URL could cross namespaces, TLS boundaries, or
    instance authorities.
    """
    configured = settings.host.strip()
    if configured in {"::", "[::]"}:
        host = "::1"
        endpoint_type = "wildcard_ipv6"
    elif configured == "0.0.0.0":
        host = "127.0.0.1"
        endpoint_type = "wildcard_ipv4"
    else:
        host = configured
        endpoint_type = (
            "loopback"
            if configured.lower() in _LOOPBACK_HOSTS
            else "concrete_ipv6"
            if ":" in configured
            else "concrete"
        )
    return OwnerChannel(
        url=f"http://{_url_host(host)}:{settings.port}",
        endpoint_type=endpoint_type,
    )
