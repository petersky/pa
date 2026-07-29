"""Socket lifecycle for PA's private owner channel and public web listeners."""

from __future__ import annotations

import hashlib
import logging
import os
import socket
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path

from pa.config import Settings

log = logging.getLogger(__name__)
_UNIX_PATH_MAX = 103


@dataclass(frozen=True)
class ListenerSpec:
    host: str
    port: int

    @property
    def label(self) -> str:
        host = f"[{self.host}]" if ":" in self.host else self.host
        return f"{host}:{self.port}"


def parse_listener(value: str, default_port: int) -> ListenerSpec:
    value = value.strip()
    if not value:
        raise ValueError("web listener cannot be empty")
    if value.startswith("["):
        closing = value.find("]")
        if closing < 0:
            raise ValueError(f"invalid bracketed IPv6 listener: {value}")
        host, suffix = value[1:closing], value[closing + 1 :]
        if suffix and not suffix.startswith(":"):
            raise ValueError(f"invalid web listener: {value}")
        return ListenerSpec(host, int(suffix[1:]) if suffix else default_port)
    if value.count(":") > 1:
        return ListenerSpec(value, default_port)
    if ":" in value:
        host, port = value.rsplit(":", 1)
        return ListenerSpec(host, int(port))
    return ListenerSpec(value, default_port)


def web_listener_specs(settings: Settings) -> list[ListenerSpec]:
    values = settings.web_listeners or [settings.host]
    specs: list[ListenerSpec] = []
    for value in values:
        spec = parse_listener(value, settings.port)
        if not 1 <= spec.port <= 65535:
            raise ValueError(f"invalid port in web listener: {value}")
        if spec not in specs:
            specs.append(spec)
    return specs


def owner_socket_path(settings: Settings) -> Path:
    explicit = os.environ.get("PA_OWNER_SOCKET", "").strip()
    if explicit:
        path = Path(explicit)
    else:
        runtime = os.environ.get("PA_RUNTIME_DIR", "").strip()
        if not runtime:
            xdg = os.environ.get("XDG_RUNTIME_DIR", "").strip()
            runtime = (
                str(Path(xdg) / "pa")
                if xdg
                else str(Path(tempfile.gettempdir()) / f"pa-{os.getuid()}")
            )
        identity = hashlib.sha256(
            f"{settings.data_dir.resolve()}:{settings.instance_id}".encode()
        ).hexdigest()[:16]
        path = Path(runtime) / identity / "owner.sock"
    if len(os.fsencode(path)) > _UNIX_PATH_MAX:
        digest = hashlib.sha256(os.fsencode(path)).hexdigest()[:24]
        path = Path(tempfile.gettempdir()) / f"pa-{os.getuid()}" / digest / "o.sock"
    return path


def bind_owner_socket(settings: Settings) -> tuple[socket.socket, Path]:
    if not hasattr(socket, "AF_UNIX"):
        raise RuntimeError(
            "Unix-domain owner channel is unsupported; configure PA_OWNER_API_URL "
            "explicitly to a private shared-namespace HTTP endpoint."
        )
    path = owner_socket_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    if path.exists() or path.is_symlink():
        info = path.lstat()
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid():
            raise RuntimeError(f"refusing to replace unsafe owner socket path: {path}")
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            probe.settimeout(0.15)
            probe.connect(str(path))
        except OSError:
            path.unlink()
        else:
            raise RuntimeError(f"owner socket is already active: {path}")
        finally:
            probe.close()
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.bind(str(path))
        os.chmod(path, 0o600)
        sock.listen(socket.SOMAXCONN)
        sock.setblocking(False)
    except BaseException:
        sock.close()
        if path.exists() and stat.S_ISSOCK(path.lstat().st_mode):
            path.unlink()
        raise
    return sock, path


def bind_web_sockets(settings: Settings) -> tuple[list[socket.socket], list[dict]]:
    sockets: list[socket.socket] = []
    health: list[dict] = []
    claimed: set[tuple] = set()
    for spec in web_listener_specs(settings):
        bound, failures = 0, []
        try:
            addresses = socket.getaddrinfo(
                spec.host, spec.port, type=socket.SOCK_STREAM, flags=socket.AI_PASSIVE
            )
        except OSError as exc:
            addresses = []
            failures.append(type(exc).__name__)
        seen: set[tuple] = set()
        for family, kind, proto, _, address in addresses:
            key = (family, address)
            if key in seen:
                continue
            seen.add(key)
            if key in claimed:
                # Multiple labels (notably 127.0.0.1 and localhost) may resolve
                # to the same socket. One bind serves every equivalent label.
                bound += 1
                continue
            sock = socket.socket(family, kind, proto)
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if family == socket.AF_INET6:
                    sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
                sock.bind(address)
                sock.listen(socket.SOMAXCONN)
                sock.setblocking(False)
            except OSError as exc:
                failures.append(type(exc).__name__)
                sock.close()
                continue
            sockets.append(sock)
            claimed.add(key)
            bound += 1
        health.append(
            {
                "endpoint_type": "web",
                "listener": spec.label,
                "bind_state": "bound" if bound else "failed",
                "bound_sockets": bound,
                "failure_classification": failures[-1] if failures else None,
                "retry_state": "restart_required" if failures else "none",
            }
        )
        if failures:
            log.warning(
                "Web listener %s partially available (%s bound; %s)",
                spec.label,
                bound,
                ",".join(failures),
            )
    return sockets, health


def close_sockets(sockets: list[socket.socket], owner_path: Path | None) -> None:
    for sock in sockets:
        sock.close()
    if owner_path is None:
        return
    try:
        if owner_path.exists() and stat.S_ISSOCK(owner_path.lstat().st_mode):
            owner_path.unlink()
    except OSError:
        log.warning("Could not remove PA owner socket %s", owner_path)
