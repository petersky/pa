"""Socket lifecycle for PA's private owner channel and public web listeners."""

from __future__ import annotations

import errno
import hashlib
import logging
import os
import socket
import stat
import tempfile
import threading
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pa.config import Settings

log = logging.getLogger(__name__)
_UNIX_PATH_MAX = 103
_owner_state_lock = threading.Lock()
_owner_socket_registration: OwnerSocketRegistration | None = None
_owner_health: dict[str, str | None] = {
    "endpoint_type": "unix",
    "state": "not_bound",
    "last_success": None,
    "last_failure": None,
    "failure_classification": "not_bound",
    "retry_state": "restart_required",
}


@dataclass(frozen=True)
class ListenerSpec:
    host: str
    port: int

    @property
    def label(self) -> str:
        host = f"[{self.host}]" if ":" in self.host else self.host
        return f"{host}:{self.port}"


@dataclass(frozen=True)
class OwnerSocketRegistration:
    path: Path
    device: int
    inode: int
    listener: socket.socket


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _path_identity(path: Path) -> tuple[int, int, int] | None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    return info.st_dev, info.st_ino, info.st_mode


def _same_socket(
    path: Path, *, device: int, inode: int, require_owner: bool = True
) -> bool:
    identity = _path_identity(path)
    if identity is None:
        return False
    current_device, current_inode, mode = identity
    if not stat.S_ISSOCK(mode):
        return False
    if require_owner:
        try:
            if path.lstat().st_uid != os.getuid():
                return False
        except OSError:
            return False
    return current_device == device and current_inode == inode


def _set_owner_health(**values: str | None) -> None:
    with _owner_state_lock:
        _owner_health.update(values)


def record_owner_probe(
    *,
    endpoint_type: str,
    success: bool,
    classification: str | None = None,
    retry_state: str | None = None,
) -> None:
    """Maintain owner-channel probe evidence for status snapshots."""
    if success:
        _set_owner_health(
            endpoint_type=endpoint_type,
            state="connected",
            last_success=_now(),
            failure_classification=None,
            retry_state=retry_state or "none",
        )
    else:
        _set_owner_health(
            endpoint_type=endpoint_type,
            state="disconnected",
            last_failure=_now(),
            failure_classification=classification or "probe_failed",
            retry_state=retry_state or "retry_required",
        )


def owner_channel_health(settings: Settings) -> dict[str, str | None]:
    """Return live listener/path health, not merely historical bind success."""
    explicit = os.environ.get("PA_OWNER_API_URL", "").strip()
    with _owner_state_lock:
        health = dict(_owner_health)
        registration = _owner_socket_registration
    if explicit:
        previously_explicit = health.get("endpoint_type") == "explicit_private_http"
        health["endpoint_type"] = "explicit_private_http"
        if not previously_explicit:
            health.update(
                state="not_probed",
                failure_classification="not_probed",
                retry_state="probe_required",
            )
        return health

    health["endpoint_type"] = "unix"
    path = owner_socket_path(settings)
    health["socket_path"] = str(path)
    if registration is None or registration.path != path:
        health.update(
            state="unverified",
            failure_classification="listener_not_owned_by_process",
            retry_state="restart_required",
        )
        return health
    if registration.listener.fileno() < 0:
        health.update(
            state="disconnected",
            last_failure=_now(),
            failure_classification="listener_closed",
            retry_state="restart_required",
        )
        return health
    identity = _path_identity(path)
    if identity is None:
        health.update(
            state="disconnected",
            last_failure=_now(),
            failure_classification="socket_path_missing",
            retry_state="restart_required",
        )
        return health
    if not _same_socket(path, device=registration.device, inode=registration.inode):
        health.update(
            state="disconnected",
            last_failure=_now(),
            failure_classification="socket_identity_changed",
            retry_state="restart_required",
        )
        return health
    health.update(
        state="bound",
        failure_classification=None,
        retry_state="none",
    )
    return health


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


def owner_socket_path(
    settings: Settings, environment: Mapping[str, str] | None = None
) -> Path:
    environment = os.environ if environment is None else environment
    explicit = environment.get("PA_OWNER_SOCKET", "").strip()
    if explicit:
        path = Path(explicit)
    else:
        runtime = environment.get("PA_RUNTIME_DIR", "").strip()
        if not runtime:
            xdg = environment.get("XDG_RUNTIME_DIR", "").strip()
            if xdg:
                runtime = str(Path(xdg) / "pa")
            else:
                linux_runtime = Path("/run/user") / str(os.getuid())
                try:
                    info = linux_runtime.stat()
                    safe_linux_runtime = (
                        sys.platform.startswith("linux")
                        and linux_runtime.is_dir()
                        and info.st_uid == os.getuid()
                        and stat.S_IMODE(info.st_mode) & 0o022 == 0
                    )
                except OSError:
                    safe_linux_runtime = False
                runtime = str(
                    linux_runtime / "pa"
                    if safe_linux_runtime
                    else Path(tempfile.gettempdir()) / f"pa-{os.getuid()}"
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
    global _owner_socket_registration
    if not hasattr(socket, "AF_UNIX"):
        raise RuntimeError(
            "Unix-domain owner channel is unsupported; configure PA_OWNER_API_URL "
            "explicitly to a private shared-namespace HTTP endpoint."
        )
    path = owner_socket_path(settings)
    log.info("Owner channel bind attempt endpoint_type=unix path=%s", path)
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
        except socket.timeout as exc:
            raise RuntimeError(
                f"owner socket probe timed out; refusing stale cleanup: {path}"
            ) from exc
        except OSError as exc:
            if exc.errno != errno.ECONNREFUSED:
                raise RuntimeError(
                    "owner socket could not be safely classified as stale "
                    f"({type(exc).__name__}, errno={exc.errno}); refusing cleanup: {path}"
                ) from exc
            if not _same_socket(path, device=info.st_dev, inode=info.st_ino):
                raise RuntimeError(
                    f"owner socket identity changed during stale probe: {path}"
                ) from exc
            path.unlink()
        else:
            raise RuntimeError(f"owner socket is already active: {path}")
        finally:
            probe.close()
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    registration: OwnerSocketRegistration | None = None
    try:
        sock.bind(str(path))
        bound = path.lstat()
        registration = OwnerSocketRegistration(
            path=path,
            device=bound.st_dev,
            inode=bound.st_ino,
            listener=sock,
        )
        os.chmod(path, 0o600)
        sock.listen(socket.SOMAXCONN)
        sock.setblocking(False)
    except BaseException as exc:
        classification = (
            "permission_denied" if isinstance(exc, PermissionError) else "bind_failed"
        )
        _set_owner_health(
            endpoint_type="unix",
            state="disconnected",
            last_failure=_now(),
            failure_classification=classification,
            retry_state="restart_required",
        )
        log.error(
            "Owner channel bind failed (classification=%s, path=%s, error=%s)",
            classification,
            path,
            type(exc).__name__,
        )
        sock.close()
        if registration and _same_socket(
            path, device=registration.device, inode=registration.inode
        ):
            path.unlink()
        raise
    with _owner_state_lock:
        _owner_socket_registration = registration
        _owner_health.update(
            endpoint_type="unix",
            state="bound",
            last_success=_now(),
            last_failure=None,
            failure_classification=None,
            retry_state="none",
        )
    log.info("Owner channel bind succeeded endpoint_type=unix path=%s mode=0600", path)
    return sock, path


def record_owner_bind_failure(settings: Settings, exc: BaseException) -> None:
    """Expose a redacted degraded state when the private listener cannot bind."""
    classification = type(exc).__name__
    _set_owner_health(
        endpoint_type="unix",
        state="degraded",
        last_failure=_now(),
        failure_classification=classification,
        retry_state="restart_required",
    )
    log.error(
        "Owner channel bind failed endpoint_type=unix path=%s classification=%s",
        owner_socket_path(settings),
        classification,
    )


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
    global _owner_socket_registration
    for sock in sockets:
        sock.close()
    if owner_path is None:
        return
    with _owner_state_lock:
        registration = _owner_socket_registration
        if registration and registration.path == owner_path:
            _owner_socket_registration = None
    try:
        if registration and _same_socket(
            owner_path,
            device=registration.device,
            inode=registration.inode,
        ):
            owner_path.unlink()
    except OSError:
        log.warning("Could not remove PA owner socket %s", owner_path)
    _set_owner_health(
        state="closed",
        last_failure=_now(),
        failure_classification="listener_closed",
        retry_state="restart_required",
    )
