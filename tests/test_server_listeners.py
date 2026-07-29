import os
import errno
import socket
import stat
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from pa.config import Settings
from pa.server.listeners import (
    bind_owner_socket,
    bind_web_sockets,
    close_sockets,
    owner_channel_health,
    owner_socket_path,
    parse_listener,
)


def settings(root: str, **kwargs) -> Settings:
    return Settings(data_dir=Path(root), agent_enabled=False, **kwargs)


def test_listener_parser_supports_common_and_per_listener_ports():
    assert parse_listener("127.0.0.1", 8080).label == "127.0.0.1:8080"
    assert parse_listener("100.78.2.112:9090", 8080).port == 9090
    assert parse_listener("::1", 8080).label == "[::1]:8080"
    assert parse_listener("[::1]:9090", 8080).port == 9090


def test_owner_socket_permissions_stale_cleanup_and_active_refusal():
    with tempfile.TemporaryDirectory() as tmp:
        value = settings(tmp, instance_id="owner")
        path = Path(tmp) / "runtime" / "owner.sock"
        with patch.dict(os.environ, {"PA_OWNER_SOCKET": str(path)}, clear=False):
            stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            path.parent.mkdir()
            stale.bind(str(path))
            stale.close()
            sock, actual = bind_owner_socket(value)
            try:
                assert actual == path
                assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
                assert stat.S_IMODE(path.stat().st_mode) == 0o600
                with pytest.raises(RuntimeError, match="already active"):
                    bind_owner_socket(value)
            finally:
                close_sockets([sock], path)
            assert not path.exists()


@pytest.mark.parametrize(
    "error",
    [
        PermissionError(errno.EPERM, "sandbox denied"),
        PermissionError(errno.EACCES, "permission denied"),
        socket.timeout("timed out"),
        OSError(errno.EIO, "unexpected I/O failure"),
    ],
)
def test_owner_socket_unknown_or_denied_probe_never_unlinks(error):
    with tempfile.TemporaryDirectory() as tmp:
        value = settings(tmp, instance_id="owner")
        path = Path(tmp) / "runtime" / "owner.sock"
        path.parent.mkdir()
        stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        stale.bind(str(path))
        stale.close()
        identity = path.lstat().st_ino
        with (
            patch.dict(os.environ, {"PA_OWNER_SOCKET": str(path)}, clear=False),
            patch.object(socket.socket, "connect", side_effect=error),
            pytest.raises(RuntimeError, match="refusing|timed out"),
        ):
            bind_owner_socket(value)
        assert path.exists()
        assert path.lstat().st_ino == identity


def test_shutdown_does_not_unlink_replacement_socket():
    with tempfile.TemporaryDirectory() as tmp:
        value = settings(tmp, instance_id="owner")
        path = Path(tmp) / "runtime" / "owner.sock"
        with patch.dict(os.environ, {"PA_OWNER_SOCKET": str(path)}, clear=False):
            owner, actual = bind_owner_socket(value)
            path.unlink()
            replacement = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            replacement.bind(str(path))
            replacement_identity = path.lstat().st_ino
            try:
                close_sockets([owner], actual)
                assert path.exists()
                assert path.lstat().st_ino == replacement_identity
            finally:
                replacement.close()
                path.unlink(missing_ok=True)


def test_owner_health_reflects_lost_socket_path():
    with tempfile.TemporaryDirectory() as tmp:
        value = settings(tmp, instance_id="owner")
        path = Path(tmp) / "runtime" / "owner.sock"
        with patch.dict(os.environ, {"PA_OWNER_SOCKET": str(path)}, clear=False):
            owner, actual = bind_owner_socket(value)
            try:
                assert owner_channel_health(value)["state"] == "bound"
                path.unlink()
                health = owner_channel_health(value)
                assert health["state"] == "disconnected"
                assert health["failure_classification"] == "socket_path_missing"
            finally:
                close_sockets([owner], actual)


def test_owner_path_is_shortened_and_does_not_expose_data_path():
    with tempfile.TemporaryDirectory() as tmp:
        long_root = Path(tmp) / ("long" * 40)
        value = settings(str(long_root), instance_id="owner")
        with patch.dict(os.environ, {"PA_RUNTIME_DIR": str(long_root)}, clear=False):
            path = owner_socket_path(value)
        assert len(os.fsencode(path)) <= 103
        assert str(value.data_dir) not in str(path)


def test_multiple_web_listeners_remain_healthy_after_partial_failure():
    with tempfile.TemporaryDirectory() as tmp:
        value = settings(
            tmp,
            port=0,
            web_listeners=["127.0.0.1", "192.0.2.123"],
        )
        # Port zero is useful for collision-free test binds; parser validation is
        # deliberately bypassed here because production config rejects it.
        value.port = 0
        with patch("pa.server.listeners.web_listener_specs") as specs:
            specs.return_value = [
                parse_listener("127.0.0.1", 0),
                parse_listener("192.0.2.123", 0),
            ]
            sockets, health = bind_web_sockets(value)
        try:
            assert sockets
            assert health[0]["bind_state"] == "bound"
            assert health[1]["bind_state"] == "failed"
            assert health[1]["failure_classification"]
        finally:
            for sock in sockets:
                sock.close()


def test_localhost_resolves_every_supported_family():
    with tempfile.TemporaryDirectory() as tmp:
        value = settings(tmp, port=0)
        value.port = 0
        with patch("pa.server.listeners.web_listener_specs") as specs:
            specs.return_value = [parse_listener("localhost", 0)]
            sockets, health = bind_web_sockets(value)
        try:
            assert health[0]["bind_state"] == "bound"
            families = {sock.family for sock in sockets}
            assert socket.AF_INET in families
            if socket.has_ipv6:
                assert families <= {socket.AF_INET, socket.AF_INET6}
        finally:
            for sock in sockets:
                sock.close()
