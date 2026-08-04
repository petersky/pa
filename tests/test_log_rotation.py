from __future__ import annotations

import gzip
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from pa.cli import service
from pa.config import Settings
from pa.core.log_rotation import (
    LogRotationPolicy,
    ServiceLogSupervisor,
    read_log_status,
    supervise_service_process,
)


def policy(**updates: object) -> LogRotationPolicy:
    values: dict[str, object] = {
        "max_bytes": 128,
        "interval_seconds": 3600.0,
        "retention_count": 100,
        "retention_max_age_seconds": 3600.0,
        "retention_max_total_bytes": 1024 * 1024,
        "disk_pressure_free_bytes": 0,
    }
    values.update(updates)
    return LogRotationPolicy(**values)  # type: ignore[arg-type]


def contents(log_dir: Path, active_name: str) -> bytes:
    chunks: list[bytes] = []
    for path in sorted(log_dir.glob(f"{active_name}.*")):
        if path.suffix == ".gz":
            with gzip.open(path, "rb") as handle:
                chunks.append(handle.read())
        elif not path.name.endswith(".tmp"):
            chunks.append(path.read_bytes())
    active = log_dir / active_name
    if active.exists():
        chunks.append(active.read_bytes())
    return b"".join(chunks)


class ServiceLogSupervisorTests(unittest.TestCase):
    def test_concurrent_stdout_and_stderr_writers_survive_rotation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            settings = Settings(
                data_dir=root,
                log_rotation_max_bytes=1024,
                log_retention_count=1000,
                log_retention_max_total_bytes=64 * 1024 * 1024,
                log_disk_pressure_free_bytes=0,
            )
            script = """
import sys, threading
def emit(stream, prefix):
    for index in range(300):
        stream.write(f"{prefix}-{index:04d}\\n")
        stream.flush()
threads = []
for worker in range(4):
    threads.append(threading.Thread(target=emit, args=(sys.stdout, f"out-{worker}")))
    threads.append(threading.Thread(target=emit, args=(sys.stderr, f"err-{worker}")))
for thread in threads: thread.start()
for thread in threads: thread.join()
"""
            self.assertEqual(
                supervise_service_process(settings, [sys.executable, "-c", script]),
                0,
            )

            stdout = contents(root / "logs", "server.log").decode()
            stderr = contents(root / "logs", "server.err.log").decode()
            for worker in range(4):
                for index in range(300):
                    self.assertIn(f"out-{worker}-{index:04d}\n", stdout)
                    self.assertIn(f"err-{worker}-{index:04d}\n", stderr)

    def test_size_and_time_rotation_compress_asynchronously(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            now = [1000.0]
            supervisor = ServiceLogSupervisor(
                Path(tmp),
                policy(max_bytes=4, interval_seconds=10),
                clock=lambda: now[0],
            )
            supervisor.stdout.write(b"size")
            now[0] += 11
            supervisor.stdout.write(b"time")
            supervisor.wait_for_maintenance()
            supervisor.close()

            archives = list(Path(tmp).glob("server.log.*.gz"))
            self.assertGreaterEqual(len(archives), 2)
            self.assertEqual(contents(Path(tmp), "server.log"), b"sizetime")

    def test_count_age_and_total_byte_retention_never_delete_active(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp)
            supervisor = ServiceLogSupervisor(
                log_dir,
                policy(
                    max_bytes=256,
                    retention_count=2,
                    retention_max_age_seconds=10,
                    retention_max_total_bytes=350,
                ),
            )
            for index in range(6):
                supervisor.stdout.write(bytes(range(256)) + bytes([index]))
            supervisor.wait_for_maintenance()
            active = log_dir / "server.log"
            self.assertTrue(active.exists())
            self.assertLessEqual(len(list(log_dir.glob("server.log.*.gz"))), 1)

            old = log_dir / "server.err.log.old.gz"
            with gzip.open(old, "wb") as handle:
                handle.write(b"old")
            os.utime(old, (1, 1))
            supervisor._prune()
            self.assertFalse(old.exists())
            self.assertTrue((log_dir / "server.err.log").exists())
            supervisor.close()

    def test_total_byte_limit_counts_active_but_only_prunes_archives(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp)
            supervisor = ServiceLogSupervisor(
                log_dir,
                policy(max_bytes=10_000, retention_max_total_bytes=50),
            )
            supervisor.stdout.write(b"active-must-survive" * 4)
            archive = log_dir / "server.log.old.gz"
            archive.write_bytes(b"archive")
            supervisor._prune()
            self.assertFalse(archive.exists())
            self.assertEqual(
                (log_dir / "server.log").read_bytes(), b"active-must-survive" * 4
            )
            supervisor.close()

    def test_disk_pressure_drops_without_blocking_and_exposes_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            supervisor = ServiceLogSupervisor(
                root / "logs",
                policy(disk_pressure_free_bytes=100),
                disk_usage=lambda _: SimpleNamespace(free=0),
            )
            started = time.monotonic()
            for _ in range(1000):
                supervisor.stdout.write(b"request output\n")
            elapsed = time.monotonic() - started
            supervisor.close()

            status = read_log_status(root)
            self.assertIsNotNone(status)
            assert status is not None
            self.assertLess(elapsed, 2.0)
            self.assertEqual(status["disk_pressure"]["state"], "dropping")
            self.assertGreater(status["dropped_bytes"], 0)
            self.assertEqual((root / "logs" / "server.log").read_bytes(), b"")

    def test_rotation_and_prune_failures_are_counted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp)
            supervisor = ServiceLogSupervisor(log_dir, policy(max_bytes=4))
            real_replace = os.replace

            def fail_active(source: object, target: object) -> None:
                if Path(source).name == "server.log":
                    raise OSError("rename failed")
                real_replace(source, target)

            with patch("pa.core.log_rotation.os.replace", side_effect=fail_active):
                supervisor.stdout.write(b"data")
            victim = log_dir / "server.log.victim.gz"
            victim.write_bytes(b"archive")
            os.utime(victim, (1, 1))
            with patch.object(Path, "unlink", side_effect=OSError("busy")):
                supervisor._prune()
            status = supervisor.snapshot()
            self.assertEqual(status["rotation_failures"], 1)
            self.assertGreaterEqual(status["prune_failures"], 1)
            self.assertTrue((log_dir / "server.log").exists())
            supervisor.close()

    def test_child_crash_and_restart_append_without_losing_prior_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                log_rotation_max_bytes=1024,
                log_disk_pressure_free_bytes=0,
            )
            first = [sys.executable, "-c", "print('before-crash'); raise SystemExit(7)"]
            second = [sys.executable, "-c", "print('after-restart')"]
            self.assertEqual(supervise_service_process(settings, first), 7)
            self.assertEqual(supervise_service_process(settings, second), 0)
            output = contents(Path(tmp) / "logs", "server.log")
            self.assertIn(b"before-crash", output)
            self.assertIn(b"after-restart", output)

    def test_restart_recovers_interrupted_compression_temporary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp)
            temporary = log_dir / "server.log.interrupted.gz.tmp"
            temporary.write_bytes(b"partial")
            supervisor = ServiceLogSupervisor(log_dir, policy())
            supervisor.wait_for_maintenance()
            supervisor.close()
            self.assertFalse(temporary.exists())

    def test_failure_counters_survive_service_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = ServiceLogSupervisor(root / "logs", policy(max_bytes=4))
            with patch("pa.core.log_rotation.os.replace", side_effect=OSError("no")):
                first.stdout.write(b"data")
            first.close()
            second = ServiceLogSupervisor(root / "logs", policy())
            self.assertEqual(second.snapshot()["rotation_failures"], 1)
            second.close()


class ServiceLayoutTests(unittest.TestCase):
    def test_launchd_and_systemd_delegate_streams_to_bounded_supervisor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp))
            plist = service.render_plist(settings, Path("/usr/bin/pa")).decode()
            unit = service.render_systemd_unit(settings, Path("/usr/bin/pa"))
            self.assertIn("<string>_service-run</string>", plist)
            self.assertIn("<string>/dev/null</string>", plist)
            self.assertIn("ExecStart=/usr/bin/pa _service-run", unit)
            self.assertIn("StandardOutput=null", unit)
            self.assertIn("StandardError=null", unit)


if __name__ == "__main__":
    unittest.main()
