from __future__ import annotations

import gzip
import os
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pa.cli import service
from pa.config import Settings
from pa.core import log_rotation
from pa.core.log_rotation import (
    BOOTSTRAP_FILE,
    LogRotationPolicy,
    LogSupervisorAlreadyOwnedError,
    ServiceLogSupervisor,
    UnsafeLogPathError,
    _pump,
    _record_bootstrap_failure,
    prepare_bootstrap_log,
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
    def test_log_directory_active_archives_and_status_are_private(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            previous = os.umask(0o022)
            try:
                supervisor = ServiceLogSupervisor(root / "logs", policy(max_bytes=4))
                supervisor.stdout.write(b"secret")
                supervisor.wait_for_maintenance()
                supervisor.close()
            finally:
                os.umask(previous)

            self.assertEqual(stat.S_IMODE((root / "logs").stat().st_mode), 0o700)
            for path in (root / "logs").iterdir():
                self.assertEqual(
                    stat.S_IMODE(path.stat().st_mode),
                    0o600,
                    f"unsafe mode for {path.name}",
                )

    def test_exclusive_lifetime_ownership_rejects_another_process(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "logs"
            script = """
import sys
from pathlib import Path
from pa.core.log_rotation import LogRotationPolicy, ServiceLogSupervisor
p=LogRotationPolicy(128,3600,10,3600,1000000,0)
s=ServiceLogSupervisor(Path(sys.argv[1]),p)
print('ready', flush=True)
sys.stdin.readline()
s.close()
"""
            child = subprocess.Popen(
                [sys.executable, "-c", script, str(log_dir)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                assert child.stdout is not None
                self.assertEqual(child.stdout.readline().strip(), "ready")
                with self.assertRaises(LogSupervisorAlreadyOwnedError):
                    ServiceLogSupervisor(log_dir, policy())
            finally:
                assert child.stdin is not None
                child.stdin.write("stop\n")
                child.stdin.flush()
                _, error = child.communicate(timeout=5)
            self.assertEqual(child.returncode, 0, error)

    def test_symlinked_active_log_is_rejected_without_touching_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            log_dir = root / "logs"
            log_dir.mkdir()
            target = root / "outside.log"
            target.write_text("outside")
            original_mode = stat.S_IMODE(target.stat().st_mode)
            (log_dir / "server.log").symlink_to(target)

            with self.assertRaises(UnsafeLogPathError):
                ServiceLogSupervisor(log_dir, policy())

            self.assertEqual(target.read_text(), "outside")
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), original_mode)

    def test_nonregular_active_log_is_rejected_without_blocking(self) -> None:
        if not hasattr(os, "mkfifo"):
            self.skipTest("named pipes are unavailable")
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "logs"
            log_dir.mkdir()
            os.mkfifo(log_dir / "server.log")

            started = time.monotonic()
            with self.assertRaises(UnsafeLogPathError):
                ServiceLogSupervisor(log_dir, policy())

            self.assertLess(time.monotonic() - started, 1.0)

    def test_rotation_open_failure_drops_bounded_output_but_keeps_draining(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp)
            supervisor = ServiceLogSupervisor(log_dir, policy(max_bytes=4))
            with patch.object(
                supervisor.stdout,
                "_create_replacement",
                side_effect=OSError("replacement unavailable"),
            ):
                supervisor.stdout.write(b"AAAA")
                supervisor.stdout.write(b"BBBB")
            supervisor.stdout.write(b"CCCC")
            supervisor.wait_for_maintenance()
            snapshot = supervisor.snapshot()
            supervisor.close()

            self.assertEqual(contents(log_dir, "server.log"), b"AAAACCCC")
            self.assertGreaterEqual(snapshot["rotation_failures"], 2)
            self.assertGreaterEqual(snapshot["dropped_bytes"], 4)

    def test_failed_second_rename_recovers_detached_inode_without_loss(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp)
            supervisor = ServiceLogSupervisor(log_dir, policy(max_bytes=4))
            real_replace = os.replace

            def fail_publish_and_restore(source: object, target: object) -> None:
                source_path, target_path = Path(source), Path(target)
                if source_path.name.endswith("handoff.tmp"):
                    raise OSError("publish failed")
                if (
                    source_path.name.startswith("server.log.")
                    and target_path.name == "server.log"
                ):
                    raise OSError("restore failed")
                real_replace(source, target)

            with patch(
                "pa.core.log_rotation.os.replace", side_effect=fail_publish_and_restore
            ):
                supervisor.stdout.write(b"AAAA")
            supervisor.stdout.write(b"CCCC")
            supervisor.wait_for_maintenance()
            supervisor.close()

            self.assertEqual(contents(log_dir, "server.log"), b"AAAACCCC")

    def test_restart_recovers_crash_between_archive_and_active_publish(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp)
            archive = log_dir / "server.log.crash"
            archive.write_bytes(b"durable-before-crash")
            handoff = log_dir / ".server.log.7.1.handoff.tmp"
            handoff.write_bytes(b"")

            supervisor = ServiceLogSupervisor(log_dir, policy())
            supervisor.wait_for_maintenance()
            supervisor.close()

            self.assertTrue((log_dir / "server.log").exists())
            self.assertFalse(handoff.exists())
            self.assertEqual(contents(log_dir, "server.log"), b"durable-before-crash")

    def test_rotation_and_compression_fsync_files_and_directory_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp)
            with (
                patch("pa.core.log_rotation.os.fsync", wraps=os.fsync) as fsync_file,
                patch(
                    "pa.core.log_rotation._fsync_directory",
                    wraps=log_rotation._fsync_directory,
                ) as fsync_directory,
            ):
                supervisor = ServiceLogSupervisor(log_dir, policy(max_bytes=4))
                supervisor.stdout.write(b"data")
                supervisor.wait_for_maintenance()
                supervisor.close()

            self.assertGreaterEqual(fsync_file.call_count, 4)
            self.assertGreaterEqual(fsync_directory.call_count, 3)

    def test_archive_name_collision_never_overwrites_existing_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp)
            supervisor = ServiceLogSupervisor(
                log_dir,
                policy(max_bytes=4),
                clock=lambda: 1000.0,
            )
            collision = log_dir / (
                f"server.log.19700101T001640.000000Z.{os.getpid()}.1"
            )
            collision.write_bytes(b"preexisting-history")

            supervisor.stdout.write(b"data")
            supervisor.wait_for_maintenance()
            supervisor.close()

            recovered = contents(log_dir, "server.log")
            self.assertIn(b"preexisting-history", recovered)
            self.assertIn(b"data", recovered)

    def test_pump_discards_after_unexpected_sink_error_and_reads_to_eof(self) -> None:
        with tempfile.SpooledTemporaryFile() as stream:
            stream.write(b"all child bytes")
            stream.seek(0)
            supervisor = MagicMock()
            supervisor.write.side_effect = RuntimeError("unexpected sink failure")

            _pump(stream, supervisor, "server.log")

            supervisor.emergency_drop.assert_called_once_with(
                len(b"all child bytes"), pump_failure=True
            )

    def test_existing_oversized_active_rotates_before_pressure_drops(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "logs"
            log_dir.mkdir()
            (log_dir / "server.log").write_bytes(b"oversized-active")
            supervisor = ServiceLogSupervisor(
                log_dir,
                policy(max_bytes=4, disk_pressure_free_bytes=100),
                disk_usage=lambda _: SimpleNamespace(free=0),
            )
            self.assertEqual((log_dir / "server.log").stat().st_size, 0)
            supervisor.stdout.write(b"new")
            self.assertGreaterEqual(supervisor.snapshot()["dropped_bytes"], 3)
            supervisor.close()

    def test_periodic_worker_rotates_and_prunes_without_new_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp)
            supervisor = ServiceLogSupervisor(
                log_dir,
                policy(interval_seconds=0.05, retention_max_age_seconds=10),
                periodic_seconds=0.01,
            )
            supervisor.stdout.write(b"periodic")
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline and not list(
                log_dir.glob("server.log.*.gz")
            ):
                time.sleep(0.01)
            self.assertTrue(list(log_dir.glob("server.log.*.gz")))
            for archive in log_dir.glob("server.log.*.gz"):
                os.utime(archive, (1, 1))
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline and list(log_dir.glob("server.log.*.gz")):
                time.sleep(0.01)
            self.assertFalse(list(log_dir.glob("server.log.*.gz")))
            supervisor.close()

    def test_archive_maintenance_requests_are_coalesced(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            supervisor = ServiceLogSupervisor(Path(tmp), policy())
            supervisor.wait_for_maintenance()
            entered = threading.Event()
            release = threading.Event()
            real_maintain = supervisor._maintain

            def blocked_maintain() -> None:
                entered.set()
                if not release.wait(2):
                    raise TimeoutError("test maintenance release timed out")
                real_maintain()

            try:
                with patch.object(
                    supervisor, "_maintain", side_effect=blocked_maintain
                ) as maintain:
                    supervisor._submit_maintenance(None)
                    self.assertTrue(entered.wait(1))
                    for index in range(10_000):
                        supervisor._submit_maintenance(Path(tmp) / f"archive-{index}")
                    with supervisor._state_lock:
                        self.assertEqual(len(supervisor._futures), 1)
                        self.assertTrue(supervisor._maintenance_again)
                    release.set()
                    supervisor.wait_for_maintenance()
                    self.assertEqual(maintain.call_count, 2)
            finally:
                release.set()
                supervisor.close()

    def test_compression_waits_for_reserve_headroom(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp)
            free = [1_100]
            supervisor = ServiceLogSupervisor(
                log_dir,
                policy(
                    retention_max_total_bytes=100_000,
                    disk_pressure_free_bytes=1_000,
                ),
                disk_usage=lambda _: SimpleNamespace(free=free[0]),
            )
            supervisor.wait_for_maintenance()
            archive = log_dir / "server.log.manual"
            archive.write_bytes(b"x" * 100)

            supervisor._maintain()
            self.assertTrue(archive.exists())
            self.assertFalse(archive.with_name(archive.name + ".gz").exists())

            free[0] = 1_000_000
            supervisor._maintain()
            self.assertFalse(archive.exists())
            self.assertTrue(archive.with_name(archive.name + ".gz").exists())
            supervisor.close()

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
            self.assertIn(f"<string>{tmp}/logs/{BOOTSTRAP_FILE}</string>", plist)
            self.assertIn("ExecStart=/usr/bin/pa _service-run", unit)
            self.assertIn("StandardOutput=null", unit)
            self.assertIn(f"StandardError=append:{tmp}/logs/{BOOTSTRAP_FILE}", unit)
            for name in (
                "PA_LOG_ROTATION_MAX_BYTES",
                "PA_LOG_ROTATION_INTERVAL_SECONDS",
                "PA_LOG_RETENTION_COUNT",
                "PA_LOG_RETENTION_MAX_AGE_SECONDS",
                "PA_LOG_RETENTION_MAX_TOTAL_BYTES",
                "PA_LOG_DISK_PRESSURE_FREE_BYTES",
            ):
                self.assertIn(name, plist)
                self.assertIn(name, unit)
            self.assertEqual(
                stat.S_IMODE((Path(tmp) / "logs" / BOOTSTRAP_FILE).stat().st_mode),
                0o600,
            )

    def test_bootstrap_diagnostics_are_private_and_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = prepare_bootstrap_log(root)
            path.write_bytes(b"x" * (2 * 1024 * 1024))

            prepare_bootstrap_log(root)

            self.assertLess(path.stat().st_size, 300 * 1024)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(path.parent.stat().st_mode), 0o700)

    def test_oversized_startup_exception_is_bounded_and_identifiable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            _record_bootstrap_failure(root, RuntimeError("x" * (2 * 1024 * 1024)))

            path = root / "logs" / BOOTSTRAP_FILE
            self.assertLess(path.stat().st_size, 300 * 1024)
            self.assertIn("supervisor startup failed", path.read_text())


if __name__ == "__main__":
    unittest.main()
