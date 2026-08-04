from __future__ import annotations

import gzip
import io
import json
import logging
import os
import tempfile
import tracemalloc
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from pa.cli.logs import (
    iter_file_records,
    journal_records,
    parse_line,
    parse_since,
    show_logs,
)
from pa.config import Settings
from pa.core.log_rotation import UnsafeLogPathError
from pa.core.logging import (
    ExpectedShutdownCancellationFilter,
    JsonFormatter,
    uvicorn_log_config,
)


class LogReaderTests(unittest.TestCase):
    def settings(self, root: Path) -> Settings:
        return Settings(
            data_dir=root, instance_name="unit-test", instance_id="instance-1"
        )

    def test_combines_orders_filters_and_deduplicates_file_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logs = root / "logs"
            logs.mkdir()
            (logs / "server.log").write_text(
                "2026-08-02T10:00:02+00:00 INFO access ok\n", encoding="utf-8"
            )
            (logs / "server.err.log").write_text(
                "2026-08-02T10:00:01+00:00 WARNING [pa.owner] owner failed\n",
                encoding="utf-8",
            )
            (logs / "pa.jsonl").write_text(
                json.dumps(
                    {
                        "timestamp": "2026-08-02T10:00:01+00:00",
                        "level": "WARNING",
                        "logger": "pa.owner",
                        "message": "owner failed",
                        "card_id": "card-1",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            output, diagnostics = io.StringIO(), io.StringIO()

            show_logs(
                settings=self.settings(root),
                sources=["stdout", "stderr", "structured"],
                lines=20,
                severity="WARNING",
                output=output,
                diagnostics=diagnostics,
            )

            self.assertEqual(output.getvalue().count("owner failed"), 1)
            self.assertNotIn("access ok", output.getvalue())
            self.assertIn("stdout=", diagnostics.getvalue())
            self.assertIn("stderr=", diagnostics.getvalue())

    def test_json_is_unicode_metadata_preserving_and_redacted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logs = root / "logs"
            logs.mkdir()
            (logs / "pa.jsonl").write_text(
                json.dumps(
                    {
                        "timestamp": "2026-08-02T10:00:01Z",
                        "level": "ERROR",
                        "logger": "pa.dispatch",
                        "message": "échec authorization: Bearer abcdef cookie=session-value",
                        "dispatch_id": "dispatch-1",
                    }
                )
                + "\n"
            )
            output = io.StringIO()

            show_logs(
                settings=self.settings(root),
                sources=["structured"],
                lines=5,
                json_output=True,
                output=output,
                diagnostics=io.StringIO(),
            )

            item = json.loads(output.getvalue())
            self.assertEqual(item["dispatch_id"], "dispatch-1")
            self.assertEqual(item["instance_id"], "instance-1")
            self.assertIn("échec", item["message"])
            self.assertNotIn("abcdef", output.getvalue())
            self.assertNotIn("session-value", output.getvalue())

    def test_initial_tail_includes_compressed_rotated_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logs = root / "logs"
            logs.mkdir()
            with gzip.open(logs / "server.log.20260801.gz", "wt") as archive:
                archive.write("2026-08-01T00:00:00Z INFO archived-access\n")
            (logs / "server.log").write_text(
                "2026-08-02T00:00:00Z INFO active-access\n"
            )
            output = io.StringIO()

            show_logs(
                settings=self.settings(root),
                sources=["stdout"],
                lines=2,
                output=output,
                diagnostics=io.StringIO(),
            )

            self.assertIn("archived-access", output.getvalue())
            self.assertIn("active-access", output.getvalue())

    def test_numbered_structured_archives_are_read_oldest_first(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logs = root / "logs"
            logs.mkdir()
            for suffix, stamp, message in (
                ("2", "2026-08-01T00:00:00Z", "oldest"),
                ("1", "2026-08-02T00:00:00Z", "newer"),
                ("", "2026-08-03T00:00:00Z", "active"),
            ):
                path = logs / ("pa.jsonl" + (f".{suffix}" if suffix else ""))
                path.write_text(
                    json.dumps(
                        {
                            "timestamp": stamp,
                            "level": "INFO",
                            "logger": "pa.test",
                            "message": message,
                        }
                    )
                    + "\n"
                )
            output = io.StringIO()

            show_logs(
                settings=self.settings(root),
                sources=["structured"],
                lines=3,
                output=output,
                diagnostics=io.StringIO(),
            )

            messages = [
                line.rsplit(" ", 1)[-1] for line in output.getvalue().splitlines()
            ]
            self.assertEqual(messages, ["oldest", "newer", "active"])

    def test_storage_diagnostics_expose_bounded_log_health(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logs = root / "logs"
            logs.mkdir()
            (logs / "server.log").write_text("active\n")
            (logs / "status.json").write_text(
                json.dumps(
                    {
                        "current_bytes": 7,
                        "total_bytes": 11,
                        "oldest_age_seconds": 12,
                        "rotation_failures": 2,
                        "compression_failures": 4,
                        "prune_failures": 3,
                        "pump_failures": 5,
                        "status_failures": 7,
                        "bootstrap_failures": 8,
                        "dropped_bytes": 6,
                        "last_error": "OSError: disk full",
                        "supervisor": {
                            "state": "running",
                            "ownership": "exclusive",
                        },
                        "disk_pressure": {
                            "state": "pressure",
                            "free_bytes": 9,
                            "minimum_free_bytes": 10,
                        },
                    }
                )
            )
            diagnostics = io.StringIO()

            show_logs(
                settings=self.settings(root),
                sources=["stdout"],
                lines=1,
                output=io.StringIO(),
                diagnostics=diagnostics,
            )

            self.assertIn("current_bytes=7", diagnostics.getvalue())
            self.assertIn("rotation_failures=2", diagnostics.getvalue())
            self.assertIn("compression_failures=4", diagnostics.getvalue())
            self.assertIn("prune_failures=3", diagnostics.getvalue())
            self.assertIn("pump_failures=5", diagnostics.getvalue())
            self.assertIn("status_failures=7", diagnostics.getvalue())
            self.assertIn("bootstrap_failures=8", diagnostics.getvalue())
            self.assertIn("dropped_bytes=6", diagnostics.getvalue())
            self.assertIn("disk_pressure=pressure", diagnostics.getvalue())
            self.assertIn("free_bytes=9", diagnostics.getvalue())
            self.assertIn("minimum_free_bytes=10", diagnostics.getvalue())
            self.assertIn("supervisor=running", diagnostics.getvalue())
            self.assertIn("ownership=exclusive", diagnostics.getvalue())
            self.assertIn("last_error=OSError: disk full", diagnostics.getvalue())

    def test_streaming_reader_rejects_symlink_without_reading_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "outside.log"
            target.write_text("private target data\n")
            link = root / "server.log"
            link.symlink_to(target)

            with self.assertRaises((OSError, UnsafeLogPathError)):
                list(iter_file_records(link, "stdout"))

            self.assertEqual(target.read_text(), "private target data\n")

    def test_missing_source_is_actionable_and_all_missing_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            diagnostics = io.StringIO()
            with self.assertRaisesRegex(RuntimeError, "No readable log sources"):
                show_logs(
                    settings=self.settings(Path(tmp)),
                    sources=["stderr"],
                    lines=5,
                    diagnostics=diagnostics,
                )
            self.assertIn("start/restart PA", diagnostics.getvalue())

    def test_unreadable_source_is_actionable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "logs" / "server.log"
            path.parent.mkdir()
            path.write_text("line\n")
            with (
                patch("pa.cli.logs.os.access", return_value=False),
                self.assertRaisesRegex(RuntimeError, "ownership and permissions"),
            ):
                show_logs(
                    settings=self.settings(Path(tmp)),
                    sources=["stdout"],
                    lines=5,
                    diagnostics=io.StringIO(),
                )

    def test_relative_and_iso_since(self) -> None:
        now = datetime(2026, 8, 2, 12, tzinfo=UTC)
        self.assertEqual(parse_since("2h", now=now).hour, 10)
        self.assertEqual(parse_since("2026-08-02T09:00:00Z").tzinfo, UTC)
        with self.assertRaisesRegex(ValueError, "ISO-8601"):
            parse_since("yesterday")

    def test_parse_legacy_and_structured_lines(self) -> None:
        fallback = datetime(2026, 8, 2, tzinfo=UTC)
        self.assertEqual(
            parse_line("INFO: Started server", "stderr", fallback).logger, "uvicorn"
        )
        structured = parse_line(
            '{"timestamp":"2026-08-02T01:02:03Z","level":"ERROR","logger":"pa.x","message":"boom","session_id":"s"}',
            "structured",
            fallback,
        )
        self.assertEqual(structured.fields, {"session_id": "s"})

    def test_follow_detects_rotation_and_truncation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "logs" / "server.log"
            path.parent.mkdir()
            path.write_text("old\n")
            output = io.StringIO()

            calls = 0

            def rotate_then_stop(_: float) -> None:
                nonlocal calls
                calls += 1
                if calls == 1:
                    replacement = path.with_suffix(".new")
                    replacement.write_text("rotated ✓\n")
                    os.replace(replacement, path)
                else:
                    raise KeyboardInterrupt

            with patch("pa.cli.logs.time.sleep", side_effect=rotate_then_stop):
                show_logs(
                    settings=self.settings(root),
                    sources=["stdout"],
                    lines=0,
                    follow=True,
                    output=output,
                    diagnostics=io.StringIO(),
                )
            self.assertIn("rotated ✓", output.getvalue())

    def test_follow_drains_old_inode_before_opening_rotated_active_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "logs" / "server.log"
            path.parent.mkdir()
            path.write_text("initial\n")
            writer = path.open("a")
            output = io.StringIO()
            calls = 0

            def rotate_then_stop(_: float) -> None:
                nonlocal calls
                calls += 1
                if calls == 1:
                    writer.write("last-old-inode\n")
                    writer.flush()
                    os.replace(path, path.with_name("server.log.1"))
                    path.write_text("first-new-inode\n")
                else:
                    raise KeyboardInterrupt

            try:
                with patch("pa.cli.logs.time.sleep", side_effect=rotate_then_stop):
                    show_logs(
                        settings=self.settings(root),
                        sources=["stdout"],
                        lines=0,
                        follow=True,
                        output=output,
                        diagnostics=io.StringIO(),
                    )
            finally:
                writer.close()
            self.assertIn("last-old-inode", output.getvalue())
            self.assertIn("first-new-inode", output.getvalue())

    def test_follow_detects_copytruncate_that_regrows_past_prior_offset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "logs" / "server.log"
            path.parent.mkdir()
            path.write_text("old-" + ("x" * 200) + "\n")
            output = io.StringIO()
            calls = 0

            def truncate_regrow_then_stop(_: float) -> None:
                nonlocal calls
                calls += 1
                if calls == 1:
                    with path.open("w") as handle:
                        handle.write("copytruncate-first-line\n" + ("z" * 400) + "\n")
                else:
                    raise KeyboardInterrupt

            with patch("pa.cli.logs.time.sleep", side_effect=truncate_regrow_then_stop):
                show_logs(
                    settings=self.settings(root),
                    sources=["stdout"],
                    lines=0,
                    follow=True,
                    output=output,
                    diagnostics=io.StringIO(),
                )

            self.assertIn("copytruncate-first-line", output.getvalue())

    def test_tail_streams_highly_compressed_history_with_bounded_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logs = root / "logs"
            logs.mkdir()
            archive = logs / "server.log.20260801.gz"
            with gzip.open(archive, "wt") as handle:
                for index in range(50_000):
                    handle.write(
                        f"2026-08-01T00:00:00Z INFO unique-{index:05d}-{'x' * 120}\n"
                    )
            (logs / "server.log").write_text(
                "2026-08-02T00:00:00Z INFO latest-record\n"
            )
            output = io.StringIO()

            tracemalloc.start()
            show_logs(
                settings=self.settings(root),
                sources=["stdout"],
                lines=1,
                output=output,
                diagnostics=io.StringIO(),
            )
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            self.assertIn("latest-record", output.getvalue())
            self.assertLess(peak, 20 * 1024 * 1024)

    def test_supervisor_bootstrap_log_is_a_first_class_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logs = root / "logs"
            logs.mkdir()
            (logs / "supervisor-bootstrap.log").write_text(
                "2026-08-02T00:00:00Z ERROR bootstrap-visible\n"
            )
            output = io.StringIO()

            show_logs(
                settings=self.settings(root),
                sources=["supervisor"],
                lines=5,
                output=output,
                diagnostics=io.StringIO(),
            )

            self.assertIn("bootstrap-visible", output.getvalue())

    def test_journal_json_is_parsed(self) -> None:
        row = json.dumps(
            {
                "__REALTIME_TIMESTAMP": "1785657600000000",
                "MESSAGE": "hello",
                "PRIORITY": "4",
                "SYSLOG_IDENTIFIER": "pa",
                "_PID": "2",
            }
        )
        completed = MagicMock(returncode=0, stdout=row, stderr="")
        with patch("pa.cli.logs.subprocess.run", return_value=completed) as run:
            records = journal_records(10)
        self.assertEqual(records[0].source, "journal")
        self.assertEqual(records[0].level, "4")
        self.assertIn("--user", run.call_args.args[0])


class LoggingConfigurationTests(unittest.TestCase):
    def test_uvicorn_human_formats_have_timestamps(self) -> None:
        config = uvicorn_log_config()
        self.assertIn("asctime", config["formatters"]["default"]["fmt"])
        self.assertIn("asctime", config["formatters"]["access"]["fmt"])
        self.assertIn("expected_shutdown_cancellation", config["filters"])
        self.assertIn(
            "expected_shutdown_cancellation",
            config["handlers"]["default"]["filters"],
        )

    def test_json_formatter_preserves_ids_and_redacts_extra_strings(self) -> None:
        record = logging.LogRecord(
            "pa.test", logging.WARNING, __file__, 1, "problem", (), None
        )
        record.card_id = "card-1"
        record.header = "authorization: Bearer secret-token"
        payload = json.loads(JsonFormatter().format(record))
        self.assertEqual(payload["card_id"], "card-1")
        self.assertNotIn("secret-token", json.dumps(payload))

    def test_expected_shutdown_cancellation_is_summarized(self) -> None:
        import asyncio

        record = logging.LogRecord(
            "uvicorn.error",
            logging.ERROR,
            __file__,
            1,
            "ASGI failure",
            (),
            (asyncio.CancelledError, asyncio.CancelledError(), None),
        )
        with patch("pa.server.shutdown.is_shutting_down", return_value=True):
            self.assertTrue(ExpectedShutdownCancellationFilter().filter(record))
        self.assertEqual(record.levelname, "WARNING")
        self.assertIsNone(record.exc_info)
        self.assertIn("expected shutdown cancellation", record.msg)

    def test_genuine_asgi_error_remains_visible(self) -> None:
        record = logging.LogRecord(
            "uvicorn.error",
            logging.ERROR,
            __file__,
            1,
            "ASGI failure",
            (),
            (RuntimeError, RuntimeError("boom"), None),
        )
        ExpectedShutdownCancellationFilter().filter(record)
        self.assertEqual(record.levelname, "ERROR")
        self.assertIsNotNone(record.exc_info)

    def test_shutdown_flag_classifies_cancellation_without_event_loop(self) -> None:
        import asyncio

        from pa.server.shutdown import reset_shutdown_event, signal_shutdown

        reset_shutdown_event()
        signal_shutdown()
        record = logging.LogRecord(
            "uvicorn.error",
            logging.ERROR,
            __file__,
            1,
            "Exception in ASGI application",
            (),
            (asyncio.CancelledError, asyncio.CancelledError(), None),
        )
        try:
            self.assertTrue(ExpectedShutdownCancellationFilter().filter(record))
            self.assertEqual(record.levelname, "WARNING")
            self.assertIsNone(record.exc_info)
        finally:
            reset_shutdown_event()


if __name__ == "__main__":
    unittest.main()
