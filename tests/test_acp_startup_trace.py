from __future__ import annotations

import unittest

from pa.acp.startup_trace import SessionStartupTrace
from pa.domain.models import AgentSession


class SessionStartupTraceTests(unittest.TestCase):
    def test_trace_persists_phase_boundaries_durations_and_failure_type(self) -> None:
        trace = SessionStartupTrace()
        session = AgentSession(agent_name="codex")
        trace.attach(session)

        with trace.phase("provider_resolution"):
            pass
        with self.assertRaisesRegex(RuntimeError, "secret detail"):
            with trace.phase("provider_launch"):
                raise RuntimeError("secret detail")

        snapshot = session.config_json["startup_trace"]
        self.assertEqual(snapshot["schema_version"], 1)
        self.assertFalse(snapshot["complete"])
        self.assertEqual(
            [phase["name"] for phase in snapshot["phases"]],
            ["provider_resolution", "provider_launch"],
        )
        self.assertGreaterEqual(snapshot["total_duration_ms"], 0)
        self.assertGreaterEqual(snapshot["phases"][0]["duration_ms"], 0)
        self.assertEqual(snapshot["phases"][1]["status"], "failed")
        self.assertEqual(snapshot["phases"][1]["error_type"], "RuntimeError")
        self.assertNotIn("secret detail", str(snapshot))

        trace.mark("response_readiness")
        self.assertTrue(session.config_json["startup_trace"]["complete"])
