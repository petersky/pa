import asyncio
import unittest
from unittest.mock import MagicMock, Mock

from pa.instance.agent_session import AgentSessionRuntime


class AgentSessionLiveEventTests(unittest.TestCase):
    def test_prompting_tracks_in_flight_turn_not_connection_or_lock_cleanup(
        self,
    ) -> None:
        runtime = AgentSessionRuntime.__new__(AgentSessionRuntime)
        runtime._in_flight = None
        runtime.connection = MagicMock(prompting=True)
        runtime._prompt_lock = MagicMock()
        runtime._prompt_lock.locked.return_value = True

        self.assertFalse(runtime.prompting)

        runtime._in_flight = MagicMock()
        runtime.connection.prompting = False
        runtime._prompt_lock.locked.return_value = False

        self.assertTrue(runtime.prompting)

    def test_full_queue_keeps_newest_event_and_subscriber(self):
        runtime = AgentSessionRuntime.__new__(AgentSessionRuntime)
        runtime.session = Mock(id="session-1")
        subscriber = asyncio.Queue(maxsize=2)
        subscriber.put_nowait({"seq": 1})
        subscriber.put_nowait({"seq": 2})
        runtime._subscribers = [subscriber]

        runtime._emit_live({"seq": 3, "type": "turn_completed"})

        self.assertEqual(runtime._subscribers, [subscriber])
        self.assertEqual(subscriber.get_nowait(), {"seq": 2})
        self.assertEqual(
            subscriber.get_nowait(), {"seq": 3, "type": "turn_completed"}
        )
