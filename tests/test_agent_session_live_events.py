import asyncio
import unittest
from unittest.mock import Mock

from pa.instance.agent_session import AgentSessionRuntime


class AgentSessionLiveEventTests(unittest.TestCase):
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
