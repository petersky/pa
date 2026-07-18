from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pa.domain.models import TranscriptEvent
from pa.domain.projection import CardProjection


class TranscriptPaginationTests(unittest.TestCase):
    def test_reverse_pages_are_chronological_exclusive_and_contiguous(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CardProjection(Path(tmp) / "pa.db")
            store.append_transcript_events(
                [
                    TranscriptEvent(
                        session_id="sess-long",
                        seq=seq,
                        event_type="message",
                        payload={"text": str(seq)},
                    )
                    for seq in range(1, 6002)
                ]
            )

            newest_with_sentinel = store.list_transcript_events_before(
                "sess-long",
                limit=1001,
            )
            newest = newest_with_sentinel[-1000:]
            older_with_sentinel = store.list_transcript_events_before(
                "sess-long",
                before_seq=newest[0].seq,
                limit=1001,
            )
            older = older_with_sentinel[-1000:]
            first = store.list_transcript_events_before(
                "sess-long",
                before_seq=2,
                limit=1001,
            )

        self.assertEqual([event.seq for event in newest], list(range(5002, 6002)))
        self.assertEqual([event.seq for event in older], list(range(4002, 5002)))
        self.assertEqual(older[-1].seq + 1, newest[0].seq)
        self.assertEqual([event.seq for event in first], [1])


if __name__ == "__main__":
    unittest.main()
