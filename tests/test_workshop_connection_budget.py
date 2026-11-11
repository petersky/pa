"""The Workshop page must leave browser connections free for navigation."""

import inspect
import re
from pathlib import Path

from pa.modules import fleet

JS = Path(__file__).resolve().parents[1] / "src" / "pa" / "server" / "static" / "js"


def test_floor_opens_no_per_peer_stream_from_the_page():
    source = (JS / "workshop.js").read_text()
    body = re.search(
        r"function floorTransportInstances\(data\) \{(.*?)\n  \}\n", source, re.S
    ).group(1)
    assert "bay.local" in body
    assert "/api/fleet/instances/" not in body
    assert "connectivity" not in body


def test_agent_proxy_fails_fast_on_an_unreachable_peer():
    source = inspect.getsource(fleet)
    assert "httpx.Timeout(120.0, connect=5.0, read=read_timeout)" in source
    assert "httpx.Timeout(120.0, read=read_timeout)" not in source
