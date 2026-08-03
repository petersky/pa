"""Smoke a wheel-installed PA MCP server through its real stdio transport."""

from __future__ import annotations

import tempfile
from pathlib import Path

from pa.acp.mcp_config import probe_pa_mcp_stdio
from pa.config import Settings


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="pa-packaged-mcp-") as root:
        settings = Settings(
            data_dir=Path(root),
            instance_id="packaged-mcp-smoke",
            host="127.0.0.1",
            port=9123,
            agent_enabled=False,
        )
        result = probe_pa_mcp_stdio(
            settings,
            timeout=20,
            owner_environment={"PA_OWNER_SOCKET": str(Path(root) / "owner.sock")},
            session_environment={},
        )
        if result.get("state") != "connected" or result.get("tool_count", 0) < 1:
            raise RuntimeError(f"packaged PA MCP smoke failed: {result}")


if __name__ == "__main__":
    main()
