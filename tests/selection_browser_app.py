"""Isolated server-owned visual/integration fixture; never uses production state.

Run: PA_OWNER_API_URL=http://127.0.0.1:8096 uv run uvicorn tests.selection_browser_app:create_app --factory --port 8096
All providers are tool-free wire fixtures, not production accounts or agents.
"""

import sys
from pathlib import Path

from pa.acp.providers.base import AgentProviderSpec, ProviderStatus
from pa.acp.providers.registry import register_provider
from pa.config import Settings
from pa.core.kernel import Kernel
from tests.fixtures.selection_acp_agent import advertised


class FixtureProvider:
    def __init__(self, harness):
        self.id, self.display_name = harness, harness + " (test fixture)"

    def resolve_spawn(self, **kwargs):
        return AgentProviderSpec(
            id=self.id,
            display_name=self.display_name,
            command=sys.executable,
            args=[
                str(Path(__file__).parent / "fixtures" / "selection_acp_agent.py"),
                self.id,
            ],
            env=kwargs.get("extra_env") or {},
            collaboration_modes=["default"],
            session_load_supported=True,
        )

    def default_spec(self):
        return self.resolve_spawn()

    def status(self, data_dir):
        data = advertised(self.id)
        return ProviderStatus(
            id=self.id,
            display_name=self.display_name,
            installed=False,
            available=True,
            auth_state="authenticated",
            auth_evidence=["tool_free_wire_fixture"],
            last_probe={"ok": True},
            meta={
                "options": data,
                "model_provider": {
                    "codex": "openai",
                    "cursor": "cursor-account",
                    "openinterpreter": "minimax",
                }[self.id],
            },
        )

    def probe(self, data_dir):
        return {"ok": True, "source": "tool_free_wire_fixture"}


def create_app():
    import pa.acp.client

    # This standalone visual fixture has no PA MCP owner listener. Its agents
    # are deliberately tool-free and cannot start or call any MCP tool.
    pa.acp.client.pa_mcp_servers = lambda *args, **kwargs: []
    root = Path(__file__).resolve().parents[1]
    for harness in ("codex", "cursor", "openinterpreter"):
        register_provider(FixtureProvider(harness))
    settings = Settings(
        data_dir=root / ".dev" / "selection-browser-v2-data",
        workspace_root=root / ".dev" / "selection-browser-v2-workspaces",
        instance_id="f903e4b2-2820-4c92-b2d6-7e8707787a16",
        instance_name="Selection visual fixture",
        instance_url="http://127.0.0.1:8096",
        port=8096,
        sync_token="isolated-loopback-selection-fixture",
        peers=[],
        agent_provider="codex",
        agent_enabled=True,
        card_summary_auth_source="dedicated",
        card_summary_api_key="",
        card_summary_anthropic_api_key="",
    )
    return Kernel.boot(settings=settings).build_app()
