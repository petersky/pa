"""Isolated restart acceptance server with a tool-free ACP wire provider.

Run with PA_OWNER_API_URL=http://127.0.0.1:8097 and uvicorn factory mode.
The restart hook terminates only this fixture process; the validation harness
starts it again against the same isolated data. Owner probes are real.
"""
import os
import signal
from pathlib import Path

from pa.acp.providers.registry import register_provider
from pa.config import Settings
from pa.core.kernel import Kernel
from tests.selection_browser_app import FixtureProvider


def create_app():
    import pa.cli.service

    root = Path(__file__).resolve().parents[1] / '.dev' / 'restart-validation'
    settings = Settings(
        data_dir=root / 'data', workspace_root=root / 'workspaces',
        instance_id='831156d3-f921-4b8b-a6ec-7a47bf9279b8',
        instance_name='Isolated restart acceptance',
        instance_url='http://127.0.0.1:8097', port=8097,
        sync_token='isolated-restart-fixture-only', peers=[],
        agent_provider='codex', agent_enabled=True,
        card_summary_auth_source='dedicated', card_summary_api_key='',
        card_summary_anthropic_api_key='',
    )
    def restart_fixture(actual_settings, **kwargs):
        assert actual_settings.data_dir == settings.data_dir
        os.kill(os.getpid(), signal.SIGTERM)
        return {'status': 'requested', 'operation_id': kwargs.get('operation_id')}
    pa.cli.service.request_restart = restart_fixture
    register_provider(FixtureProvider('codex'))
    kernel = Kernel.boot(settings=settings)
    # Emulate the old producer only on the first process. Cold startup uses the
    # unmodified current producer and must preserve this historical binding.
    if os.environ.get("PA_FIXTURE_LEGACY_PRODUCER") == "1":
        from pa.instance.agent_session import AgentSessionManager
        original = AgentSessionManager._prepare_workspace
        async def legacy_workspace(self, session, **kwargs):
            kwargs["fresh_admission"] = False
            return await original(self, session, **kwargs)
        AgentSessionManager._prepare_workspace = legacy_workspace
    app = kernel.build_app()

    @app.post("/fixture/queued-legacy-restart/{session_id}")
    async def queued_legacy_restart(session_id: str):
        from uuid import uuid4
        from pa.domain.models import RestartHandoff
        manager = kernel.ctx.require_service("instance_agent")
        runtime = manager.get(session_id)
        assert runtime and runtime.session.control_mode == "human"
        assert runtime.agent_env.get("PA_EXECUTION_CONTEXT")
        binding = dict(runtime.session.execution_binding)
        assert binding and not any(k in binding for k in ("dispatch_id", "realm_id", "principal_id"))
        receipt = RestartHandoff(
            session_id=session_id, idempotency_key="legacy-queued-" + str(uuid4()),
            continuation_prompt="Continue this exact human chat once automatically.",
            continuation_prompt_id="legacy-prompt-" + str(uuid4()),
            status="continuation_queued", instance_id=settings.instance_id,
            execution_binding=binding,
        )
        # This isolated server remains sole writer; enqueue accepted work without
        # starting it, then commit the actual quiesce snapshot before exit.
        receipt = await manager._offload("fixture.receipt", manager.store.create_restart_handoff, receipt)
        runtime.enqueue(receipt.continuation_prompt, prompt_id=receipt.continuation_prompt_id,
                        source="restart-handoff:" + receipt.id, _defer_drain=True)
        runtime.enqueue("Unrelated automation must remain held", prompt_id="held-automation",
                        source="reconciliation:unrelated", _defer_drain=True)
        snapshot = await manager.quiesce(reason="isolated legacy cold-start acceptance")
        saved = next(s for s in snapshot.sessions if s.session_id == session_id)
        assert {p.id for p in saved.queued_prompts} == {receipt.continuation_prompt_id, "held-automation"}
        assert not saved.queue_paused
        result = {"receipt": receipt.model_dump(mode="json"), "binding": binding,
                  "agent_env": runtime.agent_env, "snapshot": saved.model_dump(mode="json")}
        import asyncio
        asyncio.get_running_loop().call_later(.5, os.kill, os.getpid(), signal.SIGTERM)
        return result

    return app
