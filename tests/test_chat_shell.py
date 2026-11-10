"""`!command` in the chat composer: access policy, bounded execution and how results are recorded."""
import asyncio
import time
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from pa.modules import chat_shell
from pa.modules.chat_shell import (
    ShellBody, _Capture, child_environment, format_for_agent, run_command, run_shell, shell_available, stop_shell,
)
from tests.test_conversation_issues import setup  # noqa: F401  shared isolated store/request fixture


class FakeRuntime:
    def __init__(self):
        self.events = []

    def _append_transcript(self, event_type, payload):
        self.events.append((event_type, payload))

    async def _drain_transcripts(self):
        return None


@pytest.fixture
def world(setup, tmp_path, monkeypatch):  # noqa: F811
    store, manager, request, app = setup
    workdir = tmp_path / "work"
    workdir.mkdir()
    session = store.get_session("discussion")
    session.cwd = str(workdir)
    store.save_session(session)
    runtime = FakeRuntime()

    async def live(_request, _session_id):
        return runtime

    monkeypatch.setattr("pa.modules.agent_chat._runtime_or_404_async", live)
    chat_shell._recent.clear()
    chat_shell._running.clear()
    return SimpleNamespace(request=request, runtime=runtime, workdir=workdir, settings=app.state.ctx.settings, store=store)


async def run(world, command, principal="user:test", **body):
    return await run_shell(world.request(principal=principal), "discussion", ShellBody(command=command, **body))


# ------------------------------------------------------------------ execution


@pytest.mark.asyncio
async def test_output_exit_code_and_stderr_are_captured_in_order(tmp_path):
    result = await run_command("s", "echo out; echo err >&2; exit 3", str(tmp_path), timeout=10, limit=1000)
    assert result["output"] == "out\nerr\n"
    assert result["exit_code"] == 3 and not result["timed_out"] and not result["truncated"]


@pytest.mark.asyncio
async def test_a_command_never_waits_on_stdin_and_colours_are_stripped(tmp_path):
    result = await run_command("s", "cat; printf '\\033[31mred\\033[0m'", str(tmp_path), timeout=5, limit=1000)
    assert result["output"] == "red" and result["exit_code"] == 0


@pytest.mark.asyncio
async def test_timeout_kills_the_whole_process_group(tmp_path):
    marker = tmp_path / "survivor"
    started = time.monotonic()
    result = await run_command("s", f"(sleep 1.5; touch {marker}) & sleep 30", str(tmp_path), timeout=0.4, limit=1000)
    assert result["timed_out"] and time.monotonic() - started < 10
    await asyncio.sleep(2)
    assert not marker.exists()  # the backgrounded child died with the group


@pytest.mark.asyncio
async def test_long_output_keeps_its_start_and_its_end(tmp_path):
    result = await run_command("s", "seq 1 20000", str(tmp_path), timeout=20, limit=2000)
    assert result["truncated"] and result["omitted_chars"] > 0
    assert result["output"].startswith("1\n2\n") and result["output"].rstrip().endswith("20000")
    assert "characters omitted" in result["output"]
    assert len(result["output"]) < 2200


def test_capture_is_exact_below_the_limit():
    capture = _Capture(100)
    capture.feed("héllo\r\nworld".encode())
    assert capture.result() == ("héllo\nworld", False, 0)


def test_pa_credentials_are_not_passed_to_commands():
    env = child_environment({"PA_SESSION_SECRET": "x", "PA_API_TOKEN": "y", "PA_DATA_DIR": "/d", "GITHUB_TOKEN": "g", "PATH": "/bin"})
    assert "PA_SESSION_SECRET" not in env and "PA_API_TOKEN" not in env
    assert env["PA_DATA_DIR"] == "/d" and env["GITHUB_TOKEN"] == "g" and env["TERM"] == "dumb"


def test_agent_prompt_shows_command_status_and_survives_backticks_in_output():
    text = format_for_agent({
        "command": "echo '```'", "cwd": "/w", "output": "```\nboom\n```\n", "exit_code": 1, "signal": None,
        "timed_out": False, "cancelled": False, "truncated": True, "duration_ms": 1500,
    })
    assert "$ echo '```'" in text and "exit 1 in 1.5s" in text and "output shortened" in text
    assert "````\n```\nboom\n```\n````" in text  # the fence outgrows the output's own fences


# ------------------------------------------------------------------ endpoint


@pytest.mark.asyncio
async def test_output_goes_to_the_agent_by_default_without_a_separate_event(world):
    result = await run(world, "echo hi")
    assert result["sent_to_agent"] and "$ echo hi" in result["prompt"] and "hi" in result["prompt"]
    assert result["cwd"] == str(world.workdir) and world.runtime.events == []


@pytest.mark.asyncio
async def test_local_only_runs_are_recorded_in_the_transcript(world):
    result = await run(world, "echo quiet", send_to_agent=False)
    assert not result["sent_to_agent"] and "prompt" not in result
    (event_type, payload), = world.runtime.events
    assert event_type == "shell_result" and payload["output"] == "quiet\n" and payload["id"] == result["id"]
    assert "prompt" not in payload


@pytest.mark.asyncio
async def test_the_instance_default_can_be_flipped(world):
    world.settings.chat_shell_output_to_agent = False
    assert not (await run(world, "true"))["sent_to_agent"]
    assert (await run(world, "true", send_to_agent=True))["sent_to_agent"]


@pytest.mark.asyncio
async def test_commands_run_in_the_conversations_working_directory(world):
    (world.workdir / "marker.txt").write_text("x")
    assert "marker.txt" in (await run(world, "ls"))["output"]


@pytest.mark.asyncio
async def test_access_can_be_disabled_or_limited_to_signed_in_users(world):
    world.settings.chat_shell_access = "disabled"
    with pytest.raises(HTTPException) as off:
        await run(world, "true")
    assert off.value.status_code == 403
    world.settings.chat_shell_access = "signed_in"
    anonymous = world.request()
    assert not shell_available(anonymous)
    with pytest.raises(HTTPException) as login:
        await run_shell(anonymous, "discussion", ShellBody(command="true"))
    assert login.value.status_code == 401
    signed_in = world.request()
    signed_in.state.user_authenticated = True
    assert shell_available(signed_in)
    assert (await run_shell(signed_in, "discussion", ShellBody(command="true")))["exit_code"] == 0


@pytest.mark.asyncio
async def test_only_the_owner_or_an_admin_may_run_commands(world):
    with pytest.raises(HTTPException) as denied:
        await run(world, "true", principal="user:other")
    assert denied.value.status_code == 403
    admin = world.request(principal="user:other")
    admin.state.user = SimpleNamespace(role="admin")
    assert (await run_shell(admin, "discussion", ShellBody(command="true")))["exit_code"] == 0
    with pytest.raises(HTTPException) as missing:
        await run_shell(world.request(), "nope", ShellBody(command="true"))
    assert missing.value.status_code == 404


@pytest.mark.asyncio
async def test_a_conversation_without_a_directory_cannot_run_commands(world):
    session = world.store.get_session("discussion")
    session.cwd = None
    world.store.save_session(session)
    with pytest.raises(HTTPException) as refused:
        await run(world, "true")
    assert refused.value.status_code == 409 and refused.value.detail["code"] == "shell_no_workspace"


@pytest.mark.asyncio
async def test_a_retried_request_returns_the_first_result_instead_of_running_twice(world):
    counter = world.workdir / "count"
    first = await run(world, f"echo x >> {counter}", client_id="same")
    again = await run(world, f"echo x >> {counter}", client_id="same")
    assert again["id"] == first["id"] and again["duplicate"] and counter.read_text() == "x\n"


@pytest.mark.asyncio
async def test_one_command_at_a_time_and_it_can_be_stopped(world):
    slow = asyncio.create_task(run(world, "sleep 30", send_to_agent=False))
    for _ in range(100):
        if "discussion" in chat_shell._running:
            break
        await asyncio.sleep(0.02)
    with pytest.raises(HTTPException) as busy:
        await run(world, "true")
    assert busy.value.status_code == 409 and busy.value.detail["code"] == "shell_busy"
    assert (await stop_shell(world.request(), "discussion")) == {"stopped": True}
    result = await asyncio.wait_for(slow, 10)
    assert result["cancelled"] and result["exit_code"] is None
    assert (await stop_shell(world.request(), "discussion")) == {"stopped": False}
