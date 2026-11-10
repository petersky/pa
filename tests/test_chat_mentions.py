"""`@` mention search: files, cards, conversations and users, scoped to what the caller may see."""
import subprocess
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from pa.domain.models import AgentSession, CardCreate
from pa.modules import chat_mentions
from pa.modules.chat_mentions import extract_tokens, mentions, parse_query, record_prompt_mentions, resolve_prompt_mentions
from tests.test_conversation_issues import setup  # noqa: F401  shared isolated store/request fixture


@pytest.fixture
def workspace(tmp_path):
    root = tmp_path / "repo"
    (root / "src/app").mkdir(parents=True)
    (root / "docs").mkdir()
    (root / "src/app/server.py").write_text("print()\n")
    (root / "src/app/settings.py").write_text("x = 1\n")
    (root / "docs/design notes.md").write_text("spaces in the name\n")
    (root / "docs/shot.png").write_bytes(b"\x89PNG")
    (root / "ignored.log").write_text("noise\n")
    (root / ".gitignore").write_text("*.log\n")
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    chat_mentions._file_index.clear()
    return root


@pytest.fixture
def world(setup, workspace):  # noqa: F811
    store, manager, request, app = setup
    session = store.get_session("discussion")
    session.cwd = str(workspace)
    store.save_session(session)
    store.save_session(AgentSession(id="other-chat", agent_name="codex", purpose="chat", principal_id="user:test", title="Release planning"))
    store.save_session(AgentSession(id="foreign-chat", agent_name="codex", purpose="chat", principal_id="user:other", title="Release secrets"))
    store.save_session(AgentSession(id="worker-run", agent_name="codex", purpose="automated_run", principal_id="user:test", title="Release worker"))
    card = store.create_card(CardCreate(title="Fix release pipeline"))
    users = SimpleNamespace(list_users=lambda: [
        SimpleNamespace(id="u1", username="ada", display_name="Ada Lovelace"),
        SimpleNamespace(id="u2", username="grace", display_name=""),
    ])
    app.state.ctx.services["users"] = users
    return SimpleNamespace(request=request, card=card, store=store, manager=manager)


async def search(world, q, principal="user:test"):
    return await mentions(world.request(principal=principal), "discussion", q)


def pairs(result, kind=None):
    return [(item["kind"], item["label"]) for item in result["items"] if kind in (None, item["kind"])]


def test_a_kind_prefix_names_the_search_and_anything_else_searches_everything():
    assert parse_query("card:fix") == ("card", "fix")
    assert parse_query("File:src") == ("file", "src")
    assert parse_query("src/app") == (None, "src/app")
    assert parse_query("https://x") == (None, "https://x")  # an unknown prefix is just text


@pytest.mark.asyncio
async def test_files_come_from_the_working_tree_without_ignored_or_unmentionable_names(world):
    found = await search(world, "set")
    files = [item for item in found["items"] if item["kind"] == "file"]
    assert [item["id"] for item in files] == ["src/app/settings.py"]
    assert files[0]["insert"] == "@src/app/settings.py" and files[0]["detail"] == "src/app/settings.py"
    everything = {item["id"] for item in (await search(world, "file:"))["items"]}
    assert "ignored.log" not in everything  # .gitignore is honored
    assert "docs/design notes.md" not in everything  # a mention cannot contain whitespace


@pytest.mark.asyncio
async def test_basename_matches_outrank_directory_matches(world):
    ids = [item["id"] for item in (await search(world, "file:server"))["items"]]
    assert ids[0] == "src/app/server.py"
    # "app" is only in a directory name; both files still match, after any basename hit.
    assert set(item["id"] for item in (await search(world, "file:app"))["items"]) >= {"src/app/server.py", "src/app/settings.py"}


@pytest.mark.asyncio
async def test_images_are_marked_for_attachment_instead_of_text(world, workspace):
    shot = next(item for item in (await search(world, "file:shot"))["items"])
    assert shot["action"] == "attach_image" and shot["mime_type"] == "image/png"
    assert shot["path"] == str(workspace / "docs/shot.png")
    text = next(item for item in (await search(world, "file:server"))["items"])
    assert "action" not in text


@pytest.mark.asyncio
async def test_cards_and_users_insert_stable_references(world):
    card = (await search(world, "card:release"))["items"][0]
    assert card["insert"] == f"@card:{world.card.id}" and card["label"] == "Fix release pipeline"
    users = (await search(world, "user:"))["items"]
    assert {item["insert"] for item in users} == {"@user:ada", "@user:grace"}
    assert pairs(await search(world, "user:lovel"), "user") == [("user", "Ada Lovelace")]


@pytest.mark.asyncio
async def test_conversations_exclude_this_one_other_owners_and_automation(world):
    found = await search(world, "session:")
    assert [item["id"] for item in found["items"]] == ["other-chat"]
    assert found["items"][0]["insert"] == "@session:other-chat"
    typed = await search(world, "session:release")
    assert [item["label"] for item in typed["items"]] == ["Release planning"]
    admin = world.request(principal="user:test")
    admin.state.user = SimpleNamespace(role="admin")
    seen = {item["id"] for item in (await mentions(admin, "discussion", "session:"))["items"]}
    assert seen == {"other-chat", "foreign-chat"}


@pytest.mark.asyncio
async def test_unfiltered_search_is_capped_per_kind_and_other_principals_are_refused(world):
    capped = await search(world, "")
    kinds = [item["kind"] for item in capped["items"]]
    assert kinds.count("file") <= 6 and kinds.count("card") <= 4 and set(kinds) >= {"file", "card", "session", "user"}
    with pytest.raises(HTTPException) as denied:
        await search(world, "", principal="user:other")
    assert denied.value.status_code == 403
    with pytest.raises(HTTPException) as missing:
        await mentions(world.request(), "no-such-session", "")
    assert missing.value.status_code == 404


@pytest.mark.asyncio
async def test_a_session_without_a_working_directory_simply_has_no_files(world, setup):  # noqa: F811
    store = setup[0]
    session = store.get_session("discussion")
    session.cwd = None
    store.save_session(session)
    assert [kind for kind, _ in pairs(await search(world, ""))].count("file") == 0


# ------------------------------------------------------------------ server-side resolution


def test_tokens_are_read_like_the_composer_does():
    text = "see @src/app/server.py, @card:abc. mail ada@example.com and (@no) @card:abc @user:ada!"
    assert extract_tokens(text) == ["@src/app/server.py", "@card:abc", "@user:ada"]
    assert extract_tokens("no mentions, just an @") == []


def resolve(world, text, principal="user:test", admin=False):
    request = world.request(principal=principal)
    if admin:
        request.state.user = SimpleNamespace(role="admin")
    session = world.store.get_session("discussion")
    return {m["token"]: m for m in resolve_prompt_mentions(request, session, world.manager, text)}


def test_each_kind_resolves_to_a_label_and_link(world):
    found = resolve(world, f"@src/app/server.py @docs/shot.png @card:{world.card.id} @session:other-chat @user:ada")
    assert found["@src/app/server.py"]["ok"] and found["@src/app/server.py"]["href"].startswith("/api/files/raw?path=")
    assert found["@docs/shot.png"]["image"] is True
    assert found[f"@card:{world.card.id}"]["label"] == "Fix release pipeline"
    assert f"card={world.card.id}" in found[f"@card:{world.card.id}"]["href"]
    assert found["@session:other-chat"]["href"] == "/agent?session=other-chat"
    assert found["@user:ada"]["label"] == "Ada Lovelace" and "href" not in found["@user:ada"]


def test_unknown_explicit_mentions_are_flagged_but_bare_words_are_left_alone(world):
    found = resolve(world, "@card:nope @user:nobody @session:ghost @file:missing.txt @here @missing.txt")
    assert {token: m["ok"] for token, m in found.items()} == {
        "@card:nope": False, "@user:nobody": False, "@session:ghost": False, "@file:missing.txt": False,
    }


def test_mentions_never_reach_outside_the_working_directory_or_other_peoples_conversations(world, tmp_path):
    (tmp_path / "outside.txt").write_text("secret")
    assert resolve(world, "@../outside.txt") == {}  # a bare path outside the directory is not a mention at all
    flagged = resolve(world, "@file:../outside.txt @session:foreign-chat @session:worker-run")
    assert not any(m["ok"] for m in flagged.values()) and len(flagged) == 3
    assert resolve(world, "@session:foreign-chat", admin=True)["@session:foreign-chat"]["ok"]


@pytest.mark.asyncio
async def test_accepting_a_prompt_records_what_its_mentions_resolved_to(world):
    events = []
    live = SimpleNamespace(_append_transcript=lambda kind, payload: events.append((kind, payload)))

    async def drained():
        return None

    live._drain_transcripts = drained
    world.manager.get = lambda session_id: live
    await record_prompt_mentions(world.request(), "discussion", "p-1", "look at @src/app/server.py and @card:nope")
    (kind, payload), = events
    assert kind == "prompt_mentions" and payload["prompt_id"] == "p-1"
    assert [(m["token"], m["ok"]) for m in payload["mentions"]] == [("@src/app/server.py", True), ("@card:nope", False)]
    events.clear()
    await record_prompt_mentions(world.request(), "discussion", "p-2", "nothing to see")
    await record_prompt_mentions(world.request(), "discussion", None, "@card:nope")
    assert events == []


@pytest.mark.asyncio
async def test_only_a_newly_accepted_prompt_records_mentions(world, monkeypatch):
    from pa.modules import agent_chat

    recorded = []

    async def record(_request, session_id, prompt_id, text):
        recorded.append((session_id, prompt_id, text))

    monkeypatch.setattr(chat_mentions, "record_prompt_mentions", record)
    replies = iter([
        {"accepted": True, "prompt_id": "p-1"},
        {"accepted": True, "prompt_id": "p-1", "duplicate": True},
        {"accepted": False},
    ])

    async def admitted(*_args):
        return next(replies)

    monkeypatch.setattr(agent_chat, "_session_prompt_admitted", admitted)
    body = SimpleNamespace(message="hi @card:x")
    for _ in range(3):
        await agent_chat.session_prompt(world.request(), "discussion", body)
    assert recorded == [("discussion", "p-1", "hi @card:x")]
