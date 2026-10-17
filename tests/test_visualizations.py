"""Protocol parsing, capture durability, ownership and authenticated artifact APIs."""
from __future__ import annotations

import io
import json
import subprocess
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from pa.attachments import AttachmentStore
from pa.domain.models import AgentSession, CardAttachment, CardCreate, TranscriptEvent
from pa.instance.agent_session import AgentSessionRuntime
from pa.visualizations import START, END, VisualizationStore, references, reference_id
from tests.test_local_operator_acceptance import operator_app

FIXTURES = Path(__file__).parent / "fixtures/visualizations"
SOURCE = FIXTURES / "pa-attention-concepts.html"


@pytest.fixture
def initialized_app(operator_app):
    with TestClient(operator_app):
        yield operator_app


def marker(path, **kwargs):
    return START + json.dumps({"path": str(path), **kwargs}) + END


def seed(app, directory, text=None, sid="viz-session"):
    store = app.state.ctx.store
    manager = app.state.ctx.require_service("instance_agent")
    directory.mkdir(parents=True, exist_ok=True)
    source = directory / SOURCE.name
    source.write_bytes(SOURCE.read_bytes())
    session = store.save_session(AgentSession(id=sid, agent_name="codex", purpose="chat",
                                            cwd=str(directory), principal_id="user:local"))
    runtime = AgentSessionRuntime(manager, session)
    text = text or marker(source, title="PA Home concepts", wide=True)
    # Deliberately split the protocol and JSON across separate persisted batches.
    for chunk in (text[:4], text[4:35], text[35:]):
        runtime._append_transcript("agent_message_chunk", {"message_id":"concept", "text":chunk, "phase":"final"})
        runtime._flush_transcript()
    runtime._append_transcript("turn_completed", {})
    runtime._flush_transcript()
    return session, source, text


def test_parser_code_examples_and_browser_parity():
    m = marker("output/home.html", title="Home", wide=True)
    samples = [m, "Before " + m + " after", "```text\n" + m + "\n```\n" + m,
               "~~~~html\n" + m + "\n~~~~\n", "`" + m + "`", "``a ` " + m + "``\n" + m,
               "    " + m + "\n\t" + m, START + '{bad json}' + END,
               START + '{"path":"x.html","version":99,"title":"Future"}' + END,
               START + '{"path":"x.html","wide":"yes"}' + END,
               "\\" + m, START + '{"path":', marker("x.html", title="<img src=x>"),
               START+'{\n "path":"x.html",\n "title":"Multiline"\n}'+END+'\nAfter',
               START+'{\n "path":"x.html"\nBad reference']
    expected = [[{k:r[k] for k in ("raw", "start", "end")} for r in references(s)] for s in samples]
    script = "global.window={};eval(require('fs').readFileSync('src/pa/server/static/js/visualizations.js','utf8'));console.log(JSON.stringify(" + json.dumps(samples) + ".map(s=>window.PAVisualizations.references(s).map(({raw,start,end})=>({raw,start,end})))));"
    actual = json.loads(subprocess.check_output(["node", "-e", script], text=True))
    assert actual == expected
    assert [len(x) for x in expected[:7]] == [1,1,1,0,0,1,0]
    assert list(references(samples[7]))[0]["status"] == "malformed"
    assert list(references(samples[8]))[0]["status"] == "unsupported"
    assert list(references(samples[9]))[0]["status"] == "malformed"


def test_authenticated_remote_reload_after_source_and_provider_exit(initialized_app, tmp_path):
    app = initialized_app
    session, source, text = seed(app, tmp_path / "owned")
    ref = list(references(text))[0]
    source.unlink()
    viewer = TestClient(app)
    route = f"/api/agent/sessions/{session.id}/visualizations"
    response = viewer.get(route, params={"reference":ref["raw"]})
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["status"] == "ready" and payload["wide"] is True
    assert payload["html"] == SOURCE.read_text()
    assert "path" not in payload and "sha256" in payload
    headers = {"X-CSRF-Token":viewer.cookies.get("pa_csrf")}
    state = {"privateContent":{"machine":False, "density":"Compact"}}
    saved = viewer.put(f"{route}/{ref['id']}/state", json={"version":1,"state":state}, headers=headers)
    assert saved.status_code == 200, saved.text
    assert viewer.put(f"{route}/{ref['id']}/state", json={"version":2,"state":state}, headers=headers).status_code == 422
    assert viewer.put(f"{route}/{ref['id']}/state", json={"version":1,"state":{"unexpected":"value"}}, headers=headers).status_code == 422
    assert viewer.put(f"{route}/{ref['id']}/state", json={"version":1,"state":{"privateContent":"x"*33000}}, headers=headers).status_code == 422
    download = viewer.get(f"{route}/{ref['id']}/source")
    assert download.text == SOURCE.read_text()
    assert download.headers["content-disposition"].startswith("attachment")
    assert "sandbox" in download.headers["content-security-policy"]
    # A marker submitted by a viewer cannot trigger file resolution.
    uncaptured = viewer.get(route, params={"reference":json.dumps({"path":str(SOURCE)})})
    assert uncaptured.json()["status"] == "unavailable"
    # A separate viewing device needs only authentication; no originating paths.
    remote = TestClient(app)
    assert remote.get(route, params={"reference":ref["raw"]}).json()["state"] == state
    events = app.state.ctx.store.list_transcript_events(session.id)
    assert "".join(e.payload.get("text", "") for e in events) == text
    assert remote.get(route, params={"reference":'{"path":"x.html","version":2,"title":"Future"}'}).json()["status"] == "unsupported"
    assert remote.get(route, params={"reference":"{broken}"}).json()["status"] == "malformed"
    assert remote.get(route, params={"reference":"x"*8193}).status_code == 400
    app.state.ctx.settings.auth_required = True
    assert remote.get(route, params={"reference":ref["raw"]}).status_code == 401


def test_session_path_symlink_hardlink_and_attachment_ownership(initialized_app, tmp_path):
    app = initialized_app
    store, settings = app.state.ctx.store, app.state.ctx.settings
    session, source, _ = seed(app, tmp_path / "owned")
    other, other_source, _ = seed(app, tmp_path / "other", sid="other-session")
    artifacts = VisualizationStore(settings, store)
    escapes = [other_source, source.parent / ".." / "other" / SOURCE.name]
    link = source.parent / "linked.html"
    link.symlink_to(other_source)
    escapes.append(link)
    directory_link = source.parent / "linked-dir"
    directory_link.symlink_to(other_source.parent, target_is_directory=True)
    escapes.append(directory_link / SOURCE.name)
    hardlink = source.parent / "hardlink.html"
    hardlink.hardlink_to(other_source)
    escapes.append(hardlink)
    for path in escapes:
        ref = list(references(marker(path)))[0]
        artifacts.capture(session, ref)
        assert artifacts.get(session.id,ref["id"])["status"] == "unavailable", path
    # A different session cannot look up an artifact even if it knows its hash.
    ref = list(references(marker(other_source, title="Other")))[0]
    artifacts.capture(other, ref)
    assert artifacts.get(session.id, ref["id"]) is None
    # Portable attachments resolve only from the associated card and realm.
    card = store.create_card(CardCreate(title="Fixture attachments"))
    digest, size = AttachmentStore(settings.data_dir).ingest(io.BytesIO(SOURCE.read_bytes()))
    attachment = CardAttachment(attachment_id="fixture-html", card_id=card.id, realm_id=session.realm_id,
                               filename=SOURCE.name, media_type="text/html", sha256=digest, size=size,
                               blob_ref="sha256:"+digest, created_by_principal="user:local",
                               created_by_instance=settings.instance_id)
    store.add_attachment(attachment,principal_id="user:local",instance_id=settings.instance_id)
    portable = list(references(marker("/nonexistent/origin/"+SOURCE.name)))[0]
    artifacts.capture(session, portable)
    assert artifacts.get(session.id,portable["id"])["status"] == "unavailable"
    store.link_session_card(session.id, card.id)
    artifacts.capture(session, portable)
    assert artifacts.get(session.id,portable["id"])["status"] == "ready"
    other.principal_id = "user:someone-else"
    store.save_session(other)
    editor = app.state.ctx.require_service("users").create_user("viewer", "fixture-password", role="editor")
    client = TestClient(app)
    assert client.get(f"/api/agent/sessions/{other.id}/visualizations",params={"reference":ref["raw"]},
                      headers={"Authorization":"Bearer "+editor.cli_token}).status_code == 403


def test_snapshot_code_only_unavailable_and_corrupt_artifacts(initialized_app, tmp_path):
    app = initialized_app
    directory = tmp_path / "snapshots"
    session, source, text = seed(app,directory)
    artifacts = VisualizationStore(app.state.ctx.settings, app.state.ctx.store)
    from pa.visualizations import VisualizationCapture
    capture = VisualizationCapture(artifacts,session)
    code = marker(source,title="Code only")
    def event(text, mode="snapshot"):
        return TranscriptEvent(session_id=session.id,event_type="agent_message_chunk",seq=1,
                               payload={"text":text,"content_mode":mode})
    capture.consume([event("```html\n"+code+"\n```"),event("```html\n"+code+"\n```\nDone")])
    assert artifacts.get(session.id,list(references(code))[0]["id"]) is None
    ref = list(references(text))[0]
    record = artifacts.get(session.id,ref["id"])
    (artifacts.directory(session.id)/(record["sha256"]+".html")).write_text("tampered")
    client = TestClient(app)
    route=f"/api/agent/sessions/{session.id}/visualizations"
    assert client.get(route,params={"reference":ref["raw"]}).json()["status"] == "unavailable"
    assert client.get(f"{route}/{ref['id']}/source").status_code == 404


def test_standalone_wrapper_contains_exact_original_fragment():
    # Retain both supplied attachments as portable evidence, not host paths.
    import html
    wrapper = (FIXTURES/"pa-attention-preview.html").read_text()
    inner = html.unescape(wrapper.split('data-srcdoc="',1)[1].split('"',1)[0])
    assert SOURCE.read_text().strip() in inner
