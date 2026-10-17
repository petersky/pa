"""Versioned, session-owned visualization references and immutable HTML artifacts.

Only transcript capture can ingest files. HTTP lookup never dereferences a path
supplied by a viewer. The original transcript is not rewritten.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import threading
from pathlib import Path

from pa.attachments import AttachmentStore
from pa.core.io import atomic_write_json, atomic_write_text

START = "\ue200visualize\ue202"
END = "\ue201"
MAX_HTML_BYTES = 2 * 1024 * 1024
MAX_MESSAGE_CHARS = 250_000
CAPABILITIES = {
    "version": 1, "format": "visualize", "bridge": "pa.visualization/v1",
    "fields": ["path", "title", "wide", "version"],
    "runtime": ["html", "css", "local-javascript", "light-dark", "native-controls",
                "viz-carousel", "viz-dotted-background", "cursor-interaction", "Tweak",
                "openai.widgetState", "openai.setWidgetState"],
    "network": False, "max_html_bytes": MAX_HTML_BYTES,
    "source": "HTML within this session's admitted cwd or an associated card attachment",
}
_locks = [threading.RLock() for _ in range(64)]


def reference_id(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def references(text: str):
    """Skip fenced/indented/inline Markdown code, including variable backticks."""
    # A shared scanner contract is exercised against the browser implementation.
    fenced = None
    inline = 0
    offset = 0
    consumed = 0
    for line in text.splitlines(keepends=True):
        if offset + len(line) <= consumed:
            offset += len(line)
            continue
        i = max(0, consumed - offset)
        fence = re.match(r"^ {0,3}(`{3,}|~{3,})", line)
        if fence and not inline and not i:
            run = fence[1]
            if fenced is None:
                fenced = run
            elif run[0] == fenced[0] and len(run) >= len(fenced):
                fenced = None
            offset += len(line)
            continue
        if fenced or (not inline and (line.startswith("    ") or line.startswith("\t"))):
            offset += len(line)
            continue
        while i < len(line):
            if line[i] == "`":
                end = i + 1
                while end < len(line) and line[end] == "`":
                    end += 1
                run = end - i
                inline = 0 if inline == run else (run if not inline else inline)
                i = end
                continue
            if not inline and line.startswith(START, i) and (i == 0 or line[i - 1] != "\\"):
                start = offset + i + len(START)
                end = text.find(END, start)
                raw = text[start:end if end >= 0 else len(text)].strip()
                stop = end + len(END) if end >= 0 else len(text)
                yield {"raw": raw, "id": reference_id(raw), "start": offset + i,
                       "end": stop, "complete": end >= 0, **parse_reference(raw)}
                consumed = stop
                i = stop - offset
            else:
                i += 1
        offset += len(line)


def parse_reference(raw: str) -> dict:
    result = {"title": "Interactive preview", "wide": False, "status": "malformed"}
    if len(raw) > 8192:
        return result
    try:
        obj = json.loads(raw)
        if not isinstance(obj, dict):
            return result
        if isinstance(obj.get("title"), str) and obj["title"].strip():
            result["title"] = obj["title"].strip()[:160]
        if type(obj.get("wide", False)) is not bool:
            return result
        result["wide"] = obj.get("wide", False)
        if type(obj.get("version", 1)) is not int or obj.get("version", 1) != 1:
            return {**result, "status": "unsupported"}
        path = obj.get("path")
        if not isinstance(path, str) or not path or len(path) > 4096 or "\0" in path:
            return result
        return {**result, "path": path, "status": "pending"}
    except (ValueError, TypeError):
        return result


def _read_html(root: Path, path: Path) -> str:
    """Walk with dir fds and O_NOFOLLOW so symlink swaps cannot escape ownership."""
    root = root.resolve(strict=True)
    candidate = path if path.is_absolute() else root / path
    relative = candidate.relative_to(root)
    if ".." in relative.parts or candidate.suffix.lower() not in {".html", ".htm"}:
        raise ValueError("unavailable")
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in relative.parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        child = os.open(relative.parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        with os.fdopen(child, "rb") as handle:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_HTML_BYTES or info.st_nlink != 1:
                raise ValueError("unavailable")
            data = handle.read(MAX_HTML_BYTES + 1)
            if len(data) > MAX_HTML_BYTES:
                raise ValueError("unavailable")
            return data.decode("utf-8")
    finally:
        os.close(fd)


class VisualizationStore:
    def __init__(self, settings, store):
        self.root = Path(settings.data_dir) / "visualizations-v1"
        self.settings, self.store = settings, store

    def directory(self, session_id):
        return self.root / reference_id(session_id)

    def get(self, session_id, key):
        if not re.fullmatch(r"[0-9a-f]{64}", key):
            return None
        try:
            return json.loads((self.directory(session_id) / (key + ".json")).read_text())
        except (OSError, ValueError):
            return None

    def html(self, session_id, record):
        data = (self.directory(session_id) / (record["sha256"] + ".html")).read_bytes().decode("utf-8")
        if reference_id(data) != record["sha256"]:
            raise ValueError("unavailable")
        return data

    def capture(self, session, ref):
        if ref["status"] != "pending" or not ref.get("complete", True):
            return
        key = ref["id"]
        with _locks[int(key[:2], 16) % len(_locks)]:
            if (prior := self.get(session.id, key)) and prior.get("status") == "ready":
                return
            record = {k: ref[k] for k in ("id", "title", "wide")}
            record.update(version=1, status="unavailable")
            try:
                html = self._source(session, ref["path"])
                digest = reference_id(html)
                atomic_write_text(self.directory(session.id) / (digest + ".html"), html, mode=0o600)
                record.update(status="ready", sha256=digest)
            except (OSError, ValueError, UnicodeError):
                pass
            atomic_write_json(self.directory(session.id) / (key + ".json"), record, mode=0o600)

    def _source(self, session, source):
        if Path(source).suffix.lower() not in {".html", ".htm"}:
            raise ValueError("unavailable")
        # An attachment must belong to this exact session's associated cards.
        cards = set(self.store.list_card_ids_for_session(session.id))
        if session.card_id:
            cards.add(session.card_id)
        matches = []
        for cid in cards:
            card = self.store.get_card(cid, realm_id=session.realm_id)
            if card:
                matches.extend(a for a in card.attachments if a.state == "active"
                               and a.realm_id == session.realm_id and a.filename == Path(source).name)
        if len(matches) == 1:
            attachment = matches[0]
            blobs = AttachmentStore(self.settings.data_dir)
            if attachment.size <= MAX_HTML_BYTES and blobs.has_verified_blob(attachment.sha256, attachment.size):
                return blobs.blob_path(attachment.sha256).read_text(encoding="utf-8")
        # cwd was admitted by PA; never treat the data directory as a workspace.
        if not session.cwd:
            raise ValueError("unavailable")
        cwd = Path(session.cwd).resolve(strict=True)
        if cwd == Path(self.settings.data_dir).resolve() or cwd.is_relative_to(Path(self.settings.data_dir).resolve()):
            raise ValueError("unavailable")
        return _read_html(cwd, Path(source))

    def state(self, session_id, key):
        try:
            return json.loads((self.directory(session_id) / (key + ".state.json")).read_text())
        except (OSError, ValueError):
            return None

    def set_state(self, session_id, key, state):
        atomic_write_json(self.directory(session_id) / (key + ".state.json"), state, mode=0o600)


class VisualizationCapture:
    """Bounded per-stream reconstruction; handles split markers and snapshots."""
    def __init__(self, artifacts, session):
        self.artifacts, self.session = artifacts, session
        self.streams = {}

    def consume(self, events):
        from pa.acp.final_message import is_agent_message_type
        for event in events:
            if event.event_type in {"turn_completed", "prompt_failed", "user_message"}:
                self.streams.clear()
            if not is_agent_message_type(event.event_type):
                continue
            p = event.payload
            key = (str(p.get("message_id") or ""), str(p.get("phase") or ""))
            text = str(p.get("text") or "")
            mode = str(p.get("content_mode") or p.get("operation") or "delta").lower()
            if mode not in {"snapshot", "replace", "replacement", "accumulated"}:
                text = self.streams.get(key, "") + text
            self.streams[key] = text[-MAX_MESSAGE_CHARS:]
            if len(self.streams) > 8:
                del self.streams[next(iter(self.streams))]
            if START not in text or END not in text:
                continue
            for ref in list(references(text))[:20]:
                self.artifacts.capture(self.session, ref)
