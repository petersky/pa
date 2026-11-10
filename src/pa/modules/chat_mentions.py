"""`@` mention search for the chat composer: files, cards, conversations and users.

The composer completes a token like `@src/app` or `@card:fix` from this endpoint. Results are
scoped to what the caller may already see: files are limited to the session's working
directory, conversations to the caller's own (or all for an admin), cards to the session's realm.
"""
from __future__ import annotations

import logging
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode

from fastapi import APIRouter, HTTPException, Request

from pa.auth.middleware import get_principal_id, require_user

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/agent/sessions")

KINDS = ("file", "card", "session", "user")
IMAGE_TYPES = {
    ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".gif": "image/gif", ".webp": "image/webp",
}
# Per-kind caps for an unfiltered search, and the cap once a kind is named ("@card:...").
MIXED_LIMITS = {"file": 6, "card": 4, "session": 4, "user": 3}
FILTERED_LIMIT = 20
MAX_QUERY_CHARS = 200

FILE_INDEX_SECONDS = 20.0
FILE_INDEX_ENTRIES = 8
MAX_INDEXED_FILES = 50_000
WALK_DEPTH = 8
SKIP_DIRS = {
    ".git", "node_modules", ".venv", "venv", "__pycache__", ".mypy_cache", ".pytest_cache",
    ".ruff_cache", ".tox", "dist", "build", ".next", ".cache", ".idea", ".gradle",
}
_file_index: dict[str, tuple[float, list[str]]] = {}


def _score(query: str, *fields: str) -> int | None:
    """Lower is better; None means no match. Fields are ordered most to least specific."""
    needle = query.lower()
    if not needle:
        return 0
    best: int | None = None
    for rank, field in enumerate(fields):
        lower = field.lower()
        if lower.startswith(needle):
            score = rank * 3
        elif any(part.startswith(needle) for part in _parts(lower)):
            score = rank * 3 + 1
        elif needle in lower:
            score = rank * 3 + 2
        else:
            continue
        best = score if best is None else min(best, score)
    return best


def _parts(value: str) -> list[str]:
    out: list[str] = []
    current = ""
    for char in value:
        if char in "/\\:-_. ":
            if current:
                out.append(current)
            current = ""
        else:
            current += char
    if current:
        out.append(current)
    return out


def _walk_files(root: Path) -> list[str]:
    found: list[str] = []
    base_depth = len(root.parts)
    for current, dirs, files in os.walk(root):
        depth = len(Path(current).parts) - base_depth
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS and not d.startswith(".")] if depth < WALK_DEPTH else []
        for name in files:
            found.append(str((Path(current) / name).relative_to(root)))
            if len(found) >= MAX_INDEXED_FILES:
                return found
    return found


def list_files(root: Path) -> list[str]:
    """Relative file paths under root; a git work tree's own view when there is one."""
    key = str(root)
    now = time.monotonic()
    cached = _file_index.get(key)
    if cached and now - cached[0] < FILE_INDEX_SECONDS:
        return cached[1]
    try:
        done = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-co", "--exclude-standard", "-z"],
            capture_output=True, timeout=5, check=True,
        )
        names = [name for name in done.stdout.decode(errors="replace").split("\0") if name]
    except (OSError, subprocess.SubprocessError):
        names = _walk_files(root)
    # A mention ends at whitespace, so a path containing it could not be completed.
    names = [name for name in names[:MAX_INDEXED_FILES] if not any(char.isspace() for char in name)]
    if len(_file_index) >= FILE_INDEX_ENTRIES:
        _file_index.pop(min(_file_index, key=lambda k: _file_index[k][0]), None)
    _file_index[key] = (now, names)
    return names


def search_files(root: Path, query: str, limit: int) -> list[dict[str, Any]]:
    scored: list[tuple[int, int, str]] = []
    for name in list_files(root):
        score = _score(query, name.rsplit("/", 1)[-1], name)
        if score is not None:
            scored.append((score, len(name), name))
    scored.sort()
    items: list[dict[str, Any]] = []
    for _, _, name in scored:
        path = root / name
        try:
            if not path.is_file():
                continue
        except OSError:
            continue
        image = IMAGE_TYPES.get(path.suffix.lower())
        items.append({
            "kind": "file", "id": name, "label": name.rsplit("/", 1)[-1],
            "detail": name if "/" in name else "", "insert": "@" + name,
            **({"action": "attach_image", "path": str(path), "mime_type": image} if image else {}),
        })
        if len(items) >= limit:
            break
    return items


def search_cards(store: Any, realm_id: str, query: str, limit: int) -> list[dict[str, Any]]:
    scored: list[tuple[int, int, Any]] = []
    for index, card in enumerate(store.list_card_options(realm_id=realm_id, recent_first=True)):
        score = _score(query, card.title, card.id)
        if score is not None:
            scored.append((score, index, card))
    scored.sort(key=lambda entry: entry[:2])
    return [
        {"kind": "card", "id": card.id, "label": card.title or card.id,
         "detail": f"{card.lane} · {card.id[:8]}", "insert": f"@card:{card.id}"}
        for _, _, card in scored[:limit]
    ]


def search_sessions(
    store: Any, current: Any, principal: str, admin: bool, query: str, limit: int
) -> list[dict[str, Any]]:
    scored: list[tuple[int, Any]] = []
    for session in store.list_sessions(purposes=("chat",), archived=False, include_metrics=False):
        if session.id == current.id:
            continue
        if session.principal_id not in {None, principal} and not admin:
            continue
        title = session.title or session.label or session.agent_name or ""
        score = _score(query, title, session.id)
        if score is not None:
            scored.append((score, session))
    scored.sort(key=lambda entry: (entry[0], -entry[1].updated_at.timestamp()))
    return [
        {"kind": "session", "id": session.id,
         "label": session.title or session.label or session.agent_name or session.id[:8],
         "detail": f"{session.status} · {session.id[:8]}", "insert": f"@session:{session.id}"}
        for _, session in scored[:limit]
    ]


def search_users(users: Any, query: str, limit: int) -> list[dict[str, Any]]:
    scored: list[tuple[int, str, Any]] = []
    for user in users.list_users():
        score = _score(query, user.display_name or user.username, user.username)
        if score is not None:
            scored.append((score, user.username, user))
    scored.sort(key=lambda entry: entry[:2])
    return [
        {"kind": "user", "id": user.id, "label": user.display_name or user.username,
         "detail": "@" + user.username, "insert": "@user:" + user.username}
        for _, _, user in scored[:limit]
    ]


def parse_query(raw: str) -> tuple[str | None, str]:
    """`card:fix` names a kind; anything else searches every kind."""
    text = raw[:MAX_QUERY_CHARS]
    kind, separator, rest = text.partition(":")
    if separator and kind.lower() in KINDS:
        return kind.lower(), rest
    return None, text


def search_mentions(request: Request, session: Any, manager: Any, raw: str) -> dict[str, Any]:
    ctx = request.app.state.ctx
    user = require_user(request)
    principal = get_principal_id(request)
    admin = getattr(user, "role", None) == "admin"
    kind, query = parse_query(raw)
    wanted = (kind,) if kind else KINDS

    def limit(name: str) -> int:
        return FILTERED_LIMIT if kind else MIXED_LIMITS[name]

    items: list[dict[str, Any]] = []
    if "file" in wanted and session.cwd:
        root = Path(session.cwd)
        if root.is_dir():
            items += search_files(root, query, limit("file"))
    if "card" in wanted:
        realm = getattr(session, "realm_id", None) or ctx.settings.primary_realm
        items += search_cards(manager.store, realm, query, limit("card"))
    if "session" in wanted:
        items += search_sessions(manager.store, session, principal, admin, query, limit("session"))
    if "user" in wanted:
        users = ctx.services.get("users")
        if users is not None:
            items += search_users(users, query, limit("user"))
    return {"kind": kind, "query": query, "items": items}


# --------------------------------------------------------------------------- resolution
#
# The composer completes mentions from the endpoint above; this is the other half. When a prompt
# is accepted the server resolves the tokens it contains with the same access rules and records
# the outcome in the transcript, so every client can show the same chips and links. The message
# text itself is never changed.

TOKEN = re.compile(r"(?<!\S)@(\S+)")
TRAILING_PUNCTUATION = ".,;:!?)]}\"'"
MAX_MENTIONS = 20


def extract_tokens(text: str) -> list[str]:
    """Distinct `@...` tokens in order, the way the composer reads them (word starts only)."""
    seen: dict[str, None] = {}
    for match in TOKEN.finditer(text or ""):
        token = match.group(1).rstrip(TRAILING_PUNCTUATION)
        if token:
            seen.setdefault("@" + token, None)
    return list(seen)[:MAX_MENTIONS]


def _resolve_file(session: Any, path: str) -> dict[str, Any] | None:
    if not session.cwd:
        return None
    root = Path(session.cwd).resolve()
    try:
        target = (root / path).resolve()
        if not target.is_relative_to(root) or not target.is_file():
            return None
    except (OSError, ValueError):
        return None
    image = target.suffix.lower() in IMAGE_TYPES
    return {"kind": "file", "id": path, "label": target.name, "image": image,
            "href": "/api/files/raw?" + urlencode({"path": str(target)})}


def resolve_token(
    token: str, *, session: Any, store: Any, settings: Any, users: Any, principal: str, admin: bool,
) -> dict[str, Any] | None:
    """One resolved mention, an `ok: false` one for an explicit kind that cannot be found, or None.

    A bare `@word` is only a mention when it names a file in the working directory; otherwise it is
    ordinary text (`@here`, a handle on another service) and is left alone.
    """
    body = token[1:]
    kind, separator, rest = body.partition(":")
    kind = kind.lower()
    if not (separator and kind in KINDS and rest):
        found = _resolve_file(session, body)
        return {"token": token, **found, "ok": True} if found else None
    result: dict[str, Any] | None = None
    try:
        if kind == "file":
            result = _resolve_file(session, rest)
        elif kind == "card":
            realm = getattr(session, "realm_id", None) or settings.primary_realm
            card = store.get_card(rest, realm_id=realm)
            if card:
                result = {"kind": "card", "id": card.id, "label": card.title or card.id,
                          "href": "/?" + urlencode({"realm": card.realm_id, "card": card.id, "tab": "summary"})}
        elif kind == "session":
            other = store.get_session(rest)
            allowed = other and other.purpose == "chat" and (
                admin or other.principal_id in {None, principal})
            if allowed:
                result = {"kind": "session", "id": other.id,
                          "label": other.title or other.label or other.agent_name or other.id[:8],
                          "href": "/agent?session=" + quote(other.id)}
        elif kind == "user" and users is not None:
            wanted = rest.lower()
            user = next((u for u in users.list_users() if u.username.lower() == wanted), None)
            if user:
                result = {"kind": "user", "id": user.id, "label": user.display_name or user.username}
    except Exception:  # noqa: BLE001 - a lookup failure must read as "not found", never block a prompt
        logger.warning("Mention lookup failed for %s", token, exc_info=True)
    if result is None:
        return {"token": token, "kind": kind, "id": rest, "label": rest, "ok": False}
    return {"token": token, **result, "ok": True}


def resolve_prompt_mentions(request: Request, session: Any, manager: Any, text: str) -> list[dict[str, Any]]:
    tokens = extract_tokens(text)
    if not tokens:
        return []
    ctx = request.app.state.ctx
    user = require_user(request)
    shared = {
        "session": session, "store": manager.store, "settings": ctx.settings,
        "users": ctx.services.get("users"), "principal": get_principal_id(request),
        "admin": getattr(user, "role", None) == "admin",
    }
    found = (resolve_token(token, **shared) for token in tokens)
    return [item for item in found if item]


async def record_prompt_mentions(request: Request, session_id: str, prompt_id: str | None, text: str) -> None:
    """Append what the prompt's mentions resolved to; never lets a failure reach the sender."""
    if not prompt_id or "@" not in (text or ""):
        return
    try:
        from pa.modules.agent_chat import _manager, _offload

        manager = _manager(request)
        runtime = manager.get(session_id)
        session = await _offload(manager, "mentions.session", manager.store.get_session, session_id)
        if runtime is None or session is None:
            return
        mentions = await _offload(manager, "mentions.resolve", resolve_prompt_mentions, request, session, manager, text)
        if mentions:
            runtime._append_transcript("prompt_mentions", {"prompt_id": prompt_id, "mentions": mentions})
            await runtime._drain_transcripts()
    except Exception:  # noqa: BLE001
        logger.warning("Could not record mentions for prompt %s", prompt_id, exc_info=True)


@router.get("/{session_id}/mentions")
async def mentions(request: Request, session_id: str, q: str = ""):
    from pa.modules.agent_chat import _manager, _offload

    user = require_user(request)
    manager = _manager(request)
    session = await _offload(manager, "mentions.session", manager.store.get_session, session_id)
    if not session:
        raise HTTPException(404, "Conversation not found")
    if session.principal_id not in {None, get_principal_id(request)} and getattr(user, "role", None) != "admin":
        raise HTTPException(403, "Conversation belongs to another principal")
    return await _offload(manager, "mentions.search", search_mentions, request, session, manager, q, timeout=10.0)
