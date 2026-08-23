"""Authoritative, session-scoped browser automation and input dispatch."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import secrets
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from pa.browser.cdp import CdpError, CdpPage, validate_browser_url
from pa.browser.manager import BrowserAttachment, BrowserManager

logger = logging.getLogger(__name__)

MAX_CONTEXTS = 16
MAX_ACTIONS = 100
MAX_PAUSE_SECONDS = 2.0
MAX_TOTAL_PAUSE_SECONDS = 10.0
MAX_COORDINATE = 100_000.0
MAX_WHEEL_DELTA = 100_000.0
MAX_OPERATION_CACHE = 256
DEFAULT_IDLE_TTL_SECONDS = 30 * 60
MAX_IDLE_TTL_SECONDS = 24 * 60 * 60

BUTTONS = {"left": 0, "right": 2, "middle": 1}
MODIFIER_BITS = {"Alt": 1, "Control": 2, "Meta": 4, "Shift": 8}
MODIFIER_KEYS = {
    "Alt": ("Alt", "AltLeft"),
    "Control": ("Control", "ControlLeft"),
    "Meta": ("Meta", "MetaLeft"),
    "Shift": ("Shift", "ShiftLeft"),
}
NAMED_KEYS = {
    "Alt",
    "ArrowDown",
    "ArrowLeft",
    "ArrowRight",
    "ArrowUp",
    "Backspace",
    "Control",
    "Delete",
    "End",
    "Enter",
    "Escape",
    "F1",
    "F10",
    "F11",
    "F12",
    "F2",
    "F3",
    "F4",
    "F5",
    "F6",
    "F7",
    "F8",
    "F9",
    "Home",
    "Insert",
    "Meta",
    "PageDown",
    "PageUp",
    "Shift",
    "Space",
    "Tab",
}
KEYBOARD_CODES = {
    "Alt": ("AltLeft", 18),
    "ArrowDown": ("ArrowDown", 40),
    "ArrowLeft": ("ArrowLeft", 37),
    "ArrowRight": ("ArrowRight", 39),
    "ArrowUp": ("ArrowUp", 38),
    "Backspace": ("Backspace", 8),
    "Control": ("ControlLeft", 17),
    "Delete": ("Delete", 46),
    "End": ("End", 35),
    "Enter": ("Enter", 13),
    "Escape": ("Escape", 27),
    "Home": ("Home", 36),
    "Insert": ("Insert", 45),
    "Meta": ("MetaLeft", 91),
    "PageDown": ("PageDown", 34),
    "PageUp": ("PageUp", 33),
    "Shift": ("ShiftLeft", 16),
    "Space": ("Space", 32),
    "Tab": ("Tab", 9),
    **{f"F{index}": (f"F{index}", 111 + index) for index in range(1, 13)},
}
HANDLE_RE = re.compile(r"^br_[A-Za-z0-9_-]{32,}$")
SHARE_RE = re.compile(r"^bs_[A-Za-z0-9_-]{32,}$")
OPERATION_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


class BrowserSessionError(RuntimeError):
    """A stable structured browser error suitable for MCP/HTTP/CLI."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        ambiguous: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.ambiguous = ambiguous
        self.details = details or {}

    def as_dict(self) -> dict[str, Any]:
        guidance = {
            "retryable": self.retryable,
            "requires_same_operation_id": self.ambiguous,
            "automatic_retry_allowed": self.retryable and not self.ambiguous,
            "outcome_lookup": "browser_operation_outcome" if self.ambiguous else None,
        }
        return {
            "code": self.code,
            "message": str(self),
            "retryable": self.retryable,
            "ambiguous": self.ambiguous,
            "details": self.details,
            "retry_guidance": guidance,
        }


@dataclass(frozen=True)
class BrowserScope:
    principal_id: str
    agent_session_id: str
    instance_id: str

    def key(self) -> tuple[str, str, str]:
        return (self.principal_id, self.agent_session_id, self.instance_id)


@dataclass
class SnapshotRecord:
    snapshot_id: str
    target_id: str
    document_id: str
    revision: int
    locators: dict[str, str]
    created_at: float


@dataclass
class ShareGrant:
    token: str
    handle: str
    principal_id: str
    authorized_session_id: str
    instance_id: str
    expires_at: float
    used: bool = False


@dataclass
class BrowserSession:
    handle: str
    owner_scope: BrowserScope
    attachment: BrowserAttachment
    ownership: str
    created_at: float
    last_used_at: float
    expires_at: float
    shared_scopes: set[tuple[str, str, str]] = field(default_factory=set)
    interaction_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    snapshots: dict[str, SnapshotRecord] = field(default_factory=dict)
    operations: OrderedDict[str, dict[str, Any]] = field(default_factory=OrderedDict)
    inflight: dict[str, asyncio.Future[dict[str, Any]]] = field(default_factory=dict)
    pointer_x: float = 0
    pointer_y: float = 0
    held_buttons: set[str] = field(default_factory=set)
    held_keys: set[str] = field(default_factory=set)

    @property
    def page(self) -> CdpPage:
        return self.attachment.page


@dataclass(frozen=True)
class AuditRecord:
    timestamp: float
    principal_id: str
    agent_session_id: str
    instance_id: str
    handle: str
    target_id: str
    action_class: str
    operation_id: str | None
    outcome: str
    error_code: str | None = None


_DOCUMENT_STATE_JS = """(() => {
  if (!globalThis.__paBrowserDocumentId) {
    Object.defineProperty(globalThis, '__paBrowserDocumentId', {
      value: globalThis.crypto?.randomUUID
        ? crypto.randomUUID()
        : `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`,
      configurable: false
    });
    globalThis.__paBrowserRevision = 0;
    new MutationObserver(() => { globalThis.__paBrowserRevision += 1; })
      .observe(document, {subtree:true, childList:true, attributes:true, characterData:true});
  }
  return {
    document_id: globalThis.__paBrowserDocumentId,
    revision: globalThis.__paBrowserRevision,
    url: location.href
  };
})()"""

_SNAPSHOT_JS = """(() => {
  const state = (__PA_DOCUMENT_STATE__);
  const visible = el => {
    const r = el.getBoundingClientRect();
    const s = getComputedStyle(el);
    return r.width > 0 && r.height > 0 && s.visibility !== 'hidden' && s.display !== 'none';
  };
  const path = el => {
    const parts = [];
    while (el && el.nodeType === 1 && el !== document.documentElement) {
      let part = el.tagName.toLowerCase();
      if (el.id && CSS.escape) {
        part += '#' + CSS.escape(el.id);
        parts.unshift(part);
        break;
      }
      const siblings = Array.from(el.parentElement?.children || [])
        .filter(s => s.tagName === el.tagName);
      if (siblings.length > 1) part += `:nth-of-type(${siblings.indexOf(el) + 1})`;
      parts.unshift(part);
      el = el.parentElement;
    }
    return parts.join(' > ');
  };
  const elements = Array.from(document.querySelectorAll(
    'a,button,input,textarea,select,[role],[tabindex],h1,h2,h3,p'
  )).filter(visible).slice(0, 300).map((el, index) => ({
    index, locator: path(el), tag: el.tagName.toLowerCase(),
    role: el.getAttribute('role') || '',
    text: (el.innerText || el.value || el.getAttribute('aria-label') || '').trim().slice(0, 500),
    href: el.href || '', disabled: !!el.disabled
  }));
  return {
    state,
    document: {
      ready_state: document.readyState, url: location.href, title: document.title,
      body_text: (document.body?.innerText || '').trim().slice(0, 2000)
    },
    elements
  };
})()""".replace("__PA_DOCUMENT_STATE__", _DOCUMENT_STATE_JS)


def _opaque(prefix: str) -> str:
    return prefix + secrets.token_urlsafe(32)


def _now() -> float:
    return time.time()


def _modifiers(values: list[str] | None) -> tuple[list[str], int]:
    normalized: list[str] = []
    for value in values or []:
        aliases = {
            "alt": "Alt",
            "ctrl": "Control",
            "control": "Control",
            "cmd": "Meta",
            "command": "Meta",
            "meta": "Meta",
            "shift": "Shift",
        }
        name = aliases.get(str(value).strip().lower())
        if not name:
            raise BrowserSessionError(
                "unsupported_key",
                f"Unsupported modifier {value!r}; use Alt, Control, Meta, or Shift.",
            )
        if name not in normalized:
            normalized.append(name)
    return normalized, sum(MODIFIER_BITS[item] for item in normalized)


def _button(value: str | int | None) -> str:
    if value is None:
        return "left"
    if isinstance(value, int):
        numbered = {0: "left", 1: "middle", 2: "right"}
        if value in numbered:
            return numbered[value]
    name = str(value).strip().lower()
    if name not in BUTTONS:
        raise BrowserSessionError(
            "unsupported_button",
            "Unsupported mouse button; use left/0, middle/1, or right/2.",
        )
    return name


def _key(value: str) -> str:
    value = str(value)
    aliases = {
        " ": "Space",
        "Esc": "Escape",
        "Return": "Enter",
        "Cmd": "Meta",
        "Ctrl": "Control",
    }
    value = aliases.get(value, value)
    if len(value) == 1 or value in NAMED_KEYS:
        return value
    raise BrowserSessionError(
        "unsupported_key",
        f"Unsupported key {value!r}; use a single Unicode character or a documented named key.",
    )


class BrowserSessionManager:
    """Own browser authority and serialize all automation routes per target."""

    def __init__(
        self,
        browser: BrowserManager,
        *,
        instance_id: str,
        attached_lookup: Callable[[str], BrowserAttachment | None] | None = None,
        idle_ttl_seconds: float = DEFAULT_IDLE_TTL_SECONDS,
        max_contexts: int = MAX_CONTEXTS,
    ) -> None:
        self.browser = browser
        self.instance_id = instance_id
        self.attached_lookup = attached_lookup
        self.idle_ttl_seconds = min(
            max(float(idle_ttl_seconds), 60), MAX_IDLE_TTL_SECONDS
        )
        self.max_contexts = max_contexts
        self.sessions: dict[str, BrowserSession] = {}
        self.defaults: dict[tuple[str, str, str], str] = {}
        self.shares: dict[str, ShareGrant] = {}
        self.audit: list[AuditRecord] = []
        self._registry_lock = asyncio.Lock()
        self._attach_locks: dict[tuple[str, str, str], asyncio.Lock] = {}
        self._cleanup_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start periodic idle-session and orphan cleanup."""
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(
                self._cleanup_loop(), name="pa-browser-session-cleanup"
            )

    async def _cleanup_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(60)
                try:
                    await self.cleanup()
                except (CdpError, OSError, RuntimeError) as exc:
                    logger.warning(
                        "browser idle cleanup failed error_type=%s", type(exc).__name__
                    )
        except asyncio.CancelledError:
            pass

    def _validate_scope(self, scope: BrowserScope) -> None:
        if (
            not scope.principal_id
            or not scope.agent_session_id
            or scope.instance_id != self.instance_id
        ):
            raise BrowserSessionError(
                "ownership_failure",
                "Browser scope must match the authenticated principal, canonical agent session, and PA instance.",
            )

    async def attach(
        self,
        scope: BrowserScope,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Serialize the full attach outcome, including its idempotency receipt."""
        self._validate_scope(scope)
        lock = self._attach_locks.setdefault(scope.key(), asyncio.Lock())
        async with lock:
            return await self._attach_once(scope, **kwargs)

    async def _attach_once(
        self,
        scope: BrowserScope,
        *,
        url: str = "about:blank",
        width: int = 1440,
        height: int = 900,
        device_scale_factor: float = 1,
        share_handle: str | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        self._validate_scope(scope)
        if operation_id and not OPERATION_RE.fullmatch(operation_id):
            raise BrowserSessionError("invalid_operation_id", "Invalid operation_id.")
        if operation_id:
            try:
                prior_session = self.resolve(scope)
            except BrowserSessionError:
                prior_session = None
            if prior_session and operation_id in prior_session.operations:
                result = dict(prior_session.operations[operation_id])
                result["deduplicated"] = True
                return result
        try:
            url = validate_browser_url(url)
        except CdpError as exc:
            raise BrowserSessionError("invalid_url", str(exc)) from exc
        async with self._registry_lock:
            await self._cleanup_locked()
            if share_handle:
                session = self._redeem_share(scope, share_handle)
                self.defaults[scope.key()] = session.handle
                existing = True
                shared = True
            else:
                existing_handle = self.defaults.get(scope.key())
                session = (
                    self.sessions.get(existing_handle) if existing_handle else None
                )
                existing = session is not None
                shared = False
            if not existing:
                if len(self.sessions) >= self.max_contexts:
                    raise BrowserSessionError(
                        "quota_exceeded",
                        f"This PA instance allows at most {self.max_contexts} active browser contexts.",
                        retryable=True,
                    )
                attached = (
                    self.attached_lookup(scope.agent_session_id)
                    if self.attached_lookup
                    else None
                )
                ownership = "user_owned" if attached else "agent_owned"
                attachment = attached or await self.browser.attach(
                    f"automation-{scope.agent_session_id}",
                    url="about:blank",
                    width=width,
                    height=height,
                    device_scale_factor=device_scale_factor,
                )
                now = _now()
                session = BrowserSession(
                    handle=_opaque("br_"),
                    owner_scope=scope,
                    attachment=attachment,
                    ownership=ownership,
                    created_at=now,
                    last_used_at=now,
                    expires_at=now + self.idle_ttl_seconds,
                )
                try:
                    await attachment.resize(
                        width, height, device_scale_factor=device_scale_factor
                    )
                    if url != "about:blank":
                        await attachment.page.navigate_and_wait(url)
                except (
                    CdpError,
                    OSError,
                    RuntimeError,
                    ValueError,
                    asyncio.CancelledError,
                ):
                    if ownership == "agent_owned":
                        await self.browser.detach(
                            f"automation-{scope.agent_session_id}"
                        )
                    raise
                self.sessions[session.handle] = session
                self.defaults[scope.key()] = session.handle
        assert session is not None
        if existing:
            async with session.interaction_lock:
                session.last_used_at = _now()
                session.expires_at = session.last_used_at + self.idle_ttl_seconds
                await session.attachment.resize(
                    width, height, device_scale_factor=device_scale_factor
                )
                if url != "about:blank":
                    await session.page.navigate_and_wait(url)
                    session.snapshots.clear()
                result = await self._state(session, scope, shared=shared)
        else:
            result = await self._state(session, scope)
        self._audit(session, scope, "attach", None, "success")
        if operation_id:
            result = {**result, "operation_id": operation_id}
            self._remember(session, operation_id, result)
        return result

    def _redeem_share(self, scope: BrowserScope, token: str) -> BrowserSession:
        if not SHARE_RE.fullmatch(token):
            raise BrowserSessionError(
                "invalid_share_handle", "Invalid browser share handle."
            )
        grant = self.shares.get(token)
        if not grant or grant.used or grant.expires_at <= _now():
            raise BrowserSessionError(
                "invalid_share_handle", "Browser share handle is invalid or expired."
            )
        if (
            grant.principal_id != scope.principal_id
            or grant.authorized_session_id != scope.agent_session_id
            or grant.instance_id != scope.instance_id
        ):
            raise BrowserSessionError(
                "ownership_failure",
                "This share handle is not authorized for the authenticated principal and agent session.",
            )
        session = self.sessions.get(grant.handle)
        if not session:
            raise BrowserSessionError(
                "invalid_browser_handle",
                "The shared browser no longer exists; ask its owner for a new share handle.",
                retryable=True,
            )
        grant.used = True
        session.shared_scopes.add(scope.key())
        return session

    async def share(
        self,
        scope: BrowserScope,
        *,
        authorized_session_id: str,
        handle: str | None = None,
        ttl_seconds: int = 300,
    ) -> dict[str, Any]:
        session = self.resolve(scope, handle)
        if session.owner_scope != scope:
            raise BrowserSessionError(
                "ownership_failure", "Only the browser owner may create a share handle."
            )
        if not authorized_session_id or authorized_session_id == scope.agent_session_id:
            raise BrowserSessionError(
                "invalid_share_target",
                "authorized_session_id must name a different canonical agent session.",
            )
        ttl = min(max(int(ttl_seconds), 30), 900)
        token = _opaque("bs_")
        grant = ShareGrant(
            token=token,
            handle=session.handle,
            principal_id=scope.principal_id,
            authorized_session_id=authorized_session_id,
            instance_id=scope.instance_id,
            expires_at=_now() + ttl,
        )
        self.shares[token] = grant
        self._audit(session, scope, "share", None, "success")
        return {
            "ok": True,
            "share_handle": token,
            "authorized_session_id": authorized_session_id,
            "expires_at": grant.expires_at,
            "single_use": True,
        }

    def resolve(self, scope: BrowserScope, handle: str | None = None) -> BrowserSession:
        self._validate_scope(scope)
        requested = handle or self.defaults.get(scope.key())
        if not requested:
            raise BrowserSessionError(
                "browser_not_attached",
                "No browser is attached for this agent session. Call browser_attach first.",
                retryable=True,
            )
        if not HANDLE_RE.fullmatch(requested):
            raise BrowserSessionError(
                "invalid_browser_handle", "Invalid opaque browser handle."
            )
        session = self.sessions.get(requested)
        if not session:
            raise BrowserSessionError(
                "invalid_browser_handle",
                "Browser handle is unknown or expired. Call browser_attach to recover.",
                retryable=True,
            )
        if session.owner_scope != scope and scope.key() not in session.shared_scopes:
            raise BrowserSessionError(
                "ownership_failure",
                "Browser handle belongs to another principal or agent session.",
            )
        if session.expires_at <= _now():
            raise BrowserSessionError(
                "browser_handle_expired",
                "Browser handle expired after its idle TTL. Call browser_attach to recover.",
                retryable=True,
            )
        if session.attachment.process.returncode is not None:
            raise BrowserSessionError(
                "browser_unavailable",
                "The managed browser process exited. Call browser_attach to recover.",
                retryable=True,
            )
        session.last_used_at = _now()
        session.expires_at = session.last_used_at + self.idle_ttl_seconds
        return session

    async def state(
        self, scope: BrowserScope, *, handle: str | None = None
    ) -> dict[str, Any]:
        try:
            session = self.resolve(scope, handle)
            return await self.execute(
                scope,
                "state",
                lambda current: self._state(current, scope),
                handle=handle,
                mutation=False,
            )
        except BrowserSessionError as exc:
            if exc.code == "browser_not_attached":
                return {"ok": True, "attached": False}
            raise

    async def operation_outcome(
        self,
        scope: BrowserScope,
        *,
        operation_id: str,
        handle: str | None = None,
    ) -> dict[str, Any]:
        session = self.resolve(scope, handle)
        if not OPERATION_RE.fullmatch(operation_id):
            raise BrowserSessionError("invalid_operation_id", "Invalid operation_id.")
        completed = session.operations.get(operation_id)
        if completed is not None:
            return {
                "ok": True,
                "operation_id": operation_id,
                "state": "completed",
                "result": dict(completed),
                "safe_to_retry_with_same_operation_id": True,
            }
        if operation_id in session.inflight:
            return {
                "ok": True,
                "operation_id": operation_id,
                "state": "running",
                "result": None,
                "safe_to_retry_with_same_operation_id": True,
            }
        return {
            "ok": True,
            "operation_id": operation_id,
            "state": "not_started",
            "result": None,
            "safe_to_retry_with_same_operation_id": True,
        }

    async def _state(
        self, session: BrowserSession, scope: BrowserScope, *, shared: bool = False
    ) -> dict[str, Any]:
        metadata = await session.page.metadata()
        viewport = await session.page.viewport()
        return {
            "ok": True,
            "attached": True,
            "browser_handle": session.handle,
            "ownership": (
                "intentionally_shared"
                if shared or session.owner_scope != scope
                else session.ownership
            ),
            "agent_session_id": scope.agent_session_id,
            "instance_id": scope.instance_id,
            "expires_at": session.expires_at,
            "target": metadata,
            **viewport,
        }

    async def detach(
        self, scope: BrowserScope, *, handle: str | None = None
    ) -> dict[str, Any]:
        session = self.resolve(scope, handle)
        if session.owner_scope != scope:
            session.shared_scopes.discard(scope.key())
            self.defaults.pop(scope.key(), None)
            self._audit(session, scope, "detach", None, "success")
            return {"ok": True, "detached": True, "preserved": True}
        async with session.interaction_lock:
            async with self._registry_lock:
                self.sessions.pop(session.handle, None)
                for key, value in list(self.defaults.items()):
                    if value == session.handle:
                        self.defaults.pop(key, None)
                for token, grant in list(self.shares.items()):
                    if grant.handle == session.handle:
                        self.shares.pop(token, None)
            if session.ownership == "agent_owned":
                await self.browser.detach(
                    f"automation-{session.owner_scope.agent_session_id}"
                )
            self._audit(session, scope, "detach", None, "success")
            return {
                "ok": True,
                "attached": False,
                "detached": True,
                "preserved": session.ownership == "user_owned",
            }

    async def execute(
        self,
        scope: BrowserScope,
        action_class: str,
        callback: Callable[[BrowserSession], Awaitable[dict[str, Any]]],
        *,
        handle: str | None = None,
        operation_id: str | None = None,
        mutation: bool = True,
        timeout: float = 20.0,
    ) -> dict[str, Any]:
        session = self.resolve(scope, handle)
        if operation_id and not OPERATION_RE.fullmatch(operation_id):
            raise BrowserSessionError(
                "invalid_operation_id",
                "operation_id must contain 1-128 letters, numbers, dots, colons, underscores, or hyphens.",
            )
        explicit_operation_id = operation_id is not None
        tracked = mutation or explicit_operation_id
        if tracked and operation_id:
            cached = session.operations.get(operation_id)
            if cached is not None:
                result = dict(cached)
                result["deduplicated"] = True
                self._audit(session, scope, action_class, operation_id, "deduplicated")
                return result
            pending = session.inflight.get(operation_id)
            if pending is not None:
                result = dict(await asyncio.shield(pending))
                result["deduplicated"] = True
                self._audit(session, scope, action_class, operation_id, "deduplicated")
                return result
        operation_id = operation_id or _opaque("op_")
        if tracked:
            session.inflight[operation_id] = asyncio.get_running_loop().create_future()
        try:
            queued_at = time.monotonic()
            async with asyncio.timeout(timeout):
                async with session.interaction_lock:
                    lock_acquired_at = time.monotonic()
                    try:
                        result = await callback(session)
                    except BaseException:
                        await self._release_held(session)
                        raise
        except asyncio.CancelledError:
            result = {
                "ok": False,
                "operation_id": operation_id,
                "error": BrowserSessionError(
                    "interrupted_sequence",
                    "Browser operation was cancelled; held input was released where possible and the mutation was not replayed.",
                    retryable=False,
                    ambiguous=True,
                ).as_dict(),
            }
            self._complete_operation(session, operation_id, result, tracked=tracked)
            self._audit(
                session,
                scope,
                action_class,
                operation_id,
                "interrupted",
                "interrupted_sequence",
            )
            raise
        except TimeoutError as exc:
            error = BrowserSessionError(
                "timeout",
                f"Browser {action_class} exceeded the {timeout:g} second deadline.",
                retryable=not mutation,
                ambiguous=mutation,
            )
            result = {
                "ok": False,
                "operation_id": operation_id,
                "error": error.as_dict(),
            }
            self._complete_operation(session, operation_id, result, tracked=tracked)
            self._audit(session, scope, action_class, operation_id, "error", error.code)
            raise error from exc
        except BrowserSessionError as exc:
            result = {"ok": False, "operation_id": operation_id, "error": exc.as_dict()}
            self._complete_operation(session, operation_id, result, tracked=tracked)
            self._audit(session, scope, action_class, operation_id, "error", exc.code)
            raise
        except CdpError as exc:
            error = BrowserSessionError(
                "browser_protocol_error",
                str(exc),
                retryable=not mutation,
                ambiguous=mutation,
            )
            result = {
                "ok": False,
                "operation_id": operation_id,
                "error": error.as_dict(),
            }
            self._complete_operation(session, operation_id, result, tracked=tracked)
            self._audit(session, scope, action_class, operation_id, "error", error.code)
            raise error from exc
        except Exception as exc:
            error = BrowserSessionError(
                "browser_operation_failed",
                "Browser operation failed unexpectedly; a mutating call was not replayed.",
                retryable=not mutation,
                ambiguous=mutation,
            )
            result = {
                "ok": False,
                "operation_id": operation_id,
                "error": error.as_dict(),
            }
            self._complete_operation(session, operation_id, result, tracked=tracked)
            self._audit(session, scope, action_class, operation_id, "error", error.code)
            logger.error(
                "browser operation failed action=%s error_type=%s",
                action_class,
                type(exc).__name__,
            )
            raise error from exc
        result = {
            "ok": True,
            "operation_id": operation_id,
            "browser_handle": session.handle,
            **result,
        }
        result.setdefault("diagnostics", {})
        result["diagnostics"].update(
            {
                "operation_id": operation_id,
                "target_id": session.attachment.target_id,
                "queue_wait_ms": round((lock_acquired_at - queued_at) * 1000, 3),
                "browser_operation_ms": round(
                    (time.monotonic() - lock_acquired_at) * 1000, 3
                ),
            }
        )
        self._complete_operation(session, operation_id, result, tracked=tracked)
        self._audit(session, scope, action_class, operation_id, "success")
        return result

    def _complete_operation(
        self,
        session: BrowserSession,
        operation_id: str,
        result: dict[str, Any],
        *,
        tracked: bool,
    ) -> None:
        if tracked:
            self._remember(session, operation_id, result)
        pending = session.inflight.pop(operation_id, None)
        if pending is not None and not pending.done():
            pending.set_result(dict(result))

    def _remember(
        self, session: BrowserSession, operation_id: str, result: dict[str, Any]
    ) -> None:
        session.operations[operation_id] = dict(result)
        session.operations.move_to_end(operation_id)
        while len(session.operations) > MAX_OPERATION_CACHE:
            session.operations.popitem(last=False)

    def _audit(
        self,
        session: BrowserSession,
        scope: BrowserScope,
        action_class: str,
        operation_id: str | None,
        outcome: str,
        error_code: str | None = None,
    ) -> None:
        record = AuditRecord(
            timestamp=_now(),
            principal_id=scope.principal_id,
            agent_session_id=scope.agent_session_id,
            instance_id=scope.instance_id,
            handle=session.handle,
            target_id=session.attachment.target_id,
            action_class=action_class,
            operation_id=operation_id,
            outcome=outcome,
            error_code=error_code,
        )
        self.audit.append(record)
        if len(self.audit) > 2000:
            del self.audit[:500]
        logger.info(
            "browser_interaction principal=%s agent_session=%s instance=%s handle=%s "
            "target=%s action=%s operation=%s outcome=%s error=%s",
            record.principal_id,
            record.agent_session_id,
            record.instance_id,
            record.handle,
            record.target_id,
            record.action_class,
            record.operation_id,
            record.outcome,
            record.error_code,
        )

    async def snapshot(
        self,
        scope: BrowserScope,
        *,
        handle: str | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        async def take(session: BrowserSession) -> dict[str, Any]:
            value = await session.page.evaluate(_SNAPSHOT_JS)
            document = dict(value.get("document") or {})
            url = str(document.get("url") or "")
            if url.startswith(("chrome-error://", "edge-error://")):
                raise BrowserSessionError(
                    "navigation_invalidation",
                    f"Browser snapshot is an error page: {document.get('title') or document.get('body_text') or url}",
                    retryable=True,
                )
            state = dict(value.get("state") or {})
            metadata = await session.page.metadata()
            snapshot_id = _opaque("snap_")
            locators: dict[str, str] = {}
            elements = []
            for item in value.get("elements") or []:
                element = dict(item)
                ref = f"{snapshot_id}:{int(element.get('index', len(elements)))}"
                locator = str(element.pop("locator", ""))
                locators[ref] = locator
                element["ref"] = ref
                elements.append(element)
            session.snapshots.clear()
            session.snapshots[snapshot_id] = SnapshotRecord(
                snapshot_id=snapshot_id,
                target_id=str(metadata.get("target_id") or ""),
                document_id=str(state.get("document_id") or ""),
                revision=int(state.get("revision") or 0),
                locators=locators,
                created_at=_now(),
            )
            diagnostic = None
            if not elements:
                diagnostic = {
                    "code": "empty_snapshot",
                    "message": "The page exposed no visible interactive or text elements; inspect document/body_text or take a screenshot.",
                }
            return {
                "page": metadata,
                "snapshot_id": snapshot_id,
                "document_revision": int(state.get("revision") or 0),
                "document": document,
                "elements": elements,
                "diagnostic": diagnostic,
            }

        return await self.execute(
            scope,
            "snapshot",
            take,
            handle=handle,
            operation_id=operation_id,
            mutation=False,
        )

    async def _document_state(self, session: BrowserSession) -> dict[str, Any]:
        return dict(await session.page.evaluate(_DOCUMENT_STATE_JS) or {})

    async def _locator(
        self,
        session: BrowserSession,
        *,
        selector: str | None = None,
        ref: str | None = None,
        x: float | None = None,
        y: float | None = None,
    ) -> tuple[float, float]:
        if x is not None or y is not None:
            if selector or ref:
                raise BrowserSessionError(
                    "invalid_coordinates",
                    "Coordinate input cannot be combined with a selector or reference.",
                )
            if x is None or y is None:
                raise BrowserSessionError(
                    "invalid_coordinates",
                    "Both x and y are required for coordinate input.",
                )
            try:
                px, py = float(x), float(y)
            except (TypeError, ValueError) as exc:
                raise BrowserSessionError(
                    "invalid_coordinates", "Coordinates must be finite numbers."
                ) from exc
            if (
                not math.isfinite(px)
                or not math.isfinite(py)
                or abs(px) > MAX_COORDINATE
                or abs(py) > MAX_COORDINATE
            ):
                raise BrowserSessionError(
                    "invalid_coordinates",
                    f"Coordinates must be between {-MAX_COORDINATE:g} and {MAX_COORDINATE:g} CSS pixels.",
                )
            return px, py
        if bool(selector) == bool(ref):
            raise BrowserSessionError(
                "invalid_selector",
                "Provide exactly one of selector, ref, or the coordinate pair x/y.",
            )
        locator = selector
        if ref:
            snapshot_id = ref.rsplit(":", 1)[0]
            snapshot = session.snapshots.get(snapshot_id)
            if not snapshot or ref not in snapshot.locators:
                raise BrowserSessionError(
                    "invalid_snapshot_reference",
                    "Element reference is unknown for this browser target.",
                    retryable=True,
                    details={
                        "expected_target_id": snapshot.target_id,
                        "actual_target_id": str(metadata.get("target_id") or ""),
                        "expected_document_id": snapshot.document_id,
                        "actual_document_id": str(state.get("document_id") or ""),
                        "snapshot_revision": snapshot.revision,
                        "actual_revision": int(state.get("revision") or 0),
                    },
                )
            state = await self._document_state(session)
            metadata = await session.page.metadata()
            if (
                snapshot.target_id != str(metadata.get("target_id") or "")
                or snapshot.document_id != str(state.get("document_id") or "")
            ):
                raise BrowserSessionError(
                    "stale_snapshot_reference",
                    "Element reference is stale because the target navigated; take a new snapshot and retry.",
                    retryable=True,
                )
            locator = snapshot.locators[ref]
        expression = """(() => {
          let el;
          try { el = document.querySelector(__PA_LOCATOR__); }
          catch (error) { return {ok:false, invalid:true, message:String(error)}; }
          if (!el) return {ok:false, missing:true};
          const r = el.getBoundingClientRect();
          if (!r.width || !r.height) return {ok:false, hidden:true};
          el.scrollIntoView({block:'center', inline:'center', behavior:'instant'});
          const q = el.getBoundingClientRect();
          return {ok:true, x:q.left + q.width / 2, y:q.top + q.height / 2};
        })()""".replace("__PA_LOCATOR__", json.dumps(locator))
        result = dict(await session.page.evaluate(expression) or {})
        if result.get("invalid"):
            raise BrowserSessionError(
                "invalid_selector",
                f"Invalid CSS selector: {result.get('message') or 'syntax error'}",
            )
        if not result.get("ok"):
            reason = "not visible" if result.get("hidden") else "not found"
            raise BrowserSessionError(
                "invalid_selector",
                f"Element {reason} for the supplied selector/reference.",
                retryable=bool(result.get("missing")),
            )
        return float(result["x"]), float(result["y"])

    async def click(
        self,
        scope: BrowserScope,
        *,
        selector: str | None = None,
        ref: str | None = None,
        x: float | None = None,
        y: float | None = None,
        button: str | int | None = "left",
        click_count: int = 1,
        modifiers: list[str] | None = None,
        handle: str | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        button_name = _button(button)
        if click_count not in {1, 2, 3}:
            raise BrowserSessionError(
                "invalid_click_count", "click_count must be 1, 2, or 3."
            )
        _, modifier_bits = _modifiers(modifiers)

        async def perform(session: BrowserSession) -> dict[str, Any]:
            px, py = await self._locator(session, selector=selector, ref=ref, x=x, y=y)
            await session.page.command(
                "Input.dispatchMouseEvent",
                {
                    "type": "mousePressed",
                    "x": px,
                    "y": py,
                    "button": button_name,
                    "buttons": 1 << BUTTONS[button_name],
                    "clickCount": click_count,
                    "modifiers": modifier_bits,
                },
            )
            await session.page.command(
                "Input.dispatchMouseEvent",
                {
                    "type": "mouseReleased",
                    "x": px,
                    "y": py,
                    "button": button_name,
                    "buttons": 0,
                    "clickCount": click_count,
                    "modifiers": modifier_bits,
                },
            )
            session.pointer_x, session.pointer_y = px, py
            return {
                "action": "click",
                "button": button_name,
                "click_count": click_count,
                "point": {"x": px, "y": py},
            }

        return await self.execute(
            scope, "click", perform, handle=handle, operation_id=operation_id
        )

    async def hover(
        self,
        scope: BrowserScope,
        *,
        selector: str | None = None,
        ref: str | None = None,
        x: float | None = None,
        y: float | None = None,
        modifiers: list[str] | None = None,
        handle: str | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        _, modifier_bits = _modifiers(modifiers)

        async def perform(session: BrowserSession) -> dict[str, Any]:
            px, py = await self._locator(session, selector=selector, ref=ref, x=x, y=y)
            await session.page.command(
                "Input.dispatchMouseEvent",
                {
                    "type": "mouseMoved",
                    "x": px,
                    "y": py,
                    "buttons": self._buttons_mask(session),
                    "modifiers": modifier_bits,
                },
            )
            session.pointer_x, session.pointer_y = px, py
            return {"action": "hover", "point": {"x": px, "y": py}}

        return await self.execute(
            scope, "hover", perform, handle=handle, operation_id=operation_id
        )

    async def press(
        self,
        scope: BrowserScope,
        *,
        key: str,
        modifiers: list[str] | None = None,
        handle: str | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        key_name = _key(key)
        modifier_names, _ = _modifiers(modifiers)

        async def perform(session: BrowserSession) -> dict[str, Any]:
            for modifier in modifier_names:
                await self._key_event(session, "keyDown", modifier)
            await self._key_event(session, "keyDown", key_name)
            await self._key_event(session, "keyUp", key_name)
            for modifier in reversed(modifier_names):
                await self._key_event(session, "keyUp", modifier)
            return {"action": "press", "key": key_name, "modifiers": modifier_names}

        return await self.execute(
            scope, "press", perform, handle=handle, operation_id=operation_id
        )

    async def type_text(
        self,
        scope: BrowserScope,
        *,
        selector: str | None,
        ref: str | None = None,
        text: str,
        clear: bool = True,
        submit: bool = False,
        delay_ms: int = 0,
        modifiers: list[str] | None = None,
        handle: str | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        if len(text) > 100_000:
            raise BrowserSessionError(
                "quota_exceeded", "Typed text exceeds 100,000 characters."
            )
        if not 0 <= delay_ms <= 1000:
            raise BrowserSessionError(
                "invalid_delay", "delay_ms must be between 0 and 1000."
            )
        modifier_names, _ = _modifiers(modifiers)

        async def perform(session: BrowserSession) -> dict[str, Any]:
            px, py = await self._locator(session, selector=selector, ref=ref)
            await session.page.command(
                "Input.dispatchMouseEvent",
                {
                    "type": "mousePressed",
                    "x": px,
                    "y": py,
                    "button": "left",
                    "clickCount": 1,
                },
            )
            await session.page.command(
                "Input.dispatchMouseEvent",
                {
                    "type": "mouseReleased",
                    "x": px,
                    "y": py,
                    "button": "left",
                    "clickCount": 1,
                },
            )
            if clear:
                await self._key_event(session, "keyDown", "Control")
                await self._key_event(session, "keyDown", "a")
                await self._key_event(session, "keyUp", "a")
                await self._key_event(session, "keyUp", "Control")
                await self._key_event(session, "keyDown", "Backspace")
                await self._key_event(session, "keyUp", "Backspace")
            for modifier in modifier_names:
                await self._key_event(session, "keyDown", modifier)
            if delay_ms or modifier_names:
                for character in text:
                    await self._key_event(session, "keyDown", character)
                    await self._key_event(session, "keyUp", character)
                    if delay_ms:
                        await asyncio.sleep(delay_ms / 1000)
            else:
                await session.page.command("Input.insertText", {"text": text})
            for modifier in reversed(modifier_names):
                await self._key_event(session, "keyUp", modifier)
            if submit:
                submitted = bool(
                    await session.page.evaluate(
                        """(() => { const element = document.activeElement;
                        if (!element?.form) return false;
                        element.form.requestSubmit(); return true; })()"""
                    )
                )
                if not submitted:
                    await self._key_event(session, "keyDown", "Enter")
                    await self._key_event(session, "keyUp", "Enter")
            return {
                "action": "type",
                "characters": len(text),
                "clear": clear,
                "submit": submit,
                "delay_ms": delay_ms,
            }

        timeout = min(120.0, 20.0 + len(text) * delay_ms / 1000)
        return await self.execute(
            scope,
            "type",
            perform,
            handle=handle,
            operation_id=operation_id,
            timeout=timeout,
        )

    async def scroll(
        self,
        scope: BrowserScope,
        *,
        delta_x: float = 0,
        delta_y: float = 0,
        selector: str | None = None,
        ref: str | None = None,
        x: float | None = None,
        y: float | None = None,
        handle: str | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        if (
            not math.isfinite(delta_x)
            or not math.isfinite(delta_y)
            or abs(delta_x) > MAX_WHEEL_DELTA
            or abs(delta_y) > MAX_WHEEL_DELTA
        ):
            raise BrowserSessionError(
                "invalid_scroll_delta",
                f"Wheel deltas must be within ±{MAX_WHEEL_DELTA:g} CSS pixels.",
            )

        async def perform(session: BrowserSession) -> dict[str, Any]:
            if selector or ref or x is not None or y is not None:
                px, py = await self._locator(
                    session, selector=selector, ref=ref, x=x, y=y
                )
            else:
                viewport = await session.page.viewport()
                px, py = float(viewport["width"]) / 2, float(viewport["height"]) / 2
            await session.page.command(
                "Input.dispatchMouseEvent",
                {
                    "type": "mouseWheel",
                    "x": px,
                    "y": py,
                    "deltaX": float(delta_x),
                    "deltaY": float(delta_y),
                    "buttons": self._buttons_mask(session),
                },
            )
            return {
                "action": "scroll",
                "unit": "css_pixels",
                "delta_x": delta_x,
                "delta_y": delta_y,
                "point": {"x": px, "y": py},
            }

        return await self.execute(
            scope, "scroll", perform, handle=handle, operation_id=operation_id
        )

    async def drag(
        self,
        scope: BrowserScope,
        *,
        source_selector: str | None = None,
        source_ref: str | None = None,
        source_x: float | None = None,
        source_y: float | None = None,
        target_selector: str | None = None,
        target_ref: str | None = None,
        target_x: float | None = None,
        target_y: float | None = None,
        button: str | int | None = "left",
        steps: int = 10,
        handle: str | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        button_name = _button(button)
        if not 1 <= steps <= 50:
            raise BrowserSessionError(
                "invalid_steps", "steps must be between 1 and 50."
            )

        async def perform(session: BrowserSession) -> dict[str, Any]:
            sx, sy = await self._locator(
                session,
                selector=source_selector,
                ref=source_ref,
                x=source_x,
                y=source_y,
            )
            tx, ty = await self._locator(
                session,
                selector=target_selector,
                ref=target_ref,
                x=target_x,
                y=target_y,
            )
            await self._pointer_move(session, sx, sy)
            await self._pointer_down(session, button_name)
            for step in range(1, steps + 1):
                await self._pointer_move(
                    session,
                    sx + (tx - sx) * step / steps,
                    sy + (ty - sy) * step / steps,
                )
            await self._pointer_up(session, button_name)
            return {
                "action": "drag",
                "button": button_name,
                "steps": steps,
                "source": {"x": sx, "y": sy},
                "target": {"x": tx, "y": ty},
            }

        return await self.execute(
            scope, "drag", perform, handle=handle, operation_id=operation_id
        )

    async def actions(
        self,
        scope: BrowserScope,
        *,
        actions: list[dict[str, Any]],
        handle: str | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        validated = self.validate_actions(actions)

        async def perform(session: BrowserSession) -> dict[str, Any]:
            before = await self._document_state(session)
            completed = 0
            for action in validated:
                await self._perform_action(session, action)
                completed += 1
            after = await self._document_state(session)
            if before.get("document_id") != after.get("document_id"):
                raise BrowserSessionError(
                    "navigation_invalidation",
                    "The page navigated during the input sequence; completion is ambiguous and the sequence was not replayed.",
                    retryable=False,
                    ambiguous=True,
                    details={"completed_actions": completed},
                )
            return {"action": "actions", "action_count": completed}

        return await self.execute(
            scope,
            "actions",
            perform,
            handle=handle,
            operation_id=operation_id,
            timeout=35.0,
        )

    def validate_actions(self, actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not isinstance(actions, list) or not actions:
            raise BrowserSessionError(
                "invalid_action_sequence", "actions must be a non-empty ordered list."
            )
        if len(actions) > MAX_ACTIONS:
            raise BrowserSessionError(
                "quota_exceeded",
                f"An action sequence may contain at most {MAX_ACTIONS} actions.",
            )
        total_pause = 0.0
        validated: list[dict[str, Any]] = []
        allowed = {
            "pointer_move",
            "pointer_down",
            "pointer_up",
            "key_down",
            "key_press",
            "key_up",
            "wheel",
            "pause",
        }
        fields = {
            "pointer_move": {"type", "x", "y"},
            "pointer_down": {"type", "button"},
            "pointer_up": {"type", "button"},
            "key_down": {"type", "key"},
            "key_press": {"type", "key"},
            "key_up": {"type", "key"},
            "wheel": {"type", "delta_x", "delta_y"},
            "pause": {"type", "duration_ms"},
        }
        for index, raw in enumerate(actions):
            if not isinstance(raw, dict) or raw.get("type") not in allowed:
                raise BrowserSessionError(
                    "invalid_action_sequence",
                    f"Action {index} must use one of {sorted(allowed)}.",
                )
            action = dict(raw)
            kind = action["type"]
            unexpected = set(action) - fields[kind]
            if unexpected:
                raise BrowserSessionError(
                    "invalid_action_sequence",
                    f"Action {index} contains unsupported fields: {sorted(unexpected)}.",
                )
            if kind == "pointer_move":
                x, y = action.get("x"), action.get("y")
                try:
                    x, y = float(x), float(y)
                except TypeError, ValueError:
                    x = y = MAX_COORDINATE + 1
                if (
                    not math.isfinite(x)
                    or not math.isfinite(y)
                    or abs(x) > MAX_COORDINATE
                    or abs(y) > MAX_COORDINATE
                ):
                    raise BrowserSessionError(
                        "invalid_coordinates",
                        f"Action {index} has invalid x/y coordinates.",
                    )
                action["x"], action["y"] = x, y
            elif kind in {"pointer_down", "pointer_up"}:
                action["button"] = _button(action.get("button"))
            elif kind in {"key_down", "key_press", "key_up"}:
                action["key"] = _key(str(action.get("key", "")))
            elif kind == "wheel":
                try:
                    dx, dy = (
                        float(action.get("delta_x", 0)),
                        float(action.get("delta_y", 0)),
                    )
                except (TypeError, ValueError) as exc:
                    raise BrowserSessionError(
                        "invalid_scroll_delta",
                        f"Action {index} has non-numeric wheel deltas.",
                    ) from exc
                if (
                    not math.isfinite(dx)
                    or not math.isfinite(dy)
                    or abs(dx) > MAX_WHEEL_DELTA
                    or abs(dy) > MAX_WHEEL_DELTA
                ):
                    raise BrowserSessionError(
                        "invalid_scroll_delta", f"Action {index} exceeds wheel limits."
                    )
                action["delta_x"], action["delta_y"] = dx, dy
            elif kind == "pause":
                try:
                    duration = float(action.get("duration_ms", 0)) / 1000
                except (TypeError, ValueError) as exc:
                    raise BrowserSessionError(
                        "invalid_pause",
                        f"Action {index} has a non-numeric pause.",
                    ) from exc
                if not 0 <= duration <= MAX_PAUSE_SECONDS:
                    raise BrowserSessionError(
                        "invalid_pause",
                        f"Action {index} pause must be 0-{MAX_PAUSE_SECONDS * 1000:g} ms.",
                    )
                total_pause += duration
                action["duration_ms"] = int(duration * 1000)
            validated.append(action)
        if total_pause > MAX_TOTAL_PAUSE_SECONDS:
            raise BrowserSessionError(
                "invalid_pause",
                f"Total pause time may not exceed {MAX_TOTAL_PAUSE_SECONDS:g} seconds.",
            )
        return validated

    async def _perform_action(
        self, session: BrowserSession, action: dict[str, Any]
    ) -> None:
        kind = action["type"]
        if kind == "pointer_move":
            await self._pointer_move(session, action["x"], action["y"])
        elif kind == "pointer_down":
            await self._pointer_down(session, action["button"])
        elif kind == "pointer_up":
            await self._pointer_up(session, action["button"])
        elif kind == "key_down":
            await self._key_event(session, "keyDown", action["key"])
        elif kind == "key_press":
            await self._key_event(session, "keyDown", action["key"])
            await self._key_event(session, "keyUp", action["key"])
        elif kind == "key_up":
            await self._key_event(session, "keyUp", action["key"])
        elif kind == "wheel":
            await session.page.command(
                "Input.dispatchMouseEvent",
                {
                    "type": "mouseWheel",
                    "x": session.pointer_x,
                    "y": session.pointer_y,
                    "deltaX": action["delta_x"],
                    "deltaY": action["delta_y"],
                    "buttons": self._buttons_mask(session),
                },
            )
        elif kind == "pause":
            await asyncio.sleep(action["duration_ms"] / 1000)

    async def _pointer_move(self, session: BrowserSession, x: float, y: float) -> None:
        await session.page.command(
            "Input.dispatchMouseEvent",
            {
                "type": "mouseMoved",
                "x": x,
                "y": y,
                "buttons": self._buttons_mask(session),
            },
        )
        session.pointer_x, session.pointer_y = x, y

    async def _pointer_down(self, session: BrowserSession, button: str) -> None:
        session.held_buttons.add(button)
        await session.page.command(
            "Input.dispatchMouseEvent",
            {
                "type": "mousePressed",
                "x": session.pointer_x,
                "y": session.pointer_y,
                "button": button,
                "buttons": self._buttons_mask(session),
                "clickCount": 1,
            },
        )

    async def _pointer_up(self, session: BrowserSession, button: str) -> None:
        released_buttons = session.held_buttons - {button}
        await session.page.command(
            "Input.dispatchMouseEvent",
            {
                "type": "mouseReleased",
                "x": session.pointer_x,
                "y": session.pointer_y,
                "button": button,
                "buttons": sum(1 << BUTTONS[item] for item in released_buttons),
                "clickCount": 1,
            },
        )
        session.held_buttons.discard(button)

    @staticmethod
    def _buttons_mask(session: BrowserSession) -> int:
        return sum(1 << BUTTONS[item] for item in session.held_buttons)

    async def _key_event(
        self, session: BrowserSession, event_type: str, key: str
    ) -> None:
        key_name = _key(key)
        if event_type == "keyDown":
            session.held_keys.add(key_name)
            effective_keys = session.held_keys
        else:
            effective_keys = session.held_keys - {key_name}
        modifier_names = [item for item in effective_keys if item in MODIFIER_BITS]
        if len(key_name) == 1:
            if key_name.isalpha():
                code, virtual_key = f"Key{key_name.upper()}", ord(key_name.upper())
            elif key_name.isdigit():
                code, virtual_key = f"Digit{key_name}", ord(key_name)
            else:
                code, virtual_key = "", ord(key_name)
        else:
            code, virtual_key = KEYBOARD_CODES[key_name]
        params: dict[str, Any] = {
            "type": event_type,
            "key": " " if key_name == "Space" else key_name,
            "code": code,
            "modifiers": sum(MODIFIER_BITS[item] for item in modifier_names),
            "windowsVirtualKeyCode": virtual_key,
            "nativeVirtualKeyCode": virtual_key,
        }
        if (
            len(key_name) == 1
            and event_type == "keyDown"
            and not ({"Alt", "Control", "Meta"} & session.held_keys)
        ):
            params["text"] = key_name
        await session.page.command("Input.dispatchKeyEvent", params)
        if event_type == "keyUp":
            session.held_keys.discard(key_name)

    async def _release_held(self, session: BrowserSession) -> None:
        for button in list(session.held_buttons):
            try:
                await self._pointer_up(session, button)
            except CdpError, OSError, RuntimeError:
                session.held_buttons.discard(button)
        for key in list(session.held_keys):
            try:
                await self._key_event(session, "keyUp", key)
            except CdpError, OSError, RuntimeError:
                session.held_keys.discard(key)

    async def open(
        self,
        scope: BrowserScope,
        *,
        url: str,
        handle: str | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        async def perform(session: BrowserSession) -> dict[str, Any]:
            diagnostic = await session.page.navigate_and_wait(url)
            session.snapshots.clear()
            return {
                "action": "open",
                "page": await session.page.metadata(),
                "document": diagnostic,
            }

        return await self.execute(
            scope, "open", perform, handle=handle, operation_id=operation_id
        )

    async def back(
        self,
        scope: BrowserScope,
        *,
        handle: str | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        async def perform(session: BrowserSession) -> dict[str, Any]:
            await session.page.evaluate("history.back(); true")
            session.snapshots.clear()
            return {"action": "back", "navigating": True}

        return await self.execute(
            scope, "back", perform, handle=handle, operation_id=operation_id
        )

    async def resize(
        self,
        scope: BrowserScope,
        *,
        width: int,
        height: int,
        device_scale_factor: float = 1,
        handle: str | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        async def perform(session: BrowserSession) -> dict[str, Any]:
            await session.attachment.resize(
                width, height, device_scale_factor=device_scale_factor
            )
            return {
                "action": "resize",
                "width": width,
                "height": height,
                "device_scale_factor": device_scale_factor,
            }

        return await self.execute(
            scope, "resize", perform, handle=handle, operation_id=operation_id
        )

    async def screenshot(
        self, scope: BrowserScope, *, handle: str | None = None
    ) -> bytes:
        session = self.resolve(scope, handle)
        async with session.interaction_lock:
            data = await session.page.screenshot()
        self._audit(session, scope, "screenshot", None, "success")
        return data

    async def _cleanup_locked(self) -> None:
        now = _now()
        expired = [
            session for session in self.sessions.values() if session.expires_at <= now
        ]
        for session in expired:
            self.sessions.pop(session.handle, None)
            for key, handle in list(self.defaults.items()):
                if handle == session.handle:
                    self.defaults.pop(key, None)
            if session.ownership == "agent_owned":
                await self.browser.detach(
                    f"automation-{session.owner_scope.agent_session_id}"
                )
        for token, grant in list(self.shares.items()):
            if grant.expires_at <= now or grant.used:
                self.shares.pop(token, None)

    async def cleanup(self) -> None:
        async with self._registry_lock:
            await self._cleanup_locked()

    async def close(self) -> None:
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            await self._cleanup_task
            self._cleanup_task = None
        for session in list(self.sessions.values()):
            if session.ownership == "agent_owned":
                await self.browser.detach(
                    f"automation-{session.owner_scope.agent_session_id}"
                )
        self.sessions.clear()
        self.defaults.clear()
        self.shares.clear()
