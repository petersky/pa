"""Authenticated filesystem browser and source/diff viewer."""

from __future__ import annotations

import mimetypes
import re
import subprocess
from pathlib import Path
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse

from pa.auth.middleware import get_principal_id
from pa.core.contracts import Module

router = APIRouter()

MAX_TEXT_BYTES = 2 * 1024 * 1024
MAX_RAW_BYTES = 20 * 1024 * 1024
SOURCE_SUFFIXES = {
    ".c", ".cc", ".cpp", ".css", ".go", ".h", ".hpp", ".html", ".ini",
    ".java", ".js", ".json", ".jsx", ".kt", ".md", ".mjs", ".py", ".rb",
    ".rs", ".sh", ".sql", ".toml", ".ts", ".tsx", ".txt", ".xml", ".yaml",
    ".yml", ".zsh",
}
MARKDOWN_SUFFIXES = {".md", ".markdown", ".mdown"}
IMAGE_SUFFIXES = {".gif", ".jpeg", ".jpg", ".png", ".svg", ".webp"}
REVISION_RE = re.compile(r"^[A-Za-z0-9_./@{}^~:+-]+$")


def _browse_url(path: Path, **params: str | int) -> str:
    query: dict[str, str | int] = {"path": str(path)}
    query.update(params)
    return "/browse?" + urlencode(query)


def _trusted_roots(request: Request) -> list[Path]:
    ctx = request.app.state.ctx
    auth_required = bool(ctx.settings.auth_required)
    principal_id = get_principal_id(request)
    data_dir = Path(ctx.settings.data_dir)
    candidates: list[Path] = []
    if auth_required:
        if principal_id.startswith("user:"):
            candidates.append(data_dir / "users" / principal_id[5:])
    else:
        candidates.append(data_dir)
    try:
        candidates.extend(
            Path(session.cwd)
            for session in ctx.store.list_sessions()
            if session.cwd
            and (
                not auth_required
                or getattr(session, "principal_id", None) == principal_id
            )
        )
    except (AttributeError, TypeError):
        pass
    try:
        for project in ctx.store.list_projects():
            memberships = getattr(project, "memberships", ()) or ()
            can_access = (
                not auth_required
                or getattr(project, "created_by_principal", None) == principal_id
                or any(
                    getattr(membership, "principal_id", None) == principal_id
                    for membership in memberships
                )
            )
            if not can_access:
                continue
            candidates.extend(Path(repo.path) for repo in project.repos if repo.path)
    except (AttributeError, TypeError):
        pass
    roots: list[Path] = []
    for candidate in candidates:
        try:
            resolved = candidate.expanduser().resolve()
        except OSError:
            continue
        if resolved not in roots:
            roots.append(resolved)
    return roots


def _resolve_authorized_path(request: Request, raw_path: str) -> tuple[Path, Path]:
    if (
        request.app.state.ctx.settings.auth_required
        and not getattr(request.state, "user_authenticated", False)
    ):
        raise HTTPException(status_code=401, detail="Authentication required")
    if not raw_path:
        raise HTTPException(status_code=400, detail="A filesystem path is required")
    requested = Path(raw_path).expanduser()
    if not requested.is_absolute():
        raise HTTPException(status_code=400, detail="The path must be absolute")
    try:
        resolved = requested.resolve(strict=True)
    except (FileNotFoundError, OSError) as exc:
        raise HTTPException(status_code=404, detail=f"Path not found: {requested}") from exc
    for root in _trusted_roots(request):
        if resolved == root or resolved.is_relative_to(root):
            return resolved, root
    raise HTTPException(
        status_code=403,
        detail="Path is outside PA data, session, and project working directories",
    )


def _breadcrumbs(path: Path, root: Path) -> list[dict[str, str]]:
    crumbs = [{"label": root.name or str(root), "url": _browse_url(root)}]
    current = root
    for part in path.relative_to(root).parts:
        current = current / part
        crumbs.append({"label": part, "url": _browse_url(current)})
    return crumbs


def _is_text(path: Path, mime: str | None) -> bool:
    if path.suffix.lower() in SOURCE_SUFFIXES or (mime and mime.startswith("text/")):
        return True
    try:
        with path.open("rb") as handle:
            return b"\0" not in handle.read(4096)
    except OSError:
        return False


def _git_root(path: Path) -> Path | None:
    proc = subprocess.run(
        ["git", "-C", str(path.parent), "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        timeout=3,
        check=False,
    )
    if proc.returncode != 0:
        return None
    try:
        return Path(proc.stdout.strip()).resolve(strict=True)
    except (FileNotFoundError, OSError):
        return None


def _default_diff_base(repo: Path) -> str:
    for candidate in ("origin/main", "origin/master", "HEAD"):
        check = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "--verify", f"{candidate}^{{commit}}"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
        if check.returncode == 0:
            return candidate
    return "HEAD"


def _file_diff(path: Path, base: str | None) -> tuple[str, str | None]:
    path = path.resolve()
    repo = _git_root(path)
    if not repo or not path.is_relative_to(repo):
        return "", None
    revision = base or _default_diff_base(repo)
    if revision.startswith("-") or not REVISION_RE.fullmatch(revision):
        raise HTTPException(status_code=400, detail="Invalid Git revision")
    relative = str(path.relative_to(repo))
    proc = subprocess.run(
        [
            "git", "-C", str(repo), "diff", "--no-ext-diff", "--no-color",
            "--unified=3", revision, "--", relative,
        ],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    return proc.stdout if proc.returncode in {0, 1} else "", revision


def _inline_diff(raw: str) -> list[dict[str, str]]:
    rows = []
    for line in raw.splitlines():
        kind = "context"
        if line.startswith("@@"):
            kind = "hunk"
        elif line.startswith("+") and not line.startswith("+++"):
            kind = "added"
        elif line.startswith("-") and not line.startswith("---"):
            kind = "removed"
        elif line.startswith(("diff ", "index ", "---", "+++")):
            kind = "meta"
        rows.append({"kind": kind, "text": line})
    return rows


def _side_by_side_diff(raw: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    old_no = new_no = 0
    lines = raw.splitlines()
    index = 0
    while index < len(lines):
        line = lines[index]
        match = re.match(r"@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@", line)
        if match:
            old_no, new_no = int(match.group(1)), int(match.group(2))
            rows.append({"kind": "hunk", "left": line, "right": line, "left_no": "", "right_no": ""})
            index += 1
            continue
        if line.startswith(("diff ", "index ", "---", "+++")):
            index += 1
            continue
        if line.startswith("-"):
            removed = []
            while index < len(lines) and lines[index].startswith("-") and not lines[index].startswith("---"):
                removed.append(lines[index][1:])
                index += 1
            added = []
            while index < len(lines) and lines[index].startswith("+") and not lines[index].startswith("+++"):
                added.append(lines[index][1:])
                index += 1
            for offset in range(max(len(removed), len(added))):
                left = removed[offset] if offset < len(removed) else ""
                right = added[offset] if offset < len(added) else ""
                rows.append({
                    "kind": "changed", "left": left, "right": right,
                    "left_no": str(old_no) if left else "",
                    "right_no": str(new_no) if right else "",
                })
                old_no += bool(left)
                new_no += bool(right)
            continue
        if line.startswith("+") and not line.startswith("+++"):
            while (
                index < len(lines)
                and lines[index].startswith("+")
                and not lines[index].startswith("+++")
            ):
                rows.append(
                    {
                        "kind": "changed",
                        "left": "",
                        "right": lines[index][1:],
                        "left_no": "",
                        "right_no": str(new_no),
                    }
                )
                new_no += 1
                index += 1
            continue
        if line.startswith(" "):
            rows.append({
                "kind": "context", "left": line[1:], "right": line[1:],
                "left_no": str(old_no), "right_no": str(new_no),
            })
            old_no += 1
            new_no += 1
        index += 1
    return rows


def _file_context(request: Request, path: Path, root: Path, line: int | None, base: str | None) -> dict:
    stat = path.stat()
    mime, _ = mimetypes.guess_type(path.name)
    context = {
        "kind": "file",
        "path": path,
        "root": root,
        "breadcrumbs": _breadcrumbs(path, root),
        "size": stat.st_size,
        "mime": mime or "application/octet-stream",
        "line": line,
        "raw_url": "/api/files/raw?" + urlencode({"path": str(path)}),
        "download_url": "/api/files/raw?" + urlencode({"path": str(path), "download": "1"}),
        "is_image": path.suffix.lower() in IMAGE_SUFFIXES,
        "is_markdown": path.suffix.lower() in MARKDOWN_SUFFIXES,
        "is_text": _is_text(path, mime),
    }
    if context["is_text"]:
        if stat.st_size > MAX_TEXT_BYTES:
            context["too_large"] = True
        else:
            text = path.read_text(encoding="utf-8", errors="replace")
            context["text"] = text
            context["language"] = path.suffix.lower().lstrip(".") or "text"
            diff, revision = _file_diff(path, base)
            context["diff_base"] = revision
            context["inline_diff"] = _inline_diff(diff)
            context["side_diff"] = _side_by_side_diff(diff)
    return context


@router.get("/browse", response_class=HTMLResponse)
async def browse_files(
    request: Request,
    path: str,
    line: int | None = None,
    base: str | None = None,
) -> HTMLResponse:
    runtime = request.app.state.ctx.require_service("async_runtime")
    return await runtime.run_blocking(
        "files.browse_render",
        _render_browser,
        request,
        path,
        line,
        base,
        timeout=30.0,
    )


def _render_browser(
    request: Request, path: str, line: int | None, base: str | None
) -> HTMLResponse:
    resolved, root = _resolve_authorized_path(request, path)
    context = {
        "page": type("BrowsePage", (), {"label": "Files", "path": "/browse"})(),
        "active_path": "/browse",
    }
    from pa.modules.ui_shell import _shell_context

    context.update(_shell_context(request))
    if resolved.is_dir():
        entries = []
        try:
            children = sorted(resolved.iterdir(), key=lambda item: (not item.is_dir(), item.name.lower()))[:1000]
        except OSError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        for child in children:
            try:
                entries.append({
                    "name": child.name,
                    "is_dir": child.is_dir(),
                    "size": child.stat().st_size if child.is_file() else None,
                    "url": _browse_url(child),
                })
            except OSError:
                continue
        context.update({
            "kind": "directory", "path": resolved, "root": root,
            "breadcrumbs": _breadcrumbs(resolved, root), "entries": entries,
            "parent_url": _browse_url(resolved.parent) if resolved != root else None,
        })
    else:
        context.update(_file_context(request, resolved, root, line, base))
    templates = request.app.state.templates
    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(request, "pages/browse.html", context)
    context["include_template"] = "pages/browse.html"
    return templates.TemplateResponse(request, "shell.html", context)


@router.get("/api/files/raw")
async def raw_file(
    request: Request, path: str, download: bool = False
) -> FileResponse:
    runtime = request.app.state.ctx.require_service("async_runtime")
    return await runtime.run_blocking(
        "files.raw_resolve",
        _raw_file_response,
        request,
        path,
        download,
        timeout=10.0,
    )


def _raw_file_response(
    request: Request, path: str, download: bool
) -> FileResponse:
    resolved, _ = _resolve_authorized_path(request, path)
    if not resolved.is_file():
        raise HTTPException(status_code=400, detail="Path is not a file")
    if resolved.stat().st_size > MAX_RAW_BYTES:
        raise HTTPException(status_code=413, detail="File exceeds the 20 MB viewer limit")
    return FileResponse(
        resolved,
        filename=resolved.name if download else None,
        content_disposition_type="attachment" if download else "inline",
        headers={
            "Content-Security-Policy": "sandbox; default-src 'none'; img-src data:; style-src 'unsafe-inline'",
            "X-Content-Type-Options": "nosniff",
        },
    )


class FilesModule(Module):
    @property
    def name(self) -> str:
        return "files"

    @property
    def description(self) -> str:
        return "Authenticated filesystem, Markdown, source, and diff viewer"

    def ui_routers(self):
        return [router]
