#!/usr/bin/env python3
"""Emit a deterministic inventory of PA server concurrency boundaries.

This is intentionally a static *index*, not a claim that a call is blocking.
The reviewed transitive call paths, measurements, and decisions live in
docs/ASYNC_BLOCKING_AUDIT.md. Run this after adding endpoints or workers so the
checked-in appendix cannot silently omit a new coroutine or MCP handler.
"""

from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass, field
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "src" / "pa"

ASYNC_BOUNDARY_NAMES = {
    "on_startup": "startup",
    "on_shutdown": "shutdown",
    "start": "lifecycle",
    "stop": "lifecycle",
    "close": "lifecycle",
    "disconnect": "lifecycle",
}
HTTP_DECORATORS = {"get", "post", "put", "patch", "delete", "api_route"}
FILESYSTEM_METHODS = {
    "exists",
    "glob",
    "is_dir",
    "is_file",
    "iterdir",
    "mkdir",
    "open",
    "read_bytes",
    "read_text",
    "replace",
    "resolve",
    "rglob",
    "stat",
    "unlink",
    "write_bytes",
    "write_text",
}
STORE_RECEIVERS = {
    "store",
    "domain_store",
    "workspace_manager",
    "event_log",
    "log",
    "fleet",
    "membership",
    "peer_table",
    "registry",
    "ledger",
}


def dotted(node: ast.AST) -> str:
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def decorators(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    return [
        dotted(item.func if isinstance(item, ast.Call) else item)
        for item in node.decorator_list
    ]


@dataclass
class Boundary:
    path: str
    line: int
    qualname: str
    async_def: bool
    context: str
    signals: set[str] = field(default_factory=set)
    boundaries: set[str] = field(default_factory=set)


class FileVisitor(ast.NodeVisitor):
    def __init__(self, path: Path) -> None:
        self.path = path
        self.stack: list[str] = []
        self.boundaries: list[Boundary] = []

    def _context(self, node, *, async_def: bool) -> str | None:
        decos = decorators(node)
        if any(item.endswith("mcp.tool") for item in decos):
            return "async MCP handler" if async_def else "sync MCP handler"
        if any(item.rsplit(".", 1)[-1] in HTTP_DECORATORS for item in decos):
            return "async HTTP endpoint" if async_def else "sync HTTP endpoint"
        if async_def:
            if node.name in ASYNC_BOUNDARY_NAMES:
                return ASYNC_BOUNDARY_NAMES[node.name]
            if node.name in {"event_stream", "stream", "relay"} or any(
                isinstance(item, (ast.Yield, ast.YieldFrom)) for item in ast.walk(node)
            ):
                return "SSE/stream producer"
            if node.name.startswith("_on_") or node.name.endswith("_callback"):
                return "agent/provider callback"
            if node.name in {
                "_run",
                "_run_loop",
                "_runner",
                "run_once",
                "run_update_job",
                "run_install_job",
            }:
                return "background worker"
            return "async helper"
        return None

    def _visit_function(self, node, *, async_def: bool) -> None:
        context = self._context(node, async_def=async_def)
        self.stack.append(node.name)
        if context:
            boundary = Boundary(
                path=str(self.path.relative_to(ROOT)),
                line=node.lineno,
                qualname=".".join(self.stack),
                async_def=async_def,
                context=context,
            )
            self._scan_body(node, boundary)
            self.boundaries.append(boundary)
        for item in node.body:
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                self.visit(item)
        self.stack.pop()

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.stack.append(node.name)
        for item in node.body:
            self.visit(item)
        self.stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_function(node, async_def=True)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_function(node, async_def=False)

    def _scan_body(self, node, boundary: Boundary) -> None:
        nested = {
            item
            for item in ast.walk(node)
            if item is not node
            and isinstance(
                item, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)
            )
        }
        nested_nodes: set[ast.AST] = set()
        for parent in nested:
            nested_nodes.update(ast.walk(parent))
        for item in ast.walk(node):
            if item in nested_nodes:
                continue
            if not isinstance(item, ast.Call):
                continue
            name = dotted(item.func)
            receiver, _, method = name.rpartition(".")
            root_receiver = receiver.split(".")[-1]
            if name in {"asyncio.to_thread", "loop.run_in_executor"} or method == "run_blocking":
                boundary.boundaries.add("bounded/thread offload")
            if method == "observe" or name in {"asyncio.wait_for", "asyncio.timeout"}:
                boundary.boundaries.add("async deadline")
            if name.startswith("asyncio.create_subprocess"):
                boundary.boundaries.add("async subprocess")
            if name in {
                "subprocess.run",
                "subprocess.Popen",
                "subprocess.check_call",
                "subprocess.check_output",
            }:
                boundary.signals.add("sync subprocess")
            if name in {"time.sleep", "sleep"} and name != "asyncio.sleep":
                boundary.signals.add("blocking sleep")
            if name in {
                "httpx.Client",
                "httpx.request",
                "urllib.request.urlopen",
            } or name.startswith("requests."):
                boundary.signals.add("sync HTTP")
            if name == "sqlite3.connect":
                boundary.signals.add("SQLite")
            if method in FILESYSTEM_METHODS:
                boundary.signals.add("filesystem")
            if root_receiver in STORE_RECEIVERS and method not in {"model_dump"}:
                boundary.signals.add("store/lock")
            if name in {"json.dumps", "json.loads"} or method in {"model_dump", "model_dump_json"}:
                boundary.signals.add("serialization")
            if name.startswith("httpx.AsyncClient") or (
                method in {"get", "post", "request", "send"} and "client" in receiver
            ):
                boundary.signals.add("async HTTP")


def inventory() -> list[Boundary]:
    rows: list[Boundary] = []
    for path in sorted(SOURCE.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        visitor = FileVisitor(path)
        visitor.visit(tree)
        rows.extend(visitor.boundaries)
    return sorted(rows, key=lambda row: (row.path, row.line, row.qualname))


def decision(row: Boundary) -> str:
    if row.context.startswith("sync "):
        return "Framework worker thread; no event-loop execution"
    if row.signals & {
        "sync subprocess",
        "sync HTTP",
        "blocking sleep",
        "SQLite",
        "filesystem",
        "store/lock",
    }:
        if "bounded/thread offload" in row.boundaries:
            return "Reviewed: legacy work crosses bounded off-loop boundary"
        return "Reviewed transitively; see audit matrix/allowlist"
    if "async HTTP" in row.signals or "async subprocess" in row.boundaries:
        return "Native async; operation deadline/cancellation required"
    return "In-memory/async orchestration; transitive callees reviewed"


def emit(rows: list[Boundary]) -> str:
    lines = [
        "<!-- Generated by scripts/audit_async_boundaries.py; do not hand-edit. -->",
        "",
        "# Generated async boundary inventory",
        "",
        "Every coroutine plus every HTTP or MCP entry point found under `src/pa` "
        "is listed below. Direct signals are conservative syntax matches; the "
        "reviewed transitive decisions and latency/cancellation analysis are in "
        "`ASYNC_BLOCKING_AUDIT.md`.",
        "",
        f"Inventory rows: **{len(rows)}**.",
        "",
        "| Call path | Execution context | Direct risk signals | Boundary decision |",
        "|---|---|---|---|",
    ]
    for row in rows:
        call_path = f"`{row.path}:{row.line} {row.qualname}`"
        signals = ", ".join(sorted(row.signals)) or "none"
        lines.append(
            f"| {call_path} | {row.context} | {signals} | {decision(row)} |"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", action="store_true")
    args = parser.parse_args()
    rows = inventory()
    print(len(rows) if args.count else emit(rows), end="" if args.count else "")


if __name__ == "__main__":
    main()
