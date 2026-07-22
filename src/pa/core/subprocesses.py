"""Cancellation-safe bounded asyncio subprocess execution."""

from __future__ import annotations

import asyncio
import os
import signal
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence


class ProcessOutputLimitExceeded(RuntimeError):
    pass


@dataclass(frozen=True)
class ProcessResult:
    args: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str


async def _terminate_group(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGTERM)
        else:  # pragma: no cover - Windows fallback
            process.terminate()
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(process.wait(), timeout=2.0)
        return
    except asyncio.TimeoutError:
        pass
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGKILL)
        else:  # pragma: no cover - Windows fallback
            process.kill()
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(process.wait(), timeout=2.0)
    except asyncio.TimeoutError:
        # Preserve the caller's timeout/cancellation/output-limit exception.
        # The process has already received SIGKILL and will be reaped by the
        # event-loop child watcher when its exit is delivered.
        return


async def run_process(
    args: Sequence[str | os.PathLike[str]],
    *,
    cwd: str | Path | None = None,
    env: Mapping[str, str] | None = None,
    timeout: float = 30.0,
    output_limit: int = 4 * 1024 * 1024,
) -> ProcessResult:
    """Run one process with deadlines, group cleanup, and bounded output."""

    argv = tuple(os.fspath(value) for value in args)
    process = await asyncio.create_subprocess_exec(
        *argv,
        cwd=os.fspath(cwd) if cwd is not None else None,
        env=dict(env) if env is not None else None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=os.name == "posix",
    )
    total = 0

    async def read_limited(stream: asyncio.StreamReader | None) -> bytes:
        nonlocal total
        chunks: list[bytes] = []
        if stream is None:
            return b""
        while True:
            chunk = await stream.read(64 * 1024)
            if not chunk:
                return b"".join(chunks)
            total += len(chunk)
            if total > output_limit:
                raise ProcessOutputLimitExceeded(
                    f"process output exceeded {output_limit} bytes"
                )
            chunks.append(chunk)

    try:
        async with asyncio.timeout(timeout):
            stdout, stderr, _ = await asyncio.gather(
                read_limited(process.stdout),
                read_limited(process.stderr),
                process.wait(),
            )
    except BaseException:
        await asyncio.shield(_terminate_group(process))
        raise
    return ProcessResult(
        args=argv,
        returncode=int(process.returncode or 0),
        stdout=stdout.decode(errors="replace"),
        stderr=stderr.decode(errors="replace"),
    )
