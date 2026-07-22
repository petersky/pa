"""Isolated entry point for cancellable legacy provider actions.

The server starts this module with PA's process-group-aware async subprocess
runner. Provider implementations remain synchronous for CLI compatibility, but
an HTTP/MCP cancellation can terminate this process and every installer/probe
child it starts.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pa.acp.providers.codex import install_codex_cli
from pa.acp.providers.registry import get_provider

RESULT_MARKER = "PA_PROVIDER_RESULT="


def run(provider_id: str, action: str, data_dir: Path) -> dict:
    if action == "codex-cli-install":
        result = install_codex_cli()
    elif action in {"install", "update", "probe"}:
        result = getattr(get_provider(provider_id), action)(data_dir)
    else:
        raise ValueError(f"Unsupported provider action: {action}")
    return result if isinstance(result, dict) else result.model_dump(mode="json")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("provider_id")
    parser.add_argument("action")
    parser.add_argument("data_dir", type=Path)
    args = parser.parse_args()
    try:
        result = run(args.provider_id, args.action, args.data_dir)
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(RESULT_MARKER + json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

