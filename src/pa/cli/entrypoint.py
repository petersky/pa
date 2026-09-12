"""Route the stdio bridge without importing the service CLI."""

import sys


def main() -> None:
    if sys.argv[1:] == ["mcp"]:
        from pa.mcp.server import run_stdio

        run_stdio()
    else:
        from pa.cli.main import app

        app()
