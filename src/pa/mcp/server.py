from pa import __version__
from pa.core.kernel import Kernel

mcp = None


def _get_mcp():
    global mcp
    if mcp is None:
        from mcp.server.mcpserver import MCPServer

        mcp = MCPServer("pa", version=__version__)
        kernel = Kernel.boot()
        kernel.register_mcp(mcp)
    return mcp


def run_stdio() -> None:
    _get_mcp().run(transport="stdio")
