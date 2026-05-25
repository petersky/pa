from pa.core.kernel import Kernel

mcp = None


def _get_mcp():
    global mcp
    if mcp is None:
        from mcp.server.fastmcp import FastMCP

        mcp = FastMCP("pa")
        kernel = Kernel.boot()
        kernel.register_mcp(mcp)
    return mcp


def run_stdio() -> None:
    _get_mcp().run(transport="stdio")
