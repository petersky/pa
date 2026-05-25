"""Example external PA plugin (for development/reference only).

Install in editable mode and register via pyproject.toml::

    [project.entry-points."pa.modules"]
    example = "pa_example_plugin:ExampleModule"
"""

from __future__ import annotations

from pa.core.contracts import Module
from pa.core.context import AppContext


class ExampleModule(Module):
    @property
    def name(self) -> str:
        return "example"

    @property
    def version(self) -> str:
        return "0.0.1"

    @property
    def description(self) -> str:
        return "Reference plugin showing the PA module contract"

    def on_load(self, ctx: AppContext) -> None:
        ctx.hooks.on("app.startup", self._on_startup)

    async def _on_startup(self, **_: object) -> None:
        import logging

        logging.getLogger("pa.example").info("Example plugin loaded")
