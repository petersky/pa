from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError

from pa.domain.models import (
    CardCreate,
    CardUpdate,
    ItemCreate,
    ItemUpdate,
    RepositoryCreate,
    RepositoryUpdate,
)
from pa.modules.items import ItemsModule
from pa.modules.projects import ProjectsModule


def _property_enum(schema: dict, property_name: str) -> list[str]:
    prop = schema["properties"][property_name]
    if "$ref" in prop:
        prop = schema["$defs"][prop["$ref"].rsplit("/", 1)[-1]]
    if "anyOf" in prop:
        prop = next(part for part in prop["anyOf"] if "$ref" in part)
        prop = schema["$defs"][prop["$ref"].rsplit("/", 1)[-1]]
    return prop["enum"]


class McpDomainSchemaTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.mcp = FastMCP("schema-contract")
        ctx = SimpleNamespace(settings=SimpleNamespace())
        ItemsModule().register_mcp(self.mcp, ctx)
        ProjectsModule().register_mcp(self.mcp, ctx)
        self.schemas = {
            tool.name: tool.inputSchema for tool in await self.mcp.list_tools()
        }

    def assert_enum_matches_model(
        self,
        tool_name: str,
        tool_property: str,
        model: type,
        model_property: str,
    ) -> None:
        self.assertEqual(
            _property_enum(self.schemas[tool_name], tool_property),
            _property_enum(model.model_json_schema(), model_property),
        )

    async def test_item_and_card_tools_publish_http_domain_enums(self) -> None:
        for tool_name, tool_property, model, model_property in [
            ("create_item", "kind", ItemCreate, "kind"),
            ("create_item", "status", ItemCreate, "status"),
            ("update_item", "status", ItemUpdate, "status"),
            ("create_card", "kind", CardCreate, "kind"),
            ("create_card", "lane", CardCreate, "lane"),
            ("update_card", "lane", CardUpdate, "lane"),
        ]:
            with self.subTest(tool=tool_name, field=tool_property):
                self.assert_enum_matches_model(
                    tool_name, tool_property, model, model_property
                )

    async def test_repository_mutations_publish_http_lifecycle_enums(self) -> None:
        for tool_name, model in [
            ("create_repository", RepositoryCreate),
            ("update_repository", RepositoryUpdate),
        ]:
            for field in ("visibility", "status"):
                with self.subTest(tool=tool_name, field=field):
                    self.assert_enum_matches_model(tool_name, field, model, field)

    async def test_invalid_enum_value_fails_before_tool_handler_runs(self) -> None:
        with self.assertRaises(ToolError) as raised:
            await self.mcp.call_tool(
                "create_card",
                {"title": "invalid", "lane": "not-a-lane"},
            )

        self.assertIn(
            "1 validation error for create_cardArguments", str(raised.exception)
        )
        self.assertIn(
            "Input should be 'inbox', 'active', 'waiting' or 'done'",
            str(raised.exception),
        )
