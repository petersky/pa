from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from pa.domain.models import (
    CardCreate,
    CardUpdate,
    ItemCreate,
    ItemUpdate,
    RepositoryCreate,
    RepositoryUpdate,
)
from pa.execution.profiles import ExecutionContract
from pa.modules.fleet import FleetModule
from pa.modules.items import ItemsModule
from pa.modules.projects import ProjectsModule
from pa.modules.sync import SyncModule
from pa.workloads import CANONICAL_WORKLOAD_PROFILES


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
        self.mcp = MCPServer("schema-contract")
        ctx = SimpleNamespace(settings=SimpleNamespace())
        ItemsModule().register_mcp(self.mcp, ctx)
        FleetModule().register_mcp(self.mcp, ctx)
        ProjectsModule().register_mcp(self.mcp, ctx)
        SyncModule().register_mcp(self.mcp, ctx)
        self.schemas = {
            tool.name: tool.input_schema for tool in await self.mcp.list_tools()
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

    async def test_card_tools_publish_the_complete_canonical_mutation_schema(
        self,
    ) -> None:
        create = self.schemas["create_card"]["properties"]
        update = self.schemas["update_card"]["properties"]
        self.assertTrue(
            {
                "title",
                "kind",
                "body",
                "lane",
                "realm",
                "parent_id",
                "project_id",
                "tags",
                "auto_enrich",
                "idempotency_key",
            }.issubset(create)
        )
        self.assertTrue(
            {
                "card_id",
                "title",
                "body",
                "lane",
                "realm",
                "parent_id",
                "project_id",
                "tags",
                "expected_version",
                "field_intent",
                "idempotency_key",
            }.issubset(update)
        )

    async def test_repository_mutations_publish_http_lifecycle_enums(self) -> None:
        for tool_name, model in [
            ("create_repository", RepositoryCreate),
            ("update_repository", RepositoryUpdate),
        ]:
            for field in ("visibility", "status"):
                with self.subTest(tool=tool_name, field=field):
                    self.assert_enum_matches_model(tool_name, field, model, field)

    async def test_dispatch_mutations_require_typed_idempotency_and_authority(
        self,
    ) -> None:
        dispatch = self.schemas["dispatch_card_to_instance"]
        self.assertIn("idempotency_key", dispatch["required"])
        self.assertIn("authority_instance_id", dispatch["properties"])
        self.assertIn("resume_session_id", dispatch["properties"])
        self.assertIn(
            "resume_session_id", self.schemas["dispatch_card"]["properties"]
        )

        followup = self.schemas["prompt_dispatch_session"]
        self.assertTrue(
            {"dispatch_id", "message", "idempotency_key"}.issubset(followup["required"])
        )
        self.assertEqual(
            followup["properties"]["action"]["enum"],
            ["append", "prepend", "interrupt"],
        )

    async def test_card_and_sync_mutations_require_recoverable_operation_keys(
        self,
    ) -> None:
        for tool_name in (
            "create_card",
            "update_card",
            "sync_reconcile",
            "resolve_sync_conflicts",
        ):
            self.assertIn(
                "idempotency_key", self.schemas[tool_name]["required"]
            )
        self.assertIn(
            "idempotency_key",
            self.schemas["get_operation_outcome"]["required"],
        )

    async def test_fleet_tools_publish_the_canonical_workload_profile_enum(
        self,
    ) -> None:
        expected = list(CANONICAL_WORKLOAD_PROFILES)
        self.assertEqual(
            _property_enum(self.schemas["preview_fleet_placement"], "workload_profile"),
            expected,
        )
        self.assertEqual(
            _property_enum(self.schemas["preview_instance_group"], "workload_profile"),
            expected,
        )
        self.assertEqual(
            _property_enum(ExecutionContract.model_json_schema(), "profile"),
            expected,
        )


    async def test_invalid_enum_value_fails_before_tool_handler_runs(self) -> None:
        with self.assertRaises(ToolError) as raised:
            await self.mcp.call_tool(
                "create_card",
                {
                    "title": "invalid",
                    "lane": "not-a-lane",
                    "idempotency_key": "invalid-enum-test",
                },
            )

        self.assertIn(
            "1 validation error for create_cardArguments", str(raised.exception)
        )
        self.assertIn(
            "Input should be 'inbox', 'active', 'waiting' or 'done'",
            str(raised.exception),
        )
