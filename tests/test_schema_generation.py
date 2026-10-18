from __future__ import annotations

import tempfile
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Annotated, Generic, Literal, TypeVar
from unittest import TestCase
from unittest.mock import patch

from fastapi import Body, FastAPI
from fastapi._compat import v2
from fastapi.openapi.utils import get_openapi as framework_get_openapi
from fastapi.routing import APIRouter
from pydantic import BaseModel, Field, RootModel, computed_field
from pydantic import json_schema

from pa.config import Settings
from pa.core.kernel import Kernel
from pa.server import schema_generation


class Color(str, Enum):
    red = "red"
    blue = "blue"


class Node(BaseModel):
    label: str = Field(alias="title", min_length=1)
    children: list[Node] = []
    color: Color = Color.red

    @computed_field
    @property
    def child_count(self) -> int:
        return len(self.children)


T = TypeVar("T")


class Envelope(BaseModel, Generic[T]):
    value: T


@dataclass
class Point:
    x: int
    y: int


class Cat(BaseModel):
    kind: Literal["cat"]
    lives: int = Field(ge=1, le=9)


class Dog(BaseModel):
    kind: Literal["dog"]
    name: str


class Payload(BaseModel):
    tree: Envelope[Node]
    point: Point
    pet: Annotated[Cat | Dog, Field(discriminator="kind")]
    tags: RootModel[dict[str, list[Color]]]


def variant_app() -> FastAPI:
    app = FastAPI()
    callbacks = APIRouter()

    @callbacks.post("/callback", response_model=Envelope[Node])
    def callback(payload: Payload):
        return payload.tree

    @app.post("/tree", response_model=Envelope[Node], callbacks=callbacks.routes)
    def tree(payload: Annotated[Payload, Body()]):
        return payload.tree

    @app.webhooks.post("/hook", response_model=Node)
    def hook(payload: Payload):
        return payload.tree.value

    return app


class SchemaGenerationTests(TestCase):
    def test_definition_collisions_and_recursive_remapping_match_framework(self):
        definitions = {
            "left": {"type": "object", "properties": {"child": {"$ref": "#/$defs/inner-left"}}},
            "right": {"type": "object", "properties": {"child": {"$ref": "#/$defs/inner-right"}}},
            "inner-left": {"type": "integer"},
            "inner-right": {"type": "integer"},
            "different": {"type": "string"},
        }
        choices = {
            "left": ["Outer", "left"], "right": ["Outer", "right"],
            "inner-left": ["Inner", "inner-left"],
            "inner-right": ["Inner", "inner-right"],
            "different": ["Outer", "different"],
        }
        results = []
        for cls in (json_schema.GenerateJsonSchema,
                    schema_generation._schema_generator(json_schema.GenerateJsonSchema)):
            generator = cls()
            generator.definitions = deepcopy(definitions)
            generator._prioritized_defsref_choices = deepcopy(choices)
            remapping = generator._build_definitions_remapping()
            results.append((remapping.defs_remapping, remapping.json_remapping,
                            remapping.remap_json_schema({"$defs": deepcopy(definitions)})))
            self.assertEqual(generator.definitions, definitions)
        self.assertEqual(results[0], results[1])

    def test_definition_memo_is_invalidated_before_in_place_remapping(self):
        # Observe real remapping boundaries: upstream mutates these same dicts.
        # A hash from a previous pass must never survive into the next pass.
        original_hash = json_schema._make_json_hashable
        original_remapping = json_schema._DefinitionsRemapping
        passes, seen = [], set()

        def fingerprint(value):
            if id(value) in seen:
                self.fail("same definition traversed twice within a remapping pass")
            seen.add(id(value))
            with patch.object(json_schema, "_make_json_hashable", original_hash):
                return original_hash(value)

        def boundary(*args):
            passes.append(len(seen))
            seen.clear()
            return original_remapping(*args)

        # Keep the original static method available while observing construction.
        boundary.from_prioritized_choices = original_remapping.from_prioritized_choices
        with patch.object(json_schema, "_make_json_hashable", fingerprint), \
             patch.object(json_schema, "_DefinitionsRemapping", boundary):
            generator = schema_generation._schema_generator(json_schema.GenerateJsonSchema)()
            generator.definitions = {"a": {"type": "integer"}, "b": {"type": "integer"}}
            generator._prioritized_defsref_choices = {"a": ["Shared", "a"], "b": ["Shared", "b"]}
            result = generator._build_definitions_remapping()
        self.assertEqual(result.defs_remapping, {"a": "Shared", "b": "Shared"})
        # Upstream retains both original entries in copied_definitions even
        # after their output names collapse. Each pass must hash both again.
        self.assertEqual(passes, [2, 2])

    def test_remapping_error_and_unrecognized_pydantic_version_use_framework_contract(self):
        base = json_schema.GenerateJsonSchema
        for cls in (base, schema_generation._schema_generator(base)):
            generator = cls()
            generator.definitions = {"a": {"enum": [{1}]}}
            generator._prioritized_defsref_choices = {"a": ["a"]}
            with self.assertRaises(TypeError):
                generator._build_definitions_remapping()
        with patch.object(schema_generation.pydantic, "__version__", "future"):
            self.assertIs(schema_generation._schema_generator(base), base)

    def test_variants_match_framework_in_both_schema_modes(self):
        app = variant_app()
        for separate in (True, False):
            with self.subTest(separate=separate):
                kwargs = dict(title="variants", version="1", routes=app.routes,
                              webhooks=app.webhooks.routes,
                              separate_input_output_schemas=separate)
                self.assertEqual(schema_generation.get_openapi(**kwargs),
                                 framework_get_openapi(**kwargs))

    def test_full_pa_schema_security_and_all_routes_match(self):
        import pa.openapi

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            app = Kernel.boot(settings=Settings(
                data_dir=root / "data", workspace_root=root / "workspaces",
                agent_enabled=False, telemetry_enabled=False,
            )).build_app()
            optimized = app.openapi()
            app.openapi_schema = None
            with patch.object(pa.openapi, "get_openapi", framework_get_openapi):
                original = app.openapi()
            self.assertEqual(optimized, original)
            self.assertGreater(len(original["paths"]), 400)

    def test_framework_bindings_unchanged_even_for_concurrent_calls(self):
        app = variant_app()
        names = ("get_model_fields", "get_flat_models_from_model",
                 "get_flat_models_from_fields", "get_definitions")
        bindings = {name: getattr(v2, name) for name in names}
        kwargs = dict(title="concurrent", version="1", routes=app.routes)
        with ThreadPoolExecutor(max_workers=2) as pool:
            candidate = pool.submit(schema_generation.get_openapi, **kwargs)
            original = pool.submit(framework_get_openapi, **kwargs)
            self.assertEqual(candidate.result(), original.result())
        self.assertEqual(bindings, {name: getattr(v2, name) for name in names})

    def test_json_schema_extensions_keep_framework_filtering_and_json_types(self):
        app = FastAPI()

        @app.get("/extensions", openapi_extra={"x-extra": {
            "_sa_hidden": 1, "null": None,
            "array": [None, {"nested_null": None, "keep": Color.red}],
            "when": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "nested": {"_sa_hidden": {"keep": True}, "visible": {
                3: (None, False, 0, "", Decimal("1.25")),
                "path": Path("example/schema"), "bytes": b"hello",
            }},
        }})
        def extensions():
            return {}

        kwargs = dict(title="extensions", version="1", routes=app.routes)
        self.assertEqual(schema_generation.get_openapi(**kwargs),
                         framework_get_openapi(**kwargs))

    def test_local_encoder_keeps_general_object_and_option_semantics(self):
        from fastapi.encoders import jsonable_encoder
        from fastapi.openapi.models import OpenAPI

        document = OpenAPI(openapi="3.1.0", info={"title": "test", "version": "1"},
                           paths={}, **{"x-values": {"missing": None}})
        for value, options in ((document, {"by_alias": True, "exclude_none": True}),
                               (document, {"exclude_none": False}),
                               ({"_sa_private": 1, "value": Decimal("2.5")}, {})):
            with self.subTest(options=options, kind=type(value).__name__):
                self.assertEqual(schema_generation._encode_openapi(value, **options),
                                 jsonable_encoder(value, **options))

    def test_unknown_framework_version_uses_public_generator(self):
        with patch.object(schema_generation.fastapi, "__version__", "future"), \
             patch.object(schema_generation, "framework_get_openapi", return_value={}) as public:
            self.assertEqual(schema_generation.get_openapi(title="fallback"), {})
        public.assert_called_once_with(title="fallback")

    def test_app_caches_one_complete_schema_under_concurrent_generation(self):
        import pa.openapi

        app = variant_app()
        pa.openapi.install_openapi_contract(app)
        with patch.object(pa.openapi, "get_openapi", wraps=schema_generation.get_openapi) as generate:
            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(app.openapi)
                second = pool.submit(app.openapi)
                self.assertIs(first.result(), second.result())
        self.assertEqual(generate.call_count, 1)

    def test_discovery_removes_redundant_adapters_but_keeps_real_schema_adapters(self):
        app = variant_app()
        kwargs = dict(title="adapters", version="1", routes=app.routes)
        counts = {}
        original = v2.ModelField.__post_init__
        for name, generate in (("framework", framework_get_openapi),
                               ("candidate", schema_generation.get_openapi)):
            counts[name] = 0

            def count(field):
                counts[name] += 1
                return original(field)

            with patch.object(v2.ModelField, "__post_init__", count):
                generate(**kwargs)
        self.assertGreater(counts["candidate"], 0)
        self.assertLess(counts["candidate"], counts["framework"])
