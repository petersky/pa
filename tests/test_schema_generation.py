from __future__ import annotations

import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
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
        }})
        def extensions():
            return {}

        kwargs = dict(title="extensions", version="1", routes=app.routes)
        self.assertEqual(schema_generation.get_openapi(**kwargs),
                         framework_get_openapi(**kwargs))

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
