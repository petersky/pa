"""Release temporary native generators without invalidating escaped handlers."""
import weakref
from unittest.mock import patch

import pytest
from pydantic import BaseModel, json_schema
from pydantic_core import core_schema
from fastapi.openapi.utils import get_openapi as framework_get_openapi
from fastapi._compat.v2 import GenerateJsonSchema as FastAPISchemaGenerator

from pa.server import schema_generation as sg
from tests.test_schema_generation import variant_app


def test_complete_schema_releases_native_generator_without_collection():
    app = variant_app()
    kwargs = dict(title="generator lifetime", version="1", routes=app.routes,
                  webhooks=app.webhooks.routes)
    expected = framework_get_openapi(**kwargs)
    original = json_schema.GenerateJsonSchema.__init__
    refs = []

    def record(self, *args, **kwargs):
        original(self, *args, **kwargs)
        refs.append(weakref.ref(self))

    with patch.object(json_schema.GenerateJsonSchema, "__init__", record):
        actual = sg.get_openapi(**kwargs)
    assert actual == expected
    assert len(refs) == 1
    assert refs[0]() is None


def test_escaped_native_method_keeps_generator_until_handler_is_released():
    generator = sg._schema_generator(json_schema.GenerateJsonSchema)()
    owner = weakref.ref(generator)
    handler = generator._schema_type_to_method["int"]
    assert handler.__self__ is generator
    del generator
    assert owner() is not None
    assert handler(core_schema.int_schema()) == {"type": "integer"}
    del handler
    assert owner() is None


def test_public_custom_json_schema_handler_remains_callable_after_generation():
    escaped = []

    class Custom(BaseModel):
        value: int

        @classmethod
        def __get_pydantic_json_schema__(cls, schema, handler):
            result = handler(schema)
            escaped.append(handler)
            return result

    for factory in (lambda base: base, sg._schema_generator):
        escaped.clear()
        generator = factory(json_schema.GenerateJsonSchema)()
        owner = weakref.ref(generator)
        result = generator.generate(Custom.__pydantic_core_schema__)
        del generator
        assert result["properties"]["value"] == {"title": "Value", "type": "integer"}
        assert owner() is not None and escaped
        assert escaped[-1](core_schema.int_schema()) == {"type": "integer"}
    escaped.clear()


@pytest.mark.parametrize("native", [json_schema.GenerateJsonSchema, FastAPISchemaGenerator])
@pytest.mark.parametrize("derived", [False, True])
def test_custom_generator_subclasses_keep_native_mapping_and_overrides(derived, native):
    class Custom(native):
        def int_schema(self, schema):
            return {**super().int_schema(schema), "x-custom": True}

    if derived:
        class Selected(sg._schema_generator(native)):
            def int_schema(self, schema):
                return {**super().int_schema(schema), "x-custom": True}
    else:
        Selected = sg._schema_generator(Custom)
    generator = Selected()
    assert type(generator._schema_type_to_method) is dict
    assert generator.generate(core_schema.int_schema()) == {"type": "integer", "x-custom": True}


def test_native_mapping_mutation_views_and_errors_keep_behavior():
    for factory in (lambda base: base, sg._schema_generator):
        generator = factory(json_schema.GenerateJsonSchema)()
        mapping = generator._schema_type_to_method
        assert dict(mapping)["int"].__self__ is generator
        original = mapping.pop("int")
        with pytest.raises(KeyError):
            mapping["int"]
        mapping["int"] = original
        mapping["fixture"] = lambda schema: {"type": "integer"}
        assert mapping.get("missing", "fallback") == "fallback"
        assert dict(mapping.items())["int"].__self__ is generator
        assert mapping.pop("fixture")(None) == {"type": "integer"}
        assert generator.generate(core_schema.int_schema()) == {"type": "integer"}


def test_unknown_library_versions_keep_native_generator_behavior():
    with patch.object(sg.pydantic, "__version__", "future"):
        for native in (json_schema.GenerateJsonSchema, FastAPISchemaGenerator):
            assert sg._schema_generator(native) is native
    with patch.object(sg.fastapi, "__version__", "future"):
        selected = sg._schema_generator(FastAPISchemaGenerator)()
        assert type(selected._schema_type_to_method) is dict
        assert selected.generate(core_schema.bytes_schema()) == FastAPISchemaGenerator().generate(core_schema.bytes_schema())
