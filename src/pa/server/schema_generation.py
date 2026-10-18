"""Avoid redundant discovery work during complete OpenAPI generation.

FastAPI 0.136.3 traverses model fields three times while generating OpenAPI.
Its traversal only consumes ``field_info.annotation``, but constructs a fresh
ModelField (and Pydantic TypeAdapter) for every field on every visit. Actual
validation/serialization schema generation still needs those adapters.
The final validated OpenAPI model dump is already JSON-compatible; retain its
null/private-key filtering without rediscovering arbitrary Python encoders for
every JSON scalar.

Keep the framework's traversal and rendering, with private function bindings
local to this call. Nothing in FastAPI's module globals, request validation,
other apps, or concurrent schema generation is patched. A different framework
version uses the unmodified public generator until separately verified.
"""

from __future__ import annotations

from types import FunctionType, SimpleNamespace
from typing import Any

import fastapi
from fastapi.encoders import jsonable_encoder
from fastapi.openapi.models import OpenAPI
from fastapi.openapi.utils import get_openapi as framework_get_openapi


def _filter_schema_json(value: Any) -> Any:
    """Apply the encoder's filters to an already JSON-serialized model dump."""
    if type(value) is dict:
        return {
            key: _filter_schema_json(item) for key, item in value.items()
            if item is not None and not key.startswith("_sa")
        }
    if type(value) is list:
        # None elements in arrays survive exclude_none, just as upstream.
        return [_filter_schema_json(item) for item in value]
    return value


def _encode_openapi(value: Any, **options: Any) -> Any:
    # This is the exact final encoder call in the version-fenced generator.
    # Retain full OpenAPI validation and Pydantic's JSON serialization. Only
    # the subsequent traversal can avoid arbitrary-object encoder discovery.
    if type(value) is OpenAPI and options == {"by_alias": True, "exclude_none": True}:
        return _filter_schema_json(value.model_dump(
            mode="json", by_alias=True, exclude_none=True,
        ))
    return jsonable_encoder(value, **options)


def _bind(function: FunctionType, namespace: dict[str, Any]) -> FunctionType:
    bound = FunctionType(
        function.__code__, namespace, function.__name__,
        function.__defaults__, function.__closure__,
    )
    bound.__kwdefaults__ = function.__kwdefaults__
    return bound


def get_openapi(**kwargs: Any) -> dict[str, Any]:
    if fastapi.__version__ != "0.136.3":
        return framework_get_openapi(**kwargs)

    from fastapi._compat import v2

    # These metadata views never escape model discovery. The real ModelFields
    # and their adapters remain in get_definitions and get_openapi_path.
    def model_fields(model: Any) -> list[SimpleNamespace]:
        return [SimpleNamespace(field_info=info) for info in model.model_fields.values()]

    traversal = dict(v2.get_flat_models_from_fields.__globals__)
    traversal["get_model_fields"] = model_fields
    for name in (
        "get_flat_models_from_model", "get_flat_models_from_annotation",
        "get_flat_models_from_field", "get_flat_models_from_fields",
    ):
        traversal[name] = _bind(getattr(v2, name), traversal)

    def definitions(*, fields: Any, **options: Any) -> Any:
        input_types = {field.field_info.annotation for field in fields}

        def model_field(**values: Any) -> Any:
            # Upstream constructs these adapters then immediately discards
            # fields whose model is already in the inputs. Keep only their
            # metadata until that unchanged filter has discarded them.
            if values["field_info"].annotation in input_types:
                return SimpleNamespace(**values)
            return v2.ModelField(**values)

        namespace = {**traversal, "ModelField": model_field}
        return _bind(v2.get_definitions, namespace)(fields=fields, **options)

    generation = dict(framework_get_openapi.__globals__)
    generation["get_flat_models_from_fields"] = traversal["get_flat_models_from_fields"]
    generation["get_definitions"] = definitions
    generation["jsonable_encoder"] = _encode_openapi
    return _bind(framework_get_openapi, generation)(**kwargs)
