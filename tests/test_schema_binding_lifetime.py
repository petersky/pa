"""Temporary discovery bindings must release their owned recursive namespace."""
import weakref
from unittest.mock import patch

import pytest

from fastapi._compat import v2
from fastapi.openapi.utils import get_openapi as framework_get_openapi
from pa.server import schema_generation as sg
from tests.test_schema_generation import variant_app

DISCOVERY = {"get_flat_models_from_model", "get_flat_models_from_annotation",
             "get_flat_models_from_field", "get_flat_models_from_fields"}


def tracker():
    original = sg._bind
    refs, namespaces = [], []

    def bind(function, namespace):
        result = original(function, namespace)
        if function.__name__ in DISCOVERY:
            refs.append(weakref.ref(result))
            if not any(namespace is previous for previous in namespaces):
                namespaces.append(namespace)
        return result

    return bind, refs, namespaces


def test_complete_schema_releases_temporary_discovery_functions_without_collection():
    app = variant_app()
    kwargs = dict(title="lifetime", version="1", routes=app.routes,
                  webhooks=app.webhooks.routes)
    expected = framework_get_openapi(**kwargs)
    bind, refs, namespaces = tracker()
    with patch.object(sg, "_bind", bind):
        actual = sg.get_openapi(**kwargs)
    assert actual == expected
    assert len(refs) == 4 and len(namespaces) == 1
    # Holding the owned dict here must not retain its former cloned functions.
    # No explicit collection or GC policy change is needed for reference release.
    assert all(name not in namespaces[0] for name in DISCOVERY)
    assert all(reference() is None for reference in refs)


def test_error_releases_only_owned_bindings_and_preserves_exception():
    app = variant_app()
    bind, refs, namespaces = tracker()
    error = ValueError("private schema failure")
    originals = {name: getattr(v2, name) for name in DISCOVERY}

    def failed(**kwargs):
        raise error

    with patch.object(sg, "_bind", bind), patch.object(sg, "framework_get_openapi", failed):
        with pytest.raises(ValueError) as caught:
            sg.get_openapi(title="failure", version="1", routes=app.routes)
    assert caught.value is error
    assert len(refs) == 4 and len(namespaces) == 1
    assert all(name not in namespaces[0] for name in DISCOVERY)
    assert {name: getattr(v2, name) for name in DISCOVERY} == originals
