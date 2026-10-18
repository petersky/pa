"""Filtering owns only its serialized dump; live OpenAPI models stay unchanged."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

import pytest
from fastapi.encoders import jsonable_encoder
from fastapi.openapi.models import OpenAPI

from pa.server.schema_generation import _encode_openapi


@pytest.mark.parametrize('extra', [
    {'keep': 1, 'drop': None, '_sa_private': 'private'},
    [None, {'keep': 2, 'drop': None, '_sa_private': 3}],
    {'nested': [{'nested': {'keep': [None, False, 0, ''], '_sa_hidden': 5}}]},
    {'_sample': 'also excluded by upstream', 'safe': {'_safe': 4}},
    {'unicode': 'é雪😀', 'array': [None, {'_sa_hidden': None}, {}]},
])
def test_complete_encoding_matches_framework_without_mutating_model(extra):
    schema = OpenAPI(openapi='3.1.0', info={'title': 'Fixture', 'version': '1'}, paths={}, **{'x-owned': extra})
    original = deepcopy(schema.model_dump(mode='python'))
    expected = jsonable_encoder(schema, by_alias=True, exclude_none=True)
    result = _encode_openapi(schema, by_alias=True, exclude_none=True)
    assert result == expected
    assert schema.model_dump(mode='python') == original
    # Mutating a returned tree must not change the original model or next call.
    result['x-owned'] = {'replacement': True}
    assert _encode_openapi(schema, by_alias=True, exclude_none=True) == expected
    assert schema.model_dump(mode='python') == original


def test_concurrent_encodings_of_one_live_model_remain_independent():
    schema = OpenAPI(openapi='3.1.0', info={'title': 'Fixture', 'version': '1'}, paths={},
                     **{'x-owned': {'entries': [{'value': n, 'drop': None, '_sa_hidden': n} for n in range(100)]}})
    original = deepcopy(schema.model_dump(mode='python'))
    expected = jsonable_encoder(schema, by_alias=True, exclude_none=True)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: _encode_openapi(schema, by_alias=True, exclude_none=True), range(16)))
    assert all(result == expected for result in results)
    results[0]['x-owned']['entries'][0]['value'] = 'changed'
    assert all(result == expected for result in results[1:])
    assert schema.model_dump(mode='python') == original
