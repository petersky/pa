"""Keep full schema/default semantics while avoiding identity scalar adapters."""
from datetime import datetime,timedelta,timezone
from decimal import Decimal
from unittest.mock import patch
import warnings
import pytest
from pydantic import BaseModel,ConfigDict,Field,field_serializer,json_schema
from pydantic.type_adapter import TypeAdapter
from fastapi.openapi.utils import get_openapi as public_get_openapi
from pa.server import schema_generation as sg
candidate_factory = sg._schema_generator

class Encoded(BaseModel):
    model_config=ConfigDict(json_encoders={int:lambda v:'encoded-'+str(v),str:lambda v:'text-'+v})
    integer:int=7
    text:str='x'

class Plain(BaseModel):
    model_config=ConfigDict(serialize_by_alias=True,str_to_upper=True,ser_json_bytes='base64')
    integer:int=Field(7,alias='count')
    text:str='x'
    boolean:bool=True
    optional:int|None=None
    fraction:float=1.5
    decimal:Decimal=Decimal('2.3')
    instant:datetime=datetime(2026,1,1,tzinfo=timezone.utc)
    interval:timedelta=timedelta(seconds=2)
    data:bytes=b'abc'
    encoded:Encoded=Encoded()
    @field_serializer('integer')
    def serialize_integer(self,v):return 'field-'+str(v)

class Outer(BaseModel):
    model_config=ConfigDict(json_encoders={int:lambda v:'outer-'+str(v)})
    nested:Plain=Plain()
    local:int=9
    plain_again:Plain

@pytest.mark.parametrize('model',[Encoded,Plain,Outer])
@pytest.mark.parametrize('mode',['validation','serialization'])
@pytest.mark.parametrize('alias',[False,True])
def test_nested_configs_custom_encoders_and_field_serializers(model,mode,alias):
    results=[];warning_sets=[]
    for generate in (json_schema.GenerateJsonSchema,candidate_factory(json_schema.GenerateJsonSchema)):
        with warnings.catch_warnings(record=True) as observed:
            warnings.simplefilter('always')
            results.append(model.model_json_schema(mode=mode,by_alias=alias,schema_generator=generate))
        warning_sets.append([(x.category.__name__,str(x.message)) for x in observed])
    assert results[0]==results[1]
    assert warning_sets[0]==warning_sets[1]
    if model is Encoded:
        assert results[1]['properties']['integer']['default']==('text-encoded-7' if mode=='serialization' else 'encoded-7')

@pytest.mark.parametrize('separate',[False,True])
def test_complete_fastapi_variants_match_framework_and_current_generator(separate):
    from tests.test_schema_generation import variant_app
    app=variant_app();kwargs=dict(title='parity',version='1',routes=app.routes,separate_input_output_schemas=separate)
    expected=public_get_openapi(**kwargs)
    assert sg.get_openapi(**kwargs)==expected

def test_adapters_are_avoided_for_identity_scalars_and_retained_for_custom_encoders():
    counts=[];original=TypeAdapter.__init__
    for factory in (lambda cls:cls,candidate_factory):
        count=[0]
        def tracked(self,*a,**k):
            count[0]+=1;return original(self,*a,**k)
        generator=factory(json_schema.GenerateJsonSchema)()
        with patch.object(TypeAdapter,'__init__',tracked):
            assert [generator.encode_default(v) for v in (None,True,7,'x')]==[None,True,7,'x']
            assert generator.encode_default(1.25)==1.25
            with generator._config_wrapper_stack.push({'json_encoders':{int:lambda v:'encoded-'+str(v)}}):
                assert generator.encode_default(7)=='encoded-7'
        counts.append(count[0])
    assert counts==[6,2]
