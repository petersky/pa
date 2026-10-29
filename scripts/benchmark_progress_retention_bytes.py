"""Compare retention byte accounting on paired private native progress stores.

Current model UTF-8 bytes remain authoritative; persisted byte_size is not used.
No provider/network/production mutation. Both roots are private before PA imports.
"""
import argparse, ast, hashlib, json, os, subprocess, tempfile, time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MethodType
from unittest.mock import patch

private = tempfile.TemporaryDirectory(prefix='pa609-progress-byte-experiment-')
root = Path(private.name)
os.environ.update(PA_DATA_DIR=str(root/'unused-data'), PA_WORKSPACE_ROOT=str(root/'unused-workspaces'), PA_AGENT_ENABLED='false')
import pa.execution.dispatch as module
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.progress import DispatchProgressEventV1 as Event, ProgressPhase, ProgressValidationV1, ProgressToolDetailV1

parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);parser.add_argument('--baseline',default='3fdad48f1d0caa9127af2f9e383003493adf0e99');args=parser.parse_args()
outpath=args.output; assert not outpath.exists()
assert Path(module.__file__).resolve().is_relative_to(Path('src').resolve()), 'Source comparison requires repository PA modules'
source=Path(module.__file__).read_text()
baseline=subprocess.check_output(['git','show',args.baseline+':src/pa/execution/dispatch.py'],text=True)
trees=[ast.parse(text) for text in [baseline,source]]
classes=[next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='DispatchStore') for tree in trees]
old_method=next(n for n in classes[0].body if isinstance(n,ast.FunctionDef) and n.name=='ingest_progress')
assert not any(getattr(n,'name',None)=='_progress_event_byte_size' for n in classes[0].body)
assert any(getattr(n,'name',None)=='_progress_event_byte_size' for n in classes[1].body)
for cls in classes:cls.body=[n for n in cls.body if getattr(n,'name',None) not in ['ingest_progress','_progress_event_byte_size']]
assert ast.dump(trees[0])==ast.dump(trees[1]), 'Shared application changed; reassess comparison'
class Clock:
 stamp=datetime(2026,10,29,18,0,tzinfo=UTC)
 @classmethod
 def now(cls,tz=None):return cls.stamp
namespace=dict(vars(module));namespace['datetime']=Clock
exec(compile(ast.Module(body=[old_method],type_ignores=[]),'archived608-ingest-progress','exec'),namespace)
rows=[]
def fields(seq):
 return dict(dispatch_id='dispatch',card_id='card',acp_session_id='session',originating_instance_id='origin',authority_instance_id='authority',sequence=seq,idempotency_key=f'event-{seq}',occurred_at=Clock.stamp,last_activity_at=Clock.stamp,phase=ProgressPhase.IMPLEMENTING,summary=f'checkpoint {seq}')
def event(seq,shape):
 data=fields(seq)
 if shape!='small':
  data.update(validations=[ProgressValidationV1(command=('a'*2000 if shape=='ascii' else '界'*500),status='passed') for _ in range(20)],tool_details=[ProgressToolDetailV1(**{k:(('a' if shape=='ascii' else '界')*(80 if k in ['kind','status'] else (240 if shape=='ascii' else 80))) for k in ['title','kind','status','result']}) for _ in range(10)])
 return Event(**data)
def distribution(xs):
 xs=sorted(xs);return dict(samples=len(xs),p50_ms=xs[int((len(xs)-1)*.5)],p95_ms=xs[int((len(xs)-1)*.95)],p99_ms=xs[int((len(xs)-1)*.99)],max_ms=max(xs))
try:
 for shape in ['small','ascii','unicode']:
  record=DispatchRecord(dispatch_id='dispatch',mutation_id='fixture',authority_instance_id='authority',authority_url='https://fixture.invalid',target_instance_id='origin',session_id='session',card_id='card',created_at=Clock.stamp,updated_at=Clock.stamp)
  record.progress_events=[event(i,shape) for i in range(1,64)]
  for e in record.progress_events[:60]:e.phase=ProgressPhase.TURN_ENDED
  record.progress_accepted_ranges=[[1,63]];record.progress_highest_accepted_sequence=63;record.progress_next_sequence=64
  stores={}
  for variant in ['baseline','candidate']:
   location=root/shape/variant;location.mkdir(parents=True)
   (location/'dispatch_mutations.json').write_text(json.dumps({'dispatch':record.model_dump(mode='json',exclude_unset=True)}))
   stores[variant]=DispatchStore(location)
  stores['baseline'].ingest_progress=MethodType(namespace['ingest_progress'],stores['baseline'])
  try:
   for pair in range(30):
    Clock.stamp=datetime(2026,10,29,18,0,tzinfo=UTC)+timedelta(seconds=pair+1)
    incoming=event(64+pair,shape)
    for variant in (['baseline','candidate'] if pair%2==0 else ['candidate','baseline']):
     s=stores[variant]
     # Confirm exact UTF8 byte parity on actual hydrated models; no persisted
     # byte_size trust, delivery cache, normalization or future-state assumption.
     assert all(e.__pydantic_serializer__.to_json(e)==e.model_dump_json().encode() for e in s.get('dispatch').progress_events)
     with patch.object(module,'datetime',Clock):
      start,cpu=time.perf_counter(),time.thread_time();result=s.ingest_progress(incoming,delivered=True)
      wall_ms,cpu_ms=(time.perf_counter()-start)*1000,(time.thread_time()-cpu)*1000
     rows.append(dict(shape=shape,pair=pair,variant=variant,wall_ms=wall_ms,cpu_ms=cpu_ms,result=result.model_dump(mode='json')))
    assert rows[-1]['result']==rows[-2]['result']
    assert stores['baseline'].get('dispatch')==stores['candidate'].get('dispatch')
    if pair%5==4:
     for s in stores.values():
      with patch.object(module,'datetime',Clock):s.mark_progress_delivery_failed('dispatch','event-1','界 delivery retry '+str(pair))
     assert stores['baseline'].get('dispatch')==stores['candidate'].get('dispatch')
    for table in ['dispatches','dispatch_progress_events','dispatch_receipts']:
     dbrows=[sorted(tuple(r) for r in s._conn.execute('SELECT * FROM '+table)) for s in stores.values()]
     assert dbrows[0]==dbrows[1], 'Durable rows differ: '+table
   for variant,s in stores.items():
    visible=s.get('dispatch'); assert len(visible.progress_events)==64
    persisted=s._conn.execute('SELECT sequence,byte_size,payload_json FROM dispatch_progress_events ORDER BY sequence').fetchall()
    assert all(r['byte_size']==len(r['payload_json'].encode()) for r in persisted)
    s.close();reload=DispatchStore(root/shape/variant)
    assert reload.get('dispatch')==visible;reload.close()
  finally:
   for s in stores.values():s.close()
 value=dict(contract='pa.progress-retention-byte-comparison/v1',baseline=args.baseline,baseline_sha256=hashlib.sha256(baseline.encode()).hexdigest(),all_other_application_ast_equal=True,source_sha256=hashlib.sha256(source.encode()).hexdigest(),private_roots_before_imports=True,canonical_models_durable_rows_receipts_and_reload_equal=True,rows=rows,summary={shape:{variant:distribution([r['wall_ms'] for r in rows if r['shape']==shape and r['variant']==variant]) for variant in ['baseline','candidate']} for shape in ['small','ascii','unicode']},scope='Thirty alternating native-store pairs per small/largeASCII/largeUnicode shape, retained63 then64 events with60protected. Exact archived608 ingestion versus current source; only ingestion and canonical-byte helper differ in application AST. Omitted legacy defaults hydrate before accounting; delivery failures change live models every5pairs. Every current model canonical bytes, complete visible state/results, all durable dispatch/event/receipt rows and reload match. No persisted byte_size/cache or retention/policy/transaction/durability change. All samples/maxima retained with lower empirical percentiles. No forcedGC/controlledhardware/physicalIO/historicalcause/deployment/everybranch/fullPA acceptance.')
 outpath.write_text(json.dumps(value,indent=2)+'\n'); print(json.dumps(value['summary']))
finally:private.cleanup()
