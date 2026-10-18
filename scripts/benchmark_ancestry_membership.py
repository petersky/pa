"""Alternating exact5024 method/current method, disposable seeded index only."""
import argparse,ast,hashlib,json,subprocess,tempfile,time
from pathlib import Path
import pa.sync.event_log as module
from pa.core.performance import OperationObservation,current_observation,distribution
from pa.core.sqlite_metrics import sqlite_connection_scope
from pa.domain.models import SyncCommit
from pa.sync.object_store import ObjectStore
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--baseline', default='5024f7890314805617750bcc2e5d1e059b9a472d')
args=parser.parse_args()
BASE=args.baseline
source=subprocess.check_output(['git','show',BASE+':src/pa/sync/event_log.py']).decode()
tree=ast.parse(source)
method=next(n for c in tree.body if isinstance(c,ast.ClassDef) and c.name=='EventLog' for n in c.body if isinstance(n,ast.FunctionDef) and n.name=='is_ancestor')
method.name='baseline_is_ancestor';namespace=dict(vars(module));exec(compile(ast.Module(body=[method],type_ignores=[]),'<exact5024-is_ancestor>','exec'),namespace)
baseline=namespace['baseline_is_ancestor'];candidate=module.EventLog.is_ancestor
out={'scope':'Two-point ancestry queries on disposable synthetic ready linear index; retainedrows seeded directly, not canonical rebuild/sourcefsync/physicalIO/wholePA acceptance. Canonical missing/realm/concurrency/fork validation is tested separately. Exact5024 function AST vs candidate, alternating order with worker connection scope and normal GC. No live data/Store/EventLog changes.','baseline_source':BASE,'baseline_file_sha256':hashlib.sha256(source.encode()).hexdigest(),'candidate_files':{p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in ['src/pa/sync/event_log.py','src/pa/sync/dag_index.py']},'samples':[]}
with tempfile.TemporaryDirectory(prefix='pa-ancestry-membership-bench-') as tmp:
 for size in (5000,30000):
  root=Path(tmp)/str(size);root.mkdir();store=ObjectStore(root/'objects');log=module.EventLog(store,root,'fixture')
  hashes=[hashlib.sha256(str(i).encode()).hexdigest() for i in range(size)]
  with log.index._db() as conn:
   conn.executemany('INSERT INTO commits VALUES(?,?,?,?,?,?)',[('default',h,i,'fixture','user:fixture','2026-01-01') for i,h in enumerate(hashes)])
   conn.executemany('INSERT INTO commit_parents VALUES(?,?,?,?)',[('default',hashes[i],hashes[i-1],0) for i in range(1,size)])
   conn.execute("INSERT INTO index_meta(realm_id,schema_version,indexed_head,state,commit_count,event_count) VALUES('default',2,?,'ready',?,0)",(hashes[-1],size))
  def novel(parents):
   commit=SyncCommit(hash='',realm_id='default',instance_id='fixture',parent_hashes=parents,event_hashes=[],author_principal='user:fixture')
   return store.put_json(commit.model_dump(mode='json'))
  fork=novel([hashes[-2]]);merge=novel([hashes[-1],fork])
  for i in range(20):
   for mode,fn in ((('baseline',baseline),('candidate',candidate)) if i%2==0 else (('candidate',candidate),('baseline',baseline))):
    obs=OperationObservation();token=current_observation.set(obs);start=time.perf_counter();cpu=time.thread_time()
    try:
     with sqlite_connection_scope():
      results=[fn(log,hashes[-1],fork),fn(log,hashes[-1],merge)]
     assert results==[False,True],results
    finally:current_observation.reset(token)
    out['samples'].append({'retained':size,'mode':mode,'iteration':i,'wall_ms':(time.perf_counter()-start)*1000,'thread_cpu_ms':(time.thread_time()-cpu)*1000,'aggregate_items':obs.sqlite_aggregate_items,'aggregate_bytes':obs.sqlite_aggregate_bytes,'sqlite_calls':obs.sqlite_calls,'objects_read':obs.object_get_calls,'results':results})
  print(json.dumps({'retained':size,'summary':{mode:{key:distribution([r[key] for r in out['samples'] if r['retained']==size and r['mode']==mode]) for key in ('wall_ms','thread_cpu_ms','aggregate_items','aggregate_bytes','sqlite_calls')} for mode in ('baseline','candidate')}}),flush=True)
args.output.write_text(json.dumps(out,indent=2)+'\n')
