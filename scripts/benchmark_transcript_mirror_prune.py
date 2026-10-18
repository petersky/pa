"""Disposable compatibility-table leaf only; no PA Store/Kernel/live data."""
import argparse,ast,contextlib,hashlib,json,sqlite3,subprocess,tempfile,time
import inspect
from pa.domain.projection import CardProjection
from datetime import UTC,datetime,timedelta
from pathlib import Path
from types import SimpleNamespace
from pa.core.performance import OperationObservation,current_observation,distribution
from pa.core.sqlite_metrics import ObservedConnection
parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
source=subprocess.check_output(['git','show','5024f7890314805617750bcc2e5d1e059b9a472d:src/pa/domain/projection.py']).decode()
tree=ast.parse(source)
method=next(n for c in tree.body if isinstance(c,ast.ClassDef) and c.name=='CardProjection' for n in c.body if isinstance(n,ast.FunctionDef) and n.name=='prune_closed_session_transcripts')
namespace={'datetime':datetime};exec(compile(ast.Module(body=[method],type_ignores=[]),'<exact5024-compatibility-prune>','exec'),namespace);baseline=namespace['prune_closed_session_transcripts']
candidate=CardProjection.prune_closed_session_transcripts
out={'scope':'Synthetic temporary SQLite compatibility mirror ONLY; canonical TranscriptStorage.prune/list_range are stubs holding exactretainedseqs. No PA Store/Kernel/server/live state/retention/config changes. Canonicalproof/refcounts/GC/expiry concurrency NOTverifiedbythisleaf. Exact5024method AST vs currentproductmethod;candidateunmerged/uninstalled.','baseline_source_sha256':hashlib.sha256(source.encode()).hexdigest(),'candidate_source_sha256':hashlib.sha256(Path(inspect.getfile(CardProjection)).read_bytes()).hexdigest(),'samples':[]}
with tempfile.TemporaryDirectory(prefix='pa-compat-prune-') as tmp:
 path=Path(tmp)/'fixture.db'
 @contextlib.contextmanager
 def db():
  with contextlib.closing(sqlite3.connect(path,factory=ObservedConnection)) as conn:
   conn.row_factory=sqlite3.Row
   yield conn
   conn.commit()
 with db() as conn:
  conn.executescript('CREATE TABLE agent_sessions(id TEXT PRIMARY KEY,status TEXT,updated_at TEXT);CREATE TABLE agent_transcript_events(session_id TEXT,seq INTEGER,payload TEXT,PRIMARY KEY(session_id,seq));')
  now=datetime.now(UTC);old=now-timedelta(days=30)
  conn.executemany('INSERT INTO agent_sessions VALUES(?,?,?)',[('expired','closed',old.isoformat()),('live','idle',old.isoformat()),('young','closed',now.isoformat())])
 for size in (1000,10000):
  kept={1,size};owner=SimpleNamespace(_conn=db,transcripts=SimpleNamespace(prune=lambda ids,keep_audit:len(ids)*(size-len(kept)),list_range=lambda identity:[SimpleNamespace(seq=x) for x in kept]))
  expected=None
  for i in range(20):
   for mode,fn in ((('baseline',baseline),('candidate',candidate)) if i%2==0 else (('candidate',candidate),('baseline',baseline))):
    with db() as conn:
     conn.execute('DELETE FROM agent_transcript_events')
     conn.executemany('INSERT INTO agent_transcript_events VALUES(?,?,?)',[('expired',n,'synthetic') for n in range(1,size+1)]+[('live',1,'live'),('young',1,'young')])
    obs=OperationObservation();token=current_observation.set(obs);start=time.perf_counter();cpu=time.thread_time()
    try:deleted=fn(owner,before=now-timedelta(days=7))
    finally:current_observation.reset(token)
    elapsed=(time.perf_counter()-start)*1000;cpu=(time.thread_time()-cpu)*1000
    with db() as conn:rows=[tuple(r) for r in conn.execute('SELECT * FROM agent_transcript_events ORDER BY session_id,seq')]
    signature=hashlib.sha256(json.dumps(rows).encode()).hexdigest()
    if expected is None:expected=(deleted,signature)
    assert expected==(deleted,signature)
    assert len(rows)==4
    out['samples'].append({'size':size,'mode':mode,'iteration':i,'wall_ms':elapsed,'thread_cpu_ms':cpu,'sqlite_calls':obs.sqlite_calls,'sqlite_rows':obs.sqlite_rows,'deleted':deleted,'result_sha256':signature})
  print(json.dumps({'size':size,'summary':{mode:{metric:distribution([r[metric] for r in out['samples'] if r['size']==size and r['mode']==mode]) for metric in ('wall_ms','thread_cpu_ms','sqlite_calls','sqlite_rows')} for mode in ('baseline','candidate')}}),flush=True)
  args.output.write_text(json.dumps(out,indent=2)+'\n')
