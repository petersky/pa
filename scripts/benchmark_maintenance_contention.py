"""Private WAL maintenance/foreground contention probe; never access live PA state."""
import os, tempfile, pathlib, sqlite3, threading, time, json, dataclasses, shutil, argparse
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output',type=pathlib.Path,required=True)
parser.add_argument('--incremental-experiment',action='store_true',help='Seed incremental auto-vacuum before tables; excludes existing-database conversion')
options=parser.parse_args()
output=options.output
assert not output.exists()
with tempfile.TemporaryDirectory(prefix='pa-private-maintenance-') as temp:
 root=pathlib.Path(temp)
 os.environ.update(PA_DATA_DIR=str(root/'data'),PA_WORKSPACE_ROOT=str(root/'workspaces'),PA_AGENT_ENABLED='false')
 from pa.instance.maintenance import _maintain_database
 from pa.core.sqlite_metrics import ObservedConnection
 from pa.core.performance import OperationObservation,current_observation,distribution
 assert shutil.disk_usage(root).free>1024**3
 samples=[]
 for iteration in range(3):
  path=root/f'fixture-{iteration}.db'
  with sqlite3.connect(path) as conn:
   if options.incremental_experiment:conn.execute('PRAGMA auto_vacuum=INCREMENTAL')
   conn.execute('PRAGMA journal_mode=WAL')
   conn.execute('PRAGMA synchronous=FULL')
   conn.execute('CREATE TABLE retained(id INTEGER PRIMARY KEY, body BLOB)')
   conn.executemany('INSERT INTO retained VALUES(?,?)',((i,b'x'*4096) for i in range(32768)))
   conn.execute('CREATE TABLE foreground(id INTEGER PRIMARY KEY, value INTEGER)')
   conn.execute('INSERT INTO foreground VALUES(1,0)')
   conn.commit()
   conn.execute('DELETE FROM retained WHERE id<64')
   conn.commit()
  entered=threading.Event(); original=ObservedConnection.execute
  def observe_execute(self,sql,*args,**kw):
   if sql=='VACUUM':
    entered.set()
    if options.incremental_experiment:
     cursor=original(self,'PRAGMA incremental_vacuum',*args,**kw)
     cursor.fetchall()
     return cursor
   return original(self,sql,*args,**kw)
  def maintenance():
   with patch.object(ObservedConnection,'execute',observe_execute):
    return _maintain_database(path)
  writes=[]
  with ThreadPoolExecutor(max_workers=1) as executor:
   future=executor.submit(maintenance)
   assert entered.wait(10), 'maintenance did not reach actual VACUUM'
   with sqlite3.connect(path,timeout=10,factory=ObservedConnection) as conn:
    conn.execute('PRAGMA synchronous=FULL')
    for i in range(100):
     obs=OperationObservation();token=current_observation.set(obs)
     wall=time.perf_counter();cpu=time.thread_time()
     try:
      conn.execute('UPDATE foreground SET value=value+1 WHERE id=1');conn.commit()
     finally:
      writes.append({'wall_ms':(time.perf_counter()-wall)*1000,'thread_cpu_ms':(time.thread_time()-cpu)*1000,'observation':dataclasses.asdict(obs)})
      current_observation.reset(token)
   result=future.result(timeout=20)
  with sqlite3.connect(path) as conn:
   assert conn.execute('PRAGMA quick_check').fetchall()==[('ok',)]
   assert conn.execute('SELECT COUNT(*),SUM(length(body)) FROM retained').fetchone()==(32704,32704*4096)
   assert conn.execute('SELECT value FROM foreground').fetchone()==(100,)
  assert result['compacted'],result
  assert result['after']['freelist_count']==0,result
  assert result['after']['bytes']<result['before']['bytes'],result
  assert result['status']=='ok' or (result['status']=='deferred' and result['reason']=='busy'),result
  samples.append({'iteration':iteration,'maintenance':result,'writes':writes,'foreground_latency_ms':distribution([w['wall_ms'] for w in writes]),'retained_rows':32704,'durable_updates':100})
 output.write_text(json.dumps({'contract':'pa.private-maintenance-contention/v1','scope':'Three private 151MB WAL databases, 256KB deleted blobs, actual maintenance (full VACUUM baseline or explicit incremental experiment) plus100 FULL synchronous durable writes each; no production DB access/fault or skip. Instrumentation sets event immediately before actual VACUUM without delay; scheduler may interleave. Does not establish disk bytes, native busy or live430MB acceptance.','incremental_experiment':options.incremental_experiment,'conversion_cost':'Excluded: enabled before creating tables; existing database migration remains unmeasured','samples':samples},indent=2)+'\n')
 print(json.dumps([{'iteration':s['iteration'],'write_latency':s['foreground_latency_ms'],'vacuum':s['maintenance']['phases']['vacuum'],'before':s['maintenance']['before'],'after':s['maintenance']['after']} for s in samples]))
