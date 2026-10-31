"""Private actual-source pending predicate comparison; no live receipt effects."""
import os,tempfile,atexit
from pathlib import Path
private=tempfile.TemporaryDirectory(prefix='pa88-pending-order-');atexit.register(private.cleanup);root=Path(private.name)
os.environ.update(PA_DATA_DIR=str(root/'data'),PA_WORKSPACE_ROOT=str(root/'workspaces'),PA_AGENT_ENABLED='false')
import ast,inspect,textwrap,sqlite3,json,hashlib,time,argparse,copy,subprocess
from datetime import datetime,timezone
from pa.domain import projection as module
from pa.core.performance import distribution
parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);parser.add_argument('--baseline',default='0fe1e4ea569e069b6d43be4402dc2afb9058bfba');benchmark_options=parser.parse_args();assert not benchmark_options.output.exists()
source=Path(module.__file__).read_text();assert hashlib.sha256(source.encode()).hexdigest()==hashlib.sha256(Path('src/pa/domain/projection.py').read_bytes()).hexdigest()
baseline_source=subprocess.check_output(['git','show',benchmark_options.baseline+':src/pa/domain/projection.py'],text=True)
def method(text):
 c=next(n for n in ast.parse(text).body if isinstance(n,ast.ClassDef)and n.name=='CardProjection');return copy.deepcopy(next(n for n in c.body if isinstance(n,ast.FunctionDef)and n.name=='_pending_restart_continuation'))
original=method(baseline_source);expected=copy.deepcopy(original);changes=[]
for n in ast.walk(expected):
 if isinstance(n,ast.Constant)and isinstance(n.value,str)and "AND continuation_prompt != '' AND delivered_at IS NULL "in n.value:
  n.value=n.value.replace("AND continuation_prompt != '' AND delivered_at IS NULL ","AND delivered_at IS NULL ").replace("AND status NOT IN ('continuation_delivered', 'restart_completed') LIMIT 1","AND status NOT IN ('continuation_delivered', 'restart_completed') AND continuation_prompt != '' LIMIT 1");changes.append(True)
assert len(changes)==1 and ast.dump(expected,include_attributes=False)==ast.dump(method(source),include_attributes=False)
old=ast.unparse(original);new=ast.unparse(method(source));original.decorator_list=[];space=dict(vars(module));exec(compile(ast.fix_missing_locations(ast.Module(body=[original],type_ignores=[])),'<archived-pending-predicate>','exec'),space);baseline=space['_pending_restart_continuation'];candidate=module.CardProjection._pending_restart_continuation

sqls=[n.value for n in ast.walk(ast.parse(source))if isinstance(n,ast.Constant)and isinstance(n.value,str)and n.value.strip().startswith('CREATE TABLE IF NOT EXISTS agent_restart_handoffs')];assert len(sqls)==1
rows=[]
for shape,payload in [('empty',''),('ascii','a'*65536),('unicode','é'*65536)]:
 db=root/(shape+'.db');conn=sqlite3.connect(db);conn.execute('PRAGMA journal_mode=WAL');conn.execute(sqls[0]);conn.execute('CREATE INDEX idx_restart_handoffs_key ON agent_restart_handoffs(idempotency_key)');conn.execute('CREATE INDEX idx_restart_handoffs_status_updated ON agent_restart_handoffs(status,updated_at)')
 conn.executemany('INSERT INTO agent_restart_handoffs(id,session_id,idempotency_key,continuation_prompt,continuation_prompt_id,status,card_id,created_at,updated_at,delivered_at)VALUES(?,?,?,?,?,?,?,?,?,?)',[(str(i),'session','key-'+str(i),payload,'prompt-'+str(i),'continuation_delivered','card','t','t','delivered')for i in range(1000)]);conn.commit()
 for field in ['session_id','card_id']:
  for pair in range(30):
   pair_results=[]
   for variant in (['baseline','candidate']if pair%2==0 else['candidate','baseline']):
    wall,cpu=time.perf_counter_ns(),time.thread_time_ns();result=(baseline if variant=='baseline'else candidate)(conn,**{field:field.split('_')[0]});wall_ms=(time.perf_counter_ns()-wall)/1e6;cpu_ms=(time.thread_time_ns()-cpu)/1e6;assert result is False
    rows.append({'shape':shape,'field':field,'pair':pair,'variant':variant,'wall_ms':wall_ms,'thread_cpu_ms':cpu_ms,'result':result});pair_results.append(result)
   assert pair_results[0]==pair_results[1]
 conn.close()
# Boolean parity across statuses, delivered state and empty/nonempty/NULL prompts;
# use a minimal nullable table to also retain legacy malformed-value semantics.
conn=sqlite3.connect(root/'parity.db');conn.execute('CREATE TABLE agent_restart_handoffs(session_id,card_id,continuation_prompt,status,delivered_at)');parity=[]
for status in ['pending','restarting','failed','continuation_delivered','restart_completed',None]:
 for delivered in [None,'delivered']:
  for prompt in [None,'',' ','x',0,b'x']:
   conn.execute('DELETE FROM agent_restart_handoffs');conn.execute('INSERT INTO agent_restart_handoffs VALUES(?,?,?,?,?)',('session','card',prompt,status,delivered))
   for field in ['session_id','card_id']:
    args={field:field.split('_')[0]};a=baseline(conn,**args);b=candidate(conn,**args);assert a==b;parity.append({'status':status,'delivered':delivered is not None,'prompt_type':type(prompt).__name__,'prompt_empty':prompt=='','field':field,'result':a})
conn.close()
summary={shape:{field:{variant:distribution([r['wall_ms']for r in rows if r['shape']==shape and r['field']==field and r['variant']==variant])for variant in ['baseline','candidate']}for field in ['session_id','card_id']}for shape in ['empty','ascii','unicode']}
v={'contract':'pa.pending-continuation-predicate-comparison/v1','observed_at':datetime.now(timezone.utc).isoformat(),'baseline':benchmark_options.baseline,'baseline_source_sha256':hashlib.sha256(baseline_source.encode()).hexdigest(),'source_module':module.__file__,'source_sha256':hashlib.sha256(source.encode()).hexdigest(),'original_method':old,'actual_candidate_method':new,'rows':rows,'summary':summary,'boolean_parity':parity,'scope':'180alternatingprivatepairs360calls/sameconnection;1000retained delivered rows with0/64KASCII/64KUnicode promptcharacters. Native full tabledefinition/uniqueindices/current key+statusindices, exact archivedbaseline staticmethod versus ACTUALsourcecandidate predicate reorder only. AllBoolean filters, failed/pending statuses and session/card semantics retained;144paritycases include legacyNULL/nontext. No source edit/index/schema/liveStore/receipt mutation/admission/rearm/network/provider/GC/default change. Warm query timings not production causalproof/physicalIO/readamplificationcounts; freshnativebusy/connect/disk/hardwarelimits remain unverified. Sourcecandidate UNSHIPPED; no historicalcause claim.'}
out=benchmark_options.output;assert not out.exists();out.write_text(json.dumps(v,indent=2)+'\n');print(json.dumps({'output':str(out),'summary':summary,'paritycases':len(parity)}))
