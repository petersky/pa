"""Private actual-source versus archived reconciliation; no live workspace effects."""
import os,tempfile,atexit
from pathlib import Path
private=tempfile.TemporaryDirectory(prefix='pa88-reconcile-probe-');atexit.register(private.cleanup);root=Path(private.name)
os.environ.update(PA_DATA_DIR=str(root/'data'),PA_WORKSPACE_ROOT=str(root/'workspaces'),PA_AGENT_ENABLED='false')
import ast,copy,sqlite3,json,hashlib,time,types,argparse,subprocess
from datetime import datetime,timezone,timedelta
from pa.repository import workspace as module
from pa.core.performance import distribution
parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);parser.add_argument('--baseline',default='0fe1e4ea569e069b6d43be4402dc2afb9058bfba');args=parser.parse_args();assert not args.output.exists()
source=Path(module.__file__).read_text();assert hashlib.sha256(source.encode()).hexdigest()==hashlib.sha256(Path('src/pa/repository/workspace.py').read_bytes()).hexdigest()
baseline_source=subprocess.check_output(['git','show',args.baseline+':src/pa/repository/workspace.py'],text=True)
def cls(text):return next(n for n in ast.parse(text).body if isinstance(n,ast.ClassDef)and n.name=='WorkspaceManager')
old_class,new_class=cls(baseline_source),cls(source)
def method(c,name):return copy.deepcopy(next(n for n in c.body if isinstance(n,ast.FunctionDef)and n.name==name))
original=method(old_class,'reconcile_terminal_state');expected=copy.deepcopy(original)
loops=[n for n in ast.walk(expected)if isinstance(n,ast.For)and isinstance(n.iter,ast.Call)and isinstance(n.iter.func,ast.Attribute)and n.iter.func.attr=='list'];assert len(loops)==1 and not loops[0].iter.args and not loops[0].iter.keywords
loops[0].iter.keywords=[ast.keyword(arg='include_cleaned',value=ast.Constant(False))]
assert ast.dump(expected,include_attributes=False)==ast.dump(method(new_class,'reconcile_terminal_state'),include_attributes=False)
for name in ['list','_row','_connect','_cleanup_lease_locked','_card_completion_eligible','_refresh_origin_for_cleanup','collect_garbage']:
 assert ast.dump(method(old_class,name),include_attributes=False)==ast.dump(method(new_class,name),include_attributes=False),name
space=dict(vars(module));exec(compile(ast.fix_missing_locations(ast.Module(body=[original],type_ignores=[])),'<archived-reconciliation>','exec'),space);baseline_method=space['reconcile_terminal_state'];candidate=module.WorkspaceManager.reconcile_terminal_state

now=datetime.now(timezone.utc);rows=[]
for shape,count,payload in [('none',0,''),('ascii',610,'a'*65536),('unicode',610,'é'*65536)]:
 db=root/(shape+'.db');conn=sqlite3.connect(db);lease=module.WorkspaceLease(id='x',repository_id='fixture',repository_url='https://example.invalid/fixture',session_id='x',card_id='card',cache_path=str(root/'cache'),worktree_path=str(root/'worktree'),branch='fixture',base_sha='0'*40,fencing_token=1,expires_at=now+timedelta(hours=1),state='ready');data=lease.model_dump(mode='json');columns=list(data);conn.execute('CREATE TABLE workspace_leases ('+','.join(k+' TEXT'for k in columns)+')');batch=[]
 for i in range(count+42):
  row=dict(data,id=str(i),session_id=str(i),fencing_token=i,state='cleaned'if i<count else'ready',cleanup_evidence=json.dumps({'proof':payload}if i<count else{},ensure_ascii=False));batch.append(tuple(row[k]for k in columns))
 conn.executemany('INSERT INTO workspace_leases VALUES ('+','.join('?'for _ in columns)+')',batch);conn.commit();conn.close()
 for pair in range(30):
  pair_results=[]
  for variant in (['baseline','candidate']if pair%2==0 else['candidate','baseline']):
   counters={'models':0,'logical_utf8_bytes':0};pending=[]
   def connect():
    c=sqlite3.connect(db);c.row_factory=sqlite3.Row;return c
   def model(row):
    counters['models']+=1;counters['logical_utf8_bytes']+=len(row['cleanup_evidence'].encode());return module.WorkspaceManager._row(row)
   def pending_read(*,session_id):pending.append(session_id);return None
   def no_write(*args,**kwargs):raise AssertionError('Retained-only probe must never write or clean')
   store=types.SimpleNamespace(list_card_lanes=lambda:{'card':'active'},list_session_statuses=lambda:{},pending_restart_continuation=pending_read)
   manager=types.SimpleNamespace(store=store,_connect=connect,_row=model,_save=no_write,_increment_metric=no_write,_card_completion_eligible=no_write)
   manager.list=lambda **kwargs:module.WorkspaceManager.list(manager,**kwargs)
   wall,cpu=time.perf_counter_ns(),time.thread_time_ns();result=(baseline_method if variant=='baseline'else candidate)(manager,now=now);wall_ms=(time.perf_counter_ns()-wall)/1e6;cpu_ms=(time.thread_time_ns()-cpu)/1e6
   assert result=={'examined':42,'cards_completed':0,'standalone_completed':0,'orphaned_completed':0,'closed_expired':0,'missing_cards':0,'nonterminal_cards':42,'retained':42}
   assert pending==[str(i)for i in range(count,count+42)];assert counters['models']==(count+42 if variant=='baseline'else 42)
   row={'shape':shape,'pair':pair,'variant':variant,'wall_ms':wall_ms,'thread_cpu_ms':cpu_ms,'result':result,'pending_read_ids':pending,**counters};rows.append(row);pair_results.append((result,pending))
  assert pair_results[0]==pair_results[1]
summary={shape:{variant:{'wall_ms':distribution([r['wall_ms']for r in rows if r['shape']==shape and r['variant']==variant]),'models':sum(r['models']for r in rows if r['shape']==shape and r['variant']==variant),'logical_utf8_bytes':sum(r['logical_utf8_bytes']for r in rows if r['shape']==shape and r['variant']==variant)}for variant in ['baseline','candidate']}for shape in ['none','ascii','unicode']}
v={'contract':'pa.workspace-reconciliation-hydration-comparison/v1','observed_at':datetime.now(timezone.utc).isoformat(),'baseline':args.baseline,'baseline_source_sha256':hashlib.sha256(baseline_source.encode()).hexdigest(),'source_module':module.__file__,'source_sha256':hashlib.sha256(source.encode()).hexdigest(),'actual_candidate_method':ast.unparse(method(new_class,'reconcile_terminal_state')),'rows':rows,'summary':summary,'scope':'90 alternating private pairs/180calls; exact archivedbaseline versus ACTUAL sourcecandidate adding only list(include_cleaned=False). Private SQLite retains610cleaned proofs/42activeleases; all result counters/order/42pendingreads equal, no writes or realGit/cleanup/providers/admission. Sourcecandidate UNSHIPPED; list/row/connection/finalcleanup/cardguard/Gitrefresh/GC ASTequal tobaseline; no native protections altered. Does not establish live cleaned population/proof sizes,9.957sGC or historical cause/physicalIO. Need full native reconcile/pending/failed/missingcard/cleanup safety regressions before source change.'}
out=args.output;assert not out.exists();out.write_text(json.dumps(v,indent=2)+'\n');print(json.dumps({'output':str(out),'summary':summary}))
