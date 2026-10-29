"""Disposable logical-hydration comparison; no real worktrees/Git/cleanup effects."""
import os,tempfile,atexit
from pathlib import Path
tmp=tempfile.TemporaryDirectory(prefix='pa-cleaned-hydration-private-');atexit.register(tmp.cleanup);root=Path(tmp.name)
os.environ.update(PA_DATA_DIR=str(root/'data'),PA_WORKSPACE_ROOT=str(root/'workspaces'),PA_AGENT_ENABLED='false')
import argparse,ast,copy,hashlib,json,sqlite3,time,types,subprocess
from datetime import datetime,timezone,timedelta
from pa.repository.workspace import WorkspaceManager,WorkspaceLease
from pa.core.performance import distribution
parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);parser.add_argument('--baseline',default='8780d278003051eea1b4a585da55cba12e05d102');args=parser.parse_args()
assert not args.output.exists(), 'Keep the original result; no blind rerun'
source=Path('src/pa/repository/workspace.py').read_text();baseline_source=subprocess.check_output(['git','show',args.baseline+':src/pa/repository/workspace.py'],text=True)
import pa.repository.workspace as module
assert hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()==hashlib.sha256(source.encode()).hexdigest()
baseline_tree=ast.parse(baseline_source);baseline_class=next(n for n in baseline_tree.body if isinstance(n,ast.ClassDef)and n.name=='WorkspaceManager');methods=[copy.deepcopy(n)for n in baseline_class.body if isinstance(n,ast.FunctionDef)and n.name in ['list','collect_garbage']];assert len(methods)==2
namespace=dict(vars(module));exec(compile(ast.fix_missing_locations(ast.Module(body=methods,type_ignores=[])),'<archived-GC-baseline>','exec'),namespace);baseline_list=namespace['list'];baseline_gc=namespace['collect_garbage']
for name in ['_row','_cleanup_lease_locked','_card_completion_eligible','_refresh_origin_for_cleanup']:
 current_class=next(n for n in ast.parse(source).body if isinstance(n,ast.ClassDef)and n.name=='WorkspaceManager')
 before=next(n for n in baseline_class.body if isinstance(n,ast.FunctionDef)and n.name==name);after=next(n for n in current_class.body if isinstance(n,ast.FunctionDef)and n.name==name);assert ast.dump(before,include_attributes=False)==ast.dump(after,include_attributes=False),name
now=datetime.now(timezone.utc);rows=[]
for shape,cleaned_count,payload in [('no_cleaned',0,''),('cleaned_ascii',610,'a'*65536),('cleaned_unicode',610,'é'*65536)]:
 db=root/(shape+'.db');conn=sqlite3.connect(db);lease=WorkspaceLease(id='x',repository_id='private',repository_url='https://example.invalid/private',session_id='x',card_id='private-card',cache_path=str(root/'cache'),worktree_path=str(root/'worktree'),branch='private',base_sha='0'*40,fencing_token=1,expires_at=now+timedelta(hours=1),state='ready',completed=True,merged=True);data=lease.model_dump(mode='json');columns=list(data);conn.execute('CREATE TABLE workspace_leases ('+','.join(k+' TEXT'for k in columns)+')');batch=[]
 for i in range(cleaned_count+42):
  row=dict(data,id=str(i),session_id=str(i),fencing_token=i,state='cleaned'if i<cleaned_count else'ready',cleanup_evidence=json.dumps(dict(proof=payload)if i<cleaned_count else{},ensure_ascii=False));batch.append(tuple(row[k]for k in columns))
 conn.executemany('INSERT INTO workspace_leases VALUES ('+','.join('?'for _ in columns)+')',batch);conn.commit();conn.close()
 for pair in range(30):
  for variant in (['baseline','candidate']if pair%2==0 else['candidate','baseline']):
   counts=dict(models=0,logical_evidence_utf8_bytes=0,completion_reads=0)
   def connect():
    c=sqlite3.connect(db);c.row_factory=sqlite3.Row;return c
   def row_model(row):counts['models']+=1;counts['logical_evidence_utf8_bytes']+=len(row['cleanup_evidence'].encode());return WorkspaceManager._row(row)
   def eligible(card_id):counts['completion_reads']+=1;return True
   manager=types.SimpleNamespace(_connect=connect,_row=row_model,_card_completion_eligible=eligible)
   manager.list=lambda **kwargs:(baseline_list if variant=='baseline' else WorkspaceManager.list)(manager,**kwargs)
   wall,cpu=time.perf_counter_ns(),time.thread_time_ns();result=(baseline_gc if variant=='baseline' else WorkspaceManager.collect_garbage)(manager,now=now,active_session_ids=set());wall_ms=(time.perf_counter_ns()-wall)/1e6;cpu_ms=(time.thread_time_ns()-cpu)/1e6
   assert result==dict(cleaned=0,blocked=0,missing_evidence=0,transient_retry=0,retained=42)
   assert counts['models']==(cleaned_count+42 if variant=='baseline'else 42)and counts['completion_reads']==(42 if variant=='baseline' else 0)
   rows.append(dict(shape=shape,pair=pair,variant=variant,wall_ms=wall_ms,thread_cpu_ms=cpu_ms,result=result,**counts))
 summary={shape:{variant:dict(wall_ms=distribution([r['wall_ms']for r in rows if r['shape']==shape and r['variant']==variant]),models=sum(r['models']for r in rows if r['shape']==shape and r['variant']==variant),logical_evidence_utf8_bytes=sum(r['logical_evidence_utf8_bytes']for r in rows if r['shape']==shape and r['variant']==variant))for variant in ['baseline','candidate']}for shape in ['no_cleaned','cleaned_ascii','cleaned_unicode']}
out=args.output;assert not out.exists();out.write_text(json.dumps(dict(baseline=args.baseline,baseline_source_sha256=hashlib.sha256(baseline_source.encode()).hexdigest(),observed_at=datetime.now(timezone.utc).isoformat(),source_sha256=hashlib.sha256(source.encode()).hexdigest(),rows=rows,summary=summary,scope='Archived878GC/listmethods comparedwithACTUALcandidate WorkspaceManager source. Existing_row/finalcleanup/cardguard/GitrefreshASTequal; defaultpubliclisthistory andrealcleanupfencing checkedseparately byregressions. 610synthetic cleanedleases with64Kcharacterproof plus42unexpired retainedleases;30alternatingpairs per3shapes. Everyresult/retainedcount exact;completionreads42to0foralreadyunexpiredretainedleases. AllrawalternatingwallCPU/model/bytecountsretained. LogicalmaterializedUTF8bytes notphysicalIO; no productioncard/worktree/Git/cleanup/admission/provider effects. Doesnotestablish livecleanedpopulation/payloadsizes or1.684srootcause. Future sourceimplementation needs fullcleanup/fencing/active/failed/missingcard/dirty safetyregressions.'),indent=2)+'\n');print(json.dumps(dict(path=str(out),summary=summary)))
