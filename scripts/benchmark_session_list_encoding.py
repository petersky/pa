"""Complete handler paired encoding prototype; private native selection fixture."""
import argparse,ast,asyncio,hashlib,json,os,subprocess,sys,tempfile,time
from pathlib import Path
from datetime import datetime,timezone
p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);a=p.parse_args();assert not a.output.exists()
with tempfile.TemporaryDirectory(prefix='pa613-encoding-private-') as tmp:
 os.environ.update(PA_DATA_DIR=str(Path(tmp)/'data'),PA_WORKSPACE_ROOT=str(Path(tmp)/'workspaces'),PA_AGENT_ENABLED='false')
 sys.path.insert(0,str(Path.cwd()))
 from pa.modules import agent_chat as module
 from tests.test_session_list_snapshot import listing
 source=Path(module.__file__).read_text();assert source==subprocess.check_output(['git','show','d3d98da9fbb7174ed81236713b9135447230bb27:src/pa/modules/agent_chat.py'],text=True), 'Reassess changed application source before comparison'
 old='body = json.dumps(items, default=str).encode()';new='body = json.dumps(items, default=str, ensure_ascii=False, separators=(",", ":")).encode("utf-8", errors="backslashreplace")'
 assert source.count(old)==1
 tree=ast.parse(source);node=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='list_agent_sessions_http');text=ast.get_source_segment(source,node);assert text.count(old)==1
 namespace=dict(vars(module));exec(compile(text.replace(old,new),'<exact612-encoding-prototype>','exec'),namespace)
 candidate=namespace['list_agent_sessions_http'];rows=[];summary={}
 async def run():
  for shape in ['ascii','unicode','structured']:
   directory=Path(tmp)/shape;directory.mkdir();fixture=listing.__wrapped__(directory);store,manager,request=next(fixture)
   try:
    for session in store.list_sessions():
     if shape=='structured':proof=[{'phase':'completed','retained':True,'index':i,'nested':{'values':[1,2,3,4]}} for i in range(1500)]
     else:proof=('ascii retained proof ' if shape=='ascii' else '界🙂 retained proof ')*4096
     session.config_json['encoding_fixture']=proof;store.save_session(session)
    expected=None;field_bytes={}
    for pair in range(30):
     for name in (['baseline','candidate'] if pair%2==0 else ['candidate','baseline']):
      call=module.list_agent_sessions_http if name=='baseline' else candidate
      wall,cpu=time.perf_counter(),time.process_time();response=await call(request,view='all',limit=10,detail='full');ms,cpu_ms=(time.perf_counter()-wall)*1000,(time.process_time()-cpu)*1000
      body=response.body;decoded=json.loads(body);assert len(decoded)==10
      if expected is None:expected=decoded
      assert decoded==expected and response.media_type=='application/json'
      rows.append(dict(shape=shape,pair=pair,variant=name,wall_ms=ms,process_cpu_ms=cpu_ms,response_bytes=len(body),body_sha256=hashlib.sha256(body).hexdigest()))
    for key in expected[0]:field_bytes[key]=sum(len(json.dumps(row.get(key),default=str).encode()) for row in expected)
    def dist(values):
     xs=sorted(values);return dict(samples=len(xs),p50_ms=xs[int((len(xs)-1)*.5)],p95_ms=xs[int((len(xs)-1)*.95)],p99_ms=xs[int((len(xs)-1)*.99)],max_ms=max(xs))
    summary[shape]=dict(field_bytes_baseline=field_bytes,complete_response_schema_selection_order_values_equal=True,variants={name:dict(latency=dist([r['wall_ms'] for r in rows if r['shape']==shape and r['variant']==name]),response_bytes=next(r['response_bytes'] for r in rows if r['shape']==shape and r['variant']==name)) for name in ['baseline','candidate']})
   finally:
    try:next(fixture)
    except StopIteration:pass
 asyncio.run(run())
 value=dict(contract='pa.session-list-encoding-prototype/v1',observed_at=datetime.now(timezone.utc).isoformat(),baseline_source='d3d98da9fbb7174ed81236713b9135447230bb27',source_sha256=hashlib.sha256(source.encode()).hexdigest(),only_method_difference=dict(before=old,after=new),rows=rows,summary=summary,scope='Thirty alternating pairs per private native ASCII/Unicode/structured full ten-session handler response. Complete JSON selection/order/schema/values equal; actual list/read/offload/Response path retained. Fake providers/no server/network/production mutation. Prototype only, not installed-source repair or actual live full-payload bound proof; baseline field byte counts are independently encoded values, not additive full-body decomposition. Lowerempirical quantiles/all raw/max retained; no hardware/historical/everybranch/wholePA acceptance.')
 a.output.write_text(json.dumps(value,indent=2)+'\n');print(json.dumps(summary))
