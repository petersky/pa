"""Disposable paired first-render comparison, complete actual page HTML parity."""
import argparse,asyncio,hashlib,json,os,tempfile,time
from pathlib import Path
from unittest.mock import patch

def main(output,iterations,manifest=None):
 with tempfile.TemporaryDirectory(prefix='pa-template-cache-') as temporary:
  root=Path(temporary)
  os.environ['PA_DATA_DIR']=str(root/'data');os.environ['PA_WORKSPACE_ROOT']=str(root/'workspaces')
  os.environ['PA_AGENT_ENABLED']='false';os.environ['PA_TELEMETRY_ENABLED']='false'
  import httpx
  from fastapi.responses import HTMLResponse
  from pa.config import Settings
  from pa.core.kernel import Kernel,DEFAULT_TEMPLATES
  from pa.core.performance import distribution
  from pa.modules import ui_shell
  from pa.server.template_cache import PackagedTemplateCache,build_template_cache
  start=time.perf_counter();cache_path=root/'templates.json'
  if manifest is None:
   build_template_cache(DEFAULT_TEMPLATES,cache_path)
   build_ms=(time.perf_counter()-start)*1000
  else:
   cache_path.write_bytes(manifest.read_bytes());build_ms=None
  started=time.perf_counter();cache=PackagedTemplateCache(cache_path);load_ms=(time.perf_counter()-started)*1000
  app=Kernel.boot(settings=Settings(data_dir=root/'data',workspace_root=root/'workspaces',peers=[],agent_enabled=False,telemetry_enabled=False,auth_required=False)).build_app()
  results=[]
  def render(request,page):
   context=ui_shell._shell_context(request)
   context.update(active_path=page.path,page=page,request=request,include_template=page.template)
   context.update(page.build_context(request))
   base=app.state.templates.env
   times={label:[] for label in ('baseline','candidate')};cpu={label:[] for label in times};calls={label:0 for label in times}
   for iteration in range(iterations):
    outputs={}
    for label in (('baseline','candidate') if iteration%2==0 else ('candidate','baseline')):
     env=base.overlay(bytecode_cache=cache if label=='candidate' else None)
     compile_source=env.compile
     def compile(*args,**kwargs):
      calls[label]+=1;return compile_source(*args,**kwargs)
     env.compile=compile
     start=time.perf_counter();started_cpu=time.thread_time()
     outputs[label]=env.get_template('shell.html').render(context)
     times[label].append((time.perf_counter()-start)*1000);cpu[label].append((time.thread_time()-started_cpu)*1000)
    assert outputs['baseline']==outputs['candidate'],page.id
   results.append({'page':page.id,'samples_ms':times,'thread_cpu_samples_ms':cpu,'latency_ms':{k:distribution(v) for k,v in times.items()},'thread_cpu_ms':{k:distribution(v) for k,v in cpu.items()},'source_compilations':calls,'html_bytes':len(outputs['candidate'].encode()),'complete_html_equal_every_pair':True})
   return HTMLResponse(outputs['candidate'])
  async def run():
   async with app.router.lifespan_context(app):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://fixture') as client:
     with patch.object(ui_shell,'render_page',render):
      for path in ('/agent','/fleet','/','/work','/pull-requests','/settings?section=agent'):
       response=await client.get(path);assert response.status_code==200
  asyncio.run(run())
  result={'scope':'Fresh Jinja environments for every first render; complete actual frozen page contexts; alternating order; temporary fullKernel data/workspaces; no peer/provider/live state. Build compilation cost recorded separately, no readiness/data/timeout changes. Not whole-page or deployed acceptance.','iterations':iterations,'cache_source':'supplied_wheel_artifact' if manifest else 'built_in_disposable_fixture','manifest_sha256':hashlib.sha256(cache_path.read_bytes()).hexdigest(),'build_compilation_ms':build_ms,'manifest_load_ms':load_ms,'manifest_bytes':cache_path.stat().st_size,'template_entries':len(cache.entries),'source_hashes':{name:hashlib.sha256(Path(name).read_bytes()).hexdigest() for name in ('src/pa/server/template_cache.py','src/pa/core/kernel.py','hatch_build.py','pyproject.toml','scripts/benchmark_template_cache.py')},'pages':results}
  data=json.dumps(result,indent=2)+'\n';digest=hashlib.sha256(data.encode()).hexdigest()
  output.with_name(output.stem+'.'+digest+output.suffix).write_text(data);output.write_text(data)
  print(json.dumps({'sha256':digest,**{k:v for k,v in result.items() if k!='pages'},'pages':[{k:v for k,v in page.items() if not k.endswith('samples_ms')} for page in results]}))
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);parser.add_argument('--iterations',type=int,default=20);parser.add_argument('--manifest',type=Path);args=parser.parse_args()
 if not 1<=args.iterations<=100: parser.error('iterations must be1..100')
 main(args.output,args.iterations,args.manifest)
