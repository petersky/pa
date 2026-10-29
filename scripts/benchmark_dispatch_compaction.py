"""Compare terminal dispatch compaction with unchanged records in a private store."""
import argparse,ast,hashlib,json,os,statistics,subprocess,tempfile,textwrap,time
from datetime import UTC,datetime
from pathlib import Path
from unittest.mock import patch

BASELINE='b12760da8856b06dc014fda1d31dd14def86f941'
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);args=p.parse_args()
assert not args.output.exists()
with tempfile.TemporaryDirectory(prefix='pa-compact-comparison-') as tmp:
 root=Path(tmp);os.environ.update(PA_DATA_DIR=str(root/'data'),PA_WORKSPACE_ROOT=str(root/'workspaces'),PA_AGENT_ENABLED='false')
 from pa.execution import dispatch as module
 from pa.execution.progress import CompletionReportV1
 from pa.execution.post_turn import PostTurnEvaluationV1,PostTurnDecision,FollowupActionV1,FollowupActionName,SafetyClassification
 store=module.DispatchStore(root/'fixture')
 now=datetime.now(UTC)
 evaluation=PostTurnEvaluationV1(snapshot_id='fixture-snapshot',context_digest='a'*64,decision=PostTurnDecision.OUTCOME_ACHIEVED,rationale='Private completed fixture',confidence=1.0,recommended_actions=[FollowupActionV1(name=FollowupActionName.NO_ACTION,parameters={'reason':'No follow-up'},idempotency_key_inputs=['fixture'],safety=SafetyClassification.RECORD_ONLY,human_approval_required=False)],operator_status_text='Completed')
 for i in range(100):
  store.put(module.DispatchRecord(dispatch_id=f'd-{i}',mutation_id=f'm-{i}',idempotency_key=f'k-{i}',card_id=f'c-{i}',authority_instance_id='fixture-authority',authority_url='https://example.invalid',target_instance_id='fixture-target',state='completed',acknowledged_at=now,final_report=CompletionReportV1(outcome='Completed'),post_turn_evaluations=[evaluation],completion_payload={'proof':'x'*65536}))

 source=subprocess.check_output(['git','show',BASELINE+':src/pa/execution/dispatch.py'],text=True)
 cls=next(n for n in ast.parse(source).body if isinstance(n,ast.ClassDef) and n.name=='DispatchStore')
 method=next(n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='compact')
 namespace=dict(vars(module));exec(textwrap.dedent(ast.get_source_segment(source,method)),namespace)
 functions={'baseline':lambda:namespace['compact'](store,now=now),'candidate':lambda:store.compact(now=now)}
 original=[tuple(r) for r in store._conn.execute('SELECT dispatch_id,payload_json FROM dispatches ORDER BY dispatch_id')]
 samples=[]
 for pair in range(5):
  for variant in (['baseline','candidate'] if pair%2==0 else ['candidate','baseline']):
   start,cpu=time.perf_counter_ns(),time.thread_time_ns();result=functions[variant]()
   samples.append(dict(pair=pair,variant=variant,wall_ms=(time.perf_counter_ns()-start)/1e6,thread_cpu_ms=(time.thread_time_ns()-cpu)/1e6))
   assert result=={'events':0,'receipts':0}
   assert [tuple(r) for r in store._conn.execute('SELECT dispatch_id,payload_json FROM dispatches ORDER BY dispatch_id')]==original
 stats={}
 for variant,run in functions.items():
  counter={'snapshots':0,'persist_calls':0,'serialized_bytes':0,'sqlite_statements':0}
  snapshot,persist,payload=store._snapshot,store._persist_record_conn,store._core_payload
  def counted_snapshot(r):counter['snapshots']+=1;return snapshot(r)
  def counted_persist(c,r):counter['persist_calls']+=1;return persist(c,r)
  def counted_payload(r):v=payload(r);counter['serialized_bytes']+=len(v.encode());return v
  def traced(sql):counter['sqlite_statements']+=1
  store._conn.set_trace_callback(traced)
  try:
   with patch.object(store,'_snapshot',counted_snapshot),patch.object(store,'_persist_record_conn',counted_persist),patch.object(store,'_core_payload',counted_payload):assert run()=={'events':0,'receipts':0}
  finally:store._conn.set_trace_callback(None)
  stats[variant]=counter
 summary={}
 for variant in functions:
  values=sorted(r['wall_ms'] for r in samples if r['variant']==variant)
  summary[variant]=dict(samples=len(values),p50=statistics.median(values),p95=values[-1],p99=values[-1],max=values[-1])
 out=dict(contract='pa.dispatch-compaction-unchanged-records/v1',baseline_source=BASELINE,source_module=module.__file__,source_sha256=hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest(),records=100,completion_proof_bytes_per_record=65536,samples=samples,latency_ms=summary,untimed_amplification_pass=stats,database_payloads_unchanged=True,scope='Five alternating same-store pairs include existing writer lock, eligibility checks, transaction, receipt expiry and publication. Timers are uninstrumented; a separate untimed pass counts copies/serialization/SQL. All 100 terminal records have identical retained 64 KiB proof input and no expirable events or receipts. Small samples and uncontrolled host/normal GC do not prove whole-maintenance speedup, physical disk writes or historical cause. No production state or retention/compaction policy changes.')
 args.output.write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out));store.close()
