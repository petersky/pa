"""Private expired-progress comparison; no service state imports or writes."""
import argparse,ast,hashlib,json,os,statistics,subprocess,tempfile,textwrap,time
from datetime import datetime,timezone,timedelta
from pathlib import Path
from unittest.mock import patch
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--events',type=int,default=48);args=p.parse_args();OUT=args.output;assert not OUT.exists();assert 1<=args.events<=10000
with tempfile.TemporaryDirectory(prefix='pa-expired-compare-') as tmp:
 os.environ.update(PA_DATA_DIR=str(Path(tmp)/'data'),PA_WORKSPACE_ROOT=str(Path(tmp)/'workspaces'),PA_AGENT_ENABLED='false')
 import pa.execution.dispatch as m
 from pa.execution.progress import DispatchProgressEventV1,ProgressPhase,CompletionReportV1
 from pa.execution.post_turn import PostTurnEvaluationV1,PostTurnDecision,FollowupActionV1,FollowupActionName,SafetyClassification
 now=datetime(2026,10,29,tzinfo=timezone.utc);old=now-timedelta(days=60)
 evaluation=PostTurnEvaluationV1(snapshot_id='fixture',context_digest='a'*64,decision=PostTurnDecision.OUTCOME_ACHIEVED,rationale='Private completed fixture',confidence=1.0,recommended_actions=[FollowupActionV1(name=FollowupActionName.NO_ACTION,parameters={'reason':'Complete'},idempotency_key_inputs=['fixture'],safety=SafetyClassification.RECORD_ONLY,human_approval_required=False)],operator_status_text='Complete')
 events=[DispatchProgressEventV1(dispatch_id='d',acp_session_id='s',originating_instance_id='target',authority_instance_id='authority',sequence=i+1,idempotency_key=f'p-{i}',phase=ProgressPhase.BLOCKED if i%5==0 else ProgressPhase.IMPLEMENTING,summary='Retained fixture',occurred_at=now if i%7==0 else old,last_activity_at=old,delivered_at=old) for i in range(args.events)]
 record=m.DispatchRecord(dispatch_id='d',mutation_id='m',idempotency_key='admit',card_id='c',session_id='s',authority_instance_id='authority',authority_url='https://example.invalid',target_instance_id='target',state='completed',acknowledged_at=old,final_report=CompletionReportV1(outcome='Complete'),post_turn_evaluations=[evaluation],progress_events=events,progress_seen_keys=[e.idempotency_key for e in events],progress_compacted_ranges=[[2100,2102]],created_at=old,updated_at=old)
 source=subprocess.check_output(['git','show','c41631beb2c63a4d7a7acbf030dcaab5a345ba27:src/pa/execution/dispatch.py'],text=True)
 cls=next(n for n in ast.parse(source).body if isinstance(n,ast.ClassDef) and n.name=='DispatchStore');fn=next(n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='compact');ns=dict(vars(m));exec(textwrap.dedent(ast.get_source_segment(source,fn)),ns)
 samples=[];parity=None;counts={}
 for pair in range(5):
  for variant in (['baseline','candidate'] if pair%2==0 else ['candidate','baseline']):
   store=m.DispatchStore(Path(tmp)/f'{pair}-{variant}');store.put(record)
   with store._transaction() as conn:
    for event in events:store._persist_event_conn(conn,event)
   before_updated=store.get('d').updated_at
   start,cpu=time.perf_counter_ns(),time.thread_time_ns()
   result=ns['compact'](store,now=now) if variant=='baseline' else store.compact(now=now)
   samples.append(dict(pair=pair,variant=variant,wall_ms=(time.perf_counter_ns()-start)/1e6,thread_cpu_ms=(time.thread_time_ns()-cpu)/1e6))
   current_record=store.get('d').model_dump(mode='json');assert store.get('d').updated_at==before_updated;current_record.pop('updated_at')
   actual=dict(result=result,record=current_record,keys=[r[0] for r in store._conn.execute('SELECT idempotency_key FROM dispatch_progress_events ORDER BY sequence')])
   if parity is None:parity=actual
   assert actual==parity
   # Separate untimed normalization/materialization work; restore initial record.
   store.close();store=m.DispatchStore(Path(tmp)/f'{pair}-{variant}-untimed');store.put(record)
   with store._transaction() as conn:
    for event in events:store._persist_event_conn(conn,event)
   counter={'normalize_calls':0,'range_inputs':0,'sqlite_statements':0}
   def traced(sql):counter['sqlite_statements']+=1
   store._conn.set_trace_callback(traced)
   normalize=m._normalized_sequence_ranges
   def counted(ranges):counter['normalize_calls']+=1;counter['range_inputs']+=len(ranges);return normalize(ranges)
   # The extracted baseline has its own globals; instrument both equally.
   with patch.object(m,'_normalized_sequence_ranges',counted):
    ns['_normalized_sequence_ranges']=counted
    if variant=='baseline':ns['compact'](store,now=now)
    else:store.compact(now=now)
   store._conn.set_trace_callback(None);counts[variant]=counter;ns['_normalized_sequence_ranges']=normalize;store.close()
 summary={v:{'samples':5,'p50':statistics.median(s['wall_ms'] for s in samples if s['variant']==v),'p95':max(s['wall_ms'] for s in samples if s['variant']==v),'p99':max(s['wall_ms'] for s in samples if s['variant']==v),'max':max(s['wall_ms'] for s in samples if s['variant']==v)} for v in ['baseline','candidate']}
 value=dict(contract='pa.dispatch-expired-compaction-comparison/v1',baseline='c41631beb2c63a4d7a7acbf030dcaab5a345ba27',candidate_sha256=hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest(),input_events=len(events),expired=parity['result'],retained_events=len(parity['keys']),parity=True,samples=samples,latency_ms=summary,untimed_counts=counts,scope='Five alternating fresh private-store pairs, Delivered progress events with interleaved protected and recent events. Original same SQL per expired event and transaction/publication semantics. Uninstrumented timing; separate untimed range-normalization counts. For --events above64 this is a stored historical/legacy-shaped input beyond current ingestion limits, not steady ordinary ingress. The public-ingest 48-event regression separately checks allowed semantics. Native host load/normal GC uncontrolled; not whole-maintenance, physical disk or historical-cause acceptance.')
 OUT.write_text(json.dumps(value,indent=2)+'\n');print(json.dumps(value))
