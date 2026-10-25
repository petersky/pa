"""Compare only the legacy cursor predicate in a private in-memory fixture."""
import argparse, hashlib, json, sqlite3, time
from datetime import UTC, datetime
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
out = args.output
assert not out.exists()
conn=sqlite3.connect(':memory:')
conn.executescript('''CREATE TABLE agent_transcript_events(
 id TEXT PRIMARY KEY,session_id TEXT NOT NULL,seq INTEGER NOT NULL,event_type TEXT NOT NULL,
 payload TEXT NOT NULL DEFAULT '{}',created_at TEXT NOT NULL,UNIQUE(session_id,seq));
 CREATE INDEX idx_transcript_session_seq ON agent_transcript_events(session_id,seq);''')
payload=json.dumps({'text':'synthetic migration metadata'})
conn.executemany('INSERT INTO agent_transcript_events VALUES(?,?,?,?,?,?)',
 ((f'e-{i}',f'session-{i//1000:03d}',i%1000+1,'chunk',payload,'2026-10-25T00:00:00Z') for i in range(100000)))
conn.commit()
old='''SELECT * FROM agent_transcript_events
 WHERE session_id > ? OR (session_id=? AND seq>?) ORDER BY session_id,seq LIMIT ?'''
new='''SELECT * FROM agent_transcript_events
 WHERE (session_id,seq) > (?,?) ORDER BY session_id,seq LIMIT ?'''
def params(label,cursor,limit=500):
 return (cursor[0],cursor[0],cursor[1],limit) if label=='baseline' else (*cursor,limit)
queries={'baseline':old,'candidate':new}
cases=[]
for cursor in [('',-1),('session-000',0),('session-000',1000),('session-050',500),
 ('session-090',900),('session-099',1000),('雪',0),(None,0),('session-050',None),
 ('session-050','bad'),('session-050',1.5)]:
 values={label:conn.execute(sql,params(label,cursor)).fetchall() for label,sql in queries.items()}
 assert values['baseline']==values['candidate'],cursor
 cases.append(dict(cursor=cursor,rows=len(values['baseline'])))
cursor=('session-090',900)
plans={label:conn.execute('EXPLAIN QUERY PLAN '+sql,params(label,cursor)).fetchall() for label,sql in queries.items()}
work={}
for label,sql in queries.items():
 count=[0]
 def step():
  count[0]+=1
  return 0
 conn.set_progress_handler(step,1000)
 try:
  conn.execute(sql,params(label,cursor)).fetchall()
 finally:
  conn.set_progress_handler(None,0)
 work[label]=dict(callbacks=count[0],instruction_interval=1000,vm_steps_lower_bound=count[0]*1000)
samples=[]
for i in range(5):
 pair={}
 for label in (('baseline','candidate') if i%2==0 else ('candidate','baseline')):
  wall,cpu=time.perf_counter_ns(),time.thread_time_ns()
  rows=conn.execute(queries[label],params(label,cursor)).fetchall()
  pair[label]=dict(wall_ms=(time.perf_counter_ns()-wall)/1e6,thread_cpu_ms=(time.thread_time_ns()-cpu)/1e6,rows=len(rows))
 samples.append(pair)
value=dict(observed_at=datetime.now(UTC).isoformat(),sqlite_version=sqlite3.sqlite_version,
 input_rows=100000,page_limit=500,query_plans=plans,instruction_observations=work,parity_cases=cases,
 samples=samples,scope='Copied production table/key schema and exact cursor predicates, synthetic in-memory rows only. No PA imports, live database, migration, service mutation or faults. VM work is a lower bound sampled separately; timed pairs have no progress handler. Results do not establish the sole cause or deployment improvement for the live 2.504-second migration/health phase.')
out.write_text(json.dumps(value,indent=2)+'\n')
print(json.dumps(value|{'sha256':hashlib.sha256(out.read_bytes()).hexdigest()}))
