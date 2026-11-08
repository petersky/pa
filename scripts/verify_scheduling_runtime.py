"""Disposable real-clock delivery check. Never touches an installed PA data dir.

Default runtime exceeds twenty minutes. An accepting fixture adapter records
receipts; this does not verify a live model, messaging provider or deployment.
"""
from __future__ import annotations
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import time

from pa.scheduling.service import Scheduler
from pa.scheduling.store import Ledger, Schedule


async def verify(seconds: int, deliveries: int, output: Path):
    root = Path(__file__).resolve().parents[1]
    hashes = {str(path.relative_to(root)):hashlib.sha256(path.read_bytes()).hexdigest()
              for path in sorted((root/'src/pa/scheduling').glob('*.py'))}
    evidence = {'commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),
        'source_hashes':hashes,'interval_seconds':seconds,'requested_deliveries':deliveries,
        'compressed_time':seconds!=600,'started_at':time.time(),'receipts':[],
        'scope':'Real clock; disposable ledger and accepting adapter; no browser, live agent, messaging provider or deployment.'}
    output.parent.mkdir(parents=True,exist_ok=True)
    def persist():
        output.write_text(json.dumps(evidence,indent=2)+'\n')
    with tempfile.TemporaryDirectory(prefix='pa-scheduling-runtime-') as temporary:
        ledger=Ledger(Path(temporary)/'schedule.db')
        async def deliver(item):
            evidence['receipts'].append({'occurrence_id':item['id'],'accepted_at':time.time(),
                                         'envelope':json.loads(item['envelope'])})
            persist()
            return {'accepted':True}
        scheduler=Scheduler(None,None,ledger,deliver)
        record=ledger.create('fixture','create',Schedule(name='Real-clock scheduling fixture',
            trigger={'kind':'interval','seconds':seconds},destination={'kind':'inbox','target':'fixture'},
            payload='Check the current state and report a concise update.'),evidence['started_at'])
        evidence['schedule_id']=record['id'];persist()
        deadline=evidence['started_at']+seconds*deliveries+60
        while len(evidence['receipts'])<deliveries and time.time()<deadline:
            await scheduler.cycle()
            await asyncio.sleep(min(1,seconds))
        evidence['finished_at']=time.time()
        evidence['passed']=len(evidence['receipts'])==deliveries
        previous=evidence['started_at']
        for receipt in evidence['receipts']:
            evidence['passed'] &= receipt['accepted_at']-previous >= seconds-1
            previous=receipt['accepted_at']
        persist()
        if not evidence['passed']:
            raise RuntimeError('runtime delivery acceptance failed; see bounded evidence')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds',type=int,default=600)
    parser.add_argument('--deliveries',type=int,default=2)
    parser.add_argument('--evidence',type=Path,required=True)
    args=parser.parse_args()
    if args.seconds<1 or not 1<=args.deliveries<=10:
        parser.error('positive interval and 1–10 deliveries required')
    asyncio.run(verify(args.seconds,args.deliveries,args.evidence))
