"""Persistent chrome must release streams across full navigation/BFCache."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest


@pytest.mark.skipif(not shutil.which('node'), reason='Node is required')
def test_notification_stream_and_poll_are_single_owner_across_page_lifecycle():
    source = Path('src/pa/server/static/js/notifications.js').read_text()
    harness = r'''
const vm=require('node:vm');
const events={};let active=0,maximum=0,created=0;let timers=new Set();let timer=0;
const window={
  addEventListener:(name,fn)=>events[name]=fn,
  setInterval:()=>{timers.add(++timer);return timer},
  clearInterval:id=>timers.delete(id),
};
const document={readyState:'complete',addEventListener:()=>{},querySelector:()=>null};
class EventSource {
  constructor(){active++;created++;maximum=Math.max(maximum,active);this.closed=false}
  addEventListener(){}
  close(){if(!this.closed){active--;this.closed=true}}
}
const context={window,document,EventSource,console,Map,Set,URL,URLSearchParams,fetch:()=>Promise.resolve({ok:true,json:()=>Promise.resolve({})})};
vm.runInNewContext(SOURCE,context);
const initial=[active,timers.size];
events.pagehide({persisted:true});const hidden=[active,timers.size];
events.pageshow({persisted:true});const resumed=[active,timers.size];
events.pageshow({persisted:true});const repeat=[active,timers.size];
events.pagehide({persisted:false});
console.log(JSON.stringify({initial,hidden,resumed,repeat,final:[active,timers.size],maximum,created}));
'''.replace('SOURCE',json.dumps(source))
    result = subprocess.run(['node','-e',harness],capture_output=True,text=True)
    assert result.returncode == 0, result.stderr
    state=json.loads(result.stdout)
    assert state == {'initial':[1,1],'hidden':[0,0],'resumed':[1,1],'repeat':[1,1],'final':[0,0],'maximum':1,'created':3}
