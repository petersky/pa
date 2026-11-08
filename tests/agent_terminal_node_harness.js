const assert = require('assert');
require('../src/pa/server/static/js/agent-terminal.js');
const {project, truncate, render} = global.PAAgentTerminal;
const event = (seq, p, type='tool_call_update') => ({seq,session_id:'s',type,payload:{tool_call_id:'call',turn_id:'t',external_session_id:'native',...p}});
const start = event(1,{title:'shell',raw_input:{command:'echo hello'},status:'in_progress'},'tool_call');
const delta = event(2,{raw:{_meta:{terminal_output_delta:{data:'<script>alert(1)</script>',terminal_id:'call'}}}});
const end = event(3,{status:'completed',raw_output:{exit_code:0}});
const events=[end,delta,start,delta,{...end,session_id:'other'},event(4,{turn_id:'t2',title:'second',status:'failed',content:[{type:'content',content:{type:'image',data:'not-rendered'}}]})];
const calls=project(events,'s');
assert.equal(calls.length,2);
assert.equal(calls[0].name,'shell');assert.equal(calls[0].status,'completed');
assert.equal(calls[0].output,'<script>alert(1)</script>\n{\n  "exit_code": 0\n}');
assert.ok(calls[1].output.includes('Non-text'));assert.ok(!calls[1].output.includes('not-rendered'));
assert.deepEqual(project(JSON.parse(JSON.stringify(events)),'s'),calls);
assert.equal(truncate('😀'.repeat(1001),1000),'😀'.repeat(1000)+'\n… [truncated: 1001 characters; showing 1000]');
class El {constructor(){this.children=[];this.textContent='';}appendChild(c){this.children.push(c);}replaceChildren(){this.children=[];}set innerHTML(v){throw Error('HTML execution sink');}}
global.document={createElement:()=>new El()};
const container=new El();render(container,events,'s',1000);assert.equal(container.children.length,2);
assert.equal(events[1].payload.raw._meta.terminal_output_delta.data,'<script>alert(1)</script>');

assert.equal(project([event(1,{status:"completed"}),event(2,{status:"pending",title:"late start"},"tool_call")],"s")[0].status,"completed");

// Windowed rendering: only the most recent calls are built; the caller learns what is hidden.
{
  class Btn extends El {constructor(){super();this.attributes={};}setAttribute(k,v){this.attributes[k]=v;}}
  global.document={createElement:()=>new Btn()};
  const many=[];for(let i=1;i<=120;i++)many.push({seq:i,session_id:'s',type:'tool_call',payload:{tool_call_id:'c'+i,turn_id:'t',external_session_id:'native',title:'shell '+i,status:'completed'}});
  const box=new Btn();
  const info=render(box,many,'s',1000,{max:50,hasOlder:true});
  assert.deepEqual(info,{total:120,shown:50,hidden:70});
  assert.equal(box.children.length,51,'an earlier-calls control plus the last 50 calls');
  assert.equal(box.children[0].className,'ghost small acw-terminal-earlier');
  assert.ok(box.children[0].textContent.includes('70 more loaded'));
  assert.equal(box.children[1].children[0].textContent,'shell 71 · completed');
  assert.equal(box.children.at(-1).children[0].textContent,'shell 120 · completed');
  // Everything shown and nothing older: no control. Older server history only: a load control.
  assert.equal(render(new Btn(),many.slice(0,3),'s',1000,{max:50}).hidden,0);
  const small=new Btn();render(small,many.slice(0,3),'s',1000,{max:50});assert.equal(small.children.length,3);
  const more=new Btn();render(more,many.slice(0,3),'s',1000,{max:50,hasOlder:true});
  assert.equal(more.children.length,4);assert.equal(more.children[0].textContent,'Load earlier history');
  // No window given: unchanged behaviour (all calls).
  assert.equal(render(new Btn(),many,'s',1000).shown,120);
}
console.log('terminal ok');
