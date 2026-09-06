const fs = require('fs'), vm = require('vm'), assert = require('assert');
const noop = () => {};
class Element {
  constructor(tag='div') { this.tagName=tag;this.children=[];this.dataset={};this.attributes={};this.classList={add:noop,toggle:noop,contains:()=>false};this.scrollTop=0;this.scrollHeight=0;this.clientHeight=100;this.hidden=false; }
  appendChild(child){child.parentNode=this;this.children.push(child);return child;}
  remove(){if(this.parentNode)this.parentNode.children=this.parentNode.children.filter(c=>c!==this);}
  hasAttribute(key){return key in this.attributes;}
  setAttribute(key,value){this.attributes[key]=value;}
  querySelectorAll(selector){const all=this.children.flatMap(c=>[c,...(c.querySelectorAll?c.querySelectorAll('*'):[])]);if(selector==='*')return all;return all.filter(c=>selector.startsWith('.')?(c.className||'').split(' ').includes(selector.slice(1)):c.tagName===selector);}
  querySelector(selector){return this.querySelectorAll(selector)[0]||null;}
  cloneNode(){return this;}
  get outerHTML(){return '';}
}
const document={createElement:tag=>new Element(tag),createTextNode:text=>({textContent:text}),body:{addEventListener:noop},addEventListener:noop,querySelector:()=>null,querySelectorAll:()=>[]};
const window={addEventListener:noop};
vm.runInNewContext(fs.readFileSync('src/pa/server/static/js/agent-chat.js','utf8'), {window,document,console,URL,AbortController,setTimeout,clearTimeout,setInterval,clearInterval,performance});
const Widget=window.PAAgentChat.AgentChatWidget;
const input=JSON.parse(fs.readFileSync(0,'utf8'));
function make(){
 const w=Object.create(Widget.prototype), messages=new Element();
 Object.assign(w,{els:{messages,loadOlder:new Element('button'),loadNewer:new Element('button')},root:new Element(),sessionId:'s',apiBase:'/api/agent',subscriptionGeneration:1,transcriptEvents:[],seenEvents:{},lastSeq:0,streaming:{},activityStreams:{},toolTimers:{},plans:[],providerId:'codex',messageRowCount:0,rawText:true,resetArtifacts:noop,clearPlaceholder:noop,setPlaceholder:noop,isNearBottom:()=>true,scrollToBottom:noop,finalizeActivity:noop,upsertTool:noop,setTurnActive(active){this.prompting=active},setStatus:noop,renderQueue:noop,renderMetrics:noop});
 return w;
}
const texts=w=>w.els.messages.querySelectorAll('.acw-bubble-agent').map(b=>b.dataset.markdown);
(async()=>{
 const w=make();
 w.renderTranscript(input.events,{scrollBottom:true});
 assert.ok(texts(w).includes(input.expected),'raw 396 chunks render exactly');
 assert.ok(w.transcriptEvents.length<3100,'consecutive chunks compact before retention');
 const final=w.transcriptEvents.find(e=>e.payload.message_id==='final-396');
 assert.strictEqual(final.payload.text,input.expected);
 w._stashSessionDomCache();
 w.renderTranscript([]);
 assert.ok(w._restoreSessionDomCache('s'));
 assert.ok(texts(w).includes(input.expected),'cache restores exact message');
 const start=w.lastSeq;
 w.handleEvent({seq:start+1,type:'agent_message_chunk',payload:{message_id:'live',phase:'commentary',content_mode:'delta',text:'Comment.'}});
 w.handleEvent({seq:start+2,type:'agent_message_chunk',payload:{message_id:'live',phase:'final',content_mode:'delta',text:'Final.'}});
 w.handleEvent({seq:start+3,type:'tool_call',payload:{tool_call_id:'tool'}});
 w.handleEvent({seq:start+4,type:'agent_message_chunk',payload:{message_id:'live',phase:'final',content_mode:'delta',text:'Next'}});
 assert.ok(texts(w).includes('Comment.'));
 assert.ok(texts(w).includes('Final.Next'),'phase and keyed tool boundaries preserve exact text');
 w.handleEvent({seq:start+5,type:'agent_message_chunk',payload:{message_id:'live',phase:'final',content_mode:'snapshot',text:'Replacement.A'}});
 assert.ok(texts(w).includes('Replacement.A'));
 w._stashSessionDomCache();w.renderTranscript([]);w._restoreSessionDomCache('s');
 w.handleEvent({seq:start+6,type:'agent_message_chunk',payload:{message_id:'live',phase:'final',content_mode:'delta',text:'B'}});
 assert.ok(texts(w).includes('Replacement.AB'),'cache restores stream continuation ownership');
 w.prompting=true;w.renderTranscript(w.transcriptEvents);
 assert.strictEqual(w.prompting,true,'historical completed turn cannot end current turn');
 // Exact out-of-order recovery: the late event is not allowed to skip missing text.
 const g=make();g.lastSeq=10;
 let resolveHistory;
 g.api=()=>new Promise(resolve=>{resolveHistory=resolve});
 g.handleEvent({seq:12,type:'agent_message_chunk',payload:{message_id:'gap',content_mode:'delta',text:'B'}});
 assert.strictEqual(g.lastSeq,10);
 resolveHistory({events:[{seq:11,type:'agent_message_chunk',payload:{message_id:'gap',content_mode:'delta',text:'A'}},{seq:12,type:'agent_message_chunk',payload:{message_id:'gap',content_mode:'delta',text:'B'}}]});
 await new Promise(r=>setTimeout(r,10));
 assert.strictEqual(g.lastSeq,12);assert.deepStrictEqual(texts(g),['AB']);
 g.handleEvent({seq:11,type:'agent_message_chunk',payload:{message_id:'gap',content_mode:'delta',text:'A'}});
 assert.deepStrictEqual(texts(g),['AB']);
 // Transport replacement preserves valid history; a new selection cancels it.
 const reconnect=make();reconnect.lastSeq=10;
 const pending=[];reconnect.api=()=>new Promise(resolve=>pending.push(resolve));
 reconnect.handleEvent({seq:12,type:'agent_message_chunk',payload:{message_id:'gap',content_mode:'delta',text:'B'}});
 const selectedGeneration=reconnect.subscriptionGeneration;
 reconnect.closeSSE('fixture-reconnect');
 assert.strictEqual(reconnect.subscriptionGeneration,selectedGeneration,'transport preserves selection generation');
 reconnect.handleEvent({seq:12,type:'agent_message_chunk',payload:{message_id:'gap',content_mode:'delta',text:'B'}});
 assert.strictEqual(pending.length,1,'transport replacement coalesces valid gap history');
 reconnect.subscriptionGeneration += 1; // openSession establishes a new selection lifetime.
 reconnect.handleEvent({seq:12,type:'agent_message_chunk',payload:{message_id:'gap',content_mode:'delta',text:'B'}});
 assert.strictEqual(pending.length,2,'new generation can repair its missing events');
 pending[0]({events:[{seq:12,type:'agent_message_chunk',payload:{message_id:'gap',content_mode:'snapshot',text:'obsolete'}}]});
 await new Promise(r=>setTimeout(r,10));
 assert.strictEqual(reconnect.lastSeq,10);
 assert.strictEqual(reconnect.liveGapLoading,true,'stale response cannot clear current request');
 pending[1]({events:[{seq:12,type:'agent_message_chunk',payload:{message_id:'gap',content_mode:'snapshot',text:'AB'}}]});
 await new Promise(r=>setTimeout(r,10));
 assert.deepStrictEqual(texts(reconnect),['AB']);
 // A failed earlier page must not consume its cursor or mutate visible history.
 const before=texts(w).join('\n');w.hasOlder=true;w.olderCursor=1000;w.api=()=>Promise.reject(new Error('fixture failure'));
 w.loadOlderTranscript();await new Promise(r=>setTimeout(r,10));
 assert.strictEqual(w.olderCursor,1000);assert.strictEqual(texts(w).join('\n'),before);assert.ok(w.olderError);assert.strictEqual(w.els.loadOlder.hidden,false);
 // A fetch that never settles has a bounded, retryable failure.
 w.api=()=>new Promise(()=>{});
 w.apiWithTimeout=(path,_budget,opts)=>Widget.prototype.apiWithTimeout.call(w,path,20,opts);
 w.olderError='';w.loadOlderTranscript();await new Promise(r=>setTimeout(r,40));
 assert.strictEqual(w.loadingOlder,false);assert.ok(w.olderError);assert.strictEqual(w.olderCursor,1000);assert.strictEqual(texts(w).join('\n'),before);
 // A completion delivered during delayed paging wins over historical replay.
 let deliverPage;w.api=()=>new Promise(resolve=>{deliverPage=resolve});
 w.apiWithTimeout=Widget.prototype.apiWithTimeout;w.prompting=true;w.loadOlderTranscript();
 w.handleEvent({seq:w.lastSeq+1,type:'turn_completed',payload:{}});
 deliverPage({events:[{seq:1,type:'turn_completed',payload:{}}],page:{has_older:false,has_newer:true,oldest_seq:1,newest_seq:1}});
 await new Promise(r=>setTimeout(r,10));assert.strictEqual(w.prompting,false);
 // A failed recent history fetch preserves a previously visible final.
 const existing=texts(w).join('\n');w.api=()=>Promise.reject(new Error('history unavailable'));
 await w._loadRecentHistory('s',1).catch(()=>{});
 assert.strictEqual(texts(w).join('\n'),existing);assert.ok(w.olderError);
 // Live eviction advertises the recoverable older range immediately.
 const m=make();m.sessionId='';
 for(let seq=1;seq<=2200;seq++)m.handleEvent({seq,type:'tool_call_update',payload:{}});
 assert.ok(m.hasOlder);assert.strictEqual(m.els.loadOlder.hidden,false);assert.ok(m.olderCursor>1);
 // Interleaved live chunks retain the prefix through a bounded metadata window.
 const long=make();
 for(let index=0;index<1500;index++){
   long.handleEvent({seq:index*2+1,type:'agent_message_chunk',payload:{message_id:'long',phase:'final',content_mode:'delta',text:'x'}});
   long.handleEvent({seq:index*2+2,type:'tool_call_update',payload:{}});
 }
 long._stashSessionDomCache();long.renderTranscript([]);long._restoreSessionDomCache('s');
 assert.ok(texts(long).includes('x'.repeat(1500)),'interleaved chunks survive cache reconstruction');
 // DOM eviction must also retire cached rows, so older paging can restore them.
 const rows=make();rows.sessionId='';
 const messages=[];
 for(let index=0;index<650;index++){
   messages.push({seq:index*2+1,type:'agent_message_chunk',payload:{message_id:'row-'+index,phase:'final',content_mode:'delta',text:'Message '+index}});
   messages.push({seq:index*2+2,type:'turn_completed',payload:{}});
 }
 rows.renderTranscript(messages);
 assert.strictEqual(texts(rows).length,600);
 assert.ok(!texts(rows).includes('Message 0'));
 rows.prependTranscript(messages.slice(0,100));
 assert.ok(texts(rows).includes('Message 0'),'older paging restores DOM-evicted rows');
 assert.ok(rows.hasNewer);
 assert.ok(rows.newerCursor<1300,'forward cursor does not skip DOM-evicted newer rows');
 rows.renderTranscript(rows.transcriptEvents.concat(messages.filter(e=>e.seq>rows.newerCursor)));
 assert.ok(texts(rows).includes('Message 649'),'forward paging restores newest row');
 console.log('PASS exact rendering, phases, snapshots, cache continuation, lifecycle replay, ordered gap repair, failed paging, live history control');
})().catch(error=>{console.error(error);process.exitCode=1});
