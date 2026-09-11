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
let virtualTimers = null, nextTimer = 0;
const schedule = (callback, delay) => {
 if (!virtualTimers) return setTimeout(callback, delay);
 const id = ++nextTimer; virtualTimers.set(id, callback); return id;
};
const cancelTimer = id => virtualTimers ? virtualTimers.delete(id) : clearTimeout(id);
vm.runInNewContext(fs.readFileSync('src/pa/server/static/js/agent-chat.js','utf8'), {window,document,console,URL,AbortController,setTimeout:schedule,clearTimeout:cancelTimer,setInterval,clearInterval,performance});
const Widget=window.PAAgentChat.AgentChatWidget;
const input=JSON.parse(fs.readFileSync(0,'utf8'));
function make(){
 const w=Object.create(Widget.prototype), messages=new Element();
 Object.assign(w,{els:{messages,loadOlder:new Element('button'),loadNewer:new Element('button'),loadNewerStatus:new Element(),input:{value:'UNSENT gap draft'}},root:new Element(),sessionId:'s',apiBase:'/api/agent',subscriptionGeneration:1,transcriptEvents:[],seenEvents:{},lastSeq:0,streaming:{},activityStreams:{},toolTimers:{},plans:[],providerId:'codex',messageRowCount:0,rawText:true,resetArtifacts:noop,clearPlaceholder:noop,setPlaceholder:noop,isNearBottom:()=>true,scrollToBottom:noop,finalizeActivity:noop,upsertTool:noop,setTurnActive(active){this.prompting=active},setStatus:noop,renderQueue:noop,renderMetrics:noop});
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
 // The final can be live before a contended durable write becomes readable.
 // Keep that exact final, retry the missing predecessor automatically, and do
 // not require either another event or a user refresh to complete the bubble.
 const delayed=make();delayed.lastSeq=10;let historyReads=0;
 delayed.api=()=>Promise.resolve({events:++historyReads===1?[]:[
   {seq:11,type:'agent_message_chunk',payload:{message_id:'late-write',content_mode:'delta',text:'Progress.'}}
 ]});
 delayed.handleEvent({seq:12,type:'agent_message_chunk',payload:{message_id:'late-write',content_mode:'delta',text:'Final.'}});
 await new Promise(r=>setTimeout(r,2150));
 assert.strictEqual(historyReads,2,'a late write is retried automatically');
 assert.strictEqual(delayed.lastSeq,12,'retained final advances the contiguous cursor');
 assert.deepStrictEqual(texts(delayed),['Progress.Final.'],'exact live text survives history lag');
 delayed.handleEvent({seq:12,type:'agent_message_chunk',payload:{message_id:'late-write',content_mode:'delta',text:'Final.'}});
 assert.deepStrictEqual(texts(delayed),['Progress.Final.'],'replayed final is not duplicated');
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
 // Recovery races use controlled timers and deferred HTTP, with real DOM controls.
 virtualTimers = new Map();
 const settle = () => new Promise(resolve => setImmediate(resolve));
 const chunk = (seq, text) => ({seq,type:'agent_message_chunk',payload:{message_id:'race',content_mode:'delta',text}});
 const warning = widget => {
   widget.newerError='Live messages are waiting for history. Retrying…';
   widget.updateNewerControl();
   assert.strictEqual(widget.els.loadNewerStatus.hidden,false);
   assert.strictEqual(widget.els.loadNewer.hidden,false);
 };
 const cleared = widget => {
   assert.strictEqual(widget.newerError,'');
   assert.strictEqual(widget.els.loadNewerStatus.textContent,'');
   assert.strictEqual(widget.els.loadNewerStatus.hidden,true);
   assert.strictEqual(widget.els.loadNewer.hidden,true);
   assert.strictEqual(widget.liveGapRetryTimer,null);
   assert.strictEqual(widget.liveGapRetryCount,0);
   assert.strictEqual(widget.els.input.value,'UNSENT gap draft');
 };
 const satisfied=make();satisfied.lastSeq=21;satisfied.liveGapTarget=21;
 let unnecessaryReads=0;satisfied.apiWithTimeout=()=>{unnecessaryReads++;return Promise.resolve({events:[]})};
 warning(satisfied);satisfied.liveGapRetryTimer=schedule(()=>satisfied._repairLiveGap(),2000);
 const obsoleteTimer=virtualTimers.get(satisfied.liveGapRetryTimer);
 satisfied._repairLiveGap();cleared(satisfied);
 obsoleteTimer();await settle();cleared(satisfied);
 assert.strictEqual(unnecessaryReads,0,'already satisfied and delayed retries do not fetch history');
 assert.strictEqual(virtualTimers.size,0,'resolved recovery cancels obsolete timers');
 // An empty or failed response arriving after SSE catchup cannot resurrect the warning.
 for(const failure of [false,true]){
   const race=make();race.lastSeq=10;let resolve,reject;
   race.apiWithTimeout=()=>new Promise((a,b)=>{resolve=a;reject=b});
   race.handleEvent(chunk(12,'B'));warning(race);
   race.liveGapRetryTimer=schedule(()=>race._repairLiveGap(),2000);
   race.handleEvent(chunk(11,'A'));race.handleEvent(chunk(12,'B'));
   cleared(race);assert.deepStrictEqual(texts(race),['AB']);
   if(failure)reject(new Error('late network failure'));else resolve({events:[],page:{has_newer:false}});
   await settle();cleared(race);assert.strictEqual(race.liveGapLoading,false);
   race.handleEvent(chunk(12,'B'));assert.deepStrictEqual(texts(race),['AB']);
 }
 // A real empty history still retries, retains the final, and removes the rendered
 // warning only after the missing predecessor is available.
 const genuine=make();genuine.lastSeq=10;let reads=0;
 genuine.apiWithTimeout=()=>Promise.resolve({events:++reads===1?[]:[chunk(11,'A')]});
 genuine.handleEvent(chunk(12,'B'));await settle();
 assert.strictEqual(genuine.els.loadNewerStatus.hidden,false);
 assert.strictEqual(genuine.els.loadNewer.hidden,false);
 assert.strictEqual(genuine.livePendingEvents[12].payload.text,'B');
 assert.strictEqual(genuine.els.input.value,'UNSENT gap draft');
 const retryId=genuine.liveGapRetryTimer, retry=virtualTimers.get(retryId);
 virtualTimers.delete(retryId);retry();await settle();
 assert.strictEqual(reads,2);cleared(genuine);assert.deepStrictEqual(texts(genuine),['AB']);
 // Stale owner/session/destroyed responses and timers cannot alter current state.
 for(const change of ['owner','session','destroyed']){
   const stale=make();stale.lastSeq=10;let finish;
   stale.apiWithTimeout=()=>new Promise(resolve=>{finish=resolve});
   stale.handleEvent(chunk(12,'old'));
   if(change==='owner')stale.apiBase='/api/fleet/new/agent';
   if(change==='session'){stale.sessionId='other';stale.subscriptionGeneration++;}
   if(change==='destroyed')stale.destroyed=true;
   warning(stale);stale.liveGapRetryTimer=12345;stale.liveGapLoading=true;
   finish({events:[chunk(11,'stale'),chunk(12,'old')]});await settle();
   assert.strictEqual(stale.lastSeq,10);assert.deepStrictEqual(texts(stale),[]);
   assert.strictEqual(stale.liveGapRetryTimer,12345);assert.strictEqual(stale.liveGapLoading,true);
   assert.strictEqual(stale.els.loadNewerStatus.hidden,false);
 }
 // A queued callback from an obsolete owner cannot erase the new retry timer.
 const timerOwner=make();timerOwner.lastSeq=10;
 timerOwner.apiWithTimeout=()=>Promise.resolve({events:[]});
 timerOwner.handleEvent(chunk(12,'B'));await settle();
 const oldTimer=timerOwner.liveGapRetryTimer, oldCallback=virtualTimers.get(oldTimer);
 timerOwner.apiBase='/api/fleet/new/agent';timerOwner._repairLiveGap();await settle();
 const newTimer=timerOwner.liveGapRetryTimer;
 assert.notStrictEqual(newTimer,oldTimer);oldCallback();
 assert.strictEqual(timerOwner.liveGapRetryTimer,newTimer);
 assert.ok(virtualTimers.has(newTimer),'old owner callback preserves new owner retry');
 timerOwner.handleEvent(chunk(11,'A'));timerOwner.handleEvent(chunk(12,'B'));cleared(timerOwner);
 // Once live recovery has retired, its late response cannot hide paging failure.
 const paging=make();paging.lastSeq=10;let finishGap;
 paging.apiWithTimeout=()=>new Promise(resolve=>{finishGap=resolve});
 paging.handleEvent(chunk(12,'B'));paging.handleEvent(chunk(11,'A'));paging.handleEvent(chunk(12,'B'));
 paging.newerError='Could not load newer messages: paging failed';paging.updateNewerControl();
 finishGap({events:[]});await settle();
 assert.strictEqual(paging.els.loadNewerStatus.textContent,'Could not load newer messages: paging failed');
 assert.strictEqual(paging.els.loadNewerStatus.hidden,false);
 // New gaps raised during an older read remain recoverable after its empty response.
 const advanced=make();advanced.lastSeq=10;const responses=[];
 advanced.apiWithTimeout=()=>new Promise(resolve=>responses.push(resolve));
 advanced.handleEvent(chunk(12,'B'));advanced.handleEvent(chunk(11,'A'));advanced.handleEvent(chunk(12,'B'));
 advanced.handleEvent(chunk(14,'D'));responses[0]({events:[]});await settle();
 assert.strictEqual(responses.length,2,'new target survives earlier catchup completion');
 responses[1]({events:[chunk(13,'C')]});await settle();
 cleared(advanced);assert.deepStrictEqual(texts(advanced),['ABCD']);
 assert.strictEqual(virtualTimers.size,0);
 virtualTimers=null;
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
