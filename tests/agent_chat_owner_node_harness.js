const fs = require('fs'), vm = require('vm'), assert = require('assert/strict');
const noop = () => {};
const document = {body:{addEventListener:noop},addEventListener:noop,querySelector:()=>null,querySelectorAll:()=>[]};
const window = {addEventListener:noop};
class EventSource { static CLOSED=2; constructor(){this.readyState=1;} close(){this.readyState=2;} addEventListener(){} }
vm.runInNewContext(fs.readFileSync(process.argv[2], 'utf8'), {window, document, EventSource, console, URL, setTimeout, clearTimeout});
const Widget=window.PAAgentChat.AgentChatWidget;
const deferred=()=>{let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return {promise,resolve,reject};};
const make=()=>{
 const widget=Object.create(Widget.prototype);
 Object.assign(widget,{destroyed:false,subscriptionGeneration:0,apiBase:'/api/agent',root:{dataset:{},closest:()=>null},els:{},drafts:null,
  _setRecoveryControl:noop,showRecoveryActions:noop,renderSessionActions:noop,setComposerEnabled:noop,setPlaceholder:noop,setStatus:noop,
  retryAfterStartupRecovery:()=>false,_scheduleLiveStateRetry:noop,_applyDurableHistory:noop,
  apiWithTimeout:()=>Promise.resolve({events:[]}),_loadLiveSnapshot:async(id)=>{widget.loaded=id;},
 });return widget;
};
(async()=>{
 for(const fail of [false,true]){
  const w=make(), route=deferred();w.resolveSessionRoute=()=>route.promise;
  const pending=w.openSession('first','');
  w.useExternalEventTransport(true);w.useExternalEventTransport(false);w.useExternalEventTransport(true);
  if(fail)route.reject(Error('owner timeout'));else route.resolve({live:true});
  await pending;
  assert.equal(w.ownerResolutionPending,false,'transport changes must settle owner lookup');
  assert.equal(fail?w.sessionRoute.state:w.loaded, fail?'owner_unreachable':'first');
 }
 for(const fail of [false,true]){
  const w=make(), old=deferred(), next=deferred();w.resolveSessionRoute=id=>id==='old'?old.promise:next.promise;
  const a=w.openSession('old',''),b=w.openSession('next','');
  if(fail)old.reject(Error('old failure'));else old.resolve({live:true});
  await a;assert.equal(w.ownerResolutionPending,true);assert.equal(w.loaded,undefined);
  next.resolve({live:true});await b;assert.equal(w.loaded,'next');
 }
 // Owner failure can already be in its delayed history fallback when selection changes.
 const w=make(),history=deferred();w.resolveSessionRoute=id=>id==='old'?Promise.reject(Error('offline')):Promise.resolve({live:true});
 w.apiWithTimeout=()=>history.promise;
 const old=w.openSession('old','');await new Promise(r=>setImmediate(r));
 await w.openSession('new','');history.resolve({events:[]});await old;
 assert.equal(w.sessionRoute.live,true);assert.equal(w.loaded,'new');
 const destroyed=make(),route=deferred();destroyed.resolveSessionRoute=()=>route.promise;
 const pending=destroyed.openSession('gone','');destroyed.destroyed=true;destroyed.closeSSE('destroy');route.resolve({live:true});await pending;
 assert.equal(destroyed.loaded,undefined);
 console.log('PASS delayed owner success/failure, reconnect, switching, stale history fallback, destruction');
})().catch(e=>{console.error(e);process.exitCode=1;});
