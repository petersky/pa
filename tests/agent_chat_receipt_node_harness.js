const fs = require('fs'), vm = require('vm'), assert = require('assert/strict');
const noop = () => {};
class Storage {
  constructor(){this.values=new Map();}
  get length(){return this.values.size;}
  key(i){return [...this.values.keys()][i] || null;}
  getItem(k){return this.values.get(k) || null;}
  setItem(k,v){this.values.set(k,String(v));}
  removeItem(k){this.values.delete(k);}
}
const storage=new Storage();
global.window={localStorage:storage,addEventListener:noop};
global.document={documentElement:{dataset:{paInstanceId:'owner',paPrincipalId:'user:local'}},body:{addEventListener:noop},addEventListener:noop,querySelector:()=>null,querySelectorAll:()=>[]};
for(const path of process.argv.slice(2)) vm.runInThisContext(fs.readFileSync(path,'utf8'));
const Widget=window.PAAgentChat.AgentChatWidget;
const deferred=()=>{let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return {promise,resolve,reject};};
const tick=()=>new Promise(setImmediate);
let sequence=0;
function make(){
 const w=Object.create(Widget.prototype),status={textContent:''};
 Object.assign(w,{sessionId:'receipt-test-'+(++sequence),ownerInstanceId:'owner',apiBase:'/api/agent',subscriptionGeneration:1,
  destroyed:false,sessionClosed:false,submissionPending:false,composerEnabled:true,pendingImages:[],
  els:{input:{value:'submitted',addEventListener:noop,setSelectionRange:noop},send:{disabled:false},form:{setAttribute:noop}},
  root:{dataset:{},isConnected:true,querySelector:s=>s==='[data-acw-draft-status]'?status:null,querySelectorAll:()=>[]},
  renderPendingImages:noop,commandInvocation:()=>null,addBubble:noop,refreshQueue:noop,resolveSessionNotLive:noop,
  seenEvents:{},lastSeq:0,isNearBottom:()=>false,_pruneMessageRows:noop,
  setTurnActive:noop,scrollToBottom:noop,_isDuplicateUserBubble:()=>false, statusElement:status,
 });
 w.drafts=window.PAAgentDrafts.installWidget(w);w.els.input.value='submitted';
 return w;
}
function settled(w,state){
 assert.equal(w.submissionPending,false);assert.equal(w.els.send.disabled,false);
 assert.equal(w.drafts.submissionId,null);assert.equal(w.drafts.pendingReceipt(),null);
 assert(!w.statusElement.textContent.includes('Checking'));if(state)assert.equal(w.submissionState,state);
}
(async()=>{
 // Real controller, real widget send/reconcile. SSE handler acknowledgements can
 // settle the receipt while the POST is still outstanding.
 for(const queued of [false,true]){
  const w=make(),post=deferred();w.apiWithTimeout=()=>post.promise;w.send();
  const id=w.drafts.submissionId;
  assert.equal(w.drafts.restoringSubmission,false);
  assert.equal(w.drafts.observeAcceptance('wrong-id',queued),false);
  w.handleEvent({type:queued?'queue_enqueued':'user_message',seq:1,payload:{id,message:'submitted'}},false,false);settled(w,queued?'queued':'accepted');
  assert.equal(w.els.input.value,'');
  assert.equal(w.statusElement.textContent,'Prompt accepted.');
  w.els.input.value='next';w.drafts.changed();
  post.resolve({accepted:true,queued});await tick();assert.equal(w.els.input.value,'next');settled(w);
  assert.equal(w.drafts.observeAcceptance(id,queued),false);
 }
 for(const reconnect of [false,true]){
  const w=make(),lookup=deferred();w.drafts.beginSubmission();w.apiWithTimeout=()=>lookup.promise;
  const pending=w.reconcilePendingSubmission();if(reconnect)w.subscriptionGeneration++;
  lookup.resolve({accepted:true,status:'completed',queued:false});await pending;
  settled(w,'completed');assert.equal(w.els.input.value,'');
 }
 for(const reconnect of [false,true]){
  const w=make(),post=deferred();w.apiWithTimeout=()=>post.promise;w.send();
  if(reconnect)w.subscriptionGeneration++;
  post.resolve({accepted:true,queued:false});await tick();settled(w);assert.equal(w.els.input.value,'');
 }
 // Completed durable status cannot regress to queued on a late HTTP receipt.
 const completed=make(),completedPost=deferred();completed.apiWithTimeout=()=>completedPost.promise;completed.send();
 completed.apiWithTimeout=()=>Promise.resolve({accepted:true,status:'completed'});
 await completed.reconcilePendingSubmission();settled(completed,'completed');
 assert.equal(completed.statusElement.textContent,'Prompt accepted.');
 completedPost.resolve({accepted:true,queued:true});await tick();settled(completed,'completed');
 assert.equal(completed.statusElement.textContent,'Prompt accepted.');
 // Preserve even an edit back to identical text; retire only submitted images.
 const w=make(),post=deferred(),oldImage={name:'old',data:'a'},newImage={name:'new',data:'b'};
 w.pendingImages=[oldImage];w.apiWithTimeout=()=>post.promise;w.send();const id=w.drafts.submissionId;
 w.els.input.value='new text';w.drafts.changed();w.els.input.value='submitted';w.drafts.changed();w.pendingImages.push(newImage);
 assert(w.drafts.observeAcceptance(id,false));settled(w);assert.equal(w.els.input.value,'submitted');assert.deepEqual(w.pendingImages,[newImage]);
 const next=deferred();w.apiWithTimeout=()=>next.promise;w.send();const nextId=w.drafts.submissionId;
 post.resolve({accepted:true});await tick();assert.equal(w.drafts.submissionId,nextId);assert.equal(w.submissionPending,true);
 next.resolve({accepted:true});await tick();settled(w);
 // Old lookup success AND failure cannot settle a newer send or another scope.
 for(const mutation of ['session','owner','api','principal','newer','destroy']) for(const fail of [false,true]){
  const w=make(),lookup=deferred();w.drafts.beginSubmission();w.apiWithTimeout=()=>lookup.promise;
  const pending=w.reconcilePendingSubmission();
  if(mutation==='session'){w.drafts.switchSession('different');w.sessionId='different';}
  if(mutation==='owner')w.ownerInstanceId='different';
  if(mutation==='api')w.apiBase='/other';
  if(mutation==='principal')w.drafts.principalId='different';
  if(mutation==='newer'){w.els.input.value='new';w.drafts.changed();w.drafts.beginSubmission();}
  if(mutation==='destroy')w.destroyed=true;
  const before={text:w.els.input.value,id:w.drafts.submissionId,state:w.submissionState};
  if(fail)lookup.reject(Error('offline'));else lookup.resolve({accepted:true,status:'completed'});
  await pending;assert.deepEqual({text:w.els.input.value,id:w.drafts.submissionId,state:w.submissionState},before);
 }
 for(const result of [null,{accepted:false,status:'unknown'},Error('offline')]){
  const w=make();const id=w.drafts.beginSubmission();w.apiWithTimeout=()=>result instanceof Error?Promise.reject(result):Promise.resolve(result);
  await w.reconcilePendingSubmission();assert.equal(w.submissionPending,false);assert.equal(w.submissionState,'retryable');assert.equal(w.drafts.submissionId,id);assert.equal(w.els.input.value,'submitted');
  assert.equal(w.drafts.beginSubmission(),id);
 }
 const lost=make();let count=0;lost.apiWithTimeout=()=>++count===1?Promise.reject(Error('lost HTTP ack')):Promise.resolve({accepted:true,status:'completed'});
 lost.send();await tick();settled(lost,'completed');assert.equal(count,2);
 // Snapshot acknowledgement also settles an active (not restored) submission.
 for(const queued of [false,true]){
  const w=make(),id=w.drafts.beginSubmission();
  w.drafts.onSnapshot(queued?{queue:[{id}]}:{transcript:[{type:'user_message',payload:{id}}]});
  settled(w);assert.equal(w.els.input.value,'');
 }
 // A delayed POST must obey the same scope fences as a status lookup.
 for(const mutation of ['session','owner','api','principal','newer']){
  const w=make(),post=deferred();w.apiWithTimeout=()=>post.promise;w.send();
  if(mutation==='session'){w.drafts.switchSession('different');w.sessionId='different';}
  if(mutation==='owner')w.ownerInstanceId='different';
  if(mutation==='api')w.apiBase='/other';
  if(mutation==='principal')w.drafts.principalId='different';
  if(mutation==='newer'){w.els.input.value='new';w.drafts.changed();w.drafts.beginSubmission();}
  const before={text:w.els.input.value,id:w.drafts.submissionId,state:w.submissionState};
  post.resolve({accepted:true});await tick();assert.deepEqual({text:w.els.input.value,id:w.drafts.submissionId,state:w.submissionState},before);
 }
 // Explicit clear retires its receipt and controls, fencing the late reply.
 const cleared=make(),clearAck=deferred();cleared.apiWithTimeout=()=>clearAck.promise;cleared.send();cleared.drafts.clear(true);
 cleared.els.input.value='after clear';cleared.drafts.changed();clearAck.resolve({accepted:true});await tick();settled(cleared);assert.equal(cleared.els.input.value,'after clear');
 // A definitive fingerprint conflict rejects this attempt, even if the ID
 // belongs to an accepted older payload. Neither SSE nor a late lookup may
 // retire any of the rejected text, binary images, or metadata.
 const conflict=make(),conflictPost=deferred(),conflictLookup=deferred();
 const rejectedImage={name:'rejected.png',data:'rejected-bytes'};
 const rejectedMetadata={name:'rejected.png',mime_type:'image/png',size:14};
 conflict.pendingImages=[rejectedImage];conflict.drafts.attachmentMetadata=[rejectedMetadata];
 conflict.apiWithTimeout=()=>conflictPost.promise;conflict.send();
 const rejectedId=conflict.drafts.submissionId;
 conflict.apiWithTimeout=()=>conflictLookup.promise;
 const rejectedLookup=conflict.reconcilePendingSubmission();
 const rejection=Error('fingerprint conflict');rejection.status=409;
 rejection.detail={code:'client_prompt_id_conflict'};
 conflictPost.reject(rejection);await tick();
 assert.equal(conflict.drafts.pendingReceipt(),null);
 assert.equal(conflict.drafts.observeAcceptance(rejectedId,true),false);
 conflictLookup.resolve({accepted:true,status:'completed'});await rejectedLookup;
 settled(conflict,'failed');assert.equal(conflict.els.input.value,'submitted');
 assert.deepEqual(conflict.pendingImages,[rejectedImage]);
 assert.deepEqual(conflict.drafts.attachmentMetadata,[rejectedMetadata]);

 // A late A transport failure must not start a lookup or change B's controls.
 const late=make(),aPost=deferred(),bPost=deferred(),aLookup=deferred();
 late.apiWithTimeout=()=>aPost.promise;late.send();
 late.apiWithTimeout=()=>aLookup.promise;const aReconcile=late.reconcilePendingSubmission();
 late.drafts.observeAcceptance(late.drafts.submissionId,false);
 late.els.input.value='B';late.drafts.changed();
 let bRequests=0;late.apiWithTimeout=()=>{bRequests++;return bPost.promise;};late.send();
 const bId=late.drafts.submissionId;aPost.reject(Error('late A network failure'));
 aLookup.resolve({accepted:true,status:'completed'});await aReconcile;await tick();
 assert.equal(bRequests,1);assert.equal(late.drafts.submissionId,bId);
 assert.equal(late.els.input.value,'B');assert.equal(late.submissionPending,true);
 assert.equal(late.submissionState,'sending');bPost.resolve({accepted:true});await tick();settled(late);

 // Restored A edited away and back to identical text is still a newer draft.
 const editedRestore=make(),newImageAfterRestore={name:'new.png',data:'new-bytes'};
 const newMetadataAfterRestore={name:'new.png',mime_type:'image/png',size:9};
 editedRestore.drafts.apply({text:'restored A',submission_id:'restored-edit-id',attachments:[{name:'old.png'}]});
 editedRestore.els.input.value='different';editedRestore.drafts.changed();
 editedRestore.els.input.value='restored A';editedRestore.drafts.changed();
 editedRestore.pendingImages=[newImageAfterRestore];
 editedRestore.drafts.attachmentMetadata.push(newMetadataAfterRestore);
 assert.equal(editedRestore.drafts.observeAcceptance('restored-edit-id',false),true);
 settled(editedRestore);assert.equal(editedRestore.els.input.value,'restored A');
 assert.deepEqual(editedRestore.pendingImages,[newImageAfterRestore]);
 assert.deepEqual(editedRestore.drafts.attachmentMetadata,[newMetadataAfterRestore]);

 // Switching away and back creates a fresh restored receipt, even for the
 // same durable ID; the prior request cannot settle that new scope.
 const returned=make(),oldPost=deferred();returned.apiWithTimeout=()=>oldPost.promise;returned.send();
 const originalSession=returned.sessionId;
 returned.drafts.switchSession('away');returned.sessionId='away';
 returned.drafts.switchSession(originalSession);returned.sessionId=originalSession;
 const returnedReceipt=returned.drafts.pendingReceipt();
 returned.apiWithTimeout=()=>new Promise(noop);
 oldPost.resolve({accepted:true});await tick();
 assert.equal(returned.drafts.pendingReceipt(),returnedReceipt);
 assert.equal(returned.els.input.value,'submitted');assert.equal(returned.submissionState,'checking');
 returned.drafts.observeAcceptance(returnedReceipt.id,false);settled(returned);

 // Receipt settlement is monotonic when a slower lookup subsequently fails.
 const monotonic=make(),lateLookup=deferred();monotonic.drafts.beginSubmission();
 monotonic.apiWithTimeout=()=>lateLookup.promise;const looking=monotonic.reconcilePendingSubmission();
 monotonic.drafts.observeAcceptance(monotonic.drafts.submissionId,false);
 lateLookup.reject(Error('late lookup failure'));await looking;settled(monotonic,'accepted');
 // Applying a newer storage draft releases the old pending controls as well.
 const storageDraft=make(),storageAck=deferred();storageDraft.apiWithTimeout=()=>storageAck.promise;storageDraft.send();
 storageDraft.drafts.apply({text:'from another tab',attachments:[]});storageAck.resolve({accepted:true});await tick();settled(storageDraft);assert.equal(storageDraft.els.input.value,'from another tab');
 // A response naming another durable identity is uncertainty, not acceptance.
 for(const field of ['prompt_id','session_id']){
  const w=make();const id=w.drafts.beginSubmission();w.apiWithTimeout=()=>Promise.resolve({accepted:true,status:'completed',[field]:'other'});
  await w.reconcilePendingSubmission();assert.equal(w.drafts.submissionId,id);assert.equal(w.els.input.value,'submitted');assert.equal(w.submissionState,'retryable');assert.equal(w.submissionPending,false);
 }
 // Repeated gestures stay bounded, transient rejection keeps its stable ID.
 const retry=make(),request=deferred();let sends=0;
 retry.apiWithTimeout=()=>{sends++;return request.promise;};retry.send();const retryId=retry.drafts.submissionId;retry.send();retry.send();assert.equal(sends,1);
 const error=Error('restarting');error.status=409;error.detail={code:'session_not_live'};request.reject(error);await tick();
 assert.equal(retry.submissionPending,false);assert.equal(retry.drafts.submissionId,retryId);
 // Restored metadata is retired on authoritative acceptance, never attachment bytes.
 const restored=make();restored.drafts.apply({text:'restored',submission_id:'restored-id',attachments:[{name:'old.png'}]});
 restored.apiWithTimeout=()=>Promise.resolve({accepted:true,status:'completed'});await restored.reconcilePendingSubmission();settled(restored,'completed');assert.deepEqual(restored.drafts.attachmentMetadata,[]);
 // Escaped slash retains its original draft identity while sending a literal slash.
 const slash=make(),slashAck=deferred();slash.els.input.value='//literal';slash.apiWithTimeout=(path,timeout,options)=>{assert.equal(JSON.parse(options.body).message,'/literal');return slashAck.promise;};slash.send();slashAck.resolve({accepted:true});await tick();assert.equal(slash.els.input.value,'');settled(slash);
 console.log('PASS current/restored receipts, reconnect, late acknowledgements, edits/attachments, scope fencing, retryable uncertainty');
})().catch(e=>{console.error(e);process.exitCode=1;});
