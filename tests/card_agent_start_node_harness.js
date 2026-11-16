const fs = require('fs'), vm = require('vm'), assert = require('assert/strict');
const noop = () => {};
const listeners = {}, timers = new Map(); let nextTimer = 0, navigated = [];
const document = {
  body: {addEventListener: (name, fn) => { listeners[name] = fn; }},
  addEventListener: noop, getElementById: () => null, querySelector: () => null, querySelectorAll: () => [],
};
const window = {
  addEventListener: noop, location: {pathname: '/work', assign: href => navigated.push(href)},
  setTimeout: fn => {timers.set(++nextTimer, fn); return nextTimer;},
  clearTimeout: id => timers.delete(id),
};
const context = {window, document, console, URL, setTimeout, clearTimeout, AbortController};
vm.runInNewContext(fs.readFileSync('src/pa/server/static/js/agent-chat.js', 'utf8'), context);
const Widget = window.PAAgentChat.AgentChatWidget;
window.PAAgentChat.mount = noop;
vm.runInNewContext(fs.readFileSync('src/pa/server/static/js/spa.js', 'utf8'), context);
const deferred = () => {let resolve, reject; const promise = new Promise((a,b) => {resolve=a;reject=b;}); return {promise,resolve,reject};};
const control = () => ({hidden:true, attrs:{}, dataset:{}, textContent:'', setAttribute(k,v){this.attrs[k]=v;}, removeAttribute(k){delete this.attrs[k];}});
function fixture() {
  const response = deferred(), button = control(), status = control(), link = control(), check = control();
  const root = {dataset:{fresh:'1'}, isConnected:true};
  const widget = Object.create(Widget.prototype); let posts = [], opens = 0, retries = 0;
  Object.assign(widget, {
    root, destroyed:false, subscriptionGeneration:0, cardId:'card-1', createLabel:'card:card-1',
    api(path, opts) {posts.push({path, body:JSON.parse(opts.body)}); return response.promise;},
    openSession() {opens++; return Promise.resolve();},
    setPlaceholder:noop, setStatus:noop, retryAfterStartupRecovery() {retries++;return true;},
  }); root._acw = widget;
  const pane = {querySelector: () => root};
  const detail = {querySelector: selector => ({
    '[data-card-agent-pane="new"]':pane,
    '[data-card-agent-start-status]':status, '[data-card-agent-created-link]':link,
    '[data-card-agent-check]':check,
  })[selector] || null, querySelectorAll: () => [pane]};
  button.hasAttribute = key => key === 'data-card-agent-start-new';
  button.closest = selector => selector === '[data-card-detail], [data-card-dispatch-context]' ? detail :
    selector === '[data-card-agent-start-new], [data-card-agent-select]' ? button : null;
  const click = () => listeners.click({target:button, preventDefault:noop});
  return {root, button, status, link, check, response, click, posts, opens:()=>opens, retries:()=>retries};
}
const flush = async () => {for (let i=0;i<8;i++) await Promise.resolve();};
(async () => {
  const f = fixture(); f.click(); f.click();
  assert.equal(f.posts.length,1,'repeated clicks must not POST twice');
  assert.equal(f.button.disabled,true); assert.equal(f.button.attrs['aria-busy'],'true');
  assert.match(f.status.textContent,/Creating/);
  const body = f.posts[0].body;
  assert.equal(body.fresh,true); assert.equal(body.label,'card:card-1');
  assert.equal(body.initiating_workflow.kind,'card_chat');
  for(const key of ['provider','model_id','mode_id','execution_preferences']) assert.equal(key in body,false,'defaults stay inherited');
  // Slow creation is still in flight; a late result must open its exact identity.
  for(const callback of [...timers.values()]) callback();
  assert.match(f.status.textContent,/longer than expected/); assert.equal(f.check.hidden,false);
  assert.equal(f.button.attrs['aria-busy'],undefined); f.click(); assert.equal(f.posts.length,1);
  f.response.resolve({session:{id:'created-session',origin_instance_id:'owner'}}); await flush();
  assert.equal(navigated.pop(),'/agent?session=created-session&instance=owner');
  assert.equal(f.link.href,'/agent?session=created-session&instance=owner'); assert.equal(f.link.hidden,false);
  assert.equal(f.button.textContent,'Session created'); assert.equal(f.opens(),0,'handoff avoids embedded lookup');
  assert.equal(timers.size,0);
  // Failure is visible and never schedules a second ambiguous create.
  const failure = fixture(); failure.click(); failure.response.reject(Error('connection lost')); await flush();
  assert.match(failure.status.textContent,/connection lost/); assert.match(failure.status.textContent,/may have completed/);
  assert.equal(failure.button.attrs['aria-busy'],undefined); assert.equal(failure.check.hidden,false);
  failure.click(); assert.equal(failure.posts.length,1); assert.equal(failure.retries(),0);
  // A missing server identity is an error, not an invalid /agent navigation.
  const missing = fixture(); missing.click(); missing.response.resolve({}); await flush();
  assert.match(missing.status.textContent,/no session identity/); assert.equal(navigated.length,0);
  // Do not navigate a page the user has already left, even for late success.
  const detached = fixture(); detached.click(); detached.root.isConnected=false;
  detached.response.resolve({session:{id:'late'}}); await flush(); assert.equal(navigated.length,0);
  const existing = fixture(); existing.root._acw.sessionId='already-created'; existing.click();
  assert.equal(existing.posts.length,0); assert.equal(navigated.pop(),'/agent?session=already-created');
  console.log('PASS card start completion, stalled/failed handoff, duplicate guards, defaults, detached lifecycle');
})().catch(error => {console.error(error);process.exitCode=1;});
