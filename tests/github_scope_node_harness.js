const fs = require('fs');
const vm = require('vm');
const assert = require('assert');
const source = fs.readFileSync(process.argv[2], 'utf8');
async function run(local, fleet, localOk=true) {
  const elements = new Map();
  function element() { return { dataset: {}, children: [], textContent: '', value: '', hidden: false,
    addEventListener(name, fn) { this[name] = fn; },
    replaceChildren() { this.children = []; this.textContent = ''; },
    appendChild(child) { this.children.push(child); } }; }
  const calls = [];
  const document = {getElementById(id) { if (!elements.has(id)) elements.set(id, element()); return elements.get(id); },
    querySelector() { return null; }, createElement() { return element(); }};
  const context = {document, window: {}, crypto: {randomUUID() {return 'test-key';}},
    fetch: async (url, options) => {calls.push([url, options]); return {
      ok: url.endsWith('/comparison') || localOk,
      json: async () => url.endsWith('/comparison') ? fleet : local };}};
  vm.runInNewContext(source, context);
  await new Promise(resolve => setImmediate(resolve));
  return {elements, calls};
}
(async () => {
  let {elements, calls} = await run({instance_id:'macbook',instance_name:'Macbook',scope_mode:'allowlist',
    allowed_repositories:['petersky/pa','petersky/eschaton'],revision:'saved-two',policy_source:'configured',
    published_capability:{policy_revision:'old-one',observed_at:'2026-09-12T18:00:00Z',state:'publication_pending'}},
    {current_instance_id:'macbook',evaluation_state:'complete',candidates:[
      {instance_id:'macbook',instance_name:'Macbook',scope_mode:'allowlist',repositories:['petersky/pa','petersky/eschaton'],policy_source:'configured',policy_revision:'old-one',freshness:'fresh',observed_at:'2026-09-12T18:00:00Z',authenticated:true},
      {instance_id:'macmini',instance_name:'Macmini',scope_mode:'allowlist',repositories:['petersky/pa'],policy_source:'legacy_capability',policy_revision:null,freshness:'stale',observed_at:'2026-09-12T17:00:00Z',authenticated:true,reason_code:'capability_stale',action:'Check publisher.'},
      {instance_id:'broken',repositories:null,scope_mode:null,policy_source:'unknown',freshness:'fresh',observed_at:'2026-09-12T18:00:00Z',authenticated:true}
    ]});
  const current = elements.get('pa-github-scope-current').textContent;
  assert(current.includes('Saved revision: saved-two'));
  assert(current.includes('Published revision: old-one'));
  assert(current.includes('Capability publication pending'));
  const rows = elements.get('pa-github-scope-comparison').children.map(x=>x.textContent);
  assert(rows[0].includes('Macbook (macbook) (current instance)'));
  assert(rows[1].includes('Macmini (macmini)'));
  assert(!rows[1].includes('eschaton'));
  assert(rows[1].includes('unavailable (older peer)'));
  assert(rows[1].includes('stale'));
  assert(rows[2].includes('scope unknown'));
  assert(calls.every(x=>x[1].method === 'GET'));
  ({elements, calls} = await run({detail:{message:'Repair local scope.'}},
    {evaluation_state:'unavailable',message:'authority_unreachable. Automatic retry is scheduled.'}, false));
  assert(elements.get('pa-github-scope-current').textContent.includes('unknown'));
  assert(elements.get('pa-github-scope-comparison').textContent.includes('authority_unreachable'));
  assert(!elements.get('pa-github-scope-comparison').textContent.includes('authentication'));
  console.log('Scope UI: saved/published revisions, instance distinction, unknown/stale, authority error and GET-only comparison passed.');
})().catch(error => {console.error(error); process.exit(1);});
