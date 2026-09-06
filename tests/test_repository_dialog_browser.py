"""Exercise the shipped Add Repo markup/scripts with isolated GitHub responses."""
from __future__ import annotations

import asyncio
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import tempfile
import threading
import unittest

from jinja2 import Template

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager

ROOT = Path(__file__).parents[1] / 'src/pa/server'


@unittest.skipUnless(_browser_executable(), 'managed Chromium is not installed')
class RepositoryDialogBrowserTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        template = (ROOT / 'templates/pages/projects.html').read_text()
        start = template.index('<dialog id="new-repository-dialog"')
        markup = Template(template[start:template.index('</dialog>', start) + 9]).render(active_realm='default', csrf_token='test', selected_project='')
        scripts = '\n'.join((ROOT / 'static/js' / name).read_text() for name in ['csrf.js', 'layout.js', 'repository-dialog.js'])
        fixture = '''<!doctype html><meta name="viewport" content="width=device-width, initial-scale=1"><style>''' + (ROOT / 'static/style.css').read_text() + '''</style>
<button data-project-create-open="new-repository-dialog">Add Repo</button>''' + markup + '''<script>
window.fixture = {authenticated: true, delayed: false, resolve: null, posts: 0, queries: []};
window.fetch = async function(path, options) {
  let body = {}, status = 200;
  const url = new URL(path, location.href);
  if (url.pathname.endsWith('/identity')) {
    status = fixture.authenticated ? 200 : 401;
    body = fixture.authenticated ? {login:'octocat'} : {detail:'GitHub is not authenticated.'};
  } else if (url.pathname.endsWith('/repository-availability')) {
    body = {name:url.searchParams.get('name'), login:'octocat', available:url.searchParams.get('name') !== 'taken'};
    if (fixture.delayed) await new Promise(resolve => fixture.resolve = resolve);
  } else if (options && options.method === 'POST') {
    fixture.posts++; fixture.submission = JSON.parse(options.body);
    fixture.key = options.headers['Idempotency-Key'];
    status = 503; body = {detail:'GitHub created octocat/fresh, but PA could not add it. Retry this request.'};
  } else {
    fixture.queries.push(url.search);
    body = {repositories:[{id:42,name:'example',full_name:'octocat/example',clone_url:'https://github.com/octocat/example.git',private:true}],next_page:fixture.queries.length === 1 ? 2 : null};
  }
  return new Response(JSON.stringify(body),{status,headers:{'Content-Type':'application/json'}});
};
</script><script>''' + scripts + '</script>'
        Path(self.tmp.name, 'index.html').write_text(fixture)
        self.httpd = ThreadingHTTPServer(('127.0.0.1', 0), partial(SimpleHTTPRequestHandler, directory=self.tmp.name))
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.browser = BrowserManager(Path(self.tmp.name) / 'browser')
        self.manager = BrowserSessionManager(self.browser, instance_id='repo-browser')
        self.scope = BrowserScope('user:test', 'repo-dialog', 'repo-browser')
        await self.manager.attach(self.scope, url=f'http://127.0.0.1:{self.httpd.server_port}/index.html', width=1440, height=1000)
        self.page = self.manager.resolve(self.scope).page

    async def asyncTearDown(self):
        await self.manager.close()
        await self.browser.close()
        self.httpd.shutdown(); self.httpd.server_close(); self.thread.join(timeout=2)
        self.tmp.cleanup()

    async def js(self, script):
        result = await self.page.evaluate(script)
        await asyncio.sleep(.05)
        return result

    async def test_browse_new_stale_checks_retry_and_reopen(self):
        await self.js('document.querySelector("[data-project-create-open]").click()')
        self.assertIn('@octocat', await self.js('document.querySelector("[data-github-identity]").textContent'))
        self.assertFalse(await self.js('document.querySelector("#repository-existing").hidden'))
        await self.js('document.querySelector("[data-repository-browse]").click()')
        self.assertEqual(await self.js('document.querySelectorAll(".repository-result").length'), 1)
        await self.js('document.querySelector("[data-repository-more]").click()')
        self.assertIn('page=2', await self.js('fixture.queries[1]'))
        await self.js('var search=document.querySelector("[data-repository-search]");search.value="absent";search.dispatchEvent(new Event("input"))')
        self.assertEqual(await self.js('document.querySelectorAll(".repository-result").length'), 0)
        await self.js('search.value="example";search.dispatchEvent(new Event("input"));document.querySelector(".repository-result").click()')
        self.assertEqual(await self.js('document.querySelector("#repository-url").value'), 'https://github.com/octocat/example.git')
        await self.js('document.querySelector("[data-repository-tab=new]").focus()')
        await self.manager.press(self.scope, key='Home')
        self.assertEqual(await self.js('document.activeElement.id'), 'repository-existing-tab')
        await self.manager.press(self.scope, key='End')
        self.assertFalse(await self.js('document.querySelector("#repository-new").hidden'))
        await self.js('var n=document.querySelector("[data-repository-name]");n.value="taken";n.dispatchEvent(new Event("input"));document.querySelector("[data-repository-check]").click()')
        self.assertTrue(await self.js('document.querySelector("[data-repository-create]").disabled'))
        await self.js('fixture.delayed=true;n.value="stale";n.dispatchEvent(new Event("input"));document.querySelector("[data-repository-check]").click()')
        await self.js('n.value="fresh";n.dispatchEvent(new Event("input"));fixture.resolve();fixture.delayed=false')
        self.assertTrue(await self.js('document.querySelector("[data-repository-create]").disabled'))
        await self.js('document.querySelector("[data-repository-check]").click()')
        self.assertFalse(await self.js('document.querySelector("[data-repository-create]").disabled'))
        await self.js('document.querySelector("[data-repository-create]").click()')
        self.assertEqual(await self.js('fixture.submission'), {'name':'fresh', 'confirmed_login':'octocat', 'realm':'default'})
        first_key = await self.js('fixture.key')
        self.assertIn('Retry adding', await self.js('document.querySelector("[data-repository-create]").textContent'))
        await self.js('document.querySelector("[data-repository-create]").click()')
        self.assertEqual(await self.js('fixture.key'), first_key)
        await self.page.resize(width=390, height=844)
        self.assertTrue(await self.js('document.querySelector("dialog").scrollWidth <= document.querySelector("dialog").clientWidth'))
        await self.js('document.querySelector("[data-project-create-close]").click();fixture.authenticated=false;document.querySelector("[data-project-create-open]").click()')
        self.assertFalse(await self.js('document.querySelector("#repository-existing").hidden'))
        self.assertTrue(await self.js('document.querySelector("[data-github-identity]").classList.contains("repository-error")'))
        self.assertTrue(await self.js('document.querySelector("[data-repository-browse]").disabled'))
        self.assertFalse(await self.js('document.querySelector("#repository-url").disabled'))
