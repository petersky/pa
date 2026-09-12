"""Render the authenticated journal response in real Chromium at narrow widths."""
from __future__ import annotations

import asyncio
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
import threading

import httpx
import pytest

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from tests.test_health_journal import api, fleet, observation  # noqa: F401


@pytest.mark.browser
@pytest.mark.skipif(not _browser_executable(), reason='Chrome/Chromium is required')
@pytest.mark.asyncio
async def test_journal_preserves_complete_evidence_and_links_at_narrow_width(fleet, tmp_path):
    authority, source, _ = fleet
    ordinary = ['Synthetic isolated package smoke; no production incident.']
    long_evidence = ['redacted_' + 'x' * 980, '<script>window.journalInjected=true</script> & redacted']
    long_summary = 'redacted-id-' + 'a' * 900 + ' <img src=x onerror="window.journalInjected=true">'
    receipts = []
    for index, (summary, evidence) in enumerate([
        ('Ordinary observation', ordinary), (long_summary, long_evidence),
    ]):
        response = await api(source, 'POST', '/reports', body=observation(
            summary=summary, evidence=evidence, occurrence_key=f'browser-{index}',
        ), key=f'browser-{index}')
        assert response.status_code == 201, response.text
        receipts.append(response.json())
    await authority.service.cycle(manual=True)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=source.app), base_url='http://source') as client:
        response = await client.get('/health-journal', headers=source.headers)
        assert response.status_code == 200, response.text
    # Serve the actual authorized HTML unchanged; no substitute markup or CSS.
    (tmp_path / 'index.html').write_text(response.text)
    server = ThreadingHTTPServer(('127.0.0.1', 0), partial(SimpleHTTPRequestHandler, directory=str(tmp_path)))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    browser = BrowserManager(tmp_path / 'browser')
    manager = BrowserSessionManager(browser, instance_id='journal-browser')
    scope = BrowserScope('user:journal-browser', 'journal-browser', 'journal-browser')
    try:
        await manager.attach(scope, url=f'http://127.0.0.1:{server.server_port}/index.html', width=1440, height=900)
        page = manager.resolve(scope).page
        for width, height in [(1440, 900), (390, 844)]:
            await page.resize(width=width, height=height)
            await page.evaluate('new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))')
            (tmp_path / f'journal-{width}.png').write_bytes(await page.screenshot())
            measured = await page.evaluate('''(() => {
                const elements = Array.from(document.querySelectorAll('body, body *'));
                const textBounds = [];
                const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
                while (walker.nextNode()) {
                    const range = document.createRange(); range.selectNodeContents(walker.currentNode);
                    for (const r of range.getClientRects()) textBounds.push({left:r.left,right:r.right});
                }
                return {width:innerWidth,scrollWidth:document.documentElement.scrollWidth,
                    evidence:Array.from(document.querySelectorAll('pre')).map(e=>e.textContent),
                    summaries:Array.from(document.querySelectorAll('h2')).map(e=>e.textContent),
                    links:Array.from(document.querySelectorAll('a')).map(e=>e.getAttribute('href')),
                    injected:!!window.journalInjected,unexpectedNodes:document.querySelectorAll('script,img').length,
                    text:document.body.innerText,textBounds,
                    clipped:elements.some(e=>{const s=getComputedStyle(e);return ['hidden','clip'].includes(s.overflowX)||['hidden','clip'].includes(s.overflowY)||s.display==='none'||s.visibility==='hidden'}),
                    overflowing:elements.filter(e=>e.scrollWidth>e.clientWidth && getComputedStyle(e).display!=='inline').map(e=>e.tagName)};
            })()''')
            (tmp_path / f'journal-{width}.json').write_text(json.dumps(measured, indent=2))
            assert measured['width'] == width
            assert measured['scrollWidth'] <= width
            assert not measured['overflowing'] and not measured['clipped']
            assert all(r['left'] >= 0 and r['right'] <= width for r in measured['textBounds'])
            assert sorted(measured['evidence']) == sorted([str(ordinary), str(long_evidence)])
            assert sorted(measured['summaries']) == sorted(['Ordinary observation', long_summary])
            assert not measured['injected'] and measured['unexpectedNodes'] == 0
            assert '/api/health-journal/status' in measured['links']
            for receipt in receipts:
                assert f"/api/health-journal/reports/{receipt['report_id']}" in measured['links']
            assert 'gathered' in measured['text']
            assert 'Acceptance: not verified' in measured['text']
    finally:
        await manager.close()
        await browser.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
