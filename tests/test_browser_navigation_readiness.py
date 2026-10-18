"""Navigation readiness belongs to the committed document, never the old one."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from pa.browser.cdp import CdpError, CdpPage
from pa.browser.manager import BrowserManager


class PendingPage(CdpPage):
    def __init__(self, *, redirect=False, same_document=False):
        super().__init__("http://protocol-fixture", "target")
        self.calls = []
        self.loader = "old"
        self.navigating = False
        self.reads = 0
        self.redirect = redirect
        self.same_document = same_document

    async def command(self, method, params=None):
        self.calls.append(method)
        if method == "Page.navigate":
            self.navigating = True
            return {"frameId": "main", **({} if self.same_document else {"loaderId": "new"})}
        assert method == "Page.getFrameTree"
        if self.navigating:
            self.reads += 1
            if self.reads > 1:
                self.loader = "redirected" if self.redirect else "new"
        return {"frameTree": {"frame": {"id": "main", "loaderId": self.loader}}}

    async def evaluate(self, *args, **kwargs):
        return {"ready_state": "complete", "url": "http://fixture/same",
                "title": self.loader, "body_text": "fixture"}


@pytest.mark.asyncio
@pytest.mark.parametrize("redirect", [False, True])
async def test_complete_previous_document_does_not_satisfy_navigation(redirect):
    page = PendingPage(redirect=redirect)
    result = await page.navigate_and_wait("http://fixture/same")
    assert result["title"] == ("redirected" if redirect else "new")
    assert page.calls.count("Page.navigate") == 1
    assert page._pending_navigation is None


@pytest.mark.asyncio
async def test_same_document_navigation_does_not_wait_for_new_loader():
    page = PendingPage(same_document=True)
    result = await page.navigate_and_wait("http://fixture/same#section")
    assert result["title"] == "old"
    assert page.calls == ["Page.getFrameTree", "Page.navigate"]


@pytest.mark.asyncio
async def test_readiness_rechecks_document_after_evaluation():
    page = PendingPage()
    original = page.evaluate
    evaluations = 0
    async def replace_during_inspection(*args, **kwargs):
        nonlocal evaluations
        evaluations += 1
        value = await original(*args, **kwargs)
        if evaluations == 1:
            page.loader = "redirected"
            page.redirect = True
        return value
    page.evaluate = replace_during_inspection
    result = await page.navigate_and_wait("http://fixture/same")
    assert result["title"] == "redirected"
    assert evaluations == 2
    assert page.calls.count("Page.navigate") == 1


@pytest.mark.asyncio
async def test_unchanged_old_document_times_out_without_navigation_replay():
    page = PendingPage()
    await page.navigate("http://fixture/same")
    page.navigating = False
    with pytest.raises(CdpError, match="0.01 second deadline"):
        await page.wait_until_usable(timeout=.01)
    assert page.calls.count("Page.navigate") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, CdpError("fixture navigation failed"), asyncio.CancelledError()])
async def test_fresh_attachment_launches_blank_and_navigates_once(tmp_path, failure):
    manager = BrowserManager(tmp_path)
    manager._client = SimpleNamespace(
        get=AsyncMock(return_value=SimpleNamespace(json=lambda: [{"type": "page", "id": "target"}])),
        aclose=AsyncMock(),
    )
    process = SimpleNamespace(returncode=None)
    began, release = asyncio.Event(), asyncio.Event()
    async def navigate(url):
        assert url == "http://fixture/page"
        assert manager.get("fixture") is None
        began.set()
        await release.wait()
        if failure:
            raise failure
    with (
        patch("pa.browser.manager._browser_executable", return_value="chrome"),
        patch("pa.browser.manager.asyncio.create_subprocess_exec", AsyncMock(return_value=process)) as launch,
        patch.object(CdpPage, "resize", AsyncMock()),
        patch.object(CdpPage, "navigate", AsyncMock(side_effect=navigate)) as navigation,
        patch.object(CdpPage, "close", AsyncMock()),
        patch.object(manager, "_terminate_process", AsyncMock()) as terminate,
    ):
        task = asyncio.create_task(manager.attach("fixture", url="http://fixture/page"))
        await asyncio.wait_for(began.wait(), 2)
        assert launch.call_args.args[-1] == "about:blank"
        assert not task.done()
        release.set()
        if failure:
            with pytest.raises(type(failure)):
                await task
            assert manager.get("fixture") is None
            terminate.assert_awaited_once_with(process)
        else:
            assert (await task).target_id == "target"
            assert manager.get("fixture") is not None
            await manager.detach("fixture")
        navigation.assert_awaited_once_with("http://fixture/page")
    await manager.close()
