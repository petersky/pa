# Browser attachments

PA can attach an isolated Chromium surface to an ACP agent session. The browser remains owned and rendered by PA while the agent controls the same page through PA's MCP tools.

Use **Attach Browser** in an agent chat toolbar. PA launches Chrome or Chromium with a session-specific profile and a loopback-only, random CDP port. Attaching or detaching restarts the ACP connection and resumes the same external session so the MCP server inherits the browser endpoint.

The attached agent receives `browser_open`, `browser_snapshot`, `browser_click`, `browser_type`, `browser_back`, and `browser_screenshot`. PA's panel polls screenshots of that same target and supports navigation and coordinate clicks.

PA searches for Chrome, Chromium, and Microsoft Edge. Set `PA_BROWSER_EXECUTABLE` to use another Chromium executable. Browser URLs are limited to `http`, `https`, `about`, and `data`; privileged browser URLs, local files, and `javascript:` URLs are rejected.

Browser profiles live under `~/.pa/browser/<session-id>`. Detaching stops Chromium but preserves the profile for session restoration. Ending PA stops all managed browser processes.
