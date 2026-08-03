# Work page performance budgets

The Work page shell contains filters and four lazy lane placeholders. Card bodies,
session history, dispatch history, progress streams, and PR-watch events are not
embedded in it. Each lane returns 10 cards initially and accepts an explicit limit
up to 100. Details and activity remain lazy card-dialog requests.

Representative CI fixtures use 120 cards for repeated navigation and 600 cards
with large historical bodies for payload scaling. The stable budgets are:

| Measure | Budget |
| --- | ---: |
| Work shell transfer | < 100 KB |
| Initial lane transfer | < 50 KB |
| Initial rows per lane | 10 |
| Lane session queries | 1 |
| Lane dispatch-history scans | 1 |
| HTMX shell server duration | < 1 s |
| 20-navigation last/first mean ratio | < 2x (+20 ms noise allowance) |
| Retained Work EventSources off-page | 0 |
| Active Work EventSources | 1 |

Responses expose `Server-Timing` entries for `shell`, `page_context`, `template`,
and `total`, plus `X-PA-Work-Bytes`. Structured `work_render` logs contain only
phase durations, response bytes, and whether the request was HTMX; no card or user
content is logged.

Browser verification covers Chromium and WebKit (Safari-class), normal and compact
viewport widths. After 20 Work-to-other-page HTMX navigations, inspect
`window.__paWorkResources`: it must alternate between one active EventSource on
Work and zero off-page, with no increasing DOM-node or listener trend.
