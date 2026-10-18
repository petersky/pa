# pa-ux-06 verification

2026-10-18, macbook. Scope: repository UX guidance and one label clarification,
`Mode` → `Execution / permission mode` in the card's Agent panel. The native value,
defaults, permissions and completion/acceptance behavior are unchanged.

Source: `489122d2fd6a397b12ea201827e96b99071d6e49`; candidate: this PR's scoped
diff. Card `f94c082e-a76c-40bd-bea2-b4d0aa166d2d`, session
`515f2581-11a3-4f78-af5c-dbd8a4605405`. Source request
`fda913a9-51db-437e-8297-f41925edf316`, original revision **8**, selected issue
**pa-ux-06**. No source request was replaced or resubmitted.

## Method

Expert walkthrough and regressions against actual PA routes, HTML, HTMX and CSS
in Chrome. No participants, measured task times, success rates or usability
improvement claims. Production Work, this card's Agent tab, Settings and Fleet
were inspected read-only through PA's MCP browser. Saves ran only in disposable
PA instances, with no real ACP agent or peers, using
[`tests/ux_guidance_fixture.py`](../../tests/ux_guidance_fixture.py).

Desktop: 1440 CSS pixels; narrow: 390 CSS pixels. The controlled baseline used
the same fixture with the Agent template loaded from the source revision through
a temporary Jinja loader; the proposed render used this checkout's template.
Other journey UI is identical in this diff. Comparison is qualitative.

| Journey | Baseline → proposed; first action | Errors and help dependence |
| --- | --- | --- |
| Next action | Same visible `Start work`; a blocked saved start offers `Review start`. | No explanatory help required to locate the actions. Existing status/start regressions preserve the original request. |
| Started vs finished | Same `Not started`, `Work unfinished` after a worker turn and `Completed` for the delivered card. Open the card to distinguish card outcome from worker state. | No help required; a worker's completed state alone is insufficient. Acceptance regressions keep merge, activation and verification separate. |
| Delivered evidence | Same: open delivered card → Summary/Description → named `Regression report` link and `build-a` revision. | Found in the fixture without opening help. This checks discoverability of supplied evidence, not automatic evidence generation or the external report's contents. |
| Agent/model/permissions | Card → Agent in both; `codex`, `model-a`, `workspace-write` remain visible. The proposed label states what the native mode describes. | No help needed to read the values. Both fit at 390px; the longer label wraps. No claim that this label alone explains every provider permission. |
| Personal default | Same: Settings → Agent defaults → `My defaults` → change Model → `Save my defaults`. | Actual preference saves at both widths preserve instance defaults and permission approval; saved feedback and inheritance source are visible. Select values are set with DOM change events in automation; native select keyboard input through PA tooling remains unconfirmed. |
| Stale vs current failure | Same: Fleet → accessible `Inspect` button → field details. Freshness, last attempt and last success are separate. | Live partial observations later became fresh; timestamps were inspected. Deterministic Fleet regressions cover retained stale failures and late success. Supplementary field details are needed for exact timestamps/errors. |

The narrow controlled comparison is retained as
[baseline](ux06-baseline-worker-390.png) and
[proposed](ux06-proposed-worker-390.png). Reproduce the proposed fixture with:

```sh
uv run uvicorn tests.ux_guidance_fixture:create_app --factory --host 127.0.0.1 --port 8097
```

## Accessibility and verification limits

The new browser regressions exercise real arrow-key tab navigation, selected tab,
the changed polite `agent tab loaded` announcement, Escape dismissal and Chrome's
accessibility-tree names for the permission label/value. A screen-reader listening
session was **not** performed; DOM/AX checks do not establish audible behavior.

Zoom verification uses 200% equivalent reflow: a 1440×1000 physical viewport is
represented as 720×500 CSS pixels at device scale 2. Native browser zoom menus and
mobile pinch zoom were not tested. An earlier CSS `zoom: 2` harness was rejected
after visual review found cropping despite passing element-size assertions.
Normal 390px touch layout is checked separately.

PA browser limitations were reported without expanding this change:
`9bdf9702` (SVG node reference hit an overlapping route; accessible Inspect worked),
`62dd2cc4` (same-ID retry advice replayed a saved navigation failure), and
`b34ca15b` (native select key effect unconfirmed).
The initially unavailable comparison server was a fixture setup error, subsequently
corrected; no production failure was manufactured.

## Regression commands

```sh
BROWSER_TEST_EVIDENCE_DIR=.dev/ux-evidence uv run pytest -q tests/test_ux_guidance_browser.py
uv run pytest -q tests/test_status_actions_browser.py tests/test_settings_scope_browser.py
uv run pytest -q tests/test_work_presentation.py tests/test_fleet_overview_ui.py tests/test_local_operator_acceptance.py
```

Use isolated `PA_DATA_DIR` and `PA_WORKSPACE_ROOT` for the boot smoke test.
Browser checks need permission to bind disposable loopback servers and start Chrome.

Local results: 2 new journey checks, 4 existing status/settings browser checks and
95 work-presentation/Fleet/acceptance regressions passed; isolated boot smoke and
`uv build` passed. The wheel contains the clarified permission label.
The [bounded layout observations](ux06-observations.json) retain the per-task
viewport, visibility, overflow and help-disclosure checks. The PR supplies CI and
review evidence for its exact head. Existing Python/websocket deprecation warnings
remain; they do not change the results above.
