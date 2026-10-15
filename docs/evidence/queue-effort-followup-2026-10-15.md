# Queue cues and model-dependent effort choices

This follow-up starts at merged PR #457 (`c92eb99c42e1a3bacd21095dc997b1ebed612747`). It changes queue presentation and dependent effort selectors, not scheduling, provider permissions, or production activation.

## Execution provenance

- Instance: macbook, `0c7d8ecb-7e45-4579-8fa0-35159492d3f1`.
- Repository: `https://github.com/petersky/pa`, `5ce49cd8-f632-5f6d-a0d5-0b9eaf61f9cd`.
- Card: `d2b9d8ef-4797-4764-b398-4302319ea2b6`.
- Session: `b343b028-b0dc-47b0-8bee-6d1b1c8d8a8f`; dispatch `6c240236-fa49-4d50-b67a-31ad7aa7ae76`.
- Lease: `ad6c8f4912fa790dad51d636f8a34ae549df2414b8084aea3cb624eb6219ff59`, fence 264, ready/verified before edits.
- Worktree: `/Users/petersky/.pa-workspaces/worktrees/d2b9d8ef-4797-47-495f01ae/b343b028-b0dc-47-0878b7e2/5ce49cd8-f632-5f6d-a-b2354070266ade2c`.
- Branch: `pa/d2b9d8ef-4797-47-495f01ae-b343b028-b0dc-47-0878b7e2-266ade2c`.
- Initial Git state: clean; verified origin; fetched main equal to base (0/0 divergence). Predecessor worktree/lease not reused.
- Queue implementation: `54fc64e899284f9d3b4d1d8b54537101317e13d8`. Subsequent effort commit and PR are recorded on the canonical card and durable PR watch.

## Queue behavior

Runtime snapshots expose presentation facts in existing server order. The next eligible item uses the drain loop's control-mode predicate. An unresolved admission or restart-validation gate withholds a promise about the next item. Human takeover can hold an automated row while a later human row is marked Next. Positions include held rows rather than presenting them as delivered or silently removing them.

Short labels are Human, Response, Automated, or Other. Response deliberately includes both correlated approval answers and freeform operator answers: the source alone cannot distinguish them. The viewer does not parse private response contents to invent a narrower classification. Native provider permission requests remain in their existing permission UI.

Pause/current-reply/reconnection cues, priority and explicit-reorder explanation, labelled keyboard reorder/removal buttons, focus retention, and compact scrolling are presentation changes. Existing enqueue priority/FIFO, reorder endpoint, prompt IDs, pause semantics, and permission enforcement are unchanged.

## Effort evidence and behavior

The live read-only `execution_catalog` on this execution instance confirmed Codex/default-connection capabilities on 2026-10-15:

| Model | Advertised efforts |
| --- | --- |
| gpt-6.1-sol | low, medium, high, xhigh, max, ultra |
| gpt-6-astra | low, medium, high, xhigh, max, ultra |
| gpt-6-luna | low, medium, high, xhigh, max |

These are observed provider advertisements, not a hard-coded model table. `selection_catalog.candidates_from_advertisement` already derives model-specific pairs from advertised selectors and restricts generic config options to the current model. The UI now consumes those scoped candidates. Ambiguous routing, conflicting observations, stale evidence, and unknown/unsupported reasoning do not produce a union of levels.

Affected surfaces: current chat Agent settings (including native config model selects), the native execution receipt panel, new-chat execution preferences, legacy new-chat controls, and user/global Settings defaults. Draft changes immediately rebuild dependent effort choices, retain supported choices, and explain incompatible resets. They do not mutate the provider to discover choices. Late requests are fenced by scope/generation/revision; live snapshots do not overwrite an unsaved draft.

Current settings submit model+effort through PA's existing versioned/deferred settings path where a selection receipt exists. Legacy settings wait for a confirmed model and fresh matching effort evidence before sending effort; a failed/unconfirmed model prevents the effort request. Native permission/mode changes retain their separate path. Pending is shown as pending, not applied. Provider confirmation and immutable execution provenance remain server-owned.

## Validation and limitations

- Queue slice: 62 focused tests passed (runtime, queue view, existing SSE lifecycle, real Chromium render/keyboard/mobile/reload).
- Combined regression run: 118 passed, one older lifecycle test stub failed because it omitted settings controls. After updating that stub, all 18 affected lifecycle/effort checks passed (119 distinct combined checks now passing).
- Effort browser tests exercise Sol/Astra ultra → Luna without ultra → back, preserving high, unknown/no support, stale stored effort, model-first acknowledgment, deferred requests, rapid provider switches, stale responses and thread changes. Shared evidence tests cover instance/account/provider/model isolation and conflicting catalogs.
- Existing real ACP wire fixture tests cover native confirmation/recovery/deferred application. New browser state tests use isolated temporary storage and synthetic provider responses; they do not claim live-model generation acceptance.
- Legacy providers without an advertised effort-reset action cannot claim a default reset was applied. Selecting Automatic without a model change reports that limitation; a model change reports that no explicit effort was sent and shows the provider's confirmed settings.
- Isolated boot smoke passed with absolute data/workspace paths under this worktree's `.dev` directory. No running PA data directory was used.
- Actual browser review: PA tool snapshots `queue-desktop-snapshot-v1`, `queue-reorder-evidence-v1`; generated Chromium screenshots `.dev/evidence/queue-mobile.png`, `.dev/evidence/effort-luna.png`, and bounded option evidence `.dev/evidence/effort-luna.json` (regenerated by `PA_TEST_EVIDENCE_DIR=.dev/evidence`). Screenshots are fixture evidence, not production screenshots.
- PA browser named Enter failed to activate a focused native button in the isolated test, while Space and pointer click worked; report `24610c97-1ca2-467b-abb0-6db6e4d0efdc`. Keyboard regression uses actual Space activation. No browser repair was added.

Both queue and effort scope must be integrated together before this successor is completed. The canonical card records final test totals, commit/PR, exact-head CI/reviews, supervision, and integration separately from activation. No production restart or installation is authorized here.
