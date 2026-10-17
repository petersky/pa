# Chat visualizations v1

Assistant messages can reference UTF-8 HTML with a content reference outside a
Markdown code example:

```text
visualize{"path":"output/concept.html","title":"Home concepts","wide":true,"version":1}
```

`path` is required. `title`, boolean `wide`, and integer `version` are optional;
the default version is 1. `get_rendering_capabilities(session_id)` exposes the
current contract to every session, and human conversation capabilities include
the same metadata. Raw transcript output/export stays unchanged.

The sole PA writer captures references while persisting assistant chunks,
reconstructing deltas and snapshots. It copies HTML (maximum 2 MiB) from the
session's admitted workspace or an active HTML attachment on an associated card
in the same realm. Unique attachment filenames allow a portable attachment to
replace an unavailable originating path. Workspace traversal, symlinks and hard
links are rejected. Existing transcript pages can backfill artifacts from those
same sources. Code examples never authorize capture.

Artifacts and bounded widget state are retained under the owner's
`visualizations-v1/<session-hash>/` directory, independently of provider processes
and workspace collection. References bind immutable content digests to the exact
session. Viewer HTTP requests only look up retained references; they never read
the path in a request. The authenticated owner/admin session policy applies to
preview, source download and state writes. Fleet viewers use the existing
authenticated agent proxy. The owning server must remain available; artifacts
are not replicated into another session or peer's filesystem.

The chat renders an opaque `sandbox="allow-scripts"` iframe. Its CSP blocks
fetch, external resources, nested frames, workers, forms and objects. The fragment
cannot access the PA DOM, cookies, storage or application APIs. Self-contained
HTML/CSS and local JS are supported; referenced local images or imported JS/CSS
must be inlined. Full-size display uses a native dialog with Escape and focus
restoration. Regular Markdown keeps its existing sanitization.

The local runtime provides light/dark colors, `.cursor-interaction`,
`.viz-dotted-background`, named `.viz-carousel > [data-variant]` alternatives,
native controls/details, and `Tweak` with `addToggle` and `addSelect`. The
`openai.widgetState` / `openai.setWidgetState` compatibility surface retains
`modelContent` and `privateContent` up to 32 KiB. Its versioned
`pa.visualization/v1` bridge accepts only resize, readiness and bounded widget
state from the exact iframe window and per-frame token. It exposes no tool,
navigation or privileged host calls. State-save failures are visible.

Missing, malformed, unsupported and denied previews show named readable
fallbacks. Available previews offer source download and full-size controls.
Unsupported runtime/network dependencies are outside the v1 contract.

Regression coverage uses both original card attachments in
`tests/fixtures/visualizations/`. `tests/test_visualizations.py` verifies parsing,
source ownership, immutable capture and API access. The full PA `/agent` browser
journey is in `tests/test_visualization_chat_browser.py`; run a manual temporary
instance with `uv run python -m tests.visualization_chat_fixture` and visit
`http://127.0.0.1:18085/fixture`.
