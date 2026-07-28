# HTMX support and delivery policy

## Supported release

PA supports **htmx.org 2.0.10**. The minified production file is vendored at
`src/pa/server/static/vendor/htmx/htmx-2.0.10.min.js` with its upstream 0BSD
license. Its reviewed SHA-256 is
`71ea67185bfa8c98c39d31717c6fce5d852370fcdfd129db4543774d3145c0de`.

This replaces the jsDelivr-loaded `4.0.0-beta5`. At review time, the official npm
tags were `latest=2.0.10` and `next=4.0.0-beta6`, and the HTMX 4 documentation
still described 4.0 as under construction. PA therefore uses the current stable
major rather than depending on a prerelease request, event, header, or history
contract. Primary references:

- <https://www.npmjs.com/package/htmx.org>
- <https://htmx.org/docs/>
- <https://htmx.org/events/>
- <https://four.htmx.org/migration-guide-htmx-4/>
- <https://github.com/bigskysoftware/htmx/security/advisories>

No published upstream GitHub security advisory existed at review time. That is a
point-in-time observation, not a substitute for checking advisories during every
update.

## PA usage contract

The supported declarative surface is `hx-get`, `hx-post`, `hx-delete`,
`hx-trigger`, `hx-target`, `hx-swap`, `hx-push-url`, `hx-confirm`, and
`hx-preserve`. It covers shell navigation, board polling and card mutations,
Projects forms, Settings, Memory, Fleet, file browsing, and agent-session
fragments. `htmx.ajax()` drives SPA/popstate and Fleet refreshes;
`htmx.process()` activates card and newly created modal fragments.

PA uses the HTMX 2 event names `htmx:configRequest`, `htmx:beforeSwap`,
`htmx:afterSwap`, `htmx:responseError`, `htmx:historyRestore`,
`htmx:pushedIntoHistory`, and `htmx:replacedInHistory`. Do not add HTMX 4 aliases
alongside them: a compatibility pair can execute idempotent-looking handlers
twice. The contract test rejects known HTMX 4 event names.

Every HTMX mutation receives PA's `X-CSRF-Token` in `htmx:configRequest`.
Server routing continues to use the stable `HX-Request` request header. Status
handling is explicit in the shell: 204 and 304 never swap, 2xx/3xx swap, and
4xx/5xx neither swap nor hide the error event. This preserves JSON error toasts
without replacing application markup.

Fleet cancellation is PA-owned. It records one in-flight navigation, aborts its
own `fetch()` with `AbortController`, owns the resulting promise, treats only
cancellation/stale generations as expected, and logs genuine network failures.
The successful HTML is applied through the stable `htmx.swap()` API so dynamic
fragments are processed normally. This avoids HTMX 2's unconditional
`console.error` for `htmx:abort`. A version change must not remove that
coordinator or its tests merely because a library version happens to make
aborts quieter.

## Delivery, security, and availability

Production serves HTMX from PA's own `/static` mount with the normal build asset
version query. This provides immutable cache invalidation, reproducible wheels
and containers, no runtime CDN dependency, and local/offline Fleet startup. It
also removes the external script origin from the future CSP requirement and
avoids CDN fallback races or a second unreviewed execution path. HTMX has no
runtime dependencies.

Updates must use an exact upstream npm version (never a floating tag), retain the
upstream license, verify the registry integrity and committed SHA-256, inspect
the changelog/migration guide/security advisories, and run the matrix below.
Review quarterly and immediately for a relevant security advisory or a stable
HTMX 4 release.

## Regression matrix

Automated contracts inventory every template attribute and JavaScript event/API,
validate the vendored bytes and status policy, exercise server HTMX fragment and
CSRF behavior, and cover Fleet stale/aborted request ownership. PA's existing
tests cover board/card Markdown and mutations, Fleet overview/topology/remote
operations/sync/update, Projects, Settings, Memory, agent chat, dynamic
fragments, and history-related handlers.

Before release, run the full Python suite plus managed-browser tests. PA's
managed browser target is Chromium. If WebKit or Firefox becomes a supported PA
runtime, run the same navigation/back-forward, polling, modal, form, 204/304,
4xx/5xx, slow/duplicate/out-of-order/abort, offline-startup, and console-clean
scenarios there before changing this matrix. An offline check must block public
CDN hosts and confirm the locally served shell still initializes HTMX.

## Update and rollback

To update, add the exact new asset and license, update the shell path and hash
contract, migrate event/API/config behavior from primary documentation, then
run the full matrix. Keep dependency delivery changes separate from request
coordination changes in the commit history whenever both are needed.

Rollback is a single asset/config contract: restore the previous reviewed
`4.0.0-beta5` script tag with its recorded SRI
`sha384-5dnhUXCt1hXGvYrjAnKwgNX3I8xtIJiW6eIHIbeo7oWyXv2XpWYC/rl+ZiWfuYO5`,
restore HTMX 4 colon-separated event names and `noSwap` configuration, and
retain Fleet's PA-owned `fetch()`/`AbortController` cancellation logic. Because that rollback
reintroduces a CDN prerelease and offline dependency, it is an emergency
rollback only; follow it with a reviewed locally vendored correction.
