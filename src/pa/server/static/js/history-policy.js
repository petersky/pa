(function () {
  "use strict";

  var CACHE_KEY = "htmx-history-cache";
  var PROBE_KEY = "pa.history.probe";
  var MAX_ENTRIES = 3;
  var MAX_ENTRY_BYTES = 128 * 1024;
  var MAX_CACHE_BYTES = 256 * 1024;
  var RELOAD_MARKER = "data-pa-history-reload";
  var EXPECTED_FAILURES = {
    quota: true,
    denied: true,
    unavailable: true,
    private: true,
    oversized: true,
    miss: true,
  };
  var state = {
    disabled: false,
    reloadScheduled: false,
    fallbackPending: false,
    lastSnapshotBytes: 0,
    lastCacheBytes: 0,
    lastDiagnostic: null,
    counters: {
      snapshotsMeasured: 0,
      oversizedSnapshots: 0,
      privateSnapshotsBlocked: 0,
      privateEntriesPurged: 0,
      entriesEvicted: 0,
      quotaFailures: 0,
      deniedFailures: 0,
      unavailableFailures: 0,
      unexpectedFailures: 0,
      cacheMissReloads: 0,
      fullReloadFallbacks: 0,
      historyCacheErrors: 0,
    },
  };

  function bytes(value) {
    var text = String(value || "");
    if (window.TextEncoder) return new TextEncoder().encode(text).length;
    try {
      return unescape(encodeURIComponent(text)).length;
    } catch (_) {
      return text.length * 2;
    }
  }

  function surface() {
    if (document.querySelector("[data-agent-chat], .page-agent")) return "agent";
    if (document.getElementById("pa-workshop-root")) return "workshop";
    if (document.getElementById("pa-fleet-root")) return "fleet";
    return "other";
  }

  function errorClass(error) {
    var name = String(error && error.name || "");
    var code = Number(error && error.code || 0);
    if (
      name === "QuotaExceededError" ||
      name === "NS_ERROR_DOM_QUOTA_REACHED" ||
      code === 22 ||
      code === 1014
    ) return "quota";
    if (
      name === "SecurityError" ||
      name === "NotAllowedError" ||
      name === "InvalidStateError" ||
      code === 18
    ) return "denied";
    if (!error) return "unavailable";
    return "unexpected";
  }

  function increment(classification) {
    var key = {
      quota: "quotaFailures",
      denied: "deniedFailures",
      unavailable: "unavailableFailures",
      unexpected: "unexpectedFailures",
    }[classification];
    if (key) state.counters[key] += 1;
  }

  function diagnostic(classification, phase, values) {
    increment(classification);
    var detail = {
      schema: "pa.history-diagnostic/v1",
      classification: classification,
      phase: phase,
      surface: surface(),
      expected: !!EXPECTED_FAILURES[classification],
      snapshot_bytes: Number(values && values.snapshotBytes || 0),
      cache_bytes: Number(values && values.cacheBytes || 0),
      cache_entries: Number(values && values.cacheEntries || 0),
    };
    state.lastDiagnostic = detail;
    document.dispatchEvent(new CustomEvent("pa:historyDiagnostic", { detail: detail }));
    if (!detail.expected && window.console && typeof console.error === "function") {
      console.error("PA history cache failure", detail);
    }
  }

  function browserStorage() {
    try {
      return window.sessionStorage || null;
    } catch (_) {
      return null;
    }
  }

  function configureDisabled() {
    state.disabled = true;
    if (window.htmx && htmx.config) htmx.config.historyCacheSize = 0;
  }

  function clearCache(storage) {
    try {
      if (storage) storage.removeItem(CACHE_KEY);
    } catch (_) {}
  }

  function disableCache(classification, phase, error, values) {
    configureDisabled();
    clearCache(browserStorage());
    diagnostic(classification || errorClass(error), phase, values);
  }

  function reloadMarker(reason) {
    return '<div ' + RELOAD_MARKER + '="' + reason +
      '" role="status" aria-live="polite">Restoring current page\u2026</div>';
  }

  function containsPrivateContent(content) {
    return (
      content.indexOf("data-pa-history-private") !== -1 ||
      content.indexOf("data-agent-chat") !== -1 ||
      content.indexOf('id="pa-fleet-root"') !== -1 ||
      content.indexOf('id="pa-workshop-root"') !== -1
    );
  }

  function safeParseCache(storage) {
    var raw;
    try {
      raw = storage.getItem(CACHE_KEY);
    } catch (error) {
      disableCache(errorClass(error), "read", error);
      return null;
    }
    if (!raw) return [];
    try {
      var parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) throw new TypeError("history cache is not an array");
      return parsed;
    } catch (error) {
      clearCache(storage);
      diagnostic("unexpected", "parse", { cacheBytes: bytes(raw) });
      return [];
    }
  }

  function enforceStoredBudget(storage) {
    var cache = safeParseCache(storage);
    if (!cache) return;
    var changed = false;
    for (var index = cache.length - 1; index >= 0; index -= 1) {
      var content = String(cache[index] && cache[index].content || "");
      if (containsPrivateContent(content) || bytes(content) > MAX_ENTRY_BYTES) {
        cache.splice(index, 1);
        state.counters.privateEntriesPurged += 1;
        changed = true;
      }
    }
    while (cache.length > MAX_ENTRIES) {
      cache.shift();
      state.counters.entriesEvicted += 1;
      changed = true;
    }
    var serialized = JSON.stringify(cache);
    while (cache.length && bytes(serialized) > MAX_CACHE_BYTES) {
      cache.shift();
      state.counters.entriesEvicted += 1;
      changed = true;
      serialized = JSON.stringify(cache);
    }
    if (!changed) return;
    try {
      if (cache.length) storage.setItem(CACHE_KEY, serialized);
      else storage.removeItem(CACHE_KEY);
    } catch (error) {
      disableCache(errorClass(error), "startup-trim", error, {
        cacheBytes: bytes(serialized),
        cacheEntries: cache.length,
      });
    }
  }

  function probe() {
    var storage = browserStorage();
    if (!storage) {
      disableCache("unavailable", "probe");
      return;
    }
    try {
      storage.setItem(PROBE_KEY, "1");
      storage.removeItem(PROBE_KEY);
    } catch (error) {
      disableCache(errorClass(error), "probe", error);
      return;
    }
    enforceStoredBudget(storage);
  }

  function prepareLiveResources(reason) {
    document.dispatchEvent(new CustomEvent("pa:historyWillReload", {
      detail: { reason: reason },
    }));
  }

  function scheduleReload(reason, destination) {
    if (state.reloadScheduled) return;
    state.reloadScheduled = true;
    state.counters.fullReloadFallbacks += 1;
    prepareLiveResources(reason);
    window.setTimeout(function () {
      if (destination) window.location.assign(destination);
      else window.location.reload();
    }, 0);
  }

  function onHistoryItemCreated(event) {
    var detail = event.detail || {};
    var item = detail.item;
    var cache = detail.cache;
    if (!item || !Array.isArray(cache)) return;

    var originalBytes = bytes(item.content);
    state.lastSnapshotBytes = originalBytes;
    state.counters.snapshotsMeasured += 1;
    if (containsPrivateContent(String(item.content || ""))) {
      item.content = reloadMarker("private");
      state.counters.privateSnapshotsBlocked += 1;
      diagnostic("private", "snapshot", { snapshotBytes: originalBytes });
    } else if (originalBytes > MAX_ENTRY_BYTES) {
      item.content = reloadMarker("oversized");
      state.counters.oversizedSnapshots += 1;
      diagnostic("oversized", "snapshot", { snapshotBytes: originalBytes });
    }

    while (cache.length >= MAX_ENTRIES) {
      cache.shift();
      state.counters.entriesEvicted += 1;
    }
    var candidate = cache.concat([item]);
    var serialized;
    try {
      serialized = JSON.stringify(candidate);
    } catch (error) {
      cache.splice(0, cache.length);
      state.fallbackPending = true;
      disableCache("unexpected", "serialize", error);
      return;
    }
    while (cache.length && bytes(serialized) > MAX_CACHE_BYTES) {
      cache.shift();
      candidate = cache.concat([item]);
      serialized = JSON.stringify(candidate);
      state.counters.entriesEvicted += 1;
    }
    state.lastCacheBytes = bytes(serialized);

    var storage = browserStorage();
    if (!storage) {
      cache.splice(0, cache.length);
      state.fallbackPending = true;
      disableCache("unavailable", "preflight", null, {
        snapshotBytes: originalBytes,
        cacheBytes: state.lastCacheBytes,
        cacheEntries: candidate.length,
      });
      return;
    }
    try {
      // Preflight the exact bounded payload before HTMX performs its own write.
      // On failure, historyCacheSize=0 and this emptied shared array make HTMX
      // skip its retry/error loop for the current navigation.
      storage.setItem(CACHE_KEY, serialized);
    } catch (error) {
      cache.splice(0, cache.length);
      state.fallbackPending = true;
      disableCache(errorClass(error), "preflight", error, {
        snapshotBytes: originalBytes,
        cacheBytes: state.lastCacheBytes,
        cacheEntries: candidate.length,
      });
    }
  }

  function onHistoryCacheError(event) {
    var detail = event.detail || {};
    var cache = detail.cache;
    if (Array.isArray(cache)) cache.splice(0, cache.length);
    state.counters.historyCacheErrors += 1;
    state.fallbackPending = true;
    disableCache(errorClass(detail.cause), "htmx-write", detail.cause, {
      cacheEntries: Array.isArray(cache) ? cache.length : 0,
    });
  }

  function onBeforeSwap(event) {
    if (!state.fallbackPending) return;
    var detail = event.detail || {};
    var target = detail.target || (detail.ctx && detail.ctx.target);
    if (typeof target === "string") target = document.querySelector(target);
    if (target && target.id !== "app-view") return;
    detail.shouldSwap = false;
    event.preventDefault();
    state.fallbackPending = false;
    var pathInfo = detail.pathInfo || {};
    var destination = pathInfo.responsePath ||
      pathInfo.finalRequestPath ||
      pathInfo.requestPath ||
      "";
    scheduleReload("storage-failure", destination);
  }

  function onHistoryCacheHit(event) {
    var item = event.detail && event.detail.item;
    if (!item || String(item.content || "").indexOf(RELOAD_MARKER) === -1) return;
    event.preventDefault();
    scheduleReload("bounded-snapshot");
  }

  function onHistoryCacheMiss(event) {
    // HTMX's default miss path performs an XHR swap. PA instead reloads the
    // document so server-rendered live state and controller ownership restart
    // from one clean lifecycle.
    event.preventDefault();
    state.counters.cacheMissReloads += 1;
    diagnostic("miss", "restore");
    scheduleReload("cache-miss");
  }

  function snapshot() {
    return {
      schema: "pa.history-policy/v1",
      policy: {
        max_entries: MAX_ENTRIES,
        max_entry_bytes: MAX_ENTRY_BYTES,
        max_cache_bytes: MAX_CACHE_BYTES,
      },
      disabled: state.disabled,
      reload_scheduled: state.reloadScheduled,
      last_snapshot_bytes: state.lastSnapshotBytes,
      last_cache_bytes: state.lastCacheBytes,
      last_diagnostic: state.lastDiagnostic,
      counters: Object.assign({}, state.counters),
    };
  }

  document.addEventListener("htmx:historyItemCreated", onHistoryItemCreated);
  document.addEventListener("htmx:historyCacheError", onHistoryCacheError);
  document.addEventListener("htmx:beforeSwap", onBeforeSwap);
  document.addEventListener("htmx:historyCacheHit", onHistoryCacheHit);
  document.addEventListener("htmx:historyCacheMiss", onHistoryCacheMiss);
  document.addEventListener("DOMContentLoaded", probe);

  window.PAHistoryPolicy = { snapshot: snapshot, probe: probe };
})();
