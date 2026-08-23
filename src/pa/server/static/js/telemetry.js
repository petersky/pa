(function () {
  "use strict";

  const COLORS = ["#4f7cff", "#e96f92", "#23a896", "#d18a22", "#8e6ee8", "#5d9d3b", "#d75b48", "#607d8b"];
  const ACCESSIBLE_PAGE_SIZE = 100;
  const MAX_CHART_PATH_POINTS = 720;
  const MAX_CHART_MARKERS = 160;
  const DEFAULT_VISIBLE_SERIES = 8;
  const DASH_PATTERNS = ["", "8 4", "2 3", "10 3 2 3", "1 3", "6 2 1 2"];
  const GAP_LABELS = {
    no_sample: "No sample",
    unsupported: "Unsupported",
    temporarily_unavailable: "Temporarily unavailable",
    stale: "Stale",
    restart: "Collector restart",
    peer_failure: "Peer failure"
  };
  const headerHistory = {};
  let reportCursor = null;

  function metric(sample, name) {
    return sample && sample.metrics && sample.metrics[name] || null;
  }

  function finiteValue(value) {
    if (value === null || value === undefined) return null;
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
  }

  function human(value, unit) {
    value = finiteValue(value);
    if (value === null) return "—";
    if (unit === "percent" || unit === "percent_of_one_core") return value.toFixed(value >= 10 ? 0 : 1) + "%";
    if (unit === "bytes") {
      const names = ["B", "KiB", "MiB", "GiB", "TiB"]; let i = 0;
      while (Math.abs(value) >= 1024 && i < names.length - 1) { value /= 1024; i += 1; }
      return value.toFixed(value >= 10 || i === 0 ? 0 : 1) + " " + names[i];
    }
    if (unit === "bytes/second") return human(value, "bytes") + "/s";
    if (unit === "operations/second") return value.toFixed(1) + " IOPS";
    if (unit === "milliseconds/operation") return value.toFixed(1) + " ms/op";
    return value.toFixed(value >= 10 ? 0 : 1) + " " + (unit || "");
  }

  function missingReason(point) {
    if (!point) return "no_sample";
    const valueCount = Number(point.value_count || 0);
    if (valueCount > 0 && finiteValue(point.avg) !== null) return null;
    if (point.quality === "unsupported") return "unsupported";
    if (point.quality === "unavailable") return "temporarily_unavailable";
    if (valueCount === 0) return point.missing_reason || "no_sample";
    if (point.avg === null || point.avg === undefined) return point.missing_reason || "no_sample";
    return point.missing_reason || null;
  }

  function partialReason(point) {
    if (!point || missingReason(point)) return null;
    if (point.partial_reason) return point.partial_reason;
    if (point.quality === "unsupported") return "unsupported";
    if (point.quality === "unavailable") return "temporarily_unavailable";
    if (Number(point.value_count) < Number(point.sample_count)) return "no_sample";
    return null;
  }

  function normalizeObservation(point) {
    const timestamp = Date.parse(point && point.timestamp);
    const reason = missingReason(point);
    if (!Number.isFinite(timestamp)) {
      return {timestamp: timestamp, value: null, reason: reason || "no_sample", point: point};
    }
    if (reason) return {timestamp: timestamp, value: null, reason: reason, point: point};
    const value = finiteValue(point.avg);
    return {
      timestamp: timestamp,
      value: value,
      reason: value === null ? "no_sample" : null,
      partialReason: partialReason(point),
      point: point,
      state: value === 0 ? "genuine_zero" : "observed"
    };
  }

  function gapLabel(reason) {
    return GAP_LABELS[reason] || GAP_LABELS.no_sample;
  }

  function inDomainTimestamp(timestamp, domainStart, domainEnd) {
    const start = Date.parse(domainStart);
    const end = Date.parse(domainEnd);
    return Number.isFinite(timestamp) && Number.isFinite(start) && Number.isFinite(end) &&
      timestamp >= start && timestamp <= end;
  }

  function timeX(timestamp, domainStart, domainEnd, width) {
    const start = Date.parse(domainStart);
    const end = Date.parse(domainEnd);
    if (!inDomainTimestamp(timestamp, domainStart, domainEnd)) return null;
    const span = end - start || 1;
    return 42 + (timestamp - start) / span * ((width || 800) - 58);
  }

  function sparkline(path, values) {
    if (!path || values.length < 2) { if (path) path.setAttribute("d", ""); return; }
    const min = Math.min.apply(null, values);
    const max = Math.max.apply(null, values);
    const span = max - min || 1;
    path.setAttribute("d", values.map(function (value, index) {
      const x = index / (values.length - 1) * 46 + 1;
      const y = 15 - (value - min) / span * 13;
      return (index ? "L" : "M") + x.toFixed(1) + "," + y.toFixed(1);
    }).join(" "));
  }

  function refreshHeader() {
    const root = document.querySelector("[data-telemetry-header]");
    if (!root || document.hidden) return;
    fetch("/api/telemetry/live?scope_type=instance", {credentials: "same-origin", cache: "no-store"})
      .then(function (response) { return response.ok ? response.json() : null; })
      .then(function (data) {
        const sample = data && data.samples && data.samples[0];
        root.querySelectorAll("[data-telemetry-mini]").forEach(function (item) {
          const name = item.dataset.telemetryMini;
          const current = metric(sample, name);
          const value = finiteValue(current && current.value);
          const history = headerHistory[name] || (headerHistory[name] = []);
          if (value !== null) {
            history.push(value); if (history.length > 24) history.shift();
          }
          sparkline(item.querySelector("[data-sparkline]"), history);
          item.querySelector("[data-telemetry-value]").textContent = current
            ? human(current.value, current.unit) : "—";
          item.dataset.quality = current ? current.quality : "unavailable";
        });
      }).catch(function () {});
  }

  function refreshSessions(scope) {
    (scope || document).querySelectorAll("[data-session-telemetry]").forEach(function (panel) {
      const widget = panel.closest("[data-agent-chat]");
      const sessionId = widget && widget.dataset.sessionId;
      if (!sessionId || panel.dataset.loading === "1") return;
      panel.dataset.loading = "1";
      fetch("/api/telemetry/live?scope_type=session&scope_id=" + encodeURIComponent(sessionId), {
        credentials: "same-origin", cache: "no-store"
      }).then(function (response) { return response.ok ? response.json() : null; })
        .then(function (data) {
          const sample = data && data.samples && data.samples[0];
          const summary = panel.querySelector("[data-session-telemetry-summary]");
          const values = panel.querySelector("[data-session-telemetry-values]");
          if (!sample) {
            summary.textContent = "Attribution unavailable";
            values.textContent = "No verified live provider process tree.";
            return;
          }
          const cpu = metric(sample, "session.cpu");
          const memory = metric(sample, "session.memory_rss");
          summary.textContent = "CPU " + human(cpu && cpu.value, cpu && cpu.unit) +
            " · Memory " + human(memory && memory.value, memory && memory.unit) +
            (sample.ownership === "verified_root_and_process_tree" ? "" : " · partial");
          const names = [
            ["session.cpu", "CPU"], ["session.memory_rss", "Memory"],
            ["session.disk_read", "Disk read"], ["session.disk_write", "Disk write"],
            ["session.network_ingress", "Network"], ["session.processes", "Processes"],
            ["session.tasks", "Tasks"]
          ];
          values.replaceChildren();
          names.forEach(function (entry) {
            const item = metric(sample, entry[0]);
            const cell = document.createElement("div");
            cell.dataset.quality = item ? item.quality : "unavailable";
            const label = document.createElement("span"); label.textContent = entry[1];
            const output = document.createElement("strong");
            output.textContent = item && item.value !== null
              ? human(item.value, item.unit) : (item ? item.quality : "unavailable");
            cell.title = item && item.detail || (item && item.source) || "";
            cell.append(label, output); values.appendChild(cell);
          });
        }).catch(function () {}).finally(function () { panel.dataset.loading = "0"; });
    });
  }

  function reportBody(report, form) {
    const values = new FormData(form);
    const body = {
      range: values.get("range") || report.dataset.defaultRange || "1h",
      scope_type: values.get("scope_id") ? "session" : null,
      scope_ids: values.get("scope_id") ? [values.get("scope_id")] : [],
      instance_ids: Array.from(form.instance_id.selectedOptions).map(function (o) { return o.value; }).filter(Boolean),
      provider_ids: values.get("provider_id") ? [values.get("provider_id")] : [],
      card_ids: values.get("card_id") ? [values.get("card_id")] : [],
      metrics: []
    };
    if (body.range === "custom") {
      body.start = values.get("start") ? new Date(values.get("start")).toISOString() : null;
      body.end = values.get("end") ? new Date(values.get("end")).toISOString() : null;
    }
    return body;
  }

  function gapInterrupts(gaps, previousTime, currentTime) {
    if (previousTime === null) return false;
    return (gaps || []).some(function (gap) {
      if (gap.partial) return false;
      const start = Date.parse(gap.start); const end = Date.parse(gap.end);
      return Number.isFinite(start) && Number.isFinite(end) &&
        start <= currentTime && end > previousTime;
    });
  }

  function lineSegments(points, width, height, min, max, bucketSeconds, domainStart, domainEnd, gaps) {
    const seen = new Set();
    const observations = (points || []).map(normalizeObservation).filter(function (item) {
      if (!inDomainTimestamp(item.timestamp, domainStart, domainEnd) ||
          seen.has(item.timestamp)) return false;
      seen.add(item.timestamp); return true;
    }).sort(function (a, b) { return a.timestamp - b.timestamp; });
    const segments = []; let current = []; let previousTime = null;
    observations.forEach(function (observation) {
      const interrupted = observation.reason ||
        gapInterrupts(gaps, previousTime, observation.timestamp) ||
        (previousTime !== null && observation.timestamp - previousTime > bucketSeconds * 2500) ||
        (observation.point && observation.point.restart);
      if (interrupted && current.length) { segments.push(current); current = []; }
      if (!observation.reason) {
        current.push({
          x: timeX(observation.timestamp, domainStart, domainEnd, width),
          y: 12 + (1 - (observation.value - min) / (max - min || 1)) * (height - 36),
          point: observation.point,
          observation: observation
        });
      }
      previousTime = observation.timestamp;
    });
    if (current.length) segments.push(current);
    return segments;
  }

  function evenlySample(items, limit) {
    if (limit <= 0) return [];
    if (items.length <= limit) return items.slice();
    if (limit === 1) return [items[0]];
    const sampled = [];
    for (let index = 0; index < limit; index += 1) {
      sampled.push(items[Math.round(index * (items.length - 1) / (limit - 1))]);
    }
    return sampled;
  }

  function downsamplePoints(points, limit) {
    if (limit <= 0) return [];
    if (points.length <= limit) return points.slice();
    if (limit <= 2) return evenlySample(points, limit);
    const interior = points.slice(1, -1);
    const bucketCount = Math.max(1, Math.floor((limit - 2) / 2));
    const selected = [points[0]];
    for (let bucket = 0; bucket < bucketCount; bucket += 1) {
      const from = Math.floor(bucket * interior.length / bucketCount);
      const to = Math.max(from + 1, Math.floor((bucket + 1) * interior.length / bucketCount));
      const candidates = interior.slice(from, to);
      let minimum = 0; let maximum = 0;
      candidates.forEach(function (point, index) {
        if (point.y < candidates[minimum].y) minimum = index;
        if (point.y > candidates[maximum].y) maximum = index;
      });
      [minimum, maximum].sort(function (a, b) { return a - b; }).forEach(function (index, order) {
        if (order === 0 || index !== minimum) selected.push(candidates[index]);
      });
    }
    selected.push(points[points.length - 1]);
    return selected.length > limit ? evenlySample(selected, limit) : selected;
  }

  function boundedSegments(segments, limit) {
    if (limit <= 0 || !segments.length) return [];
    const total = segments.reduce(function (count, segment) { return count + segment.length; }, 0);
    if (total <= limit) return segments.map(function (segment) { return segment.slice(); });
    if (segments.length >= limit) {
      return evenlySample(segments, limit).map(function (segment) { return [segment[0]]; });
    }
    let remaining = limit;
    let remainingSegments = segments.length;
    return segments.map(function (segment) {
      const allocation = Math.max(1, Math.floor(remaining / remainingSegments));
      remaining -= allocation; remainingSegments -= 1;
      return downsamplePoints(segment, allocation);
    });
  }

  function addAxisText(axes, x, y, value, anchor) {
    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", x); label.setAttribute("y", y);
    label.setAttribute("text-anchor", anchor || "start");
    label.textContent = value; axes.appendChild(label);
  }

  function seriesName(series) {
    const identity = series.instance_name || "Unknown instance";
    const scope = series.scope_type === "session" ? " · " + (series.scope_id || "session").slice(0, 8) : "";
    return identity + scope + " · " + series.metric;
  }

  function seriesKey(series) {
    return [series.instance_id, series.scope_type, series.scope_id, series.metric, series.unit].join("|");
  }

  function syncUrl(form, visibleKeys) {
    const url = new URL(window.location.href);
    ["view", "provider_id", "card_id", "scope_id", "range", "start", "end"].forEach(function (name) {
      const value = form.elements[name] && form.elements[name].value;
      if (value) url.searchParams.set(name, value); else url.searchParams.delete(name);
    });
    url.searchParams.delete("instance_id");
    Array.from(form.instance_id.selectedOptions).map(function (option) { return option.value; })
      .filter(Boolean).forEach(function (value) { url.searchParams.append("instance_id", value); });
    url.searchParams.delete("series");
    (visibleKeys || []).forEach(function (value) { url.searchParams.append("series", value); });
    window.history.replaceState(null, "", url);
    const exportLink = document.querySelector("[data-telemetry-export]");
    if (exportLink) {
      const output = new URL("/api/telemetry/export", window.location.origin);
      ["range", "provider_id", "card_id", "scope_id"].forEach(function (name) {
        const value = form.elements[name] && form.elements[name].value;
        if (value) output.searchParams.append(name === "scope_id" ? "scope_id" : name, value);
      });
      Array.from(form.instance_id.selectedOptions).forEach(function (option) {
        if (option.value) output.searchParams.append("instance_id", option.value);
      });
      const metrics = new Set((visibleKeys || []).map(function (key) { return key.split("|")[3]; }).filter(Boolean));
      metrics.forEach(function (metricName) { output.searchParams.append("metric", metricName); });
      exportLink.href = output.pathname + output.search;
    }
  }

  function restoreUrl(form) {
    const values = new URL(window.location.href).searchParams;
    ["view", "provider_id", "card_id", "scope_id", "range", "start", "end"].forEach(function (name) {
      if (values.has(name) && form.elements[name]) form.elements[name].value = values.get(name);
    });
    const instances = new Set(values.getAll("instance_id"));
    if (instances.size) Array.from(form.instance_id.options).forEach(function (option) {
      option.selected = instances.has(option.value);
    });
    return values.getAll("series");
  }

  function matchingGap(series, timestamp) {
    return (series.gaps || []).find(function (gap) {
      return Date.parse(gap.start) <= timestamp && timestamp < Date.parse(gap.end);
    }) || null;
  }

  function clippedRange(start, end, domainStart, domainEnd) {
    const clippedStart = Math.max(Date.parse(start), Date.parse(domainStart));
    const clippedEnd = Math.min(Date.parse(end), Date.parse(domainEnd));
    if (!Number.isFinite(clippedStart) || !Number.isFinite(clippedEnd) ||
        clippedEnd <= clippedStart) return null;
    return {start: clippedStart, end: clippedEnd};
  }

  function accessibleCounts(data) {
    let observationCount = 0; let gapCount = 0;
    (data.series || []).forEach(function (series) {
      (series.points || []).forEach(function (point) {
        if (inDomainTimestamp(Date.parse(point.timestamp), data.start, data.end)) {
          observationCount += 1;
        }
      });
      (series.gaps || []).forEach(function (gap) {
        if (clippedRange(gap.start, gap.end, data.start, data.end)) gapCount += 1;
      });
    });
    (data.failures || []).forEach(function (failure) {
      if (clippedRange(
        failure.start || data.start, failure.end || data.end, data.start, data.end
      )) gapCount += 1;
    });
    return {
      observations: observationCount,
      gaps: gapCount,
      total: observationCount + gapCount
    };
  }

  function accessiblePage(data, page, pageSize) {
    const counts = accessibleCounts(data);
    const size = Math.max(1, pageSize || ACCESSIBLE_PAGE_SIZE);
    const pageCount = Math.max(1, Math.ceil(counts.total / size));
    const currentPage = Math.max(0, Math.min(pageCount - 1, page || 0));
    const offset = currentPage * size;
    const rows = []; let index = 0;
    function take() {
      const materialize = index >= offset && index < offset + size;
      index += 1;
      return materialize;
    }
    function add(values, gapReason) {
      rows.push({values: values, gapReason: gapReason || null});
    }
    (data.series || []).forEach(function (series) {
      (series.points || []).forEach(function (point) {
        const timestamp = Date.parse(point.timestamp);
        if (!inDomainTimestamp(timestamp, data.start, data.end) || !take()) return;
        const observation = normalizeObservation(point);
        let status = observation.reason ? gapLabel(observation.reason) :
          human(observation.value, series.unit) +
          (observation.state === "genuine_zero" ? " (genuine measured zero)" : "");
        if (observation.partialReason) {
          status += " (partial: " + gapLabel(observation.partialReason).toLowerCase() + ")";
        }
        add([
          new Date(observation.timestamp).toLocaleString(),
          seriesName(series),
          series.unit,
          status,
          (point.quality || "no sample") +
            (observation.partialReason ? " · partial" : "")
        ], observation.reason);
      });
      (series.gaps || []).forEach(function (gap) {
        const range = clippedRange(gap.start, gap.end, data.start, data.end);
        if (!range || !take()) return;
        add([
          new Date(range.start).toLocaleString() + " – " + new Date(range.end).toLocaleString(),
          seriesName(series),
          series.unit,
          gapLabel(gap.reason) + (gap.partial ? " (partial interval)" : ""),
          gap.partial ? "partial gap" : "gap"
        ], gap.reason);
      });
    });
    (data.failures || []).forEach(function (failure) {
      const range = clippedRange(
        failure.start || data.start, failure.end || data.end, data.start, data.end
      );
      if (!range || !take()) return;
      add([
        new Date(range.start).toLocaleString() + " – " + new Date(range.end).toLocaleString(),
        failure.instance_id,
        "—",
        gapLabel(failure.reason || "peer_failure"),
        "gap"
      ], failure.reason || "peer_failure");
    });
    return {
      rows: rows,
      counts: counts,
      page: currentPage,
      pageCount: pageCount,
      first: counts.total ? offset + 1 : 0,
      last: Math.min(counts.total, offset + rows.length)
    };
  }

  function drawAccessibleTable(report, data, page) {
    const body = report.querySelector("[data-telemetry-table-body]");
    const summary = report.querySelector("[data-telemetry-summary]");
    const previous = report.querySelector("[data-telemetry-table-prev]");
    const next = report.querySelector("[data-telemetry-table-next]");
    const pageStatus = report.querySelector("[data-telemetry-table-page]");
    if (!body || !summary) return;
    const windowed = accessiblePage(data, page, ACCESSIBLE_PAGE_SIZE);
    body.replaceChildren();
    windowed.rows.forEach(function (descriptor) {
      const row = document.createElement("tr");
      if (descriptor.gapReason) row.dataset.gapReason = descriptor.gapReason;
      descriptor.values.forEach(function (value) {
        const cell = document.createElement("td"); cell.textContent = value; row.appendChild(cell);
      });
      body.appendChild(row);
    });
    report._accessiblePage = windowed.page;
    summary.textContent = windowed.counts.observations + " observations and " +
      windowed.counts.gaps + " typed gaps across " + (data.series || []).length +
      " series. Showing rows " + windowed.first + "–" + windowed.last +
      " of " + windowed.counts.total + ".";
    if (previous) previous.disabled = windowed.page === 0;
    if (next) next.disabled = windowed.page >= windowed.pageCount - 1;
    if (pageStatus) pageStatus.textContent = "Page " + (windowed.page + 1) +
      " of " + windowed.pageCount + ".";
  }

  function drawReport(report, data) {
    const allSeries = data.series || [];
    if (!report._visibleKeys) {
      const requested = report._requestedSeries || [];
      report._visibleKeys = new Set(requested.length ? requested : allSeries.slice(0, DEFAULT_VISIBLE_SERIES).map(seriesKey));
    }
    const visibleSeries = allSeries.filter(function (series) { return report._visibleKeys.has(seriesKey(series)); });
    const domainStart = data.start, domainEnd = data.end;
    const diagnostics = report.querySelector("[data-report-diagnostics]");
    const domainPoints = allSeries.flatMap(function (series) { return series.points || []; }).filter(function (point) {
      return inDomainTimestamp(Date.parse(point.timestamp), domainStart, domainEnd);
    });
    const pointCount = domainPoints.length;
    const bucketCount = new Set(domainPoints.map(function (point) { return point.timestamp; })).size;
    const newest = domainPoints.slice().sort(function (a, b) {
      return Date.parse(b.timestamp) - Date.parse(a.timestamp);
    })[0];
    const newestAge = newest ? Date.now() - Date.parse(newest.timestamp) : Infinity;
    const freshness = !newest ? "unavailable" : newestAge <= Number(data.bucket_seconds || 60) * 2000 ? "sampled (current)" : "stale";
    if (diagnostics) diagnostics.textContent = "Range " + new Date(domainStart).toLocaleString() + " – " + new Date(domainEnd).toLocaleString() + " · " + bucketCount + " buckets · " + allSeries.length + " series · " + pointCount + " points · " + freshness + (newest ? " · newest " + new Date(newest.timestamp).toLocaleString() : " · no collected samples");
    const legend = report.querySelector("[data-telemetry-legend]");
    legend.replaceChildren();
    allSeries.forEach(function (series, index) {
      const item = document.createElement("span"); item.className = "telemetry-legend-item";
      const swatch = document.createElement("i");
      swatch.style.background = COLORS[index % COLORS.length];
      swatch.dataset.pattern = String(index % DASH_PATTERNS.length);
      const toggle = document.createElement("button"); toggle.type = "button";
      toggle.textContent = seriesName(series);
      toggle.setAttribute("aria-pressed", report._visibleKeys.has(seriesKey(series)) ? "true" : "false");
      toggle.addEventListener("click", function () {
        const key = seriesKey(series);
        if (report._visibleKeys.has(key)) report._visibleKeys.delete(key); else report._visibleKeys.add(key);
        drawReport(report, data); drawAccessibleTable(report, data, 0);
        syncUrl(document.querySelector("[data-telemetry-filters]"), Array.from(report._visibleKeys));
      });
      const solo = document.createElement("button"); solo.type = "button";
      solo.className = "text-btn telemetry-series-solo"; solo.textContent = "Solo";
      solo.setAttribute("aria-label", "Show only " + seriesName(series));
      solo.addEventListener("click", function () {
        report._visibleKeys = new Set([seriesKey(series)]);
        drawReport(report, data); drawAccessibleTable(report, data, 0);
        syncUrl(document.querySelector("[data-telemetry-filters]"), Array.from(report._visibleKeys));
      });
      item.append(swatch, toggle, solo);
      legend.appendChild(item);
    });
    let hasGaps = (data.failures || []).length > 0;
    report.querySelectorAll("[data-chart-group]").forEach(function (chart) {
      const names = chart.dataset.metrics.split(",");
      const unit = chart.dataset.unit;
      const selected = visibleSeries.filter(function (series) {
        return names.indexOf(series.metric) >= 0 && series.unit === unit;
      });
      const lines = chart.querySelector("[data-chart-lines]");
      const pointsLayer = chart.querySelector("[data-chart-points]");
      const empty = chart.querySelector("[data-chart-empty]");
      const status = chart.querySelector("[data-chart-status]");
      const grid = chart.querySelector("[data-chart-grid]"), axes = chart.querySelector("[data-chart-axes]");
      grid.replaceChildren(); axes.replaceChildren();
      [12, 58, 104, 150, 196].forEach(function (y) {
        const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
        line.setAttribute("x1", "42"); line.setAttribute("x2", "784");
        line.setAttribute("y1", y); line.setAttribute("y2", y); grid.appendChild(line);
      });
      lines.replaceChildren(); pointsLayer.replaceChildren(); status.replaceChildren();
      chart._series = selected; chart._domain = [domainStart, domainEnd];
      chart._cursorTimestamps = [];
      chart.querySelector("[data-chart-unit]").textContent = unit;
      addAxisText(axes, "42", "216", new Date(domainStart).toLocaleString(), "start");
      addAxisText(axes, "784", "216", new Date(domainEnd).toLocaleString(), "end");
      if (!selected.length) {
        empty.hidden = false;
        empty.textContent = names.some(function (name) { return name === "session.processes" || name === "session.tasks"; })
          ? "Unavailable—not zero. Enable per-session telemetry and verify provider process ownership, then retry."
          : "No sample for this metric and unit in the requested interval.";
        hasGaps = true; return;
      }
      const values = selected.flatMap(function (series) {
        return (series.points || []).map(normalizeObservation).filter(function (item) {
          return !item.reason &&
            inDomainTimestamp(item.timestamp, domainStart, domainEnd);
        }).map(function (item) { return item.value; });
      });
      if (!values.length) {
        empty.hidden = false;
        empty.textContent = "All observations are missing; see typed gap reasons below.";
      } else {
        empty.hidden = true;
      }
      let min = values.length ? Math.min.apply(null, values) : 0;
      let max = values.length ? Math.max.apply(null, values) : 1;
      if (min === max && min === 0) {
        max = 1;
      } else if (min === max) {
        const padding = Math.abs(min) * 0.1 || 1;
        min = Math.max(0, min - padding); max += padding;
      }
      if (values.length) {
        addAxisText(axes, "38", "16", human(max, unit), "end");
        addAxisText(axes, "38", "198", human(min, unit), "end");
      }
      const pathBudget = Math.max(1, Math.floor(MAX_CHART_PATH_POINTS / selected.length));
      const markerBudget = Math.floor(MAX_CHART_MARKERS / selected.length);
      selected.forEach(function (series) {
        const seriesBucketSeconds = Number(series.bucket_seconds || data.bucket_seconds);
        const rawSegments = lineSegments(
          series.points, 800, 220, min, max, seriesBucketSeconds,
          domainStart, domainEnd, series.gaps
        );
        const segments = boundedSegments(rawSegments, pathBudget);
        chart._cursorTimestamps.push.apply(chart._cursorTimestamps,
          segments.flat().map(function (item) { return item.observation.timestamp; }));
        const seriesGaps = series.gaps || [];
        const color = COLORS[allSeries.indexOf(series) % COLORS.length];
        const dash = DASH_PATTERNS[allSeries.indexOf(series) % DASH_PATTERNS.length];
        if (seriesGaps.length || rawSegments.length > 1) hasGaps = true;
        segments.forEach(function (segment) {
          const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
          path.setAttribute("d", segment.map(function (point, i) {
            return (i ? "L" : "M") + point.x.toFixed(1) + "," + point.y.toFixed(1);
          }).join(" "));
          path.setAttribute("stroke", color);
          if (dash) path.setAttribute("stroke-dasharray", dash);
          path.dataset.seriesIndex = String(allSeries.indexOf(series));
          lines.appendChild(path);
        });
        downsamplePoints(segments.flat(), markerBudget).forEach(function (item) {
          const point = document.createElementNS("http://www.w3.org/2000/svg", "circle");
          point.setAttribute("cx", item.x.toFixed(1)); point.setAttribute("cy", item.y.toFixed(1));
          point.setAttribute("r", rawSegments.length === 1 && rawSegments[0].length === 1 ? "4" : "2.5");
          point.setAttribute("fill", allSeries.indexOf(series) % 2 ? "none" : color);
          point.setAttribute("stroke", color);
          point.dataset.seriesIndex = String(allSeries.indexOf(series));
          point.dataset.timestamp = item.point.timestamp;
          pointsLayer.appendChild(point);
        });
        const counts = {};
        seriesGaps.forEach(function (gap) { counts[gap.reason] = (counts[gap.reason] || 0) + 1; });
        Object.keys(counts).forEach(function (reason) {
          const badge = document.createElement("span");
          badge.className = "badge"; badge.dataset.gapReason = reason;
          badge.textContent = series.metric + " · " + counts[reason] + " " + gapLabel(reason).toLowerCase();
          status.appendChild(badge);
        });
      });
    });
    (data.failures || []).forEach(function (failure) {
      const badge = document.createElement("span");
      badge.className = "badge"; badge.dataset.gapReason = failure.reason || "peer_failure";
      badge.textContent = failure.instance_id + " · " + gapLabel(failure.reason || "peer_failure").toLowerCase();
      const firstStatus = report.querySelector("[data-chart-status]");
      if (firstStatus) firstStatus.appendChild(badge);
    });
    const banner = report.querySelector("[data-telemetry-gaps]");
    banner.hidden = !hasGaps;
    banner.textContent = hasGaps
      ? "The requested interval contains typed sampling gaps. Complete gaps break lines; partial degradation preserves measured aggregates. Each affected range and reason is listed in the accessible table."
      : "";
  }

  function cursorValue(series, time) {
    const exact = (series.points || []).map(normalizeObservation).find(function (item) {
      return item.timestamp === time;
    });
    if (exact && !exact.reason) {
      let value = human(exact.value, series.unit) +
        (exact.state === "genuine_zero"
          ? " (genuine measured zero)"
          : " (" + exact.point.quality + ")");
      if (exact.partialReason) {
        value += " (partial: " + gapLabel(exact.partialReason).toLowerCase() + ")";
      }
      return value;
    }
    const gap = exact && exact.reason ? {reason: exact.reason} : matchingGap(series, time);
    return gapLabel(gap && gap.reason);
  }

  function inspectChart(report, direction, activeStage) {
    const timestamps = Array.from(new Set(Array.from(report.querySelectorAll("[data-chart-group]")).flatMap(function (chart) {
      return chart._cursorTimestamps || [];
    }))).sort(function (a, b) { return a - b; });
    if (!timestamps.length) return;
    reportCursor = reportCursor === null ? timestamps.length - 1 :
      Math.max(0, Math.min(timestamps.length - 1, reportCursor + direction));
    const time = timestamps[reportCursor];
    const activeChart = activeStage && activeStage.closest("[data-chart-group]");
    const liveOutput = report.querySelector("[data-chart-live-output]");
    report.querySelectorAll("[data-chart-group]").forEach(function (candidate) {
      const stage = candidate.querySelector(".telemetry-chart-stage");
      const cursor = candidate.querySelector("[data-chart-cursor]");
      const tooltip = candidate.querySelector("[data-chart-tooltip]");
      const x = timeX(time, candidate._domain[0], candidate._domain[1], 800);
      if (x === null) return;
      cursor.setAttribute("x1", x); cursor.setAttribute("x2", x); cursor.hidden = false;
      const rows = (candidate._series || []).map(function (series) {
        return seriesName(series) + ": " + cursorValue(series, time);
      });
      tooltip.textContent = new Date(time).toLocaleString() + " — " + rows.join(" | ");
      tooltip.hidden = false;
      tooltip.style.left = Math.min(75, Math.max(5, x / 8)) + "%";
      stage.setAttribute("aria-label", tooltip.textContent);
      if (liveOutput && candidate === activeChart) {
        liveOutput.textContent = tooltip.textContent;
      }
    });
  }

  function initReport(scope) {
    const report = (scope || document).querySelector("[data-telemetry-report]");
    const form = document.querySelector("[data-telemetry-filters]");
    if (!report || !form || report.dataset.bound === "1") return;
    report.dataset.bound = "1";
    report._requestedSeries = restoreUrl(form);
    const custom = form.querySelector("[data-custom-range]");
    custom.hidden = form.range.value !== "custom";
    form.range.addEventListener("change", function () { custom.hidden = form.range.value !== "custom"; });
    form.querySelectorAll("[data-filter-search]").forEach(function (search) {
      const select = form.elements[search.dataset.filterSearch];
      search.addEventListener("input", function () {
        const query = search.value.trim().toLocaleLowerCase(); let shown = 0;
        if (search.dataset.filterSearch === "card_id" || search.dataset.filterSearch === "scope_id") {
          window.clearTimeout(search._timer);
          search._timer = window.setTimeout(function () {
            if (search._request) search._request.abort();
            search._request = new AbortController();
            const selected = select.value;
            fetch("/api/telemetry/dimensions/search?kind=" + encodeURIComponent(search.dataset.filterSearch) + "&q=" + encodeURIComponent(query), {
              credentials: "same-origin", signal: search._request.signal
            }).then(function (response) { return response.ok ? response.json() : {results: []}; })
              .then(function (data) {
                select.replaceChildren(new Option(search.dataset.filterSearch === "card_id" ? "All cards" : "All sessions", ""));
                (data.results || []).forEach(function (item) {
                  const option = new Option(item.label + " · " + item.short_id, item.id);
                  option.selected = item.id === selected; select.appendChild(option);
                });
              }).catch(function (error) { if (error.name !== "AbortError") search.setAttribute("aria-invalid", "true"); });
          }, 150);
          return;
        }
        Array.from(select.options).forEach(function (option, index) {
          const match = !query || option.textContent.toLocaleLowerCase().includes(query) || option.value.toLocaleLowerCase().includes(query);
          option.hidden = index > 0 && (!match || shown >= 50);
          if (index > 0 && match && shown < 50) shown += 1;
        });
      });
    });
    report.querySelectorAll(".telemetry-chart-stage").forEach(function (stage) {
      stage.addEventListener("keydown", function (event) {
        if (event.key === "ArrowLeft" || event.key === "ArrowRight") {
          event.preventDefault(); inspectChart(report, event.key === "ArrowLeft" ? -1 : 1, stage);
        }
      });
      stage.addEventListener("click", function () { inspectChart(report, 0, stage); });
    });
    const previousPage = report.querySelector("[data-telemetry-table-prev]");
    const nextPage = report.querySelector("[data-telemetry-table-next]");
    function changeAccessiblePage(direction) {
      if (!report._lastData) return;
      drawAccessibleTable(
        report, report._lastData, (report._accessiblePage || 0) + direction
      );
    }
    if (previousPage) previousPage.addEventListener("click", function () {
      changeAccessiblePage(-1);
    });
    if (nextPage) nextPage.addEventListener("click", function () {
      changeAccessiblePage(1);
    });
    const resetSeries = report.querySelector("[data-series-reset]");
    if (resetSeries) resetSeries.addEventListener("click", function () {
      if (!report._lastData) return;
      report._visibleKeys = new Set((report._lastData.series || []).slice(0, DEFAULT_VISIBLE_SERIES).map(seriesKey));
      drawReport(report, report._lastData); drawAccessibleTable(report, report._lastData, 0);
      syncUrl(form, Array.from(report._visibleKeys));
    });
    report._cleanup = function () {
      if (report._request) report._request.abort();
      report._lastData = null;
    };
    function load() {
      const body = reportBody(report, form);
      syncUrl(form, report._visibleKeys ? Array.from(report._visibleKeys) : report._requestedSeries);
      if (report._request) report._request.abort();
      report._request = new AbortController();
      const fleet = form.view.value === "fleet";
      if (fleet) {
        body.scope_type = "instance"; body.scope_ids = []; body.provider_ids = []; body.card_ids = [];
      }
      fetch(fleet ? "/api/telemetry/fleet/query" : "/api/telemetry/query", {
        method: "POST", credentials: "same-origin",
        headers: Object.assign({"Content-Type": "application/json"}, csrfHeader()),
        body: JSON.stringify(body), signal: report._request.signal
      }).then(function (response) {
        if (!response.ok) return response.json().then(function (value) { throw value; });
        return response.json();
      }).then(function (data) {
        report._lastData = data;
        drawReport(report, data);
        drawAccessibleTable(report, data, 0);
        const health = document.querySelector("[data-report-health]");
        if (health) health.textContent = "Updated " + new Date().toLocaleTimeString() +
          " · " + (data.series || []).length + " series · " + data.bucket_seconds + "s aggregation";
      }).catch(function (error) {
        if (error && error.name === "AbortError") return;
        report.querySelectorAll("[data-chart-empty]").forEach(function (empty) {
          empty.hidden = false; empty.textContent = "Report could not be loaded. Check collection health and retry.";
        });
        const health = document.querySelector("[data-report-health]");
        if (health) health.textContent = "Report unavailable: " +
          (error && error.detail || "request failed");
      });
    }
    form.addEventListener("submit", function (event) { event.preventDefault(); reportCursor = null; load(); });
    const requestedMetric = new URL(window.location.href).searchParams.get("metric");
    if (requestedMetric) report.dataset.focusMetric = requestedMetric;
    load();
  }

  function csrfHeader() {
    if (window.PACSRF) return window.PACSRF.headers();
    const meta = document.querySelector('meta[name="csrf-token"]');
    return meta && meta.content ? {"X-CSRF-Token": meta.content} : {};
  }

  function init(scope) {
    initReport(scope || document);
    refreshSessions(scope || document);
  }

  if (typeof module !== "undefined" && module.exports) {
    module.exports = {
      ACCESSIBLE_PAGE_SIZE: ACCESSIBLE_PAGE_SIZE,
      MAX_CHART_PATH_POINTS: MAX_CHART_PATH_POINTS,
      MAX_CHART_MARKERS: MAX_CHART_MARKERS,
      finiteValue: finiteValue,
      missingReason: missingReason,
      partialReason: partialReason,
      normalizeObservation: normalizeObservation,
      gapLabel: gapLabel,
      inDomainTimestamp: inDomainTimestamp,
      timeX: timeX,
      gapInterrupts: gapInterrupts,
      lineSegments: lineSegments,
      downsamplePoints: downsamplePoints,
      boundedSegments: boundedSegments,
      accessibleCounts: accessibleCounts,
      accessiblePage: accessiblePage,
      cursorValue: cursorValue
    };
  }

  document.body && document.body.addEventListener("htmx:beforeSwap", function (event) {
    const target = event.detail && event.detail.target;
    const report = target && (target.matches("[data-telemetry-report]") ? target : target.querySelector("[data-telemetry-report]"));
    if (report && report._cleanup) report._cleanup();
  });

  document.addEventListener("DOMContentLoaded", function () {
    init(document); refreshHeader();
    const root = document.querySelector("[data-telemetry-header]");
    const seconds = Number(root && root.dataset.refreshSeconds || 5);
    window.setInterval(function () { refreshHeader(); refreshSessions(document); }, Math.max(2, seconds) * 1000);
  });
  document.body && document.body.addEventListener("htmx:afterSwap", function (event) {
    init(event.detail && event.detail.target || document);
  });
})();
