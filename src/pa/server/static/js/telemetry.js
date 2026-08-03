(function () {
  "use strict";

  const COLORS = ["#4f7cff", "#e96f92", "#23a896", "#d18a22", "#8e6ee8", "#5d9d3b", "#d75b48", "#607d8b"];
  const headerHistory = {};
  let reportCursor = null;

  function metric(sample, name) {
    return sample && sample.metrics && sample.metrics[name] || null;
  }

  function human(value, unit) {
    if (value === null || value === undefined || !Number.isFinite(Number(value))) return "—";
    value = Number(value);
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
          const value = current && current.value;
          const history = headerHistory[name] || (headerHistory[name] = []);
          if (Number.isFinite(Number(value))) {
            history.push(Number(value)); if (history.length > 24) history.shift();
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

  function lineSegments(points, width, height, min, max, bucketSeconds) {
    const seen = new Set(); const valid = points.filter(function (p) { const time = Date.parse(p ? p.timestamp : null), value = Number(p ? p.avg : NaN); if (!Number.isFinite(time) || !Number.isFinite(value) || seen.has(time)) return false; seen.add(time); p.avg = value; return true; }).sort(function (a, b) { return Date.parse(a.timestamp) - Date.parse(b.timestamp); });
    if (!valid.length) return [];
    const times = valid.map(function (p) { return Date.parse(p.timestamp); });
    const minTime = Math.min.apply(null, times);
    const maxTime = Math.max.apply(null, times);
    const timeSpan = maxTime - minTime || 1;
    const segments = []; let current = [];
    valid.forEach(function (point, index) {
      const timestamp = Date.parse(point.timestamp);
      if (index && timestamp - Date.parse(valid[index - 1].timestamp) > bucketSeconds * 2500) {
        if (current.length) segments.push(current); current = [];
      }
      current.push({
        x: 42 + (timestamp - minTime) / timeSpan * (width - 58),
        y: 12 + (1 - (Number(point.avg) - min) / (max - min || 1)) * (height - 36),
        point: point
      });
    });
    if (current.length) segments.push(current);
    return segments;
  }

  function drawReport(report, data) {
    const allSeries = data.series || [];
    const diagnostics = report.querySelector("[data-report-diagnostics]");
    const pointCount = allSeries.reduce(function (count, series) { return count + (series.points || []).length; }, 0);
    const bucketCount = new Set(allSeries.flatMap(function (series) { return (series.points || []).map(function (point) { return point.timestamp; }); })).size;
    const newest = allSeries.flatMap(function (series) { return series.points || []; }).sort(function (a, b) { return Date.parse(b.timestamp) - Date.parse(a.timestamp); })[0];
    if (diagnostics) diagnostics.textContent = "Range " + new Date(data.start).toLocaleString() + " – " + new Date(data.end).toLocaleString() + " · " + bucketCount + " buckets · " + allSeries.length + " series · " + pointCount + " points" + (newest ? " · newest " + new Date(newest.timestamp).toLocaleString() : " · no collected samples");
    const legend = report.querySelector("[data-telemetry-legend]");
    legend.replaceChildren();
    allSeries.slice(0, 16).forEach(function (series, index) {
      const item = document.createElement("span");
      const swatch = document.createElement("i");
      swatch.style.background = COLORS[index % COLORS.length];
      item.append(swatch, document.createTextNode(
        (series.instance_name || series.scope_id) + " · " + series.metric
      ));
      legend.appendChild(item);
    });
    let hasGaps = (data.failures || []).length > 0;
    report.querySelectorAll("[data-chart-group]").forEach(function (chart) {
      const names = chart.dataset.metrics.split(",");
      const selected = allSeries.filter(function (series) { return names.indexOf(series.metric) >= 0; });
      const lines = chart.querySelector("[data-chart-lines]");
      const pointsLayer = chart.querySelector("[data-chart-points]");
      const empty = chart.querySelector("[data-chart-empty]");
      const status = chart.querySelector("[data-chart-status]");
      const grid = chart.querySelector("[data-chart-grid]"), axes = chart.querySelector("[data-chart-axes]");
      grid.replaceChildren(); axes.replaceChildren();
      [12, 58, 104, 150, 196].forEach(function (y) { const line = document.createElementNS("http://www.w3.org/2000/svg", "line"); line.setAttribute("x1", "42"); line.setAttribute("x2", "784"); line.setAttribute("y1", y); line.setAttribute("y2", y); grid.appendChild(line); });
      lines.replaceChildren(); pointsLayer.replaceChildren(); status.replaceChildren();
      if (!selected.length) {
        empty.hidden = false; empty.textContent = "This dimension is unavailable for the selected scopes.";
        return;
      }
      empty.hidden = true;
      const values = selected.flatMap(function (series) { return series.points.map(function (point) { return Number(point.avg); }).filter(Number.isFinite); });
      if (!values.length) { empty.hidden = false; empty.textContent = "Samples were returned, but none contain finite values."; return; }
      let min = Math.min.apply(null, values), max = Math.max.apply(null, values);
      if (min === max) { const padding = Math.abs(min) * 0.1 || 1; min -= padding; max += padding; }
      selected.forEach(function (series, index) {
        const segments = lineSegments(series.points, 800, 220, min, max, data.bucket_seconds);
        if (segments.length > 1) hasGaps = true;
        segments.forEach(function (segment) {
          segment.forEach(function (item) {
            const point = document.createElementNS("http://www.w3.org/2000/svg", "circle");
            point.setAttribute("cx", item.x.toFixed(1)); point.setAttribute("cy", item.y.toFixed(1));
            point.setAttribute("r", segment.length === 1 ? "4" : "2.5");
            point.setAttribute("fill", COLORS[index % COLORS.length]);
            point.dataset.seriesIndex = String(allSeries.indexOf(series)); pointsLayer.appendChild(point);
          });
          const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
          path.setAttribute("d", segment.map(function (point, i) {
            return (i ? "L" : "M") + point.x.toFixed(1) + "," + point.y.toFixed(1);
          }).join(" "));
          path.setAttribute("stroke", COLORS[index % COLORS.length]);
          path.dataset.seriesIndex = String(allSeries.indexOf(series));
          lines.appendChild(path);
        });
        const unavailable = series.points.filter(function (point) {
          return point.value_count === 0 || ["unavailable", "unsupported"].indexOf(point.quality) >= 0;
        }).length;
        if (unavailable) {
          const badge = document.createElement("span");
          badge.className = "badge";
          badge.textContent = series.metric + " · " + unavailable + " unavailable";
          status.appendChild(badge);
        }
        if (series.points.some(function (point) { return point.restart; })) hasGaps = true;
      });
      chart._series = selected; chart._bucketSeconds = data.bucket_seconds;
      chart.querySelector("[data-chart-unit]").textContent =
        Array.from(new Set(selected.map(function (series) { return series.unit; }))).join(" · ");
    });
    const banner = report.querySelector("[data-telemetry-gaps]");
    banner.hidden = !hasGaps;
    banner.textContent = hasGaps
      ? "The selected interval contains a sampling gap, restart, stale peer, or unavailable dimension. Lines are intentionally broken across gaps."
      : "";
  }

  function inspectChart(chart, direction) {
    const points = (chart._series || []).flatMap(function (series) {
      return series.points.map(function (point) { return {series: series, point: point}; });
    }).sort(function (a, b) { return Date.parse(a.point.timestamp) - Date.parse(b.point.timestamp); });
    if (!points.length) return;
    reportCursor = reportCursor === null ? points.length - 1 :
      Math.max(0, Math.min(points.length - 1, reportCursor + direction));
    const selected = points[reportCursor];
    const time = Date.parse(selected.point.timestamp);
    document.querySelectorAll("[data-chart-group]").forEach(function (candidate) {
      const svg = candidate.querySelector("svg");
      const cursor = candidate.querySelector("[data-chart-cursor]");
      const tooltip = candidate.querySelector("[data-chart-tooltip]");
      const timestamps = (candidate._series || []).flatMap(function (series) {
        return series.points.map(function (point) { return Date.parse(point.timestamp); });
      });
      if (!timestamps.length) return;
      const min = Math.min.apply(null, timestamps), max = Math.max.apply(null, timestamps);
      const x = 42 + (time - min) / (max - min || 1) * 742;
      cursor.setAttribute("x1", x); cursor.setAttribute("x2", x); cursor.hidden = false;
      const rows = (candidate._series || []).map(function (series) {
        const nearest = series.points.reduce(function (best, point) {
          return !best || Math.abs(Date.parse(point.timestamp) - time) < Math.abs(Date.parse(best.timestamp) - time) ? point : best;
        }, null);
        return (series.instance_name || series.scope_id) + " · " + series.metric + ": " +
          (nearest ? human(nearest.avg, series.unit) + " (" + nearest.quality + ")" : "—");
      });
      tooltip.textContent = new Date(time).toLocaleString() + " — " + rows.join(" | ");
      tooltip.hidden = false;
      tooltip.style.left = Math.min(75, Math.max(5, x / 8)) + "%";
      svg.setAttribute("aria-label", tooltip.textContent);
    });
  }

  function initReport(scope) {
    const report = (scope || document).querySelector("[data-telemetry-report]");
    const form = document.querySelector("[data-telemetry-filters]");
    if (!report || !form || report.dataset.bound === "1") return;
    report.dataset.bound = "1";
    const custom = form.querySelector("[data-custom-range]");
    form.range.addEventListener("change", function () { custom.hidden = form.range.value !== "custom"; });
    report.querySelectorAll(".telemetry-chart-stage").forEach(function (stage) {
      stage.addEventListener("keydown", function (event) {
        if (event.key === "ArrowLeft" || event.key === "ArrowRight") {
          event.preventDefault(); inspectChart(stage.closest("[data-chart-group]"), event.key === "ArrowLeft" ? -1 : 1);
        }
      });
      stage.addEventListener("click", function () { inspectChart(stage.closest("[data-chart-group]"), 0); });
    });
    const resizeObserver = typeof ResizeObserver === "undefined" ? null : new ResizeObserver(function () { if (report.isConnected && report._lastData && report.getBoundingClientRect().width > 0) drawReport(report, report._lastData); });
    if (resizeObserver) resizeObserver.observe(report);
    report._cleanup = function () { if (report._request) report._request.abort(); if (resizeObserver) resizeObserver.disconnect(); report._lastData = null; };
    function load() {
      const body = reportBody(report, form);
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
        if (report.getBoundingClientRect().width > 0) drawReport(report, data);
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
    const meta = document.querySelector('meta[name="csrf-token"]');
    return meta && meta.content ? {"X-CSRF-Token": meta.content} : {};
  }

  function init(scope) {
    initReport(scope || document);
    refreshSessions(scope || document);
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
