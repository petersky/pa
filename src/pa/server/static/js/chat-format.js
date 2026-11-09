/* Presentation of chat bubbles: readable JSON, inline media and clipboard.
   Display only. A bubble's stored markdown is never rewritten, so copying and
   raw-text mode always give back exactly what was said. */
(function (root) {
  "use strict";

  const MAX_JSON_CHARS = 200000;
  const MAX_JSON_ATTEMPTS = 24;
  const MIN_EMBEDDED_JSON_CHARS = 32;
  const COLLAPSE_JSON_LINES = 12;
  const MAX_MEDIA_PER_BUBBLE = 6;
  const MEDIA_KINDS = [
    ["image", /\.(png|jpe?g|gif|webp|avif|bmp)$/i],
    ["video", /\.(mp4|webm|mov|m4v|ogv)$/i],
    ["audio", /\.(mp3|wav|ogg|oga|m4a|aac|flac|opus)$/i],
  ];
  const PREVIEW_KEYS = ["payload", "message", "summary", "title", "text", "description"];

  /* The end (exclusive) of the balanced value starting at `start`, or -1. */
  function jsonEnd(text, start) {
    let depth = 0, inString = false, escaped = false;
    const limit = Math.min(text.length, start + MAX_JSON_CHARS);
    for (let index = start; index < limit; index++) {
      const char = text[index];
      if (inString) {
        if (escaped) escaped = false;
        else if (char === "\\") escaped = true;
        else if (char === '"') inString = false;
      } else if (char === '"') inString = true;
      else if (char === "{" || char === "[") depth++;
      else if (char === "}" || char === "]") {
        depth--;
        if (depth === 0) return index + 1;
      }
    }
    return -1;
  }

  /* Re-indents the original tokens. Parsing and re-serializing would silently
     round large integers such as 19-digit IDs. */
  function prettyJson(raw) {
    let out = "", depth = 0, inString = false, escaped = false;
    const newline = function () { return "\n" + "  ".repeat(depth); };
    for (let index = 0; index < raw.length; index++) {
      const char = raw[index];
      if (inString) {
        out += char;
        if (escaped) escaped = false;
        else if (char === "\\") escaped = true;
        else if (char === '"') inString = false;
      } else if (/\s/.test(char)) {
        continue;
      } else if (char === '"') {
        inString = true;
        out += char;
      } else if (char === "{" || char === "[") {
        const next = raw.slice(index + 1).match(/^\s*(.)/);
        if (next && (next[1] === "}" || next[1] === "]")) {
          out += char;
        } else {
          depth++;
          out += char + newline();
        }
      } else if (char === "}" || char === "]") {
        if (/[{[]$/.test(out)) {
          out += char;
        } else {
          depth--;
          out += newline() + char;
        }
      } else if (char === ",") {
        out += char + newline();
      } else if (char === ":") {
        out += ": ";
      } else {
        out += char;
      }
    }
    return out;
  }

  function parseJson(text) {
    try {
      const value = JSON.parse(text);
      return value && typeof value === "object" ? { value: value } : null;
    } catch (_) {
      return null;
    }
  }

  function fenceJsonInProse(text) {
    let out = "", cursor = 0, attempts = 0;
    for (let index = 0; index < text.length && attempts < MAX_JSON_ATTEMPTS; index++) {
      const char = text[index];
      if (char !== "{" && char !== "[") continue;
      if (index > 0 && !/[\s:=(]/.test(text[index - 1])) continue;
      const lineStart = text.lastIndexOf("\n", index) + 1;
      if ((text.slice(lineStart, index).match(/`/g) || []).length % 2) continue; // Inside inline code.
      attempts++;
      const end = jsonEnd(text, index);
      if (end < 0 || end - index < MIN_EMBEDDED_JSON_CHARS) continue;
      const raw = text.slice(index, end);
      if (!parseJson(raw)) continue;
      out += text.slice(cursor, index) + "\n\n```json\n" + prettyJson(raw) + "\n```\n\n";
      cursor = end;
      index = end - 1;
    }
    return out + text.slice(cursor);
  }

  /* The markdown to render. Fenced code is left exactly as written. */
  function prepare(text) {
    const raw = String(text || "");
    let result = raw.replace(/(!\[[^\]]*\]\()file:\/\/(\/[^)\s]+)/g, "$1$2");
    if (result.indexOf("{") === -1 && result.indexOf("[") === -1) return result;
    if (result.length > MAX_JSON_CHARS) return result;
    const parts = [];
    let prose = [], fence = null;
    function flush() {
      if (prose.length) parts.push(fenceJsonInProse(prose.join("\n")));
      prose = [];
    }
    result.split("\n").forEach(function (line) {
      const marker = line.match(/^ {0,3}(`{3,}|~{3,})/);
      if (fence) {
        parts.push(line);
        if (marker && marker[1][0] === fence[0] && marker[1].length >= fence.length && !line.trim().replace(/[`~]/g, "")) fence = null;
      } else if (marker) {
        flush();
        fence = marker[1];
        parts.push(line);
      } else {
        prose.push(line);
      }
    });
    flush();
    return parts.join("\n");
  }

  function escapeHtml(text) {
    return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  function highlightJson(text) {
    if (text.length > MAX_JSON_CHARS) return escapeHtml(text);
    const token = /("(?:\\.|[^"\\])*")(\s*:)?|\b(?:true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g;
    let out = "", cursor = 0, match;
    while ((match = token.exec(text))) {
      out += escapeHtml(text.slice(cursor, match.index));
      const kind = match[1] ? (match[2] ? "key" : "string") : /\d/.test(match[0]) ? "number" : "literal";
      out += '<span class="acw-json-' + kind + '">' + escapeHtml(match[1] || match[0]) + "</span>" +
        (match[2] ? escapeHtml(match[2]) : "");
      cursor = match.index + match[0].length;
    }
    return out + escapeHtml(text.slice(cursor));
  }

  function jsonSummary(value, lineCount) {
    const fields = Array.isArray(value)
      ? value.length + (value.length === 1 ? " item" : " items")
      : Object.keys(value).length + (Object.keys(value).length === 1 ? " field" : " fields");
    let preview = "";
    if (!Array.isArray(value)) {
      const key = PREVIEW_KEYS.find(function (name) { return typeof value[name] === "string" && value[name].trim(); });
      if (key) preview = value[key].trim().replace(/\s+/g, " ");
    }
    if (preview.length > 90) preview = preview.slice(0, 89) + "…";
    return { label: "JSON · " + fields + " · " + lineCount + " lines", preview: preview };
  }

  function enhanceJson(bubble) {
    bubble.querySelectorAll("pre > code").forEach(function (code) {
      const declared = /\blanguage-jsonc?\b/.test(code.className);
      if (!declared && code.className) return;
      const text = code.textContent.trim();
      if (text[0] !== "{" && text[0] !== "[") return;
      const parsed = parseJson(text);
      if (!parsed) return;
      const pretty = prettyJson(text);
      code.innerHTML = highlightJson(pretty);
      const pre = code.parentNode;
      pre.classList.add("acw-json");
      const lineCount = pretty.split("\n").length;
      if (lineCount <= COLLAPSE_JSON_LINES || pre.parentNode.tagName === "DETAILS") return;
      const details = document.createElement("details");
      details.className = "acw-json-block";
      const summary = document.createElement("summary");
      const info = jsonSummary(parsed.value, lineCount);
      const label = document.createElement("span");
      label.className = "acw-json-label";
      label.textContent = info.label;
      summary.appendChild(label);
      if (info.preview) {
        const preview = document.createElement("span");
        preview.className = "acw-json-preview";
        preview.textContent = info.preview;
        summary.appendChild(preview);
      }
      details.appendChild(summary);
      pre.replaceWith(details);
      details.appendChild(pre);
    });
  }

  function mediaKind(url) {
    const path = String(url || "").split(/[?#]/, 1)[0];
    const found = MEDIA_KINDS.find(function (entry) { return entry[1].test(path); });
    return found ? found[0] : null;
  }

  /* A URL the page may load, or null. Local paths go through PA's authorized
     file route, which only serves session, project and data directories. */
  function mediaUrl(raw) {
    const value = String(raw || "").trim();
    const links = root.PALinks;
    const file = links && links.filesystemTarget ? links.filesystemTarget(value) : null;
    if (file) return "/api/files/raw?path=" + encodeURIComponent(file.path);
    try {
      const url = new URL(value, root.location.href);
      return /^https?:$/.test(url.protocol) ? url.href : null;
    } catch (_) {
      return null;
    }
  }

  function unavailable(element, text) {
    const note = document.createElement("span");
    note.className = "acw-media-unavailable";
    note.textContent = "Media unavailable: " + text;
    element.replaceWith(note);
  }

  function createMedia(kind, url, label) {
    const element = document.createElement(kind === "image" ? "img" : kind);
    element.className = "acw-media acw-media-" + kind;
    element.src = url;
    if (kind === "image") {
      element.alt = label;
      element.loading = "lazy";
    } else {
      element.controls = true;
      element.preload = "metadata";
      element.setAttribute("aria-label", label);
      if (kind === "video") element.setAttribute("playsinline", "");
    }
    element.addEventListener("error", function () { unavailable(element.closest("a.acw-media-link") || element, label); });
    if (kind !== "image") return element;
    const link = document.createElement("a");
    link.className = "acw-media-link";
    link.href = url;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    link.dataset.paLinkDecorated = "1";
    link.appendChild(element);
    return link;
  }

  /* Streaming re-renders the bubble for every chunk; reusing the node keeps
     playback going and avoids refetching. */
  function reusableMedia(cache, kind, url, label) {
    let element = cache.get(url);
    if (!element || !element.isConnected && element.classList.contains("acw-media-unavailable")) {
      element = createMedia(kind, url, label);
      cache.set(url, element);
    }
    return element;
  }

  function enhanceMedia(bubble) {
    const cache = bubble._mediaNodes || (bubble._mediaNodes = new Map());
    const seen = new Set();
    let count = 0;
    function reserve(url) {
      if (seen.has(url) || count >= MAX_MEDIA_PER_BUBBLE) return false;
      seen.add(url);
      count++;
      return true;
    }
    bubble.querySelectorAll("img").forEach(function (image) {
      if (image.closest(".acw-message-images")) return;
      const original = image.getAttribute("src") || "";
      const url = mediaUrl(original);
      if (!url) return;
      const kind = mediaKind(original) || "image";
      if (!reserve(url)) return;
      image.replaceWith(reusableMedia(cache, kind, url, image.getAttribute("alt") || original));
    });
    function attach(anchor, raw, label) {
      const kind = mediaKind(raw);
      const url = kind && mediaUrl(raw);
      if (!url || !reserve(url)) return;
      const holder = document.createElement("div");
      holder.className = "acw-media-holder";
      holder.appendChild(reusableMedia(cache, kind, url, label));
      (anchor.closest("p, li") || anchor).insertAdjacentElement("afterend", holder);
    }
    bubble.querySelectorAll("a[href]").forEach(function (link) {
      if (!link.closest(".acw-media-link")) attach(link, link.getAttribute("href"), link.textContent || link.getAttribute("href"));
    });
    bubble.querySelectorAll("code").forEach(function (code) {
      if (code.closest("pre")) return;
      const text = code.textContent.trim();
      if (/^(\/|file:\/\/|https?:\/\/)\S+$/.test(text)) attach(code, text, text);
    });
  }

  function enhance(bubble) {
    if (!bubble || typeof bubble.querySelectorAll !== "function") return;
    enhanceJson(bubble);
    enhanceMedia(bubble);
  }

  /* navigator.clipboard only exists on HTTPS and localhost; PA is often opened
     over plain HTTP on a LAN address. */
  function copyText(text) {
    const clipboard = root.navigator && root.navigator.clipboard;
    if (clipboard && root.isSecureContext) {
      return clipboard.writeText(text).catch(function () { return legacyCopy(text); });
    }
    return legacyCopy(text);
  }

  function legacyCopy(text) {
    return new Promise(function (resolve, reject) {
      const area = document.createElement("textarea");
      area.value = text;
      area.setAttribute("readonly", "");
      area.style.position = "fixed";
      area.style.opacity = "0";
      const host = document.activeElement && document.activeElement.closest("dialog") || document.body;
      host.appendChild(area);
      area.select();
      let copied = false;
      try { copied = document.execCommand("copy"); } catch (_) { copied = false; }
      area.remove();
      if (copied) resolve();
      else reject(new Error("Clipboard unavailable"));
    });
  }

  root.PAChatFormat = {
    copyText: copyText,
    enhance: enhance,
    mediaKind: mediaKind,
    mediaUrl: mediaUrl,
    prepare: prepare,
    prettyJson: prettyJson,
  };
})(window);
