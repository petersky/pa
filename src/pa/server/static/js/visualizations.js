/* PA visualization protocol v1. Artifact HTML runs only in an opaque iframe. */
(function () {
  "use strict";
  const START = "\ue200visualize\ue202", END = "\ue201", BRIDGE = "pa.visualization/v1";

  function references(text) {
    const found = [];
    let fence = null, inline = 0, offset = 0, consumed = 0;
    (text.match(/[^\n]*\n|[^\n]+$/g) || []).forEach(function (line) {
      if (offset + line.length <= consumed) { offset += line.length; return; }
      let i = Math.max(0, consumed - offset);
      const f = line.match(/^ {0,3}(`{3,}|~{3,})/);
      if (f && !inline && !i) {
        if (!fence) fence = f[1];
        else if (f[1][0] === fence[0] && f[1].length >= fence.length) fence = null;
        offset += line.length; return;
      }
      if (fence || (!inline && /^(    |\t)/.test(line))) { offset += line.length; return; }
      while (i < line.length) {
        if (line[i] === "`") {
          let end = i + 1;
          while (line[end] === "`") end++;
          const run = end - i;
          inline = inline === run ? 0 : (inline || run);
          i = end; continue;
        }
        if (!inline && line.startsWith(START, i) && (!i || line[i - 1] !== "\\")) {
          const start = offset + i + START.length;
          const end = text.indexOf(END, start);
          const stop = end < 0 ? text.length : end + END.length;
          const raw = text.slice(start, end < 0 ? text.length : end).trim();
          found.push({raw:raw, start:offset+i, end:stop, complete:end >= 0});
          consumed = stop; i = stop - offset;
        } else i++;
      }
      offset += line.length;
    });
    return found;
  }

  // This function is serialized into srcdoc before the untrusted fragment.
  // It has no closures or access to the parent application's JavaScript realm.
  function previewRuntime(config) {
    "use strict";
    const bridge = "pa.visualization/v1";
    let state = config.state || null, next = 0;
    const pending = new Map();
    const send = (type, fields) => parent.postMessage(Object.assign({bridge, token:config.token, type}, fields), "*");
    const api = Object.freeze({
      get widgetState() { return state; },
      setWidgetState(value) {
        let encoded;
        try { encoded = JSON.stringify(value); } catch (_) { return Promise.reject(new Error("Invalid widget state")); }
        if (!value || typeof value !== "object" || encoded.length > 32768 ||
            Object.keys(value).some(k => !["modelContent", "privateContent"].includes(k))) {
          return Promise.reject(new Error("Invalid widget state"));
        }
        state = JSON.parse(encoded);
        const id = ++next;
        send("state", {state, id});
        return new Promise((resolve, reject) => {
          const timer = setTimeout(() => { pending.delete(id); reject(new Error("State save unavailable")); }, 10000);
          pending.set(id, {resolve, reject, timer});
        });
      }
    });
    Object.defineProperty(window, "openai", {value:api, writable:false, configurable:false});
    addEventListener("message", event => {
      const m = event.data;
      if (event.source !== parent || !m || m.bridge !== bridge || m.token !== config.token) return;
      if (m.type === "appearance" && ["dark", "light"].includes(m.scheme)) document.documentElement.style.colorScheme = m.scheme;
      if (m.type === "saved" && pending.has(m.id)) {
        const p = pending.get(m.id); pending.delete(m.id); clearTimeout(p.timer);
        m.ok ? p.resolve(state) : p.reject(new Error("State save unavailable"));
      }
    });
    class Tweak {
      constructor(options) {
        this.options = options || {};
        this.panel = document.createElement("details");
        this.panel.className = "pa-viz-tweak";
        const summary = document.createElement("summary"); summary.textContent = "Tweak design";
        this.panel.append(summary);
        (this.options.container || document.body).prepend(this.panel);
      }
      control(settings, key, options, type) {
        options = options || {};
        const label = document.createElement("label");
        label.append(document.createTextNode(options.label || key));
        const input = document.createElement(type === "toggle" ? "input" : "select");
        if (type === "toggle") { input.type = "checkbox"; input.checked = !!settings[key]; }
        else {
          (options.options || []).forEach(value => {
            const option = document.createElement("option"); option.value = value; option.textContent = value; input.append(option);
          });
          input.value = settings[key];
        }
        input.addEventListener("change", () => {
          settings[key] = type === "toggle" ? input.checked : input.value;
          if (this.options.onChange) this.options.onChange(settings);
        });
        label.append(input); this.panel.append(label); return input;
      }
      addToggle(settings, key, options) { return this.control(settings, key, options, "toggle"); }
      addSelect(settings, key, options) { return this.control(settings, key, options, "select"); }
    }
    Object.defineProperty(window, "Tweak", {value:Tweak, writable:false});
    addEventListener("DOMContentLoaded", () => {
      document.querySelectorAll(".viz-carousel").forEach((carousel, n) => {
        const variants = Array.from(carousel.children).filter(el => el.hasAttribute("data-variant"));
        if (variants.length < 2) return;
        let index = 0;
        const nav = document.createElement("nav"); nav.className = "pa-viz-variants";
        nav.setAttribute("aria-label", "Design alternatives");
        const previous = document.createElement("button"), next = document.createElement("button");
        previous.textContent = "←"; previous.setAttribute("aria-label", "Previous design");
        next.textContent = "→"; next.setAttribute("aria-label", "Next design");
        const select = document.createElement("select"); select.setAttribute("aria-label", "Design variant");
        variants.forEach((v, i) => {
          v.id ||= "pa-viz-variant-" + n + "-" + i;
          const o = document.createElement("option"); o.value = i; o.textContent = v.dataset.variant; select.append(o);
        });
        function show(i) {
          index = (i + variants.length) % variants.length;
          variants.forEach((v, j) => { v.hidden = j !== index; });
          select.value = index; select.setAttribute("aria-controls", variants[index].id);
        }
        previous.onclick = () => show(index - 1); next.onclick = () => show(index + 1);
        select.onchange = () => show(Number(select.value));
        nav.append(previous, select, next); carousel.prepend(nav); show(0);
      });
      let timer;
      const resize = () => {
        clearTimeout(timer);
        timer = setTimeout(() => send("resize", {height:Math.ceil(document.documentElement.getBoundingClientRect().height)}), 40);
      };
      new ResizeObserver(resize).observe(document.body);
      resize(); send("ready", {});
    });
  }

  const runtimeStyle = `:root{color-scheme:light dark;--background:light-dark(#fff,#181818);--foreground:light-dark(#1a1c1f,#f5f5f5);--primary:light-dark(#18689c,#8ac6ff);--muted-foreground:light-dark(#636975,#a8afbd);--border:light-dark(#d8dce3,#414853);--color-background-primary:var(--background);--color-text-primary:var(--foreground);--color-text-secondary:var(--muted-foreground);--color-border-secondary:var(--border)}
    html,body{margin:0;width:100%;min-width:0}body{box-sizing:border-box;padding:12px;background:var(--background);color:var(--foreground);font:14px system-ui,sans-serif;overflow-wrap:anywhere}*,*::before,*::after{box-sizing:border-box}[hidden]{display:none!important}img,svg,video,canvas{max-width:100%}button,input,select,textarea{font:inherit;color:inherit}button,select,summary,.cursor-interaction{cursor:pointer}:focus-visible{outline:2px solid var(--primary);outline-offset:3px}.viz-dotted-background{background-image:radial-gradient(var(--border) 1px,transparent 1px);background-size:16px 16px}.pa-viz-variants{display:flex;gap:8px;align-items:center;justify-content:center;flex-wrap:wrap;padding:8px;max-width:100%}.pa-viz-variants button,.pa-viz-variants select{background:var(--background);border:1px solid var(--border);border-radius:6px;padding:7px;max-width:100%}.pa-viz-tweak{margin:0 0 8px;padding:8px;border:1px solid var(--border);border-radius:8px;background:var(--background)}.pa-viz-tweak label{display:flex;gap:12px;align-items:center;justify-content:space-between;flex-wrap:wrap;margin:12px 0}.pa-viz-tweak input,.pa-viz-tweak select{background:var(--background);max-width:100%}`;
  const csp = "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data: blob:; font-src data:; media-src data: blob:; connect-src 'none'; frame-src 'none'; worker-src 'none'; object-src 'none'; base-uri 'none'; form-action 'none'";
  const safeJSON = value => JSON.stringify(value).replace(/</g, "\\u003c");
  const escape = value => String(value).replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));

  function frameDocument(html, config) {
    return '<!doctype html><html lang="en" style="color-scheme:' + config.scheme + '"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="Content-Security-Policy" content="' + escape(csp) + '"><style>' + runtimeStyle + '</style><script>(' + previewRuntime.toString() + ')(' + safeJSON(config) + ');<\/script></head><body>' + html + '</body></html>';
  }
  function scheme() {
    const value = getComputedStyle(document.documentElement).colorScheme;
    if (value === "light" || value === "dark") return value;
    return matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }
  function headers() {
    const h = {"Content-Type":"application/json"};
    if (window.PACSRF) return window.PACSRF.headers(h);
    const meta = document.querySelector('meta[name="csrf-token"]');
    if (meta) h["X-CSRF-Token"] = meta.content;
    return h;
  }
  function fallback(card, status) {
    const messages = {malformed:"The preview reference is invalid.", unsupported:"This preview version is not supported.",
      denied:"You do not have access to this preview.", unavailable:"The preview artifact is unavailable. Ask the author to attach the HTML again."};
    card._status.textContent = messages[status] || messages.unavailable;
    card._status.hidden = false;
  }
  function mount(card, ref, options) {
    const base = options.apiBase + "/sessions/" + encodeURIComponent(options.sessionId) + "/visualizations";
    let payload;
    if (!ref.complete) { card._status.textContent = "Preparing preview…"; return; }
    try { payload = JSON.parse(ref.raw); } catch (_) { fallback(card, "malformed"); return; }
    if (payload && typeof payload.title === "string") card._title.textContent = payload.title.slice(0,160) || "Interactive preview";
    card.classList.toggle("is-wide", !!payload && payload.wide === true);
    card._status.textContent = "Loading interactive preview…";
    let attempts = 0;
    function load() {
      fetch(base + "?reference=" + encodeURIComponent(ref.raw), {credentials:"same-origin", signal:AbortSignal.timeout(8000)})
        .then(async response => {
          if (!response.ok) { fallback(card, response.status === 403 || response.status === 401 ? "denied" : "unavailable"); return; }
          const record = await response.json();
          if (record.status === "unavailable" && ++attempts < 3) {
            setTimeout(() => { if (card.isConnected) load(); }, 600); return;
          }
          if (record.status !== "ready" || record.version !== 1 || typeof record.html !== "string") {
            fallback(card, record.version && record.version !== 1 ? "unsupported" : record.status); return;
          }
          card._title.textContent = record.title;
          card.classList.toggle("is-wide", record.wide);
          card._status.textContent = "Starting interactive preview…";
          const source = document.createElement("a"); source.textContent = "Download HTML";
          source.href = base + "/" + record.id + "/source"; source.download = "interactive-preview.html";
          card._actions.append(source);
          let currentState = record.state;
          let saveQueue = Promise.resolve();
          function createFrame(container) {
            const frame = document.createElement("iframe"); frame.title = record.title;
            frame.setAttribute("sandbox", "allow-scripts"); frame.referrerPolicy = "no-referrer";
            const token = Array.from(crypto.getRandomValues(new Uint32Array(4))).map(n => n.toString(16)).join("");
            const config = {token, state:currentState, scheme:scheme()};
            frame.srcdoc = frameDocument(record.html, config);
            container.append(frame);
            let queued = 0;
            const startup = setTimeout(() => {
              card._status.classList.remove("sr-only");
              card._status.textContent = "Preview runtime unavailable. Download the HTML to inspect it.";
            }, 8000);
            const send = message => frame.contentWindow?.postMessage(Object.assign({bridge:BRIDGE, token}, message), "*");
            const listener = function (event) {
              const m = event.data;
              if (event.source !== frame.contentWindow || !m || m.bridge !== BRIDGE || m.token !== token || event.origin !== "null") return;
              if (m.type === "ready") {
                clearTimeout(startup); card._status.textContent = "Interactive preview loaded"; card._status.classList.add("sr-only");
              }
              if (m.type === "resize" && Number.isFinite(m.height)) frame.style.height = Math.max(180, Math.min(1800, m.height)) + "px";
              if (m.type === "state" && m.state && typeof m.state === "object") {
                let encoded;
                try { encoded = JSON.stringify(m.state); } catch (_) { send({type:"saved",id:m.id,ok:false}); return; }
                if (queued >= 4 || encoded.length > 32768 || Object.keys(m.state).some(k => !["modelContent","privateContent"].includes(k))) {
                  send({type:"saved",id:m.id,ok:false}); return;
                }
                queued++;
                currentState = JSON.parse(encoded);
                const stateToSave = currentState;
                saveQueue = saveQueue.catch(() => {}).then(() => fetch(base + "/" + record.id + "/state", {
                  method:"PUT", credentials:"same-origin", headers:headers(), body:JSON.stringify({version:1,state:stateToSave}), signal:AbortSignal.timeout(8000)
                })).then(r => {
                  send({type:"saved",id:m.id,ok:r.ok});
                  if (!r.ok) throw new Error("State save unavailable");
                }).catch(() => {
                  send({type:"saved",id:m.id,ok:false});
                  card._status.classList.remove("sr-only"); card._status.textContent = "Preview changes could not be saved. They apply for this view.";
                }).finally(() => { queued--; });
              }
            };
            window.addEventListener("message", listener);
            const appearance = new MutationObserver(() => send({type:"appearance",scheme:scheme()}));
            appearance.observe(document.documentElement, {attributes:true,attributeFilter:["class","style","data-theme","data-appearance"]});
            const media = matchMedia("(prefers-color-scheme: dark)");
            const change = () => send({type:"appearance",scheme:scheme()}); media.addEventListener("change", change);
            const removed = new MutationObserver(() => {
              if (!frame.isConnected) { clearTimeout(startup); window.removeEventListener("message",listener); appearance.disconnect(); removed.disconnect(); media.removeEventListener("change",change); }
            });
            removed.observe(document.body, {childList:true,subtree:true});
            return frame;
          }
          createFrame(card);
          const open = document.createElement("button"); open.type = "button"; open.textContent = "Open full size";
          open.onclick = () => {
            const dialog = document.createElement("dialog"); dialog.className = "acw-visualization-dialog";
            dialog.setAttribute("aria-label", record.title);
            const close = document.createElement("button"); close.type="button"; close.textContent="Close preview";
            close.onclick = () => dialog.close(); dialog.append(close); document.body.append(dialog);
            createFrame(dialog); dialog.addEventListener("close", () => { dialog.remove(); open.focus(); }, {once:true}); dialog.showModal(); close.focus();
          };
          card._actions.prepend(open);
        }).catch(() => fallback(card, "unavailable"));
    }
    load();
  }
  function render(bubble, text, options) {
    const refs = references(text);
    if (!refs.length) return false;
    const parts = []; let offset = 0;
    refs.forEach((ref,i) => {
      parts.push({key:"text-"+i, text:text.slice(offset,ref.start)});
      parts.push({key:"preview-"+i+":"+ref.raw, ref}); offset = ref.end;
    });
    parts.push({key:"text-"+refs.length,text:text.slice(offset)});
    if (!bubble._vizParts) { bubble.replaceChildren(); bubble._vizParts = new Map(); }
    const prior = bubble._vizParts, current = new Map(); let cursor = bubble.firstChild;
    parts.forEach(part => {
      let node = prior.get(part.key);
      if (!node) {
        node = document.createElement(part.ref ? "section" : "div");
        if (part.ref) {
          node.className = "acw-visualization";
          node._title = document.createElement("strong"); node._title.textContent="Interactive preview";
          node._actions = document.createElement("div"); node._actions.className="acw-visualization-actions";
          node._status = document.createElement("p"); node._status.setAttribute("role","status");
          node.append(node._title,node._actions,node._status);
          mount(node, part.ref, options);
        }
      }
      const rich = !!(window.marked && window.DOMPurify);
      if (!part.ref && (node._text !== part.text || node._rich !== rich)) {
        node.innerHTML = options.markdown(part.text); node._text = part.text; node._rich = rich;
      }
      current.set(part.key,node);
      if (node !== cursor) bubble.insertBefore(node,cursor);
      cursor = node.nextSibling;
    });
    prior.forEach((node,key) => { if (!current.has(key)) node.remove(); });
    bubble._vizParts = current;
    return true;
  }
  window.PAVisualizations = {references, render, frameDocument};
})();
