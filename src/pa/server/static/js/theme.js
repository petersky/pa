(function () {
  const STORAGE_KEY = "pa.appearance";
  const THEME_KEY = "pa.theme";
  const APPEARANCE_ORDER = ["system", "light", "dark"];
  const ICONS = {
    system: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8M12 17v4"/><circle cx="12" cy="10" r="2"/></svg>`,
    light: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>`,
    dark: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>`,
  };

  function resolveAppearance(mode) {
    if (mode === "light" || mode === "dark") return mode;
    return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }

  function readCookie(name) {
    const match = document.cookie.match(new RegExp("(?:^|; )" + name + "=([^;]*)"));
    return match ? decodeURIComponent(match[1]) : null;
  }

  function getPreferences() {
    const appearance =
      localStorage.getItem(STORAGE_KEY) ||
      readCookie("pa_appearance") ||
      document.documentElement.dataset.appearancePref ||
      "system";
    const themeId =
      localStorage.getItem(THEME_KEY) ||
      readCookie("pa_theme") ||
      document.documentElement.dataset.themeId ||
      "pa";
    return { appearance, themeId };
  }

  function csrfToken() {
    const meta = document.querySelector('meta[name="csrf-token"]');
    return meta ? meta.content : "";
  }

  function secureCookieSuffix() {
    return window.location.protocol === "https:" ? "; secure" : "";
  }

  async function apiPutTheme(body) {
    const headers = { "Content-Type": "application/json" };
    const token = csrfToken();
    if (token) headers["X-CSRF-Token"] = token;
    await fetch("/api/ui/theme", { method: "PUT", headers, body: JSON.stringify(body) });
  }

  function applyTheme(prefs) {
    const resolved = resolveAppearance(prefs.appearance);
    document.documentElement.dataset.theme = prefs.themeId;
    document.documentElement.dataset.appearance = resolved;
    document.documentElement.dataset.appearancePref = prefs.appearance;
    document.documentElement.style.colorScheme = resolved;
  }

  function assetVersion() {
    return (window.PA_ASSETS && window.PA_ASSETS.version) || "";
  }

  function staticUrl(path) {
    const clean = path.replace(/^\//, "");
    const v = assetVersion();
    return v ? `/static/${clean}?v=${v}` : `/static/${clean}`;
  }

  function loadVariantStyles(themeId, appearance) {
    const resolved = resolveAppearance(appearance);
    const linkId = "pa-theme-variant";
    let link = document.getElementById(linkId);
    if (!link) {
      link = document.createElement("link");
      link.id = linkId;
      link.rel = "stylesheet";
      document.head.appendChild(link);
    }
    link.href = staticUrl(`themes/${themeId}/${resolved}.css`);
  }

  function updateThemeButton(appearance) {
    const btn = document.getElementById("pa-theme-toggle");
    if (!btn) return;
    btn.dataset.appearance = appearance;
    btn.title = "Theme: " + appearance;
    btn.innerHTML = `<span class="icon" aria-hidden="true">${ICONS[appearance] || ICONS.system}</span>`;
  }

  function bindThemeToggle() {
    const btn = document.getElementById("pa-theme-toggle");
    if (!btn || btn.dataset.bound) return;
    btn.dataset.bound = "1";
    btn.addEventListener("click", cycleAppearance);
    updateThemeButton(btn.dataset.appearance || getPreferences().appearance);
  }

  function cycleAppearance() {
    const current = getPreferences().appearance;
    const idx = APPEARANCE_ORDER.indexOf(current);
    const next = APPEARANCE_ORDER[(idx + 1) % APPEARANCE_ORDER.length];
    setAppearance(next);
    updateThemeButton(next);
  }

  function init() {
    const prefs = getPreferences();
    applyTheme(prefs);
    loadVariantStyles(prefs.themeId, prefs.appearance);
    bindThemeToggle();

    window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => {
      const current = getPreferences();
      if (current.appearance === "system") {
        applyTheme(current);
        loadVariantStyles(current.themeId, current.appearance);
      }
    });
  }

  async function setAppearance(appearance) {
    localStorage.setItem(STORAGE_KEY, appearance);
    document.cookie =
      `pa_appearance=${encodeURIComponent(appearance)}; path=/; max-age=31536000; samesite=lax` +
      secureCookieSuffix();
    const prefs = getPreferences();
    applyTheme(prefs);
    loadVariantStyles(prefs.themeId, prefs.appearance);
    try {
      await apiPutTheme({ appearance });
    } catch (_) {
      /* offline — local preference still applies */
    }
  }

  async function setThemeId(themeId, button) {
    localStorage.setItem(THEME_KEY, themeId);
    document.cookie =
      `pa_theme=${encodeURIComponent(themeId)}; path=/; max-age=31536000; samesite=lax` +
      secureCookieSuffix();
    document.documentElement.dataset.themeId = themeId;
    const prefs = getPreferences();
    applyTheme({ appearance: prefs.appearance, themeId });
    loadVariantStyles(themeId, prefs.appearance);
    document.querySelectorAll(".theme-card").forEach(function (el) {
      el.classList.toggle("active", el.dataset.themeId === themeId);
    });
    if (button) button.classList.add("active");
    try {
      await apiPutTheme({ theme_id: themeId });
    } catch (_) {
      /* offline */
    }
  }

  window.PATheme = {
    init,
    setAppearance,
    setThemeId,
    getPreferences,
    resolveAppearance,
    cycleAppearance,
    staticUrl,
  };
  init();
  document.body.addEventListener("htmx:afterSwap", bindThemeToggle);
})();
