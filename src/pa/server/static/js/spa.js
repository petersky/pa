(function () {
  function setActiveNav(path) {
    document.querySelectorAll(".nav-btn").forEach(function (btn) {
      btn.classList.toggle("active", btn.getAttribute("href") === path);
    });
    document.querySelectorAll(".icon-btn[href]").forEach(function (btn) {
      if (btn.getAttribute("href") === "/settings") {
        btn.classList.toggle("active", path === "/settings");
      }
    });
  }

  function updateTitle() {
    const active = document.querySelector(".nav-btn.active span:last-child");
    const instance = document.querySelector(".instance-indicator");
    if (active && instance) {
      const name = instance.getAttribute("title") || "PA";
      const label = name.replace(/^Instance:\s*/, "");
      document.title = active.textContent.trim() + " — " + label;
    }
  }

  document.body.addEventListener("htmx:afterSwap", function (event) {
    if (event.detail.target && event.detail.target.id === "app-view") {
      setActiveNav(window.location.pathname);
      updateTitle();
    }
  });

  window.addEventListener("popstate", function () {
    if (typeof htmx === "undefined") return;
    htmx.ajax("GET", window.location.pathname, {
      target: "#app-view",
      swap: "innerHTML",
    });
    setActiveNav(window.location.pathname);
  });

  document.addEventListener("DOMContentLoaded", function () {
    setActiveNav(window.location.pathname);
    updateTitle();
  });
})();
