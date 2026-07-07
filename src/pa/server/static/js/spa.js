(function () {
  function setActiveNav(path) {
    document.querySelectorAll(".nav-btn").forEach(function (btn) {
      btn.classList.toggle("active", btn.getAttribute("href") === path);
    });
    document.querySelectorAll(".icon-btn[href]").forEach(function (btn) {
      if (btn.getAttribute("href") === "/settings") {
        btn.classList.toggle("active", path === "/settings");
      }
      if (btn.getAttribute("href") === "/agent") {
        btn.classList.toggle("active", path === "/agent");
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

  function showToast(message, kind) {
    let toast = document.getElementById("pa-toast");
    if (!toast) {
      toast = document.createElement("div");
      toast.id = "pa-toast";
      toast.className = "pa-toast";
      document.body.appendChild(toast);
    }
    toast.textContent = message;
    toast.dataset.kind = kind || "error";
    toast.classList.add("visible");
    window.clearTimeout(showToast._timer);
    showToast._timer = window.setTimeout(function () {
      toast.classList.remove("visible");
    }, 4000);
  }

  document.body.addEventListener("htmx:afterSwap", function (event) {
    if (event.detail.target && event.detail.target.id === "app-view") {
      setActiveNav(window.location.pathname);
      updateTitle();
    }
    if (event.detail.target && event.detail.target.id === "agent-messages") {
      const placeholder = document.querySelector(".chat-placeholder");
      if (placeholder) placeholder.remove();
    }
  });

  document.body.addEventListener("htmx:responseError", function (event) {
    const xhr = event.detail.xhr;
    let message = "Request failed";
    if (xhr && xhr.responseText) {
      try {
        const data = JSON.parse(xhr.responseText);
        message = data.detail || data.message || message;
        if (Array.isArray(message)) {
          message = message.map(function (item) {
            return item.msg || String(item);
          }).join("; ");
        }
      } catch (_err) {
        message = xhr.statusText || message;
      }
    }
    showToast(message, "error");
  });

  window.addEventListener("popstate", function () {
    if (typeof htmx === "undefined") return;
    htmx.ajax("GET", window.location.pathname, {
      target: "#app-view",
      swap: "innerHTML",
    });
    setActiveNav(window.location.pathname);
    updateTitle();
  });

  document.addEventListener("DOMContentLoaded", function () {
    setActiveNav(window.location.pathname);
    updateTitle();
  });
})();
