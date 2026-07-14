/**
 * Fleet wizard UI — join tokens, SSH install, register, remove, realm invites.
 * Passwords are sent once over the authenticated session and never kept in JS storage.
 * Uses event delegation so HTMX page swaps do not stack duplicate handlers.
 */
(function () {
  if (window.__paFleetBound) return;
  window.__paFleetBound = true;

  function csrfHeaders() {
    var headers = { "Content-Type": "application/json", Accept: "application/json" };
    var meta = document.querySelector('meta[name="csrf-token"]');
    if (meta && meta.content) headers["X-CSRF-Token"] = meta.content;
    return headers;
  }

  function $(sel, root) {
    return (root || document).querySelector(sel);
  }

  function $all(sel, root) {
    return Array.prototype.slice.call((root || document).querySelectorAll(sel));
  }

  async function api(path, opts) {
    var options = Object.assign(
      { credentials: "same-origin", headers: csrfHeaders() },
      opts || {}
    );
    if (options.body && typeof options.body === "object" && !(options.body instanceof FormData)) {
      options.body = JSON.stringify(options.body);
    }
    var resp = await fetch(path, options);
    var text = await resp.text();
    var data = null;
    try {
      data = text ? JSON.parse(text) : null;
    } catch (e) {
      data = { detail: text };
    }
    if (!resp.ok) {
      var detail = (data && data.detail) || resp.statusText || "Request failed";
      throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
    }
    return data;
  }

  function showPanel(name) {
    $all("[data-fleet-panel]").forEach(function (el) {
      var show = el.getAttribute("data-fleet-panel") === name;
      el.hidden = !show;
      el.classList.toggle("hidden", !show);
    });
    $all("[data-fleet-path]").forEach(function (btn) {
      btn.classList.toggle("active", btn.getAttribute("data-fleet-path") === name);
    });
  }

  function formToObject(form) {
    var fd = new FormData(form);
    var obj = {};
    fd.forEach(function (value, key) {
      obj[key] = typeof value === "string" ? value.trim() : value;
    });
    return obj;
  }

  function clearSecrets(form) {
    ["password", "passphrase"].forEach(function (name) {
      var el = form.elements[name];
      if (el) el.value = "";
    });
  }

  function refreshFleetPage() {
    var section = "";
    try {
      section = new URL(window.location.href).searchParams.get("section") || "";
    } catch (e) {}
    var url = "/fleet" + (section ? "?section=" + encodeURIComponent(section) : "");
    if (window.htmx) {
      htmx.ajax("GET", url, { target: "#app-view", swap: "innerHTML", pushUrl: true });
    } else {
      location.href = url;
    }
  }

  async function pollJob(jobId, logEl, statusEl) {
    while (true) {
      var job = await api("/api/fleet/install-remote/" + encodeURIComponent(jobId));
      if (logEl) {
        logEl.hidden = false;
        logEl.textContent = job.log || "";
        logEl.scrollTop = logEl.scrollHeight;
      }
      if (statusEl) statusEl.textContent = "Status: " + job.status;
      if (job.status === "succeeded" || job.status === "failed") {
        if (job.status === "failed" && statusEl) {
          statusEl.textContent = job.error || "Install failed";
        }
        if (job.status === "succeeded") {
          if (statusEl) statusEl.textContent = "Succeeded — refreshing…";
          setTimeout(refreshFleetPage, 800);
        }
        return job;
      }
      await new Promise(function (r) {
        setTimeout(r, 1000);
      });
    }
  }

  document.addEventListener("click", function (e) {
    if (e.target.closest("#pa-fleet-refresh")) {
      e.preventDefault();
      refreshFleetPage();
      return;
    }

    var pathBtn = e.target.closest("[data-fleet-path]");
    if (pathBtn && $("#pa-fleet-root")) {
      showPanel(pathBtn.getAttribute("data-fleet-path"));
      return;
    }

    if (e.target.closest("[data-fleet-ensure-token]") || e.target.closest("#pa-fleet-ensure-token")) {
      var status = $("#pa-fleet-readiness-status");
      api("/api/fleet/ensure-sync-token", { method: "POST", body: {} })
        .then(function () {
          if (status) status.textContent = "Sync token ready.";
          setTimeout(refreshFleetPage, 400);
        })
        .catch(function (err) {
          if (status) status.textContent = err.message;
        });
      return;
    }

    if (e.target.closest("[data-fleet-fix-bind]")) {
      var bindStatus = $("#pa-fleet-readiness-status");
      if (bindStatus) bindStatus.textContent = "Binding 0.0.0.0 and restarting…";
      api("/api/fleet/readiness", { method: "POST", body: { bind_all: true } })
        .then(function (data) {
          if (bindStatus) {
            bindStatus.textContent = data.restart_started
              ? "Saved. Restarting service so peers can connect…"
              : "Saved bind host 0.0.0.0. Restart PA if peers still cannot connect.";
          }
          setTimeout(refreshFleetPage, data.restart_started ? 2500 : 600);
        })
        .catch(function (err) {
          if (bindStatus) bindStatus.textContent = err.message;
        });
      return;
    }

    if (e.target.closest("#pa-fleet-mint-token")) {
      var out = $("#pa-fleet-token-out");
      api("/api/fleet/join-token", { method: "POST", body: {} })
        .then(function (data) {
          if (out) {
            out.hidden = false;
            out.textContent =
              "Token: " +
              data.token +
              "\nExpires: " +
              data.expires_at +
              "\nOwner: " +
              data.owner_url +
              "\n\n" +
              data.join_command;
          }
        })
        .catch(function (err) {
          if (out) {
            out.hidden = false;
            out.textContent = err.message;
          }
        });
      return;
    }

    var removeBtn = e.target.closest("[data-fleet-remove]");
    if (removeBtn) {
      var id = removeBtn.getAttribute("data-fleet-remove");
      if (!id || !confirm("Remove this instance from the fleet?")) return;
      api("/api/fleet/instances/" + encodeURIComponent(id), { method: "DELETE" })
        .then(refreshFleetPage)
        .catch(function (err) {
          alert(err.message);
        });
      return;
    }

    var inviteBtn = e.target.closest("[data-fleet-invite]");
    if (inviteBtn) {
      var realmId = inviteBtn.getAttribute("data-fleet-invite");
      var inviteOut = $("#pa-fleet-invite-out");
      api("/api/realms/invite", {
        method: "POST",
        body: { realm_id: realmId, role: "editor" },
      })
        .then(function (data) {
          if (inviteOut) {
            inviteOut.hidden = false;
            inviteOut.textContent =
              "Realm invite for " +
              data.realm_id +
              " (" +
              data.role +
              ")\nToken: " +
              data.token +
              (data.expires_at ? "\nExpires: " + data.expires_at : "");
          }
        })
        .catch(function (err) {
          if (inviteOut) {
            inviteOut.hidden = false;
            inviteOut.textContent = err.message;
          }
        });
    }
  });

  document.addEventListener("submit", function (e) {
    var form = e.target;
    if (!form) return;
    // Allow forms identified by id or data-fleet-fix (inline readiness fixes).
    if (!form.id && !form.getAttribute("data-fleet-fix")) return;

    if (form.id === "pa-fleet-ssh-form") {
      e.preventDefault();
      var logEl = $("#pa-fleet-ssh-log");
      var statusEl = $("#pa-fleet-ssh-status");
      var body = formToObject(form);
      body.port = parseInt(body.port || "22", 10);
      clearSecrets(form);
      if (statusEl) statusEl.textContent = "Starting…";
      api("/api/fleet/install-remote", { method: "POST", body: body })
        .then(function (job) {
          return pollJob(job.job_id, logEl, statusEl);
        })
        .catch(function (err) {
          if (statusEl) statusEl.textContent = err.message;
        });
      return;
    }

    if (form.id === "pa-fleet-ssh-join-form") {
      e.preventDefault();
      var joinLog = $("#pa-fleet-ssh-join-log");
      var joinBody = formToObject(form);
      joinBody.port = parseInt(joinBody.port || "22", 10);
      joinBody.join_only = true;
      clearSecrets(form);
      api("/api/fleet/install-remote", { method: "POST", body: joinBody })
        .then(function (job) {
          return pollJob(job.job_id, joinLog, null);
        })
        .catch(function (err) {
          if (joinLog) {
            joinLog.hidden = false;
            joinLog.textContent = err.message;
          }
        });
      return;
    }

    if (form.id === "pa-fleet-register-form") {
      e.preventDefault();
      var regStatus = $("#pa-fleet-register-status");
      var regBody = formToObject(form);
      if (!regBody.instance_id) delete regBody.instance_id;
      api("/api/fleet/register-remote", { method: "POST", body: regBody })
        .then(function (data) {
          if (regStatus) regStatus.textContent = "Registered " + data.name;
          setTimeout(refreshFleetPage, 500);
        })
        .catch(function (err) {
          if (regStatus) regStatus.textContent = err.message;
        });
      return;
    }

    if (
      form.id === "pa-fleet-readiness-form" ||
      form.id === "pa-fleet-fix-instance-url" ||
      form.getAttribute("data-fleet-fix") === "instance_url"
    ) {
      e.preventDefault();
      var readyStatus = $("#pa-fleet-readiness-status");
      var readyBody = formToObject(form);
      if (
        form.id === "pa-fleet-fix-instance-url" ||
        form.getAttribute("data-fleet-fix") === "instance_url"
      ) {
        readyBody = { instance_url: readyBody.instance_url || "" };
      }
      if (readyStatus) readyStatus.textContent = "Saving…";
      api("/api/fleet/readiness", { method: "POST", body: readyBody })
        .then(function (data) {
          var msg = "Saved.";
          if (data.restart_started) {
            msg = "Saved. Restarting service so the new bind address takes effect…";
          } else if (data.restart_required) {
            msg = "Saved. Restart PA (pa restart) for the bind change to take effect.";
          }
          if (readyStatus) readyStatus.textContent = msg;
          setTimeout(refreshFleetPage, data.restart_started ? 2500 : 600);
        })
        .catch(function (err) {
          if (readyStatus) readyStatus.textContent = err.message;
        });
    }
  });
})();
