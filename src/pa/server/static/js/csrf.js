/* Shared, live double-submit CSRF support for every browser surface. */
(function () {
  "use strict";

  if (window.PACSRF) return;

  var COOKIE_NAME = "pa_csrf";
  var HEADER_NAME = "X-CSRF-Token";
  var RECOVERABLE_CODES = new Set(["csrf_mismatch", "csrf_expired", "csrf_invalid"]);

  function cookieToken() {
    var prefix = COOKIE_NAME + "=";
    var cookies = String(document.cookie || "").split(";");
    for (var i = 0; i < cookies.length; i += 1) {
      var value = cookies[i].trim();
      if (value.indexOf(prefix) === 0) {
        try { return decodeURIComponent(value.slice(prefix.length)); } catch (_) { return value.slice(prefix.length); }
      }
    }
    return "";
  }

  function synchronize() {
    var meta = document.querySelector('meta[name="csrf-token"]');
    var token = cookieToken() || (meta && meta.content) || "";
    if (meta && token && meta.content !== token) meta.content = token;
    document.querySelectorAll('input[name="_csrf"]').forEach(function (input) {
      if (token && input.value !== token) input.value = token;
    });
    return token;
  }

  function headers(extra) {
    var result = Object.assign({}, extra || {});
    var token = synchronize();
    if (token) result[HEADER_NAME] = token;
    return result;
  }

  function errorCode(body) {
    var detail = body && body.detail;
    return detail && typeof detail === "object" ? detail.code || "" : "";
  }

  function retryEligible(options) {
    var method = String(options.method || "GET").toUpperCase();
    if (method === "GET" || method === "HEAD" || method === "OPTIONS") return true;
    var values = new Headers(options.headers || {});
    return Boolean(values.get("Idempotency-Key"));
  }

  function request(input, options) {
    var original = Object.assign({}, options || {});
    var originalHeaders = Object.assign({}, original.headers || {});

    function attempt(retried) {
      var current = Object.assign({}, original, {
        credentials: original.credentials || "same-origin",
        headers: headers(originalHeaders),
      });
      return fetch(input, current).then(function (response) {
        if (response.status !== 403 || retried || !retryEligible(original)) return response;
        return response.clone().json().catch(function () { return {}; }).then(function (body) {
          var code = errorCode(body);
          if (!RECOVERABLE_CODES.has(code)) return response;
          synchronize();
          return attempt(true).then(function (retryResponse) {
            if (!retryResponse.ok) {
              retryResponse.paCsrfRecoveryFailed = true;
              retryResponse.paCsrfErrorCode = code;
            }
            return retryResponse;
          });
        });
      });
    }
    return attempt(false);
  }

  document.addEventListener("visibilitychange", synchronize);
  window.addEventListener("focus", synchronize);
  document.addEventListener("htmx:configRequest", function (event) {
    var token = synchronize();
    if (token && event.detail && event.detail.headers) event.detail.headers[HEADER_NAME] = token;
  });

  window.PACSRF = {
    cookieToken: cookieToken,
    synchronize: synchronize,
    headers: headers,
    fetch: request,
    errorCode: errorCode,
  };
  synchronize();
})();
