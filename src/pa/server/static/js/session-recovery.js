(function () {
  "use strict";

  if (window.PASessionRecovery) return;

  function detail(error) {
    return error && error.detail && typeof error.detail === "object"
      ? error.detail
      : {};
  }

  function code(error) {
    return String(detail(error).code || "");
  }

  function parseRetryAfter(value, now) {
    if (value == null || value === "") return 0;
    var seconds = Number(value);
    if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1000;
    var timestamp = Date.parse(String(value));
    return Number.isFinite(timestamp)
      ? Math.max(0, timestamp - (now == null ? Date.now() : now))
      : 0;
  }

  function responseRetryAfterMs(response, responseDetail, now) {
    var header = response && response.headers && response.headers.get
      ? parseRetryAfter(response.headers.get("Retry-After"), now)
      : 0;
    var body = Number(responseDetail && responseDetail.retry_after_ms || 0);
    return Math.max(header, Number.isFinite(body) && body > 0 ? body : 0);
  }

  function retryDelayMs(error, attempt, options) {
    options = options || {};
    var minimum = Math.max(1, Number(options.minimumMs || 250));
    var maximum = Math.max(minimum, Number(options.maximumMs || 30000));
    var requested = Math.max(
      Number(error && error.retryAfterMs || 0),
      Number(detail(error).retry_after_ms || 0),
      minimum
    );
    var exponent = Math.min(8, Math.max(0, Number(attempt || 0)));
    var backedOff = Math.min(maximum, requested * Math.pow(2, exponent));
    var random = typeof options.random === "function" ? options.random : Math.random;
    var jitterRatio = Math.max(0, Number(options.jitterRatio == null ? 0.2 : options.jitterRatio));
    var jitter = Math.floor(Math.min(maximum - backedOff, backedOff * jitterRatio * random()));
    return Math.max(minimum, backedOff + jitter);
  }

  function expectedCancellation(error) {
    return !!(error && error.name === "AbortError");
  }

  function Controller(options) {
    this.options = options || {};
    this.attempt = 0;
    this.timer = null;
    this.abortController = null;
    this.promise = null;
    this.generation = 0;
    this.cancelled = false;
  }

  Controller.prototype.active = function () {
    return !this.cancelled && (!this.options.isActive || this.options.isActive());
  };

  Controller.prototype.cancel = function (reason) {
    this.cancelled = true;
    this.generation += 1;
    this.attempt = 0;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    if (this.abortController) this.abortController.abort(reason || "session-recovery-cancelled");
    this.abortController = null;
    this.promise = null;
  };

  Controller.prototype.start = function (force) {
    var self = this;
    if (this.cancelled) this.cancelled = false;
    if (!this.active()) return Promise.resolve(null);
    if (this.promise) {
      if (!force) return this.promise;
      this.generation += 1;
      this.attempt = 0;
      if (this.timer) clearTimeout(this.timer);
      this.timer = null;
      if (this.abortController) {
        this.abortController.abort("session-recovery-forced");
      }
      this.abortController = null;
      this.promise = null;
    } else if (this.timer) {
      if (!force) return Promise.resolve(null);
      clearTimeout(this.timer);
      this.timer = null;
    }
    var generation = this.generation;
    var controller = new AbortController();
    this.abortController = controller;
    var operation = this.options.operation;
    var promise = Promise.resolve().then(function () {
      return operation(controller.signal);
    }).then(function (value) {
      if (!self.active() || generation !== self.generation) return null;
      self.attempt = 0;
      if (self.options.onSuccess) self.options.onSuccess(value);
      return value;
    }).catch(function (error) {
      if (!self.active() || generation !== self.generation || expectedCancellation(error)) {
        return null;
      }
      if (code(error) !== "agent_recovery_in_progress") {
        self.attempt = 0;
        if (self.options.onError) self.options.onError(error);
        return null;
      }
      var delay = retryDelayMs(error, self.attempt, self.options);
      self.attempt += 1;
      if (self.options.onRecovery) self.options.onRecovery(error, delay);
      self.timer = setTimeout(function () {
        self.timer = null;
        if (!self.active() || generation !== self.generation) return;
        self.start(false);
      }, delay);
      return null;
    }).finally(function () {
      if (self.abortController === controller) self.abortController = null;
      if (self.promise === promise) self.promise = null;
    });
    this.promise = promise;
    return promise;
  };

  window.PASessionRecovery = {
    Controller: Controller,
    code: code,
    detail: detail,
    expectedCancellation: expectedCancellation,
    parseRetryAfter: parseRetryAfter,
    responseRetryAfterMs: responseRetryAfterMs,
    retryDelayMs: retryDelayMs,
  };
})();
