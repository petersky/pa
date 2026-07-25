(function () {
  "use strict";

  const VERSION = 1;
  const PREFIX = "pa.agent-chat-draft.v1:";
  const RETENTION_MS = 30 * 24 * 60 * 60 * 1000;
  const MAX_TEXT_LENGTH = 65536;

  function encodePart(value) {
    return encodeURIComponent(String(value || ""));
  }

  function recordOrder(record) {
    return [
      Number(record && record.revision || 0),
      Number(record && record.updated_at || 0),
      String(record && record.writer_id || ""),
    ];
  }

  function compareRecords(left, right) {
    const a = recordOrder(left);
    const b = recordOrder(right);
    for (let index = 0; index < a.length; index += 1) {
      if (a[index] < b[index]) return -1;
      if (a[index] > b[index]) return 1;
    }
    return 0;
  }

  function randomId() {
    try {
      if (window.crypto && typeof window.crypto.randomUUID === "function") {
        return window.crypto.randomUUID();
      }
      if (window.crypto && typeof window.crypto.getRandomValues === "function") {
        const bytes = new Uint8Array(16);
        window.crypto.getRandomValues(bytes);
        return Array.from(bytes, function (byte) {
          return byte.toString(16).padStart(2, "0");
        }).join("");
      }
    } catch (_) {}
    return String(Date.now()) + "-" + Math.random().toString(36).slice(2);
  }

  function browserStorage() {
    try {
      return window.localStorage || null;
    } catch (_) {
      return null;
    }
  }

  function DraftStore(options) {
    options = options || {};
    this.instanceId = String(options.instanceId || "local");
    this.principalId = String(options.principalId || "user:local");
    this.writerId = String(options.writerId || randomId());
    this.storage = Object.prototype.hasOwnProperty.call(options, "storage")
      ? options.storage
      : browserStorage();
    this.now = options.now || function () { return Date.now(); };
    this.retentionMs = Number(options.retentionMs || RETENTION_MS);
    this.maxTextLength = Number(options.maxTextLength || MAX_TEXT_LENGTH);
    this.memory = new Map();
    this.lastError = "";
  }

  DraftStore.prototype.scopePrefix = function () {
    return PREFIX + encodePart(this.instanceId) + ":" + encodePart(this.principalId) + ":";
  };

  DraftStore.prototype.key = function (sessionId) {
    return this.scopePrefix() + encodePart(sessionId);
  };

  DraftStore.prototype._valid = function (record, sessionId) {
    if (!record || typeof record !== "object" || record.version !== VERSION) return false;
    if (record.instance_id !== this.instanceId || record.principal_id !== this.principalId) return false;
    if (record.session_id !== String(sessionId || "")) return false;
    if (!Number.isFinite(Number(record.updated_at)) || !Number.isFinite(Number(record.revision))) return false;
    if (typeof record.text !== "string" || record.text.length > this.maxTextLength) return false;
    if (!Array.isArray(record.attachments)) return false;
    return true;
  };

  DraftStore.prototype._parse = function (raw, sessionId) {
    if (!raw) return null;
    try {
      const record = JSON.parse(raw);
      if (!this._valid(record, sessionId)) return null;
      if (this.now() - Number(record.updated_at) > this.retentionMs) return null;
      return record;
    } catch (_) {
      return null;
    }
  };

  DraftStore.prototype.read = function (sessionId) {
    const sid = String(sessionId || "");
    if (!sid) return null;
    let stored = null;
    if (this.storage) {
      try {
        stored = this._parse(this.storage.getItem(this.key(sid)), sid);
      } catch (_) {
        this.lastError = "unavailable";
      }
    }
    const memory = this.memory.get(sid) || null;
    const record = compareRecords(stored, memory) >= 0 ? stored : memory;
    if (record) this.memory.set(sid, record);
    return record;
  };

  DraftStore.prototype._removeExpired = function () {
    if (!this.storage) return 0;
    const now = this.now();
    let removed = 0;
    try {
      for (let index = this.storage.length - 1; index >= 0; index -= 1) {
        const key = this.storage.key(index);
        if (!key || key.indexOf(PREFIX) !== 0) continue;
        let record = null;
        try { record = JSON.parse(this.storage.getItem(key) || "null"); } catch (_) {}
        if (
          !record ||
          !Number.isFinite(Number(record.updated_at)) ||
          now - Number(record.updated_at) > this.retentionMs
        ) {
          this.storage.removeItem(key);
          removed += 1;
        }
      }
    } catch (_) {
      this.lastError = "unavailable";
    }
    return removed;
  };

  DraftStore.prototype.gc = function () {
    const now = this.now();
    this.memory.forEach(function (record, sessionId, memory) {
      if (now - Number(record.updated_at || 0) > this.retentionMs) memory.delete(sessionId);
    }, this);
    return this._removeExpired();
  };

  DraftStore.prototype.write = function (sessionId, value, knownRecord) {
    const sid = String(sessionId || "");
    if (!sid) return { record: null, persisted: false, error: "missing-session" };
    value = value || {};
    const text = String(value.text || "");
    if (text.length > this.maxTextLength) {
      this.lastError = "too-large";
      return { record: this.read(sid), persisted: false, error: "too-large" };
    }
    const current = this.read(sid);
    const baseRevision = Math.max(
      Number(current && current.revision || 0),
      Number(knownRecord && knownRecord.revision || 0)
    );
    const updatedAt = Math.max(
      Number(this.now()),
      Number(current && current.updated_at || 0) + 1,
      Number(knownRecord && knownRecord.updated_at || 0) + 1
    );
    const attachments = (Array.isArray(value.attachments) ? value.attachments : [])
      .slice(0, 4)
      .map(function (item) {
        return {
          name: String(item && item.name || "attachment").slice(0, 255),
          mime_type: String(item && item.mime_type || "").slice(0, 100),
          size: Math.max(0, Number(item && item.size || 0)),
        };
      });
    const record = {
      version: VERSION,
      instance_id: this.instanceId,
      principal_id: this.principalId,
      session_id: sid,
      card_id: value.card_id ? String(value.card_id) : null,
      project_id: value.project_id ? String(value.project_id) : null,
      text: text,
      selection_start: Math.max(0, Number(value.selection_start || 0)),
      selection_end: Math.max(0, Number(value.selection_end || 0)),
      selection_direction: ["forward", "backward", "none"].includes(value.selection_direction)
        ? value.selection_direction
        : "none",
      attachments: attachments,
      submission_id: value.submission_id ? String(value.submission_id).slice(0, 128) : null,
      cleared: !!value.cleared,
      revision: baseRevision + 1,
      updated_at: updatedAt,
      expires_at: updatedAt + this.retentionMs,
      writer_id: this.writerId,
    };
    this.memory.set(sid, record);
    if (!this.storage) {
      this.lastError = "unavailable";
      return { record: record, persisted: false, error: "unavailable" };
    }
    const serialized = JSON.stringify(record);
    try {
      this.storage.setItem(this.key(sid), serialized);
      this.lastError = "";
      return { record: record, persisted: true, error: "" };
    } catch (_) {
      this._removeExpired();
      try {
        this.storage.setItem(this.key(sid), serialized);
        this.lastError = "";
        return { record: record, persisted: true, error: "" };
      } catch (retryError) {
        this.lastError = retryError && retryError.name === "QuotaExceededError"
          ? "quota"
          : "unavailable";
        return { record: record, persisted: false, error: this.lastError };
      }
    }
  };

  DraftStore.prototype.clear = function (sessionId, knownRecord) {
    return this.write(sessionId, {
      text: "",
      attachments: [],
      selection_start: 0,
      selection_end: 0,
      selection_direction: "none",
      submission_id: null,
      cleared: true,
    }, knownRecord);
  };

  DraftStore.prototype.fromStorageEvent = function (event) {
    if (!event || event.storageArea !== this.storage || !event.key) return null;
    const prefix = this.scopePrefix();
    if (event.key.indexOf(prefix) !== 0) return null;
    let sessionId = "";
    try { sessionId = decodeURIComponent(event.key.slice(prefix.length)); } catch (_) { return null; }
    const record = this._parse(event.newValue, sessionId);
    if (!record) return null;
    const current = this.memory.get(sessionId) || null;
    if (compareRecords(record, current) <= 0) return null;
    this.memory.set(sessionId, record);
    return { sessionId: sessionId, record: record };
  };

  window.PAAgentDrafts = {
    VERSION: VERSION,
    PREFIX: PREFIX,
    RETENTION_MS: RETENTION_MS,
    MAX_TEXT_LENGTH: MAX_TEXT_LENGTH,
    DraftStore: DraftStore,
    compareRecords: compareRecords,
    randomId: randomId,
  };
})();
