(function () {
  "use strict";

  const Drafts = window.PAAgentDrafts;
  if (!Drafts) return;

  const controllers = new Set();
  let lifecycleBound = false;

  function forEachConnectedController(callback) {
    controllers.forEach(function (controller) {
      if (!controller.widget.root.isConnected) {
        controllers.delete(controller);
        return;
      }
      callback(controller);
    });
  }

  function attachmentMetadata(images) {
    return (images || []).map(function (image) {
      return {
        name: image.name || "attachment",
        mime_type: image.mime_type || "",
        size: Number(image.size || 0),
      };
    });
  }

  function documentScope() {
    const root = document.documentElement;
    return {
      instanceId: root && root.dataset.paInstanceId || "local",
      principalId: root && root.dataset.paPrincipalId || "user:local",
    };
  }

  function bindLifecycle() {
    if (lifecycleBound) return;
    lifecycleBound = true;
    window.addEventListener("storage", function (event) {
      forEachConnectedController(function (controller) {
        controller.onStorage(event);
      });
    });
    window.addEventListener("pagehide", function () {
      forEachConnectedController(function (controller) {
        controller.flush({ force: true });
      });
    });
    document.addEventListener("visibilitychange", function () {
      if (document.visibilityState !== "hidden") return;
      forEachConnectedController(function (controller) {
        controller.flush({ force: true });
      });
    });
    document.addEventListener("htmx:beforeSwap", function () {
      forEachConnectedController(function (controller) {
        controller.flush({ force: true });
      });
    });
  }

  function WidgetDraftController(widget) {
    this.widget = widget;
    const scope = documentScope();
    this.instanceId = widget.root.dataset.draftInstanceId || scope.instanceId;
    this.principalId = widget.root.dataset.draftPrincipalId || scope.principalId;
    this.store = new Drafts.DraftStore({
      instanceId: this.instanceId,
      principalId: this.principalId,
    });
    this.sessionId = widget.sessionId || "";
    this.record = null;
    this.cardId = widget.cardId || null;
    this.projectId = null;
    this.attachmentMetadata = [];
    this.submissionId = null;
    this.dirty = false;
    this.composing = false;
    this.timer = null;
    this.status = widget.root.querySelector("[data-acw-draft-status]");
    this.attachmentNotice = widget.root.querySelector("[data-acw-draft-attachments]");
    this.clearButton = widget.root.querySelector("[data-acw-clear-draft]");
    this._bind();
    this.store.gc();
    controllers.add(this);
    bindLifecycle();
    this.restore();
  }

  WidgetDraftController.prototype._bind = function () {
    const self = this;
    const input = this.widget.els.input;
    if (input) {
      input.addEventListener("compositionstart", function () {
        self.composing = true;
      });
      input.addEventListener("compositionend", function () {
        self.composing = false;
        self.changed();
      });
      input.addEventListener("input", function () {
        self.changed();
      });
      ["select", "click", "keyup"].forEach(function (name) {
        input.addEventListener(name, function () {
          if (!self.composing) self.schedule();
        });
      });
    }
    if (this.clearButton) {
      this.clearButton.addEventListener("click", function () {
        self.clear(true, "Draft cleared from this browser.");
      });
    }
  };

  WidgetDraftController.prototype.setStatus = function (message) {
    if (this.status) this.status.textContent = message;
  };

  WidgetDraftController.prototype.schedule = function () {
    const self = this;
    if (!this.sessionId || this.composing) return;
    if (this.timer) clearTimeout(this.timer);
    this.timer = setTimeout(function () {
      self.timer = null;
      self.flush();
    }, 300);
  };

  WidgetDraftController.prototype.changed = function () {
    if (this.submissionId && !this.widget.submissionPending) {
      this.submissionId = null;
    }
    this.dirty = true;
    if (!this.composing && this.widget.els.input && !this.widget.els.input.value) {
      this.flush({ force: true });
      return;
    }
    this.schedule();
  };

  WidgetDraftController.prototype._selection = function () {
    const input = this.widget.els.input;
    return {
      start: input && typeof input.selectionStart === "number" ? input.selectionStart : 0,
      end: input && typeof input.selectionEnd === "number" ? input.selectionEnd : 0,
      direction: input && input.selectionDirection || "none",
    };
  };

  WidgetDraftController.prototype._metadata = function () {
    if (this.widget.pendingImages && this.widget.pendingImages.length) {
      return attachmentMetadata(this.widget.pendingImages);
    }
    return this.attachmentMetadata.slice();
  };

  WidgetDraftController.prototype.flush = function (options) {
    options = options || {};
    if (!this.sessionId || (!options.force && this.composing)) return null;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    const input = this.widget.els.input;
    const selection = this._selection();
    const text = input ? input.value : "";
    const metadata = this._metadata();
    const empty = !text && !metadata.length && !this.submissionId;
    const result = empty
      ? this.store.clear(this.sessionId, this.record)
      : this.store.write(this.sessionId, {
          text: text,
          selection_start: selection.start,
          selection_end: selection.end,
          selection_direction: selection.direction,
          attachments: metadata,
          submission_id: this.submissionId,
          card_id: this.cardId,
          project_id: this.projectId,
          cleared: false,
        }, this.record);
    if (result && result.record) this.record = result.record;
    this.dirty = false;
    if (!result || !result.persisted) {
      if (result && result.error === "too-large") {
        const marker = this.store.clear(this.sessionId, this.record);
        if (marker && marker.record) this.record = marker.record;
        this.attachmentMetadata = [];
        this.setStatus("Draft exceeds the 64K-character local limit and is not saved.");
      } else if (result && result.error === "quota") {
        this.setStatus("Browser storage is full; this draft is only in this tab.");
      } else {
        this.setStatus("Local draft storage is unavailable; this draft is only in this tab.");
      }
    } else if (empty) {
      this.setStatus("No local draft.");
    } else {
      this.setStatus("Draft saved locally.");
    }
    return result;
  };

  WidgetDraftController.prototype._discardBinaryAttachments = function () {
    (this.widget.pendingImages || []).forEach(function (image) {
      if (image.preview) URL.revokeObjectURL(image.preview);
    });
    this.widget.pendingImages = [];
    this.widget.renderPendingImages();
  };

  WidgetDraftController.prototype.renderAttachmentNotice = function () {
    if (!this.attachmentNotice) return;
    if (!this.attachmentMetadata.length || (this.widget.pendingImages || []).length) {
      this.attachmentNotice.hidden = true;
      this.attachmentNotice.textContent = "";
      return;
    }
    const names = this.attachmentMetadata.map(function (item) { return item.name; }).join(", ");
    this.attachmentNotice.textContent =
      "Attachments are not stored. Reselect before sending: " + names;
    this.attachmentNotice.hidden = false;
  };

  WidgetDraftController.prototype.apply = function (record, message) {
    this.record = record || null;
    this.submissionId = record && record.submission_id || null;
    this.attachmentMetadata = record && !record.cleared
      ? (record.attachments || []).slice()
      : [];
    this._discardBinaryAttachments();
    const input = this.widget.els.input;
    if (input) {
      input.value = record && !record.cleared ? record.text : "";
      if (record && !record.cleared && typeof input.setSelectionRange === "function") {
        const limit = input.value.length;
        try {
          input.setSelectionRange(
            Math.min(limit, Number(record.selection_start || 0)),
            Math.min(limit, Number(record.selection_end || 0)),
            record.selection_direction || "none"
          );
        } catch (_) {}
      }
    }
    this.renderAttachmentNotice();
    this.dirty = false;
    if (message) this.setStatus(message);
    else if (record && !record.cleared) this.setStatus("Draft restored from this browser.");
    else this.setStatus("No local draft.");
  };

  WidgetDraftController.prototype.restore = function () {
    if (!this.sessionId) {
      this.apply(null, "Select a session to restore its local draft.");
      return;
    }
    this.apply(this.store.read(this.sessionId));
  };

  WidgetDraftController.prototype.switchSession = function (sessionId) {
    if (sessionId === this.sessionId) return;
    this.flush({ force: true });
    this.sessionId = String(sessionId || "");
    this.cardId = null;
    this.projectId = null;
    this.submissionId = null;
    this.attachmentMetadata = [];
    this.restore();
  };

  WidgetDraftController.prototype.setInstance = function (instanceId) {
    const next = String(instanceId || documentScope().instanceId);
    if (next === this.instanceId) return;
    this.flush({ force: true });
    this.instanceId = next;
    this.store = new Drafts.DraftStore({
      instanceId: this.instanceId,
      principalId: this.principalId,
    });
    this.store.gc();
    this.sessionId = "";
    this.apply(null, "Select a session to restore its local draft.");
  };

  WidgetDraftController.prototype.onSnapshot = function (session) {
    session = session || {};
    this.cardId = session.card_id || this.widget.cardId || null;
    this.projectId = session.project_id || null;
    if (session.status === "closed") this.clear(true, "Draft cleared because this session ended.");
  };

  WidgetDraftController.prototype.onStorage = function (event) {
    const update = this.store.fromStorageEvent(event);
    if (!update || update.sessionId !== this.sessionId) return;
    if (this.dirty || this.composing) {
      this.flush({ force: true });
      return;
    }
    this.apply(update.record, update.record.cleared
      ? "Draft cleared in another tab."
      : "Newer draft restored from another tab.");
  };

  WidgetDraftController.prototype.clear = function (clearInput, message) {
    if (!this.sessionId) return;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.submissionId = null;
    this.attachmentMetadata = [];
    this._discardBinaryAttachments();
    if (clearInput && this.widget.els.input) this.widget.els.input.value = "";
    const result = this.store.clear(this.sessionId, this.record);
    if (result && result.record) this.record = result.record;
    this.dirty = false;
    this.renderAttachmentNotice();
    this.setStatus(result && result.persisted
      ? (message || "Draft cleared from this browser.")
      : "Draft cleared in this tab; browser storage is unavailable.");
  };

  WidgetDraftController.prototype.beginSubmission = function () {
    if (!this.submissionId) this.submissionId = Drafts.randomId();
    this.flush({ force: true });
    return this.submissionId;
  };

  WidgetDraftController.prototype.submissionAccepted = function (submission) {
    const input = this.widget.els.input;
    if (input && input.value === submission.rawText) input.value = "";
    const submitted = submission.images || [];
    this.widget.pendingImages = (this.widget.pendingImages || []).filter(function (image) {
      if (submitted.indexOf(image) === -1) return true;
      if (image.preview) URL.revokeObjectURL(image.preview);
      return false;
    });
    this.widget.renderPendingImages();
    this.submissionId = null;
    this.attachmentMetadata = [];
    if (
      (!input || !input.value) &&
      !(this.widget.pendingImages && this.widget.pendingImages.length)
    ) {
      this.clear(false, "Draft cleared after the prompt was accepted.");
    } else {
      this.dirty = true;
      this.flush({ force: true });
    }
  };

  WidgetDraftController.prototype.submissionFailed = function (submission) {
    submission = submission || {};
    const inputChanged = this.widget.els.input &&
      this.widget.els.input.value !== submission.rawText;
    const currentImages = this.widget.pendingImages || [];
    const submittedImages = submission.images || [];
    const imagesChanged = currentImages.length !== submittedImages.length ||
      currentImages.some(function (image, index) {
        return image !== submittedImages[index];
      });
    if (submission.conflict || inputChanged || imagesChanged) this.submissionId = null;
    this.dirty = true;
    this.flush({ force: true });
    this.setStatus(this.submissionId
      ? "Prompt was not confirmed; the draft is retained and retry is safe."
      : "Draft changed after the attempt; it is retained for a new submission.");
  };

  Drafts.installWidget = function (widget) {
    return new WidgetDraftController(widget);
  };
})();
