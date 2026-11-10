(() => {
  const widgets = new WeakMap();
  function initialize() {
    document.querySelectorAll('[data-agent-chat]').forEach(widget => {
      if (widgets.has(widget)) return;
      const button = widget.querySelector('[data-acw-reminders]');
      const dialog = widget.querySelector('[data-acw-reminders-dialog]');
      if (!button || !dialog) return;
      let revision = 0, displayedSession = null;
      const status = dialog.querySelector('[data-acw-reminders-status]');
      const list = dialog.querySelector('[data-acw-reminders-list]');
      async function refresh() {
        const requestRevision = ++revision;
        const sid = widget.dataset.sessionId;
        if (displayedSession !== sid) { list.replaceChildren(); status.textContent = 'Loading reminders…'; displayedSession = sid; }
        // A remote owner's ledger must be queried on that owner, never locally.
        const remote = widget.dataset.ownerInstanceId && widget.dataset.currentInstanceId && widget.dataset.ownerInstanceId !== widget.dataset.currentInstanceId;
        if (!sid || remote) { button.hidden = !sid; button.setAttribute('aria-label', 'Session reminder discovery unavailable'); list.replaceChildren(); status.textContent = 'Reminder discovery unavailable for this session owner. Open the owning instance to inspect its ledger.'; return; }
        try {
          let cursor = null;
          const records = [];
          do {
            const params = new URLSearchParams({session_id:sid, pending_only:'true', limit:'200'});
            if (cursor) params.set('cursor', cursor);
            const response = await fetch('/api/schedules?' + params);
            if (!response.ok) throw Error('Reminder discovery unavailable');
            const page = await response.json();
            records.push(...page.schedules);
            cursor = page.next_cursor;
          } while (cursor && requestRevision === revision && widget.isConnected);
          if (requestRevision !== revision || widget.dataset.sessionId !== sid) return;
          button.hidden = records.length === 0;
          button.setAttribute('aria-label', `Pending session reminders (${records.length})`);
          status.textContent = `${records.length ? records.length + ' pending reminders.' : 'No pending reminders.'} Provider-owned reminders (including Codex): discovery unsupported by integrated ACP capabilities.`;
          list.replaceChildren();
          for (const record of records) {
            const article = document.createElement('article');
            const title = document.createElement('a');
            title.href = '/loops#loop-' + encodeURIComponent(record.id);
            title.textContent = record.name;
            const detail = document.createElement('p');
            detail.textContent = `${record.state} · ${record.trigger.kind} · ${record.trigger.at ?? record.trigger.cron ?? record.trigger.seconds ?? record.trigger.event_type ?? ''} ${record.trigger.timezone} · ` + [record.destination, ...(record.additional_destinations ?? [])].map(d => `${d.kind}: ${d.target}`).join('; ');
            article.append(title, detail);
            list.append(article);
          }
        } catch (error) {
          if (requestRevision !== revision) return;
          button.hidden = false;
          button.setAttribute('aria-label', 'Session reminder discovery unavailable');
          status.textContent = String(error);
          list.replaceChildren();
        }
      }
      widgets.set(widget, refresh);
      button.addEventListener('click', () => { dialog.showModal(); refresh(); });
      dialog.querySelector('[data-acw-reminders-close]').addEventListener('click', () => dialog.close());
      new MutationObserver(refresh).observe(widget, {attributes:true, attributeFilter:['data-session-id','data-owner-instance-id']});
      refresh();
    });
  }
  function refreshAll() { initialize(); document.querySelectorAll('[data-agent-chat]').forEach(widget => widgets.get(widget)?.()); }
  document.addEventListener('pa:loops-changed', refreshAll);
  document.addEventListener('visibilitychange', () => { if (!document.hidden) refreshAll(); });
  new MutationObserver(initialize).observe(document.body, {childList:true, subtree:true});
  setInterval(() => { if (!document.hidden) refreshAll(); }, 15000);
  initialize();
})();
