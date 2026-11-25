(function () {
  var button = document.querySelector('[data-workspace-menu]');
  var rail = document.getElementById('workspace-rail');
  function setOpen(open, restoreFocus) {
    document.body.classList.toggle('is-navigation-open', open);
    if (button) {
      button.setAttribute('aria-expanded', String(open));
      button.setAttribute('aria-label', open ? 'Close workspace navigation' : 'Open workspace navigation');
      if (restoreFocus) button.focus();
      if (open && rail) {
        var first = rail.querySelector('a[href], button');
        if (first) first.focus();
      }
    }
  }
  if (button) button.addEventListener('click', function () {
    setOpen(!document.body.classList.contains('is-navigation-open'));
  });
  document.addEventListener('keydown', function (event) {
    if (event.key === 'Escape' && document.body.classList.contains('is-navigation-open')) {
      setOpen(false, true);
    }
  });
  document.addEventListener('click', function (event) {
    if (!rail || !button || rail.contains(event.target) || button.contains(event.target)) return;
    setOpen(false);
  });
  ['htmx:pushedIntoHistory', 'htmx:replacedInHistory', 'htmx:historyRestore'].forEach(function (name) {
    document.body.addEventListener(name, function () { setOpen(false); });
  });
  function bindDensity() {
    document.querySelectorAll('[data-density-preference]').forEach(function (select) {
      select.value = document.documentElement.dataset.density || 'default';
      if (select.dataset.densityBound) return;
      select.dataset.densityBound = '1';
      select.addEventListener('change', function () {
        var density = select.value === 'compact' ? 'compact' : 'default';
        localStorage.setItem('pa.density', density);
        document.documentElement.dataset.density = density;
      });
    });
  }
  bindDensity();
  document.body.addEventListener('htmx:afterSwap', bindDensity);
})();
