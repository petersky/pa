(function () {
  // The shell survives HTMX navigation, and card dialogs update browser history.
  // Capture the current location at activation, then validate it again on the server.
  document.addEventListener("click", function (event) {
    var link = event.target.closest("a[data-sign-in]");
    if (link) {
      var current = new URL(window.location.href);
      if (link.dataset.signInCard) {
        current.searchParams.set("card", link.dataset.signInCard);
        current.searchParams.set("realm", link.dataset.signInRealm);
      }
      link.href = "/login?" + new URLSearchParams({
        next: current.pathname + current.search + current.hash
      });
    }
    document.querySelectorAll("[data-account-menu][open]").forEach(function (menu) {
      if (!menu.contains(event.target)) menu.open = false;
    });
  });
  document.addEventListener("keydown", function (event) {
    if (event.key !== "Escape") return;
    document.querySelectorAll("[data-account-menu][open]").forEach(function (menu) {
      menu.open = false;
      menu.querySelector("summary").focus();
    });
  });
  document.addEventListener("submit", function (event) {
    var form = event.target;
    if (!form.matches("[data-sign-out]")) return;
    form.elements.next.value = location.pathname + location.search + location.hash;
    if (window.PACSRF) form.elements.csrf.value = window.PACSRF.synchronize();
  });
})();
