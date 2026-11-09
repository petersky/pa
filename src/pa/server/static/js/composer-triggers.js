/* Trigger parsing for the chat composer (`/commands`, `$skills`, and later `@mentions`).
 *
 * Pure text functions so the rules live in one place and can be tested without a browser:
 *   find(text, caret, enabled)   the trigger token under the caret, or null
 *   replace(text, found, insert) text and caret after completing that token
 *   rank(items, query, options)  prefix matches first, then word-part, then substring
 */
(function (root) {
  "use strict";

  // startOnly: the trigger must begin the whole message. query: what may follow it.
  var TRIGGERS = {
    "/": { startOnly: true, query: /^\S*$/ },
    "$": { startOnly: false, query: /^[\w:.-]*$/ },
  };

  function isSpace(character) {
    return /\s/.test(character);
  }

  function find(text, caret, enabled) {
    text = String(text || "");
    var position = typeof caret === "number" ? Math.max(0, Math.min(caret, text.length)) : text.length;
    var start = position;
    while (start > 0 && !isSpace(text.charAt(start - 1))) start -= 1;
    if (start === position) return null;
    var trigger = text.charAt(start);
    var rule = TRIGGERS[trigger];
    if (!rule || (enabled && enabled.indexOf(trigger) === -1)) return null;
    if (rule.startOnly && start !== 0) return null;
    var end = position;
    while (end < text.length && !isSpace(text.charAt(end))) end += 1;
    var query = text.slice(start + 1, end);
    // "//" is the escape for literal leading-slash text, so it never opens a menu.
    if (trigger === "/" && query.charAt(0) === "/") return null;
    if (!rule.query.test(query)) return null;
    return { trigger: trigger, start: start, end: end, query: query };
  }

  function replace(text, found, insertion) {
    text = String(text || "");
    var after = text.slice(found.end);
    // Always leave the caret one space past the insertion, reusing a space that is already there.
    var spacer = /^\s/.test(after) ? "" : " ";
    return {
      text: text.slice(0, found.start) + insertion + spacer + after,
      caret: found.start + insertion.length + 1,
    };
  }

  function rank(items, query, options) {
    var settings = options || {};
    var name = settings.name || function (item) { return item.name; };
    var describe = settings.description || function (item) { return item.description || ""; };
    var strip = settings.strip || "";
    var needle = String(query || "").toLowerCase();
    var scored = [];
    items.forEach(function (item) {
      var label = String(name(item));
      if (strip && label.charAt(0) === strip) label = label.slice(1);
      var lower = label.toLowerCase();
      var score;
      if (!needle) score = 0;
      else if (lower.indexOf(needle) === 0) score = 0;
      else if (lower.split(/[:\-_.]/).some(function (part) { return part.indexOf(needle) === 0; })) score = 1;
      else if (lower.indexOf(needle) !== -1) score = 2;
      else if (String(describe(item)).toLowerCase().indexOf(needle) !== -1) score = 3;
      else return;
      scored.push({ item: item, score: score, label: lower });
    });
    scored.sort(function (a, b) {
      return a.score - b.score || (a.label < b.label ? -1 : a.label > b.label ? 1 : 0);
    });
    return scored.map(function (entry) { return entry.item; });
  }

  root.PAComposerTriggers = { find: find, replace: replace, rank: rank, TRIGGERS: TRIGGERS };
})(typeof window !== "undefined" ? window : globalThis);
