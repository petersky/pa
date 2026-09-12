const fs = require('fs');
const vm = require('vm');
const assert = require('assert');
const noop = () => {};
const document = {
  body: {addEventListener: noop}, addEventListener: noop,
  querySelector: () => null, querySelectorAll: () => [],
  createElement: () => ({setAttribute: noop}),
};
const window = {addEventListener: noop};
vm.runInNewContext(fs.readFileSync(process.argv[2], 'utf8'), {
  window, document, console: {debug: noop}, URL, setTimeout, clearTimeout,
});
const widget = Object.create(window.PAAgentChat.AgentChatWidget.prototype);
widget.els = {status: {parentNode: {appendChild: noop}}};
widget.renderMcpHealth({state: 'checking'});
assert.equal(widget.els.mcpHealth.textContent, 'PA tools starting');
widget.renderMcpHealth({state: 'disconnected', detail: 'PA MCP client timed out after 30 seconds'});
assert.equal(widget.els.mcpHealth.textContent, 'PA tools unavailable');
assert.equal(widget.els.mcpHealth.title, 'PA MCP client timed out after 30 seconds');
widget.renderMcpHealth({state: 'connected'});
assert.equal(widget.els.mcpHealth.hidden, true);
assert.equal(widget.els.mcpHealth.title, '');
