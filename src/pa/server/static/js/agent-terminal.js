/* A read-only projection of retained ACP evidence. Never interprets HTML/ANSI. */
(function (root) {
  "use strict";
  function valueText(value) {
    return typeof value === "string" ? value : JSON.stringify(value, null, 2);
  }
  function contentText(content) {
    if (!Array.isArray(content)) return "";
    return content.map(function (item) {
      if (!item || typeof item !== "object") return "[Unsupported tool content]";
      const block = item.type === "content" ? item.content : item;
      if (block && block.type === "text" && typeof block.text === "string") return block.text;
      if (item.type === "terminal") return "[Terminal reference: " + (item.terminalId || item.terminal_id || "unknown") + "]";
      return "[Non-text tool content: " + (item.type || "unknown") + "]";
    }).join("\n");
  }
  function project(events, sessionId) {
    const calls = new Map(), seen = new Set();
    let turn = "history";
    (events || []).slice().sort((a, b) => (a.seq || 0) - (b.seq || 0)).forEach(function (event) {
      if (event.session_id && event.session_id !== sessionId) return;
      const type = event.type || event.event_type, p = event.payload || {};
      const identity = event.seq ? "seq:" + event.seq : event.id;
      if (identity && seen.has(identity)) return;
      if (identity) seen.add(identity);
      if (type === "user_message") turn = p.id || p.prompt_id || String(event.seq || event.id);
      if (type !== "tool_call" && type !== "tool_call_update") return;
      if (typeof p.tool_call_id !== "string" || !p.tool_call_id) return;
      const raw = p.raw || {}, meta = raw._meta || {};
      const key = JSON.stringify([p.external_session_id || sessionId, p.provider_turn_id || p.turn_id || turn, p.tool_call_id]);
      let call = calls.get(key);
      if (!call) {
        call = { key: key, id: p.tool_call_id, name: "Tool", status: "unknown", input: "", output: "", chunks: "", content: "", rawOutput: "", at: event.created_at };
        calls.set(key, call);
      }
      if (p.name || raw.name || p.title || p.kind) call.name = p.name || raw.name || p.title || p.kind;
      const terminal = ["completed", "failed", "cancelled", "success", "succeeded"];
      if (p.status && (terminal.indexOf(call.status) === -1 || terminal.indexOf(p.status) !== -1)) call.status = p.status;
      const input = p.tool_input !== undefined ? p.tool_input : p.raw_input !== undefined ? p.raw_input : raw.rawInput;
      if (input != null) call.input = valueText(input);
      const output = p.tool_output !== undefined ? p.tool_output : p.raw_output !== undefined ? p.raw_output : raw.rawOutput;
      if (output != null) call.rawOutput = valueText(output);
      if (p.content != null || raw.content != null) call.content = contentText(p.content || raw.content);
      if (Array.isArray(p.output_chunks)) call.chunks += p.output_chunks.join("");
      if (p.terminal_exit) call.rawOutput = "Exit code: " + String(p.terminal_exit.exit_code);
      if (!Array.isArray(p.output_chunks)) ["terminal_output_delta", "terminal_output", "mcp_output_delta"].forEach(function (field) {
        const chunk = meta[field];
        if (chunk && typeof chunk.data === "string" && (!chunk.terminal_id || chunk.terminal_id === p.tool_call_id)) call.chunks += chunk.data;
      });
      if (meta.terminal_exit && (!meta.terminal_exit.terminal_id || meta.terminal_exit.terminal_id === p.tool_call_id)) {
        call.rawOutput = "Exit code: " + String(meta.terminal_exit.exit_code);
      }
      call.output = [call.chunks, call.content, call.rawOutput].filter(Boolean).join("\n");
    });
    return Array.from(calls.values());
  }
  function truncate(text, limit) {
    const chars = Array.from(text || "");
    return chars.length > limit ? chars.slice(0, limit).join("") + "\n… [truncated: " + chars.length + " characters; showing " + limit + "]" : text;
  }
  function render(container, events, sessionId, limit) {
    container.replaceChildren();
    const calls = project(events, sessionId);
    if (!calls.length) container.textContent = "No tool calls in loaded history. Load older messages to view earlier calls.";
    calls.forEach(function (call) {
      const row = document.createElement("section"), heading = document.createElement("strong");
      row.className = "acw-terminal-call";
      heading.textContent = call.name + " · " + call.status;
      row.appendChild(heading);
      const stamp = document.createElement("small");
      stamp.textContent = (call.at || "") + " · " + call.id;
      row.appendChild(stamp);
      if (call.input) {
        const input = document.createElement("pre");
        input.textContent = "$ " + truncate(call.input, limit);
        row.appendChild(input);
      }
      const output = document.createElement("pre");
      output.textContent = truncate(call.output, limit) || "[No output supplied]";
      row.appendChild(output);
      container.appendChild(row);
    });
  }
  root.PAAgentTerminal = { project: project, truncate: truncate, render: render };
})(typeof window !== "undefined" ? window : globalThis);
