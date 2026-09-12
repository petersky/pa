// Extracted verbatim from codex-acp 1.11.0; no provider process is launched.
class Adapter {
  static createMcpStartupUpdates(event) {
    const failedUpdates = event.failed.map((server) => this.createMcpStartupToolCallUpdate(
      server.server,
      `[codex-acp forwarded startup error] MCP server \`${server.server}\` failed to start: ${server.error}`
    ));
    const cancelledUpdates = event.cancelled.map((server) => this.createMcpStartupToolCallUpdate(
      server,
      `[codex-acp forwarded startup error] MCP server \`${server}\` startup was cancelled.`
    ));
    return [...failedUpdates, ...cancelledUpdates];
  }
  static createMcpStartupToolCallUpdate(serverName, message) {
    return {
      sessionUpdate: "tool_call",
      toolCallId: this.getMcpStartupToolCallId(serverName),
      kind: "other",
      title: `mcp__${serverName}__startup`,
      status: "failed",
      content: [{
        type: "content",
        content: {
          type: "text",
          text: message
        }
      }]
    };
  }
  static getMcpStartupToolCallId(serverName) {
    return `mcp_startup.${encodeURIComponent(serverName)}`;
  }
}
function createMcpRawInput(server, tool, argumentsValue) {
  return {
    server,
    tool,
    arguments: argumentsValue
  };
}
function createMcpRawOutput(result, error51) {
  if (result === null && error51 === null) {
    return void 0;
  }
  return {
    result,
    error: error51
  };
}

function complete(event) { switch(event.item.type) {
      case "mcpToolCall":
        return {
          sessionUpdate: "tool_call_update",
          toolCallId: event.item.id,
          status: event.item.status === "completed" ? "completed" : "failed",
          rawInput: createMcpRawInput(event.item.server, event.item.tool, event.item.arguments),
          rawOutput: createMcpRawOutput(event.item.result, event.item.error)
        };
}}
const success = complete({item:{type:"mcpToolCall",id:"call-pa-1",status:"completed",server:"pa",tool:"list_items",arguments:{},result:{content:[{type:"text",text:"[]"}],isError:false},error:null}});
process.stdout.write(JSON.stringify({ready:Adapter.createMcpStartupUpdates({ready:["pa"],failed:[],cancelled:[]}),failed:Adapter.createMcpStartupUpdates({ready:[],failed:[{server:"pa",error:"PA MCP client timed out after 30 seconds"}],cancelled:[]}),success}));
