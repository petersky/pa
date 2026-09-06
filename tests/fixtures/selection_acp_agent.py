"""Tool-free ACP wire fixture. It never runs shell commands or edits files."""

import json
import sys
from uuid import uuid4


def advertised(harness="codex"):
    model_ids = {
        "codex": ["fixture-balanced", "fixture-deep"],
        "cursor": ["fixture-grok"],
        "openinterpreter": ["fixture-minimax"],
    }[harness]
    efforts = {
        "codex": ["low", "xhigh"],
        "cursor": ["native-deep"],
        "openinterpreter": [],
    }[harness]
    options = [
        {
            "id": "model",
            "category": "model",
            "name": "Model",
            "type": "select",
            "currentValue": model_ids[0],
            "options": [{"value": m, "name": m} for m in model_ids],
        }
    ]
    if efforts:
        options.append(
            {
                "id": "reasoning_effort",
                "category": "thought_level",
                "name": "Native reasoning",
                "type": "select",
                "currentValue": efforts[0],
                "options": [{"value": e, "name": e} for e in efforts],
            }
        )
    modes = ["default", "read-only", "agent", "agent-full-access"]
    options.append(
        {
            "id": "mode",
            "category": "mode",
            "name": "Fixture permission mode",
            "type": "select",
            "currentValue": "default",
            "options": [
                {"value": m, "name": m + " (tool-free fixture)"} for m in modes
            ],
        }
    )
    return {
        "models": {
            "availableModels": [{"modelId": m, "name": m} for m in model_ids],
            "currentModelId": model_ids[0],
        },
        "config_options": options,
        "modes": {
            "currentModeId": "default",
            "availableModes": [
                {"id": m, "name": m + " (tool-free fixture)"} for m in modes
            ],
        },
    }


def main():
    harness = sys.argv[1] if len(sys.argv) > 1 else "codex"
    state = advertised(harness)
    sid = str(uuid4())
    for line in sys.stdin:
        request = json.loads(line)
        method, params = request.get("method"), request.get("params") or {}
        if "id" not in request:
            continue
        result = {}
        if method == "initialize":
            result = {
                "protocolVersion": 1,
                "agentInfo": {"name": "pa-selection-test-fixture", "version": "1"},
                "agentCapabilities": {
                    "loadSession": True,
                    "promptCapabilities": {"image": True},
                },
                "authMethods": [],
            }
        elif method in {"session/new", "session/load"}:
            sid = params.get("sessionId") or sid
            result = {
                "sessionId": sid,
                "models": state["models"],
                "configOptions": state["config_options"],
                "modes": state["modes"],
            }
        elif method == "session/set_config_option":
            for option in state["config_options"]:
                if option["id"] == params.get("configId"):
                    option["currentValue"] = params["value"]
                    if option["id"] == "model":
                        state["models"]["currentModelId"] = params["value"]
                    elif option["id"] == "mode":
                        state["modes"]["currentModeId"] = params["value"]
            result = {"configOptions": state["config_options"]}
        elif method == "session/set_model":
            state["models"]["currentModelId"] = params["modelId"]
        elif method == "session/set_mode":
            state["modes"]["currentModeId"] = params["modeId"]
            next(o for o in state["config_options"] if o["id"] == "mode")[
                "currentValue"
            ] = params["modeId"]
        elif method == "session/prompt":
            print(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "method": "session/update",
                        "params": {
                            "sessionId": sid,
                            "update": {
                                "sessionUpdate": "agent_message_chunk",
                                "content": {
                                    "type": "text",
                                    "text": "Fixture provider responded. No task implementation, tests, or repository completion is claimed.",
                                },
                            },
                        },
                    }
                ),
                flush=True,
            )
            result = {"stopReason": "end_turn"}
        print(
            json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}),
            flush=True,
        )


if __name__ == "__main__":
    main()
