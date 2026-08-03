# Settings performance budgets

The Settings route renders the local shell and one requested section. Remote/provider
validation is never part of the shell request. These budgets are enforced with
production-shaped fixtures and should be measured for both direct and HTMX navigation.

| Measure | Budget |
| --- | ---: |
| Shell server response (warm p95) | 200 ms |
| First useful render | 500 ms |
| Initial Settings transfer | 150 KiB |
| Configuration section transfer | 300 KiB |
| Local section completion | 2 s |
| Remote probe deadline | 5 s |
| Shell database reads | 4 |
| Shell subprocess launches | 0 |
| 20 repeated navigations | no latency/resource growth |

Responses expose `Server-Timing` entries for `shell`, `page_context`, `template`, and
`total`, plus the non-sensitive `X-PA-Settings-Section` and
`X-PA-Settings-Bytes` diagnostics. Structured `settings.render` logs contain only the
section name, durations, and byte count. They must never include configuration values,
environment values, credentials, authentication state, request bodies, or secret fields.

Provider/model options use live session inventory or durable cached session inventory;
rendering Settings must not launch a provider or MCP subprocess. MCP tests and provider
probes remain explicit user refresh actions. Configuration, backup, prompt, telemetry,
and service/status work is isolated to its named section so a slow peer or provider
cannot delay the Agent and Appearance controls.
