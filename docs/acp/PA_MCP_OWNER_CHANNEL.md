# PA MCP owner channel

The embedded `pa` MCP server is a child of the owning PA process. It talks to
PA only through the authenticated HTTP API and never opens or mutates
`PA_DATA_DIR` stores directly.

The parent resolves a canonical owner endpoint from the actual listener bind:

- IPv4 and IPv6 wildcard binds use loopback in the same address family.
- Concrete loopback, LAN, VPN, and IPv6 binds use that concrete listener.
- CLI host/port overrides are captured in `PA_OWNER_API_URL`.
- Advertised fleet URLs are never used for this channel.

Because ACP and MCP are subprocesses, they share the parent's network namespace
in supported service-manager and container deployments. Deployments that move
children into another namespace must expose an explicit locally reachable
listener there; PA rejects session admission if the probe cannot reach it.

Before starting an ACP provider, PA probes `/api/ready` with the local API token
and instance fence. It distinguishes unreachable, rejected authentication,
instance mismatch, incompatible API, and API-not-ready states. A failed probe
prevents prompt delivery and includes the sanitized endpoint class and recovery
action. The MCP client independently verifies `X-PA-Instance-ID` on responses.

Transient request failures retry briefly. Persistent failures open a bounded
circuit so subsequent tool calls fail quickly while periodic calls perform
recovery probes. Tokens are never included in health snapshots or logs.
