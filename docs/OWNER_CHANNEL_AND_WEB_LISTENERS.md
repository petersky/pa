# Owner channel and web listeners

PA exposes two deliberately separate network surfaces.

## Private owner channel

`pa serve` creates one Unix-domain socket before the application admits agent
sessions. PA-launched MCP bridges use this socket by default and continue to send
`PA_LOCAL_API_TOKEN`, `PA_INSTANCE_ID`, `X-PA-MCP-Instance-ID`, and a fresh
`X-Request-ID`. Responses are still instance-verified. MCP processes remain API
clients and must never open the Store, EventLog, `pa.db`, or sync refs.

The socket is selected in this order:

1. `PA_OWNER_SOCKET`, when an operator/service manager supplies an exact path;
2. `$PA_RUNTIME_DIR/<instance-hash>/owner.sock`;
3. `$XDG_RUNTIME_DIR/pa/<instance-hash>/owner.sock`;
4. the host runtime temp directory under `pa-<uid>/<instance-hash>/owner.sock`.

Directories are mode `0700` and sockets mode `0600`. Paths that exceed the
portable Unix socket limit are replaced with a hashed runtime path. Startup
removes only a stale socket owned by the service uid; it refuses non-sockets,
foreign paths, and live sockets. Graceful shutdown removes the socket. Container
and service-manager deployments should mount a private runtime directory shared
only by the PA server and PA-launched children and set `PA_RUNTIME_DIR` or
`PA_OWNER_SOCKET` consistently.

On systems without a shared Unix namespace, an operator may explicitly set
`PA_OWNER_API_URL` to a private, authenticated HTTP endpoint in the shared
namespace. PA never derives this fallback from `instance_url`, Fleet metadata, or
web binds. The existing bounded reconnect/circuit behavior applies to both
transports; a child reports a reload/reconnect action after incompatible API,
authentication, or instance fencing failures.

## Web listeners

`PA_WEB_LISTENERS` is a JSON or comma-separated list. Each entry is `HOST` (using
`PA_PORT`) or `HOST:PORT`; use `[IPv6]:PORT` when overriding an IPv6 port.
`pa config add web_listeners VALUE` persists entries. An empty list preserves the
legacy `PA_HOST` bind.

Examples:

```text
PA_WEB_LISTENERS='["127.0.0.1", "localhost", "100.78.2.112"]'
PA_WEB_LISTENERS='["127.0.0.1:8080", "[::1]:8080", "100.78.2.112:8443"]'
PA_WEB_LISTENERS='["0.0.0.0", "::"]'
```

Names such as `localhost` bind every address returned by the resolver, so IPv4
and IPv6 loopback can coexist where supported. IPv6 sockets are forced to
IPv6-only mode to avoid platform-dependent dual-stack conflicts. Explicit
addresses are recommended; wildcard listeners intentionally cover current and
future interfaces.

Each configured listener is resolved and bound independently. Address absence,
port conflicts, and family-specific failures are logged against the exact
listener while other healthy listeners (and the private owner channel) stay up.
Current failed binds require a service-manager restart after the address or port
is repaired; status reports this as `retry_state=restart_required`. Same-port and
per-listener-port layouts are both supported.

`instance_url` remains the sole advertised Fleet URL and is never inferred from
the owner socket or listener list. Browser cookie security and allowed origins
continue to follow the configured public origin/auth policy: use secure cookies
behind HTTPS, and configure each non-loopback origin explicitly. Health/status
surfaces report sanitized listener and owner state and never expose tokens.
