#!/usr/bin/env bash
# Install PA from GitHub (curl-friendly).
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/petersky/pa/main/scripts/install-remote.sh | bash
#
# Options (environment variables):
#   PA_GITHUB_REPO=petersky/pa   GitHub repository
#   PA_CHANNEL=release           Release track: release, beta, alpha, dev
#   PA_GIT_REF=                  Override ref (tag or branch); skips channel lookup
#   PA_INSTANCE_NAME=local       Instance name for pa init
#   PA_INSTANCE_URL=             Public URL (Tailscale), e.g. http://mini:8080
#   PA_FLEET_OWNER_URL=          Fleet owner URL when joining with PA_FLEET_TOKEN
#   PA_FLEET_TOKEN=              One-time fleet join token
#   PA_SYNC_TOKEN=               Shared sync secret for inter-instance API
#   PA_PEERS=                    Comma-separated peer URLs
#   PA_REALM=                    Primary realm ID
#   PA_HOST=0.0.0.0              Bind host (default 0.0.0.0 when fleet vars set)
#   PA_PORT=8080                 Server port
#   PA_SKIP_SERVICE=1            Skip service registration
set -euo pipefail

REPO="${PA_GITHUB_REPO:-petersky/pa}"
NAME="${PA_INSTANCE_NAME:-local}"
CHANNEL="${PA_CHANNEL:-release}"
PORT="${PA_PORT:-8080}"

if [[ -n "${PA_FLEET_TOKEN:-}" || -n "${PA_PEERS:-}" || -n "${PA_INSTANCE_URL:-}" ]]; then
  HOST="${PA_HOST:-0.0.0.0}"
else
  HOST="${PA_HOST:-127.0.0.1}"
fi

resolve_ref_from_channel() {
  local track="$1"
  local channels_url="https://raw.githubusercontent.com/${REPO}/main/channels.json"
  local ref=""

  if ! curl -fsSL "${channels_url}" -o /tmp/pa-channels.json; then
    if [[ "${track}" == "release" ]]; then
      echo "error: could not fetch channels.json for release track" >&2
      exit 1
    fi
    echo "main"
    return
  fi

  if command -v jq >/dev/null 2>&1; then
    ref="$(jq -r --arg ch "${track}" '.[$ch] // empty' /tmp/pa-channels.json 2>/dev/null || true)"
  else
    ref="$(sed -n "s/.*\"${track}\"[[:space:]]*:[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p" /tmp/pa-channels.json | head -1 || true)"
  fi

  if [[ -z "${ref}" ]]; then
    case "${track}" in
      dev) ref="main" ;;
      release)
        echo "error: release track missing from channels.json" >&2
        exit 1
        ;;
      *) ref="main" ;;
    esac
  fi
  echo "${ref}"
}

if [[ -n "${PA_GIT_REF:-}" ]]; then
  REF="${PA_GIT_REF}"
else
  REF="$(resolve_ref_from_channel "${CHANNEL}")"
fi

INSTALL_SPEC="git+https://github.com/${REPO}.git@${REF}"

if ! command -v uv >/dev/null 2>&1; then
  echo "Installing uv..."
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="${HOME}/.local/bin:${PATH}"
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "error: uv not found after install. Add ~/.local/bin to PATH." >&2
  exit 1
fi

echo "Installing PA from ${INSTALL_SPEC} (track: ${CHANNEL})..."
uv tool install --force "${INSTALL_SPEC}"

PA_BIN="$(command -v pa 2>/dev/null || echo "${HOME}/.local/bin/pa")"
if [[ ! -x "${PA_BIN}" && -x "${HOME}/.local/bin/pa" ]]; then
  PA_BIN="${HOME}/.local/bin/pa"
fi

if [[ ! -x "${PA_BIN}" ]]; then
  echo "error: pa binary not found after install" >&2
  exit 1
fi

INSTANCE_URL="${PA_INSTANCE_URL:-http://127.0.0.1:${PORT}}"
if [[ -n "${PA_INSTANCE_URL:-}" ]]; then
  INSTANCE_URL="${PA_INSTANCE_URL%/}"
fi

if [[ ! -f "${HOME}/.pa/config.json" ]]; then
  echo "Initializing instance '${NAME}'..."
  INIT_ARGS=(--name "${NAME}" --track "${CHANNEL}" --url "${INSTANCE_URL}")
  [[ -n "${PA_REALM:-}" ]] && INIT_ARGS+=(--realm "${PA_REALM}")
  [[ -n "${PA_PEERS:-}" ]] && INIT_ARGS+=(--peers "${PA_PEERS}")
  [[ -n "${PA_SYNC_TOKEN:-}" ]] && INIT_ARGS+=(--sync-token "${PA_SYNC_TOKEN}")
  PA_HOST="${HOST}" PA_PORT="${PORT}" "${PA_BIN}" init "${INIT_ARGS[@]}"
fi

export PA_HOST="${HOST}"
export PA_PORT="${PORT}"
export PA_RELEASE_TRACK="${CHANNEL}"
[[ -n "${PA_SYNC_TOKEN:-}" ]] && export PA_SYNC_TOKEN
[[ -n "${PA_PEERS:-}" ]] && export PA_PEERS
[[ -n "${PA_INSTANCE_URL:-}" ]] && export PA_INSTANCE_URL
[[ -n "${PA_FLEET_OWNER_URL:-}" ]] && export PA_FLEET_OWNER_URL

"${PA_BIN}" install --record-only --channel "${CHANNEL}"

SERVICE_OK=1
if [[ "${PA_SKIP_SERVICE:-0}" != "1" ]]; then
  if [[ "$(uname -s)" == "Darwin" || "$(uname -s)" == "Linux" ]]; then
    echo "Registering service..."
    if ! "${PA_BIN}" install --service-only; then
      echo "error: service registration failed" >&2
      SERVICE_OK=0
    elif ! "${PA_BIN}" start; then
      echo "error: service start failed" >&2
      SERVICE_OK=0
    fi
  fi
fi

if [[ -n "${PA_FLEET_TOKEN:-}" ]]; then
  OWNER_URL="${PA_FLEET_OWNER_URL:-}"
  if [[ -z "${OWNER_URL}" ]]; then
    echo "warning: PA_FLEET_TOKEN set but PA_FLEET_OWNER_URL missing; skipping fleet join" >&2
  else
    echo "Joining fleet at ${OWNER_URL}..."
    export PA_FLEET_OWNER_URL="${OWNER_URL}"
    JOINED=0
    for attempt in 1 2 3 4 5; do
      if PA_FLEET_OWNER_URL="${OWNER_URL}" "${PA_BIN}" fleet join "${PA_FLEET_TOKEN}" \
        --url "${INSTANCE_URL}" --owner "${OWNER_URL}" --name "${NAME}"; then
        JOINED=1
        break
      fi
      sleep 2
    done
    if [[ "${JOINED}" -eq 0 ]]; then
      echo "warning: fleet join failed; run manually after server is up:" >&2
      echo "  PA_FLEET_OWNER_URL=${OWNER_URL} pa fleet join <token> --url ${INSTANCE_URL}" >&2
    else
      "${PA_BIN}" install --service-only || true
    fi
  fi
fi

if [[ "${SERVICE_OK}" -eq 0 ]]; then
  echo "error: PA installed but service is not running. Check: ${PA_BIN} doctor" >&2
  exit 1
fi

echo ""
echo "PA installed successfully."
echo "  Version: $("${PA_BIN}" version)"
echo "  Track:   ${CHANNEL} (${REF})"
echo "  Binary:  ${PA_BIN}"
echo "  Server:  ${INSTANCE_URL}"
echo "  Status:  ${PA_BIN} status"
echo "  Doctor:  ${PA_BIN} doctor"
echo "  Logs:    ${PA_BIN} logs -f"
