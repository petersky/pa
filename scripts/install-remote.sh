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
#   PA_SKIP_SERVICE=1            Skip launchd registration (macOS)
set -euo pipefail

REPO="${PA_GITHUB_REPO:-petersky/pa}"
NAME="${PA_INSTANCE_NAME:-local}"
CHANNEL="${PA_CHANNEL:-release}"

resolve_ref_from_channel() {
  local track="$1"
  local channels_url="https://raw.githubusercontent.com/${REPO}/main/channels.json"
  local ref=""

  if command -v jq >/dev/null 2>&1; then
    ref="$(curl -fsSL "${channels_url}" | jq -r --arg ch "${track}" '.[$ch] // empty' 2>/dev/null || true)"
  else
    ref="$(curl -fsSL "${channels_url}" 2>/dev/null | sed -n "s/.*\"${track}\"[[:space:]]*:[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p" | head -1 || true)"
  fi

  if [[ -z "${ref}" ]]; then
    case "${track}" in
      dev) ref="main" ;;
      release) ref="main" ;;
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

if [[ ! -f "${HOME}/.pa/config.json" ]]; then
  echo "Initializing instance '${NAME}'..."
  "${PA_BIN}" init --name "${NAME}"
fi

if [[ "$(uname -s)" == "Darwin" && "${PA_SKIP_SERVICE:-0}" != "1" ]]; then
  echo "Registering launchd service..."
  "${PA_BIN}" install --service-only
  echo "Starting service..."
  "${PA_BIN}" start
fi

echo ""
echo "PA installed successfully."
echo "  Version: $("${PA_BIN}" version)"
echo "  Track:   ${CHANNEL} (${REF})"
echo "  Binary:  ${PA_BIN}"
echo "  Server:  http://127.0.0.1:8080"
echo "  Status:  ${PA_BIN} status"
echo "  Logs:    ${PA_BIN} logs -f"
