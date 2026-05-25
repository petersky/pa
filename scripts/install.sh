#!/usr/bin/env bash
# Install PA on the host (production instance).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="${PA_INSTANCE_NAME:-local}"

if ! command -v uv >/dev/null 2>&1; then
  echo "error: uv is required. Install from https://docs.astral.sh/uv/" >&2
  exit 1
fi

echo "Installing PA from ${ROOT}..."
uv tool install --force "${ROOT}"

PA_BIN="$(command -v pa || echo "${HOME}/.local/bin/pa")"

if [[ ! -f "${HOME}/.pa/config.json" ]]; then
  echo "Initializing instance '${NAME}'..."
  "${PA_BIN}" init --name "${NAME}"
fi

if [[ "$(uname -s)" == "Darwin" ]]; then
  echo "Registering launchd service..."
  "${PA_BIN}" install --service-only
  echo "Starting service..."
  "${PA_BIN}" start
fi

echo "PA installed."
echo "  Binary: ${PA_BIN}"
echo "  Server: http://127.0.0.1:8080"
echo "  Logs:   pa logs -f"
