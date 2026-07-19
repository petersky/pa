#!/usr/bin/env bash
# Create or amend a PA release with agent-generated notes.
#
# Usage:
#   ./scripts/release.sh patch
#   ./scripts/release.sh patch --ship     # PR, checks, merge, tag, publish
#   ./scripts/release.sh minor
#   ./scripts/release.sh patch --no-push   # local only, no push
#   ./scripts/release.sh 1.2.3 --channel beta
#   ./scripts/release.sh --amend --tag v0.1.2
#
# Environment:
#   PA_RELEASE_AGENT          Agent command (default: agent)
#   PA_RELEASE_AGENT_ARGS     Agent args (default: --print --trust)
#   PA_RELEASE_AGENT_TIMEOUT  Agent timeout in seconds (default: 300)
#   PA_RELEASE_AGENT_USE_STDIN  Pass prompt on stdin if set to 1
#
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
exec uv run python -m pa.release.script "$@"
