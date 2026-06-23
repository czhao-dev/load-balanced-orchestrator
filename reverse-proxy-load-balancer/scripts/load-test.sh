#!/usr/bin/env bash
# Runs a load test against a running proxy using hey (https://github.com/rakyll/hey).
# Usage: scripts/load-test.sh [url] [total-requests] [concurrency]
set -euo pipefail

URL="${1:-http://localhost:8080/api/hello}"
N="${2:-10000}"
C="${3:-100}"

if ! command -v hey >/dev/null 2>&1; then
  echo "hey is required: go install github.com/rakyll/hey@latest" >&2
  exit 1
fi

hey -n "$N" -c "$C" "$URL"
