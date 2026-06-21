#!/usr/bin/env bash
# Builds the proxy and three demo backends, then runs them locally
# (no Docker) on the ports referenced by config.yaml.
set -euo pipefail

cd "$(dirname "$0")/.."

mkdir -p bin
go build -o bin/proxy ./cmd/proxy
go build -o bin/backend ./cmd/backend

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

./bin/backend -name=backend-1 -addr=:9001 &
pids+=($!)
./bin/backend -name=backend-2 -addr=:9002 &
pids+=($!)
./bin/backend -name=backend-3 -addr=:9003 &
pids+=($!)

sleep 1

echo "Starting proxy on :8080 (Ctrl+C to stop everything)"
./bin/proxy -config=config.yaml
