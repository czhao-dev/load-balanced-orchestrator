# Go Reverse Proxy / Load Balancer

[![Go Version](https://img.shields.io/badge/Go-1.23%2B-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](docker-compose.yml)
[![YAML Config](https://img.shields.io/badge/Config-YAML-CB171E?logo=yaml&logoColor=white)](config.yaml)
[![Prometheus Metrics](https://img.shields.io/badge/Metrics-Prometheus%20format-E6522C?logo=prometheus&logoColor=white)](#observability)

A reverse proxy and HTTP load balancer written from scratch in Go, with no third-party HTTP or proxy frameworks. It forwards client requests to a pool of backend servers, actively monitors backend health, fails over around broken backends, and exposes Prometheus-style metrics — the same core mechanics found in API gateways and edge proxies like NGINX, HAProxy, and Envoy, implemented in a small, readable codebase.

## Why this exists

Reverse proxies sit on the critical path of nearly every distributed system: they decide which backend serves a request, how failures are detected and routed around, and what operators can observe about traffic in flight. This project builds those mechanics from first principles — manual request forwarding, pluggable load-balancing strategies, a health-check state machine, retry/failover logic, and a metrics registry — instead of wrapping an existing proxy library, in order to demonstrate the engineering underneath that category of infrastructure.

## Features

* **Reverse proxy forwarding** — rewrites and streams requests to backends, preserving path, query parameters, and headers; injects `X-Forwarded-For`, `X-Forwarded-Host`, and `X-Forwarded-Proto`.
* **Load-balancing strategies** — round robin, least connections, and weighted round robin, selected via config.
* **Active health checking** — concurrent per-backend probes on a configurable interval/path, with independent healthy/unhealthy consecutive-count thresholds to avoid flapping.
* **Retry and failover** — idempotent requests (`GET`/`HEAD`/`OPTIONS`) are retried against a different healthy backend on connection failure or timeout, up to a configurable limit; non-idempotent methods are never silently retried.
* **Per-backend connection tracking** — atomic in-flight request counters feed both the least-connections strategy and the metrics endpoint.
* **Graceful shutdown** — stops accepting new connections on `SIGINT`/`SIGTERM` while letting in-flight requests finish, bounded by a configurable timeout.
* **Structured logging** — JSON or text logs via `log/slog`, including backend health transitions and forwarding failures.
* **Observability endpoints** — `/healthz`, `/admin/backends` (live per-backend JSON status), and `/metrics` (Prometheus text exposition format, hand-rolled with no client library dependency).
* **YAML-driven configuration** — server timeouts, load-balancer strategy, health-check thresholds, and the backend pool are all declared in `config.yaml`.

## Architecture

### Request lifecycle

```
Client
  │
  ▼
http.Server  (cmd/proxy)
  │  ServeHTTP
  ▼
proxy.Handler                     internal/proxy
  │  1. balancer.Next(pool) ─────▶ internal/balancer (round robin / least conn / weighted)
  │                                       │ reads pool.Healthy()
  │  2. build outbound request            ▼
  │     (director.go: rewrite URL,   internal/backend (Pool, Backend atomics)
  │      inject X-Forwarded-*)            ▲
  │  3. client.Do via shared             │ IsAlive() / ActiveConnections()
  │     keep-alive Transport              │
  │  4. on failure + idempotent      internal/health.Checker
  │     method → retry next backend       │ probes /health on interval,
  │  5. stream response, or                │ flips Backend.alive after
  │     502/503/504 if exhausted           │ N consecutive failures/successes
  ▼                                        │
Client response               internal/metrics.Metrics ◀── counters updated
                                            │              on every request/check
                               /metrics, /admin/backends, /healthz
```

### Design decisions

**Manual forwarding instead of `httputil.ReverseProxy`.** The standard library's `ReverseProxy` forwards to a single fixed target per request; retrying against a *different* backend after a failed attempt needs control over exactly when response headers get committed to the client. `Handler.proxyOnce` (`internal/proxy/handler.go`) only writes the upstream's status line and headers to the client after a response has been successfully received — a dial failure, connection refusal, or timeout happens before anything is written to the client, so the same request can be retried against another backend transparently. This is also why redirects are never followed automatically (`CheckRedirect` returns `http.ErrUseLastResponse`): a reverse proxy must hand a 3xx straight to the client, not chase it on the client's behalf.

**Retry safety is method-aware.** Only `GET`, `HEAD`, and `OPTIONS` are retried automatically. Retrying a `POST` against a second backend after an ambiguous failure (e.g. the first backend processed the request but the response was lost) could duplicate a side effect, so non-idempotent methods get exactly one attempt and surface the failure to the client instead.

**Lock-free hot path.** `Backend` (`internal/backend/backend.go`) stores liveness, active-connection count, and request/error counters as `atomic.Bool`/`atomic.Int64` fields rather than behind a mutex, since these are read and written on every single proxied request. `Pool` itself is an immutable slice built once at startup — health checks and the balancer only ever mutate the atomic fields *inside* each `Backend`, so no locking is needed to read a consistent backend list under concurrent load.

**Health is a hysteresis state machine, not a single probe.** `health.Checker` (`internal/health/checker.go`) tracks a signed consecutive-count per backend: positive runs of successes, negative runs of failures. A backend only flips from alive to dead after `unhealthy_threshold` consecutive failed probes, and only recovers after `healthy_threshold` consecutive successful ones. This absorbs single transient blips (a dropped packet, a slow GC pause) without removing a backend from rotation, while still reacting decisively to a real outage.

**Metrics push, not pull, on every state change.** Rather than having the `/metrics` handler reach into the balancer and health checker at scrape time, each component (the proxy handler, the health checker) pushes counter/gauge updates into a shared `metrics.Metrics` registry as events happen. `Render()` then just formats whatever is currently in the registry. This keeps the metrics package fully decoupled from balancing and health-check logic — it has no imports from either.

**Why round robin / least connections / weighted round robin specifically.** Round robin is the right default when backends are roughly homogeneous and request cost is uniform. Least connections corrects for the common case where request latency varies — it routes around a backend that's currently slow rather than blindly continuing to send it 1-in-N requests. Weighted round robin exists for heterogeneous backend capacity (e.g. a bigger instance that should take proportionally more traffic) and is implemented as a deterministic cumulative-weight walk rather than a "current weight" decay algorithm, trading a small bias under concurrent access for a simpler, easily-tested implementation.

### Package layout

```
cmd/
  proxy/        entrypoint: loads config, wires balancer + health checker + handler, serves, handles shutdown
  backend/      minimal demo upstream used for local testing and the Docker Compose demo
internal/
  config/       YAML parsing, defaults, validation (config.go, duration.go)
  backend/      Backend (atomic health/connection/request state) and Pool
  balancer/     Balancer interface + round robin, least connections, weighted round robin
  health/       active health checker with consecutive-threshold hysteresis
  proxy/        HTTP handler: backend selection, retry/failover, header rewriting, response streaming
  metrics/      Prometheus-format counter/gauge registry, no external dependency
  logging/      structured slog.Logger construction (JSON/text, level)
  admin/        /admin/backends JSON status handler
tests/integration/ end-to-end tests against real httptest backends (no mocks)
```

## Load-Balancing Strategies

* **Round robin** (`internal/balancer/round_robin.go`) — an atomic counter cycles through the currently-healthy backend list. Simple, and fair when backends are equivalent.
* **Least connections** (`least_conn.go`) — scans healthy backends and picks the one with the fewest in-flight requests, tracked via the per-backend atomic connection counter. Better than round robin when request cost is uneven.
* **Weighted round robin** (`weighted.go`) — walks a cumulative-weight ring sized to the sum of healthy backend weights, so a backend with weight 3 receives ~3x the traffic of a backend with weight 1.

All three strategies only ever consider `pool.Healthy()`, so an unhealthy backend is structurally excluded from selection, not just deprioritized.

## Health Checks

Each backend is probed concurrently on `health_check.interval` against `health_check.path`. A probe counts as a success only on a 2xx response within `health_check.timeout`; anything else — non-2xx status, timeout, or connection error — counts as a failure. The checker tracks consecutive successes/failures per backend and only flips `Backend.alive` once `healthy_threshold` or `unhealthy_threshold` consecutive results are observed, which is what prevents a single slow response from yanking a backend out of rotation. Every transition is logged, and the live status of each backend is always available at `/admin/backends` and as the `proxy_backend_health_status` metric.

## Retry and Failover

When a request fails against the selected backend with a transport-level error (connection refused, timeout) — as opposed to the backend returning a valid HTTP response, even an error one — the proxy retries against a different backend selected fresh from the current healthy pool, up to `load_balancer.max_retries` additional attempts. Retries are only performed for idempotent methods. If every attempt is exhausted, the client receives `502 Bad Gateway` (forwarding/connection failure), `504 Gateway Timeout` (backend exceeded `backend_timeout`), or `503 Service Unavailable` (no healthy backend was available to try in the first place).

## Configuration

The proxy is configured entirely through a YAML file (`config.yaml` by default, override with `-config`):

| Section | Key | Purpose |
|---|---|---|
| `server` | `listen_addr`, `read_timeout`, `write_timeout`, `shutdown_timeout` | HTTP server tuning and graceful-shutdown deadline |
| `load_balancer` | `strategy`, `max_retries`, `backend_timeout` | `round_robin` \| `least_conn` \| `weighted_round_robin`, retry budget, per-attempt timeout |
| `health_check` | `enabled`, `path`, `interval`, `timeout`, `unhealthy_threshold`, `healthy_threshold` | active probe behavior |
| `backends` | `name`, `url`, `weight` | the backend pool |
| `logging` | `level`, `format` | `slog` level and `json`/`text` output |
| `metrics` | `enabled`, `path` | toggles and locates the Prometheus-format endpoint |

## API Endpoints

| Endpoint | Description |
|---|---|
| `/*` | Proxied traffic — forwarded to the backend pool per the configured strategy |
| `/healthz` | Liveness check for the proxy process itself |
| `/admin/backends` | JSON status (health, weight, active connections, request/error counts) for every backend |
| `/metrics` | Prometheus text-exposition metrics |

## Observability

`/metrics` exposes, per backend where applicable:

* `proxy_up`, `proxy_requests_total`, `proxy_retries_total`, `proxy_request_duration_seconds_{count,sum}`
* `proxy_backend_requests_total{backend=...}`, `proxy_backend_errors_total{backend=...}`
* `proxy_backend_active_connections{backend=...}`, `proxy_backend_health_status{backend=...}`

These are emitted in standard Prometheus text format and require no `prometheus/client_golang` dependency — `internal/metrics` formats them directly.

## Running Locally

```bash
git clone https://github.com/czhao-dev/reverse-proxy-load-balancer.git
cd reverse-proxy-load-balancer
go test ./...
./scripts/run-demo.sh   # builds and runs 3 demo backends + the proxy on :8080
```

Or via Docker Compose, which builds the same binaries into a container image and wires three backends behind the proxy on a private network:

```bash
docker compose up --build
```

## Benchmark Results

Measured locally on an Apple M3 (`go test -bench`, in-process, isolating proxy overhead from network cost) and with `ab` against the running proxy + 3 demo backends over loopback (round-robin strategy, health checks enabled):

**`go test -bench=. ./internal/proxy/...`**

| | ns/op |
|---|---|
| Single-core | 27,302 ns/op |
| 4 cores (parallel) | 11,644 ns/op |

**`ab -n 20000 -c 100 http://localhost:8080/api/hello`**

| Metric | Result |
|---|---|
| Requests/sec | 16,488 |
| Failed requests | 0 |
| Median latency | 6 ms |
| p95 latency | 8 ms |
| p99 latency | 12 ms |

**`ab -n 50000 -c 200 http://localhost:8080/api/hello`**

| Metric | Result |
|---|---|
| Requests/sec | 13,316 |
| Failed requests | 0 |
| Median latency | 13 ms |
| p99 latency | 35 ms |

End-to-end behavior verified manually alongside the benchmarks: traffic round-robins evenly across all backends; killing a backend process removes it from rotation within one `unhealthy_threshold` window and traffic continues uninterrupted against the remaining backends; restarting it restores it to rotation automatically; and `SIGTERM` drains in-flight requests before the process exits.

## Testing

* **Unit tests** (`internal/.../*_test.go`) cover backend selection for all three strategies, health-state transitions under consecutive thresholds, config parsing/defaults/validation, and metrics rendering.
* **Integration tests** (`tests/integration/`) run the real `proxy.Handler` and `health.Checker` against `httptest` backends — no mocks — and verify traffic distribution, automatic backend removal/recovery, and graceful shutdown end-to-end.
* **Benchmarks** (`internal/proxy/benchmark_test.go`) measure proxy handler overhead in isolation.

Current coverage: `backend` 83%, `balancer` 71%, `config` 90%, `health` 88%, `metrics` 100%, `proxy` 89%.

```bash
go test ./... -race -cover
```

## Failure Handling

| Failure | Behavior |
|---|---|
| Backend connection refused | Retry a different healthy backend (idempotent methods only) |
| Backend exceeds `backend_timeout` | `504 Gateway Timeout`, retried if idempotent |
| No healthy backend available | `503 Service Unavailable` |
| All retries exhausted | `502 Bad Gateway` |
| Health probe fails `unhealthy_threshold` times | Backend marked unhealthy, excluded from selection |
| Health probe succeeds `healthy_threshold` times | Backend restored to rotation |
| `SIGINT`/`SIGTERM` received | Stop accepting new connections, drain in-flight requests, exit |

## Security Considerations

This project focuses on proxy and load-balancing mechanics, not production hardening. Deploying it beyond a demo would additionally need: TLS termination, request size limits, rate limiting, IP allow/deny lists, header sanitization, authentication on `/admin/*`, and slowloris-style abuse protection.

## Non-Goals

This is not a replacement for NGINX, HAProxy, Envoy, or Traefik. It does not implement HTTP/2 or HTTP/3 proxying, service-mesh features, dynamic service discovery, or Kubernetes ingress integration — the goal is to make the core mechanics of a load balancer legible in a small Go codebase, not to compete with mature production systems.

## License

MIT — see [LICENSE](LICENSE).
