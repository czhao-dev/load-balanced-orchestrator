# Backend Infrastructure Platform

A declarative infrastructure control plane that accepts Deployment specs, schedules Pods onto registered Nodes, tracks Node health via heartbeats, reconciles desired vs. actual state, recovers from Node failures, and drives a health-aware reverse proxy — with Prometheus/Grafana observability across the full stack.

```
                        infractl / curl
                              │
                              ▼
                     ┌─────────────────┐
                     │  control-plane  │  deployments, pods, nodes, services
                     │      :7070      │  scheduler + reconciler loops
                     └────────┬────────┘
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
         worker-1         worker-2         worker-3   (register, heartbeat,
        (:9100 metrics)  (:9101 metrics)  (:9102 metrics)  poll, execute)
              ▲               ▲               ▲
              └───────────────┼───────────────┘
                     ┌─────────────────┐
       client  ───▶  │  dynamic-proxy  │  fetches healthy nodes from the
                     │      :8081      │  control plane, routes/fails over
                     └─────────────────┘
                              │
                     ┌────────┴────────┐
                     ▼                 ▼
               Prometheus :9090   Grafana :3000
```

This control-plane/worker stack runs **alongside** — not instead of — the repo's original demo: a static reverse-proxy/load-balancer (`:8080`) fronting 3 standalone `ml-job-orchestrator` replicas. The two demonstrate different execution models within the same repo:

- [`control-plane/`](control-plane/) — the control plane: declarative Deployments, Node registration/heartbeats, FIFO + resource-aware scheduling, desired-state reconciliation, failure detection and rescheduling, dead-lettering, label-based Service discovery, and a backend-discovery API the proxy can poll. Pods execute as OS subprocesses or Docker containers (set `image:` in the Deployment spec). CLI: `infractl`.
- [`reverse-proxy-load-balancer/`](reverse-proxy-load-balancer/README.md) — reverse proxy / load balancer: round-robin/least-conn/weighted strategies, active health checking, retry/failover. Runs **twice** in this repo: once with a static backend list (`proxy`, `:8080`, fronting the orchestrators) and once with dynamic control-plane discovery (`dynamic-proxy`, `:8081`, fronting the node fleet).
- [`ml-job-orchestrator/`](ml-job-orchestrator/README.md) — the original standalone REST API job scheduler (worker pool, subprocess executor, retry/backoff, in-memory state store). Still fully independent; see its own README.

See each project's own README/docs for implementation details — this README covers how the pieces fit together.

## Quickstart: control plane

```bash
./control-plane/scripts/run-local-cluster.sh
```

Builds and starts the control plane, 3 node agents, the dynamic-discovery proxy, Prometheus, and Grafana. This is a focused subset — it does not also start the orchestrator/proxy stack below.

- **Control plane API**: http://localhost:7070
- **Dynamic proxy**: http://localhost:8081 (backend status: `/admin/backends`)
- **Prometheus**: http://localhost:9090
- **Grafana** (admin/admin): http://localhost:3000

Then walk through the demo scripts in [`control-plane/scripts/`](control-plane/scripts/):

```bash
./control-plane/scripts/submit-demo-jobs.sh      # submit a 20-pod deployment, watch it schedule across nodes
./control-plane/scripts/demo-worker-failure.sh   # kill a node mid-pod, watch the reconciler detect and reschedule
./control-plane/scripts/demo-proxy-failover.sh   # kill a node, watch the proxy route around it and recover
./control-plane/scripts/benchmark-scheduler.sh   # throughput: deployments submitted to all pods scheduled
```

Or drive it directly with `infractl` (`cd control-plane && go run ./cmd/infractl ...`; `INFRACTL_SERVER` defaults to `http://localhost:7070`):

```bash
infractl deployment submit examples/batch-job.yaml
infractl deployment status <deployment-id>
infractl node list
infractl cluster status
```

**Known limitation:** a node that crashes and never comes back stays `UNHEALTHY` in the control plane's store forever — the heartbeat timeout only ever flips `HEALTHY → UNHEALTHY`; there's no automatic garbage-collection of permanently-dead nodes. Its pods are correctly rescheduled onto the remaining healthy nodes, so this is a bookkeeping gap, not a scheduling one. A `drain`-then-timeout path does fully remove a node (see `internal/reconciler`).

## Quickstart: original orchestrator/proxy stack

```bash
docker compose up --build proxy orchestrator-1 orchestrator-2 orchestrator-3 prometheus grafana
```

Builds and starts 3 orchestrator replicas, the static-config proxy, Prometheus, and Grafana. Allow ~5–10 seconds after startup for the proxy's active health checker to mark all 3 replicas healthy before traffic is fully balanced.

- **Proxy (entry point)**: http://localhost:8080 (backend status: `/admin/backends`)

Submit a job through the proxy:

```bash
curl -X POST localhost:8080/jobs -d '{"type":"training","command":"sleep 2"}'
```

**Known limitation: per-replica state.** Each orchestrator replica keeps an independent in-memory job store with no shared backing store across replicas. Because the proxy load-balances round-robin, a job submitted via `POST /jobs` might land on `orchestrator-2`, but a later `GET /jobs/{id}` or `DELETE /jobs/{id}` can be routed to `orchestrator-1` or `orchestrator-3` and return `404`. Fixing this properly requires either sticky routing by job ID or a shared backing store — both reasonable extensions, out of scope here.

`ml-job-orchestrator`'s CLI (`mlctl`) defaults to `MLCTL_SERVER=http://localhost:8080`, pointing at this proxy.

You can run `docker compose up --build` with no service names to start *everything* (both stacks) at once — Prometheus and Grafana scrape both regardless of which subset is actually running, tolerating absent targets.

## Local Go development

Three independent Go modules (`control-plane/`, `ml-job-orchestrator/`, `reverse-proxy-load-balancer/`) are tied together by a root [`go.work`](go.work) so editors and tools can resolve imports across all three without juggling module roots. `go.work` is a dev convenience only — none of the modules has a `replace` directive on another, so each builds, tests, and vets as a fully standalone module.

Note: `go build ./...` does **not** work from the repo root (there is no module at the root, only the workspace file). Run it per-module, or pass explicit paths:

```bash
go build ./control-plane/... ./ml-job-orchestrator/... ./reverse-proxy-load-balancer/...
go vet   ./control-plane/... ./ml-job-orchestrator/... ./reverse-proxy-load-balancer/...
go test  ./control-plane/... ./ml-job-orchestrator/... ./reverse-proxy-load-balancer/... -race
```

or `cd` into any one module and run `go build ./... && go test ./...` there directly.

## Standalone use

Each subdirectory is a fully independent project with its own `go.mod`, `Dockerfile`, and (for the two original modules) `docker-compose.yml`/`LICENSE`:

```bash
cd ml-job-orchestrator && docker compose up --build           # orchestrator + prometheus + grafana alone
cd reverse-proxy-load-balancer && docker compose up --build   # proxy + its own demo backends alone
cd control-plane && go build ./... && go test ./...           # control plane builds/tests without a running Docker daemon
```

`control-plane/` does not ship its own `docker-compose.yml` — its demo stack (workers, dynamic proxy, shared Prometheus/Grafana) is inherently multi-service and lives in the root [`docker-compose.yml`](docker-compose.yml) instead.

## References

- [Go Workspaces](https://go.dev/doc/workspaces) — the `go.work` mechanism used to tie the three modules together for local development
- [gorilla/mux](https://github.com/gorilla/mux) — HTTP router used by `ml-job-orchestrator`
- [Prometheus Go client](https://github.com/prometheus/client_golang) — instrumentation library used by all three modules
- [Prometheus](https://prometheus.io/docs/introduction/overview/) — metrics collection and alerting
- [Grafana](https://grafana.com/docs/grafana/latest/) — metrics visualization and dashboards
- [Docker Compose](https://docs.docker.com/compose/) — local multi-service orchestration for the demo stacks
- Controller / reconcile-loop pattern — the desired-vs-actual-state design that inspired `internal/reconciler`
- [go.etcd.io/bbolt](https://pkg.go.dev/go.etcd.io/bbolt) — embedded single-file ACID key-value store used as the persistent state backend in `control-plane/`
- [gopkg.in/yaml.v3](https://pkg.go.dev/gopkg.in/yaml.v3) — YAML parsing used for declarative deployment spec files
- [testify](https://github.com/stretchr/testify) — assertion and mock library used across all three modules
- [Docker Go SDK](https://pkg.go.dev/github.com/docker/docker/client) — official Docker client used by the node agent to pull images and manage container lifecycle when `image:` is set in a Deployment spec
