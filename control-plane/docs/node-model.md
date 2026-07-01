# Node model

There are two distinct things named "node" in this codebase — worth disambiguating up front:

- `internal/model.Node` — the control plane's **server-side record** of a registered node: ID, address, capacity, status, last heartbeat, labels. Pure data, lives in the state store.
- `internal/worker.Agent` — the **client-side process** (`cmd/worker`) that registers as a Node, heartbeats, polls for Pods, and executes them. This doc is about the agent.

## Lifecycle

```
start node agent
  -> register with control plane (capacity, address) -> gets a Node ID
  -> heartbeat loop: POST /api/v1/nodes/{id}/heartbeat every 5s (WORKER_HEARTBEAT_INTERVAL)
  -> poll loop: GET /api/v1/nodes/{id}/pods/poll every 1s (WORKER_POLL_INTERVAL)
       -> on a pod: acquire a concurrency-semaphore slot, run it as a subprocess
          or Docker container (if pod.Image is set), report RUNNING then
          SUCCEEDED/FAILED/CANCELLED via POST .../status
  -> on SIGTERM: stop polling immediately, let in-flight pods finish
       (up to WORKER_SHUTDOWN_TIMEOUT, default 10s), then cancel stragglers
```

Node identity is **not persisted** across restarts — a node agent that is killed and restarted registers fresh and gets a brand-new ID. The control plane never reconciles "this is actually the same physical node as before"; it is simply a new entry in the store, and the old ID stays `UNHEALTHY` forever (see the reconciler's known gap).

## Pod execution

`internal/worker/executor.go` dispatches each pod to one of two execution paths based on whether the pod carries an `image` field:

- **Subprocess** (`image` is empty): `exec.CommandContext` runs `pod.Command pod.Args...` as an OS subprocess. stdout/stderr are captured into a `bytes.Buffer`; the exit code is extracted via `*exec.ExitError`.
- **Docker container** (`image` is set): `internal/worker/container.go` uses the official Docker Go SDK (`github.com/docker/docker/client`) to pull the image, create and start a container, wait for it to exit, collect its logs via `stdcopy.StdCopy`, and then remove the container. CPU and memory limits from the pod's `resources` field are mapped to Docker `HostConfig.NanoCPUs` and `HostConfig.Memory`. The worker agent's Docker daemon socket is configured via `WORKER_DOCKER_HOST` (default: `DOCKER_HOST` env var or Docker's default socket).

In both paths the status sequence is identical: `RUNNING` on start, then `SUCCEEDED`, `FAILED`, or `CANCELLED` on finish.

## Graceful shutdown

The agent's pod-execution context (`runCtx` in `internal/worker/agent.go`) is **not** derived from the process's cancellation context — if it were, an in-flight pod would be killed the instant `SIGTERM` arrives, defeating the point of draining. Instead, `runCtx` is only cancelled by the shutdown-timeout escape hatch. This was a real bug caught by `TestAgent_GracefulShutdownDrainsInFlightPod` during development, not a hypothetical.

## Concurrency

A buffered channel (`chan struct{}`, capacity `WORKER_MAX_CONCURRENT_JOBS`) gates how many pods run at once — simpler than a full goroutine pool since each node agent self-throttles via polling rather than consuming from a shared dispatch channel.

## Metrics and HTTP listener

The node agent isn't a pod-serving HTTP service — pods run as subprocesses, not as requests it handles. Its only HTTP listener (`WORKER_METRICS_PORT`, default 9100) exists purely for `/healthz`, `/readyz`, and `/metrics` — for Prometheus scraping, container healthchecks, and (in the proxy-failover demo) as the address the dynamic-discovery proxy forwards traffic to. Node agent metrics (`worker_running_jobs`, `worker_completed_jobs_total`, etc.) live in `internal/agentmetrics`, a package separate from the control plane's `internal/metrics` — see that package's doc comment for why splitting it mattered (otherwise every node's `/metrics` would also expose always-zero control-plane metrics, and vice versa, since both binaries would be importing the same package).
