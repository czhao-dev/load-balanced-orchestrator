# ml-job-orchestrator

[![Go](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](docker-compose.yml)
[![Prometheus](https://img.shields.io/badge/Prometheus-metrics-E6522C?logo=prometheus&logoColor=white)](prometheus/prometheus.yml)
[![Grafana](https://img.shields.io/badge/Grafana-dashboard-F46800?logo=grafana&logoColor=white)](grafana/dashboard.json)
[![Tests](https://img.shields.io/badge/tests-28%20passing-brightgreen)](tests/)
[![Race detector](https://img.shields.io/badge/go%20test--race-clean-brightgreen)](#testing--verification)

> A distributed job scheduler for ML workloads built in Go — REST API for
> job submission, a goroutine-based worker pool for concurrent execution,
> per-job retry logic with exponential backoff, and a Prometheus metrics
> endpoint, containerized with Docker Compose alongside Prometheus and
> Grafana for full observability.

---

## Overview

ml-job-orchestrator is a purpose-built task scheduler for machine learning
workloads: training runs, inference jobs, data preprocessing, and evaluation
pipelines. Jobs are submitted via a REST API, queued in a buffered channel,
picked up by a goroutine worker pool, executed as subprocesses, and tracked
through a complete lifecycle with automatic retry on failure.

The project demonstrates the core of what Go was designed for: concurrent
systems where many things happen at once and must be coordinated safely.
Every major Go concurrency primitive — goroutines, channels, `sync.WaitGroup`,
mutexes, `context.Context` cancellation — appears naturally in the design
rather than as a forced exercise.

This is the same class of infrastructure that powers production ML platforms:
Argo Workflows, Celery, Ray, and Amazon SageMaker Pipelines all solve
variants of this problem. Building a stripped-down version from scratch
demonstrates you understand the distributed systems concepts those tools
are built on — job queuing, worker lifecycle management, failure recovery,
and observability — which is the knowledge ML infrastructure and backend
platform teams specifically hire for.

**Status:** fully implemented and verified — see
[Testing & Verification](#testing--verification) below.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Clients                                                        │
│  curl / CLI tool (go run ./cmd/mlctl) / any HTTP client         │
└──────────────────────────┬──────────────────────────────────────┘
                           │  REST API (JSON over HTTP)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  API Server (net/http + gorilla/mux)                            │
│  POST /jobs        submit a new job                             │
│  GET  /jobs/{id}   query job status + result                    │
│  GET  /jobs        list all jobs with optional filters          │
│  DELETE /jobs/{id} cancel a job in any non-terminal state       │
│  GET  /healthz     liveness probe                               │
│  GET  /metrics     Prometheus metrics                           │
└──────────────────────────┬──────────────────────────────────────┘
                           │  enqueue
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  Scheduler                                                      │
│  Polls the pending job store every 500ms, applies priority      │
│  ordering, writes ready jobs to the job queue channel            │
└──────────────────────────┬──────────────────────────────────────┘
                           │  chan Job (buffered)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  Worker Pool  (N goroutines, N configured at startup)           │
│  Each worker: select over queue channel + shutdown signal,      │
│  call the Executor; drains in-flight work on graceful shutdown  │
└──────────────────────────┬──────────────────────────────────────┘
                           │  exec.CommandContext
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  Job Executor                                                   │
│  Launches the job command as a subprocess, captures stdout/     │
│  stderr, enforces per-job timeout via context deadline,         │
│  updates job state in the State Store on completion or failure  │
└──────────────────────────┬──────────────────────────────────────┘
                           │  writes
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  State Store  (in-memory sync.Map, Redis-backed stretch goal)   │
│  Holds Job structs keyed by ID, updated by workers,             │
│  read by the API server for status queries                      │
└──────────────────────────┬──────────────────────────────────────┘
                           │  reads
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  Metrics Collector (prometheus/client_golang)                   │
│  jobs_submitted_total, jobs_completed_total, jobs_failed_total, │
│  job_queue_depth, job_duration_seconds histogram                │
└─────────────────────────────────────────────────────────────────┘
```

---

## Job Lifecycle

A job moves through seven states. Every transition is validated against an
explicit `State → []State` table and applied atomically in the state store.

```
                  ┌──────────┐
    POST /jobs    │ PENDING  │  created, not yet queued
                  └────┬─────┘
                       │ scheduler picks up
                       ▼
                  ┌──────────┐
                  │  QUEUED  │  sitting in the buffered channel
                  └────┬─────┘
                       │ worker dequeues
                       ▼
                  ┌──────────┐
                  │ RUNNING  │  subprocess is executing
                  └────┬─────┘
              ┌────────┴────────┐
              │                 │
              ▼                 ▼
        ┌──────────┐      ┌──────────┐
        │COMPLETED │      │  FAILED  │ exit code ≠ 0 or timeout
        └──────────┘      └────┬─────┘
                               │ retries remaining?
                          ┌────┴────┐
                          │         │
                          ▼         ▼
                      ┌───────┐  ┌──────────┐
                      │PENDING│  │EXHAUSTED │ max retries hit
                      └───────┘  └──────────┘

      DELETE /jobs/{id} from any non-terminal state (PENDING/QUEUED/RUNNING)
                               ▼
                         ┌──────────┐
                         │CANCELLED │
                         └──────────┘
```

Cancellation always wins over retry: a job killed via `DELETE` transitions
straight to `CANCELLED` and never re-enters the retry path, regardless of
how many retries it had left. Every state transition increments a
Prometheus counter, making the lifecycle observable in the bundled Grafana
dashboard without any additional instrumentation.

---

## Key Go Concepts Demonstrated

### Goroutine Worker Pool with a Buffered Channel

```go
// internal/worker/pool.go
func New(ctx context.Context, numWorkers int, q chan model.Job, runner JobRunner, shutdownTimeout time.Duration) *Pool {
    runCtx, runCancel := context.WithCancel(ctx)
    p := &Pool{jobQueue: q, stopCh: make(chan struct{}), runCancel: runCancel, shutdownTimeout: shutdownTimeout}

    for i := 0; i < numWorkers; i++ {
        p.wg.Add(1)
        go func() {
            defer p.wg.Done()
            for {
                select {
                case job, ok := <-q:
                    if !ok {
                        return
                    }
                    runner.Run(runCtx, job)
                case <-p.stopCh:
                    return // shutdown signal received
                case <-ctx.Done():
                    return
                }
            }
        }()
    }
    return p
}

func (p *Pool) Shutdown() {
    close(p.stopCh)                              // stop accepting new work
    timer := time.AfterFunc(p.shutdownTimeout, p.runCancel) // force-kill escape hatch
    p.wg.Wait()                                   // let in-flight jobs finish
    timer.Stop()
}
```

The `select` statement is Go's core concurrency primitive — it waits on
multiple channels simultaneously and handles whichever is ready first.
`Shutdown()` lets in-flight (and already-buffered) jobs finish naturally,
but forces cancellation via `shutdownTimeout` if they don't, so a stuck
subprocess can't block shutdown forever. (The real implementation also
drains any jobs still sitting in the channel buffer before honoring the
stop signal — closed channels are always "select-ready", so racing one
directly against the queue channel would let a worker exit while jobs were
still waiting. See the full source for that detail.)

### Context Propagation and Per-Job Timeouts

Every job can specify a `timeout_seconds` field. The executor enforces it
without any manual timer code:

```go
// internal/executor/executor.go
func (e *Executor) Run(parentCtx context.Context, job model.Job) {
    e.store.Transition(job.ID, model.StateRunning, "")

    ctx, cancel := context.WithTimeout(parentCtx, time.Duration(job.TimeoutSeconds)*time.Second)
    e.cancels.Store(job.ID, cancel) // shared with the API's DELETE handler
    defer func() { e.cancels.Delete(job.ID); cancel() }()

    var buf bytes.Buffer
    cmd := exec.CommandContext(ctx, job.Command, job.Args...)
    cmd.Stdout, cmd.Stderr = &buf, &buf
    runErr := cmd.Run()

    switch {
    case ctx.Err() == context.Canceled:
        e.store.Transition(job.ID, model.StateCancelled, "cancelled by user")
    case ctx.Err() == context.DeadlineExceeded:
        e.failOrRetry(job, "timeout")
    case runErr != nil:
        e.failOrRetry(job, runErr.Error())
    default:
        e.store.Transition(job.ID, model.StateCompleted, "")
    }
}
```

`exec.CommandContext` automatically sends `SIGKILL` to the subprocess when
the context is cancelled or its deadline is exceeded — no manual signal
handling required. The same per-job `context.CancelFunc` that backs the
timeout is also stored in a map keyed by job ID, shared with the API's
`DELETE /jobs/{id}` handler — cancelling a *running* job and a job that
times out go through the exact same code path.

### Retry with Exponential Backoff

```go
// internal/retry/retry.go
func ScheduleRetry(job model.Job) model.Job {
    job.RetryCount++
    backoff := time.Duration(math.Pow(2, float64(job.RetryCount))) * time.Second
    if backoff > maxBackoff { // capped at 60s
        backoff = maxBackoff
    }
    job.RunAfter = time.Now().Add(backoff)
    job.State = model.StatePending
    job.StartedAt, job.FinishedAt = nil, nil
    return job
}
```

The scheduler goroutine polls for pending jobs whose `RunAfter` timestamp
has passed, so the retry delay requires no sleeping goroutine per job.
Cancellation is checked *before* this is ever called — a cancelled job is
never retried, no matter how much retry budget remains.

### Prometheus Metrics

```go
// internal/metrics/metrics.go
var (
    JobsSubmitted = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "mlorch_jobs_submitted_total",
        Help: "Total jobs submitted, by type",
    }, []string{"job_type"})

    JobsDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
        Name:    "mlorch_job_duration_seconds",
        Help:    "Job execution duration in seconds",
        Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600},
    }, []string{"job_type", "state"})

    QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "mlorch_queue_depth",
        Help: "Number of jobs currently queued",
    })
)
```

`promauto` registers metrics automatically — no explicit `Register` call
needed. The histogram buckets for `JobsDuration` are sized for ML jobs
(seconds to tens of minutes) rather than web request latency.

---

## API Reference

### Submit a job
```
POST /jobs
Content-Type: application/json

{
    "name": "train-resnet50",
    "type": "training",
    "command": "python3",
    "args": ["train.py", "--epochs", "10", "--lr", "0.001"],
    "timeout_seconds": 3600,
    "max_retries": 2,
    "priority": 1
}

→ 201 Created
Location: /jobs/job_7f3a2c
{
    "id": "job_7f3a2c",
    "state": "PENDING",
    "created_at": "2026-06-18T10:00:00Z"
}
```
`name` and `command` are required; a missing job ID lookup or malformed
body returns `400`/`404` with a JSON `{"error": "..."}` body.

### Query job status
```
GET /jobs/job_7f3a2c

→ 200 OK
{
    "id": "job_7f3a2c",
    "name": "train-resnet50",
    "type": "training",
    "command": "python3",
    "args": ["train.py", "--epochs", "10", "--lr", "0.001"],
    "state": "COMPLETED",
    "priority": 1,
    "max_retries": 2,
    "retry_count": 0,
    "timeout_seconds": 3600,
    "created_at": "2026-06-18T10:00:00Z",
    "started_at": "2026-06-18T10:00:03Z",
    "finished_at": "2026-06-18T10:47:22Z",
    "output": "Epoch 10/10 — loss: 0.0312, acc: 0.9891\nModel saved."
}
```

### List jobs with filter
```
GET /jobs?state=FAILED&type=training&limit=20

→ 200 OK
{ "jobs": [...], "total": 3 }
```

### Cancel a job
```
DELETE /jobs/job_7f3a2c

→ 200 OK
{ "id": "job_7f3a2c", "state": "CANCELLED" }

# Cancelling a job already in a terminal state is a conflict, not a no-op:
→ 409 Conflict
{ "error": "job already in terminal state COMPLETED" }
```

---

## Example Session

Captured from a real run of the orchestrator (`docker compose up` +
`curl`/`mlctl`) — see [Testing & Verification](#testing--verification) for
the full verification process this was drawn from.

```bash
# Submit a job
$ curl -s -X POST http://localhost:8080/jobs \
    -H "Content-Type: application/json" \
    -d '{"name":"train-mnist","type":"training","command":"echo",
         "args":["hello world"],"timeout_seconds":10,"max_retries":1}'
{"created_at":"2026-06-18T20:20:10.775248-07:00","id":"job_c0f5c6","state":"PENDING"}

# Poll until complete
$ curl -s http://localhost:8080/jobs/job_c0f5c6
{"id":"job_c0f5c6","name":"train-mnist","type":"training","command":"echo",
 "args":["hello world"],"state":"COMPLETED","priority":0,"max_retries":1,
 "retry_count":0,"timeout_seconds":10,
 "created_at":"2026-06-18T20:20:10.775248-07:00",
 "started_at":"2026-06-18T20:20:10.961636-07:00",
 "finished_at":"2026-06-18T20:20:10.965622-07:00","output":"hello world\n"}

# Cancel a long-running job mid-execution — the subprocess is actually killed,
# not just marked cancelled (sleep 20 never reaches completion):
$ curl -s -X POST http://localhost:8080/jobs -d '{"name":"sleeper2","command":"sleep","args":["20"]}'
{"id":"job_a82cde","state":"PENDING", ...}
$ curl -s -X DELETE http://localhost:8080/jobs/job_a82cde
{"id":"job_a82cde","state":"CANCELLED"}
$ curl -s http://localhost:8080/jobs/job_a82cde | python3 -c \
    "import sys,json; j=json.load(sys.stdin); print(j['finished_at'], j['error_message'])"
2026-06-18T20:20:49.346976-07:00 cancelled by user   # killed after ~1.2s, not the full 20s

# Check metrics
$ curl -s http://localhost:8080/metrics | grep mlorch_jobs
mlorch_jobs_submitted_total{job_type="training"} 1
mlorch_jobs_completed_total{job_type="training"} 1

# Use the CLI tool
$ go run ./cmd/mlctl submit --name train-mnist --command python3 \
    --args "train.py --epochs 5" --timeout 600 --retries 3
Submitted job_ef4488

# train.py doesn't exist, so this exercises the full retry-then-exhaustion path:
$ go run ./cmd/mlctl status job_ef4488
ID:       job_ef4488
Name:     train-mnist
State:    EXHAUSTED
Duration: 45ms
Retries:  3
Error:    exit status 2
```

---

## Repo Structure

```
ml-job-orchestrator/
├── README.md
├── LICENSE
├── go.mod
├── go.sum
├── Dockerfile
├── docker-compose.yml          ← runs orchestrator + Prometheus + Grafana
├── cmd/
│   ├── orchestrator/
│   │   └── main.go             ← wires everything together, starts server
│   └── mlctl/
│       └── main.go             ← CLI client: submit, status, list, cancel
├── internal/
│   ├── api/
│   │   ├── server.go           ← router, HTTP server lifecycle
│   │   ├── handlers.go         ← one handler per endpoint
│   │   └── middleware.go       ← request logging + panic recovery
│   ├── model/
│   │   └── job.go              ← Job struct, State enum, transition rules
│   ├── scheduler/
│   │   └── scheduler.go        ← polls state store, writes to queue channel
│   ├── worker/
│   │   └── pool.go             ← goroutine pool, select loop, shutdown
│   ├── executor/
│   │   └── executor.go         ← exec.CommandContext, timeout, cancel, retry
│   ├── store/
│   │   └── store.go            ← sync.Map-based state store, thread-safe ops
│   ├── metrics/
│   │   └── metrics.go          ← Prometheus counter/histogram/gauge defs
│   └── retry/
│       └── retry.go            ← backoff calculation, reschedule logic
├── config/
│   └── config.go               ← env-var based config (port, workers, etc.)
├── prometheus/
│   └── prometheus.yml          ← scrape config for Docker Compose setup
├── grafana/
│   ├── dashboard.json          ← pre-built dashboard for job metrics
│   └── provisioning/
│       ├── datasources/datasource.yml  ← auto-registers the Prometheus datasource
│       └── dashboards/dashboard.yml    ← auto-loads dashboard.json on startup
├── docs/
│   └── design.md               ← three core design tradeoffs, explained
└── tests/
    ├── store_test.go           ← state transition + concurrent store tests
    ├── pool_test.go            ← worker pool concurrency + shutdown tests
    ├── executor_test.go        ← timeout, cancel-vs-retry, retry exhaustion
    ├── scheduler_test.go       ← dispatch timing, priority, backpressure
    ├── api_test.go             ← HTTP handler tests with httptest
    └── integration_test.go     ← submit → run → complete end-to-end
```

---

## Build & Run

```bash
# Dependencies: Go 1.24+ (developed with 1.26), Docker, Docker Compose

# Run locally (single node, no Docker)
go run ./cmd/orchestrator --workers 4 --port 8080

# Run with Docker Compose (orchestrator + Prometheus + Grafana)
docker compose up --build

# Services:
# http://localhost:8080  — orchestrator API
# http://localhost:9090  — Prometheus
# http://localhost:3000  — Grafana (admin/admin), dashboard auto-loaded

# Submit a job using the CLI
go run ./cmd/mlctl submit \
    --name "train-resnet" \
    --command python3 \
    --args "train.py --epochs 5" \
    --timeout 600 \
    --retries 3

# Run tests
go test ./...

# Run tests with race detector (detects concurrency bugs)
go test -race ./...

# Check code coverage (tests live in their own package, so -coverpkg is needed
# to attribute coverage back to the internal packages they exercise)
go test -coverprofile=coverage.out -coverpkg=./... ./tests/...
go tool cover -html=coverage.out
```

---

## Testing & Verification

This isn't just "it compiles" — every layer was exercised directly, not
just unit-tested in isolation.

**Automated test suite — 28 tests, all passing under `go test -race`:**

| Package | Coverage focus |
|---|---|
| `internal/model` | every valid/invalid state transition (table-driven) |
| `internal/store` | CRUD + transitions, concurrent access from 20 goroutines × 25 jobs, plus a dedicated test hammering a single shared job ID concurrently to confirm exactly one writer wins |
| `internal/worker` | 8 workers / 100 jobs from 10 concurrent goroutines; graceful shutdown draining 10 in-flight jobs; force-kill via `shutdownTimeout` when a job won't finish |
| `internal/executor` | timeout enforcement (`sleep 10` + 2s timeout → terminal within 3s), cancellation short-circuiting retry, retry-then-exhaustion off-by-one correctness |
| `internal/scheduler` | dispatch within the 500ms poll interval, `RunAfter` gating, priority ordering, non-blocking backpressure when the queue is full |
| `internal/api` | every handler via `httptest`, including DELETE-while-RUNNING actually terminating the subprocess |
| `tests/integration_test.go` | full stack over real HTTP — 5 concurrent jobs (success, failure, timeout, retry-to-exhaustion, mid-flight cancellation) all reaching the correct terminal state |

```
$ go test -race ./...
ok      github.com/czhao-dev/ML-Job-Orchestrator/tests   15.497s

$ go test -coverprofile=coverage.out -coverpkg=./... ./tests/...
ok      github.com/czhao-dev/ML-Job-Orchestrator/tests   coverage: 91.3% of statements in ./...
```

**A real concurrency bug was found and fixed via the integration test.**
The scheduler originally sent a job onto the queue channel *before*
writing its `QUEUED` transition to the store. An idle worker could dequeue
and call the executor before that store write landed, see the job still
`PENDING`, fail `QUEUED → RUNNING` validation, and silently skip it —
stranding the job once the scheduler's now-redundant write finally landed.
Every package's own unit tests passed; it took a full end-to-end test
under `-race`, run repeatedly, to surface it. The fix and full writeup are
in [`docs/design.md`](docs/design.md).

**Manual verification beyond the automated suite:**
- Full `docker compose up --build` stack: orchestrator `/healthz`,
  Prometheus `/-/healthy` with the orchestrator target reporting `up`,
  and Grafana `/api/health` with the Prometheus datasource and dashboard
  both auto-provisioned (no manual UI clicks) — confirmed by querying
  Prometheus directly for the same series each dashboard panel uses
  (submit rate, queue depth, success ratio, duration histogram) and
  getting live data back after submitting jobs.
- `mlctl` CLI exercised end-to-end against a running orchestrator:
  submit → status → list → cancel, including watching a job progress
  through retry → exhaustion in real time.
- `DELETE /jobs/{id}` against a `RUNNING` job confirmed to actually kill
  the OS subprocess (a 20s `sleep` terminates in ~1.2s, not 20s) rather
  than just flipping a state flag.

---

## Step-by-Step Build Guide

This is the plan the project was actually built from, kept here as a
record of the approach (and because the phase breakdown is useful if
you're building something similar). Every phase below is complete; see
[Testing & Verification](#testing--verification) for how each one was
checked off.

### Phase 1 — Core Data Structures

**Task 1.1 — Define the Job model**
In `internal/model/job.go`, define the `Job` struct and `State` enum:

```go
type State string

const (
    StatePending   State = "PENDING"
    StateQueued    State = "QUEUED"
    StateRunning   State = "RUNNING"
    StateCompleted State = "COMPLETED"
    StateFailed    State = "FAILED"
    StateExhausted State = "EXHAUSTED"
    StateCancelled State = "CANCELLED"
)

type Job struct {
    ID             string
    Name           string
    Type           string
    Command        string
    Args           []string
    State          State
    Priority       int
    MaxRetries     int
    RetryCount     int
    TimeoutSeconds int
    RunAfter       time.Time
    CreatedAt      time.Time
    StartedAt      *time.Time
    FinishedAt     *time.Time
    Output         string
    ErrorMessage   string
}
```

Implement a `Transition(from, to State) bool` function that validates
allowed state transitions using a map of `State → []State`. Invalid
transitions return false and leave the job state unchanged. Write a
unit test covering every valid and invalid transition. Getting state
transitions right before building anything that modifies them prevents a
class of concurrency bugs later.

**Task 1.2 — Implement the state store**
In `internal/store/store.go`, implement a `Store` backed by a `sync.Map`:

- `Create(job Job) error` — store a new job, error if ID already exists
- `Get(id string) (Job, error)` — return a copy of the job
- `Update(job Job) error` — overwrite the stored job atomically
- `Transition(id string, to State, errMsg string) error` — get the job,
  call `Transition`, update if valid, error if invalid
- `ListByState(state State) []Job` / `List(filter) ([]Job, int)` — query helpers
- `Delete(id string)` — remove a job

`sync.Map` alone doesn't give compare-and-swap across multiple fields of
one value, so `Transition` needs its own coarse mutex around the
read-modify-write — see `docs/design.md` for why. Write tests for each
method including concurrent access from many goroutines, and run with
`go test -race` to confirm no data races.

**Task 1.3 — Add configuration**
In `config/config.go`, define a `Config` struct read from environment
variables with sensible defaults (`Port`, `NumWorkers`, `QueueSize`,
`MaxJobHistory`, `ShutdownTimeout`). Environment-variable-based config is
the twelve-factor app standard and makes the Docker Compose setup
straightforward.

---

### Phase 2 — Worker Pool

**Task 2.1 — Implement the worker pool**
In `internal/worker/pool.go`, implement the goroutine pool as shown in
the Key Concepts section. The pool depends on a small `JobRunner`
interface rather than importing the executor package directly, so the
two packages don't import each other. The `jobQueue` parameter is a
`chan model.Job` created by the caller — the pool is a consumer only, it
never writes to the channel.

**Task 2.2 — Implement graceful shutdown**
`Shutdown()` should let in-flight (and already-queued) jobs finish
naturally, only forcing cancellation if they exceed a configurable
`shutdownTimeout`. Add a test that submits 10 slow jobs to a 2-worker
pool, calls `Shutdown()`, and asserts all 10 eventually reach a terminal
state with none left `RUNNING` — plus a second test confirming a job that
genuinely won't finish gets force-cancelled rather than hanging shutdown
forever.

**Task 2.3 — Test with the race detector**
Launch a pool with 8 workers and submit 100 jobs concurrently from 10
goroutines. Run with `go test -race ./internal/worker/...`. A clean run
confirms the pool has no data races — and it's worth re-running several
times, not just once (see the bug callout in
[Testing & Verification](#testing--verification)).

---

### Phase 3 — Job Executor

**Task 3.1 — Implement the executor**
In `internal/executor/executor.go`, capture both stdout and stderr into a
*fresh* `bytes.Buffer` per invocation (a shared/package-level buffer would
corrupt concurrent jobs' output), and use `exec.CommandContext` so the
process is automatically killed when the context is cancelled or its
deadline expires.

**Task 3.2 — Implement retry scheduling**
In `internal/retry/retry.go`, implement `ScheduleRetry(job Job) Job`:
increment `RetryCount`, compute backoff (`2^RetryCount` seconds, capped at
60), set `RunAfter`, reset state to `PENDING`. The caller checks
`RetryCount < MaxRetries` before calling this; if the budget is exhausted,
transition to `EXHAUSTED` instead — and a cancelled job must skip this
path entirely, regardless of remaining budget.

**Task 3.3 — Test timeout enforcement**
Submit a job running `sleep 10` with a two-second timeout and assert it
reaches a terminal state within three seconds with an error mentioning
"timeout". This is the most direct proof that `context.WithTimeout` and
`exec.CommandContext` are wired together correctly.

---

### Phase 4 — Scheduler

**Task 4.1 — Implement the scheduler loop**
In `internal/scheduler/scheduler.go`, poll the state store every 500ms for
`PENDING` jobs whose `RunAfter` has passed, sort by `(Priority desc,
CreatedAt asc)`, and dispatch. The 500ms interval bounds dispatch latency
to 500ms in the worst case; a production scheduler would use a heap and a
condition variable instead (see `docs/design.md`).

**Task 4.2 — Handle queue backpressure, and get the ordering right**
Transition a job to `QUEUED` in the store *before* it becomes visible on
the channel — not after, as sending-then-updating leaves a race window
where a worker can dequeue and try to run the job before the store
reflects `QUEUED`. If the channel send then fails because the queue is
full, revert the job back to `PENDING` and retry next tick; never block on
the send, since that would deadlock the scheduler goroutine.

---

### Phase 5 — REST API Server

**Task 5.1 — Implement the HTTP handlers**
One handler per endpoint in `internal/api/handlers.go`. Generate job IDs
with `crypto/rand`; set the `Location` header on `POST /jobs`; return
`404` with a JSON error body when a job isn't found, and `409` if a
`DELETE` targets a job already in a terminal state.

**Task 5.2 — Implement job cancellation**
For a `RUNNING` job, look up its `context.CancelFunc` in a map shared with
the executor and call it — then let the *executor* write the terminal
`CANCELLED` state once the subprocess actually dies, rather than writing
it from the handler too. Two writers racing to set a job's terminal state
is exactly the kind of bug this design avoids by giving each transition
exactly one owner. For `QUEUED`/`PENDING` jobs there's no subprocess yet,
so transition straight to `CANCELLED`.

**Task 5.3 — Add middleware**
A request logger (`log/slog`, fields: method/path/status/duration) and a
panic-recovery middleware that logs the stack trace and returns `500`
instead of crashing the server. Wrap every route with both.

**Task 5.4 — Write handler tests**
Use `net/http/httptest` to test each handler without starting a real
server, including a DELETE-while-RUNNING test that confirms the
subprocess is actually terminated, not just marked cancelled.

---

### Phase 6 — Metrics & Observability

**Task 6.1 — Instrument the critical paths**
`JobsSubmitted` on submit, `JobsCompleted`/`JobsFailed` on terminal state,
`QueueDepth` after each scheduler tick, `JobsDuration` observed from
`StartedAt` to completion.

**Task 6.2 — Docker Compose with Prometheus and Grafana**
Three services: the orchestrator, Prometheus (scraping `/metrics` every
5s), and Grafana. Getting Grafana to auto-load the dashboard with zero
manual clicks needs two provisioning files beyond just the dashboard JSON
itself — a datasource provisioner pointing at Prometheus, and a dashboard
provisioner pointing at the dashboard file — both mounted as volumes.

**Task 6.3 — Build the Grafana dashboard**
Four panels: a rate graph for `mlorch_jobs_submitted_total`, a gauge for
`mlorch_queue_depth`, a success/failure ratio stat, and a duration
heatmap. Exported to `grafana/dashboard.json`.

---

### Phase 7 — CLI Client & Integration Tests

**Task 7.1 — Build the mlctl CLI**
Stdlib `flag` package only, no external dependency. Subcommands: `submit`,
`status`, `list`, `cancel`, all making HTTP requests to the orchestrator.

**Task 7.2 — Write an end-to-end integration test**
Start the orchestrator in-process (an `httptest.Server` wrapping the real
router, backed by the real store/scheduler/pool/executor), submit several
jobs covering success, failure, timeout, retry-to-exhaustion, and
mid-flight cancellation, and assert every one reaches the correct terminal
state. This is the most valuable test in the suite — it's also the one
that caught the real scheduler race described above.

---

### Phase 8 — Polish

**Task 8.1 — Structured logging throughout**
`log/slog` everywhere instead of `fmt.Println`, with a `--log-level` flag.

**Task 8.2 — Multi-node stretch goal (not implemented)**
A natural next step: multiple orchestrator instances sharing a
Redis-backed queue instead of an in-memory channel, with a distributed
lock guaranteeing each job runs exactly once across the cluster. This is
the step that would turn this into a genuinely distributed system —
intentionally left as documented future work; see `docs/design.md` for
the tradeoff this implies today (single point of failure, no durability
across restarts).

**Task 8.3 — Document the design decisions**
`docs/design.md` covers three explicit choices: `sync.Map` vs. `map` +
`sync.RWMutex` for the state store, polling vs. a condition variable in
the scheduler, and an in-memory channel vs. Redis for the job queue — plus
a writeup of the real concurrency bug the integration test caught.

---

## How to Talk About This Project in an Interview

**What is the project?**
"I built a distributed job scheduler in Go designed for ML workloads —
training runs, inference jobs, preprocessing pipelines. Jobs are submitted
via a REST API, queued in a buffered channel, processed by a goroutine
worker pool, retried on failure with exponential backoff, and tracked
through a complete lifecycle. The whole system is observable via Prometheus
metrics and runs as a multi-service stack with Docker Compose."

**Walk me through the concurrency model.**
"The worker pool is a fixed number of goroutines, each running a `select`
loop over the job queue channel and a shutdown signal. When a job arrives,
the worker calls the executor and blocks until it's done — one job per
worker at a time. When shutdown is triggered, in-flight and already-queued
work is allowed to finish naturally, with a timeout that force-cancels
anything still running past a configurable grace period. A
`sync.WaitGroup` blocks `Shutdown()` until every worker has returned."

**How does job cancellation work for a running job?**
"The API's `DELETE /jobs/{id}` handler looks up a per-job
`context.CancelFunc` stored in a concurrent map — the same one the
executor uses to enforce timeouts — and calls it. Go's
`exec.CommandContext` sends `SIGKILL` to the subprocess automatically when
that context is cancelled. The executor's own goroutine is the only thing
that writes the resulting `CANCELLED` state; the handler just triggers the
cancellation and polls briefly for it to land, rather than writing the
terminal state itself, to avoid two goroutines racing to finalize the same
job."

**Tell me about a real bug you found.**
"While writing the end-to-end integration test, I found a genuine race in
the scheduler: it sent a job onto the queue channel before writing its
`QUEUED` state to the store, mirroring how I'd first sketched the logic.
An idle worker could dequeue and try to run the job before that store
write landed, see it still `PENDING`, fail the `QUEUED → RUNNING`
validation, and silently skip it — stranding the job. Every package's own
unit tests passed because each one only exercised its own state machine in
isolation; it took a full stack test under `-race`, run repeatedly to
catch the timing window, to surface it. The fix was to write the `QUEUED`
transition before the job becomes visible on the channel, reverting to
`PENDING` if the subsequent send fails because the queue is full."

**What would you do differently at production scale?**
"Three things. The in-memory state store would be replaced with Redis or
PostgreSQL for durability — right now a restart loses all job state. The
polling scheduler would be replaced with a heap-based priority queue and
a condition variable to avoid the 500ms dispatch latency. And the single
node would become a cluster, where multiple orchestrator instances pull
from a shared Redis queue with a distributed lock to guarantee each job
runs exactly once."

---

## Further Reading

- [The Go Programming Language — Donovan & Kernighan](https://www.gopl.io/)
  — Chapters 8 and 9 cover goroutines, channels, and concurrency in depth;
  the worker pool pattern in this project is a direct application of those
  chapters
- [Go Concurrency Patterns — Rob Pike (talk)](https://go.dev/talks/2012/concurrency.slide)
  — the canonical presentation of the select, fan-out, and pipeline patterns
- [prometheus/client_golang](https://github.com/prometheus/client_golang)
  — the official Go Prometheus client used in this project
- [The Twelve-Factor App](https://12factor.net/)
  — the methodology behind the environment-variable configuration and
  stateless worker design in this project
- [Argo Workflows](https://argoproj.github.io/argo-workflows/)
  — the production ML job orchestrator this project is a simplified version
  of; comparing designs is worth a section in your README

## License

[MIT](LICENSE)
