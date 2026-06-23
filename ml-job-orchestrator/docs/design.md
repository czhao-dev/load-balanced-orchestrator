# Design Decisions

This document explains three deliberate tradeoffs in ml-job-orchestrator's
design, and what production would do differently.

## 1. `sync.Map` vs `map` + `sync.RWMutex` for the state store

**Chosen:** `internal/store.Store` wraps a `sync.Map` keyed by job ID.

`sync.Map` is optimized for the case the Go documentation describes as
"keys are stable... and entries are either only ever added or removed for
a given key, but the values change frequently" — many goroutines reading
and writing largely disjoint keys, with low contention between them. That
matches the access pattern here reasonably well: each job ID is touched
mostly by the one worker that owns it at any given time, and reads
(`Get`, `ListByState`, `List`) vastly outnumber writes.

It does *not* give compare-and-swap across multiple fields of a single
value. `Transition` (validate the current state, then update state +
timestamps + error message together) is a read-modify-write that has to be
atomic, and `sync.Map` alone can't guarantee that for a single key under
concurrent callers. The implementation closes that gap with a coarse
`sync.Mutex` around the write path (`Create`/`Update`/`Transition`/
`SetOutput`); reads stay lock-free. This is a hybrid, not a textbook
`sync.Map` use — worth calling out explicitly rather than pretending
`sync.Map` alone solved concurrency here.

**Production alternative:** a `map[string]*Job` (each job boxed so updates
are in-place) guarded by a single `sync.RWMutex`, or skip in-memory
state entirely in favor of a real database (see tradeoff 3). The plain
`RWMutex` + map is in some ways simpler to reason about than this hybrid —
multi-field atomicity is just "hold the lock for the whole operation" —
and is what the Go team generally recommends unless profiling shows
`sync.Map` is actually faster for your access pattern.

## 2. Polling scheduler vs condition variable / heap

**Chosen:** `internal/scheduler.Scheduler` runs a `time.Ticker` every
500ms, scans all `PENDING` jobs whose `RunAfter` has passed, sorts the
ready ones by priority, and dispatches them.

This bounds dispatch latency to ~500ms in the worst case and is simple to
reason about: there's no missed-wakeup class of bug, no condition
variable to signal correctly from every code path that creates or retries
a job. The cost is exactly that latency — a job is never queued less than
0ms but can wait up to 500ms even when the system is otherwise idle — and
an O(n) scan of all pending jobs every tick, which is fine at the job
volumes this project targets but wouldn't scale to a very large pending
backlog.

**Production alternative:** a min-heap keyed by `(RunAfter, -Priority)`
with a single background goroutine blocked on a condition variable that's
signaled whenever a job is created, retried, or the heap's earliest
`RunAfter` changes. That gives near-zero dispatch latency and avoids the
repeated full scan, at the cost of more intricate signaling logic that has
to be gotten right on every insertion path.

## 3. In-memory buffered channel vs Redis-backed queue

**Chosen:** the job queue is a single Go `chan model.Job`, sized by
`Config.QueueSize`, shared in-process between the scheduler (producer) and
the worker pool (consumer).

This is the simplest thing that works: zero external dependencies, no
network hop between dispatch and execution, and the backpressure model
(non-blocking send, leave the job `PENDING` if the channel is full) falls
out naturally from Go's channel semantics. The cost is durability and
scale: the queue — and the entire `Store` — lives in one process's memory,
so a restart loses every job's state, and there is no way to run more than
one orchestrator instance, since two instances would each have their own
disconnected queue and could both try to run the same job.

**Production alternative:** a Redis-backed queue (e.g. `redis/go-redis`
with `BLPOP`/`BRPOPLPUSH` or Redis Streams), with job state persisted in
Redis or a real database instead of `sync.Map`. Multiple orchestrator
instances would then pull from the same shared queue, and a distributed
lock (or Redis Streams' consumer groups) would guarantee a given job runs
exactly once across the cluster. This is the change that turns this
single-node scheduler into a genuinely distributed system — see the
README's "Step-by-Step Build Guide" Task 8.2 for the intended shape of
that stretch goal, which this implementation deliberately leaves
unimplemented.

## Bug found via this design's own test suite

While writing `tests/integration_test.go`, the scheduler's dispatch order
turned out to have a real race: the original implementation sent a job
onto the queue channel *before* writing its `QUEUED` transition to the
store (mirroring the README's own illustrative pseudocode). An idle worker
could dequeue and call the executor before that store write landed, see
the job still `PENDING`, fail the `QUEUED -> RUNNING` validation, and
silently skip it — stranding the job once the scheduler's now-redundant
`QUEUED` write finally landed a moment later. The fix
(`internal/scheduler/scheduler.go`) writes the `QUEUED` transition first
and only then attempts the channel send, reverting to `PENDING` if the
send fails because the queue is full. This is exactly the kind of bug
`go test -race` and a real end-to-end test are supposed to catch, and it
took a full integration test — not the unit tests for any single package
— to surface it, since each package's own state transitions were
individually correct.
