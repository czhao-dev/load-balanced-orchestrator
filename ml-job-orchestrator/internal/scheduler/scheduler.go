package scheduler

import (
	"context"
	"sort"
	"time"

	"github.com/czhao-dev/ml-job-orchestrator/internal/metrics"
	"github.com/czhao-dev/ml-job-orchestrator/internal/model"
	"github.com/czhao-dev/ml-job-orchestrator/internal/store"
)

const pollInterval = 500 * time.Millisecond

// Scheduler polls the state store for PENDING jobs whose RunAfter has
// passed, orders them by priority, and dispatches them onto the job queue
// channel. A production scheduler would use a condition variable or a
// heap-based priority queue to avoid polling latency; polling is simpler to
// reason about and bounds dispatch latency to pollInterval, which is
// sufficient for this scope (see docs/design.md).
type Scheduler struct {
	store *store.Store
	queue chan model.Job
}

func New(st *store.Store, queue chan model.Job) *Scheduler {
	return &Scheduler{store: st, queue: queue}
}

func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.scheduleReady()
		case <-ctx.Done():
			return
		}
	}
}

// scheduleReady dispatches every PENDING job whose RunAfter has passed,
// ordered by priority (higher first) then CreatedAt (earlier first).
// RunAfter is a hard gate: a high-priority retry that's still backing off
// is not considered ready, regardless of priority.
func (s *Scheduler) scheduleReady() {
	pending := s.store.ListByState(model.StatePending)
	now := time.Now()

	ready := make([]model.Job, 0, len(pending))
	for _, j := range pending {
		if j.RunAfter.IsZero() || j.RunAfter.Before(now) {
			ready = append(ready, j)
		}
	}
	sort.Slice(ready, func(i, k int) bool {
		if ready[i].Priority != ready[k].Priority {
			return ready[i].Priority > ready[k].Priority
		}
		return ready[i].CreatedAt.Before(ready[k].CreatedAt)
	})

	for _, j := range ready {
		// Transition to QUEUED in the store BEFORE the job becomes visible
		// on the channel. Sending first and updating the store second (as
		// the README's illustrative pseudocode does) leaves a race window:
		// an idle worker can dequeue and call executor.Run, which tries to
		// transition QUEUED->RUNNING, before this goroutine's own QUEUED
		// write lands — that transition is invalid while the store still
		// shows PENDING, so the run is silently skipped and the job is
		// stranded once this goroutine's write finally does land.
		if err := s.store.Transition(j.ID, model.StateQueued, ""); err != nil {
			continue // already transitioned elsewhere (e.g. cancelled)
		}
		select {
		case s.queue <- j:
			// dispatched
		default:
			// Queue is full — revert to PENDING and retry next tick. A
			// blocking send here would deadlock this goroutine (and with
			// it, every other job's dispatch) if the worker pool is
			// saturated and not currently draining. Ignore the error: if
			// the job was cancelled in the meantime it's already terminal
			// and must not be reverted to PENDING.
			_ = s.store.Transition(j.ID, model.StatePending, "")
		}
	}
	metrics.QueueDepth.Set(float64(len(s.queue)))
}
