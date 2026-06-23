package executor

import (
	"bytes"
	"context"
	"os/exec"
	"sync"
	"time"

	"github.com/czhao-dev/ml-job-orchestrator/internal/metrics"
	"github.com/czhao-dev/ml-job-orchestrator/internal/model"
	"github.com/czhao-dev/ml-job-orchestrator/internal/retry"
	"github.com/czhao-dev/ml-job-orchestrator/internal/store"
)

// Executor runs jobs as subprocesses and drives their state transitions.
// It implements worker.JobRunner.
type Executor struct {
	store   *store.Store
	cancels *sync.Map // job ID -> context.CancelFunc; shared with the API's DELETE handler
}

// New constructs an Executor. cancels is a *sync.Map shared with the API
// server's DELETE handler, which looks up and calls a running job's
// CancelFunc to terminate its subprocess.
func New(st *store.Store, cancels *sync.Map) *Executor {
	return &Executor{store: st, cancels: cancels}
}

// Run executes job as a subprocess, enforcing job.TimeoutSeconds via the
// context deadline, and transitions the job to its resulting state
// (COMPLETED, FAILED -> retry-pending PENDING, EXHAUSTED, or CANCELLED).
func (e *Executor) Run(parentCtx context.Context, job model.Job) {
	if err := e.store.Transition(job.ID, model.StateRunning, ""); err != nil {
		// Already CANCELLED (or otherwise no longer eligible) — nothing to run.
		return
	}

	ctx := parentCtx
	var cancel context.CancelFunc
	if job.TimeoutSeconds > 0 {
		ctx, cancel = context.WithTimeout(parentCtx, time.Duration(job.TimeoutSeconds)*time.Second)
	} else {
		ctx, cancel = context.WithCancel(parentCtx)
	}
	e.cancels.Store(job.ID, cancel)
	defer func() {
		e.cancels.Delete(job.ID)
		cancel()
	}()

	start := time.Now()
	// A fresh buffer per invocation — sharing one across concurrent jobs
	// would corrupt each other's captured output.
	var buf bytes.Buffer
	cmd := exec.CommandContext(ctx, job.Command, job.Args...)
	cmd.Stdout = &buf
	cmd.Stderr = &buf

	runErr := cmd.Run()
	_ = e.store.SetOutput(job.ID, buf.String())
	duration := time.Since(start)

	switch {
	case ctx.Err() == context.Canceled:
		// Triggered either by the API's DELETE handler or by the worker
		// pool's shutdown-timeout escape hatch. Cancellation always wins
		// over retry, regardless of remaining retry budget.
		_ = e.store.Transition(job.ID, model.StateCancelled, "cancelled by user")
		e.recordTerminal(job.Type, model.StateCancelled, duration, false)
	case ctx.Err() == context.DeadlineExceeded:
		e.failOrRetry(job, "timeout", duration)
	case runErr != nil:
		e.failOrRetry(job, runErr.Error(), duration)
	default:
		_ = e.store.Transition(job.ID, model.StateCompleted, "")
		e.recordTerminal(job.Type, model.StateCompleted, duration, true)
	}
}

// failOrRetry transitions job to FAILED, then either schedules a retry
// (state -> PENDING with backoff) or, if the retry budget is exhausted,
// transitions to EXHAUSTED.
func (e *Executor) failOrRetry(job model.Job, errMsg string, duration time.Duration) {
	if err := e.store.Transition(job.ID, model.StateFailed, errMsg); err != nil {
		return
	}
	e.recordTerminal(job.Type, model.StateFailed, duration, false)

	current, err := e.store.Get(job.ID)
	if err != nil {
		return
	}
	if current.RetryCount < current.MaxRetries {
		_ = e.store.Update(retry.ScheduleRetry(current))
		return
	}
	_ = e.store.Transition(job.ID, model.StateExhausted, "")
}

func (e *Executor) recordTerminal(jobType string, state model.State, duration time.Duration, success bool) {
	if success {
		metrics.JobsCompleted.WithLabelValues(jobType).Inc()
	} else {
		metrics.JobsFailed.WithLabelValues(jobType).Inc()
	}
	metrics.JobsDuration.WithLabelValues(jobType, string(state)).Observe(duration.Seconds())
}
