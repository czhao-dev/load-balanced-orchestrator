package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/czhao-dev/ml-job-orchestrator/internal/model"
	"github.com/czhao-dev/ml-job-orchestrator/internal/store"
	"github.com/czhao-dev/ml-job-orchestrator/internal/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRunner simulates job execution against a real store: transitions
// RUNNING then sleeps `delay` then transitions COMPLETED (or CANCELLED if
// ctx is cancelled mid-sleep), without spawning a real subprocess.
type fakeRunner struct {
	st    *store.Store
	delay time.Duration
}

func (r *fakeRunner) Run(ctx context.Context, job model.Job) {
	_ = r.st.Transition(job.ID, model.StateRunning, "")
	select {
	case <-time.After(r.delay):
		_ = r.st.Transition(job.ID, model.StateCompleted, "")
	case <-ctx.Done():
		_ = r.st.Transition(job.ID, model.StateCancelled, "shutdown")
	}
}

func TestPoolConcurrentSubmission(t *testing.T) {
	st := store.New()
	queue := make(chan model.Job, 100)
	runner := &fakeRunner{st: st, delay: 0}
	pool := worker.New(context.Background(), 8, queue, runner, 5*time.Second)
	defer pool.Shutdown()

	const numGoroutines = 10
	const jobsPerGoroutine = 10
	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < jobsPerGoroutine; i++ {
				id := fmt.Sprintf("g%d_j%d", g, i)
				job := model.Job{ID: id, State: model.StateQueued, CreatedAt: time.Now()}
				require.NoError(t, st.Create(job))
				queue <- job
			}
		}()
	}
	wg.Wait()

	require.Eventually(t, func() bool {
		return len(st.ListByState(model.StateCompleted)) == numGoroutines*jobsPerGoroutine
	}, 5*time.Second, 10*time.Millisecond)

	assert.Empty(t, st.ListByState(model.StateRunning))
	assert.Empty(t, st.ListByState(model.StateQueued))
}

func TestPoolShutdownDrainsInFlightJobs(t *testing.T) {
	st := store.New()
	queue := make(chan model.Job, 10)
	runner := &fakeRunner{st: st, delay: 1 * time.Second}
	// 10 jobs over 2 workers naturally drains in ~5 rounds * 1s = ~5s; give a
	// shutdownTimeout comfortably larger than that so this test exercises the
	// "finish naturally" path, not the force-kill path (see the dedicated
	// TestPoolShutdownForceKillsAfterTimeout for that).
	pool := worker.New(context.Background(), 2, queue, runner, 15*time.Second)

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("slow_%d", i)
		job := model.Job{ID: id, State: model.StateQueued, CreatedAt: time.Now()}
		require.NoError(t, st.Create(job))
		queue <- job
	}

	// Give workers a moment to pick up the first jobs before shutting down.
	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	pool.Shutdown()
	elapsed := time.Since(start)

	assert.Empty(t, st.ListByState(model.StateRunning), "no job should be left RUNNING after Shutdown")
	completed := st.ListByState(model.StateCompleted)
	assert.Len(t, completed, 10, "all 10 jobs should eventually reach a terminal state")
	assert.Less(t, elapsed, 10*time.Second, "shutdown should not wait for the full shutdownTimeout when jobs finish naturally")
}

func TestPoolShutdownForceKillsAfterTimeout(t *testing.T) {
	st := store.New()
	queue := make(chan model.Job, 1)
	runner := &fakeRunner{st: st, delay: 10 * time.Second}
	pool := worker.New(context.Background(), 1, queue, runner, 200*time.Millisecond)

	job := model.Job{ID: "stuck", State: model.StateQueued, CreatedAt: time.Now()}
	require.NoError(t, st.Create(job))
	queue <- job
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	pool.Shutdown()
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 2*time.Second, "shutdown must force-cancel stuck jobs after shutdownTimeout rather than waiting for the full job duration")
	got, err := st.Get("stuck")
	require.NoError(t, err)
	assert.Equal(t, model.StateCancelled, got.State)
}
