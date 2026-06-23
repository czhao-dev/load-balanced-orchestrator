package tests

import (
	"context"
	"testing"
	"time"

	"github.com/czhao-dev/ml-job-orchestrator/internal/model"
	"github.com/czhao-dev/ml-job-orchestrator/internal/scheduler"
	"github.com/czhao-dev/ml-job-orchestrator/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchedulerDispatchesWithinPollInterval(t *testing.T) {
	st := store.New()
	queue := make(chan model.Job, 10)
	sched := scheduler.New(st, queue)

	job := model.Job{ID: "ready_job", State: model.StatePending, CreatedAt: time.Now()}
	require.NoError(t, st.Create(job))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sched.Run(ctx)

	require.Eventually(t, func() bool {
		got, err := st.Get("ready_job")
		return err == nil && got.State == model.StateQueued
	}, 1*time.Second, 20*time.Millisecond, "job should be dispatched within ~500ms of the poll interval")
}

func TestSchedulerRespectsRunAfter(t *testing.T) {
	st := store.New()
	queue := make(chan model.Job, 10)
	sched := scheduler.New(st, queue)

	job := model.Job{
		ID: "future_job", State: model.StatePending, CreatedAt: time.Now(),
		RunAfter: time.Now().Add(2 * time.Second),
	}
	require.NoError(t, st.Create(job))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sched.Run(ctx)

	time.Sleep(700 * time.Millisecond)
	got, err := st.Get("future_job")
	require.NoError(t, err)
	assert.Equal(t, model.StatePending, got.State, "a job whose RunAfter hasn't passed must not be dispatched even if ready otherwise")

	require.Eventually(t, func() bool {
		got, err := st.Get("future_job")
		return err == nil && got.State == model.StateQueued
	}, 2*time.Second, 50*time.Millisecond)
}

func TestSchedulerPriorityOrdering(t *testing.T) {
	st := store.New()
	queue := make(chan model.Job, 10)
	sched := scheduler.New(st, queue)

	low := model.Job{ID: "low", State: model.StatePending, Priority: 1, CreatedAt: time.Now()}
	high := model.Job{ID: "high", State: model.StatePending, Priority: 5, CreatedAt: time.Now().Add(time.Millisecond)}
	require.NoError(t, st.Create(low))
	require.NoError(t, st.Create(high))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sched.Run(ctx)

	first := <-queue
	second := <-queue
	assert.Equal(t, "high", first.ID, "higher priority job should be dispatched first even though it was created later")
	assert.Equal(t, "low", second.ID)
}

func TestSchedulerBackpressureLeavesJobPending(t *testing.T) {
	st := store.New()
	queue := make(chan model.Job, 1) // tiny queue to force backpressure
	sched := scheduler.New(st, queue)

	for i := 0; i < 3; i++ {
		job := model.Job{ID: string(rune('a' + i)), State: model.StatePending, CreatedAt: time.Now()}
		require.NoError(t, st.Create(job))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sched.Run(ctx)

	// Don't drain the queue — give the scheduler time to try (and fail) to
	// dispatch the jobs that don't fit.
	time.Sleep(700 * time.Millisecond)

	pending := st.ListByState(model.StatePending)
	queued := st.ListByState(model.StateQueued)
	assert.Len(t, queued, 1, "only as many jobs as fit in the buffered channel should be QUEUED")
	assert.Len(t, pending, 2, "jobs that didn't fit must remain PENDING, not be dropped or block the scheduler")
}
