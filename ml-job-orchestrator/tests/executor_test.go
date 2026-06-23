package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/czhao-dev/ml-job-orchestrator/internal/executor"
	"github.com/czhao-dev/ml-job-orchestrator/internal/model"
	"github.com/czhao-dev/ml-job-orchestrator/internal/retry"
	"github.com/czhao-dev/ml-job-orchestrator/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryScheduleRetryBackoff(t *testing.T) {
	job := model.Job{ID: "j", RetryCount: 0, MaxRetries: 3}
	before := time.Now()

	r1 := retry.ScheduleRetry(job)
	assert.Equal(t, 1, r1.RetryCount)
	assert.Equal(t, model.StatePending, r1.State)
	assert.True(t, r1.RunAfter.After(before.Add(1*time.Second)), "first retry should back off ~2s")
	assert.True(t, r1.RunAfter.Before(before.Add(3*time.Second)))

	r2 := retry.ScheduleRetry(r1)
	assert.Equal(t, 2, r2.RetryCount)
	assert.True(t, r2.RunAfter.After(before.Add(3*time.Second)), "second retry should back off ~4s")

	// Backoff caps at 60s even for a large retry count.
	high := model.Job{ID: "j", RetryCount: 9}
	rHigh := retry.ScheduleRetry(high)
	assert.LessOrEqual(t, rHigh.RunAfter.Sub(time.Now()), 61*time.Second)
}

func TestExecutorTimeoutEnforcement(t *testing.T) {
	st := store.New()
	cancels := &sync.Map{}
	ex := executor.New(st, cancels)

	job := model.Job{
		ID: "timeout_job", State: model.StateQueued, Command: "sleep", Args: []string{"10"},
		TimeoutSeconds: 2, MaxRetries: 0, CreatedAt: time.Now(),
	}
	require.NoError(t, st.Create(job))

	start := time.Now()
	ex.Run(context.Background(), job)
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 3*time.Second, "exec.CommandContext should kill the subprocess at the timeout, not let it run to sleep's full 10s")

	got, err := st.Get("timeout_job")
	require.NoError(t, err)
	// With MaxRetries=0 the job has no retry budget, so failOrRetry advances
	// straight from FAILED to EXHAUSTED within the same Run() call — the
	// ErrorMessage set when entering FAILED is preserved across that
	// follow-on transition.
	assert.Equal(t, model.StateExhausted, got.State)
	assert.Contains(t, got.ErrorMessage, "timeout")
}

func TestExecutorCancelShortCircuitsRetry(t *testing.T) {
	st := store.New()
	cancels := &sync.Map{}
	ex := executor.New(st, cancels)

	job := model.Job{
		ID: "cancel_job", State: model.StateQueued, Command: "sleep", Args: []string{"10"},
		MaxRetries: 3, CreatedAt: time.Now(),
	}
	require.NoError(t, st.Create(job))

	done := make(chan struct{})
	go func() {
		ex.Run(context.Background(), job)
		close(done)
	}()

	require.Eventually(t, func() bool {
		_, ok := cancels.Load("cancel_job")
		return ok
	}, 2*time.Second, 10*time.Millisecond, "executor should register a CancelFunc once the job is running")

	cancelFn, _ := cancels.Load("cancel_job")
	cancelFn.(context.CancelFunc)()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run() did not return after cancellation")
	}

	got, err := st.Get("cancel_job")
	require.NoError(t, err)
	assert.Equal(t, model.StateCancelled, got.State, "cancellation must win over retry even with retries remaining")
	assert.Equal(t, 0, got.RetryCount, "a cancelled job must never be retried")
}

func TestExecutorRetryThenExhaustion(t *testing.T) {
	st := store.New()
	cancels := &sync.Map{}
	ex := executor.New(st, cancels)

	job := model.Job{
		ID: "flaky_job", State: model.StateQueued, Command: "false", MaxRetries: 2, CreatedAt: time.Now(),
	}
	require.NoError(t, st.Create(job))

	// Attempt 1: fails, retry scheduled (RetryCount 0 -> 1).
	ex.Run(context.Background(), job)
	got, err := st.Get("flaky_job")
	require.NoError(t, err)
	require.Equal(t, model.StatePending, got.State)
	assert.Equal(t, 1, got.RetryCount)

	// Simulate the scheduler picking it back up for attempt 2.
	require.NoError(t, st.Transition("flaky_job", model.StateQueued, ""))
	ex.Run(context.Background(), got)
	got, err = st.Get("flaky_job")
	require.NoError(t, err)
	require.Equal(t, model.StatePending, got.State, "RetryCount(1) < MaxRetries(2) should still retry")
	assert.Equal(t, 2, got.RetryCount)

	// Attempt 3: retry budget (MaxRetries=2) is now exhausted.
	require.NoError(t, st.Transition("flaky_job", model.StateQueued, ""))
	ex.Run(context.Background(), got)
	got, err = st.Get("flaky_job")
	require.NoError(t, err)
	assert.Equal(t, model.StateExhausted, got.State, "exactly MaxRetries retries should occur, not MaxRetries+1")
	assert.Equal(t, 2, got.RetryCount)
}

func TestExecutorCompletesSuccessfully(t *testing.T) {
	st := store.New()
	cancels := &sync.Map{}
	ex := executor.New(st, cancels)

	job := model.Job{
		ID: "ok_job", State: model.StateQueued, Command: "echo", Args: []string{"hello"}, CreatedAt: time.Now(),
	}
	require.NoError(t, st.Create(job))

	ex.Run(context.Background(), job)

	got, err := st.Get("ok_job")
	require.NoError(t, err)
	assert.Equal(t, model.StateCompleted, got.State)
	assert.Contains(t, got.Output, "hello")
	require.NotNil(t, got.StartedAt)
	require.NotNil(t, got.FinishedAt)
}
