package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/czhao-dev/ml-job-orchestrator/internal/api"
	"github.com/czhao-dev/ml-job-orchestrator/internal/executor"
	"github.com/czhao-dev/ml-job-orchestrator/internal/model"
	"github.com/czhao-dev/ml-job-orchestrator/internal/scheduler"
	"github.com/czhao-dev/ml-job-orchestrator/internal/store"
	"github.com/czhao-dev/ml-job-orchestrator/internal/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegrationFullStack exercises the entire stack — API, scheduler,
// worker pool, executor, retry, store — in a single process: submit jobs
// over real HTTP, let the scheduler/pool/executor drive them to completion,
// and assert the final states. This is the most valuable test in the suite.
func TestIntegrationFullStack(t *testing.T) {
	st := store.New()
	queue := make(chan model.Job, 50)
	cancels := &sync.Map{}
	exec := executor.New(st, cancels)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := worker.New(ctx, 4, queue, exec, 5*time.Second)
	defer pool.Shutdown()
	sched := scheduler.New(st, queue)
	go sched.Run(ctx)

	handlers := api.NewHandlers(st, queue, cancels)
	srv := httptest.NewServer(api.NewRouter(handlers))
	defer srv.Close()

	submit := func(t *testing.T, payload map[string]any) string {
		body, _ := json.Marshal(payload)
		resp, err := http.Post(srv.URL+"/jobs", "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusCreated, resp.StatusCode)
		var created struct {
			ID string `json:"id"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&created))
		return created.ID
	}

	successID := submit(t, map[string]any{
		"name": "ok", "command": "echo", "args": []string{"hello"},
	})
	failID := submit(t, map[string]any{
		"name": "fails", "command": "false", "max_retries": 0,
	})
	timeoutID := submit(t, map[string]any{
		"name": "slow", "command": "sleep", "args": []string{"5"}, "timeout_seconds": 1, "max_retries": 0,
	})
	retryThenExhaustID := submit(t, map[string]any{
		"name": "flaky", "command": "false", "max_retries": 1,
	})
	toCancelID := submit(t, map[string]any{
		"name": "long-running", "command": "sleep", "args": []string{"30"},
	})

	// Cancel the long-running job shortly after submission, while it should
	// be RUNNING.
	require.Eventually(t, func() bool {
		got, err := st.Get(toCancelID)
		return err == nil && got.State == model.StateRunning
	}, 2*time.Second, 20*time.Millisecond)
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/jobs/"+toCancelID, nil)
	delResp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	delResp.Body.Close()

	terminal := func(s model.State) bool {
		switch s {
		case model.StateCompleted, model.StateFailed, model.StateExhausted, model.StateCancelled:
			return true
		default:
			return false
		}
	}

	// retryThenExhaustID needs one full backoff cycle (~2s) plus a poll
	// interval before its second, exhausting attempt — give every job a
	// comfortable margin above that.
	for _, id := range []string{successID, failID, timeoutID, retryThenExhaustID, toCancelID} {
		require.Eventually(t, func() bool {
			got, err := st.Get(id)
			return err == nil && terminal(got.State)
		}, 8*time.Second, 100*time.Millisecond, "job %s should reach a terminal state", id)
	}

	got, _ := st.Get(successID)
	assert.Equal(t, model.StateCompleted, got.State)
	assert.Contains(t, got.Output, "hello")

	got, _ = st.Get(failID)
	assert.Equal(t, model.StateExhausted, got.State)

	got, _ = st.Get(timeoutID)
	assert.Equal(t, model.StateExhausted, got.State)
	assert.Contains(t, got.ErrorMessage, "timeout")

	got, _ = st.Get(retryThenExhaustID)
	assert.Equal(t, model.StateExhausted, got.State)
	assert.Equal(t, 1, got.RetryCount)

	got, _ = st.Get(toCancelID)
	assert.Equal(t, model.StateCancelled, got.State)

	// GET /jobs should reflect the final state for a representative job.
	listResp, err := http.Get(srv.URL + "/jobs?state=COMPLETED")
	require.NoError(t, err)
	defer listResp.Body.Close()
	var listed struct {
		Jobs  []model.Job `json:"jobs"`
		Total int         `json:"total"`
	}
	require.NoError(t, json.NewDecoder(listResp.Body).Decode(&listed))
	assert.Equal(t, 1, listed.Total)
}
