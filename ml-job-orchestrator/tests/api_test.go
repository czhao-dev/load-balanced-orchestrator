package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/czhao-dev/ml-job-orchestrator/internal/api"
	"github.com/czhao-dev/ml-job-orchestrator/internal/model"
	"github.com/czhao-dev/ml-job-orchestrator/internal/store"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHandlers() (*api.Handlers, *store.Store, chan model.Job, *sync.Map) {
	st := store.New()
	queue := make(chan model.Job, 10)
	cancels := &sync.Map{}
	return api.NewHandlers(st, queue, cancels), st, queue, cancels
}

func TestSubmitJobHandler(t *testing.T) {
	h, st, _, _ := newTestHandlers()

	body := `{"name":"train-mnist","type":"training","command":"echo","args":["hi"],"timeout_seconds":300,"max_retries":2}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.SubmitJob(w, req)

	require.Equal(t, http.StatusCreated, w.Code)
	assert.NotEmpty(t, w.Header().Get("Location"))

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	id, _ := resp["id"].(string)
	assert.NotEmpty(t, id)
	assert.Equal(t, "PENDING", resp["state"])

	got, err := st.Get(id)
	require.NoError(t, err)
	assert.Equal(t, "train-mnist", got.Name)
	assert.Equal(t, 2, got.MaxRetries)
}

func TestSubmitJobHandlerValidation(t *testing.T) {
	h, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"name":"missing-command"}`))
	w := httptest.NewRecorder()
	h.SubmitJob(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)

	req2 := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`not json`))
	w2 := httptest.NewRecorder()
	h.SubmitJob(w2, req2)
	assert.Equal(t, http.StatusBadRequest, w2.Code)
}

func TestGetJobHandler(t *testing.T) {
	h, st, _, _ := newTestHandlers()
	require.NoError(t, st.Create(model.Job{ID: "job_abc", Name: "x", State: model.StatePending, CreatedAt: time.Now()}))

	req := withRouteVars(httptest.NewRequest(http.MethodGet, "/jobs/job_abc", nil), map[string]string{"id": "job_abc"})
	w := httptest.NewRecorder()
	h.GetJob(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var job model.Job
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &job))
	assert.Equal(t, "job_abc", job.ID)

	req2 := withRouteVars(httptest.NewRequest(http.MethodGet, "/jobs/missing", nil), map[string]string{"id": "missing"})
	w2 := httptest.NewRecorder()
	h.GetJob(w2, req2)
	assert.Equal(t, http.StatusNotFound, w2.Code)
}

func TestListJobsHandler(t *testing.T) {
	h, st, _, _ := newTestHandlers()
	require.NoError(t, st.Create(model.Job{ID: "j1", Type: "training", State: model.StateFailed, CreatedAt: time.Now()}))
	require.NoError(t, st.Create(model.Job{ID: "j2", Type: "training", State: model.StatePending, CreatedAt: time.Now()}))

	req := httptest.NewRequest(http.MethodGet, "/jobs?state=FAILED&type=training&limit=20", nil)
	w := httptest.NewRecorder()
	h.ListJobs(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		Jobs  []model.Job `json:"jobs"`
		Total int         `json:"total"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, 1, resp.Total)
	require.Len(t, resp.Jobs, 1)
	assert.Equal(t, "j1", resp.Jobs[0].ID)
}

func TestCancelJobHandlerPendingAndTerminal(t *testing.T) {
	h, st, _, _ := newTestHandlers()
	require.NoError(t, st.Create(model.Job{ID: "pending_job", State: model.StatePending, CreatedAt: time.Now()}))

	req := withRouteVars(httptest.NewRequest(http.MethodDelete, "/jobs/pending_job", nil), map[string]string{"id": "pending_job"})
	w := httptest.NewRecorder()
	h.CancelJob(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	got, err := st.Get("pending_job")
	require.NoError(t, err)
	assert.Equal(t, model.StateCancelled, got.State)

	// Cancelling an already-terminal job is a conflict, not a silent no-op.
	req2 := withRouteVars(httptest.NewRequest(http.MethodDelete, "/jobs/pending_job", nil), map[string]string{"id": "pending_job"})
	w2 := httptest.NewRecorder()
	h.CancelJob(w2, req2)
	assert.Equal(t, http.StatusConflict, w2.Code)
}

func TestCancelJobHandlerRunningKillsSubprocess(t *testing.T) {
	h, st, queue, cancels := newTestHandlers()

	// A long-lived file-growth signal stands in for `ps aux`: the running
	// "job" is simulated by a goroutine that respects the stored CancelFunc,
	// matching what the real executor does for exec.CommandContext.
	job := model.Job{ID: "running_job", State: model.StatePending, CreatedAt: time.Now()}
	require.NoError(t, st.Create(job))
	require.NoError(t, st.Transition("running_job", model.StateQueued, ""))
	require.NoError(t, st.Transition("running_job", model.StateRunning, ""))

	ctx, cancel := context.WithCancel(context.Background())
	cancels.Store("running_job", cancel)
	stopped := make(chan struct{})
	go func() {
		<-ctx.Done()
		_ = st.Transition("running_job", model.StateCancelled, "cancelled by user")
		close(stopped)
	}()

	req := withRouteVars(httptest.NewRequest(http.MethodDelete, "/jobs/running_job", nil), map[string]string{"id": "running_job"})
	w := httptest.NewRecorder()
	h.CancelJob(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	select {
	case <-stopped:
	case <-time.After(1 * time.Second):
		t.Fatal("cancel func was never invoked by the handler")
	}

	var resp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "CANCELLED", resp["state"])
	_ = queue
}

func TestHealthzHandler(t *testing.T) {
	w := httptest.NewRecorder()
	api.Healthz(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRouterMethodRouting(t *testing.T) {
	h, _, _, _ := newTestHandlers()
	router := api.NewRouter(h)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.Equal(t, http.StatusOK, w.Code)

	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	assert.Equal(t, http.StatusOK, w2.Code)
}

// withRouteVars injects gorilla/mux path variables into a request the way
// the router would when a real request matches a {id} route, so handlers
// can be unit tested directly with httptest without going through the
// router.
func withRouteVars(r *http.Request, vars map[string]string) *http.Request {
	return mux.SetURLVars(r, vars)
}
