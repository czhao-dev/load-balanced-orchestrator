package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/czhao-dev/ml-job-orchestrator/internal/metrics"
	"github.com/czhao-dev/ml-job-orchestrator/internal/model"
	"github.com/czhao-dev/ml-job-orchestrator/internal/store"
	"github.com/gorilla/mux"
)

// Handlers holds the dependencies shared by every HTTP handler.
type Handlers struct {
	store   *store.Store
	queue   chan model.Job
	cancels *sync.Map // job ID -> context.CancelFunc; shared with the executor
}

func NewHandlers(st *store.Store, queue chan model.Job, cancels *sync.Map) *Handlers {
	return &Handlers{store: st, queue: queue, cancels: cancels}
}

type submitJobRequest struct {
	Name           string   `json:"name"`
	Type           string   `json:"type"`
	Command        string   `json:"command"`
	Args           []string `json:"args"`
	TimeoutSeconds int      `json:"timeout_seconds"`
	MaxRetries     int      `json:"max_retries"`
	Priority       int      `json:"priority"`
}

func generateID() (string, error) {
	b := make([]byte, 3)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "job_" + hex.EncodeToString(b), nil
}

// SubmitJob handles POST /jobs.
func (h *Handlers) SubmitJob(w http.ResponseWriter, r *http.Request) {
	var req submitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Name == "" || req.Command == "" {
		writeJSONError(w, http.StatusBadRequest, "name and command are required")
		return
	}

	const maxIDAttempts = 5
	var id string
	for attempt := 0; attempt < maxIDAttempts; attempt++ {
		genID, err := generateID()
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to generate job id")
			return
		}
		job := model.Job{
			ID:             genID,
			Name:           req.Name,
			Type:           req.Type,
			Command:        req.Command,
			Args:           req.Args,
			State:          model.StatePending,
			Priority:       req.Priority,
			MaxRetries:     req.MaxRetries,
			TimeoutSeconds: req.TimeoutSeconds,
			CreatedAt:      time.Now(),
		}
		if err := h.store.Create(job); err == nil {
			id = genID
			break
		}
	}
	if id == "" {
		writeJSONError(w, http.StatusInternalServerError, "failed to allocate a unique job id")
		return
	}

	metrics.JobsSubmitted.WithLabelValues(req.Type).Inc()

	job, err := h.store.Get(id)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "job vanished after creation")
		return
	}
	w.Header().Set("Location", "/jobs/"+id)
	writeJSON(w, http.StatusCreated, map[string]any{
		"id":         job.ID,
		"state":      job.State,
		"created_at": job.CreatedAt,
	})
}

// GetJob handles GET /jobs/{id}.
func (h *Handlers) GetJob(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	job, err := h.store.Get(id)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

// ListJobs handles GET /jobs?state=&type=&limit=.
func (h *Handlers) ListJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := store.ListFilter{State: q.Get("state"), Type: q.Get("type")}
	if l := q.Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil {
			filter.Limit = n
		}
	}
	jobs, total := h.store.List(filter)
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs, "total": total})
}

// CancelJob handles DELETE /jobs/{id}.
//
// For PENDING/QUEUED jobs (no subprocess yet) it transitions straight to
// CANCELLED. For RUNNING jobs, it triggers the job's CancelFunc and then
// waits briefly for the executor — the sole owner of that terminal write —
// to record CANCELLED, rather than writing it here itself; two writers
// racing to set a job's terminal state would be a bug.
func (h *Handlers) CancelJob(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	job, err := h.store.Get(id)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "job not found")
		return
	}

	switch job.State {
	case model.StateCompleted, model.StateFailed, model.StateExhausted, model.StateCancelled:
		writeJSONError(w, http.StatusConflict, fmt.Sprintf("job already in terminal state %s", job.State))
		return
	case model.StateRunning:
		if cancelAny, ok := h.cancels.Load(id); ok {
			cancelAny.(context.CancelFunc)()
		}
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if current, err := h.store.Get(id); err == nil && current.State == model.StateCancelled {
				job = current
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
	default: // PENDING or QUEUED
		if err := h.store.Transition(id, model.StateCancelled, "cancelled by user"); err != nil {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		job, _ = h.store.Get(id)
	}

	writeJSON(w, http.StatusOK, map[string]string{"id": job.ID, "state": string(job.State)})
}

// Healthz handles GET /healthz.
func Healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
