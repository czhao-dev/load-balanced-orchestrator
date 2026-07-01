package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/czhao-dev/control-plane/internal/metrics"
	"github.com/czhao-dev/control-plane/internal/model"
	"github.com/czhao-dev/control-plane/internal/state"
)

type submitDeploymentRequest struct {
	Name          string                `json:"name"`
	Namespace     string                `json:"namespace"`
	Labels        map[string]string     `json:"labels"`
	Type          model.DeploymentType  `json:"type"`
	Image         string                `json:"image,omitempty"`
	Command       string                `json:"command"`
	Args          []string              `json:"args"`
	Replicas      int                   `json:"replicas"`
	MaxRetries    int                   `json:"max_retries"`
	RestartPolicy model.RestartPolicy   `json:"restart_policy"`
	Resources     model.ResourceRequest `json:"resources"`
}

// CreateDeployment handles POST /api/v1/deployments.
func (h *Handlers) CreateDeployment(w http.ResponseWriter, r *http.Request) {
	var req submitDeploymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Name == "" || (req.Command == "" && req.Image == "") {
		writeJSONError(w, http.StatusBadRequest, "name and either command or image are required")
		return
	}
	if req.Replicas <= 0 {
		req.Replicas = 1
	}
	if req.Type == "" {
		req.Type = model.DeploymentBatch
	}
	if req.RestartPolicy == "" {
		req.RestartPolicy = model.RestartOnFailure
	}
	if req.Namespace == "" {
		req.Namespace = "default"
	}

	const maxIDAttempts = 5
	var id string
	for attempt := 0; attempt < maxIDAttempts; attempt++ {
		genID, err := generateID("deploy")
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to generate deployment id")
			return
		}
		now := time.Now()
		deployment := &model.Deployment{
			ID:            genID,
			Name:          req.Name,
			Namespace:     req.Namespace,
			Labels:        req.Labels,
			Type:          req.Type,
			Image:         req.Image,
			Command:       req.Command,
			Args:          req.Args,
			Replicas:      req.Replicas,
			MaxRetries:    req.MaxRetries,
			RestartPolicy: req.RestartPolicy,
			Resources:     req.Resources,
			Status:        model.DeploymentPending,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		if err := h.store.CreateDeployment(r.Context(), deployment); err == nil {
			id = genID
			break
		}
	}
	if id == "" {
		writeJSONError(w, http.StatusInternalServerError, "failed to allocate a unique deployment id")
		return
	}

	metrics.DeploymentsTotal.Inc()

	deployment, err := h.store.GetDeployment(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "deployment vanished after creation")
		return
	}
	w.Header().Set("Location", "/api/v1/deployments/"+id)
	writeJSON(w, http.StatusCreated, deployment)
}

// ListDeployments handles GET /api/v1/deployments.
// Supports optional ?namespace=<ns> query param for namespace filtering.
func (h *Handlers) ListDeployments(w http.ResponseWriter, r *http.Request) {
	ns := r.URL.Query().Get("namespace")
	var deployments []*model.Deployment
	var err error
	if ns != "" {
		deployments, err = h.store.ListDeploymentsByNamespace(r.Context(), ns)
	} else {
		deployments, err = h.store.ListDeployments(r.Context())
	}
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deployments": deployments, "total": len(deployments)})
}

// GetDeployment handles GET /api/v1/deployments/{id}.
func (h *Handlers) GetDeployment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	deployment, err := h.store.GetDeployment(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "deployment not found")
		return
	}
	writeJSON(w, http.StatusOK, deployment)
}

// CancelDeployment handles DELETE /api/v1/deployments/{id}. It marks the
// deployment CANCELLED; the reconciler stops refilling its replica slots on
// its next pass (running pods are left to finish).
func (h *Handlers) CancelDeployment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := h.store.GetDeployment(r.Context(), id); err != nil {
		writeJSONError(w, http.StatusNotFound, "deployment not found")
		return
	}
	if err := h.store.TransitionDeployment(r.Context(), id, model.DeploymentCancelled); err != nil {
		if err == state.ErrInvalidTransition {
			writeJSONError(w, http.StatusConflict, "deployment already cancelled")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	deployment, _ := h.store.GetDeployment(r.Context(), id)
	writeJSON(w, http.StatusOK, deployment)
}

// ListDeploymentPods handles GET /api/v1/deployments/{id}/pods.
func (h *Handlers) ListDeploymentPods(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := h.store.GetDeployment(r.Context(), id); err != nil {
		writeJSONError(w, http.StatusNotFound, "deployment not found")
		return
	}
	pods, err := h.store.ListPodsByDeployment(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"pods": pods, "total": len(pods)})
}
