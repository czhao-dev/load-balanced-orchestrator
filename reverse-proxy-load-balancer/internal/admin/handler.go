// Package admin exposes read-only introspection endpoints for operators.
package admin

import (
	"encoding/json"
	"net/http"

	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/backend"
)

// backendStatus is the JSON representation of one backend's runtime state.
type backendStatus struct {
	Name              string `json:"name"`
	URL               string `json:"url"`
	Healthy           bool   `json:"healthy"`
	Weight            int    `json:"weight"`
	ActiveConnections int64  `json:"active_connections"`
	TotalRequests     int64  `json:"total_requests"`
	TotalErrors       int64  `json:"total_errors"`
}

// BackendsHandler returns an http.HandlerFunc that reports the status of
// every backend in the pool as JSON.
func BackendsHandler(pool *backend.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		backends := pool.Backends()
		statuses := make([]backendStatus, 0, len(backends))
		for _, b := range backends {
			statuses = append(statuses, backendStatus{
				Name:              b.Name,
				URL:               b.URL.String(),
				Healthy:           b.IsAlive(),
				Weight:            b.Weight,
				ActiveConnections: b.ActiveConnections(),
				TotalRequests:     b.TotalRequests(),
				TotalErrors:       b.TotalErrors(),
			})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(statuses)
	}
}
