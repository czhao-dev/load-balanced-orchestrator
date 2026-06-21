// Package backend models a single upstream server and its runtime state.
package backend

import (
	"net/url"
	"sync/atomic"
)

// Backend represents a single upstream server that the proxy can forward
// requests to, along with the mutable state needed to load-balance and
// health-check it.
type Backend struct {
	Name   string
	URL    *url.URL
	Weight int

	alive       atomic.Bool
	activeConns atomic.Int64
	totalReqs   atomic.Int64
	totalErrs   atomic.Int64
}

// New creates a Backend in the "alive" state.
func New(name string, rawURL string, weight int) (*Backend, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if weight <= 0 {
		weight = 1
	}
	b := &Backend{Name: name, URL: u, Weight: weight}
	b.alive.Store(true)
	return b, nil
}

// SetAlive updates the backend's health status.
func (b *Backend) SetAlive(alive bool) {
	b.alive.Store(alive)
}

// IsAlive reports whether the backend currently passes health checks.
func (b *Backend) IsAlive() bool {
	return b.alive.Load()
}

// ActiveConnections returns the number of in-flight requests on this backend.
func (b *Backend) ActiveConnections() int64 {
	return b.activeConns.Load()
}

// IncConnections increments the in-flight request counter.
func (b *Backend) IncConnections() {
	b.activeConns.Add(1)
}

// DecConnections decrements the in-flight request counter.
func (b *Backend) DecConnections() {
	b.activeConns.Add(-1)
}

// IncRequests increments the total request counter.
func (b *Backend) IncRequests() {
	b.totalReqs.Add(1)
}

// IncErrors increments the total error counter.
func (b *Backend) IncErrors() {
	b.totalErrs.Add(1)
}

// TotalRequests returns the lifetime request count for this backend.
func (b *Backend) TotalRequests() int64 {
	return b.totalReqs.Load()
}

// TotalErrors returns the lifetime error count for this backend.
func (b *Backend) TotalErrors() int64 {
	return b.totalErrs.Load()
}
