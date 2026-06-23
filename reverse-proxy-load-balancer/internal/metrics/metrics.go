// Package metrics tracks proxy-wide and per-backend counters and renders
// them in Prometheus text exposition format, without pulling in the
// Prometheus client library.
package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Metrics holds counters for the proxy and its backends.
type Metrics struct {
	requestsTotal  atomic.Int64
	retriesTotal   atomic.Int64
	requestSeconds durationHistogram

	mu       sync.Mutex
	backends map[string]*backendMetrics
}

type backendMetrics struct {
	requestsTotal     atomic.Int64
	errorsTotal       atomic.Int64
	activeConnections atomic.Int64
	healthy           atomic.Bool
}

// New creates an empty Metrics registry.
func New() *Metrics {
	return &Metrics{backends: make(map[string]*backendMetrics)}
}

func (m *Metrics) backendFor(name string) *backendMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	bm, ok := m.backends[name]
	if !ok {
		bm = &backendMetrics{}
		m.backends[name] = bm
	}
	return bm
}

// ObserveRequest records a completed proxy request and its duration.
func (m *Metrics) ObserveRequest(seconds float64) {
	m.requestsTotal.Add(1)
	m.requestSeconds.observe(seconds)
}

// IncRetries increments the total retry counter.
func (m *Metrics) IncRetries() {
	m.retriesTotal.Add(1)
}

// IncBackendRequests increments the request counter for a named backend.
func (m *Metrics) IncBackendRequests(name string) {
	m.backendFor(name).requestsTotal.Add(1)
}

// IncBackendErrors increments the error counter for a named backend.
func (m *Metrics) IncBackendErrors(name string) {
	m.backendFor(name).errorsTotal.Add(1)
}

// SetBackendActiveConnections records the current in-flight request count
// for a named backend.
func (m *Metrics) SetBackendActiveConnections(name string, n int64) {
	m.backendFor(name).activeConnections.Store(n)
}

// SetBackendHealth records whether a named backend is currently healthy.
func (m *Metrics) SetBackendHealth(name string, healthy bool) {
	m.backendFor(name).healthy.Store(healthy)
}

// Render writes all metrics in Prometheus text exposition format.
func (m *Metrics) Render() string {
	var sb strings.Builder

	fmt.Fprintf(&sb, "# HELP proxy_up Whether the proxy process is up.\n")
	fmt.Fprintf(&sb, "# TYPE proxy_up gauge\n")
	fmt.Fprintf(&sb, "proxy_up 1\n")

	fmt.Fprintf(&sb, "# HELP proxy_requests_total Total number of requests handled by the proxy.\n")
	fmt.Fprintf(&sb, "# TYPE proxy_requests_total counter\n")
	fmt.Fprintf(&sb, "proxy_requests_total %d\n", m.requestsTotal.Load())

	fmt.Fprintf(&sb, "# HELP proxy_retries_total Total number of request retries against alternate backends.\n")
	fmt.Fprintf(&sb, "# TYPE proxy_retries_total counter\n")
	fmt.Fprintf(&sb, "proxy_retries_total %d\n", m.retriesTotal.Load())

	count, sum := m.requestSeconds.snapshot()
	fmt.Fprintf(&sb, "# HELP proxy_request_duration_seconds Request duration in seconds.\n")
	fmt.Fprintf(&sb, "# TYPE proxy_request_duration_seconds summary\n")
	fmt.Fprintf(&sb, "proxy_request_duration_seconds_count %d\n", count)
	fmt.Fprintf(&sb, "proxy_request_duration_seconds_sum %f\n", sum)

	m.mu.Lock()
	names := make([]string, 0, len(m.backends))
	for name := range m.backends {
		names = append(names, name)
	}
	m.mu.Unlock()
	sort.Strings(names)

	fmt.Fprintf(&sb, "# HELP proxy_backend_requests_total Total requests routed to each backend.\n")
	fmt.Fprintf(&sb, "# TYPE proxy_backend_requests_total counter\n")
	for _, name := range names {
		bm := m.backendFor(name)
		fmt.Fprintf(&sb, "proxy_backend_requests_total{backend=%q} %d\n", name, bm.requestsTotal.Load())
	}

	fmt.Fprintf(&sb, "# HELP proxy_backend_errors_total Total errors encountered routing to each backend.\n")
	fmt.Fprintf(&sb, "# TYPE proxy_backend_errors_total counter\n")
	for _, name := range names {
		bm := m.backendFor(name)
		fmt.Fprintf(&sb, "proxy_backend_errors_total{backend=%q} %d\n", name, bm.errorsTotal.Load())
	}

	fmt.Fprintf(&sb, "# HELP proxy_backend_active_connections Current in-flight requests per backend.\n")
	fmt.Fprintf(&sb, "# TYPE proxy_backend_active_connections gauge\n")
	for _, name := range names {
		bm := m.backendFor(name)
		fmt.Fprintf(&sb, "proxy_backend_active_connections{backend=%q} %d\n", name, bm.activeConnections.Load())
	}

	fmt.Fprintf(&sb, "# HELP proxy_backend_health_status Backend health, 1 for healthy and 0 for unhealthy.\n")
	fmt.Fprintf(&sb, "# TYPE proxy_backend_health_status gauge\n")
	for _, name := range names {
		bm := m.backendFor(name)
		val := 0
		if bm.healthy.Load() {
			val = 1
		}
		fmt.Fprintf(&sb, "proxy_backend_health_status{backend=%q} %d\n", name, val)
	}

	return sb.String()
}

// durationHistogram is a minimal count/sum accumulator sufficient for
// reporting average request latency without full histogram buckets.
type durationHistogram struct {
	count atomic.Int64
	sumNs atomic.Int64
}

func (h *durationHistogram) observe(seconds float64) {
	h.count.Add(1)
	h.sumNs.Add(int64(seconds * 1e9))
}

func (h *durationHistogram) snapshot() (count int64, sumSeconds float64) {
	return h.count.Load(), float64(h.sumNs.Load()) / 1e9
}
