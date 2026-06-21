package metrics

import (
	"strings"
	"testing"
)

func TestMetrics_RenderIncludesObservedValues(t *testing.T) {
	m := New()
	m.ObserveRequest(0.1)
	m.ObserveRequest(0.2)
	m.IncRetries()
	m.IncBackendRequests("b1")
	m.IncBackendRequests("b1")
	m.IncBackendErrors("b1")
	m.SetBackendActiveConnections("b1", 3)
	m.SetBackendHealth("b1", true)

	out := m.Render()

	checks := []string{
		"proxy_requests_total 2",
		"proxy_retries_total 1",
		`proxy_backend_requests_total{backend="b1"} 2`,
		`proxy_backend_errors_total{backend="b1"} 1`,
		`proxy_backend_active_connections{backend="b1"} 3`,
		`proxy_backend_health_status{backend="b1"} 1`,
	}
	for _, want := range checks {
		if !strings.Contains(out, want) {
			t.Errorf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestMetrics_UnknownBackendDefaultsToUnhealthy(t *testing.T) {
	m := New()
	m.IncBackendRequests("b1")
	out := m.Render()

	if !strings.Contains(out, `proxy_backend_health_status{backend="b1"} 0`) {
		t.Errorf("expected backend with no recorded health to default to 0, got:\n%s", out)
	}
}
