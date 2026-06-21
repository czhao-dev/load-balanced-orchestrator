package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/backend"
	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/balancer"
	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/metrics"
)

// BenchmarkHandler_RoundRobin measures end-to-end overhead of the proxy
// handler (backend selection, header rewriting, request/response copy)
// against an in-process upstream, isolating proxy cost from network cost.
func BenchmarkHandler_RoundRobin(b *testing.B) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	backends := make([]*backend.Backend, 0, 3)
	for i := 0; i < 3; i++ {
		bk, _ := backend.New(upstream.URL, upstream.URL, 1)
		backends = append(backends, bk)
	}
	pool := backend.NewPool(backends)
	rr := balancer.NewRoundRobin()
	h := New(pool, rr, metrics.New(), discardLogger(), Config{MaxRetries: 0, BackendTimeout: time.Second})

	req := httptest.NewRequest(http.MethodGet, "/bench", nil)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
		}
	})
}
