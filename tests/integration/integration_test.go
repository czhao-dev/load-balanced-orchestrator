// Package integration exercises the proxy, balancer, and health checker
// together against real backend HTTP servers, rather than unit-testing
// each package in isolation.
package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/backend"
	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/balancer"
	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/health"
	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/metrics"
	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/proxy"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newDemoBackend starts an httptest server that identifies itself in its
// response body and answers /health for the health checker.
func newDemoBackend(name string, healthy func() bool) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if healthy() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "hello from %s", name)
	})
	return httptest.NewServer(mux)
}

func TestProxy_DistributesAcrossBackends(t *testing.T) {
	srv1 := newDemoBackend("backend-1", func() bool { return true })
	defer srv1.Close()
	srv2 := newDemoBackend("backend-2", func() bool { return true })
	defer srv2.Close()

	b1, _ := backend.New("backend-1", srv1.URL, 1)
	b2, _ := backend.New("backend-2", srv2.URL, 1)
	pool := backend.NewPool([]*backend.Backend{b1, b2})

	rr := balancer.NewRoundRobin()
	h := proxy.New(pool, rr, metrics.New(), discardLogger(), proxy.Config{MaxRetries: 1, BackendTimeout: time.Second})

	proxySrv := httptest.NewServer(h)
	defer proxySrv.Close()

	seen := map[string]int{}
	for i := 0; i < 10; i++ {
		resp, err := http.Get(proxySrv.URL + "/api/hello")
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		seen[string(body)]++
	}

	if len(seen) != 2 {
		t.Errorf("expected traffic to be distributed across both backends, got %v", seen)
	}
}

func TestProxy_RemovesAndRestoresFailedBackend(t *testing.T) {
	srv1 := newDemoBackend("backend-1", func() bool { return true })
	defer srv1.Close()

	var failing atomic.Bool
	failing.Store(true)
	srv2 := newDemoBackend("backend-2", func() bool { return !failing.Load() })
	defer srv2.Close()

	b1, _ := backend.New("backend-1", srv1.URL, 1)
	b2, _ := backend.New("backend-2", srv2.URL, 1)
	pool := backend.NewPool([]*backend.Backend{b1, b2})

	checker := health.New(pool, health.Config{
		Path:               "/health",
		Interval:           10 * time.Millisecond,
		Timeout:            100 * time.Millisecond,
		UnhealthyThreshold: 1,
		HealthyThreshold:   1,
	}, discardLogger(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && b2.IsAlive() {
		time.Sleep(5 * time.Millisecond)
	}
	if b2.IsAlive() {
		t.Fatal("expected backend-2 to be marked unhealthy")
	}

	rr := balancer.NewRoundRobin()
	for i := 0; i < 5; i++ {
		b, err := rr.Next(pool)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b.Name != "backend-1" {
			t.Errorf("expected only healthy backend-1 to be selected, got %s", b.Name)
		}
	}

	failing.Store(false)
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) && !b2.IsAlive() {
		time.Sleep(5 * time.Millisecond)
	}
	if !b2.IsAlive() {
		t.Fatal("expected backend-2 to recover once its health endpoint succeeds")
	}
}

func TestProxy_GracefulShutdownStopsAcceptingRequests(t *testing.T) {
	backendSrv := newDemoBackend("backend-1", func() bool { return true })
	defer backendSrv.Close()

	b, _ := backend.New("backend-1", backendSrv.URL, 1)
	pool := backend.NewPool([]*backend.Backend{b})
	rr := balancer.NewRoundRobin()
	h := proxy.New(pool, rr, metrics.New(), discardLogger(), proxy.Config{MaxRetries: 0, BackendTimeout: time.Second})

	server := httptest.NewServer(h)

	resp, err := http.Get(server.URL + "/")
	if err != nil {
		t.Fatalf("GET before shutdown: %v", err)
	}
	resp.Body.Close()

	server.Close()

	if _, err := http.Get(server.URL + "/"); err == nil {
		t.Error("expected request after shutdown to fail")
	}
}
