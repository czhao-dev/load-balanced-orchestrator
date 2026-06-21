package health

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/backend"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestChecker_MarksUnhealthyAfterThreshold(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	b, _ := backend.New("b1", srv.URL, 1)
	pool := backend.NewPool([]*backend.Backend{b})

	var transitions int32
	checker := New(pool, Config{
		Path:               "/health",
		Interval:           10 * time.Millisecond,
		Timeout:            100 * time.Millisecond,
		UnhealthyThreshold: 2,
		HealthyThreshold:   2,
	}, discardLogger(), func(name string, healthy bool) {
		if !healthy {
			atomic.AddInt32(&transitions, 1)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	checker.Run(ctx)

	if b.IsAlive() {
		t.Error("expected backend to be marked unhealthy after repeated failures")
	}
	if atomic.LoadInt32(&transitions) == 0 {
		t.Error("expected onStatusChange callback to fire for the unhealthy transition")
	}
}

func TestChecker_KeepsHealthyBackendAlive(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	b, _ := backend.New("b1", srv.URL, 1)
	pool := backend.NewPool([]*backend.Backend{b})

	checker := New(pool, Config{
		Path:               "/health",
		Interval:           10 * time.Millisecond,
		Timeout:            100 * time.Millisecond,
		UnhealthyThreshold: 2,
		HealthyThreshold:   2,
	}, discardLogger(), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	checker.Run(ctx)

	if !b.IsAlive() {
		t.Error("expected backend to remain healthy")
	}
}

func TestChecker_RecoversAfterHealthyThreshold(t *testing.T) {
	var healthy atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if healthy.Load() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()

	b, _ := backend.New("b1", srv.URL, 1)
	pool := backend.NewPool([]*backend.Backend{b})

	checker := New(pool, Config{
		Path:               "/health",
		Interval:           5 * time.Millisecond,
		Timeout:            200 * time.Millisecond,
		UnhealthyThreshold: 1,
		HealthyThreshold:   2,
	}, discardLogger(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	waitFor(t, time.Second, func() bool { return !b.IsAlive() })

	healthy.Store(true)
	waitFor(t, time.Second, func() bool { return b.IsAlive() })
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}
