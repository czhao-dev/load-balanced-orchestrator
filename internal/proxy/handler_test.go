package proxy

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/backend"
	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/balancer"
	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/metrics"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestHandler_ForwardsRequestAndStreamsResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/hello" {
			t.Errorf("expected path /api/hello, got %s", r.URL.Path)
		}
		if r.Header.Get("X-Forwarded-For") == "" {
			t.Error("expected X-Forwarded-For header to be set")
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello"))
	}))
	defer upstream.Close()

	b, _ := backend.New("b1", upstream.URL, 1)
	pool := backend.NewPool([]*backend.Backend{b})
	rr := balancer.NewRoundRobin()
	h := New(pool, rr, metrics.New(), discardLogger(), Config{MaxRetries: 0, BackendTimeout: time.Second})

	req := httptest.NewRequest(http.MethodGet, "/api/hello", nil)
	req.RemoteAddr = "203.0.113.1:54321"
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if w.Body.String() != "hello" {
		t.Errorf("expected body %q, got %q", "hello", w.Body.String())
	}
}

func TestHandler_RetriesAgainstHealthyBackendOnFailure(t *testing.T) {
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("from healthy backend"))
	}))
	defer healthy.Close()

	// down points at a closed listener so the dial fails immediately.
	down, _ := backend.New("down", "http://127.0.0.1:1", 1)
	up, _ := backend.New("up", healthy.URL, 1)

	pool := backend.NewPool([]*backend.Backend{down, up})
	rr := balancer.NewRoundRobin()
	h := New(pool, rr, metrics.New(), discardLogger(), Config{MaxRetries: 1, BackendTimeout: time.Second})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected successful retry to return 200, got %d: %s", w.Code, w.Body.String())
	}
	if w.Body.String() != "from healthy backend" {
		t.Errorf("expected response from healthy backend, got %q", w.Body.String())
	}
}

func TestHandler_NoHealthyBackendsReturns503(t *testing.T) {
	b, _ := backend.New("b1", "http://127.0.0.1:1", 1)
	b.SetAlive(false)
	pool := backend.NewPool([]*backend.Backend{b})
	rr := balancer.NewRoundRobin()
	h := New(pool, rr, metrics.New(), discardLogger(), Config{MaxRetries: 1, BackendTimeout: time.Second})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}

func TestHandler_AllBackendsDownReturnsBadGateway(t *testing.T) {
	b, _ := backend.New("b1", "http://127.0.0.1:1", 1)
	pool := backend.NewPool([]*backend.Backend{b})
	rr := balancer.NewRoundRobin()
	h := New(pool, rr, metrics.New(), discardLogger(), Config{MaxRetries: 0, BackendTimeout: time.Second})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadGateway {
		t.Errorf("expected 502, got %d", w.Code)
	}
}

func TestHandler_BackendTimeoutReturnsGatewayTimeout(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer slow.Close()

	b, _ := backend.New("b1", slow.URL, 1)
	pool := backend.NewPool([]*backend.Backend{b})
	rr := balancer.NewRoundRobin()
	h := New(pool, rr, metrics.New(), discardLogger(), Config{MaxRetries: 0, BackendTimeout: 5 * time.Millisecond})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusGatewayTimeout {
		t.Errorf("expected 504, got %d", w.Code)
	}
}

func TestHandler_DoesNotRetryNonIdempotentMethod(t *testing.T) {
	var calls int
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
	}))
	defer healthy.Close()

	down, _ := backend.New("down", "http://127.0.0.1:1", 1)
	pool := backend.NewPool([]*backend.Backend{down})
	rr := balancer.NewRoundRobin()
	h := New(pool, rr, metrics.New(), discardLogger(), Config{MaxRetries: 2, BackendTimeout: time.Second})

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadGateway {
		t.Errorf("expected 502 for failed POST with no retry, got %d", w.Code)
	}
}
