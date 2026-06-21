package balancer

import (
	"testing"

	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/backend"
)

func newTestBackend(t *testing.T, name, rawURL string, weight int, alive bool) *backend.Backend {
	t.Helper()
	b, err := backend.New(name, rawURL, weight)
	if err != nil {
		t.Fatalf("backend.New: %v", err)
	}
	b.SetAlive(alive)
	return b
}

func newPoolFromBackends(backends ...*backend.Backend) *backend.Pool {
	return backend.NewPool(backends)
}

func TestRoundRobin_DistributesEvenly(t *testing.T) {
	pool := backend.NewPool([]*backend.Backend{
		newTestBackend(t, "b1", "http://localhost:9001", 1, true),
		newTestBackend(t, "b2", "http://localhost:9002", 1, true),
		newTestBackend(t, "b3", "http://localhost:9003", 1, true),
	})

	rr := NewRoundRobin()
	var got []string
	for i := 0; i < 6; i++ {
		b, err := rr.Next(pool)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		got = append(got, b.Name)
	}

	want := []string{"b1", "b2", "b3", "b1", "b2", "b3"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("request %d: got %s, want %s", i, got[i], want[i])
		}
	}
}

func TestRoundRobin_SkipsUnhealthy(t *testing.T) {
	pool := backend.NewPool([]*backend.Backend{
		newTestBackend(t, "b1", "http://localhost:9001", 1, true),
		newTestBackend(t, "b2", "http://localhost:9002", 1, false),
	})

	rr := NewRoundRobin()
	for i := 0; i < 4; i++ {
		b, err := rr.Next(pool)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b.Name != "b1" {
			t.Errorf("expected only healthy backend b1, got %s", b.Name)
		}
	}
}

func TestRoundRobin_NoHealthyBackends(t *testing.T) {
	pool := backend.NewPool([]*backend.Backend{
		newTestBackend(t, "b1", "http://localhost:9001", 1, false),
	})

	rr := NewRoundRobin()
	if _, err := rr.Next(pool); err != ErrNoHealthyBackends {
		t.Errorf("expected ErrNoHealthyBackends, got %v", err)
	}
}
