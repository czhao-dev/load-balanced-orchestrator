package backend

import "testing"

func TestPool_Healthy(t *testing.T) {
	b1, _ := New("b1", "http://localhost:9001", 1)
	b2, _ := New("b2", "http://localhost:9002", 1)
	b2.SetAlive(false)

	pool := NewPool([]*Backend{b1, b2})

	healthy := pool.Healthy()
	if len(healthy) != 1 || healthy[0].Name != "b1" {
		t.Errorf("expected only b1 to be healthy, got %v", healthy)
	}

	if len(pool.Backends()) != 2 {
		t.Errorf("expected Backends() to return all backends regardless of health")
	}
}

func TestBackend_ConnectionTracking(t *testing.T) {
	b, _ := New("b1", "http://localhost:9001", 1)

	b.IncConnections()
	b.IncConnections()
	if got := b.ActiveConnections(); got != 2 {
		t.Errorf("expected 2 active connections, got %d", got)
	}

	b.DecConnections()
	if got := b.ActiveConnections(); got != 1 {
		t.Errorf("expected 1 active connection, got %d", got)
	}
}

func TestBackend_DefaultWeight(t *testing.T) {
	b, err := New("b1", "http://localhost:9001", 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if b.Weight != 1 {
		t.Errorf("expected default weight of 1, got %d", b.Weight)
	}
}

func TestBackend_InvalidURL(t *testing.T) {
	if _, err := New("b1", "http://[::1]:namedport", 1); err == nil {
		t.Error("expected error for invalid backend URL")
	}
}
