package balancer

import "testing"

func TestLeastConn_PicksFewestActiveConnections(t *testing.T) {
	b1 := newTestBackend(t, "b1", "http://localhost:9001", 1, true)
	b2 := newTestBackend(t, "b2", "http://localhost:9002", 1, true)
	b3 := newTestBackend(t, "b3", "http://localhost:9003", 1, true)

	b1.IncConnections()
	b1.IncConnections()
	b2.IncConnections()

	pool := newPoolFromBackends(b1, b2, b3)

	lc := NewLeastConn()
	got, err := lc.Next(pool)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if got.Name != "b3" {
		t.Errorf("expected b3 (0 active connections), got %s", got.Name)
	}
}

func TestLeastConn_NoHealthyBackends(t *testing.T) {
	b1 := newTestBackend(t, "b1", "http://localhost:9001", 1, false)
	pool := newPoolFromBackends(b1)

	lc := NewLeastConn()
	if _, err := lc.Next(pool); err != ErrNoHealthyBackends {
		t.Errorf("expected ErrNoHealthyBackends, got %v", err)
	}
}
