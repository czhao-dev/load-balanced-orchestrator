package balancer

import "testing"

func TestWeightedRoundRobin_DistributesByWeight(t *testing.T) {
	b1 := newTestBackend(t, "b1", "http://localhost:9001", 3, true)
	b2 := newTestBackend(t, "b2", "http://localhost:9002", 1, true)
	pool := newPoolFromBackends(b1, b2)

	w := NewWeightedRoundRobin()
	counts := map[string]int{}
	const n = 400
	for i := 0; i < n; i++ {
		b, err := w.Next(pool)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		counts[b.Name]++
	}

	gotRatio := float64(counts["b1"]) / float64(counts["b2"])
	wantRatio := 3.0
	if gotRatio < wantRatio-0.2 || gotRatio > wantRatio+0.2 {
		t.Errorf("expected b1:b2 ratio close to %.1f, got %.2f (b1=%d, b2=%d)", wantRatio, gotRatio, counts["b1"], counts["b2"])
	}
}
