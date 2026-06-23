package balancer

import (
	"sync/atomic"

	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/backend"
)

// WeightedRoundRobin distributes requests across healthy backends in
// proportion to their configured weight.
type WeightedRoundRobin struct {
	counter atomic.Uint64
}

// NewWeightedRoundRobin creates a weighted round-robin balancer.
func NewWeightedRoundRobin() *WeightedRoundRobin {
	return &WeightedRoundRobin{}
}

func (w *WeightedRoundRobin) Name() string { return "weighted_round_robin" }

func (w *WeightedRoundRobin) Next(pool *backend.Pool) (*backend.Backend, error) {
	healthy := pool.Healthy()
	if len(healthy) == 0 {
		return nil, ErrNoHealthyBackends
	}

	totalWeight := 0
	for _, b := range healthy {
		totalWeight += b.Weight
	}
	if totalWeight <= 0 {
		idx := w.counter.Add(1) - 1
		return healthy[idx%uint64(len(healthy))], nil
	}

	idx := w.counter.Add(1) - 1
	pos := idx % uint64(totalWeight)

	cumulative := 0
	for _, b := range healthy {
		cumulative += b.Weight
		if pos < uint64(cumulative) {
			return b, nil
		}
	}
	return healthy[len(healthy)-1], nil
}
