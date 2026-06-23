package balancer

import (
	"sync/atomic"

	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/backend"
)

// RoundRobin cycles through healthy backends in order.
type RoundRobin struct {
	counter atomic.Uint64
}

// NewRoundRobin creates a round-robin balancer.
func NewRoundRobin() *RoundRobin {
	return &RoundRobin{}
}

func (r *RoundRobin) Name() string { return "round_robin" }

func (r *RoundRobin) Next(pool *backend.Pool) (*backend.Backend, error) {
	healthy := pool.Healthy()
	if len(healthy) == 0 {
		return nil, ErrNoHealthyBackends
	}
	idx := r.counter.Add(1) - 1
	return healthy[idx%uint64(len(healthy))], nil
}
