// Package balancer implements backend-selection strategies used by the
// reverse proxy to pick which upstream server should serve a request.
package balancer

import (
	"errors"

	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/backend"
)

// ErrNoHealthyBackends is returned when no backend in the pool is alive.
var ErrNoHealthyBackends = errors.New("no healthy backends available")

// Balancer selects a backend from a pool for a given request.
type Balancer interface {
	// Next returns the next backend to use, or ErrNoHealthyBackends if
	// none are currently healthy.
	Next(pool *backend.Pool) (*backend.Backend, error)

	// Name identifies the strategy, e.g. for logging.
	Name() string
}

// New constructs a Balancer for the given strategy name.
func New(strategy string) (Balancer, error) {
	switch strategy {
	case "", "round_robin":
		return NewRoundRobin(), nil
	case "least_conn":
		return NewLeastConn(), nil
	case "weighted_round_robin":
		return NewWeightedRoundRobin(), nil
	default:
		return nil, errors.New("unknown load balancer strategy: " + strategy)
	}
}
