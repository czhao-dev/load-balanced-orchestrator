package balancer

import "github.com/czhao-dev/reverse-proxy-load-balancer/internal/backend"

// LeastConn selects the healthy backend with the fewest active connections.
type LeastConn struct{}

// NewLeastConn creates a least-connections balancer.
func NewLeastConn() *LeastConn {
	return &LeastConn{}
}

func (l *LeastConn) Name() string { return "least_conn" }

func (l *LeastConn) Next(pool *backend.Pool) (*backend.Backend, error) {
	healthy := pool.Healthy()
	if len(healthy) == 0 {
		return nil, ErrNoHealthyBackends
	}

	best := healthy[0]
	for _, b := range healthy[1:] {
		if b.ActiveConnections() < best.ActiveConnections() {
			best = b
		}
	}
	return best, nil
}
