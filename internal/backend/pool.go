package backend

// Pool is a fixed set of backends shared by the load balancer and the
// health checker. The slice itself never changes after construction, so
// reads of Backends() require no locking; only each Backend's own atomic
// fields mutate at runtime.
type Pool struct {
	backends []*Backend
}

// NewPool creates a Pool from the given backends.
func NewPool(backends []*Backend) *Pool {
	return &Pool{backends: backends}
}

// Backends returns all backends in the pool, healthy or not.
func (p *Pool) Backends() []*Backend {
	return p.backends
}

// Healthy returns the subset of backends currently marked alive.
func (p *Pool) Healthy() []*Backend {
	healthy := make([]*Backend, 0, len(p.backends))
	for _, b := range p.backends {
		if b.IsAlive() {
			healthy = append(healthy, b)
		}
	}
	return healthy
}
