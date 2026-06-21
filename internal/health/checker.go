// Package health implements active health checking of backend servers.
package health

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/backend"
)

// Checker periodically probes every backend in a pool and flips its alive
// state once consecutive successes or failures cross the configured
// thresholds.
type Checker struct {
	pool               *backend.Pool
	path               string
	interval           time.Duration
	timeout            time.Duration
	unhealthyThreshold int
	healthyThreshold   int
	logger             *slog.Logger
	onStatusChange     func(name string, healthy bool)

	client *http.Client

	mu     sync.Mutex
	counts map[string]int // consecutive successes (positive) or failures (negative)
}

// Config holds the tunables for a Checker.
type Config struct {
	Path               string
	Interval           time.Duration
	Timeout            time.Duration
	UnhealthyThreshold int
	HealthyThreshold   int
}

// New creates a health Checker for the given pool.
func New(pool *backend.Pool, cfg Config, logger *slog.Logger, onStatusChange func(name string, healthy bool)) *Checker {
	if cfg.UnhealthyThreshold <= 0 {
		cfg.UnhealthyThreshold = 1
	}
	if cfg.HealthyThreshold <= 0 {
		cfg.HealthyThreshold = 1
	}
	return &Checker{
		pool:               pool,
		path:               cfg.Path,
		interval:           cfg.Interval,
		timeout:            cfg.Timeout,
		unhealthyThreshold: cfg.UnhealthyThreshold,
		healthyThreshold:   cfg.HealthyThreshold,
		logger:             logger,
		onStatusChange:     onStatusChange,
		client:             &http.Client{Timeout: cfg.Timeout},
		counts:             make(map[string]int),
	}
}

// Run blocks, probing every backend on a fixed interval until ctx is
// canceled. An initial probe runs immediately so backend state is known
// before the first request arrives.
func (c *Checker) Run(ctx context.Context) {
	c.checkAll(ctx)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.checkAll(ctx)
		}
	}
}

func (c *Checker) checkAll(ctx context.Context) {
	var wg sync.WaitGroup
	for _, b := range c.pool.Backends() {
		wg.Add(1)
		go func(b *backend.Backend) {
			defer wg.Done()
			c.check(ctx, b)
		}(b)
	}
	wg.Wait()
}

func (c *Checker) check(ctx context.Context, b *backend.Backend) {
	checkCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	url := b.URL.String() + c.path
	req, err := http.NewRequestWithContext(checkCtx, http.MethodGet, url, nil)
	if err != nil {
		c.recordFailure(b, err.Error())
		return
	}

	start := time.Now()
	resp, err := c.client.Do(req)
	latency := time.Since(start)

	if err != nil {
		c.recordFailure(b, err.Error())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		c.recordSuccess(b, latency)
	} else {
		c.recordFailure(b, "unexpected status "+resp.Status)
	}
}

func (c *Checker) recordSuccess(b *backend.Backend, latency time.Duration) {
	c.mu.Lock()
	if c.counts[b.Name] < 0 {
		c.counts[b.Name] = 0
	}
	c.counts[b.Name]++
	consecutive := c.counts[b.Name]
	c.mu.Unlock()

	wasAlive := b.IsAlive()
	if consecutive >= c.healthyThreshold {
		b.SetAlive(true)
	}

	if !wasAlive && b.IsAlive() {
		c.logger.Info("backend recovered", "backend", b.Name, "url", b.URL.String(), "latency", latency.String())
	} else {
		c.logger.Debug("backend healthy", "backend", b.Name, "url", b.URL.String(), "latency", latency.String())
	}
	if c.onStatusChange != nil {
		c.onStatusChange(b.Name, b.IsAlive())
	}
}

func (c *Checker) recordFailure(b *backend.Backend, reason string) {
	c.mu.Lock()
	if c.counts[b.Name] > 0 {
		c.counts[b.Name] = 0
	}
	c.counts[b.Name]--
	consecutive := -c.counts[b.Name]
	c.mu.Unlock()

	wasAlive := b.IsAlive()
	if consecutive >= c.unhealthyThreshold {
		b.SetAlive(false)
	}

	if wasAlive && !b.IsAlive() {
		c.logger.Warn("backend marked unhealthy", "backend", b.Name, "url", b.URL.String(), "error", reason)
	} else {
		c.logger.Debug("backend check failed", "backend", b.Name, "url", b.URL.String(), "error", reason)
	}
	if c.onStatusChange != nil {
		c.onStatusChange(b.Name, b.IsAlive())
	}
}
