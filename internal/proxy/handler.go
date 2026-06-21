// Package proxy implements the reverse proxy HTTP handler: backend
// selection, request forwarding, retry/failover, and response streaming.
package proxy

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/backend"
	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/balancer"
	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/metrics"
)

// idempotentMethods are safe to retry against a different backend after a
// failure, since re-sending them cannot cause duplicate side effects.
var idempotentMethods = map[string]bool{
	http.MethodGet:     true,
	http.MethodHead:    true,
	http.MethodOptions: true,
}

// Handler is the top-level reverse proxy http.Handler.
type Handler struct {
	pool       *backend.Pool
	balancer   balancer.Balancer
	metrics    *metrics.Metrics
	logger     *slog.Logger
	client     *http.Client
	maxRetries int
}

// Config configures a Handler.
type Config struct {
	MaxRetries     int
	BackendTimeout time.Duration
}

// New creates a reverse proxy Handler.
func New(pool *backend.Pool, b balancer.Balancer, m *metrics.Metrics, logger *slog.Logger, cfg Config) *Handler {
	return &Handler{
		pool:     pool,
		balancer: b,
		metrics:  m,
		logger:   logger,
		client: &http.Client{
			Transport: NewTransport(),
			Timeout:   cfg.BackendTimeout,
			// A reverse proxy must forward redirects to the client as-is
			// rather than following them itself.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		maxRetries: cfg.MaxRetries,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		h.metrics.ObserveRequest(time.Since(start).Seconds())
	}()

	attempts := h.maxRetries + 1
	canRetry := idempotentMethods[r.Method]

	var lastErr error
	var lastStatus int

	for attempt := 0; attempt < attempts; attempt++ {
		b, err := h.balancer.Next(h.pool)
		if err != nil {
			h.logger.Error("no healthy backends", "error", err)
			http.Error(w, "503 Service Unavailable", http.StatusServiceUnavailable)
			return
		}

		status, err := h.proxyOnce(w, r, b)
		if err == nil {
			return
		}

		lastErr = err
		lastStatus = status

		h.logger.Warn("backend request failed", "backend", b.Name, "url", b.URL.String(), "attempt", attempt+1, "error", err)

		if !canRetry || attempt == attempts-1 {
			break
		}
		h.metrics.IncRetries()
	}

	if lastStatus != 0 {
		http.Error(w, http.StatusText(lastStatus), lastStatus)
	} else {
		h.logger.Error("proxy request failed", "error", lastErr)
		http.Error(w, "502 Bad Gateway", http.StatusBadGateway)
	}
}

// proxyOnce attempts a single forward to b. It returns a non-nil error if
// the attempt failed, along with a status code suitable for the client
// response if all retries are exhausted. Response headers are only
// written to w once the backend itself has responded, so a connection or
// timeout failure can still be retried against another backend.
func (h *Handler) proxyOnce(w http.ResponseWriter, r *http.Request, b *backend.Backend) (int, error) {
	outReq, err := newOutboundRequest(r, b)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	b.IncConnections()
	b.IncRequests()
	h.metrics.IncBackendRequests(b.Name)
	h.metrics.SetBackendActiveConnections(b.Name, b.ActiveConnections())
	defer func() {
		b.DecConnections()
		h.metrics.SetBackendActiveConnections(b.Name, b.ActiveConnections())
	}()

	resp, err := h.client.Do(outReq)
	if err != nil {
		b.IncErrors()
		h.metrics.IncBackendErrors(b.Name)
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return http.StatusGatewayTimeout, err
		}
		return http.StatusBadGateway, err
	}
	defer resp.Body.Close()

	for key, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(key, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		h.logger.Warn("error streaming response body", "backend", b.Name, "error", err)
	}
	return resp.StatusCode, nil
}
