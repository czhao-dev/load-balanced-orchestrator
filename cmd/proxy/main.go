// Command proxy runs the reverse proxy and load balancer described by a
// YAML configuration file.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/admin"
	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/backend"
	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/balancer"
	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/config"
	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/health"
	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/logging"
	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/metrics"
	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/proxy"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML configuration file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	logger := logging.New(cfg.Logging.Level, cfg.Logging.Format)
	slog.SetDefault(logger)

	backends := make([]*backend.Backend, 0, len(cfg.Backends))
	for _, bc := range cfg.Backends {
		b, err := backend.New(bc.Name, bc.URL, bc.Weight)
		if err != nil {
			logger.Error("invalid backend url", "backend", bc.Name, "url", bc.URL, "error", err)
			os.Exit(1)
		}
		backends = append(backends, b)
	}
	pool := backend.NewPool(backends)

	lb, err := balancer.New(cfg.LoadBalancer.Strategy)
	if err != nil {
		logger.Error("failed to create load balancer", "error", err)
		os.Exit(1)
	}

	m := metrics.New()

	proxyHandler := proxy.New(pool, lb, m, logger, proxy.Config{
		MaxRetries:     cfg.LoadBalancer.MaxRetries,
		BackendTimeout: time.Duration(cfg.LoadBalancer.BackendTimeout),
	})

	mux := http.NewServeMux()
	mux.Handle("/", proxyHandler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/admin/backends", admin.BackendsHandler(pool))
	if cfg.Metrics.Enabled {
		mux.HandleFunc(cfg.Metrics.Path, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			w.Write([]byte(m.Render()))
		})
	}

	srv := &http.Server{
		Addr:         cfg.Server.ListenAddr,
		Handler:      mux,
		ReadTimeout:  time.Duration(cfg.Server.ReadTimeout),
		WriteTimeout: time.Duration(cfg.Server.WriteTimeout),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if cfg.HealthCheck.Enabled {
		checker := health.New(pool, health.Config{
			Path:               cfg.HealthCheck.Path,
			Interval:           time.Duration(cfg.HealthCheck.Interval),
			Timeout:            time.Duration(cfg.HealthCheck.Timeout),
			UnhealthyThreshold: cfg.HealthCheck.UnhealthyThreshold,
			HealthyThreshold:   cfg.HealthCheck.HealthyThreshold,
		}, logger, m.SetBackendHealth)
		go checker.Run(ctx)
	} else {
		for _, b := range backends {
			m.SetBackendHealth(b.Name, true)
		}
	}

	go func() {
		logger.Info("proxy listening", "addr", cfg.Server.ListenAddr, "strategy", lb.Name(), "backends", len(backends))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	logger.Info("shutting down")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Server.ShutdownTimeout))
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
		os.Exit(1)
	}
	logger.Info("shutdown complete")
}
