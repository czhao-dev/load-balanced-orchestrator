package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/czhao-dev/control-plane/internal/logging"
	"github.com/czhao-dev/control-plane/internal/worker"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	logger := logging.New(envString("WORKER_LOG_LEVEL", "info"), envString("WORKER_LOG_FORMAT", "json"))
	slog.SetDefault(logger)

	hostname, _ := os.Hostname()
	metricsPort := envInt("WORKER_METRICS_PORT", 9100)

	cfg := worker.Config{
		ControlPlaneURL:   envString("WORKER_CONTROL_PLANE_URL", "http://localhost:7070"),
		Hostname:          envString("WORKER_HOSTNAME", hostname),
		Address:           envString("WORKER_ADDRESS", fmt.Sprintf("http://localhost:%d", metricsPort)),
		CPU:               envFloat("WORKER_CPU", 2),
		MemoryMB:          envInt("WORKER_MEMORY_MB", 2048),
		MaxConcurrentJobs: envInt("WORKER_MAX_CONCURRENT_JOBS", 4),
		HeartbeatInterval: envDuration("WORKER_HEARTBEAT_INTERVAL", 5*time.Second),
		PollInterval:      envDuration("WORKER_POLL_INTERVAL", 1*time.Second),
		ShutdownTimeout:   envDuration("WORKER_SHUTDOWN_TIMEOUT", 10*time.Second),
		DockerHost:        envString("WORKER_DOCKER_HOST", ""),
	}

	agent, err := worker.New(cfg, logger)
	if err != nil {
		logger.Error("failed to initialize worker agent", "error", err)
		os.Exit(1)
	}
	defer agent.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Minimal HTTP listener purely for Prometheus scraping, container
	// healthchecks, and as the address the dynamic-discovery proxy forwards
	// traffic to in the proxy-failover demo. The worker is a client agent,
	// not a job-serving HTTP service, so this listener exists only for
	// observability/demo purposes, not for receiving dispatched jobs (those
	// arrive via polling, see internal/worker/agent.go).
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"ready"}`))
	})
	mux.Handle("/metrics", promhttp.Handler())
	metricsSrv := &http.Server{Addr: fmt.Sprintf(":%d", metricsPort), Handler: mux}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics server error", "error", err)
		}
	}()

	logger.Info("worker starting",
		"control_plane", cfg.ControlPlaneURL,
		"address", cfg.Address,
		"max_concurrent_jobs", cfg.MaxConcurrentJobs,
		"metrics_port", metricsPort,
	)

	if err := agent.Run(ctx); err != nil {
		logger.Error("worker exited with error", "error", err)
		os.Exit(1)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
}

func envString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envFloat(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return n
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
