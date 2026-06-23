package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/czhao-dev/ml-job-orchestrator/config"
	"github.com/czhao-dev/ml-job-orchestrator/internal/api"
	"github.com/czhao-dev/ml-job-orchestrator/internal/executor"
	"github.com/czhao-dev/ml-job-orchestrator/internal/model"
	"github.com/czhao-dev/ml-job-orchestrator/internal/scheduler"
	"github.com/czhao-dev/ml-job-orchestrator/internal/store"
	"github.com/czhao-dev/ml-job-orchestrator/internal/worker"
)

func main() {
	cfg := config.Load()

	// CLI flags take precedence over env vars when both are set.
	port := flag.Int("port", cfg.Port, "HTTP port to listen on")
	workers := flag.Int("workers", cfg.NumWorkers, "number of worker goroutines")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	flag.Parse()
	cfg.Port = *port
	cfg.NumWorkers = *workers

	setLogLevel(*logLevel)

	st := store.New()
	queue := make(chan model.Job, cfg.QueueSize)
	cancels := &sync.Map{}
	exec := executor.New(st, cancels)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := worker.New(ctx, cfg.NumWorkers, queue, exec, cfg.ShutdownTimeout)
	sched := scheduler.New(st, queue)
	go sched.Run(ctx)

	handlers := api.NewHandlers(st, queue, cancels)
	srv := api.NewServer(fmt.Sprintf(":%d", cfg.Port), handlers)

	go func() {
		slog.Info("orchestrator starting", "port", cfg.Port, "workers", cfg.NumWorkers, "queue_size", cfg.QueueSize)
		if err := srv.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server error", "error", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("server shutdown error", "error", err)
	}
	pool.Shutdown()
	slog.Info("orchestrator stopped")
}

func setLogLevel(level string) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	slog.SetLogLoggerLevel(lvl)
}
