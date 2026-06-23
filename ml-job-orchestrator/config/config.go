package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port            int
	NumWorkers      int
	QueueSize       int
	MaxJobHistory   int
	ShutdownTimeout time.Duration
}

func Load() Config {
	return Config{
		Port:            envInt("MLORCH_PORT", 8080),
		NumWorkers:      envInt("MLORCH_NUM_WORKERS", 4),
		QueueSize:       envInt("MLORCH_QUEUE_SIZE", 100),
		MaxJobHistory:   envInt("MLORCH_MAX_JOB_HISTORY", 1000),
		ShutdownTimeout: envDuration("MLORCH_SHUTDOWN_TIMEOUT", 30*time.Second),
	}
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
