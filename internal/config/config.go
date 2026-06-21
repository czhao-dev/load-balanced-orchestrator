// Package config loads and validates the proxy's YAML configuration.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration for the proxy.
type Config struct {
	Server       ServerConfig       `yaml:"server"`
	LoadBalancer LoadBalancerConfig `yaml:"load_balancer"`
	HealthCheck  HealthCheckConfig  `yaml:"health_check"`
	Backends     []BackendConfig    `yaml:"backends"`
	Logging      LoggingConfig      `yaml:"logging"`
	Metrics      MetricsConfig      `yaml:"metrics"`
}

type ServerConfig struct {
	ListenAddr      string   `yaml:"listen_addr"`
	ReadTimeout     Duration `yaml:"read_timeout"`
	WriteTimeout    Duration `yaml:"write_timeout"`
	ShutdownTimeout Duration `yaml:"shutdown_timeout"`
}

type LoadBalancerConfig struct {
	Strategy       string   `yaml:"strategy"`
	MaxRetries     int      `yaml:"max_retries"`
	BackendTimeout Duration `yaml:"backend_timeout"`
}

type HealthCheckConfig struct {
	Enabled            bool     `yaml:"enabled"`
	Path               string   `yaml:"path"`
	Interval           Duration `yaml:"interval"`
	Timeout            Duration `yaml:"timeout"`
	UnhealthyThreshold int      `yaml:"unhealthy_threshold"`
	HealthyThreshold   int      `yaml:"healthy_threshold"`
}

type BackendConfig struct {
	Name   string `yaml:"name"`
	URL    string `yaml:"url"`
	Weight int    `yaml:"weight"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"`
}

// Load reads and parses a YAML config file from path, applying defaults
// and validating required fields.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	applyDefaults(&cfg)

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}

func applyDefaults(cfg *Config) {
	if cfg.Server.ListenAddr == "" {
		cfg.Server.ListenAddr = ":8080"
	}
	if cfg.Server.ReadTimeout == 0 {
		cfg.Server.ReadTimeout = Duration(5 * time.Second)
	}
	if cfg.Server.WriteTimeout == 0 {
		cfg.Server.WriteTimeout = Duration(10 * time.Second)
	}
	if cfg.Server.ShutdownTimeout == 0 {
		cfg.Server.ShutdownTimeout = Duration(5 * time.Second)
	}

	if cfg.LoadBalancer.Strategy == "" {
		cfg.LoadBalancer.Strategy = "round_robin"
	}
	if cfg.LoadBalancer.BackendTimeout == 0 {
		cfg.LoadBalancer.BackendTimeout = Duration(2 * time.Second)
	}

	if cfg.HealthCheck.Path == "" {
		cfg.HealthCheck.Path = "/health"
	}
	if cfg.HealthCheck.Interval == 0 {
		cfg.HealthCheck.Interval = Duration(2 * time.Second)
	}
	if cfg.HealthCheck.Timeout == 0 {
		cfg.HealthCheck.Timeout = Duration(500 * time.Millisecond)
	}
	if cfg.HealthCheck.UnhealthyThreshold == 0 {
		cfg.HealthCheck.UnhealthyThreshold = 2
	}
	if cfg.HealthCheck.HealthyThreshold == 0 {
		cfg.HealthCheck.HealthyThreshold = 2
	}

	for i := range cfg.Backends {
		if cfg.Backends[i].Weight == 0 {
			cfg.Backends[i].Weight = 1
		}
	}

	if cfg.Logging.Level == "" {
		cfg.Logging.Level = "info"
	}
	if cfg.Logging.Format == "" {
		cfg.Logging.Format = "json"
	}

	if cfg.Metrics.Path == "" {
		cfg.Metrics.Path = "/metrics"
	}
}

// Validate checks that the configuration is usable.
func (c *Config) Validate() error {
	if len(c.Backends) == 0 {
		return fmt.Errorf("at least one backend must be configured")
	}
	for _, b := range c.Backends {
		if b.Name == "" {
			return fmt.Errorf("backend missing name")
		}
		if b.URL == "" {
			return fmt.Errorf("backend %q missing url", b.Name)
		}
	}
	switch c.LoadBalancer.Strategy {
	case "round_robin", "least_conn", "weighted_round_robin":
	default:
		return fmt.Errorf("unsupported load_balancer.strategy %q", c.LoadBalancer.Strategy)
	}
	return nil
}
