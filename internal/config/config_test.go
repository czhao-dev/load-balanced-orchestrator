package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTempConfig(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

func TestLoad_ParsesDurationsAndDefaults(t *testing.T) {
	path := writeTempConfig(t, `
load_balancer:
  strategy: "least_conn"

backends:
  - name: "b1"
    url: "http://localhost:9001"
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Server.ListenAddr != ":8080" {
		t.Errorf("expected default listen_addr, got %q", cfg.Server.ListenAddr)
	}
	if time.Duration(cfg.HealthCheck.Interval) != 2*time.Second {
		t.Errorf("expected default health check interval of 2s, got %s", time.Duration(cfg.HealthCheck.Interval))
	}
	if cfg.LoadBalancer.Strategy != "least_conn" {
		t.Errorf("expected configured strategy to be preserved, got %q", cfg.LoadBalancer.Strategy)
	}
	if cfg.Backends[0].Weight != 1 {
		t.Errorf("expected default backend weight of 1, got %d", cfg.Backends[0].Weight)
	}
}

func TestLoad_ExplicitDurations(t *testing.T) {
	path := writeTempConfig(t, `
health_check:
  interval: "10s"
  timeout: "250ms"

backends:
  - name: "b1"
    url: "http://localhost:9001"
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if time.Duration(cfg.HealthCheck.Interval) != 10*time.Second {
		t.Errorf("expected interval 10s, got %s", time.Duration(cfg.HealthCheck.Interval))
	}
	if time.Duration(cfg.HealthCheck.Timeout) != 250*time.Millisecond {
		t.Errorf("expected timeout 250ms, got %s", time.Duration(cfg.HealthCheck.Timeout))
	}
}

func TestLoad_RejectsNoBackends(t *testing.T) {
	path := writeTempConfig(t, `
server:
  listen_addr: ":8080"
`)

	if _, err := Load(path); err == nil {
		t.Error("expected error when no backends are configured")
	}
}

func TestLoad_RejectsUnknownStrategy(t *testing.T) {
	path := writeTempConfig(t, `
load_balancer:
  strategy: "magic"

backends:
  - name: "b1"
    url: "http://localhost:9001"
`)

	if _, err := Load(path); err == nil {
		t.Error("expected error for unknown load balancer strategy")
	}
}

func TestLoad_MissingFile(t *testing.T) {
	if _, err := Load("/nonexistent/config.yaml"); err == nil {
		t.Error("expected error for missing config file")
	}
}
