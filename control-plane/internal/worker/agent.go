// Package worker implements the node AGENT's client-side logic: register
// with the control plane as a Node, heartbeat, poll for assigned Pods,
// execute them as subprocesses or Docker containers, and report status back.
// This is distinct from internal/model.Node, which is the control plane's
// server-side record of a registered node.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	dockerclient "github.com/docker/docker/client"

	"github.com/czhao-dev/control-plane/internal/agentmetrics"
	"github.com/czhao-dev/control-plane/internal/model"
)

// Config holds the node agent's runtime configuration.
type Config struct {
	ControlPlaneURL   string
	Hostname          string
	Address           string
	CPU               float64
	MemoryMB          int
	MaxConcurrentJobs int
	HeartbeatInterval time.Duration
	PollInterval      time.Duration
	ShutdownTimeout   time.Duration
	// DockerHost is the Docker daemon socket (e.g. "unix:///var/run/docker.sock").
	// Empty string falls back to the DOCKER_HOST env var or Docker's default socket.
	DockerHost string
}

// Agent is a node agent process: it registers with the control plane,
// heartbeats, polls for assigned pods, and executes them.
type Agent struct {
	cfg    Config
	client *http.Client
	docker *dockerclient.Client
	logger *slog.Logger

	id  string
	sem chan struct{}
	wg  sync.WaitGroup
}

// New creates an Agent. Returns an error only if the Docker client cannot be
// constructed (e.g. malformed DockerHost URL). The Docker daemon does not need
// to be running at construction time — connection errors surface at first use.
func New(cfg Config, logger *slog.Logger) (*Agent, error) {
	opts := []dockerclient.Opt{dockerclient.WithAPIVersionNegotiation()}
	if cfg.DockerHost != "" {
		opts = append(opts, dockerclient.WithHost(cfg.DockerHost))
	} else {
		opts = append(opts, dockerclient.FromEnv)
	}
	cli, err := dockerclient.NewClientWithOpts(opts...)
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}
	return &Agent{
		cfg:    cfg,
		client: &http.Client{Timeout: 10 * time.Second},
		docker: cli,
		logger: logger,
		sem:    make(chan struct{}, max(cfg.MaxConcurrentJobs, 1)),
	}, nil
}

// Close releases resources held by the agent (primarily the Docker client).
func (a *Agent) Close() {
	if a.docker != nil {
		a.docker.Close()
	}
}

// Run registers with the control plane, then blocks running the heartbeat
// and poll loops until ctx is cancelled, at which point it stops polling
// immediately and waits (up to ShutdownTimeout) for in-flight pods to finish.
func (a *Agent) Run(ctx context.Context) error {
	if err := a.register(ctx); err != nil {
		return fmt.Errorf("register: %w", err)
	}
	a.logger.Info("node agent registered", "node_id", a.id, "control_plane", a.cfg.ControlPlaneURL)

	// runCtx is deliberately NOT derived from ctx: it must stay alive after
	// ctx is cancelled so in-flight pods can run to completion during the
	// graceful-shutdown drain below.
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	var loopWG sync.WaitGroup
	loopWG.Add(2)
	go func() { defer loopWG.Done(); a.heartbeatLoop(ctx) }()
	go func() { defer loopWG.Done(); a.pollLoop(ctx, runCtx) }()

	<-ctx.Done()
	a.logger.Info("shutdown signal received, draining in-flight pods", "timeout", a.cfg.ShutdownTimeout)

	drained := make(chan struct{})
	go func() { a.wg.Wait(); close(drained) }()

	select {
	case <-drained:
	case <-time.After(a.cfg.ShutdownTimeout):
		a.logger.Warn("shutdown timeout exceeded, cancelling in-flight pods")
		runCancel()
		<-drained
	}

	loopWG.Wait()
	a.logger.Info("node agent stopped", "node_id", a.id)
	return nil
}

type registerRequest struct {
	Hostname      string                 `json:"hostname"`
	Address       string                 `json:"address"`
	Capacity      model.ResourceCapacity `json:"capacity"`
	MaxConcurrent int                    `json:"max_concurrent_jobs"`
}

func (a *Agent) register(ctx context.Context) error {
	body, _ := json.Marshal(registerRequest{
		Hostname:      a.cfg.Hostname,
		Address:       a.cfg.Address,
		Capacity:      model.ResourceCapacity{CPU: a.cfg.CPU, MemoryMB: a.cfg.MemoryMB},
		MaxConcurrent: a.cfg.MaxConcurrentJobs,
	})

	var node model.Node
	if err := a.post(ctx, "/api/v1/nodes/register", body, http.StatusCreated, &node); err != nil {
		return err
	}
	a.id = node.ID
	return nil
}

func (a *Agent) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(a.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.sendHeartbeat(ctx)
		}
	}
}

type heartbeatRequest struct {
	RunningJobs int                    `json:"running_jobs"`
	Available   model.ResourceCapacity `json:"available"`
}

func (a *Agent) sendHeartbeat(ctx context.Context) {
	running := len(a.sem)
	body, _ := json.Marshal(heartbeatRequest{RunningJobs: running})

	start := time.Now()
	err := a.post(ctx, "/api/v1/nodes/"+a.id+"/heartbeat", body, http.StatusOK, nil)
	agentmetrics.WorkerHeartbeatLatencySeconds.Observe(time.Since(start).Seconds())
	if err != nil {
		a.logger.Warn("heartbeat failed", "error", err)
	}
}

func (a *Agent) pollLoop(ctx, runCtx context.Context) {
	ticker := time.NewTicker(a.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.pollOnce(ctx, runCtx)
		}
	}
}

type pollResponse struct {
	Pod *model.Pod `json:"pod"`
}

func (a *Agent) pollOnce(ctx, runCtx context.Context) {
	select {
	case a.sem <- struct{}{}:
	default:
		return // already at max concurrency; try again next tick
	}

	var resp pollResponse
	if err := a.get(ctx, "/api/v1/nodes/"+a.id+"/pods/poll", &resp); err != nil {
		<-a.sem
		a.logger.Warn("poll failed", "error", err)
		return
	}
	if resp.Pod == nil {
		<-a.sem
		return
	}

	pod := *resp.Pod
	a.wg.Add(1)
	agentmetrics.WorkerRunningJobs.Set(float64(len(a.sem)))
	go func() {
		defer a.wg.Done()
		defer func() { <-a.sem; agentmetrics.WorkerRunningJobs.Set(float64(len(a.sem))) }()
		a.runPod(runCtx, pod)
	}()
}

func (a *Agent) post(ctx context.Context, path string, body []byte, wantStatus int, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.ControlPlaneURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return a.do(req, wantStatus, out)
}

func (a *Agent) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.cfg.ControlPlaneURL+path, nil)
	if err != nil {
		return err
	}
	return a.do(req, http.StatusOK, out)
}

func (a *Agent) do(req *http.Request, wantStatus int, out any) error {
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s %s: %s: %s", req.Method, req.URL.Path, resp.Status, string(b))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
