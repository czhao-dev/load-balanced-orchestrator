package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/pkg/stdcopy"

	"github.com/czhao-dev/control-plane/internal/agentmetrics"
	"github.com/czhao-dev/control-plane/internal/model"
)

// runContainerPod executes a pod by pulling an OCI image and running it as a
// Docker container. It mirrors runSubprocessPod's status-reporting contract.
func (a *Agent) runContainerPod(ctx context.Context, pod model.Pod) {
	// Pull the image (idempotent; output discarded).
	reader, err := a.docker.ImagePull(ctx, pod.Image, image.PullOptions{})
	if err != nil {
		a.reportStatus(pod.ID, model.PodFailed, nil, fmt.Sprintf("image pull: %v", err), "")
		agentmetrics.WorkerFailedJobsTotal.Inc()
		return
	}
	io.Copy(io.Discard, reader)
	reader.Close()

	// Build the container command (empty → use image ENTRYPOINT/CMD as-is).
	var cmd []string
	if pod.Command != "" {
		cmd = append([]string{pod.Command}, pod.Args...)
	}

	// Map pod resource requests onto Docker host-config limits (0 = unlimited).
	var nanoCPUs int64
	if pod.Resources.CPU > 0 {
		nanoCPUs = int64(pod.Resources.CPU * 1e9)
	}
	var memBytes int64
	if pod.Resources.MemoryMB > 0 {
		memBytes = int64(pod.Resources.MemoryMB) * 1024 * 1024
	}

	containerName := "pod-" + pod.ID
	resp, err := a.docker.ContainerCreate(
		ctx,
		&container.Config{Image: pod.Image, Cmd: cmd},
		&container.HostConfig{Resources: container.Resources{
			NanoCPUs: nanoCPUs,
			Memory:   memBytes,
		}},
		nil, nil, containerName,
	)
	if err != nil {
		a.reportStatus(pod.ID, model.PodFailed, nil, fmt.Sprintf("container create: %v", err), "")
		agentmetrics.WorkerFailedJobsTotal.Inc()
		return
	}
	containerID := resp.ID

	// Always clean up the container, even on error or cancellation.
	defer a.docker.ContainerRemove(
		context.Background(), containerID, container.RemoveOptions{Force: true})

	if err := a.docker.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		a.reportStatus(pod.ID, model.PodFailed, nil, fmt.Sprintf("container start: %v", err), "")
		agentmetrics.WorkerFailedJobsTotal.Inc()
		return
	}

	// Wait on a separate context so we can race against ctx.Done() without
	// having ContainerWait fail the moment the caller's context is cancelled.
	waitCtx, waitCancel := context.WithCancel(context.Background())
	defer waitCancel()
	statusCh, errCh := a.docker.ContainerWait(waitCtx, containerID, container.WaitConditionNotRunning)

	var exitCode int
	var runErr error
	select {
	case status := <-statusCh:
		exitCode = int(status.StatusCode)
		if status.Error != nil {
			runErr = errors.New(status.Error.Message)
		}
	case err := <-errCh:
		runErr = err
	case <-ctx.Done():
		// Graceful-shutdown timeout hit: stop the container, then report.
		timeout := 5
		a.docker.ContainerStop(context.Background(), containerID,
			container.StopOptions{Timeout: &timeout})
		output := a.collectContainerLogs(containerID)
		a.reportStatus(pod.ID, model.PodCancelled, nil, "node agent shutting down", output)
		return
	}

	output := a.collectContainerLogs(containerID)

	switch {
	case runErr != nil:
		a.reportStatus(pod.ID, model.PodFailed, nil, runErr.Error(), output)
		agentmetrics.WorkerFailedJobsTotal.Inc()
	case exitCode != 0:
		a.reportStatus(pod.ID, model.PodFailed, &exitCode,
			fmt.Sprintf("exit code %d", exitCode), output)
		agentmetrics.WorkerFailedJobsTotal.Inc()
	default:
		zero := 0
		a.reportStatus(pod.ID, model.PodSucceeded, &zero, "", output)
		agentmetrics.WorkerCompletedJobsTotal.Inc()
	}
}

// collectContainerLogs fetches stdout+stderr from the container and returns
// them as a single string. It uses stdcopy to demultiplex Docker's multiplexed
// log stream format.
func (a *Agent) collectContainerLogs(containerID string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := a.docker.ContainerLogs(ctx, containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err != nil {
		return ""
	}
	defer r.Close()
	var buf bytes.Buffer
	stdcopy.StdCopy(&buf, &buf, r)
	return buf.String()
}
