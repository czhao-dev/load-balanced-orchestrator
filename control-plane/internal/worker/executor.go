package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"

	"github.com/czhao-dev/control-plane/internal/agentmetrics"
	"github.com/czhao-dev/control-plane/internal/model"
)

// runPod reports RUNNING, then dispatches to the container or subprocess path
// based on whether the pod specifies an OCI image.
func (a *Agent) runPod(ctx context.Context, pod model.Pod) {
	a.reportStatus(pod.ID, model.PodRunning, nil, "", "")
	if pod.Image != "" {
		a.runContainerPod(ctx, pod)
	} else {
		a.runSubprocessPod(ctx, pod)
	}
}

// runSubprocessPod executes a pod as an OS subprocess and reports its outcome
// back to the control plane.
func (a *Agent) runSubprocessPod(ctx context.Context, pod model.Pod) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var buf bytes.Buffer
	cmd := exec.CommandContext(runCtx, pod.Command, pod.Args...)
	cmd.Stdout = &buf
	cmd.Stderr = &buf

	runErr := cmd.Run()
	output := buf.String()

	switch {
	case ctx.Err() != nil:
		a.reportStatus(pod.ID, model.PodCancelled, nil, "node agent shutting down", output)
	case runErr != nil:
		exitCode := exitCodeOf(runErr)
		a.reportStatus(pod.ID, model.PodFailed, exitCode, runErr.Error(), output)
		agentmetrics.WorkerFailedJobsTotal.Inc()
	default:
		zero := 0
		a.reportStatus(pod.ID, model.PodSucceeded, &zero, "", output)
		agentmetrics.WorkerCompletedJobsTotal.Inc()
	}
}

func exitCodeOf(err error) *int {
	if exitErr, ok := err.(*exec.ExitError); ok {
		code := exitErr.ExitCode()
		return &code
	}
	return nil
}

type podStatusRequest struct {
	Status   model.PodStatus `json:"status"`
	ExitCode *int            `json:"exit_code,omitempty"`
	Error    string          `json:"error,omitempty"`
	Output   string          `json:"output,omitempty"`
}

func (a *Agent) reportStatus(podID string, status model.PodStatus, exitCode *int, errMsg, output string) {
	body, _ := json.Marshal(podStatusRequest{Status: status, ExitCode: exitCode, Error: errMsg, Output: output})
	path := "/api/v1/nodes/" + a.id + "/pods/" + podID + "/status"
	if err := a.post(context.Background(), path, body, 200, nil); err != nil {
		a.logger.Warn("report pod status failed", "pod_id", podID, "status", status, "error", err)
	}
}
