package reconciler

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/czhao-dev/control-plane/internal/metrics"
	"github.com/czhao-dev/control-plane/internal/model"
	"github.com/czhao-dev/control-plane/internal/state"
)

// Reconciler maintains desired state under failures: it creates/cancels pods
// to match each deployment's desired replica count, and detects node heartbeat
// timeouts, marking nodes unhealthy and rescheduling their in-flight pods.
type Reconciler struct {
	store            state.Store
	interval         time.Duration
	heartbeatTimeout time.Duration
	logger           *slog.Logger
}

func New(st state.Store, interval, heartbeatTimeout time.Duration, logger *slog.Logger) *Reconciler {
	return &Reconciler{store: st, interval: interval, heartbeatTimeout: heartbeatTimeout, logger: logger}
}

// Run blocks, ticking every interval until ctx is cancelled.
func (rc *Reconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(rc.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rc.Tick(ctx)
		}
	}
}

// Tick runs one reconciliation pass (replica reconciliation, then heartbeat
// timeout detection). Exported so tests and demo scripts can trigger it
// synchronously.
func (rc *Reconciler) Tick(ctx context.Context) {
	rc.reconcileDeployments(ctx)
	rc.detectUnhealthyNodes(ctx)
	metrics.ReconcilerIterations.Inc()
}

func (rc *Reconciler) reconcileDeployments(ctx context.Context) {
	deployments, err := rc.store.ListDeployments(ctx)
	if err != nil {
		rc.logger.Error("reconciler: list deployments", "error", err)
		return
	}

	for _, d := range deployments {
		if d.Status == model.DeploymentCancelled {
			continue
		}
		if d.Status == model.DeploymentPending {
			if err := rc.store.TransitionDeployment(ctx, d.ID, model.DeploymentActive); err != nil {
				rc.logger.Warn("reconciler: activate deployment", "deployment_id", d.ID, "error", err)
				continue
			}
			d.Status = model.DeploymentActive
		}

		pods, err := rc.store.ListPodsByDeployment(ctx, d.ID)
		if err != nil {
			rc.logger.Error("reconciler: list pods", "deployment_id", d.ID, "error", err)
			continue
		}

		active := 0
		hasDeadLetter := false
		var cancellable []*model.Pod // PENDING/SCHEDULED, candidates for scale-down
		for _, p := range pods {
			if p.Active() {
				active++
			}
			if p.Status == model.PodDeadLetter {
				hasDeadLetter = true
			}
			if p.Status == model.PodPending || p.Status == model.PodScheduled {
				cancellable = append(cancellable, p)
			}
		}

		switch {
		case active < d.Replicas:
			for i := 0; i < d.Replicas-active; i++ {
				id, err := newPodID()
				if err != nil {
					rc.logger.Error("reconciler: generate pod id", "error", err)
					continue
				}
				// Pods inherit Namespace and Labels from their owning Deployment
				// (pod-template semantics: the Deployment is the template, Pods are instances).
				pod := &model.Pod{
					ID:           id,
					DeploymentID: d.ID,
					Namespace:    d.Namespace,
					Labels:       cloneLabels(d.Labels),
					Status:       model.PodPending,
					Image:        d.Image,
					Command:      d.Command,
					Args:         d.Args,
					Resources:    d.Resources,
					CreatedAt:    time.Now(),
				}
				if err := rc.store.CreatePod(ctx, pod); err != nil {
					rc.logger.Error("reconciler: create pod", "deployment_id", d.ID, "error", err)
					continue
				}
				metrics.JobsTotal.Inc()
			}
		case active > d.Replicas:
			excess := active - d.Replicas
			sort.Slice(cancellable, func(i, j int) bool {
				return cancellable[i].CreatedAt.After(cancellable[j].CreatedAt) // newest first
			})
			for i := 0; i < excess && i < len(cancellable); i++ {
				if err := rc.store.TransitionPod(ctx, cancellable[i].ID, model.PodCancelled, "deployment scaled down"); err != nil {
					rc.logger.Warn("reconciler: cancel excess pod", "pod_id", cancellable[i].ID, "error", err)
				}
			}
		}

		switch {
		case hasDeadLetter && d.Status == model.DeploymentActive:
			_ = rc.store.TransitionDeployment(ctx, d.ID, model.DeploymentDegraded)
		case !hasDeadLetter && d.Status == model.DeploymentDegraded:
			_ = rc.store.TransitionDeployment(ctx, d.ID, model.DeploymentActive)
		}
	}
}

func (rc *Reconciler) detectUnhealthyNodes(ctx context.Context) {
	nodes, err := rc.store.ListNodes(ctx)
	if err != nil {
		rc.logger.Error("reconciler: list nodes", "error", err)
		return
	}

	now := time.Now()
	unhealthyCount := 0
	for _, n := range nodes {
		if n.Status == model.NodeUnhealthy {
			unhealthyCount++
		}
		timedOut := now.Sub(n.LastHeartbeatAt) > rc.heartbeatTimeout

		switch {
		case n.Status == model.NodeHealthy && timedOut:
			if err := rc.store.TransitionNode(ctx, n.ID, model.NodeUnhealthy); err != nil {
				rc.logger.Warn("reconciler: mark node unhealthy", "node_id", n.ID, "error", err)
				continue
			}
			unhealthyCount++
			rc.logger.Warn("reconciler: node heartbeat timeout", "node_id", n.ID)
			rc.reschedulePodsFor(ctx, n.ID)

		case n.Status == model.NodeDraining && timedOut:
			// An operator-initiated decommission that's gone quiet: remove
			// rather than mark unhealthy.
			if err := rc.store.TransitionNode(ctx, n.ID, model.NodeRemoved); err != nil {
				rc.logger.Warn("reconciler: remove drained node", "node_id", n.ID, "error", err)
				continue
			}
			rc.reschedulePodsFor(ctx, n.ID)
		}
	}
	metrics.UnhealthyWorkers.Set(float64(unhealthyCount))
}

// reschedulePodsFor requeues (with backoff) or dead-letters every RUNNING pod
// assigned to a node that just went unhealthy/removed. We can't know whether
// the pod actually died or the node just had a network blip, so we
// pessimistically requeue it.
func (rc *Reconciler) reschedulePodsFor(ctx context.Context, nodeID string) {
	pods, err := rc.store.ListPodsByNode(ctx, nodeID)
	if err != nil {
		rc.logger.Error("reconciler: list pods by node", "node_id", nodeID, "error", err)
		return
	}

	for _, p := range pods {
		// A pod that was SCHEDULED but never reached RUNNING (the node
		// died between dispatch and pickup) never actually executed, so it
		// doesn't burn a retry attempt -- just send it straight back to
		// PENDING (a legal SCHEDULED->PENDING transition) for the scheduler
		// to reassign.
		if p.Status == model.PodScheduled {
			p.NodeID = ""
			if err := rc.store.UpdatePod(ctx, p); err != nil {
				rc.logger.Warn("reconciler: clear node on orphaned scheduled pod", "pod_id", p.ID, "error", err)
				continue
			}
			if err := rc.store.TransitionPod(ctx, p.ID, model.PodPending, ""); err != nil {
				rc.logger.Warn("reconciler: requeue orphaned scheduled pod", "pod_id", p.ID, "error", err)
			}
			continue
		}
		if p.Status != model.PodRunning {
			continue
		}

		deployment, err := rc.store.GetDeployment(ctx, p.DeploymentID)
		maxRetries := 0
		if err == nil {
			maxRetries = deployment.MaxRetries
		}

		newAttempt := p.Attempt + 1
		if newAttempt <= maxRetries {
			p.Attempt = newAttempt
			p.RunAfter = time.Now().Add(backoff(newAttempt))
			p.NodeID = ""
			if err := rc.store.UpdatePod(ctx, p); err != nil {
				rc.logger.Warn("reconciler: bump pod attempt", "pod_id", p.ID, "error", err)
				continue
			}
			if err := rc.store.TransitionPod(ctx, p.ID, model.PodFailed, "node heartbeat timeout"); err != nil {
				rc.logger.Warn("reconciler: transition pod to failed", "pod_id", p.ID, "error", err)
				continue
			}
			_ = rc.store.TransitionPod(ctx, p.ID, model.PodRetrying, "")
			_ = rc.store.TransitionPod(ctx, p.ID, model.PodPending, "")
			metrics.JobsFailed.Inc()
		} else {
			p.Attempt = newAttempt
			if err := rc.store.UpdatePod(ctx, p); err != nil {
				rc.logger.Warn("reconciler: bump pod attempt", "pod_id", p.ID, "error", err)
				continue
			}
			if err := rc.store.TransitionPod(ctx, p.ID, model.PodFailed, "node heartbeat timeout"); err != nil {
				rc.logger.Warn("reconciler: transition pod to failed", "pod_id", p.ID, "error", err)
				continue
			}
			_ = rc.store.TransitionPod(ctx, p.ID, model.PodDeadLetter, "max retries exceeded after node failure")
			metrics.JobsFailed.Inc()
			metrics.JobsDeadLetter.Inc()
		}
	}
}

func cloneLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		out[k] = v
	}
	return out
}
