package model

import "time"

// PodStatus is the lifecycle state of a single execution unit created from a Deployment.
type PodStatus string

const (
	PodPending    PodStatus = "PENDING"
	PodScheduled  PodStatus = "SCHEDULED"
	PodRunning    PodStatus = "RUNNING"
	PodSucceeded  PodStatus = "SUCCEEDED"
	PodFailed     PodStatus = "FAILED"
	PodRetrying   PodStatus = "RETRYING"
	PodDeadLetter PodStatus = "DEAD_LETTER"
	PodCancelled  PodStatus = "CANCELLED"
)

// allowedPodTransitions maps a pod status to the set of statuses it may move to.
// Terminal states (SUCCEEDED, DEAD_LETTER, CANCELLED) have no outgoing transitions.
var allowedPodTransitions = map[PodStatus][]PodStatus{
	PodPending: {PodScheduled, PodCancelled},
	// SCHEDULED -> PENDING covers the scheduler reverting a dispatch that the
	// node never actually picked up (e.g. node became unhealthy mid-dispatch).
	PodScheduled:  {PodRunning, PodPending, PodCancelled},
	PodRunning:    {PodSucceeded, PodFailed, PodCancelled},
	PodFailed:     {PodRetrying, PodDeadLetter},
	PodRetrying:   {PodPending, PodScheduled},
	PodSucceeded:  {},
	PodDeadLetter: {},
	PodCancelled:  {},
}

// TransitionPod reports whether moving a pod from `from` to `to` is legal.
// It is pure and stateless; callers apply the transition atomically (see state.Store).
func TransitionPod(from, to PodStatus) bool {
	for _, s := range allowedPodTransitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Pod is a concrete execution unit created from a Deployment.
type Pod struct {
	ID           string            `json:"id"`
	DeploymentID string            `json:"deployment_id"`
	NodeID       string            `json:"node_id,omitempty"`
	Namespace    string            `json:"namespace"`
	Labels       map[string]string `json:"labels,omitempty"`
	Attempt      int               `json:"attempt"`
	Status       PodStatus         `json:"status"`
	Image        string            `json:"image,omitempty"`
	ContainerID  string            `json:"container_id,omitempty"`
	Command      string            `json:"command"`
	Args         []string          `json:"args"`
	Resources    ResourceRequest   `json:"resources,omitempty"`
	ExitCode     *int              `json:"exit_code,omitempty"`
	Error        string            `json:"error,omitempty"`
	Output       string            `json:"output,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	ScheduledAt  *time.Time        `json:"scheduled_at,omitempty"`
	StartedAt    *time.Time        `json:"started_at,omitempty"`
	FinishedAt   *time.Time        `json:"finished_at,omitempty"`
	RunAfter     time.Time         `json:"run_after,omitzero"`
}

// Clone returns a deep copy so callers can mutate it without racing on
// slice/pointer/map fields shared with a value stored elsewhere.
func (p Pod) Clone() Pod {
	c := p
	if p.Args != nil {
		c.Args = append([]string(nil), p.Args...)
	}
	if p.Labels != nil {
		c.Labels = make(map[string]string, len(p.Labels))
		for k, v := range p.Labels {
			c.Labels[k] = v
		}
	}
	if p.ExitCode != nil {
		v := *p.ExitCode
		c.ExitCode = &v
	}
	if p.ScheduledAt != nil {
		t := *p.ScheduledAt
		c.ScheduledAt = &t
	}
	if p.StartedAt != nil {
		t := *p.StartedAt
		c.StartedAt = &t
	}
	if p.FinishedAt != nil {
		t := *p.FinishedAt
		c.FinishedAt = &t
	}
	return c
}

// Active reports whether the pod still occupies one of a deployment's desired
// replica slots. PENDING/SCHEDULED/RUNNING/RETRYING are still in flight;
// FAILED is a brief pre-retry-decision state the reconciler resolves on its
// next tick; SUCCEEDED permanently fills its slot (batch semantics).
// Only DEAD_LETTER and CANCELLED free a slot for the reconciler to refill.
func (p Pod) Active() bool {
	switch p.Status {
	case PodPending, PodScheduled, PodRunning, PodRetrying, PodFailed, PodSucceeded:
		return true
	default:
		return false
	}
}
