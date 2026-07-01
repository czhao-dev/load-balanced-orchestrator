package model

import "time"

// DeploymentType distinguishes a one-shot batch of pod instances from a
// long-running service-style deployment.
type DeploymentType string

const (
	DeploymentBatch   DeploymentType = "batch"
	DeploymentService DeploymentType = "service"
)

// RestartPolicy controls whether a finished pod's replica slot is refilled.
type RestartPolicy string

const (
	RestartNever     RestartPolicy = "never"
	RestartOnFailure RestartPolicy = "on_failure"
	RestartAlways    RestartPolicy = "always"
)

// DeploymentStatus is the lifecycle state of desired state submitted by a user.
type DeploymentStatus string

const (
	DeploymentPending   DeploymentStatus = "PENDING"
	DeploymentActive    DeploymentStatus = "ACTIVE"
	DeploymentDegraded  DeploymentStatus = "DEGRADED" // some replicas dead-lettered/unhealthy
	DeploymentCancelled DeploymentStatus = "CANCELLED"
)

var allowedDeploymentTransitions = map[DeploymentStatus][]DeploymentStatus{
	DeploymentPending:   {DeploymentActive, DeploymentCancelled},
	DeploymentActive:    {DeploymentDegraded, DeploymentCancelled},
	DeploymentDegraded:  {DeploymentActive, DeploymentCancelled},
	DeploymentCancelled: {},
}

// TransitionDeployment reports whether moving a deployment from `from` to `to` is legal.
func TransitionDeployment(from, to DeploymentStatus) bool {
	for _, s := range allowedDeploymentTransitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// ResourceRequest is the resources a single pod instance of a deployment requires.
type ResourceRequest struct {
	CPU      float64 `json:"cpu"`
	MemoryMB int     `json:"memory_mb"`
}

// Deployment represents desired state submitted by a user.
type Deployment struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Namespace     string            `json:"namespace"`
	Labels        map[string]string `json:"labels,omitempty"`
	Type          DeploymentType    `json:"type"`
	Image         string            `json:"image,omitempty"`
	Command       string            `json:"command"`
	Args          []string          `json:"args"`
	Replicas      int               `json:"replicas"`
	MaxRetries    int               `json:"max_retries"`
	RestartPolicy RestartPolicy     `json:"restart_policy"`
	Resources     ResourceRequest   `json:"resources"`
	Status        DeploymentStatus  `json:"status"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
}

// Clone returns a deep copy so callers can mutate it without racing on
// slice/map fields shared with a value stored elsewhere.
func (d Deployment) Clone() Deployment {
	c := d
	if d.Args != nil {
		c.Args = append([]string(nil), d.Args...)
	}
	if d.Labels != nil {
		c.Labels = make(map[string]string, len(d.Labels))
		for k, v := range d.Labels {
			c.Labels[k] = v
		}
	}
	return c
}
