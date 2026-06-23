package model

import "time"

type State string

const (
	StatePending   State = "PENDING"
	StateQueued    State = "QUEUED"
	StateRunning   State = "RUNNING"
	StateCompleted State = "COMPLETED"
	StateFailed    State = "FAILED"
	StateExhausted State = "EXHAUSTED"
	StateCancelled State = "CANCELLED"
)

// allowedTransitions maps a state to the set of states it may transition to.
// Terminal states (COMPLETED, EXHAUSTED, CANCELLED) have no outgoing transitions.
var allowedTransitions = map[State][]State{
	StatePending: {StateQueued, StateCancelled},
	// QUEUED -> PENDING covers the scheduler reverting a dispatch when the
	// job queue channel is full (see scheduler.scheduleReady).
	StateQueued: {StateRunning, StateCancelled, StatePending},
	StateRunning:   {StateCompleted, StateFailed, StateCancelled},
	StateFailed:    {StatePending, StateExhausted},
	StateCompleted: {},
	StateExhausted: {},
	StateCancelled: {},
}

// Transition reports whether moving from `from` to `to` is a legal state transition.
// It is pure and stateless; callers are responsible for atomically applying the
// transition to a stored Job (see store.Store.Transition).
func Transition(from, to State) bool {
	for _, s := range allowedTransitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

type Job struct {
	ID             string     `json:"id"`
	Name           string     `json:"name"`
	Type           string     `json:"type"`
	Command        string     `json:"command"`
	Args           []string   `json:"args"`
	State          State      `json:"state"`
	Priority       int        `json:"priority"`
	MaxRetries     int        `json:"max_retries"`
	RetryCount     int        `json:"retry_count"`
	TimeoutSeconds int        `json:"timeout_seconds"`
	RunAfter       time.Time  `json:"run_after,omitzero"`
	CreatedAt      time.Time  `json:"created_at"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	Output         string     `json:"output,omitempty"`
	ErrorMessage   string     `json:"error_message,omitempty"`
}

// Clone returns a deep copy of the Job so callers can mutate it without
// racing on the slice/pointer fields shared with a value stored elsewhere
// (e.g. inside store.Store's sync.Map).
func (j Job) Clone() Job {
	c := j
	if j.Args != nil {
		c.Args = append([]string(nil), j.Args...)
	}
	if j.StartedAt != nil {
		t := *j.StartedAt
		c.StartedAt = &t
	}
	if j.FinishedAt != nil {
		t := *j.FinishedAt
		c.FinishedAt = &t
	}
	return c
}
