package store

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/czhao-dev/ml-job-orchestrator/internal/model"
)

var (
	ErrNotFound          = errors.New("job not found")
	ErrAlreadyExists     = errors.New("job already exists")
	ErrInvalidTransition = errors.New("invalid state transition")
)

// Store is a concurrent, in-memory Job state store backed by sync.Map.
//
// sync.Map handles the common case of many goroutines reading/writing
// disjoint job IDs without contention. It cannot, by itself, make a
// multi-field read-modify-write (as Transition performs) atomic across
// concurrent callers touching the SAME id, so a coarse mutex guards the
// write path. Reads (Get/ListByState/List) remain lock-free. See
// docs/design.md for the full tradeoff discussion.
type Store struct {
	m  sync.Map // id -> model.Job
	mu sync.Mutex
}

func New() *Store {
	return &Store{}
}

func (s *Store) Create(job model.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.m.Load(job.ID); exists {
		return ErrAlreadyExists
	}
	s.m.Store(job.ID, job.Clone())
	return nil
}

func (s *Store) Get(id string) (model.Job, error) {
	v, ok := s.m.Load(id)
	if !ok {
		return model.Job{}, ErrNotFound
	}
	return v.(model.Job).Clone(), nil
}

// Update overwrites the stored job wholesale. Callers own validating any
// state-transition rules before calling Update (Transition is the
// validated alternative for state changes).
func (s *Store) Update(job model.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.m.Load(job.ID); !exists {
		return ErrNotFound
	}
	s.m.Store(job.ID, job.Clone())
	return nil
}

// Transition validates and applies a state transition for job id, updating
// timestamps (StartedAt on entering RUNNING, FinishedAt on entering a
// terminal state) and ErrorMessage (if non-empty) as part of the same
// atomic update.
func (s *Store) Transition(id string, to model.State, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.m.Load(id)
	if !ok {
		return ErrNotFound
	}
	job := v.(model.Job)

	if !model.Transition(job.State, to) {
		return ErrInvalidTransition
	}

	job.State = to
	if errMsg != "" {
		job.ErrorMessage = errMsg
	}

	now := time.Now()
	if to == model.StateRunning && job.StartedAt == nil {
		job.StartedAt = &now
	}
	if isTerminal(to) && job.FinishedAt == nil {
		job.FinishedAt = &now
	}

	s.m.Store(id, job)
	return nil
}

// SetOutput records subprocess output for a job without changing its state.
func (s *Store) SetOutput(id, output string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m.Load(id)
	if !ok {
		return ErrNotFound
	}
	job := v.(model.Job)
	job.Output = output
	s.m.Store(id, job)
	return nil
}

func (s *Store) Delete(id string) {
	s.m.Delete(id)
}

func (s *Store) ListByState(state model.State) []model.Job {
	var out []model.Job
	s.m.Range(func(_, v any) bool {
		job := v.(model.Job)
		if job.State == state {
			out = append(out, job.Clone())
		}
		return true
	})
	return out
}

type ListFilter struct {
	State string
	Type  string
	Limit int
}

// List returns jobs matching the filter (state/type, case-sensitive exact
// match) sorted by CreatedAt descending, along with the total count of
// matching jobs before Limit is applied.
func (s *Store) List(f ListFilter) ([]model.Job, int) {
	var matched []model.Job
	s.m.Range(func(_, v any) bool {
		job := v.(model.Job)
		if f.State != "" && string(job.State) != f.State {
			return true
		}
		if f.Type != "" && job.Type != f.Type {
			return true
		}
		matched = append(matched, job.Clone())
		return true
	})

	sort.Slice(matched, func(i, j int) bool {
		return matched[i].CreatedAt.After(matched[j].CreatedAt)
	})

	total := len(matched)
	if f.Limit > 0 && len(matched) > f.Limit {
		matched = matched[:f.Limit]
	}
	return matched, total
}

func isTerminal(s model.State) bool {
	switch s {
	case model.StateCompleted, model.StateFailed, model.StateExhausted, model.StateCancelled:
		return true
	default:
		return false
	}
}
