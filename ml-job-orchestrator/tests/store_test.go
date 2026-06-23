package tests

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/czhao-dev/ml-job-orchestrator/internal/model"
	"github.com/czhao-dev/ml-job-orchestrator/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModelTransition(t *testing.T) {
	allStates := []model.State{
		model.StatePending, model.StateQueued, model.StateRunning,
		model.StateCompleted, model.StateFailed, model.StateExhausted, model.StateCancelled,
	}

	valid := map[model.State]map[model.State]bool{
		model.StatePending: {model.StateQueued: true, model.StateCancelled: true},
		// QUEUED -> PENDING covers the scheduler reverting a dispatch when
		// the job queue channel is full.
		model.StateQueued: {model.StateRunning: true, model.StateCancelled: true, model.StatePending: true},
		model.StateRunning:   {model.StateCompleted: true, model.StateFailed: true, model.StateCancelled: true},
		model.StateFailed:    {model.StatePending: true, model.StateExhausted: true},
		model.StateCompleted: {},
		model.StateExhausted: {},
		model.StateCancelled: {},
	}

	for _, from := range allStates {
		for _, to := range allStates {
			from, to := from, to
			want := valid[from][to]
			t.Run(fmt.Sprintf("%s_to_%s", from, to), func(t *testing.T) {
				got := model.Transition(from, to)
				assert.Equal(t, want, got, "Transition(%s, %s)", from, to)
			})
		}
	}
}

func newTestJob(id string) model.Job {
	return model.Job{
		ID:        id,
		Name:      "test-job",
		Type:      "training",
		Command:   "echo",
		Args:      []string{"hi"},
		State:     model.StatePending,
		CreatedAt: time.Now(),
	}
}

func TestStoreCreateGetUpdate(t *testing.T) {
	s := store.New()
	job := newTestJob("job_1")

	require.NoError(t, s.Create(job))
	require.ErrorIs(t, s.Create(job), store.ErrAlreadyExists)

	got, err := s.Get("job_1")
	require.NoError(t, err)
	assert.Equal(t, job.ID, got.ID)
	assert.Equal(t, model.StatePending, got.State)

	_, err = s.Get("does_not_exist")
	require.ErrorIs(t, err, store.ErrNotFound)

	got.Priority = 5
	require.NoError(t, s.Update(got))
	got2, err := s.Get("job_1")
	require.NoError(t, err)
	assert.Equal(t, 5, got2.Priority)

	err = s.Update(newTestJob("never_created"))
	require.ErrorIs(t, err, store.ErrNotFound)
}

func TestStoreGetReturnsIndependentCopy(t *testing.T) {
	s := store.New()
	job := newTestJob("job_copy")
	require.NoError(t, s.Create(job))

	got, err := s.Get("job_copy")
	require.NoError(t, err)
	got.Args[0] = "mutated"

	got2, err := s.Get("job_copy")
	require.NoError(t, err)
	assert.Equal(t, "hi", got2.Args[0], "mutating a Get() result must not affect the stored job")
}

func TestStoreTransition(t *testing.T) {
	s := store.New()
	job := newTestJob("job_2")
	require.NoError(t, s.Create(job))

	require.NoError(t, s.Transition("job_2", model.StateQueued, ""))
	got, err := s.Get("job_2")
	require.NoError(t, err)
	assert.Equal(t, model.StateQueued, got.State)

	err = s.Transition("job_2", model.StateCompleted, "")
	require.ErrorIs(t, err, store.ErrInvalidTransition)

	require.NoError(t, s.Transition("job_2", model.StateRunning, ""))
	got, err = s.Get("job_2")
	require.NoError(t, err)
	require.NotNil(t, got.StartedAt)

	require.NoError(t, s.Transition("job_2", model.StateFailed, "boom"))
	got, err = s.Get("job_2")
	require.NoError(t, err)
	assert.Equal(t, "boom", got.ErrorMessage)
	require.NotNil(t, got.FinishedAt)

	err = s.Transition("missing", model.StateQueued, "")
	require.ErrorIs(t, err, store.ErrNotFound)
}

func TestStoreListByStateAndFilter(t *testing.T) {
	s := store.New()
	for i := 0; i < 3; i++ {
		j := newTestJob(fmt.Sprintf("pending_%d", i))
		j.Type = "training"
		require.NoError(t, s.Create(j))
	}
	running := newTestJob("running_1")
	running.Type = "inference"
	require.NoError(t, s.Create(running))
	require.NoError(t, s.Transition("running_1", model.StateQueued, ""))
	require.NoError(t, s.Transition("running_1", model.StateRunning, ""))

	pending := s.ListByState(model.StatePending)
	assert.Len(t, pending, 3)

	jobs, total := s.List(store.ListFilter{State: "PENDING", Limit: 2})
	assert.Equal(t, 3, total)
	assert.Len(t, jobs, 2)

	jobs, total = s.List(store.ListFilter{Type: "inference"})
	assert.Equal(t, 1, total)
	require.Len(t, jobs, 1)
	assert.Equal(t, "running_1", jobs[0].ID)
}

func TestStoreDelete(t *testing.T) {
	s := store.New()
	require.NoError(t, s.Create(newTestJob("to_delete")))
	s.Delete("to_delete")
	_, err := s.Get("to_delete")
	require.ErrorIs(t, err, store.ErrNotFound)
}

// TestStoreConcurrentAccess exercises Create/Get/Update/Transition from many
// goroutines simultaneously against disjoint and shared IDs. Run with
// `go test -race` to confirm no data races.
func TestStoreConcurrentAccess(t *testing.T) {
	s := store.New()
	const numGoroutines = 20
	const numJobsEach = 25

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < numJobsEach; i++ {
				id := fmt.Sprintf("g%d_job%d", g, i)
				job := newTestJob(id)
				if err := s.Create(job); err != nil {
					t.Errorf("Create(%s): %v", id, err)
					continue
				}
				if _, err := s.Get(id); err != nil {
					t.Errorf("Get(%s): %v", id, err)
				}
				if err := s.Transition(id, model.StateQueued, ""); err != nil {
					t.Errorf("Transition(%s): %v", id, err)
				}
			}
		}()
	}
	wg.Wait()

	jobs := s.ListByState(model.StateQueued)
	assert.Len(t, jobs, numGoroutines*numJobsEach)

	// Now hammer a single shared job ID concurrently to confirm Transition
	// serializes correctly without racing (last valid transition wins, no
	// torn writes).
	require.NoError(t, s.Create(newTestJob("shared")))
	require.NoError(t, s.Transition("shared", model.StateQueued, ""))

	var wg2 sync.WaitGroup
	successes := make([]bool, numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		g := g
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			err := s.Transition("shared", model.StateRunning, "")
			successes[g] = err == nil
		}()
	}
	wg2.Wait()

	successCount := 0
	for _, ok := range successes {
		if ok {
			successCount++
		}
	}
	assert.Equal(t, 1, successCount, "exactly one goroutine should win the QUEUED->RUNNING transition")
}
