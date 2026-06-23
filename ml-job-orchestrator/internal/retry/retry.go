package retry

import (
	"math"
	"time"

	"github.com/czhao-dev/ml-job-orchestrator/internal/model"
)

const maxBackoff = 60 * time.Second

// ScheduleRetry returns a copy of job bumped for another attempt: RetryCount
// incremented, RunAfter set to now plus an exponential backoff (2^RetryCount
// seconds, capped at 60s), state reset to PENDING, and the previous run's
// timestamps cleared so the next attempt starts clean.
//
// Callers must check job.RetryCount < job.MaxRetries before calling this —
// if the budget is already exhausted, transition to EXHAUSTED instead.
// Callers must also never call this for a job whose failure was caused by
// cancellation (DELETE) rather than a real execution failure or timeout —
// cancellation always wins over retry regardless of remaining budget.
func ScheduleRetry(job model.Job) model.Job {
	job.RetryCount++
	backoff := time.Duration(math.Pow(2, float64(job.RetryCount))) * time.Second
	if backoff > maxBackoff {
		backoff = maxBackoff
	}
	job.RunAfter = time.Now().Add(backoff)
	job.State = model.StatePending
	job.StartedAt = nil
	job.FinishedAt = nil
	return job
}
