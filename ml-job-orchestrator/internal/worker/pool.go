package worker

import (
	"context"
	"sync"
	"time"

	"github.com/czhao-dev/ml-job-orchestrator/internal/model"
)

// JobRunner executes a single job to completion, updating its state in the
// store as a side effect. Pool depends on this interface rather than the
// concrete executor package so the two packages don't import each other.
type JobRunner interface {
	Run(ctx context.Context, job model.Job)
}

// Pool is a fixed-size goroutine worker pool that consumes jobs from a
// shared queue channel. It is a consumer only: it never writes to the
// channel (the scheduler is the producer).
//
// Shutdown semantics: closing the pool stops workers from blocking forever
// on an empty queue once it's drained, but any job already in flight (or
// already buffered in the channel) is allowed to run to completion. If
// in-flight work doesn't finish within shutdownTimeout, the run context is
// cancelled, which propagates down to exec.CommandContext and kills any
// running subprocess.
type Pool struct {
	jobQueue        chan model.Job
	wg              sync.WaitGroup
	stopCh          chan struct{}
	runCancel       context.CancelFunc
	shutdownTimeout time.Duration
}

func New(ctx context.Context, numWorkers int, q chan model.Job, runner JobRunner, shutdownTimeout time.Duration) *Pool {
	runCtx, runCancel := context.WithCancel(ctx)
	p := &Pool{
		jobQueue:        q,
		stopCh:          make(chan struct{}),
		runCancel:       runCancel,
		shutdownTimeout: shutdownTimeout,
	}

	for i := 0; i < numWorkers; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				// A closed stopCh is always "ready", so racing it directly
				// against q in one select would let a worker exit while q
				// still has buffered jobs (select picks ready cases at
				// random). Draining q with a non-blocking check first
				// guarantees every already-queued job gets processed before
				// the worker honors the stop signal.
				select {
				case job, ok := <-q:
					if !ok {
						return
					}
					runner.Run(runCtx, job)
					continue
				default:
				}

				select {
				case job, ok := <-q:
					if !ok {
						return // channel closed, worker exits
					}
					runner.Run(runCtx, job)
				case <-p.stopCh:
					return // shutdown signal received, queue drained
				case <-ctx.Done():
					return // parent context cancelled directly
				}
			}
		}()
	}
	return p
}

// Shutdown signals workers to stop once the queue is drained, then blocks
// until all in-flight (and buffered) jobs finish — forcing cancellation
// after shutdownTimeout if they don't.
func (p *Pool) Shutdown() {
	close(p.stopCh)
	timer := time.AfterFunc(p.shutdownTimeout, p.runCancel)
	p.wg.Wait()
	timer.Stop()
	p.runCancel()
}
