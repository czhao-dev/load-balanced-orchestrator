package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	JobsSubmitted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mlorch_jobs_submitted_total",
		Help: "Total jobs submitted, by type",
	}, []string{"job_type"})

	JobsCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mlorch_jobs_completed_total",
		Help: "Total jobs completed successfully, by type",
	}, []string{"job_type"})

	JobsFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mlorch_jobs_failed_total",
		Help: "Total jobs that failed (including timeouts and exhausted retries), by type",
	}, []string{"job_type"})

	JobsDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mlorch_job_duration_seconds",
		Help:    "Job execution duration in seconds",
		Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600},
	}, []string{"job_type", "state"})

	QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "mlorch_queue_depth",
		Help: "Number of jobs currently queued",
	})
)
