package blob

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/metrics"
)

const (
	// defaultWorkerBatchSize is how many due tasks one tick pulls from the queue.
	defaultWorkerBatchSize = 32

	// defaultWorkerMaxConcurrency bounds how many blobs are processed at once.
	// Rendition generation is CPU-bound (resample + encode per ladder rung), so
	// a batch must not fan out unboundedly.
	defaultWorkerMaxConcurrency = 4

	// defaultWorkerMaxAttempts is how many failed attempts a blob gets before it
	// is terminally rejected as internal. With the default backoff it spans
	// hours, so a moderation-provider outage self-heals, while the client still
	// gets a definitive answer instead of an eternal PROCESSING.
	defaultWorkerMaxAttempts = 20

	// defaultWorkerBackoffBase and defaultWorkerMaxBackoffDelay shape the
	// exponential retry backoff between failed attempts.
	defaultWorkerBackoffBase     = 2 * time.Second
	defaultWorkerMaxBackoffDelay = 10 * time.Minute

	// defaultWorkerClaimLease is how far a claim pushes a task's due time out.
	// It must comfortably exceed the finalize timeout so a task cannot come due
	// again while its claimant is still working on it.
	defaultWorkerClaimLease = 2 * time.Minute

	// defaultWorkerFinalizeTimeout bounds a single finalization attempt.
	// Finalization is dominated by moderation (an external call), so it is
	// generous, but bounded so a wedged dependency cannot hold a worker slot
	// forever.
	defaultWorkerFinalizeTimeout = time.Minute

	// workerQueueStatsEventName is the metric event carrying a kind's queue
	// gauges — depth and max age — emitted once per second by every running
	// worker (mirroring the OCP task runtime's polling gauge). Charted over
	// time, a depth rising faster than workers drain it means the queue is
	// growing, and a max age climbing while depth stays flat means something is
	// stuck retrying rather than merely busy.
	workerQueueStatsEventName = "BlobFinalizationQueuePollingCheck"

	// workerQueueStatsInterval is how often the queue gauges poll.
	workerQueueStatsInterval = time.Second
)

// Worker drains one content kind's finalization queue: it polls the blob store
// for uploads of that kind awaiting processing and drives each through the
// finalization pipeline (validation, moderation, promotion, rendition
// generation).
//
// Each ContentKind gets its own Worker, so a kind's tuning (concurrency,
// timeout, backoff) matches its pipeline's cost — an image resample and a
// video transcode should not share a knob — and a backlog in one kind never
// starves another.
//
// It implements the OCP worker.Runtime interface, so the parent application
// registers it alongside its other background runtimes and controls the poll
// interval. It is safe to run on every server instance: workers claim tasks
// before processing them, and the pipeline itself is idempotent, so overlap
// costs duplicate work at worst, never a wrong state.
//
// Construct with NewWorker and either run it via Start (the OCP runtime entry
// point) or drive single ticks with Process (tests).
type Worker struct {
	log       *zap.Logger
	blobs     Store
	finalizer *Finalizer

	// kind is the single finalization queue this worker drains.
	kind ContentKind

	batchSize       int
	maxConcurrency  int
	maxAttempts     uint32
	backoffBase     time.Duration
	maxBackoffDelay time.Duration
	claimLease      time.Duration
	finalizeTimeout time.Duration
}

// WorkerOption overrides one of the worker's tuning knobs.
type WorkerOption func(*Worker)

// WithWorkerBatchSize overrides how many due tasks one tick pulls from the queue.
func WithWorkerBatchSize(n int) WorkerOption {
	return func(w *Worker) { w.batchSize = n }
}

// WithWorkerMaxConcurrency overrides how many blobs are processed at once.
func WithWorkerMaxConcurrency(n int) WorkerOption {
	return func(w *Worker) { w.maxConcurrency = n }
}

// WithWorkerMaxAttempts overrides how many failed attempts a blob gets before
// it is terminally rejected.
func WithWorkerMaxAttempts(n uint32) WorkerOption {
	return func(w *Worker) { w.maxAttempts = n }
}

// WithWorkerBackoff overrides the retry backoff's base and maximum delay.
func WithWorkerBackoff(base, maxDelay time.Duration) WorkerOption {
	return func(w *Worker) {
		w.backoffBase = base
		w.maxBackoffDelay = maxDelay
	}
}

// WithWorkerClaimLease overrides how far a claim pushes a task's due time out.
func WithWorkerClaimLease(lease time.Duration) WorkerOption {
	return func(w *Worker) { w.claimLease = lease }
}

// WithWorkerFinalizeTimeout overrides the bound on a single finalization attempt.
func WithWorkerFinalizeTimeout(timeout time.Duration) WorkerOption {
	return func(w *Worker) { w.finalizeTimeout = timeout }
}

// NewWorker returns a Worker draining kind's finalization queue over the given
// blob store and finalizer.
func NewWorker(log *zap.Logger, blobs Store, finalizer *Finalizer, kind ContentKind, opts ...WorkerOption) *Worker {
	w := &Worker{
		log:       log,
		blobs:     blobs,
		finalizer: finalizer,
		kind:      kind,

		batchSize:       defaultWorkerBatchSize,
		maxConcurrency:  defaultWorkerMaxConcurrency,
		maxAttempts:     defaultWorkerMaxAttempts,
		backoffBase:     defaultWorkerBackoffBase,
		maxBackoffDelay: defaultWorkerMaxBackoffDelay,
		claimLease:      defaultWorkerClaimLease,
		finalizeTimeout: defaultWorkerFinalizeTimeout,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Start satisfies the OCP worker.Runtime interface: it polls the finalization
// queue every interval until ctx is cancelled, whose error it returns. The
// cadence is fixed-rate, not fixed-delay: each sleep is the interval minus the
// time the tick's processing took, so slow ticks do not stretch the gap between
// polls (a tick that overran the interval polls again immediately). A full
// batch also polls again immediately, so a burst drains at processing speed
// rather than one batch per interval. It also runs the queue gauges (depth,
// max age) for its kind alongside the processing loop.
func (w *Worker) Start(ctx context.Context, interval time.Duration) error {
	go w.queueStatsGaugeWorker(ctx)

	for {
		start := time.Now()
		processed, err := w.Process(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			w.log.Warn("Failed to process finalization queue", zap.Error(err))
		}
		if processed == w.batchSize && ctx.Err() == nil {
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		// A non-positive remainder fires immediately, still giving cancellation
		// the chance to win the select.
		case <-time.After(interval - time.Since(start)):
		}
	}
}

// Process runs one worker tick: it pulls the due tasks and processes them with
// bounded concurrency, reporting how many this worker actually took on (claimed,
// or terminally failed for exhaustion). Zero means the due queue is drained —
// tasks other workers hold claims on are not counted.
func (w *Worker) Process(runtimeCtx context.Context) (int, error) {
	metricsProvider := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
	trace := metricsProvider.StartTrace(fmt.Sprintf("blob_%s_worker", w.kind.String()))
	defer trace.End()
	tracedCtx := metrics.NewContext(runtimeCtx, trace)

	due, err := w.blobs.GetDueForFinalization(tracedCtx, w.kind, time.Now(), w.batchSize)
	if err != nil {
		return 0, err
	}

	sem := make(chan struct{}, w.maxConcurrency)
	var wg sync.WaitGroup
	var processed atomic.Int64
	for _, task := range due {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			if w.processOne(tracedCtx, task) {
				processed.Add(1)
			}
		})
	}
	wg.Wait()
	return int(processed.Load()), nil
}

// processOne drives a single due task: exhausted tasks are terminally failed,
// anything else is claimed and run through the finalization pipeline, with a
// failed attempt rescheduled under backoff. It reports whether this worker took
// the task on.
func (w *Worker) processOne(ctx context.Context, task *FinalizationTask) bool {
	log := w.log.With(zap.String("blob_id", IDString(task.ID)))

	// A task that burned through its attempts gets a terminal (internal)
	// rejection instead of another try, so the client sees a definitive status.
	// Fail is idempotent, so racing another worker here is harmless.
	if task.Attempts >= w.maxAttempts {
		log.Warn("Blob exhausted its finalization attempts; rejecting",
			zap.Uint32("attempts", task.Attempts))
		if err := w.finalizer.Fail(ctx, task.ID); err != nil {
			log.Warn("Failed to reject exhausted blob", zap.Error(err))
		}
		return true
	}

	record, err := w.blobs.GetByID(ctx, task.ID)
	if errors.Is(err, ErrNotFound) {
		// Reclaimed (TTL) between the poll and now; the queue entry went with it.
		return false
	} else if err != nil {
		// No claim is held yet, so the task stays due and the next tick simply
		// retries the read.
		log.Warn("Failed to load blob for finalization", zap.Error(err))
		return false
	}

	if record.ContentKind() != w.kind {
		log.Warn("unexpected content kind")
		return false
	}

	claimed, err := w.blobs.ClaimForFinalization(ctx, task.ID, time.Now(), time.Now().Add(w.claimLease))
	if err != nil {
		log.Warn("Failed to claim blob for finalization", zap.Error(err))
		return false
	}
	if !claimed {
		// Another worker got there first (or the blob just finalized); nothing to do.
		return false
	}

	finalizeCtx, cancel := context.WithTimeout(ctx, w.finalizeTimeout)
	defer cancel()

	if _, err := w.finalizer.Finalize(finalizeCtx, record); err != nil {
		// The pipeline stopped short of a terminal state (it resumes from its
		// last checkpoint next time); reschedule under backoff.
		attempts := task.Attempts + 1
		delay := w.backoffDelay(attempts)
		log.Warn("Blob finalization attempt failed; rescheduling",
			zap.Error(err),
			zap.Uint32("attempts", attempts),
			zap.Duration("delay", delay),
		)
		if err := w.blobs.DelayFinalization(ctx, task.ID, time.Now().Add(delay)); err != nil {
			log.Warn("Failed to reschedule blob finalization", zap.Error(err))
		}
	}
	return true
}

// queueStatsGaugeWorker emits this worker's kind's finalization queue gauges —
// depth and max age — as a recurring metric event until ctx is cancelled, so
// dashboards can chart whether the queue is growing or draining and whether
// anything is stuck. Both include not-yet-due retries: a blob waiting out
// backoff is still backlog, and its age keeps counting from its FIRST enqueue.
// An empty queue reports a max age of zero. Emission is a no-op when ctx
// carries no metrics provider, and a failed stats poll is skipped rather than
// surfaced: the gauges are observability, never a reason to disturb processing.
func (w *Worker) queueStatsGaugeWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(workerQueueStatsInterval):
			stats, err := w.blobs.GetFinalizationQueueStats(ctx, w.kind)
			if err != nil {
				continue
			}
			var maxAge time.Duration
			if !stats.OldestEnqueuedAt.IsZero() {
				maxAge = time.Since(stats.OldestEnqueuedAt)
			}
			metrics.RecordEvent(ctx, workerQueueStatsEventName, map[string]any{
				"count":        stats.Depth,
				"max_age_ms":   maxAge.Milliseconds(),
				"content_kind": w.kind.String(),
			})
		}
	}
}

// backoffDelay is the exponential retry delay after the given number of failed
// attempts (>= 1), capped at the configured maximum.
func (w *Worker) backoffDelay(attempts uint32) time.Duration {
	shift := attempts - 1
	// Guard the shift itself: past 62 bits the doubling has long since blown
	// through any sane cap.
	if shift > 62 {
		return w.maxBackoffDelay
	}
	delay := w.backoffBase << shift
	if delay <= 0 || delay > w.maxBackoffDelay {
		return w.maxBackoffDelay
	}
	return delay
}
