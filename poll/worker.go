package poll

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	pollpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/poll/v1"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/metrics/noop"

	"github.com/code-payments/flipcash2-server/balance"
)

const (
	// defaultWorkerBatchSize is how many due polls one tick pulls from the
	// queue.
	defaultWorkerBatchSize = 32

	// defaultWorkerMaxConcurrency bounds how many polls are tallied at once.
	// A tally is reads (votes, balances), so this bounds the load
	// one server puts on the stores and OCP.
	defaultWorkerMaxConcurrency = 4

	// defaultWorkerBackoffBase and defaultWorkerMaxBackoffDelay shape the
	// exponential retry backoff between failed attempts. There is no limit on
	// attempts: a poll's results are final, so a tally is retried until it
	// succeeds rather than closed on a guess, and a poll whose tally keeps
	// failing (an OCP outage, say) is retried every defaultWorkerMaxBackoffDelay.
	defaultWorkerBackoffBase     = 2 * time.Second
	defaultWorkerMaxBackoffDelay = 10 * time.Minute

	// defaultWorkerClaimLease is how far a claim pushes a poll's due time out.
	// It must comfortably exceed the tally timeout so a poll cannot come due
	// again while its claimant is still tallying it.
	defaultWorkerClaimLease = 10 * time.Minute

	// defaultWorkerTallyTimeout bounds a single tally attempt.
	defaultWorkerTallyTimeout = 5 * time.Minute

	// workerQueueStatsEventName is the metric event carrying the tally's backlog
	// gauges, due depth and max age, emitted once per workerQueueStatsInterval
	// by every running worker.
	workerQueueStatsEventName = "PollTallyQueuePollingCheck"

	// workerQueueStatsInterval is how often the queue gauges poll.
	workerQueueStatsInterval = time.Second
)

// Worker drains the tally queue: it polls the poll store for live polls whose
// tally is due (TallyDelay after they end), counts each one's votes (see
// tally), and closes it with its results.
//
// It implements the OCP worker.Runtime interface, so the parent application
// registers it alongside its other background runtimes and controls the poll
// interval. It is safe to run on every server instance: workers claim a poll
// before tallying it, a tally writes nothing until it closes the poll, and
// closing is conditional, so overlap costs duplicate reads at worst, never a
// second set of results.
//
// Construct with NewWorker and either run it via Start (the OCP runtime entry
// point) or drive single ticks with Process (tests).
type Worker struct {
	log      *zap.Logger
	polls    Store
	balances *balance.Client

	batchSize       int
	maxConcurrency  int
	backoffBase     time.Duration
	maxBackoffDelay time.Duration
	claimLease      time.Duration
	tallyTimeout    time.Duration
}

// WorkerOption overrides one of the worker's tuning knobs.
type WorkerOption func(*Worker)

// WithWorkerBatchSize overrides how many due polls one tick pulls from the
// queue.
func WithWorkerBatchSize(n int) WorkerOption {
	return func(w *Worker) { w.batchSize = n }
}

// WithWorkerMaxConcurrency overrides how many polls are tallied at once.
func WithWorkerMaxConcurrency(n int) WorkerOption {
	return func(w *Worker) { w.maxConcurrency = n }
}

// WithWorkerBackoff overrides the retry backoff's base and maximum delay.
func WithWorkerBackoff(base, maxDelay time.Duration) WorkerOption {
	return func(w *Worker) {
		w.backoffBase = base
		w.maxBackoffDelay = maxDelay
	}
}

// WithWorkerClaimLease overrides how far a claim pushes a poll's due time out.
func WithWorkerClaimLease(lease time.Duration) WorkerOption {
	return func(w *Worker) { w.claimLease = lease }
}

// WithWorkerTallyTimeout overrides the bound on a single tally attempt.
func WithWorkerTallyTimeout(timeout time.Duration) WorkerOption {
	return func(w *Worker) { w.tallyTimeout = timeout }
}

// NewWorker returns a Worker draining the tally queue of polls, reading
// BALANCE polls' balances through balances.
func NewWorker(log *zap.Logger, polls Store, balances *balance.Client, opts ...WorkerOption) *Worker {
	w := &Worker{
		log:      log,
		polls:    polls,
		balances: balances,

		batchSize:       defaultWorkerBatchSize,
		maxConcurrency:  defaultWorkerMaxConcurrency,
		backoffBase:     defaultWorkerBackoffBase,
		maxBackoffDelay: defaultWorkerMaxBackoffDelay,
		claimLease:      defaultWorkerClaimLease,
		tallyTimeout:    defaultWorkerTallyTimeout,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Start satisfies the OCP worker.Runtime interface: it polls the tally queue
// every interval until ctx is cancelled, whose error it returns. The cadence is
// fixed-rate, not fixed-delay: each sleep is the interval minus the time the
// tick's processing took. A full batch polls again immediately, so a burst of
// polls ending together drains at processing speed rather than one batch per
// interval. It also runs the queue gauges (depth, max age) alongside the
// processing loop.
func (w *Worker) Start(ctx context.Context, interval time.Duration) error {
	go w.queueStatsGaugeWorker(ctx)

	for {
		start := time.Now()
		processed, err := w.Process(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			w.log.Warn("Failed to process tally queue", zap.Error(err))
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

// Process runs one worker tick: it pulls the due polls and tallies them with
// bounded concurrency, reporting how many this worker took on (claimed). Zero
// means the due queue is drained: polls other workers hold claims on are not
// counted.
func (w *Worker) Process(runtimeCtx context.Context) (int, error) {
	// A context carrying no provider traces into the void rather than panicking:
	// tracing is observability, never a reason to fail a tick.
	metricsProvider, ok := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
	if !ok || metricsProvider == nil {
		metricsProvider = noop.NewProvider()
	}
	trace := metricsProvider.StartTrace("poll_tally_worker")
	defer trace.End()
	tracedCtx := metrics.NewContext(runtimeCtx, trace)

	due, err := w.polls.GetDueForTally(tracedCtx, time.Now(), w.batchSize)
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

// processOne drives a single due poll: it is claimed, read, tallied and
// closed, with a failed attempt rescheduled under backoff. The claim comes
// first, so of the workers that list the same due poll only the one that wins
// it pays the poll's strongly consistent read. It reports whether this worker
// took the poll on.
func (w *Worker) processOne(ctx context.Context, task *TallyTask) bool {
	log := w.log.With(zap.String("poll_id", hex.EncodeToString(task.ID.Value)))

	claimed, err := w.polls.ClaimForTally(ctx, task.ID, time.Now(), time.Now().Add(w.claimLease))
	if err != nil {
		log.Warn("Failed to claim poll for tally", zap.Error(err))
		return false
	}
	if !claimed {
		// Another worker got there first, or the poll just closed.
		return false
	}

	if err := w.tallyAndClose(ctx, task.ID); err != nil {
		attempts := task.Attempts + 1
		delay := w.backoffDelay(attempts)
		log.Warn("Poll tally attempt failed; rescheduling",
			zap.Error(err),
			zap.Uint32("attempts", attempts),
			zap.Duration("delay", delay),
		)
		if err := w.polls.DelayTally(ctx, task.ID, time.Now().Add(delay)); err != nil {
			log.Warn("Failed to reschedule poll tally", zap.Error(err))
		}
	}
	return true
}

// tallyAndClose reads a claimed poll, counts its votes and closes it with the
// results. A poll a concurrent tally closed first keeps its results. A failed
// read is a failed attempt like any other: the claim is held, so the poll is
// rescheduled under backoff rather than left until its lease ends.
func (w *Worker) tallyAndClose(ctx context.Context, pollID *pollpb.PollId) error {
	tallyCtx, cancel := context.WithTimeout(ctx, w.tallyTimeout)
	defer cancel()

	p, err := w.polls.GetPoll(tallyCtx, pollID)
	if err != nil {
		// Not found included: only a live poll is queued, and a live poll is
		// never discarded, so a claimed poll that cannot be read is a fault.
		return fmt.Errorf("loading poll: %w", err)
	}

	results, err := tally(tallyCtx, w.polls, w.balances, p)
	if err != nil {
		return err
	}
	_, err = w.polls.Close(tallyCtx, p.ID, results)
	return err
}

// queueStatsGaugeWorker emits the tally's backlog gauges, depth and max age, as
// a recurring metric event until ctx is cancelled. Both count due polls alone
// (see TallyQueueStats): the depth is how many are waiting for a worker, and
// the max age how long the most overdue has waited past its due time, so a
// climbing max age means the tally is falling behind. Neither reads a poll that
// is not yet due, so the cost follows the backlog, not the number of open
// polls. No due poll reports a max age of zero. Emission is a no-op when ctx
// carries no metrics provider, and a failed stats poll is skipped rather than
// surfaced.
func (w *Worker) queueStatsGaugeWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(workerQueueStatsInterval):
			now := time.Now()
			stats, err := w.polls.GetTallyQueueStats(ctx, now)
			if err != nil {
				continue
			}
			var maxAge time.Duration
			if !stats.OldestDueAt.IsZero() {
				maxAge = max(now.Sub(stats.OldestDueAt), 0)
			}
			metrics.RecordEvent(ctx, workerQueueStatsEventName, map[string]any{
				"count":      stats.DueDepth,
				"max_age_ms": maxAge.Milliseconds(),
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
