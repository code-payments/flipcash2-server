package paymentrequest

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	paymentrequestpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/paymentrequest/v1"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/metrics/noop"
)

const (
	// defaultWorkerBatchSize is how many due requests one tick pulls from the
	// queue.
	defaultWorkerBatchSize = 32

	// defaultWorkerMaxConcurrency bounds how many requests are expired at
	// once. An expiry is one read and one write of the request, so this bounds
	// the load one server puts on the store.
	defaultWorkerMaxConcurrency = 4

	// defaultWorkerBackoffBase and defaultWorkerMaxBackoffDelay shape the
	// exponential retry backoff between failed attempts. There is no limit on
	// attempts: a request whose expiry keeps failing is retried every
	// defaultWorkerMaxBackoffDelay until it is recorded.
	defaultWorkerBackoffBase     = 2 * time.Second
	defaultWorkerMaxBackoffDelay = 10 * time.Minute

	// defaultWorkerClaimLease is how far a claim pushes a request's due time
	// out. It must comfortably exceed the expire timeout so a request cannot
	// come due again while its claimant is still expiring it.
	defaultWorkerClaimLease = 2 * time.Minute

	// defaultWorkerExpireTimeout bounds a single expiry attempt.
	defaultWorkerExpireTimeout = 30 * time.Second

	// workerQueueStatsEventName is the metric event carrying the expiry
	// backlog gauges, due depth and max age, emitted once per
	// workerQueueStatsInterval by every running worker.
	workerQueueStatsEventName = "PaymentRequestExpiryQueuePollingCheck"

	// workerQueueStatsInterval is how often the queue gauges poll.
	workerQueueStatsInterval = time.Second
)

// Worker drains the expiry queue: it polls the store for requests stored open
// whose expiry is due (ExpireDelay after their ends_at) and stores each one
// expired (see Store.Expire).
//
// It decides nothing a reader sees. Expiry is already the clock's: from its
// ends_at on, a request stored open is EXPIRED to every reader, closed at its
// ends_at, at the version it was stored with (see PaymentRequest.StatusAt and
// ToProto), and no share can be marked paid on it. Storing the expiry
// changes none of that; the worker exists only to move the request from its
// chat's open section to the resolved one, with closed_at = ends_at, so that
// the RESOLVED listing is one read of one index rather than a merge of the
// resolved requests and the open ones past their end (see Store.GetChatPage).
// A slow or stopped worker therefore delays only the listing: an expired
// request sits at the head of the open section, which a reader drops by
// StatusAt, until the worker files it, and never shows a state it would not
// show afterward.
//
// Like poll's tally, a failed attempt is retried under backoff, forever: a
// request stored open past its end must eventually be filed as resolved, and
// a later attempt is no less correct than the first. A payment recorded
// between the worker's read and its write is never lost: both writes are
// conditioned on the version and status they read, so whichever lands second
// decides again (see ExpireDelay).
//
// It implements the OCP worker.Runtime interface, so the parent application
// registers it alongside its other background runtimes and controls the poll
// interval. It is safe to run on every server instance: workers claim a
// request before expiring it, and Expire is conditional and idempotent, so
// overlap costs a duplicate read at worst.
//
// Construct with NewWorker and either run it via Start (the OCP runtime entry
// point) or drive single ticks with Process (tests).
type Worker struct {
	log      *zap.Logger
	requests Store

	batchSize       int
	maxConcurrency  int
	backoffBase     time.Duration
	maxBackoffDelay time.Duration
	claimLease      time.Duration
	expireTimeout   time.Duration
}

// WorkerOption overrides one of the worker's tuning knobs.
type WorkerOption func(*Worker)

// WithWorkerBatchSize overrides how many due requests one tick pulls from the
// queue.
func WithWorkerBatchSize(n int) WorkerOption {
	return func(w *Worker) { w.batchSize = n }
}

// WithWorkerMaxConcurrency overrides how many requests are expired at once.
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

// WithWorkerClaimLease overrides how far a claim pushes a request's due time
// out.
func WithWorkerClaimLease(lease time.Duration) WorkerOption {
	return func(w *Worker) { w.claimLease = lease }
}

// WithWorkerExpireTimeout overrides the bound on a single expiry attempt.
func WithWorkerExpireTimeout(timeout time.Duration) WorkerOption {
	return func(w *Worker) { w.expireTimeout = timeout }
}

// NewWorker returns a Worker draining the expiry queue of requests.
func NewWorker(log *zap.Logger, requests Store, opts ...WorkerOption) *Worker {
	w := &Worker{
		log:      log,
		requests: requests,

		batchSize:       defaultWorkerBatchSize,
		maxConcurrency:  defaultWorkerMaxConcurrency,
		backoffBase:     defaultWorkerBackoffBase,
		maxBackoffDelay: defaultWorkerMaxBackoffDelay,
		claimLease:      defaultWorkerClaimLease,
		expireTimeout:   defaultWorkerExpireTimeout,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Start satisfies the OCP worker.Runtime interface: it polls the expiry queue
// every interval until ctx is cancelled, whose error it returns. The cadence
// is fixed-rate, not fixed-delay: each sleep is the interval minus the time
// the tick's processing took. A full batch polls again immediately, so a burst
// of requests ending together drains at processing speed rather than one batch
// per interval. It also runs the queue gauges (depth, max age) alongside the
// processing loop.
func (w *Worker) Start(ctx context.Context, interval time.Duration) error {
	go w.queueStatsGaugeWorker(ctx)

	for {
		start := time.Now()
		processed, err := w.Process(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			w.log.Warn("Failed to process payment request expiry queue", zap.Error(err))
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

// Process runs one worker tick: it pulls the due requests and expires them
// with bounded concurrency, reporting how many this worker took on (claimed).
// Zero means the due queue is drained: requests other workers hold claims on
// are not counted.
func (w *Worker) Process(runtimeCtx context.Context) (int, error) {
	// A context carrying no provider traces into the void rather than panicking:
	// tracing is observability, never a reason to fail a tick.
	metricsProvider, ok := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
	if !ok || metricsProvider == nil {
		metricsProvider = noop.NewProvider()
	}
	trace := metricsProvider.StartTrace("payment_request_expiry_worker")
	defer trace.End()
	tracedCtx := metrics.NewContext(runtimeCtx, trace)

	due, err := w.requests.GetDueForExpiry(tracedCtx, time.Now(), w.batchSize)
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

// processOne drives a single due request: it is claimed, read and expired,
// with a failed attempt rescheduled under backoff. The claim comes first, so
// of the workers that list the same due request only the one that wins it pays
// the request's strongly consistent read. It reports whether this worker took
// the request on.
func (w *Worker) processOne(ctx context.Context, task *ExpiryTask) bool {
	log := w.log.With(zap.String("payment_request_id", hex.EncodeToString(task.ID.Value)))

	claimed, err := w.requests.ClaimForExpiry(ctx, task.ID, time.Now(), time.Now().Add(w.claimLease))
	if err != nil {
		log.Warn("Failed to claim payment request for expiry", zap.Error(err))
		return false
	}
	if !claimed {
		// Another worker got there first, or the request was just made final.
		return false
	}

	if err := w.expire(ctx, task.ID); err != nil {
		attempts := task.Attempts + 1
		delay := w.backoffDelay(attempts)
		log.Warn("Payment request expiry attempt failed; rescheduling",
			zap.Error(err),
			zap.Uint32("attempts", attempts),
			zap.Duration("delay", delay),
		)
		if err := w.requests.DelayExpiry(ctx, task.ID, time.Now().Add(delay)); err != nil {
			log.Warn("Failed to reschedule payment request expiry", zap.Error(err))
		}
	}
	return true
}

// expire reads a claimed request and stores it expired, provided it has
// reached its ends_at by this server's clock. One that has not (the queue's
// due time is ExpireDelay past ends_at, so only a clock well behind the one
// that claimed it gets here) is put back on the queue at its due time. One
// already final is left to Expire's no-op, which only dequeues it should it
// still be queued. A failed read is a failed attempt like any other: the claim
// is held, so the request is rescheduled under backoff rather than left until
// its lease ends.
//
// Nothing is announced (see event.go): expiry moves no version, and every
// reader has judged the request EXPIRED off its ends_at since before the
// worker took it up.
func (w *Worker) expire(ctx context.Context, id *paymentrequestpb.PaymentRequestId) error {
	expireCtx, cancel := context.WithTimeout(ctx, w.expireTimeout)
	defer cancel()

	r, err := w.requests.Get(expireCtx, id)
	if err != nil {
		// Not found included: only a stored request is queued, and requests
		// are never deleted, so a claimed request that cannot be read is a
		// fault.
		return fmt.Errorf("loading payment request: %w", err)
	}

	if !r.Status.IsFinal() && time.Now().Before(r.EndsAt()) {
		// Not a failure, but DelayExpiry is the one way to reschedule, so it
		// counts as an attempt; only clock skew between servers gets here.
		if err := w.requests.DelayExpiry(expireCtx, id, ExpiryDueAt(r)); err != nil {
			return fmt.Errorf("rescheduling payment request not yet ended: %w", err)
		}
		return nil
	}

	_, err = w.requests.Expire(expireCtx, id)
	return err
}

// queueStatsGaugeWorker emits the expiry backlog gauges, depth and max age, as
// a recurring metric event until ctx is cancelled. Both count due requests
// alone (see ExpiryQueueStats): the depth is how many are waiting for a
// worker, and the max age how long the most overdue has waited past its due
// time, so a climbing max age means the worker is falling behind. Neither
// reads a request not yet due, so the cost follows the backlog, not the number
// of open requests. No due request reports a max age of zero. Emission is a
// no-op when ctx carries no metrics provider, and a failed stats poll is
// skipped rather than surfaced.
func (w *Worker) queueStatsGaugeWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(workerQueueStatsInterval):
			now := time.Now()
			stats, err := w.requests.GetExpiryQueueStats(ctx, now)
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
