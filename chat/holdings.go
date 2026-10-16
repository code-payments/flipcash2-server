package chat

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/metrics/noop"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"

	"github.com/code-payments/flipcash2-server/balance"
	"github.com/code-payments/flipcash2-server/model"
)

// A group's holdings are what its members hold of the currency the group is
// gated on (see GatedMint): a snapshot taken by a refresh that walks the
// roster and reads each member's balance, recorded with when it was taken
// (see GroupHoldings). They are shown on the group's metadata as
// holdings_summary wherever it is hydrated, and they rank a currency's
// groups for its community (see Store.GetTopGroupsByHoldings).
//
// A snapshot is derived data, never a gate: nothing decides access on it, so
// it may trail the roster and the balances by a refresh interval, and a
// group that has never been refreshed simply has none. Every refresh stores
// what it found, however little, but only a listed snapshot is shown or
// ranked: one where at least MinGroupHoldingsHolders members hold a positive
// amount (see GroupHoldings.Listed). Below that, the snapshot is kept, out of
// the ranking and never hydrated, so a group that drops below the minimum
// stops being listed at its next refresh, and one that rises above it starts.
// The store keeps the latest snapshot a group has been given and never moves
// it backwards (see Store.PutGroupHoldings), so concurrent or retried
// refreshes are safe.
//
// Every gated group is on a refresh queue, ordered by when it is next due
// (see HoldingsRefresh): enqueued when it is created (see Store.PutChat), or
// by a backfill for a group created before holdings existed (see
// Store.EnqueueHoldingsRefresh), and never removed, since a group's members
// and their balances keep changing for as long as it exists. A refresh claims
// a due group by pushing its due time out by a lease (see
// Store.ClaimHoldingsRefresh), so of the workers that list it only one takes
// it, and reschedules it when done or failed (see
// Store.RescheduleHoldingsRefresh). HoldingsWorker drains it.

// GatedMint returns the mint of the currency c is gated on, and false when
// it is gated on none. A group is gated on a currency when it is public and
// its minimum balance requirements, listener and speaker alike, all name the
// same one mint, and that mint is not the core mint (USDF, which every
// balance is valued in and no community is built around). The fiat currency
// a requirement is denominated in does not matter. A group with no minimum
// balance requirement is gated on nothing, as is one with a requirement that
// names no mint (and so counts every mint) or whose requirements name
// different mints, and a DM or a private group (which carries no rules)
// never is.
//
// It reads the record alone. A group can only be created gated on a currency
// launched on Flipcash and available (its creator is valued in it before the
// group is written), so the mint it returns is always one, and a refresh
// computes its holdings without asking.
func GatedMint(c *Chat) (*commonpb.PublicKey, bool) {
	if !IsGroupChatID(c.ID) || c.IsPrivate {
		return nil, false
	}

	var mint []byte
	for _, req := range []*MinimumBalance{c.MinimumListenerBalance, c.MinimumSpeakerBalance} {
		if req == nil {
			continue
		}
		if len(req.Mints) != 1 {
			return nil, false
		}
		named := req.Mints[0].GetValue()
		if mint != nil && !bytes.Equal(mint, named) {
			return nil, false
		}
		mint = named
	}
	if mint == nil {
		return nil, false
	}

	account, err := ocp_common.NewAccountFromPublicKeyBytes(mint)
	if err != nil || ocp_common.IsCoreMint(account) {
		return nil, false
	}
	return &commonpb.PublicKey{Value: append([]byte(nil), mint...)}, true
}

// GroupHoldings is one group's holdings snapshot (see GatedMint): what its
// members held of Mint, in Mint's quarks, and how many of them held any, as
// of AsOf. Holdings are counted for whoever was a member when the snapshot
// was taken, whether or not each still met the group's requirements.
type GroupHoldings struct {
	ChatID      *commonpb.ChatId
	Mint        *commonpb.PublicKey
	Quarks      uint64
	HolderCount uint64
	AsOf        time.Time
}

// MinGroupHoldingsHolders is the fewest members holding the currency a
// group's snapshot must count to be listed, and so shown and ranked (see
// GroupHoldings.Listed): a group with fewer is not one a community lists, and
// its total would say what a handful of people hold. The proto leaves the
// minimum to the server.
const MinGroupHoldingsHolders = 10

// ErrHoldingsMintMismatch is returned when a write names a mint other than
// the one a group's holdings already record (see Store.PutGroupHoldings and
// Store.EnqueueHoldingsRefresh). A group is gated on one mint, fixed at
// creation, so its queue entry and its snapshots are all of that mint; a
// write of another is a caller's error, refused rather than left to move the
// currency a refresh totals.
var ErrHoldingsMintMismatch = errors.New("holdings mint differs from the group's")

// ErrInvalidGroupHoldings is returned by ValidateGroupHoldings.
var ErrInvalidGroupHoldings = errors.New("invalid group holdings")

// ValidateGroupHoldings returns an error unless h is a snapshot a store
// accepts: a group chat ID, a mint the shape of a public key, a holder count
// that agrees with the total (no holders hold nothing, and holders hold
// something), and a time it was taken. Any number of holders is valid,
// including none: whether a snapshot is listed is GroupHoldings.Listed, not
// validity. It does not check that the group exists or is gated on Mint:
// those need the group's record, and are the caller's.
func ValidateGroupHoldings(h GroupHoldings) error {
	switch {
	case !IsGroupChatID(h.ChatID):
		return fmt.Errorf("%w: not a group chat id", ErrInvalidGroupHoldings)
	case len(h.Mint.GetValue()) != 32:
		return fmt.Errorf("%w: mint must be 32 bytes", ErrInvalidGroupHoldings)
	case h.HolderCount == 0 && h.Quarks > 0:
		return fmt.Errorf("%w: holdings with no holders", ErrInvalidGroupHoldings)
	case h.HolderCount > 0 && h.Quarks == 0:
		return fmt.Errorf("%w: holders holding nothing", ErrInvalidGroupHoldings)
	case h.AsOf.IsZero():
		return fmt.Errorf("%w: as_of is required", ErrInvalidGroupHoldings)
	}
	return nil
}

// Listed reports whether the snapshot is shown and ranked: a positive total
// held by at least MinGroupHoldingsHolders members. Every holder holds a
// positive amount, so the holder minimum implies the positive total; both
// are checked all the same. A snapshot that is not listed is stored but never
// ranked (see Store.GetTopGroupsByHoldings) nor hydrated as holdings_summary;
// any other reader of Store.GetGroupHoldings must apply it too.
func (h GroupHoldings) Listed() bool {
	return h.Quarks > 0 && h.HolderCount >= MinGroupHoldingsHolders
}

// Clone returns a deep copy of the snapshot.
func (h GroupHoldings) Clone() GroupHoldings {
	out := h
	if h.ChatID != nil {
		out.ChatID = &commonpb.ChatId{Value: append([]byte(nil), h.ChatID.Value...)}
	}
	if h.Mint != nil {
		out.Mint = &commonpb.PublicKey{Value: append([]byte(nil), h.Mint.Value...)}
	}
	return out
}

// ToProto projects the snapshot onto a chatpb.HoldingsSummary.
func (h GroupHoldings) ToProto() *chatpb.HoldingsSummary {
	return &chatpb.HoldingsSummary{
		Mint:        &commonpb.PublicKey{Value: append([]byte(nil), h.Mint.GetValue()...)},
		Quarks:      h.Quarks,
		HolderCount: h.HolderCount,
		AsOf:        timestamppb.New(h.AsOf),
	}
}

// HoldingsLess orders snapshots as a currency's groups are ranked: by
// Quarks descending, then by chat ID descending, so the order is total for
// any set of distinct groups, and a position in it (quarks, chat ID) is one a
// paged read could resume strictly after.
func HoldingsLess(a, b GroupHoldings) bool {
	if a.Quarks != b.Quarks {
		return a.Quarks > b.Quarks
	}
	return bytes.Compare(a.ChatID.GetValue(), b.ChatID.GetValue()) > 0
}

// HoldingsRefresh is a gated group on the refresh queue: its mint, as the
// queue recorded it from the group's rules (which never change), when it is
// next due, and how many refreshes in a row have failed since the last that
// succeeded.
type HoldingsRefresh struct {
	ChatID   *commonpb.ChatId
	Mint     *commonpb.PublicKey
	DueAt    time.Time
	Attempts uint32
}

const (
	// defaultHoldingsRefreshInterval is how long after a refresh a group is
	// next due. It is how stale a group's holdings may be, and so its place
	// in its community's ranking: the proto leaves the rate to the server.
	// Every 4 hours to start, the same for every group whatever its size.
	defaultHoldingsRefreshInterval = 4 * time.Hour

	// defaultHoldingsWorkerBatchSize is how many due groups one tick pulls
	// from the queue.
	defaultHoldingsWorkerBatchSize = 32

	// defaultHoldingsWorkerMaxConcurrency bounds how many groups are
	// refreshed at once. A refresh is reads (roster pages, balances), so this
	// bounds the load one server puts on the chat store and OCP.
	defaultHoldingsWorkerMaxConcurrency = 4

	// defaultHoldingsWorkerBackoffBase and defaultHoldingsWorkerMaxBackoffDelay
	// shape the exponential backoff after failed refreshes. The cap is the
	// refresh interval, so a failing group is retried no less often than a
	// healthy one is refreshed, and there is no limit on attempts: a group
	// stays on the queue for as long as it exists.
	defaultHoldingsWorkerBackoffBase     = 10 * time.Second
	defaultHoldingsWorkerMaxBackoffDelay = defaultHoldingsRefreshInterval

	// defaultHoldingsWorkerClaimLease is how far a claim pushes a group's due time
	// out. It must comfortably exceed the refresh timeout so a group cannot
	// come due again while its claimant is still refreshing it.
	defaultHoldingsWorkerClaimLease = 10 * time.Minute

	// defaultHoldingsWorkerRefreshTimeout bounds a single refresh attempt.
	defaultHoldingsWorkerRefreshTimeout = 5 * time.Minute

	// defaultHoldingsRosterPageSize is how many members a refresh reads per roster
	// page, and so asks OCP about per balance read: under OCP's batch limit,
	// so a page is one call.
	defaultHoldingsRosterPageSize = 1000
)

// HoldingsWorker drains the holdings refresh queue (see HoldingsRefresh): it
// polls the chat store for gated groups whose refresh is due, totals what
// each one's members hold of the currency it is gated on (see refresh), and
// records the snapshot the community's ranking is read from.
//
// It implements the OCP worker.Runtime interface, so the parent application
// registers it alongside its other background runtimes and controls the poll
// interval. It is safe to run on every server instance: workers claim a group
// before refreshing it, and a snapshot never replaces a newer one (see
// Store.PutGroupHoldings), so overlap costs duplicate reads at worst,
// never a stale ranking.
//
// Construct with NewHoldingsWorker and either run it via Start (the OCP
// runtime entry point) or drive single ticks with Process (tests).
type HoldingsWorker struct {
	log      *zap.Logger
	chats    Store
	balances *balance.Client

	refreshInterval time.Duration
	batchSize       int
	maxConcurrency  int
	backoffBase     time.Duration
	maxBackoffDelay time.Duration
	claimLease      time.Duration
	refreshTimeout  time.Duration
	rosterPageSize  int
}

// HoldingsWorkerOption overrides one of the worker's tuning knobs.
type HoldingsWorkerOption func(*HoldingsWorker)

// WithHoldingsRefreshInterval overrides how long after a refresh a group is next due.
func WithHoldingsRefreshInterval(interval time.Duration) HoldingsWorkerOption {
	return func(w *HoldingsWorker) { w.refreshInterval = interval }
}

// WithHoldingsWorkerBatchSize overrides how many due groups one tick pulls from the
// queue.
func WithHoldingsWorkerBatchSize(n int) HoldingsWorkerOption {
	return func(w *HoldingsWorker) { w.batchSize = n }
}

// WithHoldingsWorkerMaxConcurrency overrides how many groups are refreshed at once.
func WithHoldingsWorkerMaxConcurrency(n int) HoldingsWorkerOption {
	return func(w *HoldingsWorker) { w.maxConcurrency = n }
}

// WithHoldingsWorkerBackoff overrides the retry backoff's base and maximum delay.
func WithHoldingsWorkerBackoff(base, maxDelay time.Duration) HoldingsWorkerOption {
	return func(w *HoldingsWorker) {
		w.backoffBase = base
		w.maxBackoffDelay = maxDelay
	}
}

// WithHoldingsWorkerClaimLease overrides how far a claim pushes a group's due time
// out.
func WithHoldingsWorkerClaimLease(lease time.Duration) HoldingsWorkerOption {
	return func(w *HoldingsWorker) { w.claimLease = lease }
}

// WithHoldingsWorkerRefreshTimeout overrides the bound on a single refresh attempt.
func WithHoldingsWorkerRefreshTimeout(timeout time.Duration) HoldingsWorkerOption {
	return func(w *HoldingsWorker) { w.refreshTimeout = timeout }
}

// WithHoldingsRosterPageSize overrides how many members a refresh reads per roster
// page, for tests that walk several pages without thousands of members.
func WithHoldingsRosterPageSize(n int) HoldingsWorkerOption {
	return func(w *HoldingsWorker) { w.rosterPageSize = n }
}

// NewHoldingsWorker returns a HoldingsWorker draining the chat store's
// holdings refresh queue, reading members' holdings through balances.
func NewHoldingsWorker(log *zap.Logger, chats Store, balances *balance.Client, opts ...HoldingsWorkerOption) *HoldingsWorker {
	w := &HoldingsWorker{
		log:      log,
		chats:    chats,
		balances: balances,

		refreshInterval: defaultHoldingsRefreshInterval,
		batchSize:       defaultHoldingsWorkerBatchSize,
		maxConcurrency:  defaultHoldingsWorkerMaxConcurrency,
		backoffBase:     defaultHoldingsWorkerBackoffBase,
		maxBackoffDelay: defaultHoldingsWorkerMaxBackoffDelay,
		claimLease:      defaultHoldingsWorkerClaimLease,
		refreshTimeout:  defaultHoldingsWorkerRefreshTimeout,
		rosterPageSize:  defaultHoldingsRosterPageSize,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Start satisfies the OCP worker.Runtime interface: it polls the refresh
// queue every interval until ctx is cancelled, whose error it returns. The
// cadence is fixed-rate, not fixed-delay: each sleep is the interval minus the
// time the tick's processing took. A full batch polls again immediately, so a
// backlog drains at processing speed rather than one batch per interval.
func (w *HoldingsWorker) Start(ctx context.Context, interval time.Duration) error {
	for {
		start := time.Now()
		processed, err := w.Process(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			w.log.Warn("Failed to process holdings refresh queue", zap.Error(err))
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

// Process runs one worker tick: it pulls the due groups and refreshes them
// with bounded concurrency, reporting how many this worker took on (claimed).
// Zero means the due queue is drained: groups other workers hold claims on
// are not counted.
func (w *HoldingsWorker) Process(runtimeCtx context.Context) (int, error) {
	// A context carrying no provider traces into the void rather than panicking:
	// tracing is observability, never a reason to fail a tick.
	metricsProvider, ok := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
	if !ok || metricsProvider == nil {
		metricsProvider = noop.NewProvider()
	}
	trace := metricsProvider.StartTrace("chat_holdings_worker")
	defer trace.End()
	tracedCtx := metrics.NewContext(runtimeCtx, trace)

	due, err := w.chats.GetDueHoldingsRefreshes(tracedCtx, time.Now(), w.batchSize)
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

// processOne drives a single due group: it is claimed, refreshed and
// rescheduled, a refresh interval out when it succeeded and under backoff
// when it failed. It reports whether this worker took the group on.
func (w *HoldingsWorker) processOne(ctx context.Context, task HoldingsRefresh) bool {
	log := w.log.With(zap.String("chat_id", model.ChatIDString(task.ChatID)))

	claimed, err := w.chats.ClaimHoldingsRefresh(ctx, task.ChatID, time.Now(), time.Now().Add(w.claimLease))
	if err != nil {
		log.Warn("Failed to claim group for holdings refresh", zap.Error(err))
		return false
	}
	if !claimed {
		// Another worker got there first.
		return false
	}

	nextDueAt, attempts := time.Now().Add(w.refreshInterval), uint32(0)
	if err := w.refresh(ctx, task); err != nil {
		attempts = task.Attempts + 1
		delay := w.backoffDelay(attempts)
		nextDueAt = time.Now().Add(delay)
		log.Warn("Holdings refresh failed; rescheduling",
			zap.Error(err),
			zap.Uint32("attempts", attempts),
			zap.Duration("delay", delay),
		)
	}
	if err := w.chats.RescheduleHoldingsRefresh(ctx, task.ChatID, nextDueAt, attempts); err != nil {
		// The claim's lease brings it back regardless.
		log.Warn("Failed to reschedule holdings refresh", zap.Error(err))
	}
	return true
}

// refresh totals what a claimed group's members hold of its mint and records
// the snapshot, listed or not (see GroupHoldings.Listed): one below the
// minimum replaces the stored one all the same, which is what takes a group
// that drops below it out of the ranking. The snapshot is as of the moment
// the refresh began, and counts whoever the roster walk finds joined: a
// member who joins or leaves during the walk may or may not count, which the
// next refresh settles. The mint is the queue entry's, which is the group's
// gated mint (see GatedMint), so it is a launched currency and is not looked
// up.
func (w *HoldingsWorker) refresh(ctx context.Context, task HoldingsRefresh) error {
	ctx, cancel := context.WithTimeout(ctx, w.refreshTimeout)
	defer cancel()

	asOf := time.Now()

	quarks, holders, err := w.memberHoldings(ctx, task.ChatID, task.Mint)
	if err != nil {
		return err
	}

	_, err = w.chats.PutGroupHoldings(ctx, GroupHoldings{
		ChatID:      task.ChatID,
		Mint:        task.Mint,
		Quarks:      quarks,
		HolderCount: holders,
		AsOf:        asOf,
	})
	return err
}

// memberHoldings walks a group's roster a page at a time, reading each page's
// holdings of mint in one balance read, and returns the total quarks held
// and how many members held any. Each member is on the roster once, so the
// total is at most the mint's supply, which fits a uint64; an overflow is
// reported rather than wrapped all the same.
func (w *HoldingsWorker) memberHoldings(ctx context.Context, chatID *commonpb.ChatId, mint *commonpb.PublicKey) (quarks, holders uint64, err error) {
	var after *commonpb.UserId
	for {
		page, err := w.chats.GetGroupMembersPage(ctx, chatID, after, w.rosterPageSize)
		if err != nil {
			return 0, 0, fmt.Errorf("reading roster page: %w", err)
		}
		if len(page.Users) > 0 {
			held, err := w.balances.GetMintQuarksForUsers(ctx, mint, page.Users...)
			if err != nil {
				return 0, 0, fmt.Errorf("reading member holdings: %w", err)
			}
			for _, q := range held {
				var carry uint64
				quarks, carry = bits.Add64(quarks, q, 0)
				if carry != 0 {
					return 0, 0, errors.New("member holdings overflow")
				}
				holders++
			}
		}
		if page.Next == nil {
			return quarks, holders, nil
		}
		after = page.Next
	}
}

// backoffDelay is the exponential retry delay after the given number of failed
// attempts (>= 1), capped at the configured maximum.
func (w *HoldingsWorker) backoffDelay(attempts uint32) time.Duration {
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
