package e2ee

import (
	"strconv"
	"time"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// BucketConfig describes a rate limit as a token bucket does: it admits a
// burst of Size and a sustained rate of one per Refill, and a take that
// does not fit is refused with how long until it would. Stores keep it as
// a sliding window rather than a bucket: Size permits over Window (the
// time the bucket would take to refill from empty), with the window
// before the current one counted at the share of it still in view. The
// two shapes admit the same burst and the same rate; what the window
// gives up is exactness within one window, in exchange for a take that is
// one unconditional ADD, so a key many callers hit at once (one
// recipient's inbound bytes) never contends and never retries. Signal
// keeps its buckets exact in one Redis script; DynamoDB has no script,
// and a read-then-compare-and-set loses races under exactly the load a
// limit exists for. A cost above Size can never be served.
//
// Size and Refill are positive.
type BucketConfig struct {
	Size   int64
	Refill time.Duration
}

// Window is the sliding window's length: Size times Refill, the time the
// bucket the config describes would take to refill from empty.
func (c BucketConfig) Window() time.Duration {
	return time.Duration(c.Size) * c.Refill
}

// WindowIndex is the index of the window now falls in: windows are
// aligned to the Unix epoch, so every store and every server agrees on
// them without coordination.
func (c BucketConfig) WindowIndex(now time.Time) int64 {
	index, _ := c.position(now)
	return index
}

// position is the window now falls in and how far through it now is, in
// [0, 1).
func (c BucketConfig) position(now time.Time) (index int64, elapsed float64) {
	w := int64(c.Window())
	if w <= 0 {
		return 0, 0
	}
	n := now.UnixNano()
	index = n / w
	return index, float64(n-index*w) / float64(w)
}

// WindowCounts is what a store holds per limit key: how much was taken
// in one window and in the one before it. Nothing older matters.
type WindowCounts struct {
	// Window is the index of the window Count is for.
	Window int64

	// Count is what was taken in Window.
	Count int64

	// PrevCount is what was taken in Window-1.
	PrevCount int64
}

// At returns the counts as they stand at now: moved forward to now's
// window, with what has fallen out of view dropped.
func (w WindowCounts) At(cfg BucketConfig, now time.Time) WindowCounts {
	index := cfg.WindowIndex(now)
	switch w.Window {
	case index:
		return w
	case index - 1:
		return WindowCounts{Window: index, PrevCount: w.Count}
	default:
		return WindowCounts{Window: index}
	}
}

// Decide reports whether cost fits at now, given counts already At now,
// and when it does not, how long until it would: the use in view is the
// previous window's count at the share of it still in view plus the
// current window's, and cost fits when that plus cost is within Size.
// The wait is exact for the counts as they stand: within this window as
// the previous one's share decays, or past it once enough of this one
// has. A cost above Size never fits and waits a whole Window.
func (c BucketConfig) Decide(w WindowCounts, now time.Time, cost int64) (ok bool, retry time.Duration) {
	if cost > c.Size {
		return false, c.Window()
	}
	_, f := c.position(now)
	prev, cur := float64(w.PrevCount), float64(w.Count)
	size, need := float64(c.Size), float64(cost)
	if prev*(1-f)+cur+need <= size {
		return true, 0
	}
	window := float64(c.Window())
	if room := size - need - cur; room >= 0 {
		// prev is positive here: with none, the take would have fit.
		fit := 1 - room/prev
		return false, time.Duration((fit - f) * window)
	}
	fit := max(0, 1-(size-need)/cur)
	return false, time.Duration((1-f)*window + fit*window)
}

// Limits are the server's rate limits, each a BucketConfig per key (see
// Store.TakeTokens). The shapes and defaults are Signal-Server's, whose
// limiters they mirror: a bundle fetch is limited per (caller device,
// target device, target registration ID), so a device that registers anew
// is a fresh target with a fresh budget, and per caller device across all
// targets, sized so a caller who has just joined a chat fetches its whole
// roster in one call; a send is limited per (sender user, recipient user)
// and by the bytes a recipient user takes in, the peer only, since a
// sender's own devices are its own. A refused fetch fails closed and a
// refused send fails open on a limiter error, as Signal's do.
type Limits struct {
	BundlesPerTarget BucketConfig
	BundlesPerCaller BucketConfig
	SendsPerPair     BucketConfig
	InboundBytes     BucketConfig
}

// DefaultLimits are Signal-Server's sizes, with the per-caller bundle
// budget sized to a full group roster.
var DefaultLimits = Limits{
	BundlesPerTarget: BucketConfig{Size: 6, Refill: 10 * time.Minute},
	BundlesPerCaller: BucketConfig{Size: 250, Refill: time.Minute},
	SendsPerPair:     BucketConfig{Size: 60, Refill: time.Second},
	InboundBytes:     BucketConfig{Size: 128 << 20, Refill: 500 * time.Microsecond},
}

// The bucket kinds, which namespace their keys in the store.
const (
	limitBundleTarget = "bundle-target"
	limitBundleCaller = "bundle-caller"
	limitSendPair     = "send-pair"
	limitInbound      = "inbound"
)

// deviceKeyString is a printable, unambiguous form of an address for a
// bucket key.
func deviceKeyString(a DeviceAddress) string {
	return model.UserIDString(a.UserID) + "." + strconv.FormatUint(uint64(a.DeviceID), 10)
}

func bundleTargetKey(caller DeviceAddress, target *Device) string {
	return deviceKeyString(caller) + "__" + deviceKeyString(target.Address) + "." + strconv.FormatUint(uint64(target.RegistrationID), 10)
}

func sendPairKey(sender, recipient *commonpb.UserId) string {
	return model.UserIDString(sender) + "__" + model.UserIDString(recipient)
}
