package blocklist

import (
	"context"
	"time"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

// Cursor marks a position within a blocklist read. The next page resumes at the
// entry immediately after (BlockedAt, UserID) in the list's descending
// (blocked_at, user_id) order.
type Cursor struct {
	BlockedAt time.Time
	UserID    *commonpb.UserId
}

// Store persists each user's blocklist: the set of users they have blocked.
//
// A blocklist is scoped to its owner (the blocker); ownerID keys every method.
// The only ordering exposed is most-recently-blocked first, so blocked_at is
// recorded per entry and is fixed at the time of blocking — re-blocking an
// already-blocked user preserves the original blocked_at (see Block).
type Store interface {
	// Block adds blockedID to ownerID's blocklist, recording blockedAt as the
	// time it was blocked. Blocking a user already on the list is a no-op that
	// preserves the existing entry (and its original blocked_at). It reports
	// whether a new entry was added.
	Block(ctx context.Context, ownerID, blockedID *commonpb.UserId, blockedAt time.Time) (added bool, err error)

	// Unblock removes blockedID from ownerID's blocklist. Removing a user that
	// is not on the list is a no-op. It reports whether an entry was removed.
	Unblock(ctx context.Context, ownerID, blockedID *commonpb.UserId) (removed bool, err error)

	// IsBlocked reports whether blockedID is on ownerID's blocklist.
	IsBlocked(ctx context.Context, ownerID, blockedID *commonpb.UserId) (bool, error)

	// GetBlockedCount returns the number of users on ownerID's blocklist. It is a
	// maintained aggregate, not a scan, so it is a cheap O(1) read intended as a
	// signal for sizing a read strategy (e.g. a range scan while small, a cached
	// set once large). An owner who has blocked no one reports 0.
	GetBlockedCount(ctx context.Context, ownerID *commonpb.UserId) (int, error)

	// GetBlocked returns which of candidateIDs are on ownerID's blocklist, as a
	// set keyed by string(userID.Value): a candidate is present (value true) iff
	// ownerID has blocked it. Candidates that are not blocked — along with
	// duplicate or empty input — are simply absent from the map.
	//
	// It is the batch form of IsBlocked, for checking one owner against many
	// candidates in a single round trip (e.g. resolving hidden state for a page of
	// DM peers) rather than a lookup per candidate.
	GetBlocked(ctx context.Context, ownerID *commonpb.UserId, candidateIDs []*commonpb.UserId) (map[string]bool, error)

	// GetBlocklistPage returns one page of ownerID's blocklist ordered by
	// (blocked_at, user_id) descending (most recently blocked first), at most
	// limit entries (limit <= 0 means unbounded). When cursor is nil the page
	// starts at the most recently blocked entry; otherwise it resumes strictly
	// after cursor. An empty result (no error) is returned when no entries
	// remain.
	//
	// Unlike a chat feed, a blocklist entry's sort key (blocked_at) never
	// changes once written, so a multi-page read needs no snapshot watermark: an
	// entry can neither move within the ordering nor be duplicated across pages.
	// Newly-blocked users sort above any cursor and simply do not appear until
	// the list is read afresh.
	GetBlocklistPage(ctx context.Context, ownerID *commonpb.UserId, cursor *Cursor, limit int) ([]*BlockedUser, error)
}
