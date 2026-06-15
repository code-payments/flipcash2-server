package badge

import (
	"context"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

// Store persists per-user app-icon badge counts.
//
// The count is a "notifications since last open" counter, not a live unread
// total: Increment bumps it as unread-eligible messages fan out to a user, and
// Reset zeroes it when the user opens the app. All mutations are atomic, so the
// concurrent increments produced by a noisy chat's fan-out serialize on the
// user's item rather than racing through a read-modify-write.
type Store interface {
	// Increment atomically adds delta to a user's badge count and returns the
	// resulting value. A user with no badge yet is treated as zero. delta is
	// typically 1 per unread-eligible message, or a larger amount when several
	// have been coalesced.
	Increment(ctx context.Context, userID *commonpb.UserId, delta uint64) (uint64, error)

	// Get returns a user's current badge count, or zero if the user has none.
	Get(ctx context.Context, userID *commonpb.UserId) (uint64, error)

	// Reset sets a user's badge count back to zero, e.g. when the app is opened.
	Reset(ctx context.Context, userID *commonpb.UserId) error
}
