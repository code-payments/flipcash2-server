package push

import (
	"context"
	"errors"
	"time"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"
)

// ErrCurrencyStateNotFound is returned when no push state exists for a mint.
var ErrCurrencyStateNotFound = errors.New("currency state not found")

// Token represents a push notification token.
//
// Tokens are bound to a (user, device) pair, identified by the AppInstallID.
type Token struct {
	Type         pushpb.TokenType
	Token        string
	AppInstallID string
}

type TokenStore interface {
	// GetTokens returns all tokens for a user.
	GetTokens(ctx context.Context, userID *commonpb.UserId) ([]Token, error)

	// GetTokensBatch returns all tokens for a batch of users.
	GetTokensBatch(ctx context.Context, userIDs ...*commonpb.UserId) ([]Token, error)

	// AddToken adds a token for a user.
	//
	// If the token already exists for the same user and device, it will be updated.
	AddToken(ctx context.Context, userID *commonpb.UserId, appInstallID *commonpb.AppInstallId, tokenType pushpb.TokenType, token string) error

	// DeleteToken deletes a token for a user.
	DeleteToken(ctx context.Context, tokenType pushpb.TokenType, token string) error

	// FilterUsersWithTokens returns the subset of user IDs that have at least one push token.
	FilterUsersWithTokens(ctx context.Context, userIDs ...*commonpb.UserId) ([]*commonpb.UserId, error)
}

// CurrencyState captures per-currency state used to gate push notifications.
//
// AllTimeHighSupply tracks the circulating supply (in quarks) observed at the
// last granted gain push, NOT a true high-water mark: it only advances when a
// gain push is actually granted (see CurrencyStateStore.ClaimGainPush).
type CurrencyState struct {
	Mint              *commonpb.PublicKey
	AllTimeHighSupply uint64
	AllTimeHighSlot   uint64
	LastGainPushAt    *time.Time // nil if a gain push has never been granted for the mint
}

type CurrencyStateStore interface {
	// ClaimGainPush atomically gates a "currency gain" push wave for a mint.
	//
	// granted is true only when both conditions hold against the currently stored
	// state: (1) supply is strictly greater than the stored all-time high, and
	// (2) at least cooldown has elapsed since the last granted gain push (or none
	// has ever been granted). When granted, the stored all-time high (supply,
	// slot) and last-gain-push timestamp are advanced to the provided values / now.
	// When not granted, the stored state is left unchanged — in particular a new
	// high observed during cooldown is NOT recorded.
	//
	// Whenever err is nil, the returned CurrencyState reflects the resulting stored
	// state — the freshly written values when granted, or the existing values when
	// not — so callers can populate a local cache regardless of the outcome.
	ClaimGainPush(ctx context.Context, mint *commonpb.PublicKey, supply, slot uint64, cooldown time.Duration) (granted bool, state *CurrencyState, err error)

	// GetCurrencyState returns the stored push state for a mint, or
	// ErrCurrencyStateNotFound if none has been recorded yet.
	GetCurrencyState(ctx context.Context, mint *commonpb.PublicKey) (*CurrencyState, error)
}

// Store is the combined push persistence surface, implemented by each backend.
type Store interface {
	TokenStore
	CurrencyStateStore
}
