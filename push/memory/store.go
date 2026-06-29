package memory

import (
	"context"
	"sync"
	"time"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"

	"github.com/code-payments/flipcash2-server/push"
)

type memory struct {
	sync.RWMutex

	// Map of userID -> map of appInstallID -> Token
	tokens map[string]map[string]push.Token

	// Map of mint -> currency push state
	currencyStates map[string]push.CurrencyState
}

func NewInMemory() push.Store {
	return &memory{
		tokens:         make(map[string]map[string]push.Token),
		currencyStates: make(map[string]push.CurrencyState),
	}
}

func (m *memory) reset() {
	m.Lock()
	defer m.Unlock()

	m.tokens = make(map[string]map[string]push.Token)
	m.currencyStates = make(map[string]push.CurrencyState)
}

func (m *memory) GetTokens(_ context.Context, userID *commonpb.UserId) ([]push.Token, error) {
	m.RLock()
	defer m.RUnlock()

	userTokens, ok := m.tokens[string(userID.Value)]
	if !ok {
		return nil, nil
	}

	tokens := make([]push.Token, 0, len(userTokens))
	for _, token := range userTokens {
		tokens = append(tokens, token)
	}

	return tokens, nil
}

func (m *memory) GetTokensBatch(ctx context.Context, userIDs ...*commonpb.UserId) ([]push.Token, error) {
	m.RLock()
	defer m.RUnlock()

	var tokens []push.Token
	for _, userID := range userIDs {
		userTokens, ok := m.tokens[string(userID.Value)]
		if !ok {
			continue
		}

		for _, token := range userTokens {
			tokens = append(tokens, token)
		}
	}

	return tokens, nil
}

func (m *memory) AddToken(_ context.Context, userID *commonpb.UserId, appInstallID *commonpb.AppInstallId, tokenType pushpb.TokenType, token string) error {
	m.Lock()
	defer m.Unlock()

	userTokens, ok := m.tokens[string(userID.Value)]
	if !ok {
		userTokens = make(map[string]push.Token)
		m.tokens[string(userID.Value)] = userTokens
	}

	userTokens[appInstallID.Value] = push.Token{
		Type:         tokenType,
		Token:        token,
		AppInstallID: appInstallID.Value,
	}

	return nil
}

func (m *memory) FilterUsersWithTokens(_ context.Context, userIDs ...*commonpb.UserId) ([]*commonpb.UserId, error) {
	m.RLock()
	defer m.RUnlock()

	var result []*commonpb.UserId
	for _, userID := range userIDs {
		if tokens, ok := m.tokens[string(userID.Value)]; ok && len(tokens) > 0 {
			result = append(result, userID)
		}
	}
	return result, nil
}

func (m *memory) DeleteToken(_ context.Context, tokenType pushpb.TokenType, token string) error {
	m.Lock()
	defer m.Unlock()

	// Need to scan all users and devices to find matching token.
	for _, userTokens := range m.tokens {
		for appInstallID, existingToken := range userTokens {
			if existingToken.Type == tokenType && existingToken.Token == token {
				delete(userTokens, appInstallID)
			}
		}
	}

	return nil
}

func (m *memory) ClaimGainPush(_ context.Context, mint *commonpb.PublicKey, supply, slot uint64, cooldown time.Duration) (bool, *push.CurrencyState, error) {
	m.Lock()
	defer m.Unlock()

	key := string(mint.Value)
	now := time.Now()
	state, ok := m.currencyStates[key]

	isNewHigh := !ok || supply > state.AllTimeHighSupply
	cooldownElapsed := !ok || state.LastGainPushAt == nil || now.Sub(*state.LastGainPushAt) >= cooldown
	if !isNewHigh || !cooldownElapsed {
		// Not a new high, or still within cooldown: leave the stored state untouched
		// but still return it so callers can populate a local cache.
		return false, cloneCurrencyState(state), nil
	}

	grantedAt := now
	state = push.CurrencyState{
		Mint:              mint,
		AllTimeHighSupply: supply,
		AllTimeHighSlot:   slot,
		LastGainPushAt:    &grantedAt,
	}
	m.currencyStates[key] = state
	return true, cloneCurrencyState(state), nil
}

func (m *memory) GetCurrencyState(_ context.Context, mint *commonpb.PublicKey) (*push.CurrencyState, error) {
	m.RLock()
	defer m.RUnlock()

	state, ok := m.currencyStates[string(mint.Value)]
	if !ok {
		return nil, push.ErrCurrencyStateNotFound
	}
	return cloneCurrencyState(state), nil
}

// cloneCurrencyState returns a deep copy so callers can't mutate the stored
// LastGainPushAt through the returned pointer.
func cloneCurrencyState(state push.CurrencyState) *push.CurrencyState {
	clone := state
	if state.LastGainPushAt != nil {
		t := *state.LastGainPushAt
		clone.LastGainPushAt = &t
	}
	return &clone
}
