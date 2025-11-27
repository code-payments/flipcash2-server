package memory

import (
	"context"
	"sync"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"

	"github.com/code-payments/flipcash2-server/push"
)

type memory struct {
	sync.RWMutex

	// Map of userID -> map of appInstallID -> Token
	tokens map[string]map[string]push.Token
}

func NewInMemory() push.TokenStore {
	return &memory{
		tokens: make(map[string]map[string]push.Token),
	}
}

func (m *memory) reset() {
	m.Lock()
	defer m.Unlock()

	m.tokens = make(map[string]map[string]push.Token)
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
