package push

import (
	"context"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"
)

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
}
