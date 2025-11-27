package push

import (
	"context"

	"firebase.google.com/go/v4/messaging"
	"go.uber.org/zap"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

type Pusher interface {
	SendBasicPushes(ctx context.Context, title, body string, users ...*commonpb.UserId) error
}

type NoOpPusher struct{}

func (n *NoOpPusher) SendBasicPushes(_ context.Context, _, _ string, _ ...*commonpb.UserId) error {
	return nil
}

func NewNoOpPusher() Pusher {
	return &NoOpPusher{}
}

type FCMPusher struct {
	log    *zap.Logger
	tokens TokenStore
	client FCMClient
}

type FCMClient interface {
	SendEachForMulticast(ctx context.Context, message *messaging.MulticastMessage) (*messaging.BatchResponse, error)
}

func NewFCMPusher(log *zap.Logger, tokens TokenStore, client FCMClient) *FCMPusher {
	return &FCMPusher{
		log:    log,
		tokens: tokens,
		client: client,
	}
}

// todo: Some duplicated code, but the existing push per message flow is likely going away anyways. We'll refactor when we get to that.
func (p *FCMPusher) SendBasicPushes(ctx context.Context, title, body string, users ...*commonpb.UserId) error {
	if len(users) == 0 {
		return nil
	}

	pushTokens, err := p.getTokenList(ctx, users)
	if err != nil {
		return err
	}

	// A single MulticastMessage may contain up to 500 registration tokens.
	if len(pushTokens) > 500 {
		p.log.Warn("Dropping push, too many tokens", zap.Int("num_tokens", len(pushTokens)))
		return nil
	}
	if len(pushTokens) == 0 {
		p.log.Debug("Dropping push, no tokens for users", zap.Int("num_users", len(users)))
		return nil
	}

	tokens := extractTokens(pushTokens)

	message := &messaging.MulticastMessage{
		Tokens: tokens,
		Notification: &messaging.Notification{
			Title: title,
			Body:  body,
		},
	}

	response, err := p.client.SendEachForMulticast(ctx, message)
	if err != nil {
		return err
	}

	if response == nil {
		p.log.Debug("No response from FCM")
		return nil
	}

	p.log.Debug("Send pushes", zap.Int("success", response.SuccessCount), zap.Int("failed", response.FailureCount))
	if response.FailureCount == 0 {
		return nil
	}

	p.processResponse(response, pushTokens, tokens)

	return nil
}

func (p *FCMPusher) processResponse(response *messaging.BatchResponse, pushTokens []Token, tokens []string) {
	var invalidTokens []Token

	for i, resp := range response.Responses {
		if resp.Success {
			continue
		}

		if messaging.IsUnregistered(resp.Error) {
			invalidTokens = append(invalidTokens, pushTokens[i])
		} else {
			p.log.Warn("Failed to send push notification",
				zap.Error(resp.Error),
				zap.String("token", tokens[i]),
			)
		}
	}

	if len(invalidTokens) > 0 {
		go func() {
			ctx := context.Background()
			for _, token := range invalidTokens {
				_ = p.tokens.DeleteToken(ctx, token.Type, token.Token)
			}
			p.log.Debug("Removed invalid tokens", zap.Int("count", len(invalidTokens)))
		}()
	}
}

func (p *FCMPusher) getTokenList(ctx context.Context, users []*commonpb.UserId) ([]Token, error) {
	allPushTokens, err := p.tokens.GetTokensBatch(ctx, users...)
	if err != nil {
		return nil, err
	}
	return allPushTokens, nil
}

func extractTokens(pushTokens []Token) []string {
	tokens := make([]string, len(pushTokens))
	for i, token := range pushTokens {
		tokens[i] = token.Token
	}
	return tokens
}
