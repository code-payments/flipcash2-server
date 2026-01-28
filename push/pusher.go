package push

import (
	"context"
	"encoding/base64"
	"fmt"

	"firebase.google.com/go/v4/messaging"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"
	"github.com/mr-tron/base58"
)

type Pusher interface {
	SendPushes(ctx context.Context, title, body string, customPayload *pushpb.Payload, users ...*commonpb.UserId) error
}

type NoOpPusher struct{}

func (n *NoOpPusher) SendPushes(_ context.Context, _, _ string, _ *pushpb.Payload, _ ...*commonpb.UserId) error {
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

func (p *FCMPusher) SendPushes(ctx context.Context, title, body string, customPayload *pushpb.Payload, users ...*commonpb.UserId) error {
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

	if customPayload == nil {
		customPayload = &pushpb.Payload{}
	}

	err = customPayload.Validate()
	if err != nil {
		return err
	}

	marshalledCustomPayload, err := proto.Marshal(customPayload)
	if err != nil {
		return err
	}
	encodedCustomPayload := base64.StdEncoding.EncodeToString(marshalledCustomPayload)

	customData := map[string]string{
		"flipcash_payload": encodedCustomPayload,
	}
	if customPayload.Navigation != nil {
		var targetUrl string
		switch typed := customPayload.Navigation.Type.(type) {
		case *pushpb.Navigation_CurrencyInfo:
			targetUrl = fmt.Sprintf("https://app.flipcash.com/token/%s", base58.Encode(typed.CurrencyInfo.Value))
		}
		if len(targetUrl) > 0 {
			customData["target_url"] = targetUrl
		}
	}

	message := &messaging.MulticastMessage{
		Tokens: tokens,
		Notification: &messaging.Notification{
			Title: title,
			Body:  body,
		},

		Data: customData,
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
