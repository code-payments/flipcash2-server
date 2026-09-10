package push

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"firebase.google.com/go/v4/messaging"
	"github.com/mr-tron/base58"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"
)

const (
	// maxBatchSize is FCM's limit on messages per SendEach call.
	maxBatchSize = 500

	// maxConcurrentBatches bounds how many SendEach calls are in flight at once
	// for a single push. Each call already fans out over the SDK's own worker
	// pool, so a few in parallel is enough to keep a large group from
	// serializing on batch boundaries without multiplying connections.
	maxConcurrentBatches = 4

	// invalidTokenCleanupTimeout bounds the detached delete of tokens FCM
	// reported as unregistered.
	invalidTokenCleanupTimeout = 10 * time.Second
)

// BadgeCounts maps a user (keyed by string(userID.Value)) to the app-icon badge
// to display alongside a push. A user absent from the map gets no badge.
type BadgeCounts map[string]uint64

// BadgeResolver produces the badge count to display for each of the given
// users. A pusher invokes it with only the users who own at least one iOS
// device — APNs is the only platform with a server-settable numeric badge —
// so any per-user work behind it (e.g. bumping a stored count) is skipped for
// users who could never display the result.
//
// A resolver may return a partial map alongside an error: the counts it did
// produce are still carried on the push, and the error is reported after the
// push is sent.
type BadgeResolver func(ctx context.Context, users []*commonpb.UserId) (BadgeCounts, error)

type Pusher interface {
	// SendPushes sends the same notification to every device of the given users.
	SendPushes(ctx context.Context, title, body string, customPayload *pushpb.Payload, users ...*commonpb.UserId) error

	// SendPushesWithBadges is SendPushes with a per-user app-icon badge carried
	// on the notification itself, resolved on demand for the recipients whose
	// devices can display one (see BadgeResolver).
	SendPushesWithBadges(ctx context.Context, title, body string, customPayload *pushpb.Payload, resolveBadges BadgeResolver, users ...*commonpb.UserId) error
}

type NoOpPusher struct{}

func (n *NoOpPusher) SendPushes(_ context.Context, _, _ string, _ *pushpb.Payload, _ ...*commonpb.UserId) error {
	return nil
}

func (n *NoOpPusher) SendPushesWithBadges(_ context.Context, _, _ string, _ *pushpb.Payload, _ BadgeResolver, _ ...*commonpb.UserId) error {
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

// FCMClient is the subset of *messaging.Client the pusher depends on. SendEach
// takes one fully independent message per device, which is what lets each
// recipient carry its own badge; the SDK fans it out one request per message
// over a worker pool, exactly as it does for a multicast.
type FCMClient interface {
	SendEach(ctx context.Context, messages []*messaging.Message) (*messaging.BatchResponse, error)
}

func NewFCMPusher(log *zap.Logger, tokens TokenStore, client FCMClient) *FCMPusher {
	return &FCMPusher{
		log:    log,
		tokens: tokens,
		client: client,
	}
}

func (p *FCMPusher) SendPushes(ctx context.Context, title, body string, customPayload *pushpb.Payload, users ...*commonpb.UserId) error {
	return p.SendPushesWithBadges(ctx, title, body, customPayload, nil, users...)
}

func (p *FCMPusher) SendPushesWithBadges(ctx context.Context, title, body string, customPayload *pushpb.Payload, resolveBadges BadgeResolver, users ...*commonpb.UserId) error {
	if len(users) == 0 {
		return nil
	}

	pushTokens, err := p.getTokenList(ctx, users)
	if err != nil {
		return err
	}

	if len(pushTokens) == 0 {
		p.log.Debug("Dropping push, no tokens for users", zap.Int("num_users", len(users)))
		return nil
	}

	// Badges are resolved for iOS users only, and only once the token list is in
	// hand: the same read that addresses the push decides who can display one,
	// so the resolver's per-user work is never spent on an Android-only user.
	// A resolver failure costs the badges, not the push — the alerts still go
	// out, and the error is reported once they have.
	var badges BadgeCounts
	var badgeErr error
	if resolveBadges != nil {
		if iosUsers := usersWithTokenType(users, pushTokens, pushpb.TokenType_FCM_APNS); len(iosUsers) > 0 {
			badges, badgeErr = resolveBadges(ctx, iosUsers)
		}
	}

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

	customDataAndroid := map[string]string{
		"push_notification_title": title,
		"push_notification_body":  body,
		"flipcash_payload":        encodedCustomPayload,
	}
	if customPayload.Navigation != nil {
		var targetUrl string
		switch typed := customPayload.Navigation.Type.(type) {
		case *pushpb.Navigation_CurrencyInfo:
			targetUrl = fmt.Sprintf("https://app.flipcash.com/token/%s", base58.Encode(typed.CurrencyInfo.Value))
		case *pushpb.Navigation_ChatId:
			targetUrl = fmt.Sprintf("https://app.flipcash.com/chat/%s", base64.URLEncoding.EncodeToString(typed.ChatId.Value))
		case *pushpb.Navigation_ChatContactPhoneNumber:
			targetUrl = fmt.Sprintf("https://app.flipcash.com/chat/%s", strings.Replace(typed.ChatContactPhoneNumber.Value, "+", "%2B", 1))
		}
		if len(targetUrl) > 0 {
			customDataAndroid["target_url"] = targetUrl
		}
	}
	customDataApns := make(map[string]any)
	for k, v := range customDataAndroid {
		customDataApns[k] = v
	}

	categoryString := customPayload.Category.String()
	hasSubstitutions := len(customPayload.TitleSubstitutions) > 0 || len(customPayload.BodySubstitutions) > 0

	// The alert, category, thread, and custom data are identical for every
	// device. Only the badge varies, so each message gets its own Aps while the
	// read-only pieces are shared.
	android := &messaging.AndroidConfig{
		Priority: "high",
		Data:     customDataAndroid,
	}
	messages := make([]*messaging.Message, len(pushTokens))
	for i, pushToken := range pushTokens {
		aps := &messaging.Aps{
			Alert: &messaging.ApsAlert{
				Title: title,
				Body:  body,
			},
			Category:       categoryString,
			ThreadID:       customPayload.GroupKey,
			MutableContent: hasSubstitutions || customPayload.ChatMetadata != nil,
			CustomData:     customDataApns,
		}
		if count, ok := badges[string(pushToken.UserID.Value)]; ok {
			badge := int(count)
			aps.Badge = &badge
		}
		messages[i] = &messaging.Message{
			Token:   pushToken.Token,
			Android: android,
			APNS: &messaging.APNSConfig{
				Payload: &messaging.APNSPayload{Aps: aps},
			},
		}
	}

	return errors.Join(p.sendInBatches(ctx, messages, pushTokens), badgeErr)
}

// sendInBatches sends messages to FCM in batches of at most maxBatchSize,
// concurrently up to maxConcurrentBatches at a time. Batches are independent:
// one failing costs only its own devices, and its error is joined with the
// rest. pushTokens must be positionally aligned with messages.
func (p *FCMPusher) sendInBatches(ctx context.Context, messages []*messaging.Message, pushTokens []Token) error {
	var (
		mu            sync.Mutex
		errs          error
		invalidTokens []Token
		wg            sync.WaitGroup
		sem           = make(chan struct{}, maxConcurrentBatches)
	)
	for start := 0; start < len(messages); start += maxBatchSize {
		end := min(start+maxBatchSize, len(messages))
		batch, batchTokens := messages[start:end], pushTokens[start:end]

		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			response, err := p.client.SendEach(ctx, batch)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = errors.Join(errs, err)
				return
			}
			if response == nil {
				p.log.Debug("No response from FCM")
				return
			}
			p.log.Debug("Send pushes", zap.Int("success", response.SuccessCount), zap.Int("failed", response.FailureCount))
			if response.FailureCount > 0 {
				invalidTokens = append(invalidTokens, p.processResponse(response, batchTokens)...)
			}
		}()
	}
	wg.Wait()

	// Tokens FCM reports as unregistered are gone for good, so drop them in one
	// batched delete. The cleanup is detached from the send — it must not delay
	// the caller and must survive the caller's context ending — but bounded, so
	// a stalled store can't hold a goroutine forever.
	if len(invalidTokens) > 0 {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), invalidTokenCleanupTimeout)
			defer cancel()

			if err := p.tokens.DeleteTokens(ctx, invalidTokens...); err != nil {
				p.log.Warn("Failed to remove invalid tokens", zap.Error(err), zap.Int("count", len(invalidTokens)))
				return
			}
			p.log.Debug("Removed invalid tokens", zap.Int("count", len(invalidTokens)))
		}()
	}

	return errs
}

// usersWithTokenType returns the subset of users, in their given order, that
// own at least one token of the given type.
func usersWithTokenType(users []*commonpb.UserId, pushTokens []Token, tokenType pushpb.TokenType) []*commonpb.UserId {
	hasType := make(map[string]struct{})
	for _, token := range pushTokens {
		if token.Type == tokenType && token.UserID != nil {
			hasType[string(token.UserID.Value)] = struct{}{}
		}
	}

	var matched []*commonpb.UserId
	for _, user := range users {
		if _, ok := hasType[string(user.Value)]; ok {
			matched = append(matched, user)
		}
	}
	return matched
}

// processResponse logs per-message failures and returns the tokens FCM
// reported as unregistered, for the caller to delete. Responses are
// positionally aligned with the messages sent, and therefore with pushTokens.
func (p *FCMPusher) processResponse(response *messaging.BatchResponse, pushTokens []Token) []Token {
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
				zap.String("token", pushTokens[i].Token),
			)
		}
	}

	return invalidTokens
}

func (p *FCMPusher) getTokenList(ctx context.Context, users []*commonpb.UserId) ([]Token, error) {
	allPushTokens, err := p.tokens.GetTokensBatch(ctx, users...)
	if err != nil {
		return nil, err
	}
	return allPushTokens, nil
}
