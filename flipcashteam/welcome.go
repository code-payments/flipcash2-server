package flipcashteam

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/rpc"
	ocp_client "github.com/code-payments/ocp-server/grpc/client"
)

// welcomeV3IdempotencyKey is the key SendWelcomeV3 sends under. Each version
// of the welcome takes a key of its own (the earlier were "welcome_v1" and
// "welcome_v2"), so a later one is sent even to a user who already has an
// earlier one.
const welcomeV3IdempotencyKey = "welcome_v3"

// SendWelcomeV3 sends the third version of the team's welcome to userID: one
// text message (see SendMessages).
//
// It is idempotent on the user: every call for them sends under the same key,
// so a retry returns what the first call sent without sending again (for as
// long as SendMessages' idempotency lasts).
func SendWelcomeV3(
	ctx context.Context,
	chats chat.Store,
	sender *messaging.Sender,
	userID *commonpb.UserId,
) (*commonpb.ChatId, []*messagingpb.Message, error) {
	return SendMessages(ctx, chats, sender, userID, welcomeV3IdempotencyKey, welcomeV3Messages())
}

// welcomeTimeout bounds one welcome sent by a Welcomer: creating the chat
// and persisting the messages. The side effects of the send run under the
// Sender's own budgets.
const welcomeTimeout = 30 * time.Second

// Welcomer sends the welcome (see SendWelcomeV3) to every user given their
// first handle. It is the profile.FirstUsernameHandler the parent builds the
// profile server with (see profile.WithFirstUsernameHandler), from the chat
// store and the Sender built with the team account.
//
// A welcome goes only to a user whose handle was assigned from a client at or
// above minWelcomeClientVersion, read off the request that assigned it (see
// rpc.GetClientVersion); a user whose first handle came from an older client,
// or one that cannot be identified, is never welcomed, since there is no
// later first handle to try again on.
//
// A welcome is best effort: it is sent in the background, detached from the
// request that assigned the handle, and a failure is logged, never retried.
// Nothing records a welcome that was never sent, so a process that stops
// before sending one loses it. A second call for a user, as when two of their
// claims race, sends nothing new (see SendWelcomeV3).
type Welcomer struct {
	log    *zap.Logger
	chats  chat.Store
	sender *messaging.Sender
}

var _ profile.FirstUsernameHandler = (*Welcomer)(nil)

func NewWelcomer(log *zap.Logger, chats chat.Store, sender *messaging.Sender) *Welcomer {
	return &Welcomer{log: log, chats: chats, sender: sender}
}

// minWelcomeClientVersion is the first client release, on iOS and Android
// alike, that rendered the second version of the welcome (its
// ShareProfileWidget). The third is text alone, which any client renders, but
// the gate is kept as it stands.
var minWelcomeClientVersion = &ocp_client.Version{Major: 2026, Minor: 9, Patch: 5}

// OnFirstUsername sends userID the welcome in the background, if the client
// that assigned username is at or above minWelcomeClientVersion.
func (w *Welcomer) OnFirstUsername(ctx context.Context, userID *commonpb.UserId, username string) {
	log := w.log.With(zap.String("user_id", model.UserIDString(userID)), zap.String("username", username))

	if clientVersion := rpc.GetClientVersion(ctx); clientVersion == nil || clientVersion.Before(minWelcomeClientVersion) {
		return
	}

	// The request that assigned the handle returns without waiting, so its
	// cancellation must not abort the send. Context values are kept.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), welcomeTimeout)
	go func() {
		defer cancel()

		_, _, err := SendWelcomeV3(ctx, w.chats, w.sender, userID)
		switch {
		case err == nil:
		case errors.Is(err, ErrNoTeamAccount):
			// A process with no team account configured sends no welcome.
			log.Debug("No team account to send welcome from")
		default:
			log.Warn("Failed to send welcome", zap.Error(err))
		}
	}()
}

// welcomeV3Messages is the content of the third version of the welcome: one
// text message.
func welcomeV3Messages() []*messagingpb.Content {
	return []*messagingpb.Content{
		textContent("Welcome to Flipcash, the group money app. If someone sent you an invite link to a Flipcash group, tap it again to join."),
	}
}

func textContent(text string) *messagingpb.Content {
	return &messagingpb.Content{Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: text}}}
}
