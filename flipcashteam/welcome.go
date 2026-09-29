package flipcashteam

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
)

// welcomeV1IdempotencyKey is the key SendWelcomeV1 sends under. A later
// version of the welcome takes a key of its own, so it is sent even to a user
// who already has this one.
const welcomeV1IdempotencyKey = "welcome_v1"

// SendWelcomeV1 sends the first version of the team's welcome to userID, whose
// handle is username, as one batch (see SendMessages): each line of the
// welcome is its own message. username must be a valid handle as held, in
// canonical form (see profile.ValidateUsername), since it is shown to the user
// and linked to: anything else did not come from the store and is refused
// rather than repaired. It is the caller's to know that userID holds it.
//
// It is idempotent on the user: every call for them sends under the same key,
// so a retry returns what the first call sent, the username it was sent with
// included, without sending again (for as long as SendMessages' idempotency
// lasts).
func SendWelcomeV1(
	ctx context.Context,
	chats chat.Store,
	sender *messaging.Sender,
	userID *commonpb.UserId,
	username string,
) (*commonpb.ChatId, []*messagingpb.Message, error) {
	if err := profile.ValidateUsername(username); err != nil {
		return nil, nil, fmt.Errorf("invalid username %q: %w", username, err)
	}
	return SendMessages(ctx, chats, sender, userID, welcomeV1IdempotencyKey, welcomeV1Messages(username))
}

// welcomeTimeout bounds one welcome sent by a Welcomer: creating the chat
// and persisting the messages. The side effects of the send run under the
// Sender's own budgets.
const welcomeTimeout = 30 * time.Second

// Welcomer sends the welcome (see SendWelcomeV1) to every user given their
// first handle. It is the profile.FirstUsernameHandler the parent builds the
// profile server with (see profile.WithFirstUsernameHandler), from the chat
// store and the Sender built with the team account.
//
// A welcome is best effort: it is sent in the background, detached from the
// request that assigned the handle, and a failure is logged, never retried.
// Nothing records a welcome that was never sent, so a process that stops
// before sending one loses it. A second call for a user, as when two of their
// claims race, sends nothing new (see SendWelcomeV1).
type Welcomer struct {
	log    *zap.Logger
	chats  chat.Store
	sender *messaging.Sender
}

var _ profile.FirstUsernameHandler = (*Welcomer)(nil)

func NewWelcomer(log *zap.Logger, chats chat.Store, sender *messaging.Sender) *Welcomer {
	return &Welcomer{log: log, chats: chats, sender: sender}
}

// OnFirstUsername sends userID the welcome for username in the background.
func (w *Welcomer) OnFirstUsername(ctx context.Context, userID *commonpb.UserId, username string) {
	log := w.log.With(zap.String("user_id", model.UserIDString(userID)), zap.String("username", username))

	// The request that assigned the handle returns without waiting, so its
	// cancellation must not abort the send. Context values are kept.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), welcomeTimeout)
	go func() {
		defer cancel()

		_, _, err := SendWelcomeV1(ctx, w.chats, w.sender, userID, username)
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

// welcomeV1Messages is the content of the first version of the welcome to the
// holder of username, one message per line.
func welcomeV1Messages(username string) []*messagingpb.Content {
	lines := []string{
		"Welcome to Flipcash!",
		fmt.Sprintf("Tell people your Flipcash username is %s to connect with you", username),
		fmt.Sprintf("https://flipcash.com/%s", username),
		"Flipcash is the only chat app where people have to send you cash before they can message you, so you can say goodbye to spam",
		"Happy chatting!",
	}
	content := make([]*messagingpb.Content, len(lines))
	for i, line := range lines {
		content[i] = &messagingpb.Content{Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: line}}}
	}
	return content
}
