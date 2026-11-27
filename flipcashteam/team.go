// Package flipcashteam is what the server needs to act as the Flipcash team:
// the handle the team account holds, which account that is, and sending
// messages from it.
package flipcashteam

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/google/uuid"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/profile"
)

// Username is the handle the Flipcash team account holds. It is a reserved word
// (see profile.IsUsernameReserved), so the SetUsername RPC refuses it to every
// user and it is only ever held through AssignUsername.
const Username = "flipcash"

// AssignUsername gives userID the team handle, replacing any handle they hold.
// It writes through the store directly, past the reservation, balance and
// moderation gates the SetUsername RPC applies, so it is for setting up the team
// account, never for a request path. Assigning it to the user who already holds
// it changes nothing. It returns profile.ErrUsernameTaken if another user holds
// it. A running server does not see the assignment: it resolved the team
// account at startup (see GetUserID) and needs a restart to act as it.
func AssignUsername(ctx context.Context, profiles profile.Store, userID *commonpb.UserId) error {
	return profiles.SetUsername(ctx, userID, Username)
}

// GetUserID returns the team account: the user holding Username, read from
// the store on every call. It returns profile.ErrNotFound while nobody holds
// the handle.
//
// The parent resolves it here once, at startup, and builds everything that
// treats the team specially with that one ID, each taking it as a required
// argument so none can be built without it: the chat store (its
// excludedFromFeed, see chat.FeedExclusions), and the teamUserID of the chat
// RuleEvaluator, the chat server, the chat Hydrator, the Sender (which
// SendDmMessages and SendWelcomeGroupMessages take it from), the payment
// request server and the intent integration. None of them calls this: chat
// and messaging cannot, since this package imports them, and every one takes
// the ID the parent resolved, so they all agree on it. A process started
// before the team account was set up has none of them configured, and needs a
// restart once it is.
func GetUserID(ctx context.Context, profiles profile.Store) (*commonpb.UserId, error) {
	return profiles.GetUserIdByUsername(ctx, Username)
}

// ErrNoTeamAccount is returned by SendDmMessages and SendWelcomeGroupMessages
// when the Sender they are given was built with no team account (see
// messaging.NewSender), and wrapped by NewGroupWelcomer, which refuses such a
// Sender at startup.
var ErrNoTeamAccount = errors.New("no team account configured")

// clientMessageIDNamespace is the UUID namespace the team's sends
// (SendDmMessages and SendWelcomeGroupMessages) derive their client message
// IDs under, so they do not collide by chance with an ID a client draws at
// random or another server-authored send derives. The two share it safely,
// since the chat ID is part of what is hashed (see clientMessageID). It is no
// secret: in a welcome group, where the user may write, the user could send
// with one of the team's IDs first, and the team's send is then refused (see
// messaging.ErrClientMessageIDTaken).
var clientMessageIDNamespace = uuid.MustParse("0fea31a1-c7d6-4084-b481-fde267cf1177")

// clientMessageID derives the client message ID of the index-th message the
// team sends into chatID under idempotencyKey (see SendDmMessages and
// SendWelcomeGroupMessages). The key is length-prefixed so no two (key,
// index) pairs hash the same input.
func clientMessageID(chatID *commonpb.ChatId, idempotencyKey string, index int) *messagingpb.ClientMessageId {
	var data []byte
	data = append(data, chatID.Value...)
	data = binary.BigEndian.AppendUint32(data, uint32(len(idempotencyKey)))
	data = append(data, idempotencyKey...)
	data = binary.BigEndian.AppendUint32(data, uint32(index))

	id := uuid.NewSHA1(clientMessageIDNamespace, data)
	return &messagingpb.ClientMessageId{Value: id[:]}
}
