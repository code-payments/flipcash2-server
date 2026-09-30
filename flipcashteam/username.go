// Package flipcashteam is what the server needs to act as the Flipcash team:
// the handle the team account holds, which account that is, and sending
// messages from it.
package flipcashteam

import (
	"context"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

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
// RuleEvaluator, the chat server, the Sender (which SendMessages takes it
// from) and the intent integration. None of them calls this, since this package
// imports them. A process started before the
// team account was set up has none of them configured, and needs a restart
// once it is.
func GetUserID(ctx context.Context, profiles profile.Store) (*commonpb.UserId, error) {
	return profiles.GetUserIdByUsername(ctx, Username)
}
