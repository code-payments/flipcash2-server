package chat

import (
	"context"
	"errors"
	"time"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/database"
)

var (
	// ErrChatNotFound indicates that no chat exists for the given chat ID.
	ErrChatNotFound = errors.New("chat not found")

	// ErrChatExists indicates that a chat with the given ID already exists.
	ErrChatExists = errors.New("chat already exists")
)

// Store persists chats and their membership.
//
// Membership is fixed at creation time (e.g. the two participants of a DM) and
// is never mutated afterward. The only mutable field is last_activity, which is
// advanced as new activity (typically messages) occurs and is the sort key for
// a user's chat list.
type Store interface {
	// PutChat persists a new chat and its membership. It returns ErrChatExists
	// if a chat with the same ID already exists.
	PutChat(ctx context.Context, chat *Chat) error

	// GetChatByID returns the chat with the given ID, or ErrChatNotFound.
	GetChatByID(ctx context.Context, chatID *commonpb.ChatId) (*Chat, error)

	// GetDmsForUserByLastActivity returns the DMs userID is a member of, ordered
	// by last_activity (descending by default), paged via the provided query
	// options. An empty result (no error) is returned when the user is in no DMs.
	//
	// It is scoped to DMs because the per-user inbox is split by chat type (see
	// the dynamodb store). Group chats will have a parallel accessor, and the
	// server merges the two last_activity-ordered streams into one chat list.
	//
	// Paging follows the repo convention (see activity/server.go): the paging
	// token's value is the chat ID of the last chat from the previous page. The
	// store resolves it to that chat's last_activity and resumes from there. A
	// token referencing a chat the user is not a member of yields an empty page.
	//
	// Because last_activity is mutable, pagination is a best-effort snapshot: a
	// chat whose activity changes mid-pagination may be missed or duplicated
	// across pages. Clients reconcile against the live MetadataUpdate event
	// stream, so a transient gap self-heals.
	GetDmsForUserByLastActivity(ctx context.Context, userID *commonpb.UserId, opts ...database.QueryOption) ([]*Chat, error)

	// GetMembers returns the member user IDs of a chat, or ErrChatNotFound.
	GetMembers(ctx context.Context, chatID *commonpb.ChatId) ([]*commonpb.UserId, error)

	// IsMember reports whether userID is a member of chatID. It returns false
	// (no error) when the chat does not exist.
	IsMember(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error)

	// AdvanceLastActivity moves chatID's last_activity forward to ts and reports
	// whether it advanced. If the stored value is already at or after ts, it is a
	// no-op and returns false. It returns ErrChatNotFound if the chat does not
	// exist.
	AdvanceLastActivity(ctx context.Context, chatID *commonpb.ChatId, ts time.Time) (bool, error)
}
