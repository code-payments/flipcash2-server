package chat

import (
	"context"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/blob"
)

// dmMembership adapts a chat Store to blob.DMMembership, the gate the blob
// service consults before reserving an end-to-end encrypted upload for a DM.
// It lives here rather than in blob because blob must not import chat; the
// dependency is one-way. Keeping it here also keeps the chat ID discriminator
// (see DmChatIDSize) in the package that owns it: a group ID is refused before
// membership is ever read, because encrypted content is a DM's alone.
type dmMembership struct {
	store Store
}

// NewBlobDMMembership returns a blob.DMMembership backed by the given chat
// store, for wiring the blob service.
func NewBlobDMMembership(store Store) blob.DMMembership {
	return &dmMembership{store: store}
}

func (m *dmMembership) IsDMMember(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	if len(chatID.GetValue()) != DmChatIDSize {
		return false, nil
	}
	return m.store.IsMember(ctx, chatID, userID)
}
