package chat

import (
	"context"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/blob"
)

// encryptedUploadGate adapts an Access to blob.EncryptedUploadGate, the gate
// the blob service consults before reserving an end-to-end encrypted upload
// for a chat. It lives here rather than in blob because blob must not import
// chat; the dependency is one-way. The rule is membership, then the speaker's
// standing (see SpeakerStanding.takesEncrypted): an encrypted blob is sent
// as encrypted content, so an upload for a chat is admitted exactly when the
// caller is a member who may send encrypted content there — of a DM, or of a
// private group that has its key — and refused for a chat that takes none, a
// public group or a keyless private group, however well the caller stands in
// it. That also keeps a DM with the Flipcash team account, where no one
// sends, free of uploads. A chat ID of neither shape is refused before
// anything is read, and a non-member before the chat is looked at.
//
// The standing already rests on membership, so the explicit check in front
// of it repeats a read (one a store caches for a DM, one keyed read for a
// group). It is there so the gate says what it is — a member's alone — in its
// own terms, rather than through what SpeakerStanding happens to check first,
// and so a non-member is refused before the rules or the key are read. A
// public group's member is refused only after their standing is found in
// full, rules included, since what the group takes comes with the standing;
// no client that follows the contract asks, so the cost is theirs alone.
type encryptedUploadGate struct {
	access *Access
}

// NewBlobEncryptedUploadGate returns a blob.EncryptedUploadGate backed by the
// given Access, for wiring the blob service. It should be the one Access the
// chat and messaging servers share, so the gate and the sends it stands
// before agree.
func NewBlobEncryptedUploadGate(access *Access) blob.EncryptedUploadGate {
	return &encryptedUploadGate{access: access}
}

func (g *encryptedUploadGate) CanUploadEncrypted(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	if n := len(chatID.GetValue()); n != DmChatIDSize && n != GroupChatIDSize {
		return false, nil
	}
	isMember, err := g.access.IsMember(ctx, chatID, userID)
	if err != nil || !isMember {
		return false, err
	}
	speaker, err := g.access.SpeakerStanding(ctx, chatID, userID)
	return speaker.takesEncrypted(), err
}
