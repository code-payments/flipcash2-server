package model

import (
	"encoding/hex"

	"github.com/google/uuid"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

// ChatIDString renders a chat ID for logs and messages, in the form its
// family is known by. A group chat's 16-byte ID is UUID-shaped (see
// chat.MustDeriveGroupChatID) and prints as a UUID; a DM's 32-byte digest
// prints as hex. Length is the family's discriminator everywhere else (see
// chat.GroupChatIDSize), so it is here too — and any other length prints as
// hex rather than failing, since a log line must never choke on its subject.
func ChatIDString(chatID *commonpb.ChatId) string {
	if chatID == nil {
		return "<nil>"
	}
	if id, err := uuid.FromBytes(chatID.GetValue()); err == nil {
		return id.String()
	}
	return hex.EncodeToString(chatID.GetValue())
}
