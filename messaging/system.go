package messaging

import (
	"context"
	"encoding/binary"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
)

// System messages are the server's own entries in a chat's log: a message
// with no sender (see SendBatch), carrying SystemContent.
//
// A system message names people only in its structured variant, never in its
// fallback text: clients that know the variant render each user's current
// profile, so a rename shows everywhere at once, while text written into the
// log would keep the old name forever. The fallback is generic and only for a
// client that does not know the variant, which could not show a current name
// anyway.

// userJoinedFallbackText is the fallback text of every UserJoined message (see
// above).
const userJoinedFallbackText = "A new member joined the group"

// userJoinedClientMessageIDNamespace is the UUID namespace SendUserJoined
// derives its client message IDs under, so they cannot collide with an ID a
// client or another server-authored send picks.
var userJoinedClientMessageIDNamespace = uuid.MustParse("ed9db3a4-e725-4119-ae69-6f72f97a0cab")

// SendUserJoined writes the system message telling a group's members that
// userID joined it: Admitted naming admittedBy when someone let them in, and
// Direct when admittedBy is nil, for a user who joined on their own.
// rosterVersion is the group's roster version after the join, which names the
// join: the message's client message ID is derived from it, so a repeat of
// one join writes nothing and every later join (a rejoin included) writes its
// own.
//
// It is a system message (see SendBatch): no read pointer is advanced, no
// activity is recorded and no one is pushed, and it does not count toward
// unread. It is still the group's newest message, so it becomes the group's
// last message and moves its last activity, and it is broadcast on the chat's
// topic like any send.
//
// Only a group takes one; a DM is refused.
func (s *Sender) SendUserJoined(ctx context.Context, chatID *commonpb.ChatId, userID, admittedBy *commonpb.UserId, rosterVersion uint64) error {
	if !chat.IsGroupChatID(chatID) {
		return errors.New("user joined messages are for groups only")
	}

	joined := &messagingpb.SystemContent_UserJoined{UserId: userID}
	if admittedBy == nil {
		joined.Method = &messagingpb.SystemContent_UserJoined_Direct_{
			Direct: &messagingpb.SystemContent_UserJoined_Direct{},
		}
	} else {
		joined.Method = &messagingpb.SystemContent_UserJoined_Admitted_{
			Admitted: &messagingpb.SystemContent_UserJoined_Admitted{AdmittedBy: admittedBy},
		}
	}
	content := []*messagingpb.Content{{
		Type: &messagingpb.Content_System{System: &messagingpb.SystemContent{
			FallbackText: userJoinedFallbackText,
			Type:         &messagingpb.SystemContent_UserJoined_{UserJoined: joined},
		}},
	}}

	_, err := s.Send(ctx, chatID, nil, content, userJoinedClientMessageID(chatID, userID, rosterVersion), false)
	return err
}

var _ chat.SystemMessenger = (*Sender)(nil)

// userJoinedClientMessageID derives the client message ID of userID's join of
// chatID at rosterVersion (see SendUserJoined). A group ID and a user ID are
// both fixed length, so their concatenation needs no framing.
func userJoinedClientMessageID(chatID *commonpb.ChatId, userID *commonpb.UserId, rosterVersion uint64) *messagingpb.ClientMessageId {
	var data []byte
	data = append(data, chatID.Value...)
	data = append(data, userID.GetValue()...)
	data = binary.BigEndian.AppendUint64(data, rosterVersion)

	id := uuid.NewSHA1(userJoinedClientMessageIDNamespace, data)
	return &messagingpb.ClientMessageId{Value: id[:]}
}
