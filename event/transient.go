package event

import (
	"google.golang.org/protobuf/reflect/protoreflect"

	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
)

var (
	chatUpdateChatField   = (&eventpb.ChatUpdate{}).ProtoReflect().Descriptor().Fields().ByName("chat").Number()
	chatUpdateTypingField = (&eventpb.ChatUpdate{}).ProtoReflect().Descriptor().Fields().ByName("is_typing_notifications").Number()
)

// isTypingOnly reports whether e is a chat update carrying typing
// notifications and nothing else, which is what a stream that is behind may
// drop (see NewEventStream). Typing is transient and best-effort: it is not
// part of the event log, a client never catches up on it, and the next
// notification supersedes it, so losing one costs a client nothing a lag
// close and reconnect would win back. An update that carries anything else
// beside it is kept whole, so a field added to ChatUpdate later is never
// dropped by accident.
func isTypingOnly(e *eventpb.Event) bool {
	update := e.GetChatUpdate()
	if update.GetIsTypingNotifications() == nil {
		return false
	}
	only := true
	update.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		switch fd.Number() {
		case chatUpdateChatField, chatUpdateTypingField:
			return true
		}
		only = false
		return false
	})
	return only
}
