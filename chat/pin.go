package chat

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// A chat's pins are messages in it that a member has pinned (see
// Chat.PinMessage), shown with the chat as its PinSummary and paged whole
// with GetPinnedMessages. They belong to the chat, a DM or a group alike, and
// are recorded against the IDs alone: the store neither knows nor checks
// that the chat exists, that the message is in it and may be pinned, or that
// whoever pins may; those are the server's gates.
//
// A chat's pins are ordered most recently pinned first (see PinPosition),
// and a pin keeps its place: pinning a message already pinned is a no-op
// that writes nothing and leaves it where it is, with the time and pinner
// of the pin that made it, and unpinning a message that is not pinned is a
// no-op too. Unpinning then pinning a message again is a fresh pin, at the
// top of the order with the new time and pinner. A chat holds at most a
// capped number of pins (MaxPinnedMessages), which the server names and the
// store enforces in the write that would pass it (see Store.PinMessage), so
// pins racing at the cap never leave a chat over it.
//
// The chat's pins carry one version (see PinSummary), state rather than a
// delta, as a roster's: it moves by exactly one with each real pin and
// unpin, never on a no-op, and is zero for a chat that has never had a pin.
// Editing or deleting a pinned message does not move it; the change rides
// the chat's event log like any other. Every read is strongly consistent, so
// a caller that pins and then reads the chat's pins sees its pin.

// MaxPinnedMessages is the most messages one chat may have pinned, the cap
// the server passes to Store.PinMessage. It is the cap for now and may be
// raised; clients are told not to hardcode it (see Chat.PinMessage).
const MaxPinnedMessages = 5

// Pin is one pinned message in a chat as stored: which message, who pinned
// it and when. It is what Pin is projected from, with the message itself
// read by the caller.
type Pin struct {
	MessageID *messagingpb.MessageId
	PinnedBy  *commonpb.UserId
	PinnedAt  time.Time
}

// Clone deep-copies the pin, so a store's copy and its callers' share no
// memory.
func (p Pin) Clone() Pin {
	clone := Pin{PinnedAt: p.PinnedAt}
	if p.MessageID != nil {
		clone.MessageID = &messagingpb.MessageId{Value: p.MessageID.Value}
	}
	if p.PinnedBy != nil {
		clone.PinnedBy = &commonpb.UserId{Value: append([]byte(nil), p.PinnedBy.Value...)}
	}
	return clone
}

// Position is the pin's place in its chat's order (see PinPosition).
func (p Pin) Position() PinPosition {
	return PinPosition{PinnedAt: p.PinnedAt, MessageID: p.MessageID}
}

// ToProto projects the pin onto a chatpb.Pin carrying message, the pinned
// message as the viewer may read it, which the caller reads and supplies.
func (p Pin) ToProto(message *messagingpb.Message) *chatpb.Pin {
	clone := p.Clone()
	return &chatpb.Pin{
		MessageId: clone.MessageID,
		PinnedBy:  clone.PinnedBy,
		PinnedAt:  timestamppb.New(p.PinnedAt),
		Message:   message,
	}
}

// PinPosition is a place in a chat's order of pins (see Store.GetPinsPage):
// most recently pinned first, ties broken by message ID descending, so the
// order is total and a page can resume strictly after the last pin it
// carried. A message is pinned at most once at a time, so the message ID
// alone names a pin; ties in pin time are nanosecond coincidences within one
// chat, and the tie-break exists for totality rather than for anything a
// client sees.
type PinPosition struct {
	PinnedAt  time.Time
	MessageID *messagingpb.MessageId
}

// Compare orders positions in pin order: negative when p comes before o
// (pinned later, or at the same instant with the greater message ID),
// positive when after, zero when they name the same place.
func (p PinPosition) Compare(o PinPosition) int {
	if c := o.PinnedAt.Compare(p.PinnedAt); c != 0 {
		return c
	}
	return cmp.Compare(o.MessageID.GetValue(), p.MessageID.GetValue())
}

// PinSummary is the state of a chat's pins as stored: the most recently
// pinned (Latest, nil when nothing is pinned), how many are pinned, and the
// version of the chat's pins (see the rules above). A chat that has never
// had a pin is the zero summary.
type PinSummary struct {
	Latest  *Pin
	Count   uint32
	Version uint64
}

// Clone deep-copies the summary.
func (s PinSummary) Clone() PinSummary {
	clone := PinSummary{Count: s.Count, Version: s.Version}
	if s.Latest != nil {
		latest := s.Latest.Clone()
		clone.Latest = &latest
	}
	return clone
}

// ToProto projects the summary onto a chatpb.PinSummary, with latestMessage,
// which the caller reads and supplies, as the message of its latest pin. The
// latest pin is omitted when there is none, and latestMessage is then
// ignored.
func (s PinSummary) ToProto(latestMessage *messagingpb.Message) *chatpb.PinSummary {
	out := &chatpb.PinSummary{Count: s.Count, Version: s.Version}
	if s.Latest != nil {
		out.Latest = s.Latest.ToProto(latestMessage)
	}
	return out
}

// PinTime is t as a store records a pin's time: at nanosecond precision, in
// UTC, with no monotonic reading, which is what a read gives back.
func PinTime(t time.Time) time.Time {
	return time.Unix(0, t.UnixNano()).UTC()
}

// maxPinTime is the latest pin time a store can record, the last instant
// whose Unix nanoseconds fit an int64.
var maxPinTime = time.Unix(0, math.MaxInt64)

// ValidatePin returns an error unless a store accepts pin as a pin in
// chatID: chatID a DM or a group chat ID (either; the store does not care
// which), a message ID, a pinner that is a user ID, and a pin time at or
// after the Unix epoch that Unix nanoseconds can hold.
func ValidatePin(chatID *commonpb.ChatId, pin Pin) error {
	if err := validatePinChatID(chatID); err != nil {
		return err
	}
	switch {
	case pin.MessageID == nil:
		return fmt.Errorf("pin: message id is required")
	case len(pin.PinnedBy.GetValue()) != model.UserIDSize:
		return fmt.Errorf("pin: pinned by must be a user id")
	case pin.PinnedAt.Before(time.Unix(0, 0)):
		return fmt.Errorf("pin: pinned before the unix epoch")
	case pin.PinnedAt.After(maxPinTime):
		return fmt.Errorf("pin: pinned after the last representable instant")
	}
	return nil
}

// ValidatePinPosition returns an error unless pos is a position a store
// can resume a page of pins after: a message ID and a pin time a pin could
// hold (see ValidatePin).
func ValidatePinPosition(pos PinPosition) error {
	switch {
	case pos.MessageID == nil:
		return fmt.Errorf("pin position: message id is required")
	case pos.PinnedAt.Before(time.Unix(0, 0)):
		return fmt.Errorf("pin position: pinned before the unix epoch")
	case pos.PinnedAt.After(maxPinTime):
		return fmt.Errorf("pin position: pinned after the last representable instant")
	}
	return nil
}

// validatePinChatID returns an error unless chatID is a DM or a group chat
// ID, the two families whose chats hold pins.
func validatePinChatID(chatID *commonpb.ChatId) error {
	if n := len(chatID.GetValue()); n != DmChatIDSize && n != GroupChatIDSize {
		return fmt.Errorf("pin: chat id %x is neither a dm nor a group chat id", chatID.GetValue())
	}
	return nil
}

// ValidatePinChatIDs returns an error unless every ID is a DM or a group chat
// ID (see ValidatePin), for the reads and the unpin, which take no Pin.
func ValidatePinChatIDs(chatIDs ...*commonpb.ChatId) error {
	for _, chatID := range chatIDs {
		if err := validatePinChatID(chatID); err != nil {
			return err
		}
	}
	return nil
}

// Pinning, the RPCs: PinMessage and UnpinMessage change a chat's pins, and
// GetPinnedMessages pages them.
//
// Who may pin is ViewerState.Permissions.can_pin (see Chat.PermissionsFor):
// either member of a DM, and a group's recorded creator while a member,
// decided as an edit is, so a departed creator pins nothing until they
// rejoin and a legacy group with no recorded creator is pinned by no one.
// Pinning and unpinning share one gate (see pinnableChat): the record
// (NOT_FOUND), then can_pin as Chat.PermissionsFor decides it, DENIED
// otherwise: first as if the caller were a member, so one who could not pin
// even then (in a group, anyone but the creator today) costs no membership
// read, then with membership read (see Access.IsMemberWithChat: a DM's off
// its record, a group's strongly consistent). An ID that names no chat, a
// malformed one included, is NOT_FOUND off the record read, as GetChat
// answers it.
//
// PinMessage then reads the message, in full, since the caller is a member:
// one absent from the read is MESSAGE_NOT_FOUND, and one of a kind that may
// not be pinned (see pinnableMessage: a deleted message, a system message, or
// any kind this server does not know) MESSAGE_NOT_ALLOWED, both judged after
// the gate so that no one who may not pin can probe which message IDs a chat
// holds. The read is eventually consistent, so a message sent a moment ago
// may briefly be refused, which a retry outlives. UnpinMessage reads no
// message: unpinning one that is not pinned, or that never existed, is the
// OK no-op, as the proto describes. The cap on a chat's pins is the server's
// (maxPinnedMessages, MaxPinnedMessages in production) and enforced by the
// store in the write (see Store.PinMessage), so a pin past it is
// TOO_MANY_PINS however pins race.
//
// GetPinnedMessages is gated as messaging's GetMessages is: the record
// (NOT_FOUND), then the caller's standing under view_mode (see
// ListenerStanding.Reading), DENIED when it allows no reading, and the
// messages are read and redacted under that reading. The chat's summary is
// read first and the page after it, each a strongly consistent read of its
// own (see Store.GetPinsPage), so the page is never older than the summary's
// version: every change up to it is in the page, and one landing between the
// reads may be too, which a client applying the changes after that version
// takes as the no-op it is (a pin of a message already shown, an unpin of
// one already gone). A summary counting no pins (a chat never pinned
// included) skips the page read and returns an empty page, which is the
// page as of that version. The token binds the page to the chat (see
// encodePinToken).
//
// Every Pin the service returns or publishes carries its message, which the
// proto requires: the messages of every pin a call shows, the summary's
// latest among them, are read in one batch (see pinsToProto, and hydrate for
// the summary a chat's metadata carries), and a pinned message missing from
// it fails the call with Internal rather than show a pin without it. A
// message deleted after it was pinned is not missing: its tombstone is shown
// like any message, and clients hide such a pin (see Pin.message). Nothing
// unpins it on a delete: it stays pinned, counting toward the cap, until
// someone permitted to pin unpins it.
//
// A real pin or unpin, never a no-op, is published once as a
// MetadataUpdate.PinsChanged carrying the change and the summary after it,
// to every member, the caller's own devices included (see publishPinsChanged).
// It is built from the very protos the response carries, so when those
// cannot be built the call fails as above and nothing is published: the
// write has landed, so a retry is the no-op and publishes nothing either,
// and the chat's members converge from the next read of its pins. Publishing
// is best-effort, as everywhere in the service.

const (
	// maxGetPinnedMessagesPageSize bounds a single GetPinnedMessages page
	// and is its default.
	maxGetPinnedMessagesPageSize = 100

	pinTokenVersion = 1

	// pinTokenLen is the fixed size of a GetPinnedMessages paging token: the
	// version byte, the chat ID padded to a DM's size, so that a DM's and a
	// group's are bound alike, the pin time and the message ID (see
	// encodePinToken).
	pinTokenLen = 1 + DmChatIDSize + 8 + 8
)

// errPinnedMessageMissing is pinsToProto's refusal of a pin whose message the
// read did not return (see above).
var errPinnedMessageMissing = errors.New("pinned message missing")

// PinMessage pins a message in a chat (see above).
func (s *Server) PinMessage(ctx context.Context, req *chatpb.PinMessageRequest) (*chatpb.PinMessageResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.pinLog(userID, req.ChatId, req.MessageId)

	c, result, err := s.pinnableChat(ctx, log, req.ChatId, userID)
	if err != nil {
		return nil, err
	}
	switch result {
	case pinNotFound:
		return &chatpb.PinMessageResponse{Result: chatpb.PinMessageResponse_NOT_FOUND}, nil
	case pinDenied:
		return &chatpb.PinMessageResponse{Result: chatpb.PinMessageResponse_DENIED}, nil
	}

	// The caller is a member, so the message is read in full, and the same
	// read serves the response and the event, which only members receive.
	known, err := s.messaging.Messages(ctx, []MessageRef{{ChatID: c.ID, MessageID: req.MessageId}}, ReadingFull)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure reading message to pin")
		return nil, status.Error(codes.Internal, "")
	}
	message, ok := known[MessageKeyOf(c.ID, req.MessageId)]
	if !ok {
		return &chatpb.PinMessageResponse{Result: chatpb.PinMessageResponse_MESSAGE_NOT_FOUND}, nil
	}
	if !pinnableMessage(message) {
		return &chatpb.PinMessageResponse{Result: chatpb.PinMessageResponse_MESSAGE_NOT_ALLOWED}, nil
	}

	pin := Pin{MessageID: req.MessageId, PinnedBy: userID, PinnedAt: PinTime(time.Now())}
	summary, changed, err := s.chats.PinMessage(ctx, c.ID, pin.MessageID, pin.PinnedBy, pin.PinnedAt, s.maxPinnedMessages)
	switch {
	case errors.Is(err, ErrTooManyPins):
		return &chatpb.PinMessageResponse{Result: chatpb.PinMessageResponse_TOO_MANY_PINS}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure pinning message")
		return nil, status.Error(codes.Internal, "")
	}

	// A real pin is the one this call made, which the event carries; a no-op
	// made none, and shows the summary alone.
	var pins []Pin
	if changed {
		pins = []Pin{pin}
	}
	pinned, summaryProto, err := s.pinsToProto(ctx, c.ID, ReadingFull, known, pins, summary)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure reading pinned messages")
		return nil, status.Error(codes.Internal, "")
	}
	if changed {
		s.publishPinsChanged(c, &chatpb.MetadataUpdate_PinsChanged{
			Change:  &chatpb.MetadataUpdate_PinsChanged_Pinned{Pinned: pinned[0]},
			Summary: summaryProto,
		})
	}

	return &chatpb.PinMessageResponse{
		Result:     chatpb.PinMessageResponse_OK,
		PinSummary: summaryProto,
	}, nil
}

// UnpinMessage unpins a message in a chat, whoever pinned it (see above).
func (s *Server) UnpinMessage(ctx context.Context, req *chatpb.UnpinMessageRequest) (*chatpb.UnpinMessageResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.pinLog(userID, req.ChatId, req.MessageId)

	c, result, err := s.pinnableChat(ctx, log, req.ChatId, userID)
	if err != nil {
		return nil, err
	}
	switch result {
	case pinNotFound:
		return &chatpb.UnpinMessageResponse{Result: chatpb.UnpinMessageResponse_NOT_FOUND}, nil
	case pinDenied:
		return &chatpb.UnpinMessageResponse{Result: chatpb.UnpinMessageResponse_DENIED}, nil
	}

	summary, changed, err := s.chats.UnpinMessage(ctx, c.ID, req.MessageId)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure unpinning message")
		return nil, status.Error(codes.Internal, "")
	}

	_, summaryProto, err := s.pinsToProto(ctx, c.ID, ReadingFull, nil, nil, summary)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure reading pinned messages")
		return nil, status.Error(codes.Internal, "")
	}
	if changed {
		s.publishPinsChanged(c, &chatpb.MetadataUpdate_PinsChanged{
			Change:  &chatpb.MetadataUpdate_PinsChanged_Unpinned{Unpinned: &messagingpb.MessageId{Value: req.MessageId.Value}},
			Summary: summaryProto,
		})
	}

	return &chatpb.UnpinMessageResponse{
		Result:     chatpb.UnpinMessageResponse_OK,
		PinSummary: summaryProto,
	}, nil
}

// GetPinnedMessages pages a chat's pins, most recently pinned first, each with
// its message as the caller may read it (see above).
func (s *Server) GetPinnedMessages(ctx context.Context, req *chatpb.GetPinnedMessagesRequest) (*chatpb.GetPinnedMessagesResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", model.ChatIDString(req.ChatId)),
	)

	limit := maxGetPinnedMessagesPageSize
	if pageSize := req.GetQueryOptions().GetPageSize(); pageSize > 0 && int(pageSize) < limit {
		limit = int(pageSize)
	}

	var after *PinPosition
	if token := req.GetQueryOptions().GetPagingToken(); token != nil {
		pos, ok := decodePinToken(token, req.ChatId)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "invalid paging token")
		}
		after = &pos
	}

	c, err := s.chats.GetChatByID(ctx, req.ChatId)
	switch {
	case errors.Is(err, ErrChatNotFound):
		return &chatpb.GetPinnedMessagesResponse{Result: chatpb.GetPinnedMessagesResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting chat")
		return nil, status.Error(codes.Internal, "")
	}

	standing, err := s.access.ListenerStandingWithChat(ctx, c, userID, req.GetViewMode())
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure determining chat standing")
		return nil, status.Error(codes.Internal, "")
	}
	reading := standing.Reading(req.GetViewMode())
	if reading == ReadingDenied {
		return &chatpb.GetPinnedMessagesResponse{Result: chatpb.GetPinnedMessagesResponse_DENIED}, nil
	}

	// The summary before the page, so the page is never older than the
	// summary's version (see above).
	summaries, err := s.chats.GetPinSummaries(ctx, []*commonpb.ChatId{c.ID})
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure reading pin summary")
		return nil, status.Error(codes.Internal, "")
	}
	summary := summaries[string(c.ID.Value)]

	// No pins as of the summary is an empty page, with no read of the pins.
	var page []Pin
	if summary.Count > 0 {
		// One more than the page, for an exact has_more.
		page, err = s.chats.GetPinsPage(ctx, c.ID, after, limit+1)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure reading pins")
			return nil, status.Error(codes.Internal, "")
		}
	}

	hasMore := len(page) > limit
	if hasMore {
		page = page[:limit]
	}

	pins, summaryProto, err := s.pinsToProto(ctx, c.ID, reading, nil, page, summary)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure reading pinned messages")
		return nil, status.Error(codes.Internal, "")
	}

	resp := &chatpb.GetPinnedMessagesResponse{
		Result:     chatpb.GetPinnedMessagesResponse_OK,
		Pins:       pins,
		PinSummary: summaryProto,
		HasMore:    hasMore,
	}
	if hasMore {
		resp.PagingToken = encodePinToken(c.ID, page[len(page)-1].Position())
	}
	return resp, nil
}

// pinResult is the shared gate's verdict on a pin or unpin's chat.
type pinResult uint8

const (
	pinOK pinResult = iota
	pinNotFound
	pinDenied
)

// pinnableChat is the gate PinMessage and UnpinMessage start with (see
// above): the chat's record exists (NOT_FOUND), and the caller may pin in it,
// can_pin as Chat.PermissionsFor decides it (DENIED otherwise): asked first
// as if the caller were a member, so one who could not pin even then costs
// no membership read, and then with membership read.
// On OK the record is returned. A gRPC error is returned only for a failed
// read.
func (s *Server) pinnableChat(ctx context.Context, log *zap.Logger, chatID *commonpb.ChatId, userID *commonpb.UserId) (*Chat, pinResult, error) {
	c, err := s.chats.GetChatByID(ctx, chatID)
	switch {
	case errors.Is(err, ErrChatNotFound):
		return nil, pinNotFound, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting chat")
		return nil, 0, status.Error(codes.Internal, "")
	}
	// Membership only ever grants, so a caller who could not pin even as a
	// member is refused before membership is read.
	if !c.PermissionsFor(userID, true).CanPin {
		return nil, pinDenied, nil
	}
	isMember, err := s.access.IsMemberWithChat(ctx, c, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat membership")
		return nil, 0, status.Error(codes.Internal, "")
	}
	if !c.PermissionsFor(userID, isMember).CanPin {
		return nil, pinDenied, nil
	}
	return c, pinOK, nil
}

// pinsToProto projects pins, and summary, onto their protos, each pin
// carrying its message as a read under reading returns it: known holds
// messages of chatID already read under reading, keyed by MessageKeyOf, and
// every other message the pins and the summary's latest name is read in one
// batch (see readPinnedMessages). A message that neither holds fails the
// projection with errPinnedMessageMissing (see above); a tombstone is a
// message like any other.
func (s *Server) pinsToProto(ctx context.Context, chatID *commonpb.ChatId, reading Reading, known map[MessageKey]*messagingpb.Message, pins []Pin, summary PinSummary) ([]*chatpb.Pin, *chatpb.PinSummary, error) {
	refs := make([]MessageRef, 0, len(pins)+1)
	for _, p := range pins {
		refs = append(refs, MessageRef{ChatID: chatID, MessageID: p.MessageID})
	}
	if summary.Latest != nil {
		refs = append(refs, MessageRef{ChatID: chatID, MessageID: summary.Latest.MessageID})
	}
	messages, err := readPinnedMessages(ctx, s.messaging, reading, known, refs)
	if err != nil {
		return nil, nil, err
	}

	out := make([]*chatpb.Pin, len(pins))
	for i, p := range pins {
		m, err := pinnedMessage(messages, chatID, p)
		if err != nil {
			return nil, nil, err
		}
		out[i] = p.ToProto(m)
	}
	summaryProto, err := pinSummaryToProto(messages, chatID, summary)
	if err != nil {
		return nil, nil, err
	}
	return out, summaryProto, nil
}

// readPinnedMessages returns the pinned messages refs name, across any number
// of chats, keyed by MessageKeyOf, as a read under reading returns them:
// those known holds (already read under reading) as they are, and the rest
// in one batch from reader, each read once however often it is named. A ref
// whose message the read did not return maps to nil, which pinnedMessage
// refuses.
func readPinnedMessages(ctx context.Context, reader MessagingReader, reading Reading, known map[MessageKey]*messagingpb.Message, refs []MessageRef) (map[MessageKey]*messagingpb.Message, error) {
	messages := make(map[MessageKey]*messagingpb.Message, len(refs))
	var toRead []MessageRef
	for _, ref := range refs {
		key := MessageKeyOf(ref.ChatID, ref.MessageID)
		if _, ok := messages[key]; ok {
			continue
		}
		if m, ok := known[key]; ok {
			messages[key] = m
			continue
		}
		// Marked as asked for, so a message named twice is read once.
		messages[key] = nil
		toRead = append(toRead, ref)
	}
	if len(toRead) > 0 {
		read, err := reader.Messages(ctx, toRead, reading)
		if err != nil {
			return nil, err
		}
		for _, ref := range toRead {
			key := MessageKeyOf(ref.ChatID, ref.MessageID)
			messages[key] = read[key]
		}
	}
	return messages, nil
}

// pinnedMessage returns the message of chatID's pin p from messages (see
// readPinnedMessages), or errPinnedMessageMissing when it holds none: every
// Pin the service shows carries its message (see above).
func pinnedMessage(messages map[MessageKey]*messagingpb.Message, chatID *commonpb.ChatId, p Pin) (*messagingpb.Message, error) {
	m := messages[MessageKeyOf(chatID, p.MessageID)]
	if m == nil {
		return nil, fmt.Errorf("%w: message %d", errPinnedMessageMissing, p.MessageID.GetValue())
	}
	return m, nil
}

// pinSummaryToProto projects chatID's summary onto its proto, its latest pin
// carrying its message from messages (see readPinnedMessages), failing with
// errPinnedMessageMissing as pinnedMessage does. A summary with no latest pin
// needs no message.
func pinSummaryToProto(messages map[MessageKey]*messagingpb.Message, chatID *commonpb.ChatId, summary PinSummary) (*chatpb.PinSummary, error) {
	var latest *messagingpb.Message
	if summary.Latest != nil {
		m, err := pinnedMessage(messages, chatID, *summary.Latest)
		if err != nil {
			return nil, err
		}
		latest = m
	}
	return summary.ToProto(latest), nil
}

// publishPinsChanged announces a real change to c's pins to its members (see
// above), as one ChatUpdate carrying change: a group's once on its chat
// topic, excluding no one, so the caller's other devices hear it too; a DM's
// on each member's user topic, as every DM update is, except the Flipcash
// team account's, which is never delivered to (see messaging.NewSender).
// Best-effort and non-blocking, like every publish here.
func (s *Server) publishPinsChanged(c *Chat, change *chatpb.MetadataUpdate_PinsChanged) {
	e := &eventpb.Event{
		Id: model.MustGenerateEventID(),
		Ts: timestamppb.Now(),
		Type: &eventpb.Event_ChatUpdate{ChatUpdate: &eventpb.ChatUpdate{
			Chat: c.ID,
			MetadataUpdates: []*chatpb.MetadataUpdate{{
				Kind: &chatpb.MetadataUpdate_PinsChanged_{PinsChanged: change},
			}},
		}},
	}
	if IsGroupChatID(c.ID) {
		s.chatEventBus.OnEvent(c.ID, &eventpb.ChatEvent{ChatId: c.ID, Event: e})
		return
	}
	team := s.rules.teamUserID
	for _, m := range c.Members {
		if team != nil && bytes.Equal(m.Value, team.Value) {
			continue
		}
		s.userEventBus.OnEvent(m, e)
	}
}

// pinnableMessage reports whether m is of a kind that may be pinned: it has
// content, and every piece of it is a kind on the allowlist. A deleted
// message's tombstone (DeletedContent) and a system message (SystemContent)
// are not, and nor is any kind this server does not know, so a kind the proto
// gains is refused until it is added here.
func pinnableMessage(m *messagingpb.Message) bool {
	if len(m.Content) == 0 {
		return false
	}
	for _, c := range m.Content {
		switch c.Type.(type) {
		case *messagingpb.Content_Text,
			*messagingpb.Content_Cash,
			*messagingpb.Content_Reply,
			*messagingpb.Content_Media,
			*messagingpb.Content_Encrypted,
			*messagingpb.Content_Widget:
		default:
			return false
		}
	}
	return true
}

// pinLog is the logger a pin or unpin logs with: the caller, the chat and the
// message.
func (s *Server) pinLog(userID *commonpb.UserId, chatID *commonpb.ChatId, messageID *messagingpb.MessageId) *zap.Logger {
	return s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", model.ChatIDString(chatID)),
		zap.Uint64("message_id", messageID.GetValue()),
	)
}

// encodePinToken serializes a GetPinnedMessages page's resume position,
// bound to the chat it pages: the version byte, the chat ID zero-padded to
// DmChatIDSize, the pin time as big-endian Unix nanoseconds, and the message
// ID, big-endian.
func encodePinToken(chatID *commonpb.ChatId, pos PinPosition) *commonpb.PagingToken {
	buf := make([]byte, pinTokenLen)
	buf[0] = pinTokenVersion
	copy(buf[1:1+DmChatIDSize], chatID.Value)
	binary.BigEndian.PutUint64(buf[1+DmChatIDSize:], uint64(pos.PinnedAt.UnixNano()))
	binary.BigEndian.PutUint64(buf[1+DmChatIDSize+8:], pos.MessageID.GetValue())
	return &commonpb.PagingToken{Value: buf}
}

// decodePinToken parses a GetPinnedMessages token minted for chatID; a
// malformed token, one whose pin time no pin could hold, or one bound to
// another chat, is refused.
func decodePinToken(token *commonpb.PagingToken, chatID *commonpb.ChatId) (PinPosition, bool) {
	b := token.GetValue()
	if len(b) != pinTokenLen || b[0] != pinTokenVersion {
		return PinPosition{}, false
	}
	bound := make([]byte, DmChatIDSize)
	copy(bound, chatID.GetValue())
	if !bytes.Equal(b[1:1+DmChatIDSize], bound) {
		return PinPosition{}, false
	}
	nanos := binary.BigEndian.Uint64(b[1+DmChatIDSize:])
	if nanos > math.MaxInt64 {
		return PinPosition{}, false
	}
	return PinPosition{
		PinnedAt:  time.Unix(0, int64(nanos)).UTC(),
		MessageID: &messagingpb.MessageId{Value: binary.BigEndian.Uint64(b[1+DmChatIDSize+8:])},
	}, true
}
