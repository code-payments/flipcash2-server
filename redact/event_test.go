package redact

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	paymentrequestpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/paymentrequest/v1"
)

// chatUpdateFixture is an update carrying a message in each place one rides
// — a sent, an edited and a deleted mutation, a metadata refresh's last
// message, and a join's metadata — alongside every overlay.
func chatUpdateFixture() *eventpb.ChatUpdate {
	member := &commonpb.UserId{Value: bytes.Repeat([]byte{6}, 16)}
	return &eventpb.ChatUpdate{
		Chat: chatID,
		PointerUpdates: &messagingpb.PointerBatch{Pointers: []*messagingpb.Pointer{{
			Type: messagingpb.Pointer_READ, Value: messageID, UserId: member,
		}}},
		IsTypingNotifications: &messagingpb.IsTypingNotificationBatch{IsTypingNotifications: []*messagingpb.IsTypingNotification{{
			UserId: member, State: messagingpb.IsTypingNotification_STARTED_TYPING,
		}}},
		ReactionUpdates: &messagingpb.ReactionUpdateBatch{ReactionUpdates: []*messagingpb.ReactionUpdate{{
			MessageId: messageID, Emoji: &messagingpb.Emoji{Value: "🔥"}, Version: 4,
		}}},
		MetadataUpdates: []*chatpb.MetadataUpdate{
			{Kind: &chatpb.MetadataUpdate_FullRefresh_{FullRefresh: &chatpb.MetadataUpdate_FullRefresh{
				Metadata: &chatpb.Metadata{ChatId: chatID, Title: "Title", LastMessage: messageFixture(textFixture())},
			}}},
			{Kind: &chatpb.MetadataUpdate_LastActivityChanged_{LastActivityChanged: &chatpb.MetadataUpdate_LastActivityChanged{
				NewLastActivity: timestamppb.Now(),
			}}},
			{Kind: &chatpb.MetadataUpdate_ViewerStateChanged_{ViewerStateChanged: &chatpb.MetadataUpdate_ViewerStateChanged{
				ViewerState: &chatpb.ViewerState{
					Settings:    &chatpb.ViewerState_Settings{Mute: &chatpb.MuteState{Duration: &chatpb.MuteState_Forever_{Forever: &chatpb.MuteState_Forever{}}}},
					Permissions: &chatpb.ViewerState_Permissions{CanEdit: true},
					Version:     2,
				},
			}}},
			{Kind: &chatpb.MetadataUpdate_TitleChanged_{TitleChanged: &chatpb.MetadataUpdate_TitleChanged{
				NewTitle: "New Title",
			}}},
			{Kind: &chatpb.MetadataUpdate_ProfilePictureChanged_{ProfilePictureChanged: &chatpb.MetadataUpdate_ProfilePictureChanged{
				NewProfilePicture: &blobpb.Media{Renditions: []*blobpb.Rendition{{Role: blobpb.Rendition_ORIGINAL, BlobId: &blobpb.BlobId{Value: bytes.Repeat([]byte{7}, 16)}}}},
			}}},
			{Kind: &chatpb.MetadataUpdate_DescriptionChanged_{DescriptionChanged: &chatpb.MetadataUpdate_DescriptionChanged{
				NewDescription: "New description",
			}}},
			{Kind: &chatpb.MetadataUpdate_CoverPictureChanged_{CoverPictureChanged: &chatpb.MetadataUpdate_CoverPictureChanged{
				NewCoverPicture: &blobpb.Media{Renditions: []*blobpb.Rendition{{Role: blobpb.Rendition_ORIGINAL, BlobId: &blobpb.BlobId{Value: bytes.Repeat([]byte{8}, 16)}}}},
			}}},
			{Kind: &chatpb.MetadataUpdate_RulesChanged_{RulesChanged: &chatpb.MetadataUpdate_RulesChanged{
				NewRules: &chatpb.Rules{
					Listener: []*chatpb.ListenerRules{{Kind: &chatpb.ListenerRules_MinimumBalance{MinimumBalance: &chatpb.MinimumBalanceRequirement{
						Amount: &commonpb.FiatPaymentAmount{Currency: "usd", NativeAmount: 5},
					}}}},
					Speaker: []*chatpb.SpeakerRules{{Kind: &chatpb.SpeakerRules_MinimumBalance{MinimumBalance: &chatpb.MinimumBalanceRequirement{
						Amount: &commonpb.FiatPaymentAmount{Currency: "usd", NativeAmount: 10},
					}}}},
				},
			}}},
		},
		Events: &messagingpb.EventBatch{Events: []*messagingpb.Event{{
			Sequence: 3,
			Count:    3,
			Ts:       timestamppb.Now(),
			Mutations: []*messagingpb.Mutation{
				{Type: &messagingpb.Mutation_MessageSent{MessageSent: messageFixture(textFixture())}},
				{Type: &messagingpb.Mutation_MessageEdited{MessageEdited: messageFixture(mediaFixture())}},
				{Type: &messagingpb.Mutation_MessageDeleted{MessageDeleted: messageFixture(deletedFixture())}},
			},
		}}},
		RosterUpdates: &chatpb.RosterUpdateBatch{RosterUpdates: []*chatpb.RosterUpdate{
			{
				Kind: &chatpb.RosterUpdate_MemberJoined_{MemberJoined: &chatpb.RosterUpdate_MemberJoined{
					Member:   &chatpb.Member{UserId: member},
					Metadata: &chatpb.Metadata{ChatId: chatID, LastMessage: messageFixture(textFixture())},
				}},
				RosterSummary: &chatpb.RosterSummary{MemberCount: 2, Version: 5},
			},
			{
				Kind:          &chatpb.RosterUpdate_MemberLeft_{MemberLeft: &chatpb.RosterUpdate_MemberLeft{UserId: member}},
				RosterSummary: &chatpb.RosterSummary{MemberCount: 1, Version: 6},
			},
			{
				Kind:          &chatpb.RosterUpdate_MembershipChanged_{MembershipChanged: &chatpb.RosterUpdate_MembershipChanged{}},
				RosterSummary: &chatpb.RosterSummary{MemberCount: 2, Version: 7},
			},
		}},
	}
}

func TestChatUpdate(t *testing.T) {
	in := chatUpdateFixture()
	before := proto.Clone(in)

	out, err := ChatUpdate(chatID, in)
	require.NoError(t, err)
	assert.True(t, proto.Equal(before, in), "input modified")

	// The overlays pass through as they are, and as copies.
	assert.True(t, proto.Equal(in.Chat, out.Chat))
	assert.True(t, proto.Equal(in.PointerUpdates, out.PointerUpdates))
	assert.True(t, proto.Equal(in.IsTypingNotifications, out.IsTypingNotifications))
	assert.True(t, proto.Equal(in.ReactionUpdates, out.ReactionUpdates))
	assert.NotSame(t, in.PointerUpdates, out.PointerUpdates)
	assert.NotSame(t, in.IsTypingNotifications, out.IsTypingNotifications)
	assert.NotSame(t, in.ReactionUpdates, out.ReactionUpdates)

	// Every mutation's message is redacted, in place, whichever kind it is;
	// the event's position and time are kept.
	require.Len(t, out.Events.Events, 1)
	inEvent, outEvent := in.Events.Events[0], out.Events.Events[0]
	assert.Equal(t, inEvent.Sequence, outEvent.Sequence)
	assert.Equal(t, inEvent.Count, outEvent.Count)
	assert.True(t, proto.Equal(inEvent.Ts, outEvent.Ts))
	require.Len(t, outEvent.Mutations, 3)
	for i, kind := range []func(*messagingpb.Mutation) *messagingpb.Message{
		(*messagingpb.Mutation).GetMessageSent,
		(*messagingpb.Mutation).GetMessageEdited,
		(*messagingpb.Mutation).GetMessageDeleted,
	} {
		expected, err := Message(chatID, kind(inEvent.Mutations[i]))
		require.NoError(t, err)
		assert.True(t, proto.Equal(expected, kind(outEvent.Mutations[i])), "mutation %d", i)
	}

	// A metadata refresh keeps its record with the last message redacted; a
	// last-activity change, a viewer-state change, a title, profile picture,
	// description or cover picture change, and a rules change are as they
	// were.
	require.Len(t, out.MetadataUpdates, 8)
	refreshed := out.MetadataUpdates[0].GetFullRefresh().GetMetadata()
	assert.Equal(t, "Title", refreshed.Title)
	expected, err := Message(chatID, in.MetadataUpdates[0].GetFullRefresh().GetMetadata().LastMessage)
	require.NoError(t, err)
	assert.True(t, proto.Equal(expected, refreshed.LastMessage))
	for i := 1; i < 8; i++ {
		assert.True(t, proto.Equal(in.MetadataUpdates[i], out.MetadataUpdates[i]), "metadata update %d", i)
		assert.NotSame(t, in.MetadataUpdates[i], out.MetadataUpdates[i])
	}

	// A join keeps its member and summary with its metadata's last message
	// redacted; a departure, and a change naming no one, are as they were.
	require.Len(t, out.RosterUpdates.RosterUpdates, 3)
	joined := out.RosterUpdates.RosterUpdates[0]
	assert.True(t, proto.Equal(in.RosterUpdates.RosterUpdates[0].GetMemberJoined().Member, joined.GetMemberJoined().Member))
	assert.True(t, proto.Equal(in.RosterUpdates.RosterUpdates[0].RosterSummary, joined.RosterSummary))
	expected, err = Message(chatID, in.RosterUpdates.RosterUpdates[0].GetMemberJoined().Metadata.LastMessage)
	require.NoError(t, err)
	assert.True(t, proto.Equal(expected, joined.GetMemberJoined().Metadata.LastMessage))
	assert.True(t, proto.Equal(in.RosterUpdates.RosterUpdates[1], out.RosterUpdates.RosterUpdates[1]))
	assert.True(t, proto.Equal(in.RosterUpdates.RosterUpdates[2], out.RosterUpdates.RosterUpdates[2]))

	// A redacted update is its own redaction.
	again, err := ChatUpdate(chatID, out)
	require.NoError(t, err)
	assert.True(t, proto.Equal(out, again))

	// What is absent stays absent: nothing is invented for a bare update.
	bare, err := ChatUpdate(chatID, &eventpb.ChatUpdate{Chat: chatID})
	require.NoError(t, err)
	assert.True(t, proto.Equal(&eventpb.ChatUpdate{Chat: chatID}, bare))

	// Payment request updates are left out, alongside everything else and
	// alone: a redacted viewer sees a request's widget, not the request.
	requestID := &paymentrequestpb.PaymentRequestId{Value: bytes.Repeat([]byte{7}, 16)}
	requests := &paymentrequestpb.PaymentRequestUpdateBatch{PaymentRequestUpdates: []*paymentrequestpb.PaymentRequestUpdate{
		{
			PaymentRequestId: requestID,
			Kind:             &paymentrequestpb.PaymentRequestUpdate_Created{Created: &paymentrequestpb.PaymentRequestMetadata{PaymentRequestId: requestID, Version: 1}},
		},
		{
			PaymentRequestId: requestID,
			Kind: &paymentrequestpb.PaymentRequestUpdate_Updates_{Updates: &paymentrequestpb.PaymentRequestUpdate_Updates{
				Version: 2,
				Updates: []*paymentrequestpb.PaymentRequestUpdate_Update{{
					Kind: &paymentrequestpb.PaymentRequestUpdate_Update_StatusUpdate{StatusUpdate: &paymentrequestpb.PaymentRequestUpdate_StatusUpdate{
						Status:   paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_CLOSED,
						ClosedAt: timestamppb.Now(),
					}},
				}},
			}},
		},
	}}
	withRequests := proto.Clone(in).(*eventpb.ChatUpdate)
	withRequests.PaymentRequestUpdates = requests
	redacted, err := ChatUpdate(chatID, withRequests)
	require.NoError(t, err)
	assert.Nil(t, redacted.PaymentRequestUpdates)
	assert.True(t, proto.Equal(out, redacted))
	alone, err := ChatUpdate(chatID, &eventpb.ChatUpdate{Chat: chatID, PaymentRequestUpdates: requests})
	require.NoError(t, err)
	assert.True(t, proto.Equal(&eventpb.ChatUpdate{Chat: chatID}, alone))

	// A join without metadata carries none.
	noMetadata, err := ChatUpdate(chatID, &eventpb.ChatUpdate{Chat: chatID, RosterUpdates: &chatpb.RosterUpdateBatch{RosterUpdates: []*chatpb.RosterUpdate{{
		Kind:          &chatpb.RosterUpdate_MemberJoined_{MemberJoined: &chatpb.RosterUpdate_MemberJoined{Member: &chatpb.Member{UserId: &commonpb.UserId{Value: bytes.Repeat([]byte{6}, 16)}}}},
		RosterSummary: &chatpb.RosterSummary{MemberCount: 2, Version: 5},
	}}}})
	require.NoError(t, err)
	assert.Nil(t, noMetadata.RosterUpdates.RosterUpdates[0].GetMemberJoined().Metadata)
}

// TestChatUpdate_Refuses: anything the walker has no decision for fails the
// whole update, as content Message cannot redact fails the message.
func TestChatUpdate_Refuses(t *testing.T) {
	unknownMutation := &eventpb.ChatUpdate{Chat: chatID, Events: &messagingpb.EventBatch{Events: []*messagingpb.Event{{
		Sequence: 1, Count: 1, Ts: timestamppb.Now(), Mutations: []*messagingpb.Mutation{{}},
	}}}}
	_, err := ChatUpdate(chatID, unknownMutation)
	assert.Error(t, err)

	unknownMetadata := &eventpb.ChatUpdate{Chat: chatID, MetadataUpdates: []*chatpb.MetadataUpdate{{}}}
	_, err = ChatUpdate(chatID, unknownMetadata)
	assert.Error(t, err)

	unknownRoster := &eventpb.ChatUpdate{Chat: chatID, RosterUpdates: &chatpb.RosterUpdateBatch{RosterUpdates: []*chatpb.RosterUpdate{{}}}}
	_, err = ChatUpdate(chatID, unknownRoster)
	assert.Error(t, err)

	unredactable := &eventpb.ChatUpdate{Chat: chatID, Events: &messagingpb.EventBatch{Events: []*messagingpb.Event{{
		Sequence: 1, Count: 1, Ts: timestamppb.Now(),
		Mutations: []*messagingpb.Mutation{{Type: &messagingpb.Mutation_MessageSent{MessageSent: messageFixture(&messagingpb.Content{})}}},
	}}}}
	_, err = ChatUpdate(chatID, unredactable)
	assert.ErrorIs(t, err, ErrUnsupportedContent)
}

// pinFixture is a pin of messageID carrying a text message, made by a member.
func pinFixture() *chatpb.Pin {
	return &chatpb.Pin{
		MessageId: messageID,
		PinnedBy:  &commonpb.UserId{Value: bytes.Repeat([]byte{9}, 16)},
		PinnedAt:  timestamppb.Now(),
		Message:   messageFixture(textFixture()),
	}
}

// requireRedactedPin asserts out is in with its message redacted and nothing
// else changed: which message, who pinned it and when.
func requireRedactedPin(t *testing.T, in, out *chatpb.Pin) {
	t.Helper()
	require.NotNil(t, out)
	expected, err := Message(chatID, in.Message)
	require.NoError(t, err)
	assert.True(t, out.Message.Redacted)
	assert.True(t, proto.Equal(expected, out.Message))
	assert.True(t, proto.Equal(in.MessageId, out.MessageId))
	assert.True(t, proto.Equal(in.PinnedBy, out.PinnedBy))
	assert.True(t, proto.Equal(in.PinnedAt, out.PinnedAt))
	assert.NotSame(t, in, out)
}

// TestChatUpdate_Pins: every pinned message an update carries is redacted —
// the pin a PinsChanged makes, the latest of its summary, and the latest of
// the pin summary on a metadata refresh's or a join's metadata — and the rest
// of each pin and summary is kept.
func TestChatUpdate_Pins(t *testing.T) {
	summary := func() *chatpb.PinSummary {
		return &chatpb.PinSummary{Latest: pinFixture(), Count: 2, Version: 7}
	}
	in := &eventpb.ChatUpdate{
		Chat: chatID,
		MetadataUpdates: []*chatpb.MetadataUpdate{
			{Kind: &chatpb.MetadataUpdate_PinsChanged_{PinsChanged: &chatpb.MetadataUpdate_PinsChanged{
				Change:  &chatpb.MetadataUpdate_PinsChanged_Pinned{Pinned: pinFixture()},
				Summary: summary(),
			}}},
			{Kind: &chatpb.MetadataUpdate_PinsChanged_{PinsChanged: &chatpb.MetadataUpdate_PinsChanged{
				Change:  &chatpb.MetadataUpdate_PinsChanged_Unpinned{Unpinned: &messagingpb.MessageId{Value: 41}},
				Summary: summary(),
			}}},
			{Kind: &chatpb.MetadataUpdate_PinsChanged_{PinsChanged: &chatpb.MetadataUpdate_PinsChanged{
				Change:  &chatpb.MetadataUpdate_PinsChanged_Unpinned{Unpinned: &messagingpb.MessageId{Value: 42}},
				Summary: &chatpb.PinSummary{Version: 8},
			}}},
			{Kind: &chatpb.MetadataUpdate_FullRefresh_{FullRefresh: &chatpb.MetadataUpdate_FullRefresh{
				Metadata: &chatpb.Metadata{ChatId: chatID, Title: "Title", LastMessage: messageFixture(textFixture()), PinSummary: summary()},
			}}},
		},
		RosterUpdates: &chatpb.RosterUpdateBatch{RosterUpdates: []*chatpb.RosterUpdate{{
			Kind: &chatpb.RosterUpdate_MemberJoined_{MemberJoined: &chatpb.RosterUpdate_MemberJoined{
				Member:   &chatpb.Member{UserId: &commonpb.UserId{Value: bytes.Repeat([]byte{6}, 16)}},
				Metadata: &chatpb.Metadata{ChatId: chatID, PinSummary: summary()},
			}},
			RosterSummary: &chatpb.RosterSummary{MemberCount: 2, Version: 5},
		}}},
	}
	before := proto.Clone(in)

	out, err := ChatUpdate(chatID, in)
	require.NoError(t, err)
	assert.True(t, proto.Equal(before, in), "input modified")
	require.Len(t, out.MetadataUpdates, 4)

	requireRedactedSummary := func(in, out *chatpb.PinSummary) {
		t.Helper()
		require.NotNil(t, out)
		assert.Equal(t, in.Count, out.Count)
		assert.Equal(t, in.Version, out.Version)
		if in.Latest == nil {
			assert.Nil(t, out.Latest)
			return
		}
		requireRedactedPin(t, in.Latest, out.Latest)
	}

	// A pin: the pin made and the summary's latest are redacted.
	pinned := out.MetadataUpdates[0].GetPinsChanged()
	requireRedactedPin(t, in.MetadataUpdates[0].GetPinsChanged().GetPinned(), pinned.GetPinned())
	requireRedactedSummary(in.MetadataUpdates[0].GetPinsChanged().Summary, pinned.Summary)

	// An unpin: the ID as it was, and the summary's latest redacted, or no
	// latest when nothing is left pinned.
	for _, i := range []int{1, 2} {
		unpinned := out.MetadataUpdates[i].GetPinsChanged()
		assert.True(t, proto.Equal(in.MetadataUpdates[i].GetPinsChanged().GetUnpinned(), unpinned.GetUnpinned()))
		requireRedactedSummary(in.MetadataUpdates[i].GetPinsChanged().Summary, unpinned.Summary)
	}

	// A metadata refresh's and a join's metadata: the pin summary's latest is
	// redacted beside the last message.
	refreshed := out.MetadataUpdates[3].GetFullRefresh().GetMetadata()
	assert.Equal(t, "Title", refreshed.Title)
	assert.True(t, refreshed.LastMessage.Redacted)
	requireRedactedSummary(in.MetadataUpdates[3].GetFullRefresh().GetMetadata().PinSummary, refreshed.PinSummary)
	joined := out.RosterUpdates.RosterUpdates[0].GetMemberJoined().GetMetadata()
	requireRedactedSummary(in.RosterUpdates.RosterUpdates[0].GetMemberJoined().GetMetadata().PinSummary, joined.PinSummary)

	// A redacted update is its own redaction.
	again, err := ChatUpdate(chatID, out)
	require.NoError(t, err)
	assert.True(t, proto.Equal(out, again))

	// A pin whose message cannot be redacted fails the update, wherever it
	// rides; a pins change of a kind this version does not know is refused.
	for _, md := range []*chatpb.MetadataUpdate{
		{Kind: &chatpb.MetadataUpdate_PinsChanged_{PinsChanged: &chatpb.MetadataUpdate_PinsChanged{
			Change:  &chatpb.MetadataUpdate_PinsChanged_Pinned{Pinned: &chatpb.Pin{MessageId: messageID, Message: messageFixture(&messagingpb.Content{})}},
			Summary: &chatpb.PinSummary{},
		}}},
		{Kind: &chatpb.MetadataUpdate_PinsChanged_{PinsChanged: &chatpb.MetadataUpdate_PinsChanged{
			Change:  &chatpb.MetadataUpdate_PinsChanged_Unpinned{Unpinned: messageID},
			Summary: &chatpb.PinSummary{Latest: &chatpb.Pin{MessageId: messageID, Message: messageFixture(&messagingpb.Content{})}},
		}}},
		{Kind: &chatpb.MetadataUpdate_FullRefresh_{FullRefresh: &chatpb.MetadataUpdate_FullRefresh{
			Metadata: &chatpb.Metadata{ChatId: chatID, PinSummary: &chatpb.PinSummary{Latest: &chatpb.Pin{MessageId: messageID, Message: messageFixture(&messagingpb.Content{})}}},
		}}},
	} {
		_, err := ChatUpdate(chatID, &eventpb.ChatUpdate{Chat: chatID, MetadataUpdates: []*chatpb.MetadataUpdate{md}})
		assert.ErrorIs(t, err, ErrUnsupportedContent)
	}
	_, err = ChatUpdate(chatID, &eventpb.ChatUpdate{Chat: chatID, MetadataUpdates: []*chatpb.MetadataUpdate{{
		Kind: &chatpb.MetadataUpdate_PinsChanged_{PinsChanged: &chatpb.MetadataUpdate_PinsChanged{Summary: &chatpb.PinSummary{}}},
	}}})
	assert.Error(t, err)
}
