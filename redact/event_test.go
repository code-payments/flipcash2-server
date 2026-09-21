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
			{Kind: &chatpb.MetadataUpdate_PictureChanged_{PictureChanged: &chatpb.MetadataUpdate_PictureChanged{
				NewPicture: &blobpb.Media{Renditions: []*blobpb.Rendition{{Role: blobpb.Rendition_ORIGINAL, BlobId: &blobpb.BlobId{Value: bytes.Repeat([]byte{7}, 16)}}}},
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
	// last-activity change, a viewer-state change, a title change and a
	// picture change are as they were.
	require.Len(t, out.MetadataUpdates, 5)
	refreshed := out.MetadataUpdates[0].GetFullRefresh().GetMetadata()
	assert.Equal(t, "Title", refreshed.Title)
	expected, err := Message(chatID, in.MetadataUpdates[0].GetFullRefresh().GetMetadata().LastMessage)
	require.NoError(t, err)
	assert.True(t, proto.Equal(expected, refreshed.LastMessage))
	for i := 1; i < 5; i++ {
		assert.True(t, proto.Equal(in.MetadataUpdates[i], out.MetadataUpdates[i]), "metadata update %d", i)
		assert.NotSame(t, in.MetadataUpdates[i], out.MetadataUpdates[i])
	}

	// A join keeps its member and summary with its metadata's last message
	// redacted; a departure is as it was.
	require.Len(t, out.RosterUpdates.RosterUpdates, 2)
	joined := out.RosterUpdates.RosterUpdates[0]
	assert.True(t, proto.Equal(in.RosterUpdates.RosterUpdates[0].GetMemberJoined().Member, joined.GetMemberJoined().Member))
	assert.True(t, proto.Equal(in.RosterUpdates.RosterUpdates[0].RosterSummary, joined.RosterSummary))
	expected, err = Message(chatID, in.RosterUpdates.RosterUpdates[0].GetMemberJoined().Metadata.LastMessage)
	require.NoError(t, err)
	assert.True(t, proto.Equal(expected, joined.GetMemberJoined().Metadata.LastMessage))
	assert.True(t, proto.Equal(in.RosterUpdates.RosterUpdates[1], out.RosterUpdates.RosterUpdates[1]))

	// A redacted update is its own redaction.
	again, err := ChatUpdate(chatID, out)
	require.NoError(t, err)
	assert.True(t, proto.Equal(out, again))

	// What is absent stays absent: nothing is invented for a bare update.
	bare, err := ChatUpdate(chatID, &eventpb.ChatUpdate{Chat: chatID})
	require.NoError(t, err)
	assert.True(t, proto.Equal(&eventpb.ChatUpdate{Chat: chatID}, bare))

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
