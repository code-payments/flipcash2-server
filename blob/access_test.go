package blob

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// newChatID mints a chat ID the way the chat domain mints a group's: a random
// UUID. The resolvers treat the ID as opaque, so its family does not matter
// here, and using a local helper keeps this package's tests free of the chat
// package, which imports this one.
func newChatID() *commonpb.ChatId {
	id := uuid.New()
	return &commonpb.ChatId{Value: id[:]}
}

// stubMembership is a ChatMembership answering from a fixed roster per chat.
// An unknown chat has no members, as the real store reports it.
type stubMembership struct {
	members map[string][]*commonpb.UserId
}

func (s *stubMembership) IsMember(_ context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	for _, m := range s.members[string(chatID.Value)] {
		if bytes.Equal(m.Value, userID.Value) {
			return true, nil
		}
	}
	return false, nil
}

// stubResolver is a controllable PrincipalResolver that records how often it is
// consulted, so routing can be asserted.
type stubResolver struct {
	result bool
	err    error
	calls  int
}

func (s *stubResolver) Covers(context.Context, Principal, *commonpb.UserId) (bool, error) {
	s.calls++
	return s.result, s.err
}

func TestCompositeResolver_Routes(t *testing.T) {
	ctx := context.Background()
	user := model.MustGenerateUserID()

	chatStub := &stubResolver{result: true}
	r := NewCompositeResolver(map[PrincipalType]PrincipalResolver{
		PrincipalTypeChat: chatStub,
	})

	// A chat principal routes to the registered resolver and returns its decision.
	ok, err := r.Covers(ctx, Principal{Type: PrincipalTypeChat, ID: []byte("chat")}, user)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, chatStub.calls)

	// A principal whose type has no registered resolver is not covered, and no
	// resolver is consulted.
	ok, err = r.Covers(ctx, Principal{Type: PrincipalTypeUser, ID: []byte("user")}, user)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, 1, chatStub.calls)

	// The routed resolver's error propagates.
	chatStub.err = errors.New("boom")
	_, err = r.Covers(ctx, Principal{Type: PrincipalTypeChat, ID: []byte("chat")}, user)
	require.Error(t, err)
}

func TestCompositeResolver_CopiesRoutingTable(t *testing.T) {
	ctx := context.Background()
	user := model.MustGenerateUserID()

	chatStub := &stubResolver{result: true}
	input := map[PrincipalType]PrincipalResolver{PrincipalTypeChat: chatStub}
	r := NewCompositeResolver(input)

	// Mutating the input map after construction does not change routing.
	delete(input, PrincipalTypeChat)

	ok, err := r.Covers(ctx, Principal{Type: PrincipalTypeChat, ID: []byte("chat")}, user)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestChatResolver_Covers(t *testing.T) {
	ctx := context.Background()

	member := model.MustGenerateUserID()
	stranger := model.MustGenerateUserID()
	chatID := newChatID()
	chats := &stubMembership{members: map[string][]*commonpb.UserId{
		string(chatID.Value): {member},
	}}

	r := NewChatResolver(chats)

	// A member of the chat is covered by the chat principal.
	ok, err := r.Covers(ctx, PrincipalForChat(chatID), member)
	require.NoError(t, err)
	require.True(t, ok)

	// A non-member is not covered.
	ok, err = r.Covers(ctx, PrincipalForChat(chatID), stranger)
	require.NoError(t, err)
	require.False(t, ok)

	// An unknown chat is not covered (IsMember reports false without error).
	unknownChat := newChatID()
	ok, err = r.Covers(ctx, PrincipalForChat(unknownChat), member)
	require.NoError(t, err)
	require.False(t, ok)

	// A non-chat principal is outside this resolver's scope and is never covered
	// — including the chat's public profile, which is deliberately not a
	// membership question.
	ok, err = r.Covers(ctx, PrincipalForUser(member), member)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = r.Covers(ctx, PrincipalForChatProfile(chatID), member)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestChatProfileResolver_Covers(t *testing.T) {
	ctx := context.Background()

	member := model.MustGenerateUserID()
	stranger := model.MustGenerateUserID()
	chatID := newChatID()

	r := NewChatProfileResolver()

	// A chat's profile is public: every caller is covered, member or not, and no
	// chat store is consulted (the chat need not even exist).
	for _, user := range []*commonpb.UserId{member, stranger} {
		ok, err := r.Covers(ctx, PrincipalForChatProfile(chatID), user)
		require.NoError(t, err)
		require.True(t, ok)
	}

	// Coverage is universal, so the grant must carry the decision: any other
	// principal type — the chat's members in particular — is outside this
	// resolver's scope and is never covered through it.
	for _, principal := range []Principal{PrincipalForChat(chatID), PrincipalForUserProfile(member), PrincipalForUser(member)} {
		ok, err := r.Covers(ctx, principal, member)
		require.NoError(t, err)
		require.False(t, ok)
	}
}

func TestGrant_Validate_PrincipalTypes(t *testing.T) {
	blobID := &blobpb.BlobId{Value: []byte("blob")}
	chatID := newChatID()
	user := model.MustGenerateUserID()

	// Every principal type is a well-formed grant subject.
	for _, principal := range []Principal{PrincipalForUser(user), PrincipalForChat(chatID), PrincipalForUserProfile(user), PrincipalForChatProfile(chatID)} {
		require.NoError(t, (&Grant{BlobID: blobID, Principal: principal, Permission: PermissionRead}).Validate())
	}

	// An unknown type or an empty id is not.
	require.ErrorIs(t, (&Grant{BlobID: blobID, Principal: Principal{Type: PrincipalTypeUnknown, ID: []byte("x")}, Permission: PermissionRead}).Validate(), ErrInvalidGrant)
	require.ErrorIs(t, (&Grant{BlobID: blobID, Principal: Principal{Type: PrincipalTypeChatProfile}, Permission: PermissionRead}).Validate(), ErrInvalidGrant)
}
