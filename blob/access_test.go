package blob

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/chat"
	chat_memory "github.com/code-payments/flipcash2-server/chat/memory"
	"github.com/code-payments/flipcash2-server/model"
)

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

	chats := chat_memory.NewInMemory()
	member := model.MustGenerateUserID()
	stranger := model.MustGenerateUserID()
	chatID := chat.MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, member, stranger)
	require.NoError(t, chats.PutChat(ctx, &chat.Chat{
		ID:      chatID,
		Members: []*commonpb.UserId{member},
	}))

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
	unknownChat := chat.MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, member, model.MustGenerateUserID())
	ok, err = r.Covers(ctx, PrincipalForChat(unknownChat), member)
	require.NoError(t, err)
	require.False(t, ok)

	// A non-chat principal is outside this resolver's scope and is never covered.
	ok, err = r.Covers(ctx, PrincipalForUser(member), member)
	require.NoError(t, err)
	require.False(t, ok)
}
