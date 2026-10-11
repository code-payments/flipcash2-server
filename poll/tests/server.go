package tests

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"
	pollpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/poll/v1"

	"github.com/code-payments/flipcash2-server/account"
	accountmemory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/balance"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/moderation"
	"github.com/code-payments/flipcash2-server/poll"
	"github.com/code-payments/flipcash2-server/testutil"
	ocp_balancepb "github.com/code-payments/ocp-protobuf-api/generated/go/balance/v1"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	ocp_currency "github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/solana/currencycreator"
)

// RunServerTests runs the shared poll.Server test suite. chats and polls are
// the backing stores; teardown resets both between tests.
func RunServerTests(t *testing.T, chats chat.Store, polls poll.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, chats chat.Store, polls poll.Store){
		testServer_CreatePoll_PublicGroup,
		testServer_CreatePoll_Idempotent,
		testServer_CreatePoll_Refused,
		testServer_CreatePoll_BalanceParameters,
		testServer_CreatePoll_Moderation,
		testServer_CreatePoll_PrivateGroup,
		testServer_GetPoll_Visibility,
		testServer_GetPolls,
		testServer_Vote,
		testServer_Vote_MemberOnly,
		testServer_Vote_Closed,
		testServer_BalanceResults_Ranked,
	} {
		tf(t, chats, polls)
		teardown()
	}
}

// testServer_CreatePoll_PublicGroup pins a public group's poll: created
// unsent, visible to its creator alone and taking no votes until it goes
// live, then everyone's.
func testServer_CreatePoll_PublicGroup(t *testing.T, chats chat.Store, polls poll.Store) {
	e := newServerEnv(t, chats, polls)
	member := e.addUser()
	chatID := e.putPublicGroup(nil, member)

	definition := plaintextDefinition(e.clock.Now().Add(time.Hour), "Lunch?", "Pizza", "Tacos")
	resp := e.createPoll(e.creator, chatID, definition, newIdempotencyKey())
	require.Equal(t, pollpb.CreatePollResponse_OK, resp.Result)
	require.Equal(t, moderationpb.FlaggedCategory_NONE, resp.FlaggedCategory)

	created := resp.Poll
	require.Equal(t, chatID.Value, created.ChatId.Value)
	require.Equal(t, e.creator.id.Value, created.Creator.Value)
	require.True(t, proto.Equal(definition, created.Definition))
	require.True(t, e.clock.Now().Equal(created.CreatedAt.AsTime()))
	requireOpen(t, created.State, nil)

	stored, err := polls.GetPoll(e.ctx, created.PollId)
	require.NoError(t, err)
	require.False(t, stored.Live)

	// Unsent, it is its creator's alone.
	got := e.getPoll(e.creator, created.PollId)
	require.Equal(t, pollpb.GetPollResponse_OK, got.Result)
	require.True(t, proto.Equal(created, got.Poll))
	require.Equal(t, pollpb.GetPollResponse_NOT_FOUND, e.getPoll(member, created.PollId).Result)
	require.Equal(t, pollpb.VoteResponse_NOT_FOUND, e.vote(member, created.PollId).Result)
	require.Equal(t, pollpb.VoteResponse_NOT_FOUND, e.vote(e.creator, created.PollId, 0).Result)

	// Sent, it is the chat's. (Sending is messaging's to do; the store is
	// what it calls.)
	_, err = polls.MarkLive(e.ctx, created.PollId, newClientMessageID(), e.clock.Now())
	require.NoError(t, err)

	got = e.getPoll(member, created.PollId)
	require.Equal(t, pollpb.GetPollResponse_OK, got.Result)
	requireOpen(t, got.Poll.State, nil)
	require.Equal(t, pollpb.VoteResponse_OK, e.vote(member, created.PollId, 1).Result)
}

func testServer_CreatePoll_Idempotent(t *testing.T, chats chat.Store, polls poll.Store) {
	e := newServerEnv(t, chats, polls)
	other := e.addUser()
	chatID := e.putPublicGroup(nil, other)
	otherChatID := e.putPublicGroup(nil)

	key := newIdempotencyKey()
	first := e.createPoll(e.creator, chatID, plaintextDefinition(e.clock.Now().Add(time.Hour), "Q", "a", "b"), key)
	require.Equal(t, pollpb.CreatePollResponse_OK, first.Result)
	require.Equal(t, 1, e.moderator.classifiedCount())

	// A retry is answered with the original poll whatever it carries, even a
	// definition that would be refused and another chat, and nothing is
	// checked again.
	e.clock.advance(time.Minute)
	retried := plaintextDefinition(e.clock.Now().Add(-time.Hour), "Other", "x", "y", "z")
	again := e.createPoll(e.creator, otherChatID, retried, key)
	require.Equal(t, pollpb.CreatePollResponse_OK, again.Result)
	require.True(t, proto.Equal(first.Poll, again.Poll))
	require.Equal(t, 1, e.moderator.classifiedCount())

	// The same key from someone else is someone else's poll.
	theirs := e.createPoll(other, chatID, plaintextDefinition(e.clock.Now().Add(time.Hour), "Q", "a", "b"), key)
	require.Equal(t, pollpb.CreatePollResponse_OK, theirs.Result)
	require.NotEqual(t, first.Poll.PollId.Value, theirs.Poll.PollId.Value)
	require.Equal(t, other.id.Value, theirs.Poll.Creator.Value)
}

func testServer_CreatePoll_Refused(t *testing.T, chats chat.Store, polls poll.Store) {
	e := newServerEnv(t, chats, polls)
	member := e.addUser()
	outsider := e.addUser()
	chatID := e.putPublicGroup(nil, member)

	// A creator-only group lets nobody but its creator speak, so nobody but
	// its creator creates a poll in it.
	creatorOnly := e.putPublicGroup(func(c *chat.Chat) { c.IsCreatorOnlySpeaker = true }, member)

	valid := func() *pollpb.PollDefinition {
		return plaintextDefinition(e.clock.Now().Add(time.Hour), "Q", "a", "b")
	}
	encrypted := valid()
	encrypted.Text = encryptedText()
	mismatched := valid()
	mismatched.OptionCount = 3
	tooSoon := plaintextDefinition(e.clock.Now().Add(poll.MinDuration-time.Second), "Q", "a", "b")
	tooLate := plaintextDefinition(e.clock.Now().Add(poll.MaxDuration+time.Second), "Q", "a", "b")

	dmID := &commonpb.ChatId{Value: bytes.Repeat([]byte{1}, chat.DmChatIDSize)}

	for _, tc := range []struct {
		name       string
		user       user
		chatID     *commonpb.ChatId
		definition *pollpb.PollDefinition
		want       pollpb.CreatePollResponse_Result
	}{
		{"dm", e.creator, dmID, valid(), pollpb.CreatePollResponse_CHAT_NOT_SUPPORTED},
		{"non-member", outsider, chatID, valid(), pollpb.CreatePollResponse_DENIED},
		{"missing chat", e.creator, chat.MustGenerateGroupChatID(), valid(), pollpb.CreatePollResponse_DENIED},
		{"cannot speak", member, creatorOnly, valid(), pollpb.CreatePollResponse_DENIED},
		{"encrypted in public group", member, chatID, encrypted, pollpb.CreatePollResponse_ENCRYPTION_NOT_ALLOWED},
		{"option count mismatch", member, chatID, mismatched, pollpb.CreatePollResponse_OPTION_COUNT_MISMATCH},
		{"ends too soon", member, chatID, tooSoon, pollpb.CreatePollResponse_INVALID_ENDS_AT},
		{"ends too late", member, chatID, tooLate, pollpb.CreatePollResponse_INVALID_ENDS_AT},
	} {
		key := newIdempotencyKey()
		resp := e.createPoll(tc.user, tc.chatID, tc.definition, key)
		require.Equal(t, tc.want, resp.Result, tc.name)
		require.Nil(t, resp.Poll, tc.name)

		_, err := polls.GetPoll(e.ctx, poll.MustDerivePollID(tc.user.id, key))
		require.ErrorIs(t, err, poll.ErrPollNotFound, tc.name)
	}
	require.Zero(t, e.moderator.classifiedCount())

	// The bounds themselves are allowed.
	require.Equal(t, pollpb.CreatePollResponse_OK, e.createPoll(member, chatID, plaintextDefinition(e.clock.Now().Add(poll.MinDuration), "Q", "a", "b"), newIdempotencyKey()).Result)
	require.Equal(t, pollpb.CreatePollResponse_OK, e.createPoll(member, chatID, plaintextDefinition(e.clock.Now().Add(poll.MaxDuration), "Q", "a", "b"), newIdempotencyKey()).Result)
	require.Equal(t, pollpb.CreatePollResponse_OK, e.createPoll(e.creator, creatorOnly, valid(), newIdempotencyKey()).Result)

}

func testServer_CreatePoll_BalanceParameters(t *testing.T, chats chat.Store, polls poll.Store) {
	e := newServerEnv(t, chats, polls)
	chatID := e.putPublicGroup(nil)

	coreMint := &commonpb.PublicKey{Value: ocp_common.CoreMintAccount.PublicKey().ToBytes()}
	launched := e.putCurrency(ocp_currency.MetadataStateAvailable)
	launching := e.putCurrency(ocp_currency.MetadataStateWaitingForInitialPurchase)
	unknown := &commonpb.PublicKey{Value: model.MustGenerateKeyPair().Proto().Value}

	balanceDefinition := func(parameters *pollpb.BalanceParameters) *pollpb.PollDefinition {
		d := plaintextDefinition(e.clock.Now().Add(time.Hour), "Q", "a", "b")
		d.Mode = pollpb.Mode_BALANCE
		d.BalanceParameters = parameters
		return d
	}
	countWithParameters := plaintextDefinition(e.clock.Now().Add(time.Hour), "Q", "a", "b")
	countWithParameters.BalanceParameters = &pollpb.BalanceParameters{Mint: coreMint, Currency: "usd"}

	for _, tc := range []struct {
		name       string
		definition *pollpb.PollDefinition
		want       pollpb.CreatePollResponse_Result
	}{
		{"count with parameters", countWithParameters, pollpb.CreatePollResponse_INVALID_BALANCE_PARAMETERS},
		{"balance without parameters", balanceDefinition(nil), pollpb.CreatePollResponse_INVALID_BALANCE_PARAMETERS},
		{"unknown mint", balanceDefinition(&pollpb.BalanceParameters{Mint: unknown, Currency: "usd"}), pollpb.CreatePollResponse_INVALID_BALANCE_PARAMETERS},
		{"currency not yet available", balanceDefinition(&pollpb.BalanceParameters{Mint: launching, Currency: "usd"}), pollpb.CreatePollResponse_INVALID_BALANCE_PARAMETERS},
		{"unsupported fiat currency", balanceDefinition(&pollpb.BalanceParameters{Mint: coreMint, Currency: "xyz"}), pollpb.CreatePollResponse_INVALID_BALANCE_PARAMETERS},
		{"core mint", balanceDefinition(&pollpb.BalanceParameters{Mint: coreMint, Currency: "usd"}), pollpb.CreatePollResponse_OK},
		{"launched currency", balanceDefinition(&pollpb.BalanceParameters{Mint: launched, Currency: "cad"}), pollpb.CreatePollResponse_OK},
	} {
		resp := e.createPoll(e.creator, chatID, tc.definition, newIdempotencyKey())
		require.Equal(t, tc.want, resp.Result, tc.name)
		if tc.want == pollpb.CreatePollResponse_OK {
			require.True(t, proto.Equal(tc.definition, resp.Poll.Definition), tc.name)
		}
	}
}

func testServer_CreatePoll_Moderation(t *testing.T, chats chat.Store, polls poll.Store) {
	e := newServerEnv(t, chats, polls)
	chatID := e.putPublicGroup(nil)

	// The question and options are classified as one text, a line each, so a
	// flag anywhere in it is the poll's.
	e.moderator.flag("Bad question\nfine\nfine too", "impersonation")
	e.moderator.flag("Good question\nfine\nbad option", "impersonation")
	e.moderator.fail("Unscorable\na\nb", moderation.ErrUnsupportedLanguage)

	for _, tc := range []struct {
		name     string
		question string
		options  []string
		want     pollpb.CreatePollResponse_Result
		category moderationpb.FlaggedCategory
	}{
		{"question flagged", "Bad question", []string{"fine", "fine too"}, pollpb.CreatePollResponse_TEXT_MODERATED, moderationpb.FlaggedCategory_IMPERSONATION},
		{"option flagged", "Good question", []string{"fine", "bad option"}, pollpb.CreatePollResponse_TEXT_MODERATED, moderationpb.FlaggedCategory_IMPERSONATION},
		{"clean", "Good question", []string{"fine", "fine too"}, pollpb.CreatePollResponse_OK, moderationpb.FlaggedCategory_NONE},
	} {
		key := newIdempotencyKey()
		resp := e.createPoll(e.creator, chatID, plaintextDefinition(e.clock.Now().Add(time.Hour), tc.question, tc.options...), key)
		require.Equal(t, tc.want, resp.Result, tc.name)
		require.Equal(t, tc.category, resp.FlaggedCategory, tc.name)

		_, err := polls.GetPoll(e.ctx, poll.MustDerivePollID(e.creator.id, key))
		if tc.want == pollpb.CreatePollResponse_OK {
			require.NoError(t, err, tc.name)
		} else {
			require.ErrorIs(t, err, poll.ErrPollNotFound, tc.name)
		}
	}
	require.Equal(t, []string{
		"Bad question\nfine\nfine too",
		"Good question\nfine\nbad option",
		"Good question\nfine\nfine too",
	}, e.moderator.classifiedTexts())

	// Text the classifier cannot score is refused rather than posted
	// unmoderated, as is any failure to classify.
	key := newIdempotencyKey()
	req := &pollpb.CreatePollRequest{
		ChatId:         chatID,
		Definition:     plaintextDefinition(e.clock.Now().Add(time.Hour), "Unscorable", "a", "b"),
		IdempotencyKey: key,
	}
	require.NoError(t, e.creator.keys.Auth(req, &req.Auth))
	_, err := e.client.CreatePoll(e.ctx, req)
	require.Equal(t, codes.Internal, status.Code(err))
	_, err = polls.GetPoll(e.ctx, poll.MustDerivePollID(e.creator.id, key))
	require.ErrorIs(t, err, poll.ErrPollNotFound)

	// Encrypted text is never moderated.
	private := e.putPrivateGroup(true)
	before := e.moderator.classifiedCount()
	definition := plaintextDefinition(e.clock.Now().Add(time.Hour), "Q", "a", "b")
	definition.Text = encryptedText()
	require.Equal(t, pollpb.CreatePollResponse_OK, e.createPoll(e.creator, private, definition, newIdempotencyKey()).Result)
	require.Equal(t, before, e.moderator.classifiedCount())
}

// testServer_CreatePoll_PrivateGroup pins a private group's poll: its text
// encrypted under the chat key, and live from creation.
func testServer_CreatePoll_PrivateGroup(t *testing.T, chats chat.Store, polls poll.Store) {
	e := newServerEnv(t, chats, polls)
	member := e.addUser()
	outsider := e.addUser()
	keyed := e.putPrivateGroup(true, member)
	keyless := e.putPrivateGroup(false, member)

	plaintext := plaintextDefinition(e.clock.Now().Add(time.Hour), "Q", "a", "b")
	require.Equal(t, pollpb.CreatePollResponse_ENCRYPTION_REQUIRED, e.createPoll(member, keyed, plaintext, newIdempotencyKey()).Result)

	encrypted := plaintextDefinition(e.clock.Now().Add(time.Hour), "Q", "a", "b")
	encrypted.Text = encryptedText()

	// A group without its key takes nothing from anyone.
	require.Equal(t, pollpb.CreatePollResponse_DENIED, e.createPoll(e.creator, keyless, encrypted, newIdempotencyKey()).Result)
	require.Equal(t, pollpb.CreatePollResponse_DENIED, e.createPoll(outsider, keyed, encrypted, newIdempotencyKey()).Result)

	resp := e.createPoll(member, keyed, encrypted, newIdempotencyKey())
	require.Equal(t, pollpb.CreatePollResponse_OK, resp.Result)
	require.True(t, proto.Equal(encrypted, resp.Poll.Definition))

	// The text is stored as given; the option count is the client's to
	// check against it once decrypted.
	stored, err := polls.GetPoll(e.ctx, resp.Poll.PollId)
	require.NoError(t, err)
	require.True(t, stored.Live)

	// Live from creation: the other members see it and vote at once, and a
	// non-member sees nothing.
	got := e.getPoll(e.creator, resp.Poll.PollId)
	require.Equal(t, pollpb.GetPollResponse_OK, got.Result)
	require.Equal(t, pollpb.VoteResponse_OK, e.vote(e.creator, resp.Poll.PollId, 0).Result)
	require.Equal(t, pollpb.GetPollResponse_DENIED, e.getPoll(outsider, resp.Poll.PollId).Result)
	require.Equal(t, pollpb.VoteResponse_DENIED, e.vote(outsider, resp.Poll.PollId, 0).Result)
}

// testServer_GetPoll_Visibility pins who sees a live poll: whoever may read
// its chat in full, which in a public group includes a non-member who
// satisfies its listener rules.
func testServer_GetPoll_Visibility(t *testing.T, chats chat.Store, polls poll.Store) {
	e := newServerEnv(t, chats, polls)
	member := e.addUser()
	outsider := e.addUser()

	const requirement = 10
	gated := e.putPublicGroup(func(c *chat.Chat) {
		c.MinimumListenerBalance = &chat.MinimumBalance{Currency: "usd", NativeAmount: requirement}
	}, member)
	e.fund(e.creator, requirement)
	id := e.createLivePoll(e.creator, gated)

	require.Equal(t, pollpb.GetPollResponse_OK, e.getPoll(member, id).Result)
	require.Equal(t, pollpb.GetPollResponse_DENIED, e.getPoll(outsider, id).Result)

	// Meeting the listener rules lets a non-member see it, but never vote.
	e.fund(outsider, requirement)
	got := e.getPoll(outsider, id)
	require.Equal(t, pollpb.GetPollResponse_OK, got.Result)
	requireOpen(t, got.Poll.State, nil)
	require.Equal(t, pollpb.VoteResponse_DENIED, e.vote(outsider, id, 0).Result)

	// A private group's poll is its members' alone.
	private := e.putPrivateGroup(true, member)
	definition := plaintextDefinition(e.clock.Now().Add(time.Hour), "Q", "a", "b")
	definition.Text = encryptedText()
	privatePoll := e.createPoll(e.creator, private, definition, newIdempotencyKey())
	require.Equal(t, pollpb.CreatePollResponse_OK, privatePoll.Result)
	require.Equal(t, pollpb.GetPollResponse_OK, e.getPoll(member, privatePoll.Poll.PollId).Result)
	require.Equal(t, pollpb.GetPollResponse_DENIED, e.getPoll(outsider, privatePoll.Poll.PollId).Result)

	require.Equal(t, pollpb.GetPollResponse_NOT_FOUND, e.getPoll(member, newPollID()).Result)
}

func testServer_GetPolls(t *testing.T, chats chat.Store, polls poll.Store) {
	e := newServerEnv(t, chats, polls)
	member := e.addUser()
	chatID := e.putPublicGroup(nil, member)
	members := e.putPublicGroup(func(c *chat.Chat) {
		c.MinimumListenerBalance = &chat.MinimumBalance{Currency: "usd", NativeAmount: 10}
	})

	live := e.createLivePoll(e.creator, chatID)
	voted := e.createLivePoll(e.creator, chatID)
	mine := e.createPoll(member, chatID, plaintextDefinition(e.clock.Now().Add(time.Hour), "Q", "a", "b"), newIdempotencyKey()).Poll.PollId
	theirs := e.createPoll(e.creator, chatID, plaintextDefinition(e.clock.Now().Add(time.Hour), "Q", "a", "b"), newIdempotencyKey()).Poll.PollId
	e.fund(e.creator, 10)
	elsewhere := e.createLivePoll(e.creator, members)

	require.Equal(t, pollpb.VoteResponse_OK, e.vote(member, voted, 1).Result)

	// Shown: live polls in a chat the caller reads, and their own unsent one.
	// Left out: someone else's unsent poll, a poll in a chat they cannot
	// read, a poll that doesn't exist, and a repeat.
	resp := e.getPolls(member, live, voted, mine, theirs, elsewhere, newPollID(), live)
	require.Equal(t, pollpb.GetPollsResponse_OK, resp.Result)
	byID := make(map[string]*pollpb.PollMetadata)
	for _, p := range resp.Polls {
		byID[string(p.PollId.Value)] = p
	}
	require.Len(t, byID, 3)
	require.Len(t, resp.Polls, 3)
	requireOpen(t, byID[string(live.Value)].State, nil)
	requireOpen(t, byID[string(voted.Value)].State, []uint32{1})
	requireOpen(t, byID[string(mine.Value)].State, nil)

	// Each poll is shown as GetPoll shows it.
	for _, p := range resp.Polls {
		got := e.getPoll(member, p.PollId)
		require.Equal(t, pollpb.GetPollResponse_OK, got.Result)
		require.True(t, proto.Equal(got.Poll, p))
	}
}

func testServer_Vote(t *testing.T, chats chat.Store, polls poll.Store) {
	e := newServerEnv(t, chats, polls)
	member := e.addUser()
	chatID := e.putPublicGroup(nil, member)

	single := e.createLivePoll(e.creator, chatID)
	multiple := e.createLivePollWith(e.creator, chatID, func(d *pollpb.PollDefinition) {
		d.AllowsMultipleChoices = true
		d.OptionCount = 3
		d.GetPlaintext().Options = append(d.GetPlaintext().Options, &pollpb.Option{Text: "c"})
	})

	// A single-choice poll takes one option, in range.
	for _, options := range [][]uint32{{0, 1}, {2}} {
		require.Equal(t, pollpb.VoteResponse_INVALID_SELECTION, e.vote(member, single, options...).Result)
	}
	resp := e.vote(member, single, 1)
	require.Equal(t, pollpb.VoteResponse_OK, resp.Result)
	requireOpen(t, resp.State, []uint32{1})
	requireOpen(t, e.getPoll(member, single).Poll.State, []uint32{1})

	// A vote is replaced whole, the same vote again changes nothing, and an
	// empty one removes it.
	requireOpen(t, e.vote(member, single, 0).State, []uint32{0})
	requireOpen(t, e.vote(member, single, 0).State, []uint32{0})
	requireOpen(t, e.getPoll(member, single).Poll.State, []uint32{0})
	requireOpen(t, e.vote(member, single).State, nil)
	requireOpen(t, e.getPoll(member, single).Poll.State, nil)

	// A multiple-choice poll takes several, held in option order.
	require.Equal(t, pollpb.VoteResponse_INVALID_SELECTION, e.vote(member, multiple, 0, 3).Result)
	requireOpen(t, e.vote(member, multiple, 2, 0).State, []uint32{0, 2})
	requireOpen(t, e.getPoll(member, multiple).Poll.State, []uint32{0, 2})

	// Each voter sees their own vote and no one else's.
	requireOpen(t, e.getPoll(e.creator, multiple).Poll.State, nil)

	require.Equal(t, pollpb.VoteResponse_NOT_FOUND, e.vote(member, newPollID(), 0).Result)

	// The write's bound runs on the real clock, not the server's: a server
	// clock well behind real time still lands the vote.
	e.clock.advance(-time.Hour)
	requireOpen(t, e.vote(member, single, 1).State, []uint32{1})
}

// testServer_Vote_MemberOnly pins that voting is a member's, whatever the
// chat's speaker rules.
func testServer_Vote_MemberOnly(t *testing.T, chats chat.Store, polls poll.Store) {
	e := newServerEnv(t, chats, polls)
	member := e.addUser()
	outsider := e.addUser()

	creatorOnly := e.putPublicGroup(func(c *chat.Chat) { c.IsCreatorOnlySpeaker = true }, member)
	id := e.createLivePoll(e.creator, creatorOnly)
	require.Equal(t, pollpb.VoteResponse_OK, e.vote(member, id, 0).Result)

	// A member whose balance has fallen below the speaker minimum still votes.
	const listener, speaker = 10, 20
	balanced := e.putPublicGroup(func(c *chat.Chat) {
		c.MinimumListenerBalance = &chat.MinimumBalance{Currency: "usd", NativeAmount: listener}
		c.MinimumSpeakerBalance = &chat.MinimumBalance{Currency: "usd", NativeAmount: speaker}
	}, member)
	e.fund(e.creator, speaker)
	balancedPoll := e.createLivePoll(e.creator, balanced)
	require.Equal(t, pollpb.VoteResponse_OK, e.vote(member, balancedPoll, 1).Result)

	// A non-member never votes, even one who reads the chat.
	e.fund(outsider, speaker)
	require.Equal(t, pollpb.GetPollResponse_OK, e.getPoll(outsider, balancedPoll).Result)
	require.Equal(t, pollpb.VoteResponse_DENIED, e.vote(outsider, balancedPoll, 1).Result)

	// Nor does a member who has left.
	_, _, err := chats.RemoveGroupMember(e.ctx, creatorOnly, member.id, false)
	require.NoError(t, err)
	require.Equal(t, pollpb.VoteResponse_DENIED, e.vote(member, id, 1).Result)
}

// testServer_Vote_Closed pins a poll past its end: it takes no votes and is
// shown tallying until its results are recorded, then closed with them.
func testServer_Vote_Closed(t *testing.T, chats chat.Store, polls poll.Store) {
	e := newServerEnv(t, chats, polls)
	member := e.addUser()
	chatID := e.putPublicGroup(nil, member)

	id := e.createLivePoll(e.creator, chatID)
	require.Equal(t, pollpb.VoteResponse_OK, e.vote(member, id, 1).Result)
	stored, err := polls.GetPoll(e.ctx, id)
	require.NoError(t, err)
	endsAt := stored.EndsAt()

	// The end itself is closed.
	e.clock.set(endsAt)
	for _, options := range [][]uint32{{0}, nil, {7}} {
		resp := e.vote(member, id, options...)
		require.Equal(t, pollpb.VoteResponse_POLL_CLOSED, resp.Result)
		requireStatus(t, resp.State, pollpb.PollState_TALLYING, endsAt, []uint32{1})
		require.Nil(t, resp.State.Results)
	}
	requireStatus(t, e.getPoll(member, id).Poll.State, pollpb.PollState_TALLYING, endsAt, []uint32{1})

	results := &pollpb.Results{Type: &pollpb.Results_Withheld{Withheld: &pollpb.WithheldResults{}}}
	_, err = polls.Close(e.ctx, id, results)
	require.NoError(t, err)

	state := e.getPoll(member, id).Poll.State
	requireStatus(t, state, pollpb.PollState_CLOSED, endsAt, []uint32{1})
	require.True(t, proto.Equal(results, state.Results))

	resp := e.vote(member, id, 0)
	require.Equal(t, pollpb.VoteResponse_POLL_CLOSED, resp.Result)
	requireStatus(t, resp.State, pollpb.PollState_CLOSED, endsAt, []uint32{1})
	require.True(t, proto.Equal(results, resp.State.Results))
}

// testServer_BalanceResults_Ranked pins that a closed BALANCE poll's recorded
// totals are never shown: every reader gets the options' ranking alone.
func testServer_BalanceResults_Ranked(t *testing.T, chats chat.Store, polls poll.Store) {
	e := newServerEnv(t, chats, polls)
	member := e.addUser()
	chatID := e.putPublicGroup(nil, member)

	coreMint := &commonpb.PublicKey{Value: ocp_common.CoreMintAccount.PublicKey().ToBytes()}
	id := e.createLivePollWith(e.creator, chatID, func(d *pollpb.PollDefinition) {
		d.Mode = pollpb.Mode_BALANCE
		d.BalanceParameters = &pollpb.BalanceParameters{Mint: coreMint, Currency: "usd"}
		d.OptionCount = 4
		d.GetPlaintext().Options = append(d.GetPlaintext().Options, &pollpb.Option{Text: "c"}, &pollpb.Option{Text: "d"})
	})
	require.Equal(t, pollpb.VoteResponse_OK, e.vote(member, id, 1).Result)

	amount := func(whole uint64, native float64) *commonpb.CryptoPaymentAmount {
		return &commonpb.CryptoPaymentAmount{Currency: "usd", NativeAmount: native, Quarks: ocp_common.ToCoreMintQuarks(whole), Mint: coreMint}
	}
	// The ranking is by quarks, not value: options 0 and 2 hold the same
	// quarks behind values a float sum left a bit apart, and share a rank.
	recorded := &pollpb.Results{Type: &pollpb.Results_Balance{Balance: &pollpb.BalanceResults{
		Total:          amount(90, 90),
		OptionBalances: []*commonpb.CryptoPaymentAmount{amount(20, 0.1+0.2), amount(50, 50), amount(20, 0.3), amount(0, 0)},
	}}}
	stored, err := polls.GetPoll(e.ctx, id)
	require.NoError(t, err)
	e.clock.set(stored.EndsAt())
	_, err = polls.Close(e.ctx, id, recorded)
	require.NoError(t, err)

	ranked := &pollpb.Results{Type: &pollpb.Results_Ranked{Ranked: &pollpb.RankedResults{
		OptionRanks: []uint32{2, 1, 2, 4},
	}}}
	for _, state := range []*pollpb.PollState{
		e.getPoll(member, id).Poll.State,
		e.getPoll(e.creator, id).Poll.State,
		e.getPolls(member, id).Polls[0].State,
		e.vote(member, id, 0).State,
	} {
		require.Equal(t, pollpb.PollState_CLOSED, state.Status)
		require.True(t, proto.Equal(ranked, state.Results), "got %v", state.Results)
		require.NoError(t, state.Results.Validate())
	}

	// The totals stay recorded.
	stored, err = polls.GetPoll(e.ctx, id)
	require.NoError(t, err)
	require.True(t, proto.Equal(recorded, stored.Results))
}

type user struct {
	id   *commonpb.UserId
	keys model.KeyPair
}

type serverEnv struct {
	t   *testing.T
	ctx context.Context

	client pollpb.PollClient
	authz  *auth.StaticAuthorizer

	chats      chat.Store
	polls      poll.Store
	accounts   account.Store
	ocpBalance *fakeOcpBalance
	moderator  *fakeModerator
	ocpData    ocp_data.Provider
	clock      *fakeClock

	// creator is the user who creates every group the env puts.
	creator user
}

func newServerEnv(t *testing.T, chats chat.Store, polls poll.Store) *serverEnv {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := auth.NewStaticAuthorizer(log)
	accounts := accountmemory.NewInMemory()
	ocpBalance := &fakeOcpBalance{byOwner: make(map[string]fakeHolding)}
	balances := balance.NewClient(log, accounts, ocpBalance)

	// Nothing is remembered between asks, so a test's changes to balances and
	// rosters show on the next call.
	access := chat.NewAccess(
		chats,
		chat.NewRuleEvaluator(accounts, balances, chats, nil),
		chat.WithListenerAdmissionTTL(0),
		chat.WithSpeakerAdmissionTTL(0),
	)

	moderator := newFakeModerator()
	ocpData := ocp_data.NewTestDataProvider()
	clock := &fakeClock{now: time.Now().UTC().Truncate(time.Millisecond)}

	server := poll.NewServer(log, authz, polls, access, moderator, ocpData, poll.WithClock(clock.Now))
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		pollpb.RegisterPollServer(s, server)
	}))

	e := &serverEnv{
		t:          t,
		ctx:        ctx,
		client:     pollpb.NewPollClient(cc),
		authz:      authz,
		chats:      chats,
		polls:      polls,
		accounts:   accounts,
		ocpBalance: ocpBalance,
		moderator:  moderator,
		ocpData:    ocpData,
		clock:      clock,
	}
	e.creator = e.addUser()
	return e
}

func (e *serverEnv) addUser() user {
	u := user{id: model.MustGenerateUserID(), keys: model.MustGenerateKeyPair()}
	e.authz.Add(u.id, u.keys)
	return u
}

// fund binds u's key, if it isn't yet, and gives them amount USD.
func (e *serverEnv) fund(u user, amount uint64) {
	bound, err := e.accounts.GetPubKeys(e.ctx, u.id)
	require.NoError(e.t, err)
	if len(bound) == 0 {
		_, err = e.accounts.Bind(e.ctx, u.id, u.keys.Proto())
		require.NoError(e.t, err)
	}
	e.ocpBalance.setBalance(u.keys.Proto(), ocp_common.ToCoreMintQuarks(amount))
}

// putCurrency stores a launched currency in OCP's data, in state, and returns
// its mint.
func (e *serverEnv) putCurrency(state ocp_currency.MetadataState) *commonpb.PublicKey {
	mint := model.MustGenerateKeyPair().Proto()
	address := func() string { return base58.Encode(model.MustGenerateKeyPair().Proto().Value) }
	// Names and symbols are unique across currencies.
	unique := base58.Encode(mint.Value)[:8]
	require.NoError(e.t, e.ocpData.SaveCurrencyMetadata(e.ctx, &ocp_currency.MetadataRecord{
		Name:               "Test Coin " + unique,
		Symbol:             unique,
		Description:        "A test currency",
		ImageUrl:           "https://example.com/test.png",
		Seed:               address(),
		Authority:          address(),
		Mint:               base58.Encode(mint.Value),
		MintBump:           1,
		Decimals:           currencycreator.DefaultMintDecimals,
		CurrencyConfig:     address(),
		CurrencyConfigBump: 1,
		LiquidityPool:      address(),
		LiquidityPoolBump:  1,
		VaultMint:          address(),
		VaultMintBump:      1,
		VaultCore:          address(),
		VaultCoreBump:      1,
		SellFeeBps:         currencycreator.DefaultSellFeeBps,
		Alt:                address(),
		State:              state,
		CreatedBy:          address(),
		CreatedAt:          time.Now(),
	}))
	return mint
}

// putPublicGroup stores a public group created by the env's creator, with
// members joined after them, and configure applied to its record first.
func (e *serverEnv) putPublicGroup(configure func(*chat.Chat), members ...user) *commonpb.ChatId {
	c := &chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{e.creator.id},
		Title:        "Group",
		CreatorID:    e.creator.id,
		LastActivity: time.Now(),
	}
	if configure != nil {
		configure(c)
	}
	return e.putGroup(c, members)
}

// putPrivateGroup stores a private group created by the env's creator, with
// members joined after them, and its key when keyed.
func (e *serverEnv) putPrivateGroup(keyed bool, members ...user) *commonpb.ChatId {
	chatID := e.putGroup(&chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{e.creator.id},
		Title:        "Private",
		IsPrivate:    true,
		CreatorID:    e.creator.id,
		LastActivity: time.Now(),
	}, members)
	if keyed {
		envelope := &chatpb.KeyEnvelope{
			Scheme:     chatpb.KeyEnvelope_X25519_XCHACHA20POLY1305,
			Nonce:      bytes.Repeat([]byte{1}, 24),
			Ciphertext: bytes.Repeat([]byte{1}, 48),
		}
		_, err := e.chats.SetKeyEnvelope(e.ctx, chatID, e.creator.id, chat.KeyEnvelopeFromProto(envelope, e.creator.id))
		require.NoError(e.t, err)
	}
	return chatID
}

func (e *serverEnv) putGroup(c *chat.Chat, members []user) *commonpb.ChatId {
	require.NoError(e.t, e.chats.PutChat(e.ctx, c))
	if len(members) > 0 {
		ids := make([]*commonpb.UserId, len(members))
		for i, m := range members {
			ids[i] = m.id
		}
		_, _, err := e.chats.AddGroupMembers(e.ctx, c.ID, ids)
		require.NoError(e.t, err)
	}
	return c.ID
}

// createLivePoll creates a COUNT poll in a public group and makes it live, as
// its first send would.
func (e *serverEnv) createLivePoll(u user, chatID *commonpb.ChatId) *pollpb.PollId {
	return e.createLivePollWith(u, chatID, nil)
}

func (e *serverEnv) createLivePollWith(u user, chatID *commonpb.ChatId, configure func(*pollpb.PollDefinition)) *pollpb.PollId {
	definition := plaintextDefinition(e.clock.Now().Add(time.Hour), "Q", "a", "b")
	if configure != nil {
		configure(definition)
	}
	resp := e.createPoll(u, chatID, definition, newIdempotencyKey())
	require.Equal(e.t, pollpb.CreatePollResponse_OK, resp.Result)
	_, err := e.polls.MarkLive(e.ctx, resp.Poll.PollId, newClientMessageID(), e.clock.Now())
	require.NoError(e.t, err)
	return resp.Poll.PollId
}

func (e *serverEnv) createPoll(u user, chatID *commonpb.ChatId, definition *pollpb.PollDefinition, key *pollpb.IdempotencyKey) *pollpb.CreatePollResponse {
	req := &pollpb.CreatePollRequest{
		ChatId:         chatID,
		Definition:     definition,
		IdempotencyKey: key,
	}
	require.NoError(e.t, u.keys.Auth(req, &req.Auth))
	resp, err := e.client.CreatePoll(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) getPoll(u user, pollID *pollpb.PollId) *pollpb.GetPollResponse {
	req := &pollpb.GetPollRequest{PollId: pollID}
	require.NoError(e.t, u.keys.Auth(req, &req.Auth))
	resp, err := e.client.GetPoll(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) getPolls(u user, pollIDs ...*pollpb.PollId) *pollpb.GetPollsResponse {
	req := &pollpb.GetPollsRequest{PollIds: pollIDs}
	require.NoError(e.t, u.keys.Auth(req, &req.Auth))
	resp, err := e.client.GetPolls(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) vote(u user, pollID *pollpb.PollId, options ...uint32) *pollpb.VoteResponse {
	req := &pollpb.VoteRequest{PollId: pollID, OptionIndices: options}
	require.NoError(e.t, u.keys.Auth(req, &req.Auth))
	resp, err := e.client.Vote(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

// plaintextDefinition is a single-choice COUNT poll asking question with the
// given options, ending at endsAt.
func plaintextDefinition(endsAt time.Time, question string, options ...string) *pollpb.PollDefinition {
	text := &pollpb.PollText{Question: question}
	for _, option := range options {
		text.Options = append(text.Options, &pollpb.Option{Text: option})
	}
	return &pollpb.PollDefinition{
		OptionCount: uint32(len(options)),
		Mode:        pollpb.Mode_COUNT,
		EndsAt:      timestamppb.New(endsAt),
		Text:        &pollpb.PollDefinition_Plaintext{Plaintext: text},
	}
}

// encryptedText is poll text encrypted under the chat key, as far as the
// server can tell.
func encryptedText() *pollpb.PollDefinition_Encrypted {
	return &pollpb.PollDefinition_Encrypted{Encrypted: &pollpb.EncryptedPollText{
		Scheme:     pollpb.EncryptedPollText_CHAT_KEY_XCHACHA20POLY1305,
		Nonce:      bytes.Repeat([]byte{2}, 24),
		Ciphertext: bytes.Repeat([]byte{3}, 64),
	}}
}

func newIdempotencyKey() *pollpb.IdempotencyKey {
	return &pollpb.IdempotencyKey{Value: model.MustGenerateUserID().Value}
}

func requireOpen(t *testing.T, state *pollpb.PollState, selfVote []uint32) {
	t.Helper()
	require.Equal(t, pollpb.PollState_OPEN, state.Status)
	require.Nil(t, state.ClosedAt)
	require.Nil(t, state.Results)
	requireSelfVote(t, state, selfVote)
}

func requireStatus(t *testing.T, state *pollpb.PollState, want pollpb.PollState_Status, closedAt time.Time, selfVote []uint32) {
	t.Helper()
	require.Equal(t, want, state.Status)
	require.NotNil(t, state.ClosedAt)
	require.True(t, closedAt.Equal(state.ClosedAt.AsTime()))
	requireSelfVote(t, state, selfVote)
}

func requireSelfVote(t *testing.T, state *pollpb.PollState, want []uint32) {
	t.Helper()
	if len(want) == 0 {
		require.Empty(t, state.SelfVote)
		return
	}
	require.Equal(t, want, state.SelfVote)
}

// fakeClock is the server's clock, which a test moves.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *fakeClock) set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

// fakeModerator is a moderation.Client that classifies every text as clean,
// except the texts a test flags or fails, and records every text it was asked
// about, in order. The poll server classifies text alone: every other
// classification is clean.
type fakeModerator struct {
	mu         sync.Mutex
	flagged    map[string][]string
	failures   map[string]error
	classified []string
}

func newFakeModerator() *fakeModerator {
	return &fakeModerator{
		flagged:  make(map[string][]string),
		failures: make(map[string]error),
	}
}

func (m *fakeModerator) flag(text string, categories ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flagged[text] = categories
}

func (m *fakeModerator) fail(text string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failures[text] = err
}

// classifiedCount is how many CreatePoll requests reached moderation: each
// classifies its text once.
func (m *fakeModerator) classifiedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.classified)
}

func (m *fakeModerator) classifiedTexts() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.classified...)
}

func (m *fakeModerator) ClassifyImage(context.Context, []byte) (*moderation.Result, error) {
	return &moderation.Result{}, nil
}

func (m *fakeModerator) ClassifyCurrencyName(context.Context, string) (*moderation.Result, error) {
	return &moderation.Result{}, nil
}

func (m *fakeModerator) ClassifyUsername(context.Context, string) (*moderation.Result, error) {
	return &moderation.Result{}, nil
}

func (m *fakeModerator) ClassifyDisplayName(context.Context, string) (*moderation.Result, error) {
	return &moderation.Result{}, nil
}

func (m *fakeModerator) ClassifyGroupTitle(context.Context, string) (*moderation.Result, error) {
	return &moderation.Result{}, nil
}

func (m *fakeModerator) ClassifyText(_ context.Context, text string) (*moderation.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.classified = append(m.classified, text)
	if err, ok := m.failures[text]; ok {
		return nil, err
	}
	if categories, ok := m.flagged[text]; ok {
		scores := make(map[string]float64, len(categories))
		for i, category := range categories {
			scores[category] = float64(i + 1)
		}
		return &moderation.Result{Flagged: true, FlaggedCategories: categories, CategoryScores: scores}, nil
	}
	return &moderation.Result{}, nil
}

// fakeOcpBalance is the OCP balance service behind the env's balance client,
// answering each owner's total from a ledger a test sets, one holding per
// owner: its value in USDF quarks, valued in a fiat currency at the rate a test
// sets for it (none, and the currency is one OCP cannot price), and its amount
// in the mint's own quarks. It ignores the request's mint filter, and reports
// the holding as one in each mint requested. A test may fail every call with
// err.
type fakeOcpBalance struct {
	mu      sync.Mutex
	byOwner map[string]fakeHolding
	rates   map[string]float64
	err     error
}

type fakeHolding struct {
	quarks        uint64
	coreMintValue uint64
}

// setBalance gives owner coreMintValue USDF quarks, held as that many quarks.
func (f *fakeOcpBalance) setBalance(owner *commonpb.PublicKey, coreMintValue uint64) {
	f.setHolding(owner, coreMintValue, coreMintValue)
}

// setHolding gives owner quarks of a mint worth coreMintValue USDF quarks.
func (f *fakeOcpBalance) setHolding(owner *commonpb.PublicKey, quarks, coreMintValue uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byOwner[base58.Encode(owner.Value)] = fakeHolding{quarks: quarks, coreMintValue: coreMintValue}
}

func (f *fakeOcpBalance) setRate(currency string, rate float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rates == nil {
		f.rates = make(map[string]float64)
	}
	f.rates[currency] = rate
}

func (f *fakeOcpBalance) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeOcpBalance) GetBalances(_ context.Context, req *ocp_balancepb.GetBalancesRequest, _ ...grpc.CallOption) (*ocp_balancepb.GetBalancesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	resp := &ocp_balancepb.GetBalancesResponse{BalancesByOwner: make(map[string]*ocp_balancepb.OwnerBalance)}
	for _, owner := range req.Owners {
		key := base58.Encode(owner.Value)
		holding, ok := f.byOwner[key]
		if !ok {
			continue
		}
		fiatValues := make(map[string]float64)
		for _, code := range req.CurrencyCodes {
			if rate, ok := f.rates[code]; ok {
				fiatValues[code] = float64(holding.coreMintValue) / float64(ocp_common.CoreMintQuarksPerUnit) * rate
			}
		}
		ownerBalance := &ocp_balancepb.OwnerBalance{
			Owner:                owner,
			CoreMintValue:        holding.coreMintValue,
			FiatValuesByCurrency: fiatValues,
		}
		if !req.ExcludePerMintBalances && holding.quarks > 0 {
			ownerBalance.BalancesByMint = make(map[string]*ocp_balancepb.MintBalance, len(req.Mints))
			for _, mint := range req.Mints {
				ownerBalance.BalancesByMint[base58.Encode(mint.Value)] = &ocp_balancepb.MintBalance{
					Mint:                 mint,
					CoreMintValue:        holding.coreMintValue,
					FiatValuesByCurrency: fiatValues,
					Quarks:               holding.quarks,
				}
			}
		}
		resp.BalancesByOwner[key] = ownerBalance
	}
	return resp, nil
}
