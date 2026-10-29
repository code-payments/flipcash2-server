package community_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	communitypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/community/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	ocp_currency "github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/solana/currencycreator"

	"github.com/code-payments/flipcash2-server/account"
	accountmemory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/balance"
	"github.com/code-payments/flipcash2-server/chat"
	chatmemory "github.com/code-payments/flipcash2-server/chat/memory"
	"github.com/code-payments/flipcash2-server/community"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/testutil"
)

type serverEnv struct {
	t        *testing.T
	ctx      context.Context
	client   communitypb.CommunityClient
	authz    *auth.StaticAuthorizer
	accounts account.Store
	chats    chat.Store
	ocpData  ocp_data.Provider
	profiles *fakeProfiles
}

func newServerEnv(t *testing.T) *serverEnv {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := auth.NewStaticAuthorizer(log)
	accounts := accountmemory.NewInMemory()
	chats := chatmemory.NewInMemory(nil)
	ocpData := ocp_data.NewTestDataProvider()

	// The hydrator is what projects the groups. A public list reads nothing
	// about a viewer and no messaging state, so of its readers it asks only
	// the profiles (about no one), and the groups here have no pictures; the
	// readers it must not ask fail the test if it does. The profiles are the
	// server's too, for the leader.
	rules := chat.NewRuleEvaluator(accounts, balance.NewClient(log, accounts, nil), chats, nil)
	unexpected := unexpectedReaders{t: t}
	profiles := &fakeProfiles{byUser: make(map[string]*profilepb.UserProfile)}
	hydrator := chat.NewHydrator(log, unexpected, chats, unexpected, unexpected, profiles, rules, nil)

	server := community.NewServer(log, authz, accounts, chats, hydrator, ocpData, profiles)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		communitypb.RegisterCommunityServer(s, server)
	}))

	return &serverEnv{
		t:        t,
		ctx:      ctx,
		client:   communitypb.NewCommunityClient(cc),
		authz:    authz,
		accounts: accounts,
		chats:    chats,
		ocpData:  ocpData,
		profiles: profiles,
	}
}

// fakeProfiles is a chat.ProfileReader answering row profiles from the
// profiles a test sets. A public list hydrates no members, so only the
// leader's lookup asks it about anyone.
type fakeProfiles struct {
	mu     sync.Mutex
	byUser map[string]*profilepb.UserProfile
}

func (f *fakeProfiles) set(userID *commonpb.UserId, profile *profilepb.UserProfile) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byUser[string(userID.Value)] = profile
}

func (f *fakeProfiles) GetPhoneNumbers(context.Context, []*commonpb.UserId) (map[string]*commonpb.PhoneNumber, error) {
	return map[string]*commonpb.PhoneNumber{}, nil
}

func (f *fakeProfiles) GetLimitedPublicProfilesForRow(_ context.Context, userIDs []*commonpb.UserId) (map[string]*profilepb.UserProfile, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]*profilepb.UserProfile)
	for _, userID := range userIDs {
		if profile, ok := f.byUser[string(userID.Value)]; ok {
			out[string(userID.Value)] = proto.Clone(profile).(*profilepb.UserProfile)
		}
	}
	return out, nil
}

func (f *fakeProfiles) GetUserIDByUsername(context.Context, string) (*commonpb.UserId, bool, error) {
	return nil, false, nil
}

// unexpectedReaders stands in for the chat readers a public list never
// asks (see newServerEnv): the blocklist, messaging and media. Each fails the
// test if it is asked.
type unexpectedReaders struct {
	t *testing.T
}

func (u unexpectedReaders) fail(method string) error {
	u.t.Errorf("unexpected call to %s", method)
	return errors.New("unexpected call to " + method)
}

func (u unexpectedReaders) GetBlocked(context.Context, *commonpb.UserId, []*commonpb.UserId) (map[string]bool, error) {
	return nil, u.fail("GetBlocked")
}

func (u unexpectedReaders) LastMessages(context.Context, []chat.MessageRef) (map[string]*messagingpb.Message, error) {
	return nil, u.fail("LastMessages")
}

func (u unexpectedReaders) Messages(context.Context, []chat.MessageRef, chat.Reading) (map[chat.MessageKey]*messagingpb.Message, error) {
	return nil, u.fail("Messages")
}

func (u unexpectedReaders) Pointers(context.Context, []chat.PointerRef) (map[string][]*messagingpb.Pointer, error) {
	return nil, u.fail("Pointers")
}

func (u unexpectedReaders) LatestEventSequences(context.Context, []*commonpb.ChatId) (map[string]uint64, error) {
	return nil, u.fail("LatestEventSequences")
}

func (u unexpectedReaders) ResolveRenditions(context.Context, []*blobpb.BlobId) (map[string][]*blobpb.Rendition, error) {
	return nil, u.fail("ResolveRenditions")
}

func (u unexpectedReaders) SetAsChatMedia(context.Context, *commonpb.UserId, *commonpb.ChatId, *blobpb.BlobId) error {
	return u.fail("SetAsChatMedia")
}

// putCurrency stores a launched currency named name in OCP's data, in
// state, launched by an account no one holds, and returns its mint.
func (e *serverEnv) putCurrency(name string, state ocp_currency.MetadataState) *commonpb.PublicKey {
	return e.putCurrencyLaunchedBy(name, state, model.MustGenerateKeyPair().Proto())
}

// putCurrencyLaunchedBy is putCurrency launched by the owner account
// launcher.
func (e *serverEnv) putCurrencyLaunchedBy(name string, state ocp_currency.MetadataState, launcher *commonpb.PublicKey) *commonpb.PublicKey {
	mint := model.MustGenerateKeyPair().Proto()
	address := func() string { return base58.Encode(model.MustGenerateKeyPair().Proto().Value) }
	unique := base58.Encode(mint.Value)[:8]
	require.NoError(e.t, e.ocpData.SaveCurrencyMetadata(e.ctx, &ocp_currency.MetadataRecord{
		Name:               name,
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
		CreatedBy:          base58.Encode(launcher.Value),
		CreatedAt:          time.Now(),
	}))
	return mint
}

// putGatedGroup stores a group gated on mint, private or not, and its
// holdings snapshot of quarks.
func (e *serverEnv) putGatedGroup(title string, mint *commonpb.PublicKey, isPrivate bool, quarks uint64) chat.GroupHoldings {
	creator := model.MustGenerateUserID()
	c := &chat.Chat{
		ID:        chat.MustGenerateGroupChatID(),
		Type:      chatpb.ChatType_GROUP,
		Members:   []*commonpb.UserId{creator},
		Title:     title,
		IsPrivate: isPrivate,
		CreatorID: creator,
		MinimumListenerBalance: &chat.MinimumBalance{
			Currency:     "usd",
			NativeAmount: 1,
			Mints:        []*commonpb.PublicKey{mint},
		},
		LastActivity: time.Now(),
	}
	require.NoError(e.t, e.chats.PutChat(e.ctx, c))
	return e.putHoldings(c.ID, mint, quarks)
}

// putHoldings stores chatID's holdings snapshot of quarks of mint, held by
// the fewest members a snapshot may have.
func (e *serverEnv) putHoldings(chatID *commonpb.ChatId, mint *commonpb.PublicKey, quarks uint64) chat.GroupHoldings {
	h := chat.GroupHoldings{
		ChatID:      chatID,
		Mint:        mint,
		Quarks:      quarks,
		HolderCount: chat.MinGroupHoldingsHolders,
		AsOf:        time.UnixMilli(time.Now().UnixMilli()),
	}
	_, err := e.chats.PutGroupHoldings(e.ctx, h)
	require.NoError(e.t, err)
	return h
}

func (e *serverEnv) getCommunity(mint *commonpb.PublicKey, keys ...model.KeyPair) *communitypb.GetCommunityResponse {
	req := &communitypb.GetCommunityRequest{
		Identifier: &communitypb.GetCommunityRequest_Mint{Mint: mint},
	}
	if len(keys) > 0 {
		require.NoError(e.t, keys[0].Auth(req, &req.Auth))
	}
	resp, err := e.client.GetCommunity(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func TestGetCommunity_NotFound(t *testing.T) {
	e := newServerEnv(t)

	unknown := model.MustGenerateKeyPair().Proto()
	core := &commonpb.PublicKey{Value: ocp_common.CoreMintAccount.PublicKey().ToBytes()}
	abandoned := e.putCurrency("Abandoned Coin", ocp_currency.MetadataStateAbandoned)

	for _, mint := range []*commonpb.PublicKey{unknown, core, abandoned} {
		resp := e.getCommunity(mint)
		require.Equal(t, communitypb.GetCommunityResponse_NOT_FOUND, resp.Result)
		require.Nil(t, resp.Community)
	}
}

func TestGetCommunity_NoGroups(t *testing.T) {
	e := newServerEnv(t)
	mint := e.putCurrency("Quiet Coin", ocp_currency.MetadataStateAvailable)

	resp := e.getCommunity(mint)
	require.Equal(t, communitypb.GetCommunityResponse_OK, resp.Result)
	require.Equal(t, mint.Value, resp.Community.Mint.Value)
	require.Equal(t, "Quiet Coin", resp.Community.Name)
	require.Empty(t, resp.Community.TopHolderChats)
	require.Nil(t, resp.Community.Leader)
}

func TestGetCommunity_TopHolderChats(t *testing.T) {
	e := newServerEnv(t)
	mint := e.putCurrency("Busy Coin", ocp_currency.MetadataStateAvailable)
	other := e.putCurrency("Other Coin", ocp_currency.MetadataStateAvailable)

	middle := e.putGatedGroup("Middle", mint, false, 200)
	top := e.putGatedGroup("Top", mint, false, 900)
	bottom := e.putGatedGroup("Bottom", mint, false, 10)

	// Left out: a group of another currency, a private group (which no
	// refresh should rank, and which is never shown if one is), and a
	// snapshot of a group with no record.
	e.putGatedGroup("Other", other, false, 5_000)
	e.putGatedGroup("Private", mint, true, 1_000)
	e.putHoldings(chat.MustGenerateGroupChatID(), mint, 2_000)

	// And a group holding the most, but with too few holders to list.
	unlisted := e.putGatedGroup("Unlisted", mint, false, 1)
	_, err := e.chats.PutGroupHoldings(e.ctx, chat.GroupHoldings{
		ChatID:      unlisted.ChatID,
		Mint:        mint,
		Quarks:      9_000_000,
		HolderCount: chat.MinGroupHoldingsHolders - 1,
		AsOf:        time.UnixMilli(time.Now().Add(time.Second).UnixMilli()),
	})
	require.NoError(t, err)

	keys := model.MustGenerateKeyPair()
	e.authz.Add(model.MustGenerateUserID(), keys)

	// The same for no caller and a registered one.
	for _, resp := range []*communitypb.GetCommunityResponse{
		e.getCommunity(mint),
		e.getCommunity(mint, keys),
	} {
		require.Equal(t, communitypb.GetCommunityResponse_OK, resp.Result)
		require.Equal(t, "Busy Coin", resp.Community.Name)

		chats := resp.Community.TopHolderChats
		require.Len(t, chats, 3)
		for i, want := range []chat.GroupHoldings{top, middle, bottom} {
			md := chats[i]
			require.Equal(t, want.ChatID.Value, md.ChatId.Value)
			require.True(t, proto.Equal(want.ToProto(), md.HoldingsSummary))

			// A public list view: nothing about a viewer, no messaging
			// state.
			require.Empty(t, md.Members)
			require.Nil(t, md.ViewerState)
			require.Nil(t, md.LastMessage)
			require.NotNil(t, md.Rules)
		}
		require.Equal(t, "Top", chats[0].Title)
	}
}

func TestGetCommunity_Capped(t *testing.T) {
	e := newServerEnv(t)
	mint := e.putCurrency("Crowded Coin", ocp_currency.MetadataStateAvailable)

	const groups = 30
	for i := range groups {
		e.putGatedGroup("Group", mint, false, uint64(1_000+i))
	}

	resp := e.getCommunity(mint)
	require.Equal(t, communitypb.GetCommunityResponse_OK, resp.Result)
	chats := resp.Community.TopHolderChats
	require.Len(t, chats, 5)
	require.EqualValues(t, 1_000+groups-1, chats[0].HoldingsSummary.Quarks)
	for i := 1; i < len(chats); i++ {
		require.Greater(t, chats[i-1].HoldingsSummary.Quarks, chats[i].HoldingsSummary.Quarks)
	}
}

func TestGetCommunity_Leader(t *testing.T) {
	e := newServerEnv(t)

	// launcher binds a new user to a new key, with a row profile carrying
	// displayName and username when known, and returns both.
	launcher := func(known bool, displayName, username string) (*commonpb.UserId, *commonpb.PublicKey) {
		userID := model.MustGenerateUserID()
		key := model.MustGenerateKeyPair().Proto()
		_, err := e.accounts.Bind(e.ctx, userID, key)
		require.NoError(t, err)
		if known {
			e.profiles.set(userID, &profilepb.UserProfile{
				UserId:                userID,
				DisplayName:           displayName,
				Username:              &commonpb.Username{Value: username},
				JoinTs:                timestamppb.Now(),
				FlipcardCustomization: profile.DefaultFlipcardCustomization(),
			})
		}
		return userID, key
	}

	// A user with a display name who launched the currency leads it.
	leaderID, leaderKey := launcher(true, "Leader", "leader")
	mint := e.putCurrencyLaunchedBy("Led Coin", ocp_currency.MetadataStateAvailable, leaderKey)
	resp := e.getCommunity(mint)
	require.Equal(t, communitypb.GetCommunityResponse_OK, resp.Result)
	require.NotNil(t, resp.Community.Leader)
	leader := resp.Community.Leader.Profile
	require.Equal(t, leaderID.Value, leader.UserId.Value)
	require.Equal(t, "Leader", leader.DisplayName)
	require.Equal(t, "leader", leader.GetUsername().GetValue())

	// No leader: a key no user holds, a user with no display name, and a user
	// the profile domain does not know.
	_, nameless := launcher(true, "", "nameless")
	_, unknown := launcher(false, "", "")
	for name, key := range map[string]*commonpb.PublicKey{
		"unbound":    model.MustGenerateKeyPair().Proto(),
		"nameless":   nameless,
		"no profile": unknown,
	} {
		mint := e.putCurrencyLaunchedBy("Leaderless "+name, ocp_currency.MetadataStateAvailable, key)
		resp := e.getCommunity(mint)
		require.Equal(t, communitypb.GetCommunityResponse_OK, resp.Result, name)
		require.Nil(t, resp.Community.Leader, name)
	}
}
