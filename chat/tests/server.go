package tests

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"
	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"
	ocp_balancepb "github.com/code-payments/ocp-protobuf-api/generated/go/balance/v1"

	ocp_common "github.com/code-payments/ocp-server/ocp/common"

	"github.com/code-payments/flipcash2-server/account"
	accountmemory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/balance"
	"github.com/code-payments/flipcash2-server/blob"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/event"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/moderation"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/protoutil"
	"github.com/code-payments/flipcash2-server/push"
	"github.com/code-payments/flipcash2-server/redact"
	"github.com/code-payments/flipcash2-server/testutil"
)

// RunServerTests runs the shared chat.Server test suite against s. teardown is
// called between tests to reset the store.
func RunServerTests(t *testing.T, s chat.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s chat.Store){
		testServer_GetChat_OK,
		testServer_GetChat_NotFound,
		testServer_GetChat_Denied,
		testServer_GetChat_Hydrates,
		testServer_GetChat_HydrationFailureCancelsSiblings,
		testServer_GetChat_Dm_HidesPhoneNumbers,
		testServer_GetChat_Dm_NoCreator,
		testServer_GetChat_Dm_UseE2ee,
		testServer_GetChat_Dm_UseE2ee_TeamAccount,
		testServer_GetChat_Dm_TeamAccount_NeverSpeak,
		testServer_GetChat_HiddenWhenPeerBlocked,
		testServer_GetChat_Group_Hydrates,
		testServer_GetChat_Group_Picture,
		testServer_GetChat_Group_MembershipLifecycle,
		testServer_GetChat_Group_NonMember,
		testServer_GetChat_ViewMode,
		testServer_GetChat_Unauthenticated,
		testServer_GetRoster_Group,
		testServer_GetRoster_Paging,
		testServer_GetRoster_Dm,
		testServer_GetRoster_Gates,
		testServer_GetRoster_Invite,
		testServer_GetRoster_PublicGroupDisabled,
		testServer_GetMentionSuggestions,
		testServer_GetMentionSuggestions_Size,
		testServer_GetMentionSuggestions_Gates,
		testServer_SampleChatters,
		testServer_SampleChatters_Size,
		testServer_SampleChatters_Window,
		testServer_SampleChatters_Activity,
		testServer_SampleChatters_Gates,
		testServer_GetDmChatFeed_Empty,
		testServer_GetDmChatFeed_OrderAndContent,
		testServer_GetDmChatFeed_Paging,
		testServer_GetDmChatFeed_Hydrates,
		testServer_GetDmChatFeed_TypeScoped,
		testServer_GetDmChatFeed_TokenBoundToType,
		testServer_GetDmChatFeed_HiddenPerViewer,
		testServer_GetGroupChatFeed_Empty,
		testServer_GetGroupChatFeed_OrderAndContent,
		testServer_GetGroupChatFeed_Paging,
		testServer_GetGroupChatFeed_Hydrates,
		testServer_GetGroupChatFeed_SnapshotPinned,
		testServer_GetGroupChatFeed_DropsDepartedBetweenPages,
		testServer_GetGroupChatFeed_InvalidToken,
		testServer_JoinChat_OK,
		testServer_JoinChat_Idempotent,
		testServer_JoinChat_NotFound,
		testServer_JoinChat_DeniedForDm,
		testServer_JoinChat_StaffRule,
		testServer_JoinChat_MinimumBalanceRule,
		testServer_JoinChat_FiatMinimumBalanceRule,
		testServer_JoinChat_MemberSkipsRules,
		testServer_JoinChat_UnevaluableRuleFails,
		testServer_LeaveChat_OK,
		testServer_LeaveChat_Idempotent,
		testServer_LeaveChat_NotFound,
		testServer_LeaveChat_DeniedForDm,
		testServer_LeaveChat_ThenRejoin,
		testServer_StartChat_OK,
		testServer_StartChat_Idempotent,
		testServer_StartChat_WithPicture,
		testServer_StartChat_PictureNotAccepted,
		testServer_StartChat_WithDescriptionAndCover,
		testServer_StartChat_CoverPictureNotAccepted,
		testServer_StartChat_Description,
		testServer_StartChat_TitleModerated,
		testServer_StartChat_ModerationFailureIsInternal,
		testServer_StartChat_PrivateGroup,
		testServer_OpenVersusPrivate_NonMember,
		testServer_PrivateGroup_Visibility,
		testServer_PrivateGroup_Membership,
		testServer_KeyEnvelope_Gates,
		testServer_KeyEnvelope_Creator,
		testServer_KeyEnvelope_Member,
		testServer_Lobby_Gates,
		testServer_Lobby_EnterAndLeave,
		testServer_Lobby_Limits,
		testServer_Lobby_GetLobbyMembers,
		testServer_Lobby_Admit,
		testServer_Lobby_Deny,
		testServer_PrivateGroup_UserJoinedMessages,
		testServer_StartChat_InvalidRules,
		testServer_StartChat_RulesNotSatisfied,
		testServer_StartChat_MinimumSpeakerBalance,
		testServer_StartChat_WithRules,
		testServer_StartChat_FiatMinimumBalance,
		testServer_MuteChat_Lifecycle,
		testServer_MuteChat_Group,
		testServer_MuteChat_Gates,
		testServer_LeaveChat_ClearsMute,
		testServer_GetChat_ViewerState_LapsedMuteReturned,
		testServer_GetDmChatFeed_ViewerState,
		testServer_EditChat_Title,
		testServer_EditChat_Picture,
		testServer_EditChat_Both,
		testServer_EditChat_NoOp,
		testServer_EditChat_Denied,
		testServer_EditChat_NotFound,
		testServer_EditChat_TitleModerated,
		testServer_EditChat_PictureNotAccepted,
		testServer_EditChat_Description,
		testServer_EditChat_CoverPicture,
		testServer_EditChat_AllFields,
		testServer_ChatProfileDetail_Carriers,
		testServer_EditChat_ModerationFailureIsInternal,
		testServer_EditChat_Rules,
		testServer_EditChat_RulesWithOtherFields,
		testServer_EditChat_Rules_Fixed,
		testServer_EditChat_Rules_Invalid,
		testServer_EditChat_Rules_NotSatisfied,
		testServer_EditChat_Rules_NoOp,
		testServer_EditChat_Rules_Grandfathered,
		testServer_ViewerState_Permissions,
		testServer_FeaturedGroups_SetAndGet,
		testServer_FeaturedGroups_Refused,
		testServer_FeaturedGroups_UnknownUsername,
		testServer_HoldingsSummary,
		testServer_Moderation_Gates,
		testServer_Moderation_Silence,
		testServer_Moderation_SilenceNonMember,
		testServer_Moderation_BanMember,
		testServer_Moderation_BanNonMember,
		testServer_Moderation_BanPrivateGroup,
		testServer_Moderation_BannedReads,
		testServer_Moderation_GetRestrictedUsers,
		testServer_Moderation_GetRestrictedUsers_Paging,
		testServer_Moderation_MalformedTarget,
		testServer_Moderation_GetRestrictedUsers_UnknownUser,
		testServer_Moderation_MentionSuggestions,
		testServer_Invite_Create,
		testServer_Invite_Gates,
		testServer_Invite_ExpiresAt,
		testServer_Invite_Signature,
		testServer_Invite_Cap,
		testServer_Invite_Retry,
		testServer_Invite_Get,
		testServer_Invite_GetUnauthenticated,
		testServer_Invite_List,
		testServer_Invite_Revoke,
		testServer_Invite_Permissions,
		testServer_InviteJoin,
		testServer_InviteJoin_Announcements,
		testServer_InviteJoin_NoOps,
		testServer_InviteJoin_Rejoin,
		testServer_InviteJoin_Refused,
		testServer_InviteJoin_PublicGroup,
		testServer_InviteJoin_Creator,
		testServer_Pin_Gates,
		testServer_Pin_MessageNotFound,
		testServer_Pin_ContentKinds,
		testServer_Pin_Cap,
		testServer_Pin_Group,
		testServer_Pin_Dm,
		testServer_GetPinnedMessages_Paging,
		testServer_GetPinnedMessages_Reading,
		testServer_Pin_MissingMessage,
		testServer_PinSummary_Hydration,
		testServer_PinSummary_Reading,
		testServer_PinSummary_MissingMessage,
	} {
		tf(t, s)
		teardown()
	}
}

type serverEnv struct {
	t          *testing.T
	ctx        context.Context
	client     chatpb.ChatClient
	authz      *auth.StaticAuthorizer
	accounts   *staffAccounts
	ocpBalance *fakeOcpBalance
	store      chat.Store
	messaging  *fakeMessagingReader
	profiles   *fakeProfileReader
	blocklist  *fakeBlocklistReader
	media      *fakeMedia
	moderator  *fakeModerator

	systemMessages *fakeSystemMessenger
	pusher         *fakePusher

	userObserver *event.TestEventObserver[*commonpb.UserId, *eventpb.Event]
	chatObserver *event.TestEventObserver[*commonpb.ChatId, *eventpb.ChatEvent]

	userID *commonpb.UserId
	keys   model.KeyPair

	// teamUserID is the Flipcash team account the server is built with.
	teamUserID *commonpb.UserId
}

// serverConfig is the configuration a test env's server is built with; the
// zero value is what every test gets unless it asks otherwise.
type serverConfig struct {
	disablePublicGroupRoster bool

	// lobbyLimits, when set, override chat.DefaultLobbyLimits.
	lobbyLimits *chat.LobbyLimits

	// maxActiveInvites, when set, overrides chat.MaxActiveInvites.
	maxActiveInvites int

	// maxPinnedMessages, when set, overrides chat.MaxPinnedMessages.
	maxPinnedMessages int
}

func newServerEnv(t *testing.T, s chat.Store) *serverEnv {
	return newServerEnvWithConfig(t, s, serverConfig{})
}

func newServerEnvWithConfig(t *testing.T, s chat.Store, cfg serverConfig) *serverEnv {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := auth.NewStaticAuthorizer(log)
	userID := model.MustGenerateUserID()
	keys := model.MustGenerateKeyPair()
	authz.Add(userID, keys)
	teamUserID := model.MustGenerateUserID()

	accounts := newStaffAccounts(accountmemory.NewInMemory())
	ocpBalance := &fakeOcpBalance{byOwner: make(map[string]uint64)}
	balances := balance.NewClient(log, accounts, ocpBalance)

	userBus := event.NewBus[*commonpb.UserId, *eventpb.Event]()
	userObserver := event.NewTestEventObserver[*commonpb.UserId, *eventpb.Event]()
	userBus.AddHandler(userObserver)
	chatBus := event.NewBus[*commonpb.ChatId, *eventpb.ChatEvent]()
	chatObserver := event.NewTestEventObserver[*commonpb.ChatId, *eventpb.ChatEvent]()
	chatBus.AddHandler(chatObserver)

	messaging := newFakeMessagingReader()
	profiles := newFakeProfileReader()
	blocklist := newFakeBlocklistReader()
	media := newFakeMedia()
	moderator := &fakeModerator{}
	systemMessages := &fakeSystemMessenger{}
	pusher := &fakePusher{}
	access := chat.NewAccess(s, chat.NewRuleEvaluator(accounts, balances, s, teamUserID))
	var opts []chat.ServerOption
	if cfg.lobbyLimits != nil {
		opts = append(opts, chat.WithLobbyLimits(*cfg.lobbyLimits))
	}
	if cfg.maxActiveInvites > 0 {
		opts = append(opts, chat.WithMaxActiveInvites(cfg.maxActiveInvites))
	}
	if cfg.maxPinnedMessages > 0 {
		opts = append(opts, chat.WithMaxPinnedMessages(cfg.maxPinnedMessages))
	}
	hydrator := chat.NewHydrator(log, blocklist, s, media, messaging, profiles, access.Rules(), teamUserID)
	server := chat.NewServer(log, authz, accounts, blocklist, s, media, messaging, moderator, profiles, systemMessages, pusher, access, hydrator, userBus, chatBus, cfg.disablePublicGroupRoster, opts...)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		chatpb.RegisterChatServer(s, server)
	}))

	return &serverEnv{
		t:              t,
		ctx:            ctx,
		client:         chatpb.NewChatClient(cc),
		authz:          authz,
		store:          s,
		messaging:      messaging,
		profiles:       profiles,
		blocklist:      blocklist,
		media:          media,
		moderator:      moderator,
		accounts:       accounts,
		systemMessages: systemMessages,
		pusher:         pusher,
		ocpBalance:     ocpBalance,
		userObserver:   userObserver,
		chatObserver:   chatObserver,
		userID:         userID,
		keys:           keys,
		teamUserID:     teamUserID,
	}
}

// addUser registers a second authorized user with the env.
func (e *serverEnv) addUser() (*commonpb.UserId, model.KeyPair) {
	userID := model.MustGenerateUserID()
	keys := model.MustGenerateKeyPair()
	e.authz.Add(userID, keys)
	return userID, keys
}

// bindKey records keys as userID's public key in the account store, which
// the test authorizer does not, for RPCs that report a user's key (such as
// GetKeyEnvelope's wrapped_by_public_key). Binding the same key again is a
// no-op.
func (e *serverEnv) bindKey(userID *commonpb.UserId, keys model.KeyPair) {
	_, err := e.accounts.Bind(e.ctx, userID, keys.Proto())
	require.NoError(e.t, err)
}

// staffAccounts is an account.Store whose staff flag a test can set: the
// in-memory store answers IsStaff false for everyone, and has no setter.
type staffAccounts struct {
	account.Store

	mu    sync.Mutex
	staff map[string]bool
}

func newStaffAccounts(db account.Store) *staffAccounts {
	return &staffAccounts{Store: db, staff: make(map[string]bool)}
}

func (a *staffAccounts) setStaff(userID *commonpb.UserId, isStaff bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.staff[string(userID.Value)] = isStaff
}

func (a *staffAccounts) IsStaff(_ context.Context, userID *commonpb.UserId) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.staff[string(userID.Value)], nil
}

// fakeOcpBalance is the OCP balance service behind the env's balance client,
// answering each owner's total from a ledger a test sets in USDF quarks. It
// ignores the request's mint filter: mint restriction is covered by the rule
// evaluator's own tests.
type fakeOcpBalance struct {
	mu      sync.Mutex
	byOwner map[string]uint64
	// rates values a USDF unit in each fiat currency the fake can price; a
	// requested currency without one is left unvalued, as OCP answers a code
	// it cannot price.
	rates map[string]float64
}

func (f *fakeOcpBalance) setRate(currency string, rate float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rates == nil {
		f.rates = make(map[string]float64)
	}
	f.rates[currency] = rate
}

func (f *fakeOcpBalance) setBalance(owner *commonpb.PublicKey, quarks uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byOwner[base58.Encode(owner.Value)] = quarks
}

func (f *fakeOcpBalance) GetBalances(_ context.Context, req *ocp_balancepb.GetBalancesRequest, _ ...grpc.CallOption) (*ocp_balancepb.GetBalancesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	resp := &ocp_balancepb.GetBalancesResponse{BalancesByOwner: make(map[string]*ocp_balancepb.OwnerBalance)}
	for _, owner := range req.Owners {
		key := base58.Encode(owner.Value)
		quarks, ok := f.byOwner[key]
		if !ok {
			continue // An owner OCP has no accounts for is left out.
		}
		ownerBalance := &ocp_balancepb.OwnerBalance{Owner: owner, CoreMintValue: quarks}
		for _, code := range req.CurrencyCodes {
			rate, ok := f.rates[code]
			if !ok {
				continue
			}
			if ownerBalance.FiatValuesByCurrency == nil {
				ownerBalance.FiatValuesByCurrency = make(map[string]float64)
			}
			ownerBalance.FiatValuesByCurrency[code] = float64(quarks) / float64(ocp_common.CoreMintQuarksPerUnit) * rate
		}
		resp.BalancesByOwner[key] = ownerBalance
	}
	return resp, nil
}

// fakeMedia is a canned chat.Media for server tests: it resolves
// whatever rendition sets a test registers per ORIGINAL blob ID, and omits the
// rest the way the real reader omits an unknown or not-yet-servable original.
// It attaches, as a chat picture, only the blobs a test has registered as
// attachable, records what it attached to which chat, and records every
// original it was asked to resolve.
type fakeMedia struct {
	renditions map[string][]*blobpb.Rendition

	// attachable is the set of blob IDs SetAsChatMedia accepts, standing in
	// for "a READY image original the caller owns"; any other blob is refused
	// as blob.ErrBlobNotFound.
	attachable map[string]bool
	// chatPictures records the blobs attached to each chat, in attach order,
	// keyed by chat ID.
	chatPictures map[string][]*blobpb.BlobId
	// resolved records every original ResolveRenditions was asked for, keyed
	// by blob ID.
	resolved map[string]bool

	// attachErr, when set, fails every SetAsChatMedia call with it, standing
	// in for a blob-domain outage.
	attachErr error
}

func newFakeMedia() *fakeMedia {
	return &fakeMedia{
		renditions:   make(map[string][]*blobpb.Rendition),
		attachable:   make(map[string]bool),
		chatPictures: make(map[string][]*blobpb.BlobId),
		resolved:     make(map[string]bool),
	}
}

// setAttachable registers a blob SetAsChatMedia will accept, with its
// rendition set resolvable the way a READY original's is.
func (f *fakeMedia) setAttachable(originalID *blobpb.BlobId) []*blobpb.Rendition {
	f.attachable[string(originalID.Value)] = true
	return f.setRenditions(originalID)
}

func (f *fakeMedia) SetAsChatMedia(_ context.Context, _ *commonpb.UserId, chatID *commonpb.ChatId, blobID *blobpb.BlobId) error {
	if f.attachErr != nil {
		return f.attachErr
	}
	if !f.attachable[string(blobID.Value)] {
		return blob.ErrBlobNotFound
	}
	f.chatPictures[string(chatID.Value)] = append(f.chatPictures[string(chatID.Value)], blobID)
	return nil
}

// attachedTo returns the raw IDs of the blobs attached to chatID, in attach
// order.
func (f *fakeMedia) attachedTo(chatID *commonpb.ChatId) [][]byte {
	var out [][]byte
	for _, id := range f.chatPictures[string(chatID.Value)] {
		out = append(out, id.Value)
	}
	return out
}

// setRenditions registers the resolved rendition set for an original: the
// ORIGINAL itself plus a THUMBNAIL, each carrying blob metadata and a download
// URL the way the real reader mints them.
func (f *fakeMedia) setRenditions(originalID *blobpb.BlobId) []*blobpb.Rendition {
	thumbnailID := &blobpb.BlobId{Value: append([]byte(nil), originalID.Value...)}
	thumbnailID.Value[0] ^= 0xff
	renditions := []*blobpb.Rendition{
		{
			Role:   blobpb.Rendition_ORIGINAL,
			BlobId: originalID,
			Blob: &blobpb.BlobMetadata{
				MimeType:  "image/jpeg",
				SizeBytes: 4096,
				DownloadUrl: &blobpb.DownloadUrl{
					Url:       "https://cdn.blobs.test/" + hex.EncodeToString(originalID.Value),
					ExpiresAt: timestamppb.New(at(1).Add(time.Hour)),
				},
			},
		},
		{
			Role:   blobpb.Rendition_THUMBNAIL,
			BlobId: thumbnailID,
			Blob: &blobpb.BlobMetadata{
				MimeType:  "image/jpeg",
				SizeBytes: 256,
				DownloadUrl: &blobpb.DownloadUrl{
					Url:       "https://cdn.blobs.test/" + hex.EncodeToString(thumbnailID.Value),
					ExpiresAt: timestamppb.New(at(1).Add(time.Hour)),
				},
			},
		},
	}
	f.renditions[string(originalID.Value)] = renditions
	return renditions
}

func (f *fakeMedia) ResolveRenditions(_ context.Context, ids []*blobpb.BlobId) (map[string][]*blobpb.Rendition, error) {
	out := make(map[string][]*blobpb.Rendition)
	for _, id := range ids {
		f.resolved[string(id.Value)] = true
		if r, ok := f.renditions[string(id.Value)]; ok {
			out[string(id.Value)] = r
		}
	}
	return out, nil
}

// fakeMessagingReader is a canned chat.MessagingReader for server tests: it
// returns whatever last messages, pointers, and head event sequences a test
// registers per chat, and whatever messages it registers per message (see
// Messages).
type fakeMessagingReader struct {
	lastMessages    map[string]*messagingpb.Message
	messages        map[chat.MessageKey]*messagingpb.Message
	pointers        map[string][]*messagingpb.Pointer
	latestEventSeqs map[string]uint64
	pointerLookups  map[string]int

	// beforeLastMessages, when set, runs first in LastMessages with the call's
	// context, and its error is returned — to hold or fail that one read.
	beforeLastMessages func(ctx context.Context) error
}

func newFakeMessagingReader() *fakeMessagingReader {
	return &fakeMessagingReader{
		lastMessages:    make(map[string]*messagingpb.Message),
		messages:        make(map[chat.MessageKey]*messagingpb.Message),
		pointers:        make(map[string][]*messagingpb.Pointer),
		latestEventSeqs: make(map[string]uint64),
		pointerLookups:  make(map[string]int),
	}
}

func (f *fakeMessagingReader) LastMessages(ctx context.Context, refs []chat.MessageRef) (map[string]*messagingpb.Message, error) {
	if f.beforeLastMessages != nil {
		if err := f.beforeLastMessages(ctx); err != nil {
			return nil, err
		}
	}
	out := make(map[string]*messagingpb.Message)
	for _, ref := range refs {
		if m, ok := f.lastMessages[string(ref.ChatID.Value)]; ok {
			out[string(ref.ChatID.Value)] = m
		}
	}
	return out, nil
}

// Messages returns the registered message for each ref, redacted under a
// redacted reading as the real reader does (see redact.Message); refs with
// none registered are absent.
func (f *fakeMessagingReader) Messages(_ context.Context, refs []chat.MessageRef, reading chat.Reading) (map[chat.MessageKey]*messagingpb.Message, error) {
	out := make(map[chat.MessageKey]*messagingpb.Message)
	for _, ref := range refs {
		key := chat.MessageKeyOf(ref.ChatID, ref.MessageID)
		m, ok := f.messages[key]
		if !ok {
			continue
		}
		if reading == chat.ReadingRedacted {
			redacted, err := redact.Message(ref.ChatID, m)
			if err != nil {
				return nil, err
			}
			m = redacted
		}
		out[key] = m
	}
	return out, nil
}

func (f *fakeMessagingReader) Pointers(_ context.Context, refs []chat.PointerRef) (map[string][]*messagingpb.Pointer, error) {
	out := make(map[string][]*messagingpb.Pointer)
	for _, ref := range refs {
		f.pointerLookups[string(ref.ChatID.Value)]++
		if p, ok := f.pointers[string(ref.ChatID.Value)]; ok {
			out[string(ref.ChatID.Value)] = p
		}
	}
	return out, nil
}

func (f *fakeMessagingReader) LatestEventSequences(_ context.Context, chatIDs []*commonpb.ChatId) (map[string]uint64, error) {
	out := make(map[string]uint64)
	for _, chatID := range chatIDs {
		if seq, ok := f.latestEventSeqs[string(chatID.Value)]; ok {
			out[string(chatID.Value)] = seq
		}
	}
	return out, nil
}

// fakeProfileReader is a canned chat.ProfileReader for server tests: it returns
// whatever phone number, display name and profile picture a test registers per user
// ID.
type fakeProfileReader struct {
	phoneNumbers    map[string]*commonpb.PhoneNumber
	displayNames    map[string]string
	usernames       map[string]string
	profilePictures map[string]*blobpb.Media
	joinedAt        map[string]time.Time

	// unknown holds users the profile domain does not know, whom
	// GetLimitedPublicProfilesForRow leaves out.
	unknown map[string]struct{}

	// phoneNumbersErr, when set, fails every GetPhoneNumbers call.
	phoneNumbersErr error
}

func newFakeProfileReader() *fakeProfileReader {
	return &fakeProfileReader{
		phoneNumbers:    make(map[string]*commonpb.PhoneNumber),
		displayNames:    make(map[string]string),
		usernames:       make(map[string]string),
		profilePictures: make(map[string]*blobpb.Media),
		joinedAt:        make(map[string]time.Time),
		unknown:         make(map[string]struct{}),
	}
}

// setProfilePicture registers a picture for a user, already hydrated the way the
// real reader returns it — the renditions carry their resolved blob metadata.
func (f *fakeProfileReader) setProfilePicture(userID *commonpb.UserId, blobID *blobpb.BlobId) *blobpb.Media {
	picture := &blobpb.Media{
		Renditions: []*blobpb.Rendition{{
			Role:   blobpb.Rendition_ORIGINAL,
			BlobId: blobID,
			Blob: &blobpb.BlobMetadata{
				MimeType:  "image/jpeg",
				SizeBytes: 1024,
				DownloadUrl: &blobpb.DownloadUrl{
					Url:       "https://cdn.blobs.test/" + hex.EncodeToString(blobID.Value),
					ExpiresAt: timestamppb.New(at(1).Add(time.Hour)),
				},
			},
		}},
	}
	f.profilePictures[string(userID.Value)] = picture
	return picture
}

func (f *fakeProfileReader) GetPhoneNumbers(_ context.Context, userIDs []*commonpb.UserId) (map[string]*commonpb.PhoneNumber, error) {
	if f.phoneNumbersErr != nil {
		return nil, f.phoneNumbersErr
	}
	out := make(map[string]*commonpb.PhoneNumber)
	for _, userID := range userIDs {
		if p, ok := f.phoneNumbers[string(userID.Value)]; ok {
			out[string(userID.Value)] = p
		}
	}
	return out, nil
}

// GetLimitedPublicProfilesForRow returns an entry for every user asked about, the way the real
// reader does for a chat's members: they are all users the profile domain knows,
// so each has at least a join timestamp even with no name or picture set. A
// user a test marks unknown is left out.
func (f *fakeProfileReader) GetLimitedPublicProfilesForRow(_ context.Context, userIDs []*commonpb.UserId) (map[string]*profilepb.UserProfile, error) {
	out := make(map[string]*profilepb.UserProfile)
	for _, userID := range userIDs {
		key := string(userID.Value)
		if _, ok := f.unknown[key]; ok {
			continue
		}

		joinedAt, ok := f.joinedAt[key]
		if !ok {
			joinedAt = at(0)
		}

		out[key] = &profilepb.UserProfile{
			UserId:                userID,
			DisplayName:           f.displayNames[key],
			ProfilePicture:        f.profilePictures[key],
			JoinTs:                timestamppb.New(joinedAt),
			FlipcardCustomization: profile.DefaultFlipcardCustomization(),
		}
		if username, ok := f.usernames[key]; ok {
			out[key].Username = &commonpb.Username{Value: username}
		}
	}
	return out, nil
}

// GetUserIDByUsername finds the user a test registered the handle for.
func (f *fakeProfileReader) GetUserIDByUsername(_ context.Context, username string) (*commonpb.UserId, bool, error) {
	for key, held := range f.usernames {
		if held == username {
			return &commonpb.UserId{Value: []byte(key)}, true, nil
		}
	}
	return nil, false, nil
}

// fakeBlocklistReader is a canned chat.BlocklistReader for server tests: it
// reports, for each owner, which candidates the test has registered as blocked.
type fakeBlocklistReader struct {
	// blocked maps an owner to the set of user IDs they have blocked.
	blocked map[string]map[string]bool
}

func newFakeBlocklistReader() *fakeBlocklistReader {
	return &fakeBlocklistReader{blocked: make(map[string]map[string]bool)}
}

// block registers that owner has blocked blockedID.
func (f *fakeBlocklistReader) block(owner, blockedID *commonpb.UserId) {
	set, ok := f.blocked[string(owner.Value)]
	if !ok {
		set = make(map[string]bool)
		f.blocked[string(owner.Value)] = set
	}
	set[string(blockedID.Value)] = true
}

func (f *fakeBlocklistReader) GetBlocked(_ context.Context, ownerID *commonpb.UserId, candidateIDs []*commonpb.UserId) (map[string]bool, error) {
	set := f.blocked[string(ownerID.Value)]
	if len(set) == 0 {
		return nil, nil
	}
	out := make(map[string]bool)
	for _, c := range candidateIDs {
		if set[string(c.Value)] {
			out[string(c.Value)] = true
		}
	}
	return out, nil
}

// userJoinedCall is one call to fakeSystemMessenger.SendUserJoined.
type userJoinedCall struct {
	chatID        *commonpb.ChatId
	userID        *commonpb.UserId
	method        chat.JoinMethod
	rosterVersion uint64
}

// fakePusher records every push the server sends, for tests to assert on
// what would reach a device.
type fakePusher struct {
	mu     sync.Mutex
	pushes []fakePush
}

type fakePush struct {
	title, body string
	payload     *pushpb.Payload
	users       []*commonpb.UserId
	badged      bool
}

func (p *fakePusher) SendPushes(_ context.Context, title, body string, customPayload *pushpb.Payload, users ...*commonpb.UserId) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pushes = append(p.pushes, fakePush{title: title, body: body, payload: customPayload, users: users})
	return nil
}

func (p *fakePusher) SendPushesWithBadges(_ context.Context, title, body string, customPayload *pushpb.Payload, _ push.BadgeResolver, users ...*commonpb.UserId) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pushes = append(p.pushes, fakePush{title: title, body: body, payload: customPayload, users: users, badged: true})
	return nil
}

// pushesTo returns the pushes sent to userID, in order.
func (p *fakePusher) pushesTo(userID *commonpb.UserId) []fakePush {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []fakePush
	for _, sent := range p.pushes {
		for _, u := range sent.users {
			if bytes.Equal(u.Value, userID.Value) {
				out = append(out, sent)
				break
			}
		}
	}
	return out
}

// waitForPushes waits for n pushes to userID, sent off the request path, and
// returns every push sent to them.
func (e *serverEnv) waitForPushes(userID *commonpb.UserId, n int) []fakePush {
	require.Eventually(e.t, func() bool {
		return len(e.pusher.pushesTo(userID)) >= n
	}, time.Second, 5*time.Millisecond)
	return e.pusher.pushesTo(userID)
}

// requireLobbyPush asserts that a push is a lobby push for chatID: a group
// chat push titled title and reading body, navigating to the chat and
// carrying no message, sent to one user with no badge.
func requireLobbyPush(t *testing.T, got fakePush, chatID *commonpb.ChatId, title, body string) {
	require.Equal(t, title, got.title)
	require.Equal(t, body, got.body)
	require.False(t, got.badged)
	require.Len(t, got.users, 1)
	require.Equal(t, pushpb.Payload_CHAT, got.payload.Category)
	require.Equal(t, base64.StdEncoding.EncodeToString(chatID.Value), got.payload.GroupKey)
	require.Equal(t, chatID.Value, got.payload.Navigation.GetChatId().GetValue())
	require.Equal(t, chatpb.ChatType_GROUP, got.payload.ChatMetadata.GetType())
	require.Nil(t, got.payload.ChatMetadata.GetMessageRef())
	require.Nil(t, got.payload.ChatMetadata.GetSendingUserId())
}

// fakeSystemMessenger is a chat.SystemMessenger for server tests: it records
// every join it is asked to write, and fails each with err when set. The
// chat server calls it on the request path, so a test reads its calls with no
// waiting.
type fakeSystemMessenger struct {
	mu    sync.Mutex
	joins []userJoinedCall
	err   error
}

func (f *fakeSystemMessenger) SendUserJoined(_ context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, method chat.JoinMethod, rosterVersion uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.joins = append(f.joins, userJoinedCall{chatID: chatID, userID: userID, method: method, rosterVersion: rosterVersion})
	return f.err
}

func (f *fakeSystemMessenger) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

// requireUserJoinedCall asserts that call wrote userID's join by method at
// rosterVersion.
func requireUserJoinedCall(t *testing.T, call userJoinedCall, userID *commonpb.UserId, method chat.JoinMethod, rosterVersion uint64) {
	t.Helper()
	require.Equal(t, userID.Value, call.userID.Value)
	require.Equal(t, method.Kind, call.method.Kind)
	if method.By == nil {
		require.Nil(t, call.method.By)
	} else {
		require.Equal(t, method.By.Value, call.method.By.GetValue())
	}
	require.Equal(t, rosterVersion, call.rosterVersion)
}

// userJoinedCalls returns the joins recorded for chatID, in order.
func (f *fakeSystemMessenger) userJoinedCalls(chatID *commonpb.ChatId) []userJoinedCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []userJoinedCall
	for _, call := range f.joins {
		if bytes.Equal(call.chatID.Value, chatID.Value) {
			out = append(out, call)
		}
	}
	return out
}

// putDM persists a contact DM the env user is a member of, with the given last
// activity.
func (e *serverEnv) putDM(lastActivity time.Time) *commonpb.ChatId {
	return e.putDMOfType(chatpb.ChatType_CONTACT_DM, lastActivity)
}

// putDMOfType persists a DM of the given type the env user is a member of,
// with the given last activity.
func (e *serverEnv) putDMOfType(chatType chatpb.ChatType, lastActivity time.Time) *commonpb.ChatId {
	return e.putDMWithPeer(chatType, model.MustGenerateUserID(), lastActivity)
}

// putDMWithPeer persists a DM of the given type between the env user and the
// given peer, with the given last activity. It lets a test control the peer's
// identity (e.g. to then block it).
func (e *serverEnv) putDMWithPeer(chatType chatpb.ChatType, peer *commonpb.UserId, lastActivity time.Time) *commonpb.ChatId {
	chatID := generateDmChatID()
	require.NoError(e.t, e.store.PutChat(e.ctx, &chat.Chat{
		ID:           chatID,
		Type:         chatType,
		Members:      []*commonpb.UserId{e.userID, peer},
		LastActivity: lastActivity,
	}))
	return chatID
}

// putGroup persists a group chat whose members are the env user and the given
// others, with the given title and last activity.
func (e *serverEnv) putGroup(title string, lastActivity time.Time, others ...*commonpb.UserId) *commonpb.ChatId {
	return e.putGroupWithPicture(title, nil, lastActivity, others...)
}

// putGroupWithPicture is putGroup with the group's picture set to the blob
// holding its ORIGINAL rendition (nil for no picture).
func (e *serverEnv) putGroupWithPicture(title string, pictureBlobID *blobpb.BlobId, lastActivity time.Time, others ...*commonpb.UserId) *commonpb.ChatId {
	chatID := chat.MustGenerateGroupChatID()
	require.NoError(e.t, e.store.PutChat(e.ctx, &chat.Chat{
		ID:                   chatID,
		Type:                 chatpb.ChatType_GROUP,
		Members:              append([]*commonpb.UserId{e.userID}, others...),
		Title:                title,
		ProfilePictureBlobID: pictureBlobID,
		LastActivity:         lastActivity,
	}))
	return chatID
}

func (e *serverEnv) getChat(keys model.KeyPair, chatID *commonpb.ChatId) *chatpb.GetChatResponse {
	return e.getChatWithMode(keys, chatID, messagingpb.ViewMode_FULL)
}

func (e *serverEnv) getChatWithMode(keys model.KeyPair, chatID *commonpb.ChatId, mode messagingpb.ViewMode) *chatpb.GetChatResponse {
	req := &chatpb.GetChatRequest{ChatId: chatID, ViewMode: mode}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.GetChat(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

// getPublicChat asks for a chat with no auth at all (see chat.Server.GetChat).
func (e *serverEnv) getPublicChat(chatID *commonpb.ChatId, mode messagingpb.ViewMode) *chatpb.GetChatResponse {
	resp, err := e.client.GetChat(e.ctx, &chatpb.GetChatRequest{ChatId: chatID, ViewMode: mode})
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) getRoster(keys model.KeyPair, chatID *commonpb.ChatId, opts *commonpb.QueryOptions) (*chatpb.GetRosterResponse, error) {
	req := &chatpb.GetRosterRequest{ChatId: chatID, QueryOptions: opts}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.GetRoster(e.ctx, req)
}

func (e *serverEnv) mustGetRoster(keys model.KeyPair, chatID *commonpb.ChatId, opts *commonpb.QueryOptions) *chatpb.GetRosterResponse {
	resp, err := e.getRoster(keys, chatID, opts)
	require.NoError(e.t, err)
	return resp
}

// getRosterWithInvite asks for a roster as keys, sending an invite's token
// (see chat.Server.GetRoster).
func (e *serverEnv) getRosterWithInvite(keys model.KeyPair, chatID *commonpb.ChatId, token *chatpb.InviteToken, opts *commonpb.QueryOptions) *chatpb.GetRosterResponse {
	req := &chatpb.GetRosterRequest{ChatId: chatID, QueryOptions: opts, InviteToken: token}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.GetRoster(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

// getPublicRoster asks for a roster with no auth at all, an invite's token
// alone, which may be nil (see chat.Server.GetRoster).
func (e *serverEnv) getPublicRoster(chatID *commonpb.ChatId, token *chatpb.InviteToken, opts *commonpb.QueryOptions) *chatpb.GetRosterResponse {
	resp, err := e.client.GetRoster(e.ctx, &chatpb.GetRosterRequest{ChatId: chatID, QueryOptions: opts, InviteToken: token})
	require.NoError(e.t, err)
	return resp
}

// memberUserIDs projects a page onto its members' user IDs, in page order.
func memberUserIDs(members []*chatpb.Member) [][]byte {
	out := make([][]byte, len(members))
	for i, m := range members {
		out[i] = m.UserId.Value
	}
	return out
}

func (e *serverEnv) getDmFeed(opts *commonpb.QueryOptions) *chatpb.GetDmChatFeedResponse {
	resp, err := e.getDmFeedOfType(chatpb.ChatType_CONTACT_DM, opts)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) getDmFeedOfType(chatType chatpb.ChatType, opts *commonpb.QueryOptions) (*chatpb.GetDmChatFeedResponse, error) {
	req := &chatpb.GetDmChatFeedRequest{DmChatType: chatType, QueryOptions: opts}
	require.NoError(e.t, e.keys.Auth(req, &req.Auth))
	return e.client.GetDmChatFeed(e.ctx, req)
}

func (e *serverEnv) getGroupFeed(opts *commonpb.QueryOptions) (*chatpb.GetGroupChatFeedResponse, error) {
	req := &chatpb.GetGroupChatFeedRequest{QueryOptions: opts}
	require.NoError(e.t, e.keys.Auth(req, &req.Auth))
	return e.client.GetGroupChatFeed(e.ctx, req)
}

func (e *serverEnv) mustGetGroupFeed(opts *commonpb.QueryOptions) *chatpb.GetGroupChatFeedResponse {
	resp, err := e.getGroupFeed(opts)
	require.NoError(e.t, err)
	require.Equal(e.t, chatpb.GetGroupChatFeedResponse_OK, resp.Result)
	return resp
}

func (e *serverEnv) joinChat(keys model.KeyPair, chatID *commonpb.ChatId) (*chatpb.JoinChatResponse, error) {
	req := &chatpb.JoinChatRequest{ChatId: chatID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.JoinChat(e.ctx, req)
}

func (e *serverEnv) mustJoinChat(keys model.KeyPair, chatID *commonpb.ChatId) *chatpb.JoinChatResponse {
	resp, err := e.joinChat(keys, chatID)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) leaveChat(keys model.KeyPair, chatID *commonpb.ChatId) (*chatpb.LeaveChatResponse, error) {
	req := &chatpb.LeaveChatRequest{ChatId: chatID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.LeaveChat(e.ctx, req)
}

func (e *serverEnv) mustLeaveChat(keys model.KeyPair, chatID *commonpb.ChatId) *chatpb.LeaveChatResponse {
	resp, err := e.leaveChat(keys, chatID)
	require.NoError(e.t, err)
	return resp
}

// rosterUpdatesOnChatTopic returns every RosterUpdate published on chatID's
// topic, paired with the users each publish excluded, in publish order.
func (e *serverEnv) rosterUpdatesOnChatTopic(chatID *commonpb.ChatId) (updates []*chatpb.RosterUpdate, excludes [][]*commonpb.UserId) {
	for _, ev := range e.chatObserver.GetEvents(func(k *commonpb.ChatId) bool { return bytes.Equal(k.Value, chatID.Value) }) {
		require.Equal(e.t, chatID.Value, ev.Event.ChatId.Value)
		require.Equal(e.t, chatID.Value, ev.Event.Event.GetChatUpdate().GetChat().GetValue())
		for _, u := range ev.Event.Event.GetChatUpdate().GetRosterUpdates().GetRosterUpdates() {
			updates = append(updates, u)
			excludes = append(excludes, ev.Event.ExcludeUserIds)
		}
	}
	return updates, excludes
}

// rosterUpdatesOnUserTopic returns every RosterUpdate for chatID published on
// userID's topic, in publish order.
func (e *serverEnv) rosterUpdatesOnUserTopic(userID *commonpb.UserId, chatID *commonpb.ChatId) []*chatpb.RosterUpdate {
	var updates []*chatpb.RosterUpdate
	for _, ev := range e.userObserver.GetEvents(func(k *commonpb.UserId) bool { return bytes.Equal(k.Value, userID.Value) }) {
		update := ev.Event.GetChatUpdate()
		if !bytes.Equal(update.GetChat().GetValue(), chatID.Value) {
			continue
		}
		updates = append(updates, update.GetRosterUpdates().GetRosterUpdates()...)
	}
	return updates
}

// waitForRosterUpdates waits for at least n roster updates on chatID's topic
// and n on userID's, which is what one transition publishes. The bus hands
// events to observers on their own goroutines, so a test asserts on them only
// after waiting.
func (e *serverEnv) waitForRosterUpdates(userID *commonpb.UserId, chatID *commonpb.ChatId, n int) {
	e.chatObserver.WaitFor(e.t, func([]*event.KeyAndEvent[*commonpb.ChatId, *eventpb.ChatEvent]) bool {
		updates, _ := e.rosterUpdatesOnChatTopic(chatID)
		return len(updates) >= n
	})
	e.userObserver.WaitFor(e.t, func([]*event.KeyAndEvent[*commonpb.UserId, *eventpb.Event]) bool {
		return len(e.rosterUpdatesOnUserTopic(userID, chatID)) >= n
	})
}

// requireNoRosterUpdates asserts, after giving the bus a moment to deliver,
// that no roster update was published on chatID's topic or userID's.
func (e *serverEnv) requireNoRosterUpdates(userID *commonpb.UserId, chatID *commonpb.ChatId) {
	time.Sleep(100 * time.Millisecond)
	updates, _ := e.rosterUpdatesOnChatTopic(chatID)
	require.Empty(e.t, updates)
	require.Empty(e.t, e.rosterUpdatesOnUserTopic(userID, chatID))
}

func testServer_GetChat_OK(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	chatID := e.putDM(at(1))

	resp := e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.NotNil(t, resp.Metadata)
	require.Equal(t, chatID.Value, resp.Metadata.ChatId.Value)
	require.Equal(t, chatpb.ChatType_CONTACT_DM, resp.Metadata.Type)
	require.Len(t, resp.Metadata.Members, 2)
	// A DM's roster is fixed at creation: its inline members at version zero.
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 2, Version: 0}, resp.Metadata.GetRosterSummary()))
	require.Equal(t, e.userID.Value, resp.Metadata.Members[0].UserId.Value)
	require.True(t, resp.Metadata.LastActivity.AsTime().Equal(at(1)))

	// Pictures are a group-only feature; a DM never carries one.
	require.Nil(t, resp.Metadata.ProfilePicture)
}

func testServer_GetChat_NotFound(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	resp := e.getChat(e.keys, generateDmChatID())
	require.Equal(t, chatpb.GetChatResponse_NOT_FOUND, resp.Result)
	require.Nil(t, resp.Metadata)
}

func testServer_GetChat_Denied(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	chatID := e.putDM(at(1))

	// A registered user who is not a member of the chat is denied.
	strangerID := model.MustGenerateUserID()
	strangerKeys := model.MustGenerateKeyPair()
	e.authz.Add(strangerID, strangerKeys)

	resp := e.getChat(strangerKeys, chatID)
	require.Equal(t, chatpb.GetChatResponse_DENIED, resp.Result)
	require.Nil(t, resp.Metadata)
}

func testServer_GetChat_HiddenWhenPeerBlocked(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// A DM whose peer the viewer has blocked comes back hidden.
	blockedPeer := model.MustGenerateUserID()
	hidden := e.putDMWithPeer(chatpb.ChatType_CONTACT_DM, blockedPeer, at(1))
	e.blocklist.block(e.userID, blockedPeer)

	resp := e.getChat(e.keys, hidden)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.True(t, resp.Metadata.IsHidden)

	// A DM whose peer the viewer has not blocked stays visible — proving the flag
	// tracks the block, not merely DM-ness.
	visible := e.putDMWithPeer(chatpb.ChatType_CONTACT_DM, model.MustGenerateUserID(), at(1))
	resp = e.getChat(e.keys, visible)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.False(t, resp.Metadata.IsHidden)
}

// testServer_GetRoster_Group pins a group's roster page: every joined
// member, most recently joined first, each with a hydrated profile, their
// join time and the version of the join that placed them; the summary
// alongside; no pointers, and no phone numbers, whatever is registered.
func testServer_GetRoster_Group(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	memberB := model.MustGenerateUserID()
	chatID := e.putGroup("Weekend Trip", at(1), memberB)
	e.profiles.displayNames[string(e.userID.Value)] = "Viewer"
	e.profiles.displayNames[string(memberB.Value)] = "Member B"
	e.profiles.phoneNumbers[string(memberB.Value)] = &commonpb.PhoneNumber{Value: "+15551234567"}
	e.messaging.pointers[string(chatID.Value)] = []*messagingpb.Pointer{
		{Type: messagingpb.Pointer_READ, UserId: memberB, Value: &messagingpb.MessageId{Value: 4}, Ts: timestamppb.New(at(4))},
	}

	// A later joiner heads the roster.
	joiner := model.MustGenerateUserID()
	e.profiles.displayNames[string(joiner.Value)] = "Joiner"
	settle()
	addGroupMembers(t, s, chatID, joiner)

	resp := e.mustGetRoster(e.keys, chatID, nil)
	require.Equal(t, chatpb.GetRosterResponse_OK, resp.Result)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 3, Version: 1}, resp.RosterSummary))
	require.False(t, resp.HasMore)
	require.Nil(t, resp.PagingToken)
	require.Len(t, resp.Members, 3)

	// Newest first: the joiner, then the two creation members. Only the joiner
	// has a transition to be stamped with.
	require.Equal(t, joiner.Value, resp.Members[0].UserId.Value)
	require.Equal(t, "Joiner", resp.Members[0].UserProfile.DisplayName)
	require.EqualValues(t, 1, resp.Members[0].Version)
	require.ElementsMatch(t, [][]byte{e.userID.Value, memberB.Value}, memberUserIDs(resp.Members[1:]))
	for _, m := range resp.Members[1:] {
		require.Zero(t, m.Version)
	}
	for i, m := range resp.Members {
		require.NotNil(t, m.JoinedAt, "member %d", i)
		if i > 0 {
			require.False(t, m.JoinedAt.AsTime().After(resp.Members[i-1].JoinedAt.AsTime()), "member %d joined after member %d", i, i-1)
		}
		require.Nil(t, m.UserProfile.PhoneNumber)
		require.Empty(t, m.Pointers)
	}
	require.Zero(t, e.messaging.pointerLookups[string(chatID.Value)], "a group's pointers are not so much as read")

	// A departed member is off the roster; the summary moves with them.
	removeGroupMember(t, s, chatID, memberB)
	resp = e.mustGetRoster(e.keys, chatID, nil)
	require.Equal(t, chatpb.GetRosterResponse_OK, resp.Result)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 2, Version: 2}, resp.RosterSummary))
	require.Equal(t, [][]byte{joiner.Value, e.userID.Value}, memberUserIDs(resp.Members))
}

// testServer_GetRoster_Paging pins the walk: pages of the requested size,
// each carrying the summary and a token that resumes strictly after it, whose
// concatenation is the whole roster in order; has_more exact, so the walk
// ends on a full page; and a token refused outside the chat it was minted
// for, or when malformed.
func testServer_GetRoster_Paging(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	chatID := e.putGroup("Walked", at(1))
	joiners := make([]*commonpb.UserId, 4)
	for i := range joiners {
		joiners[i] = model.MustGenerateUserID()
		settle()
		addGroupMembers(t, s, chatID, joiners[i])
	}
	want := [][]byte{joiners[3].Value, joiners[2].Value, joiners[1].Value, joiners[0].Value, e.userID.Value}

	var got [][]byte
	var token *commonpb.PagingToken
	for page := 0; ; page++ {
		resp := e.mustGetRoster(e.keys, chatID, &commonpb.QueryOptions{PageSize: 2, PagingToken: token})
		require.Equal(t, chatpb.GetRosterResponse_OK, resp.Result)
		require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 5, Version: 4}, resp.RosterSummary))
		require.LessOrEqual(t, len(resp.Members), 2)
		got = append(got, memberUserIDs(resp.Members)...)
		if !resp.HasMore {
			require.Nil(t, resp.PagingToken)
			require.Equal(t, 2, page, "five members page by two in three pages")
			break
		}
		require.NotNil(t, resp.PagingToken)
		token = resp.PagingToken
	}
	require.Equal(t, want, got)

	// A token is bound to its chat: replayed into another group the caller is
	// in, it is refused rather than resumed.
	first := e.mustGetRoster(e.keys, chatID, &commonpb.QueryOptions{PageSize: 2})
	require.NotNil(t, first.PagingToken)
	other := e.putGroup("Other", at(2))
	_, err := e.getRoster(e.keys, other, &commonpb.QueryOptions{PagingToken: first.PagingToken})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// As is a token the server did not mint.
	_, err = e.getRoster(e.keys, chatID, &commonpb.QueryOptions{PagingToken: &commonpb.PagingToken{Value: []byte("garbage")}})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// The default page holds the whole roster of a small group.
	resp := e.mustGetRoster(e.keys, chatID, nil)
	require.Equal(t, want, memberUserIDs(resp.Members))
	require.False(t, resp.HasMore)
}

// testServer_GetRoster_Dm pins a DM's roster: its two participants, with
// profiles, pointers and — for a contact DM — phone numbers hydrated as
// GetChat hydrates them, at version zero with no join time, under the DM's
// fixed summary.
func testServer_GetRoster_Dm(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	peer := model.MustGenerateUserID()
	chatID := e.putDMWithPeer(chatpb.ChatType_CONTACT_DM, peer, at(1))
	e.profiles.displayNames[string(peer.Value)] = "Peer"
	e.profiles.phoneNumbers[string(peer.Value)] = &commonpb.PhoneNumber{Value: "+15551234567"}
	e.messaging.pointers[string(chatID.Value)] = []*messagingpb.Pointer{
		{Type: messagingpb.Pointer_READ, UserId: peer, Value: &messagingpb.MessageId{Value: 3}, Ts: timestamppb.New(at(3))},
		{Type: messagingpb.Pointer_SENT, UserId: e.userID, Value: &messagingpb.MessageId{Value: 4}, Ts: timestamppb.New(at(4))},
	}

	resp := e.mustGetRoster(e.keys, chatID, nil)
	require.Equal(t, chatpb.GetRosterResponse_OK, resp.Result)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 2, Version: 0}, resp.RosterSummary))
	require.False(t, resp.HasMore)
	require.Len(t, resp.Members, 2)
	members := byUserID(resp.Members)
	for _, m := range resp.Members {
		require.Nil(t, m.JoinedAt)
		require.Zero(t, m.Version)
	}
	require.Equal(t, "Peer", members[string(peer.Value)].UserProfile.DisplayName)
	require.Equal(t, "+15551234567", members[string(peer.Value)].UserProfile.PhoneNumber.Value)
	require.Len(t, members[string(peer.Value)].Pointers, 1)
	require.Equal(t, messagingpb.Pointer_READ, members[string(peer.Value)].Pointers[0].Type)
	// SENT pointers are the sender's alone and never surfaced.
	require.Empty(t, members[string(e.userID.Value)].Pointers)

	// A DM pages too, under the same cursor, should a client ask for one at a
	// time.
	first := e.mustGetRoster(e.keys, chatID, &commonpb.QueryOptions{PageSize: 1})
	require.Len(t, first.Members, 1)
	require.True(t, first.HasMore)
	second := e.mustGetRoster(e.keys, chatID, &commonpb.QueryOptions{PageSize: 1, PagingToken: first.PagingToken})
	require.Len(t, second.Members, 1)
	require.False(t, second.HasMore)
	require.ElementsMatch(t, [][]byte{e.userID.Value, peer.Value}, append(memberUserIDs(first.Members), memberUserIDs(second.Members)...))
}

// testServer_GetRoster_Gates pins who may read a roster: a member; a
// non-member a group's listener rules admit; and no one else — not a DM's
// stranger, not a non-member of a group without rules, and not a non-member
// who fails them, who may preview the group but not its roster.
func (e *serverEnv) getMentionSuggestions(keys model.KeyPair, chatID *commonpb.ChatId) *chatpb.GetMentionSuggestionsResponse {
	req := &chatpb.GetMentionSuggestionsRequest{ChatId: chatID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.GetMentionSuggestions(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

// recordSend records userID's send in a group at sentAt, as the send path
// does, and requires that it was recorded.
func (e *serverEnv) recordSend(chatID *commonpb.ChatId, userID *commonpb.UserId, sentAt time.Time) {
	recorded, err := e.store.RecordSend(e.ctx, chatID, userID, sentAt)
	require.NoError(e.t, err)
	require.True(e.t, recorded)
}

// suggestedUserIDs projects suggestions onto their users' IDs, in order.
func suggestedUserIDs(suggestions []*chatpb.MentionSuggestion) [][]byte {
	out := make([][]byte, len(suggestions))
	for i, suggestion := range suggestions {
		out[i] = suggestion.UserProfile.UserId.Value
	}
	return out
}

func (e *serverEnv) sampleChatters(keys model.KeyPair, chatID *commonpb.ChatId) *chatpb.SampleChattersResponse {
	req := &chatpb.SampleChattersRequest{ChatId: chatID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.SampleChatters(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

// samplePublicChatters asks for a sample with no auth at all (see
// chat.Server.SampleChatters).
func (e *serverEnv) samplePublicChatters(chatID *commonpb.ChatId) *chatpb.SampleChattersResponse {
	resp, err := e.client.SampleChatters(e.ctx, &chatpb.SampleChattersRequest{ChatId: chatID})
	require.NoError(e.t, err)
	return resp
}

// putGroupWithCreator creates a public group with the env's user and others
// as members, recording creator as the group's creator.
func (e *serverEnv) putGroupWithCreator(title string, creator *commonpb.UserId, others ...*commonpb.UserId) *commonpb.ChatId {
	chatID := chat.MustGenerateGroupChatID()
	require.NoError(e.t, e.store.PutChat(e.ctx, &chat.Chat{
		ID:           chatID,
		Type:         chatpb.ChatType_GROUP,
		Members:      append([]*commonpb.UserId{e.userID, creator}, others...),
		Title:        title,
		CreatorID:    creator,
		LastActivity: at(1),
	}))
	return chatID
}

// sampledUserIDs projects a sample onto its users' IDs, in order.
func sampledUserIDs(chatters []*chatpb.SampledChatter) [][]byte {
	out := make([][]byte, len(chatters))
	for i, chatter := range chatters {
		out[i] = chatter.UserProfile.UserId.Value
	}
	return out
}

func testServer_SampleChatters(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	creator := model.MustGenerateUserID()  // never sent, at first
	recent := model.MustGenerateUserID()   // the most recent sender but the caller
	earlier := model.MustGenerateUserID()  // sent before the others
	blocked := model.MustGenerateUserID()  // the caller blocked them; still shown
	departed := model.MustGenerateUserID() // sent, then left
	quiet := model.MustGenerateUserID()    // a member who never sent
	groupID := e.putGroupWithCreator("Chatters", creator, recent, earlier, blocked, departed, quiet)
	e.profiles.displayNames[string(creator.Value)] = "Creator"
	e.blocklist.block(e.userID, blocked)

	e.recordSend(groupID, earlier, at(10))
	e.recordSend(groupID, departed, at(25))
	e.recordSend(groupID, recent, at(30))
	e.recordSend(groupID, blocked, at(40))
	e.recordSend(groupID, e.userID, at(50))
	_, _, err := s.RemoveGroupMember(e.ctx, groupID, departed, false)
	require.NoError(t, err)

	// The creator first, though they never sent; then the senders who are
	// still members, most recent first, the caller and a blocked user
	// included. Someone who left, and a member who never sent, are not.
	resp := e.sampleChatters(e.keys, groupID)
	require.Equal(t, chatpb.SampleChattersResponse_OK, resp.Result)
	require.Equal(t, [][]byte{creator.Value, e.userID.Value, blocked.Value, recent.Value, earlier.Value}, sampledUserIDs(resp.Chatters))
	require.False(t, resp.HasMore)
	require.True(t, resp.Chatters[0].IsCreator)
	require.Nil(t, resp.Chatters[0].LastSentAt)
	require.Equal(t, "Creator", resp.Chatters[0].UserProfile.DisplayName)
	for i, want := range []time.Time{at(50), at(40), at(30), at(10)} {
		chatter := resp.Chatters[i+1]
		require.False(t, chatter.IsCreator)
		require.True(t, chatter.LastSentAt.AsTime().Equal(want), "chatter %d", i+1)
	}

	// A creator who sends stays first, at their send.
	e.recordSend(groupID, creator, at(60))
	resp = e.sampleChatters(e.keys, groupID)
	require.Equal(t, [][]byte{creator.Value, e.userID.Value, blocked.Value, recent.Value, earlier.Value}, sampledUserIDs(resp.Chatters))
	require.True(t, resp.Chatters[0].LastSentAt.AsTime().Equal(at(60)))

	// Someone who rejoins is back at their last send.
	_, _, err = s.AddGroupMembers(e.ctx, groupID, []*commonpb.UserId{departed})
	require.NoError(t, err)
	resp = e.sampleChatters(e.keys, groupID)
	require.Equal(t, [][]byte{creator.Value, e.userID.Value, blocked.Value, recent.Value, departed.Value, earlier.Value}, sampledUserIDs(resp.Chatters))

	// A creator who has left is not shown, though they have sent.
	_, _, err = s.RemoveGroupMember(e.ctx, groupID, creator, false)
	require.NoError(t, err)
	resp = e.sampleChatters(e.keys, groupID)
	require.Equal(t, [][]byte{e.userID.Value, blocked.Value, recent.Value, departed.Value, earlier.Value}, sampledUserIDs(resp.Chatters))
	require.False(t, resp.Chatters[0].IsCreator)

	// A group no one has sent in is its creator alone, and a group with no
	// recorded creator starts with its senders.
	quietGroup := e.putGroupWithCreator("Quiet", creator)
	resp = e.sampleChatters(e.keys, quietGroup)
	require.Equal(t, chatpb.SampleChattersResponse_OK, resp.Result)
	require.Equal(t, [][]byte{creator.Value}, sampledUserIDs(resp.Chatters))
	legacy := e.putGroup("Legacy", at(1))
	resp = e.sampleChatters(e.keys, legacy)
	require.Equal(t, chatpb.SampleChattersResponse_OK, resp.Result)
	require.Empty(t, resp.Chatters)
	e.recordSend(legacy, e.userID, at(5))
	resp = e.sampleChatters(e.keys, legacy)
	require.Equal(t, [][]byte{e.userID.Value}, sampledUserIDs(resp.Chatters))
}

func testServer_SampleChatters_Size(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// The creator and 19 senders fill the sample exactly: nothing more.
	creator := model.MustGenerateUserID()
	senders := make([]*commonpb.UserId, 25)
	for i := range senders {
		senders[i] = model.MustGenerateUserID()
	}
	groupID := e.putGroupWithCreator("Busy", creator, senders...)
	for i := range 19 {
		e.recordSend(groupID, senders[i], at(int64(i+1)))
	}
	resp := e.sampleChatters(e.keys, groupID)
	require.Equal(t, chatpb.SampleChattersResponse_OK, resp.Result)
	require.Len(t, resp.Chatters, 20)
	require.False(t, resp.HasMore)

	// More senders, and the sample keeps the creator and the 19 most
	// recent, and says there are more.
	for i := 19; i < len(senders); i++ {
		e.recordSend(groupID, senders[i], at(int64(i+1)))
	}
	resp = e.sampleChatters(e.keys, groupID)
	require.Len(t, resp.Chatters, 20)
	require.True(t, resp.HasMore)
	require.Equal(t, creator.Value, resp.Chatters[0].UserProfile.UserId.Value)
	for i, chatter := range resp.Chatters[1:] {
		require.Equal(t, senders[len(senders)-1-i].Value, chatter.UserProfile.UserId.Value, "chatter %d", i+1)
	}
}

func testServer_SampleChatters_Window(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// A full window of senders, nearly all of whom have left: the members
	// among them are found past the first check, and since the window was
	// full, there may be more.
	creator := model.MustGenerateUserID()
	stayed := []*commonpb.UserId{model.MustGenerateUserID(), model.MustGenerateUserID()}
	groupID := e.putGroupWithCreator("Churn", creator, stayed...)
	const window = 100
	for i := range window {
		sender := model.MustGenerateUserID()
		switch i {
		case 40:
			sender = stayed[0]
		case 90:
			sender = stayed[1]
		}
		e.recordSend(groupID, sender, at(int64(window-i)))
	}
	resp := e.sampleChatters(e.keys, groupID)
	require.Equal(t, chatpb.SampleChattersResponse_OK, resp.Result)
	require.Equal(t, [][]byte{creator.Value, stayed[0].Value, stayed[1].Value}, sampledUserIDs(resp.Chatters))
	require.True(t, resp.HasMore)
	require.Nil(t, resp.Chatters[0].LastSentAt)

	// A creator who sent before everyone in the window is still shown at
	// their send.
	e.recordSend(groupID, creator, at(0))
	resp = e.sampleChatters(e.keys, groupID)
	require.Equal(t, [][]byte{creator.Value, stayed[0].Value, stayed[1].Value}, sampledUserIDs(resp.Chatters))
	require.True(t, resp.Chatters[0].IsCreator)
	require.True(t, resp.Chatters[0].LastSentAt.AsTime().Equal(at(0)))
}

func testServer_SampleChatters_Activity(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	creator := model.MustGenerateUserID()
	regular := model.MustGenerateUserID() // sent daily for a week
	oneOff := model.MustGenerateUserID()  // sent once, a day after the regular
	nearby := model.MustGenerateUserID()  // sent once, later, at a score just below the regular's
	groupID := e.putGroupWithCreator("Regulars", creator, regular, oneOff, nearby)

	day := 24 * time.Hour
	var score, last time.Time
	for d := range 7 {
		sentAt := at(0).Add(time.Duration(d) * day)
		e.recordSend(groupID, regular, sentAt)
		score, last = chat.NextActivityScore(score, last, sentAt), sentAt
	}
	e.recordSend(groupID, oneOff, last.Add(day))

	// Distant scores go to the more active: the regular, about six days
	// ahead of their last send, above someone who sent once a day after it.
	// Each is shown at their own last send.
	resp := e.sampleChatters(e.keys, groupID)
	require.Equal(t, chatpb.SampleChattersResponse_OK, resp.Result)
	require.Equal(t, [][]byte{creator.Value, regular.Value, oneOff.Value}, sampledUserIDs(resp.Chatters))
	require.True(t, resp.Chatters[1].LastSentAt.AsTime().Equal(last))
	require.True(t, resp.Chatters[2].LastSentAt.AsTime().Equal(last.Add(day)))

	// Close scores go to the more recent: a single send at nine tenths of
	// the regular's lead scores just below them, but its last send is so
	// much later that it ranks first.
	lead := score.Sub(last)
	nearbyAt := last.Add(lead * 9 / 10)
	e.recordSend(groupID, nearby, nearbyAt)
	senders, err := s.GetActiveSenders(e.ctx, groupID, 0)
	require.NoError(t, err)
	require.Equal(t, regular.Value, senders[0].UserID.Value, "by score alone, the regular leads")
	resp = e.sampleChatters(e.keys, groupID)
	require.Equal(t, [][]byte{creator.Value, nearby.Value, regular.Value, oneOff.Value}, sampledUserIDs(resp.Chatters))
}

func testServer_SampleChatters_Gates(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	_, strangerKeys := e.addUser()

	// A DM, before anything is read.
	resp := e.sampleChatters(e.keys, e.putDM(at(1)))
	require.Equal(t, chatpb.SampleChattersResponse_DENIED, resp.Result)

	// A group that does not exist.
	resp = e.sampleChatters(e.keys, chat.MustGenerateGroupChatID())
	require.Equal(t, chatpb.SampleChattersResponse_NOT_FOUND, resp.Result)

	// A private group, even to its creator and members.
	private := e.putPrivateGroup("Private")
	resp = e.sampleChatters(e.keys, private)
	require.Equal(t, chatpb.SampleChattersResponse_DENIED, resp.Result)

	// Without auth: the same answers.
	require.Equal(t, chatpb.SampleChattersResponse_DENIED, e.samplePublicChatters(e.putDM(at(1))).Result)
	require.Equal(t, chatpb.SampleChattersResponse_NOT_FOUND, e.samplePublicChatters(chat.MustGenerateGroupChatID()).Result)
	require.Equal(t, chatpb.SampleChattersResponse_DENIED, e.samplePublicChatters(private).Result)

	// A public group's sample is the same to a member, a non-member and
	// someone with no auth at all, the member's own place in it included.
	creator := model.MustGenerateUserID()
	public := e.putGroupWithCreator("Public", creator)
	e.recordSend(public, e.userID, at(5))
	member := e.sampleChatters(e.keys, public)
	require.Equal(t, chatpb.SampleChattersResponse_OK, member.Result)
	require.Equal(t, [][]byte{creator.Value, e.userID.Value}, sampledUserIDs(member.Chatters))
	for _, resp := range []*chatpb.SampleChattersResponse{
		e.sampleChatters(strangerKeys, public),
		e.samplePublicChatters(public),
	} {
		require.True(t, proto.Equal(member, resp))
	}

	// Auth, when set, must still be valid.
	unregistered := model.MustGenerateKeyPair()
	req := &chatpb.SampleChattersRequest{ChatId: public}
	require.NoError(t, unregistered.Auth(req, &req.Auth))
	_, err := e.client.SampleChatters(e.ctx, req)
	require.Error(t, err)
}

func testServer_GetMentionSuggestions(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	recent := model.MustGenerateUserID()   // most recent sender
	departed := model.MustGenerateUserID() // sent, then left
	earlier := model.MustGenerateUserID()  // sent before the others
	blocked := model.MustGenerateUserID()  // the caller blocked them
	blocker := model.MustGenerateUserID()  // they blocked the caller; still suggested
	nameless := model.MustGenerateUserID() // holds no username
	quiet := model.MustGenerateUserID()    // a member who never sent
	groupID := e.putGroup("Mentions", at(1), recent, departed, earlier, blocked, blocker, nameless, quiet)

	for userID, username := range map[*commonpb.UserId]string{
		recent:   "recent",
		departed: "departed",
		earlier:  "earlier",
		blocked:  "blocked",
		blocker:  "blocker",
		quiet:    "quiet",
	} {
		e.profiles.usernames[string(userID.Value)] = username
	}
	e.blocklist.block(e.userID, blocked)
	e.blocklist.block(blocker, e.userID)

	e.recordSend(groupID, earlier, at(10))
	e.recordSend(groupID, departed, at(25))
	e.recordSend(groupID, recent, at(30))
	e.recordSend(groupID, blocked, at(40))
	e.recordSend(groupID, blocker, at(45))
	e.recordSend(groupID, nameless, at(50))
	e.recordSend(groupID, e.userID, at(60))
	_, _, err := s.RemoveGroupMember(e.ctx, groupID, departed, false)
	require.NoError(t, err)

	// Most recent first; someone who left is still suggested, and so is
	// someone who blocked the caller. The caller, the user the caller blocked
	// and the user without a username are dropped, and a member who never
	// sent is not suggested.
	resp := e.getMentionSuggestions(e.keys, groupID)
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_OK, resp.Result)
	require.Equal(t, [][]byte{blocker.Value, recent.Value, departed.Value, earlier.Value}, suggestedUserIDs(resp.Suggestions))
	for i, want := range []struct {
		username string
		sentAt   time.Time
	}{
		{"blocker", at(45)},
		{"recent", at(30)},
		{"departed", at(25)},
		{"earlier", at(10)},
	} {
		require.Equal(t, want.username, resp.Suggestions[i].UserProfile.GetUsername().GetValue())
		require.True(t, resp.Suggestions[i].LastSentAt.AsTime().Equal(want.sentAt))
	}

	// A group nobody has sent in has no suggestions.
	empty := e.putGroup("Quiet", at(1), recent)
	resp = e.getMentionSuggestions(e.keys, empty)
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_OK, resp.Result)
	require.Empty(t, resp.Suggestions)
}

func testServer_GetMentionSuggestions_Size(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	groupID := e.putGroup("Busy", at(1))

	// More senders than a response carries: the most recent are kept.
	const senders = 80
	userIDs := make([]*commonpb.UserId, senders)
	for i := range senders {
		userIDs[i] = model.MustGenerateUserID()
		e.profiles.usernames[string(userIDs[i].Value)] = fmt.Sprintf("user_%d", i)
		e.recordSend(groupID, userIDs[i], at(int64(i+1)))
	}

	resp := e.getMentionSuggestions(e.keys, groupID)
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_OK, resp.Result)
	require.Len(t, resp.Suggestions, 50)
	for i, suggestion := range resp.Suggestions {
		require.Equal(t, userIDs[senders-1-i].Value, suggestion.UserProfile.UserId.Value, "suggestion %d", i)
	}
}

func testServer_GetMentionSuggestions_Gates(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	_, strangerKeys := e.addUser()

	resp := e.getMentionSuggestions(e.keys, chat.MustGenerateGroupChatID())
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_NOT_FOUND, resp.Result)

	// A DM is refused, even to its members.
	dm := e.putDM(at(1))
	resp = e.getMentionSuggestions(e.keys, dm)
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_DENIED, resp.Result)

	// A non-member cannot speak, so is refused.
	groupID := e.putGroup("Members only", at(1))
	resp = e.getMentionSuggestions(strangerKeys, groupID)
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_DENIED, resp.Result)
	resp = e.getMentionSuggestions(e.keys, groupID)
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_OK, resp.Result)

	// In a creator-only group, a member who is not the creator cannot speak.
	creator := model.MustGenerateUserID()
	creatorOnly := &chat.Chat{
		ID:                   chat.MustGenerateGroupChatID(),
		Type:                 chatpb.ChatType_GROUP,
		Members:              []*commonpb.UserId{creator, e.userID},
		Title:                "Announcements",
		IsCreatorOnlySpeaker: true,
		CreatorID:            creator,
		LastActivity:         at(1),
	}
	require.NoError(t, s.PutChat(e.ctx, creatorOnly))
	resp = e.getMentionSuggestions(e.keys, creatorOnly.ID)
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_DENIED, resp.Result)
}

func testServer_GetRoster_Gates(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	_, strangerKeys := e.addUser()

	resp := e.mustGetRoster(e.keys, chat.MustGenerateGroupChatID(), nil)
	require.Equal(t, chatpb.GetRosterResponse_NOT_FOUND, resp.Result)

	dm := e.putDM(at(1))
	resp = e.mustGetRoster(strangerKeys, dm, nil)
	require.Equal(t, chatpb.GetRosterResponse_DENIED, resp.Result)

	closed := e.putGroup("Closed", at(1))
	resp = e.mustGetRoster(strangerKeys, closed, nil)
	require.Equal(t, chatpb.GetRosterResponse_DENIED, resp.Result)

	// A roster is a member's alone. A non-member is denied whether or not
	// they satisfy the group's rules: one who fails them may only preview the
	// group, and one who meets them may read its messages, but neither may
	// see who is in it until they join.
	const requirement = 100
	founder := model.MustGenerateUserID()
	gated := &chat.Chat{
		ID:                     chat.MustGenerateGroupChatID(),
		Type:                   chatpb.ChatType_GROUP,
		Members:                []*commonpb.UserId{founder},
		Title:                  "Whales",
		MinimumListenerBalance: &chat.MinimumBalance{Currency: "usd", NativeAmount: requirement},
		LastActivity:           at(1),
	}
	require.NoError(t, s.PutChat(e.ctx, gated))
	resp = e.mustGetRoster(e.keys, gated.ID, nil)
	require.Equal(t, chatpb.GetRosterResponse_DENIED, resp.Result)
	e.fundEnvUser(requirement)
	require.Equal(t, chatpb.GetChatResponse_OK, e.getChat(e.keys, gated.ID).Result)
	resp = e.mustGetRoster(e.keys, gated.ID, nil)
	require.Equal(t, chatpb.GetRosterResponse_DENIED, resp.Result)

	// Joining makes it theirs to read, and leaving takes it away again.
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(e.keys, gated.ID).Result)
	resp = e.mustGetRoster(e.keys, gated.ID, nil)
	require.Equal(t, chatpb.GetRosterResponse_OK, resp.Result)
	require.ElementsMatch(t, [][]byte{founder.Value, e.userID.Value}, memberUserIDs(resp.Members))
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, gated.ID).Result)
	resp = e.mustGetRoster(e.keys, gated.ID, nil)
	require.Equal(t, chatpb.GetRosterResponse_DENIED, resp.Result)
}

// testServer_GetRoster_Invite pins the roster read with an invite's token: the
// page a member gets, with auth or without, while the invite admits users to
// this chat, and DENIED otherwise, as is a request with neither auth nor a
// token. A ban does not stop it, a member's token is never looked at, the
// invite is read again on every page, and reading counts no use. Auth that
// is set must still be valid.
func testServer_GetRoster_Invite(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.bindKey(e.userID, e.keys)
	strangerID, strangerKeys := e.addBoundUser("Stranger")
	joinerID, _ := e.addBoundUser("Joiner")
	chatID := e.putKeyedPrivateGroup("Sunday Hikers", model.MustGenerateUserID(), model.MustGenerateUserID())
	token, invite := e.mustCreateInvite(e.keys, chatID)

	member := e.mustGetRoster(e.keys, chatID, nil)
	require.Equal(t, chatpb.GetRosterResponse_OK, member.Result)
	require.Len(t, member.Members, 3)

	// The token alone is served the member's page, and so is a non-member
	// with auth who sends it.
	require.NoError(t, protoutil.ProtoEqualError(member, e.getPublicRoster(chatID, token, nil)))
	require.NoError(t, protoutil.ProtoEqualError(member, e.getRosterWithInvite(strangerKeys, chatID, token, nil)))

	// Reading counts no use.
	e.waitForChatInvites(e.keys, chatID, invite)

	// Neither auth nor a token, or a non-member without a token, is DENIED.
	resp := e.getPublicRoster(chatID, nil, nil)
	require.Equal(t, chatpb.GetRosterResponse_DENIED, resp.Result)
	require.Empty(t, resp.Members)
	require.Equal(t, chatpb.GetRosterResponse_DENIED, e.mustGetRoster(strangerKeys, chatID, nil).Result)

	// A member's token is never looked at: one that names no invite changes
	// nothing.
	require.NoError(t, protoutil.ProtoEqualError(member, e.getRosterWithInvite(e.keys, chatID, newInviteToken(), nil)))

	// A token that names no invite, or an invite to another chat, is DENIED.
	unknown := newInviteToken()
	require.Equal(t, chatpb.GetRosterResponse_DENIED, e.getPublicRoster(chatID, unknown, nil).Result)
	require.Equal(t, chatpb.GetRosterResponse_DENIED, e.getRosterWithInvite(strangerKeys, chatID, unknown, nil).Result)
	other := e.putKeyedPrivateGroup("Other")
	otherToken, _ := e.mustCreateInvite(e.keys, other)
	require.Equal(t, chatpb.GetRosterResponse_OK, e.getPublicRoster(other, otherToken, nil).Result)
	require.Equal(t, chatpb.GetRosterResponse_DENIED, e.getPublicRoster(chatID, otherToken, nil).Result)
	require.Equal(t, chatpb.GetRosterResponse_DENIED, e.getRosterWithInvite(strangerKeys, chatID, otherToken, nil).Result)

	// A token never serves a public group or a DM.
	public := e.putGroup("Open", at(1))
	dm := e.putDM(at(1))
	for _, id := range []*commonpb.ChatId{public, dm} {
		resp = e.getPublicRoster(id, token, nil)
		require.Equal(t, chatpb.GetRosterResponse_DENIED, resp.Result)
		require.Empty(t, resp.Members)
		require.Equal(t, chatpb.GetRosterResponse_DENIED, e.getRosterWithInvite(strangerKeys, id, token, nil).Result)
	}

	// A ban stops a user joining, not reading the roster with a link.
	require.Equal(t, chatpb.BanUserResponse_OK, e.mustBanUser(e.keys, chatID, strangerID))
	require.NoError(t, protoutil.ProtoEqualError(member, e.getRosterWithInvite(strangerKeys, chatID, token, nil)))

	// Auth that is set must be valid: a request altered after it was signed
	// is refused, not answered off its token.
	req := &chatpb.GetRosterRequest{ChatId: chatID}
	require.NoError(t, strangerKeys.Auth(req, &req.Auth))
	req.InviteToken = token
	_, err := e.client.GetRoster(e.ctx, req)
	require.Error(t, err)

	// A walk with the token pages the roster as a member's does.
	var walked [][]byte
	var pageToken *commonpb.PagingToken
	for {
		resp = e.getPublicRoster(chatID, token, &commonpb.QueryOptions{PageSize: 1, PagingToken: pageToken})
		require.Equal(t, chatpb.GetRosterResponse_OK, resp.Result)
		walked = append(walked, memberUserIDs(resp.Members)...)
		if !resp.HasMore {
			break
		}
		pageToken = resp.PagingToken
	}
	require.Equal(t, memberUserIDs(member.Members), walked)

	// The invite is read again on every page: once revoked, the walk ends.
	first := e.getPublicRoster(chatID, token, &commonpb.QueryOptions{PageSize: 1})
	require.True(t, first.HasMore)
	require.Equal(t, chatpb.RevokeInviteResponse_OK, e.revokeInvite(e.keys, chatID, invite.InviteId).Result)
	resp = e.getPublicRoster(chatID, token, &commonpb.QueryOptions{PageSize: 1, PagingToken: first.PagingToken})
	require.Equal(t, chatpb.GetRosterResponse_DENIED, resp.Result)
	require.Empty(t, resp.Members)
	require.Equal(t, chatpb.GetRosterResponse_DENIED, e.getRosterWithInvite(strangerKeys, chatID, token, nil).Result)

	// An invite used up admits no one, and so shows no one.
	limitedToken := newInviteToken()
	limitedReq := createInviteRequest(e.keys, chatID, limitedToken)
	limitedReq.MaxUses = 1
	limited := e.createInvite(e.keys, limitedReq)
	require.Equal(t, chatpb.CreateInviteResponse_OK, limited.Result)
	require.Equal(t, chatpb.GetRosterResponse_OK, e.getPublicRoster(chatID, limitedToken, nil).Result)
	_, err = e.store.JoinWithInvite(e.ctx, chatID, joinerID, limited.Invite.InviteId, inviteSecret(3), time.Now())
	require.NoError(t, err)
	require.Equal(t, chatpb.GetRosterResponse_DENIED, e.getPublicRoster(chatID, limitedToken, nil).Result)

	// Nor does one past its expiry. Written through the store, which takes an
	// expiry in the past, so the read is deterministic.
	expiredToken := newInviteToken()
	expiresAt := time.Now().Add(-time.Minute)
	_, created, err := e.store.CreateInvite(e.ctx, chat.Invite{
		ID:             chat.MustDeriveInviteID(expiredToken),
		ChatID:         chatID,
		CreatedBy:      e.userID,
		CreatedAt:      time.Now().Add(-time.Hour),
		ExpiresAt:      &expiresAt,
		SecretEnvelope: inviteSecret(1),
	}, signedInviteKeyEnvelope(e.keys, chatID, chat.MustDeriveInviteID(expiredToken)), chat.MaxActiveInvites, time.Now())
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, chatpb.GetRosterResponse_DENIED, e.getPublicRoster(chatID, expiredToken, nil).Result)
}

// testServer_GetRoster_PublicGroupDisabled pins the operator's switch: with
// public group rosters disabled, a member of a public group is DENIED, as is
// a non-member (the switch is judged before membership), while a DM's participant and a private group's member are
// served their roster as ever, and a non-member of a private group is still
// DENIED. A chat that does not exist is still NOT_FOUND, since the switch is
// judged off the record. A request that fails authorization is refused on
// that ground first: the switch is behind the authorizer, not in front of it.
func testServer_GetRoster_PublicGroupDisabled(t *testing.T, s chat.Store) {
	e := newServerEnvWithConfig(t, s, serverConfig{disablePublicGroupRoster: true})

	other := model.MustGenerateUserID()
	group := e.putGroup("Group", at(1), other)
	dm := e.putDM(at(1))
	private := e.putPrivateGroup("Private", other)

	resp := e.mustGetRoster(e.keys, group, nil)
	require.Equal(t, chatpb.GetRosterResponse_DENIED, resp.Result)
	require.Empty(t, resp.Members)

	resp = e.mustGetRoster(e.keys, dm, nil)
	require.Equal(t, chatpb.GetRosterResponse_OK, resp.Result)
	require.Len(t, resp.Members, 2)

	resp = e.mustGetRoster(e.keys, private, nil)
	require.Equal(t, chatpb.GetRosterResponse_OK, resp.Result)
	require.ElementsMatch(t, [][]byte{e.userID.Value, other.Value}, memberUserIDs(resp.Members))

	_, outsider := e.addUser()
	resp = e.mustGetRoster(outsider, group, nil)
	require.Equal(t, chatpb.GetRosterResponse_DENIED, resp.Result)

	resp = e.mustGetRoster(outsider, private, nil)
	require.Equal(t, chatpb.GetRosterResponse_DENIED, resp.Result)

	resp = e.mustGetRoster(e.keys, chat.MustGenerateGroupChatID(), nil)
	require.Equal(t, chatpb.GetRosterResponse_NOT_FOUND, resp.Result)

	// Auth that fails is refused on that ground, not by the switch. A request
	// with neither auth nor an invite's token is DENIED before anything.
	req := &chatpb.GetRosterRequest{ChatId: dm}
	require.NoError(t, e.keys.Auth(req, &req.Auth))
	req.ChatId = group
	_, err := e.client.GetRoster(e.ctx, req)
	require.Error(t, err)
	require.NotEqual(t, codes.OK, status.Code(err))
	resp = e.getPublicRoster(group, nil, nil)
	require.Equal(t, chatpb.GetRosterResponse_DENIED, resp.Result)
}

func testServer_GetDmChatFeed_Empty(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	resp := e.getDmFeed(&commonpb.QueryOptions{})
	require.Equal(t, chatpb.GetDmChatFeedResponse_OK, resp.Result)
	require.Empty(t, resp.Chats)
	require.False(t, resp.HasMore)
	require.Nil(t, resp.PagingToken)
}

func testServer_GetDmChatFeed_HiddenPerViewer(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// Two DMs in the viewer's feed; the peer of one is blocked. A single batched
	// blocklist read must mark exactly that chat hidden.
	blockedPeer := model.MustGenerateUserID()
	hidden := e.putDMWithPeer(chatpb.ChatType_CONTACT_DM, blockedPeer, at(2))
	visible := e.putDMWithPeer(chatpb.ChatType_CONTACT_DM, model.MustGenerateUserID(), at(1))
	e.blocklist.block(e.userID, blockedPeer)

	resp := e.getDmFeed(&commonpb.QueryOptions{})
	require.Equal(t, chatpb.GetDmChatFeedResponse_OK, resp.Result)
	require.Len(t, resp.Chats, 2)

	byID := make(map[string]*chatpb.Metadata, len(resp.Chats))
	for _, c := range resp.Chats {
		byID[string(c.ChatId.Value)] = c
	}
	require.True(t, byID[string(hidden.Value)].IsHidden)
	require.False(t, byID[string(visible.Value)].IsHidden)
}

func testServer_GetDmChatFeed_OrderAndContent(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// Persist out of order; the feed must return most-recent activity first.
	older := e.putDM(at(1))
	newer := e.putDM(at(2))

	resp := e.getDmFeed(&commonpb.QueryOptions{})
	require.Equal(t, chatpb.GetDmChatFeedResponse_OK, resp.Result)
	require.False(t, resp.HasMore)
	require.Len(t, resp.Chats, 2)

	require.Equal(t, newer.Value, resp.Chats[0].ChatId.Value)
	require.Equal(t, older.Value, resp.Chats[1].ChatId.Value)

	// The chat-domain metadata is populated for each entry.
	first := resp.Chats[0]
	require.Equal(t, chatpb.ChatType_CONTACT_DM, first.Type)
	require.Len(t, first.Members, 2)
	require.Equal(t, e.userID.Value, first.Members[0].UserId.Value)
	require.True(t, first.LastActivity.AsTime().Equal(at(2)))

	// A paging token is minted (it opaquely pins the snapshot and cursor).
	require.NotNil(t, resp.PagingToken)
}

func testServer_GetDmChatFeed_Paging(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	const total = 5
	want := make([][]byte, total)
	for i := 0; i < total; i++ {
		// Increasing activity, so DESC order is the reverse of insertion order.
		chatID := e.putDM(at(int64(i + 1)))
		want[total-1-i] = chatID.Value
	}

	var got [][]byte
	var token *commonpb.PagingToken
	for {
		resp := e.getDmFeed(&commonpb.QueryOptions{PageSize: 2, PagingToken: token})
		require.Equal(t, chatpb.GetDmChatFeedResponse_OK, resp.Result)
		require.LessOrEqual(t, len(resp.Chats), 2)
		for _, c := range resp.Chats {
			got = append(got, c.ChatId.Value)
		}
		if !resp.HasMore {
			break
		}
		require.NotNil(t, resp.PagingToken)
		token = resp.PagingToken
	}

	require.Equal(t, want, got)
}

func testServer_GetChat_Hydrates(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	peer := model.MustGenerateUserID()
	chatID := generateDmChatID()
	require.NoError(t, s.PutChat(e.ctx, &chat.Chat{
		ID:            chatID,
		Type:          chatpb.ChatType_CONTACT_DM,
		Members:       []*commonpb.UserId{e.userID, peer},
		LastActivity:  at(1),
		LastMessageID: &messagingpb.MessageId{Value: 7},
	}))

	e.messaging.lastMessages[string(chatID.Value)] = textMessage(7, peer, "hi")
	e.messaging.pointers[string(chatID.Value)] = []*messagingpb.Pointer{
		{Type: messagingpb.Pointer_READ, UserId: e.userID, Value: &messagingpb.MessageId{Value: 7}, Ts: timestamppb.New(at(7))},
		{Type: messagingpb.Pointer_DELIVERED, UserId: peer, Value: &messagingpb.MessageId{Value: 7}, Ts: timestamppb.New(at(7))},
	}
	// The head sits ahead of the last message's event sequence (e.g. an older
	// message was since edited), so this exercises that it is read from the
	// messaging reader rather than derived from last_message.
	e.messaging.latestEventSeqs[string(chatID.Value)] = 9
	e.profiles.phoneNumbers[string(peer.Value)] = &commonpb.PhoneNumber{Value: "+15551234567"}
	e.profiles.displayNames[string(peer.Value)] = "Peer Name"
	peerPicture := e.profiles.setProfilePicture(peer, &blobpb.BlobId{Value: make([]byte, 16)})

	resp := e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)

	// The last message is hydrated from the messaging reader.
	require.NotNil(t, resp.Metadata.LastMessage)
	require.Equal(t, uint64(7), resp.Metadata.LastMessage.MessageId.Value)
	require.Equal(t, "hi", resp.Metadata.LastMessage.Content[0].GetText().Text)

	// The chat's head event sequence is hydrated, independent of last_message.
	require.Equal(t, uint64(9), resp.Metadata.LatestEventSequence)

	// Pointers are distributed onto the matching member by user ID.
	members := byUserID(resp.Metadata.Members)
	require.Len(t, members[string(e.userID.Value)].Pointers, 1)
	require.Equal(t, messagingpb.Pointer_READ, members[string(e.userID.Value)].Pointers[0].Type)
	require.Len(t, members[string(peer.Value)].Pointers, 1)
	require.Equal(t, messagingpb.Pointer_DELIVERED, members[string(peer.Value)].Pointers[0].Type)

	// The other DM member's phone number, display name and profile picture are
	// hydrated onto their profile.
	require.NotNil(t, members[string(peer.Value)].UserProfile)
	require.NotNil(t, members[string(peer.Value)].UserProfile.PhoneNumber)
	require.Equal(t, "+15551234567", members[string(peer.Value)].UserProfile.PhoneNumber.Value)
	require.Equal(t, "Peer Name", members[string(peer.Value)].UserProfile.DisplayName)

	// The avatar arrives with its blob metadata already resolved, so the client can
	// render it without a follow-up GetBlobs.
	picture := members[string(peer.Value)].UserProfile.GetProfilePicture()
	require.NotNil(t, picture)
	require.Len(t, picture.Renditions, 1)
	require.Equal(t, peerPicture.Renditions[0].BlobId.Value, picture.Renditions[0].BlobId.Value)
	require.NotNil(t, picture.Renditions[0].Blob)
	require.NotEmpty(t, picture.Renditions[0].Blob.GetDownloadUrl().GetUrl())

	// The env user registered none of them, so they all stay unset.
	require.NotNil(t, members[string(e.userID.Value)].UserProfile)
	require.Nil(t, members[string(e.userID.Value)].UserProfile.PhoneNumber)
	require.Empty(t, members[string(e.userID.Value)].UserProfile.DisplayName)
	require.Nil(t, members[string(e.userID.Value)].UserProfile.GetProfilePicture())
}

// The hydration reads run concurrently, and one failing must not leave the
// others running to completion for a response that is already an error: the
// failure cancels their context.
func testServer_GetChat_HydrationFailureCancelsSiblings(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	peer := model.MustGenerateUserID()
	chatID := generateDmChatID()
	require.NoError(t, s.PutChat(e.ctx, &chat.Chat{
		ID:            chatID,
		Type:          chatpb.ChatType_CONTACT_DM,
		Members:       []*commonpb.UserId{e.userID, peer},
		LastActivity:  at(1),
		LastMessageID: &messagingpb.MessageId{Value: 1},
	}))

	// One read fails outright; another holds until its context is cancelled,
	// and reports whether that happened rather than hanging the test.
	e.profiles.phoneNumbersErr = errors.New("profiles unavailable")
	cancelled := make(chan bool, 1)
	e.messaging.beforeLastMessages = func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			cancelled <- true
			return ctx.Err()
		case <-time.After(5 * time.Second):
			cancelled <- false
			return errors.New("never cancelled")
		}
	}

	req := &chatpb.GetChatRequest{ChatId: chatID}
	require.NoError(t, e.keys.Auth(req, &req.Auth))
	_, err := e.client.GetChat(e.ctx, req)
	require.Equal(t, codes.Internal, status.Code(err))
	require.True(t, <-cancelled)
}

func testServer_GetChat_Dm_HidesPhoneNumbers(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	peer := model.MustGenerateUserID()
	chatID := generateDmChatID()
	require.NoError(t, s.PutChat(e.ctx, &chat.Chat{
		ID:           chatID,
		Type:         chatpb.ChatType_DM,
		Members:      []*commonpb.UserId{e.userID, peer},
		LastActivity: at(1),
	}))

	e.profiles.phoneNumbers[string(peer.Value)] = &commonpb.PhoneNumber{Value: "+15551234567"}
	e.profiles.displayNames[string(peer.Value)] = "Peer Name"
	e.profiles.setProfilePicture(peer, &blobpb.BlobId{Value: make([]byte, 16)})

	resp := e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Equal(t, chatpb.ChatType_DM, resp.Metadata.Type)

	members := byUserID(resp.Metadata.Members)
	profile := members[string(peer.Value)].UserProfile
	require.NotNil(t, profile)
	require.Nil(t, profile.PhoneNumber)
	require.Equal(t, "Peer Name", profile.DisplayName)
	require.NotNil(t, profile.GetProfilePicture())
}

func testServer_GetChat_Dm_NoCreator(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// A DM has no creator, whatever its type and whichever read serves it.
	for _, chatType := range []chatpb.ChatType{chatpb.ChatType_CONTACT_DM, chatpb.ChatType_DM} {
		chatID := e.putDMOfType(chatType, at(1))

		resp := e.getChat(e.keys, chatID)
		require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
		require.Equal(t, chatType, resp.Metadata.Type)
		require.Nil(t, resp.Metadata.Creator)

		feed, err := e.getDmFeedOfType(chatType, &commonpb.QueryOptions{})
		require.NoError(t, err)
		require.Equal(t, chatpb.GetDmChatFeedResponse_OK, feed.Result)
		require.Len(t, feed.Chats, 1)
		require.Equal(t, chatID.Value, feed.Chats[0].ChatId.Value)
		require.Nil(t, feed.Chats[0].Creator)
	}
}

func testServer_GetChat_Dm_UseE2ee_TeamAccount(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// The Flipcash team account's DMs never carry use_e2ee: the server writes
	// into them on the team's behalf and holds no keys for it.
	chatID := e.putDMWithPeer(chatpb.ChatType_DM, e.teamUserID, at(1))

	resp := e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.False(t, resp.Metadata.UseE2Ee)

	feed, err := e.getDmFeedOfType(chatpb.ChatType_DM, &commonpb.QueryOptions{})
	require.NoError(t, err)
	require.Len(t, feed.Chats, 1)
	require.Equal(t, chatID.Value, feed.Chats[0].ChatId.Value)
	require.False(t, feed.Chats[0].UseE2Ee)
}

func testServer_GetChat_Dm_TeamAccount_NeverSpeak(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// A DM with the Flipcash team account is shown with a Never speaker rule,
	// whatever its type and whichever read serves it; any other DM carries no
	// rules.
	neverSpeak := &chatpb.Rules{
		Speaker: []*chatpb.SpeakerRules{{
			Kind: &chatpb.SpeakerRules_Never{Never: &chatpb.Never{}},
		}},
	}
	for _, chatType := range []chatpb.ChatType{chatpb.ChatType_CONTACT_DM, chatpb.ChatType_DM} {
		teamChatID := e.putDMWithPeer(chatType, e.teamUserID, at(2))
		otherChatID := e.putDMWithPeer(chatType, model.MustGenerateUserID(), at(1))

		resp := e.getChat(e.keys, teamChatID)
		require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
		require.True(t, proto.Equal(neverSpeak, resp.Metadata.Rules), "%s", chatType)

		resp = e.getChat(e.keys, otherChatID)
		require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
		require.Nil(t, resp.Metadata.Rules, "%s", chatType)

		feed, err := e.getDmFeedOfType(chatType, &commonpb.QueryOptions{})
		require.NoError(t, err)
		require.Len(t, feed.Chats, 2)
		for _, md := range feed.Chats {
			if bytes.Equal(md.ChatId.Value, teamChatID.Value) {
				require.True(t, proto.Equal(neverSpeak, md.Rules), "%s", chatType)
			} else {
				require.Nil(t, md.Rules, "%s", chatType)
			}
		}
	}
}

func testServer_GetChat_Dm_UseE2ee(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// use_e2ee is set on every DM, whatever its type and whichever read serves
	// it, and never on a group.
	for _, chatType := range []chatpb.ChatType{chatpb.ChatType_CONTACT_DM, chatpb.ChatType_DM} {
		chatID := e.putDMWithPeer(chatType, model.MustGenerateUserID(), at(1))

		resp := e.getChat(e.keys, chatID)
		require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
		require.True(t, resp.Metadata.UseE2Ee)

		feed, err := e.getDmFeedOfType(chatType, &commonpb.QueryOptions{})
		require.NoError(t, err)
		require.Equal(t, chatpb.GetDmChatFeedResponse_OK, feed.Result)
		require.Len(t, feed.Chats, 1)
		require.Equal(t, chatID.Value, feed.Chats[0].ChatId.Value)
		require.True(t, feed.Chats[0].UseE2Ee)
	}

	// A group never carries it.
	groupID := e.putGroup("Group", at(2), model.MustGenerateUserID())
	resp := e.getChat(e.keys, groupID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.False(t, resp.Metadata.UseE2Ee)
}

func testServer_GetChat_Group_Hydrates(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	memberB := model.MustGenerateUserID()
	memberC := model.MustGenerateUserID()
	chatID := e.putGroup("Weekend Trip", at(1), memberB, memberC)

	// The viewer has a full profile registered — including a phone number, which
	// a group must never expose.
	e.profiles.displayNames[string(e.userID.Value)] = "Viewer"
	e.profiles.displayNames[string(memberB.Value)] = "Member B"
	e.profiles.phoneNumbers[string(e.userID.Value)] = &commonpb.PhoneNumber{Value: "+15551234567"}

	// The viewer and another member each have a stored READ pointer. Only the
	// viewer's is surfaced: it is what their unread count is computed from.
	e.messaging.pointers[string(chatID.Value)] = []*messagingpb.Pointer{
		{Type: messagingpb.Pointer_READ, UserId: e.userID, Value: &messagingpb.MessageId{Value: 3}, Ts: timestamppb.New(at(3))},
		{Type: messagingpb.Pointer_READ, UserId: memberB, Value: &messagingpb.MessageId{Value: 4}, Ts: timestamppb.New(at(4))},
	}

	// One member is on the viewer's blocklist: a group has no single peer, so
	// the DM peer-blocked hiding must not apply.
	e.blocklist.block(e.userID, memberC)

	resp := e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Equal(t, chatpb.ChatType_GROUP, resp.Metadata.Type)
	require.Equal(t, "Weekend Trip", resp.Metadata.Title)
	require.False(t, resp.Metadata.IsHidden)
	require.True(t, resp.Metadata.LastActivity.AsTime().Equal(at(1)))

	// The roster is not enumerated: the viewer is the only member carried, with
	// a hydrated profile, and the summary says how large the roster really is.
	require.Len(t, resp.Metadata.Members, 1)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 3, Version: 0}, resp.Metadata.GetRosterSummary()))
	self := resp.Metadata.Members[0]
	require.Equal(t, e.userID.Value, self.UserId.Value)
	require.Equal(t, "Viewer", self.UserProfile.DisplayName)

	// The viewer's own entry carries neither join time nor version: those are
	// the roster page's (see GetRoster), not the metadata's.
	require.Nil(t, self.JoinedAt)
	require.Zero(t, self.Version)

	// The viewer's own pointer is surfaced; the other member's is not, and the
	// viewer's phone number is not, registered or not.
	require.Len(t, self.Pointers, 1)
	require.Equal(t, messagingpb.Pointer_READ, self.Pointers[0].Type)
	require.Equal(t, uint64(3), self.Pointers[0].Value.Value)
	require.Nil(t, self.UserProfile.PhoneNumber)
}

func testServer_GetChat_Group_Picture(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// A group with a picture whose original resolves: the metadata carries the
	// full rendition set — ORIGINAL plus derived — each with its blob metadata
	// and a download URL, so the client renders the avatar with no follow-up.
	pictureBlobID := &blobpb.BlobId{Value: []byte("group-picture-01")}
	want := e.media.setRenditions(pictureBlobID)
	withPicture := e.putGroupWithPicture("Weekend Trip", pictureBlobID, at(1), model.MustGenerateUserID())

	resp := e.getChat(e.keys, withPicture)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	picture := resp.Metadata.GetProfilePicture()
	require.NotNil(t, picture)
	require.Len(t, picture.Renditions, len(want))
	for i, r := range picture.Renditions {
		require.Equal(t, want[i].Role, r.Role)
		require.Equal(t, want[i].BlobId.Value, r.BlobId.Value)
		require.NotNil(t, r.Blob)
		require.Equal(t, want[i].Blob.MimeType, r.Blob.MimeType)
		require.Equal(t, want[i].Blob.GetDownloadUrl().GetUrl(), r.Blob.GetDownloadUrl().GetUrl())
	}

	// A group without a picture carries none.
	withoutPicture := e.putGroup("No Picture", at(1), model.MustGenerateUserID())
	resp = e.getChat(e.keys, withoutPicture)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Nil(t, resp.Metadata.GetProfilePicture())

	// A picture whose original no longer resolves (unknown to blob storage, or
	// not servable) is not an error: the metadata still names the stored
	// ORIGINAL, unresolved, for the client to treat as unavailable.
	unresolvable := &blobpb.BlobId{Value: []byte("group-picture-02")}
	stale := e.putGroupWithPicture("Stale Picture", unresolvable, at(1), model.MustGenerateUserID())
	resp = e.getChat(e.keys, stale)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	picture = resp.Metadata.GetProfilePicture()
	require.NotNil(t, picture)
	require.Len(t, picture.Renditions, 1)
	require.Equal(t, blobpb.Rendition_ORIGINAL, picture.Renditions[0].Role)
	require.Equal(t, unresolvable.Value, picture.Renditions[0].BlobId.Value)
	require.Nil(t, picture.Renditions[0].Blob)

	// Replacing the picture through the store is what the next read reflects.
	replacement := &blobpb.BlobId{Value: []byte("group-picture-03")}
	e.media.setRenditions(replacement)
	require.NoError(t, s.SetGroupPicture(e.ctx, withPicture, replacement))
	resp = e.getChat(e.keys, withPicture)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Equal(t, replacement.Value, resp.Metadata.GetProfilePicture().GetRenditions()[0].GetBlobId().GetValue())

	// And clearing it removes it.
	require.NoError(t, s.SetGroupPicture(e.ctx, withPicture, nil))
	resp = e.getChat(e.keys, withPicture)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Nil(t, resp.Metadata.GetProfilePicture())
}

func testServer_GetChat_Group_MembershipLifecycle(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	member := model.MustGenerateUserID()
	chatID := e.putGroup("", at(1), member)

	// An untitled group's metadata simply carries an empty title (display
	// fallbacks are a rendering concern). A fresh group's roster is at version
	// zero.
	resp := e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Empty(t, resp.Metadata.Title)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 2, Version: 0}, resp.Metadata.GetRosterSummary()))

	// A registered non-member gets the group's record, with no member hydrated:
	// they are not on the roster (see testServer_GetChat_Group_NonMember for
	// what else their standing decides).
	strangerID := model.MustGenerateUserID()
	strangerKeys := model.MustGenerateKeyPair()
	e.authz.Add(strangerID, strangerKeys)
	resp = e.getChat(strangerKeys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Empty(t, resp.Metadata.Members)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 2, Version: 0}, resp.Metadata.GetRosterSummary()))

	// A removed member is a non-member — the tombstone is not membership — and
	// is shown the group as one...
	_, _, err := s.RemoveGroupMember(e.ctx, chatID, e.userID, false)
	require.NoError(t, err)
	resp = e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Empty(t, resp.Metadata.Members)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 1, Version: 1}, resp.Metadata.GetRosterSummary()))

	// ...and a rejoin restores their membership. The count is back where it
	// started; the version records both transitions, which is what tells a
	// client holding the original member list that it is stale.
	_, _, err = s.AddGroupMembers(e.ctx, chatID, []*commonpb.UserId{e.userID})
	require.NoError(t, err)
	resp = e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Len(t, resp.Metadata.Members, 1)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 2, Version: 2}, resp.Metadata.GetRosterSummary()))
}

// testServer_GetChat_Group_NonMember pins what a group's record shows a
// registered user who is not on its roster, and how the listener rules decide
// the rest (see chat.Server.GetChat).
func testServer_GetChat_Group_NonMember(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	const requirement = 100
	pictureBlobID := &blobpb.BlobId{Value: []byte("group-picture-01")}
	wantPicture := e.media.setRenditions(pictureBlobID)
	founder := model.MustGenerateUserID()
	group := &chat.Chat{
		ID:                     chat.MustGenerateGroupChatID(),
		Type:                   chatpb.ChatType_GROUP,
		Members:                []*commonpb.UserId{founder},
		Title:                  "Whales",
		ProfilePictureBlobID:   pictureBlobID,
		MinimumListenerBalance: &chat.MinimumBalance{Currency: "usd", NativeAmount: requirement},
		LastActivity:           at(1),
		LastMessageID:          &messagingpb.MessageId{Value: 7},
	}
	require.NoError(t, s.PutChat(e.ctx, group))
	key := string(group.ID.Value)
	e.messaging.lastMessages[key] = textMessage(7, founder, "hi")
	e.messaging.latestEventSeqs[key] = 9
	// A pointer row stored for the viewer — as a former member's would be —
	// which a non-member must never be shown as theirs.
	e.messaging.pointers[key] = []*messagingpb.Pointer{
		{Type: messagingpb.Pointer_READ, UserId: e.userID, Value: &messagingpb.MessageId{Value: 3}, Ts: timestamppb.New(at(3))},
	}

	// With no owner account the env user holds nothing and fails the rules.
	// They still get the record — what identifies the group and what it takes
	// to join — but none of its messaging state, and no member. The pointers
	// are not so much as read.
	resp := e.getChat(e.keys, group.ID)
	require.Zero(t, e.messaging.pointerLookups[key])
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	md := resp.Metadata
	require.Equal(t, group.ID.Value, md.ChatId.Value)
	require.Equal(t, chatpb.ChatType_GROUP, md.Type)
	require.Equal(t, "Whales", md.Title)
	require.True(t, md.LastActivity.AsTime().Equal(at(1)))
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 1, Version: 0}, md.GetRosterSummary()))
	require.Len(t, md.GetRules().GetListener(), 1)
	require.Equal(t, float64(requirement), md.GetRules().GetListener()[0].GetMinimumBalance().GetAmount().GetNativeAmount())
	require.Len(t, md.GetProfilePicture().GetRenditions(), len(wantPicture))
	require.NotEmpty(t, md.GetProfilePicture().GetRenditions()[0].GetBlob().GetDownloadUrl().GetUrl())
	require.Empty(t, md.Members)
	require.Nil(t, md.LastMessage)
	require.Zero(t, md.LatestEventSequence)

	// Funded to the requirement they satisfy the rules: the messaging state
	// comes with the record. They are still not on the roster.
	_, err := e.accounts.Bind(e.ctx, e.userID, e.keys.Proto())
	require.NoError(t, err)
	e.ocpBalance.setBalance(e.keys.Proto(), ocp_common.ToCoreMintQuarks(requirement))
	resp = e.getChat(e.keys, group.ID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	md = resp.Metadata
	require.Equal(t, "Whales", md.Title)
	require.Empty(t, md.Members)
	require.Zero(t, e.messaging.pointerLookups[key])
	require.NotNil(t, md.LastMessage)
	require.Equal(t, uint64(7), md.LastMessage.MessageId.Value)
	require.Equal(t, uint64(9), md.LatestEventSequence)

	// Their admission is remembered for a while: drained, they still read
	// within the window (see chat.Access).
	e.ocpBalance.setBalance(e.keys.Proto(), 0)
	resp = e.getChat(e.keys, group.ID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.NotNil(t, resp.Metadata.LastMessage)

	// Joining puts them on the roster, and a member sees everything: their
	// hydrated entry, their pointer, and the messaging state.
	e.ocpBalance.setBalance(e.keys.Proto(), ocp_common.ToCoreMintQuarks(requirement))
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(e.keys, group.ID).Result)
	resp = e.getChat(e.keys, group.ID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	md = resp.Metadata
	require.Len(t, md.Members, 1)
	require.Equal(t, e.userID.Value, md.Members[0].UserId.Value)
	// One lookup by the join's own hydration, one by this read.
	require.Equal(t, 2, e.messaging.pointerLookups[key])
	require.Len(t, md.Members[0].Pointers, 1)
	require.Equal(t, uint64(3), md.Members[0].Pointers[0].Value.Value)
	require.Equal(t, uint64(7), md.LastMessage.MessageId.Value)
	require.Equal(t, uint64(9), md.LatestEventSequence)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 2, Version: 1}, md.GetRosterSummary()))

	// A group with no listener rules is open: a non-member, funded or not,
	// sees it as a qualifying non-member does, record and messaging state, but
	// is hydrated as no member (see chat.Access).
	strangerID, strangerKeys := e.addUser()
	_, err = e.accounts.Bind(e.ctx, strangerID, strangerKeys.Proto())
	require.NoError(t, err)
	open := &chat.Chat{
		ID:            chat.MustGenerateGroupChatID(),
		Type:          chatpb.ChatType_GROUP,
		Members:       []*commonpb.UserId{founder},
		Title:         "Legacy",
		LastActivity:  at(1),
		LastMessageID: &messagingpb.MessageId{Value: 2},
	}
	require.NoError(t, s.PutChat(e.ctx, open))
	e.messaging.lastMessages[string(open.ID.Value)] = textMessage(2, founder, "open to all")
	e.messaging.latestEventSeqs[string(open.ID.Value)] = 2
	resp = e.getChat(strangerKeys, open.ID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Equal(t, "Legacy", resp.Metadata.Title)
	require.Nil(t, resp.Metadata.Rules)
	require.Empty(t, resp.Metadata.Members)
	require.NotNil(t, resp.Metadata.LastMessage)
	require.False(t, resp.Metadata.LastMessage.Redacted)
	require.Equal(t, "open to all", resp.Metadata.LastMessage.Content[0].GetText().GetText())
	require.Equal(t, uint64(2), resp.Metadata.LatestEventSequence)

	// A DM is unchanged: a non-member is denied outright, funded or not.
	dm := e.putDM(at(1))
	resp = e.getChat(strangerKeys, dm)
	require.Equal(t, chatpb.GetChatResponse_DENIED, resp.Result)
	require.Nil(t, resp.Metadata)
}

// testServer_GetChat_ViewMode pins how the view mode a client asks for decides
// a group's messaging state (see chat.Server.GetChat): a non-member who fails
// the rules gets the last message redacted under FULL_OR_REDACTED where FULL
// withholds it, and REDACTED gives everyone who may read the group a
// placeholder — a member included — without evaluating the rules.
func testServer_GetChat_ViewMode(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	const requirement = 100
	founder := model.MustGenerateUserID()
	group := &chat.Chat{
		ID:                     chat.MustGenerateGroupChatID(),
		Type:                   chatpb.ChatType_GROUP,
		Members:                []*commonpb.UserId{founder},
		Title:                  "Whales",
		MinimumListenerBalance: &chat.MinimumBalance{Currency: "usd", NativeAmount: requirement},
		LastActivity:           at(1),
		LastMessageID:          &messagingpb.MessageId{Value: 7},
	}
	require.NoError(t, s.PutChat(e.ctx, group))
	key := string(group.ID.Value)
	const secret = "meet at the old bank at six"
	e.messaging.lastMessages[key] = textMessage(7, founder, secret)
	e.messaging.latestEventSeqs[key] = 9
	e.messaging.pointers[key] = []*messagingpb.Pointer{
		{Type: messagingpb.Pointer_READ, UserId: e.userID, Value: &messagingpb.MessageId{Value: 3}, Ts: timestamppb.New(at(3))},
	}

	// requireRedacted asserts a last message is the redacted copy of the
	// stored one: same identity and envelope, placeholder text, and the flag
	// set — and returns the placeholder for comparison across reads.
	requireRedacted := func(md *chatpb.Metadata) string {
		t.Helper()
		require.NotNil(t, md.LastMessage)
		require.True(t, md.LastMessage.Redacted)
		require.Equal(t, uint64(7), md.LastMessage.MessageId.Value)
		require.Equal(t, founder.Value, md.LastMessage.SenderId.Value)
		require.Equal(t, uint64(7), md.LastMessage.EventSequence)
		require.True(t, md.LastMessage.Ts.AsTime().Equal(at(7)))
		placeholder := md.LastMessage.Content[0].GetText().GetText()
		require.NotEmpty(t, placeholder)
		require.NotEqual(t, secret, placeholder)
		require.Equal(t, uint64(9), md.LatestEventSequence)
		return placeholder
	}

	// With no owner account the env user fails the rules. Under FULL they get
	// the record alone, as before; under FULL_OR_REDACTED and REDACTED the
	// messaging state comes redacted. They are still not on the roster, and
	// the pointer stored for them is still not read.
	resp := e.getChat(e.keys, group.ID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Nil(t, resp.Metadata.LastMessage)
	require.Zero(t, resp.Metadata.LatestEventSequence)

	resp = e.getChatWithMode(e.keys, group.ID, messagingpb.ViewMode_FULL_OR_REDACTED)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Equal(t, "Whales", resp.Metadata.Title)
	require.Len(t, resp.Metadata.GetRules().GetListener(), 1)
	require.Empty(t, resp.Metadata.Members)
	placeholder := requireRedacted(resp.Metadata)

	resp = e.getChatWithMode(e.keys, group.ID, messagingpb.ViewMode_REDACTED)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Empty(t, resp.Metadata.Members)
	require.Equal(t, placeholder, requireRedacted(resp.Metadata))
	require.Zero(t, e.messaging.pointerLookups[key])

	// The stored message is untouched by the redaction.
	require.Equal(t, secret, e.messaging.lastMessages[key].Content[0].GetText().GetText())
	require.False(t, e.messaging.lastMessages[key].Redacted)

	// Funded to the requirement they read in full under the modes that allow
	// it, and redacted under REDACTED, which does not ask the rules: drained
	// again, REDACTED still answers the moment the admission window closes —
	// here, with no window at all, on the very next read.
	_, err := e.accounts.Bind(e.ctx, e.userID, e.keys.Proto())
	require.NoError(t, err)
	e.ocpBalance.setBalance(e.keys.Proto(), ocp_common.ToCoreMintQuarks(requirement))
	resp = e.getChatWithMode(e.keys, group.ID, messagingpb.ViewMode_FULL_OR_REDACTED)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.NotNil(t, resp.Metadata.LastMessage)
	require.False(t, resp.Metadata.LastMessage.Redacted)
	require.Equal(t, secret, resp.Metadata.LastMessage.Content[0].GetText().GetText())
	resp = e.getChatWithMode(e.keys, group.ID, messagingpb.ViewMode_REDACTED)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Equal(t, placeholder, requireRedacted(resp.Metadata))

	// A member asking for REDACTED is still a member — hydrated, with their
	// pointer — and gets the placeholder they asked for.
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(e.keys, group.ID).Result)
	resp = e.getChatWithMode(e.keys, group.ID, messagingpb.ViewMode_REDACTED)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Len(t, resp.Metadata.Members, 1)
	require.Equal(t, e.userID.Value, resp.Metadata.Members[0].UserId.Value)
	require.Len(t, resp.Metadata.Members[0].Pointers, 1)
	require.Equal(t, placeholder, requireRedacted(resp.Metadata))
	resp = e.getChat(e.keys, group.ID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.False(t, resp.Metadata.LastMessage.Redacted)
	require.Equal(t, secret, resp.Metadata.LastMessage.Content[0].GetText().GetText())

	// A group with no listener rules is open: an unfunded non-member reads it
	// in full under FULL and FULL_OR_REDACTED and redacted under REDACTED,
	// exactly as a qualifying non-member reads a gated group. Its member gets
	// a placeholder on request like any other.
	strangerID, strangerKeys := e.addUser()
	_, err = e.accounts.Bind(e.ctx, strangerID, strangerKeys.Proto())
	require.NoError(t, err)
	open := &chat.Chat{
		ID:            chat.MustGenerateGroupChatID(),
		Type:          chatpb.ChatType_GROUP,
		Members:       []*commonpb.UserId{e.userID},
		Title:         "Legacy",
		LastActivity:  at(1),
		LastMessageID: &messagingpb.MessageId{Value: 2},
	}
	require.NoError(t, s.PutChat(e.ctx, open))
	e.messaging.lastMessages[string(open.ID.Value)] = textMessage(2, e.userID, "open to all")
	e.messaging.latestEventSeqs[string(open.ID.Value)] = 2
	for _, mode := range []messagingpb.ViewMode{messagingpb.ViewMode_FULL, messagingpb.ViewMode_FULL_OR_REDACTED, messagingpb.ViewMode_REDACTED} {
		resp = e.getChatWithMode(strangerKeys, open.ID, mode)
		require.Equal(t, chatpb.GetChatResponse_OK, resp.Result, mode)
		require.Equal(t, "Legacy", resp.Metadata.Title)
		require.Empty(t, resp.Metadata.Members, mode)
		require.NotNil(t, resp.Metadata.LastMessage, mode)
		require.Equal(t, uint64(2), resp.Metadata.LatestEventSequence, mode)
		redacted := mode == messagingpb.ViewMode_REDACTED
		require.Equal(t, redacted, resp.Metadata.LastMessage.Redacted, mode)
		require.Equal(t, !redacted, resp.Metadata.LastMessage.Content[0].GetText().GetText() == "open to all", mode)
	}
	resp = e.getChatWithMode(e.keys, open.ID, messagingpb.ViewMode_REDACTED)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.NotNil(t, resp.Metadata.LastMessage)
	require.True(t, resp.Metadata.LastMessage.Redacted)
	require.NotEqual(t, "open to all", resp.Metadata.LastMessage.Content[0].GetText().GetText())

	// A DM is unchanged: a non-member is denied outright under every mode, and
	// a member may ask for a placeholder.
	dm := generateDmChatID()
	require.NoError(t, s.PutChat(e.ctx, &chat.Chat{
		ID:            dm,
		Type:          chatpb.ChatType_CONTACT_DM,
		Members:       []*commonpb.UserId{e.userID, model.MustGenerateUserID()},
		LastActivity:  at(1),
		LastMessageID: &messagingpb.MessageId{Value: 1},
	}))
	e.messaging.lastMessages[string(dm.Value)] = textMessage(1, e.userID, "just us")
	for _, mode := range []messagingpb.ViewMode{messagingpb.ViewMode_FULL, messagingpb.ViewMode_FULL_OR_REDACTED, messagingpb.ViewMode_REDACTED} {
		resp = e.getChatWithMode(strangerKeys, dm, mode)
		require.Equal(t, chatpb.GetChatResponse_DENIED, resp.Result, mode)
		require.Nil(t, resp.Metadata)
	}
	resp = e.getChatWithMode(e.keys, dm, messagingpb.ViewMode_REDACTED)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.True(t, resp.Metadata.LastMessage.Redacted)
	require.NotEqual(t, "just us", resp.Metadata.LastMessage.Content[0].GetText().GetText())
}

// testServer_GetChat_Unauthenticated: a request with no auth gets a group's
// public view under REDACTED — what a registered non-member previewing it
// gets, with no per-viewer fields — and DENIED under any other mode or for a
// DM, whether or not the DM exists.
func testServer_GetChat_Unauthenticated(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	founder := model.MustGenerateUserID()
	group := &chat.Chat{
		ID:                     chat.MustGenerateGroupChatID(),
		Type:                   chatpb.ChatType_GROUP,
		Members:                []*commonpb.UserId{founder},
		Title:                  "Whales",
		MinimumListenerBalance: &chat.MinimumBalance{Currency: "usd", NativeAmount: 100},
		LastActivity:           at(1),
		LastMessageID:          &messagingpb.MessageId{Value: 7},
	}
	require.NoError(t, s.PutChat(e.ctx, group))
	key := string(group.ID.Value)
	const secret = "meet at the old bank at six"
	e.messaging.lastMessages[key] = textMessage(7, founder, secret)
	e.messaging.latestEventSeqs[key] = 9

	// The public view matches a registered non-member's REDACTED read, bar
	// the per-viewer fields, which it never carries.
	public := e.getPublicChat(group.ID, messagingpb.ViewMode_REDACTED)
	require.Equal(t, chatpb.GetChatResponse_OK, public.Result)
	md := public.Metadata
	require.Equal(t, "Whales", md.Title)
	require.Len(t, md.GetRules().GetListener(), 1)
	require.Empty(t, md.Members)
	require.Nil(t, md.ViewerState)
	require.False(t, md.IsHidden)
	require.NotNil(t, md.LastMessage)
	require.True(t, md.LastMessage.Redacted)
	require.NotEqual(t, secret, md.LastMessage.Content[0].GetText().GetText())
	require.Equal(t, uint64(9), md.LatestEventSequence)
	require.Zero(t, e.messaging.pointerLookups[key])

	registered := e.getChatWithMode(e.keys, group.ID, messagingpb.ViewMode_REDACTED)
	require.Equal(t, chatpb.GetChatResponse_OK, registered.Result)
	require.True(t, proto.Equal(registered.Metadata, md))

	// Any other mode is refused.
	for _, mode := range []messagingpb.ViewMode{messagingpb.ViewMode_FULL, messagingpb.ViewMode_FULL_OR_REDACTED} {
		resp := e.getPublicChat(group.ID, mode)
		require.Equal(t, chatpb.GetChatResponse_DENIED, resp.Result, mode)
		require.Nil(t, resp.Metadata)
	}

	// A group with no listener rules is open, so its public view is the
	// preview any public group's is: the record and its messaging state, the
	// last message redacted.
	open := &chat.Chat{
		ID:            chat.MustGenerateGroupChatID(),
		Type:          chatpb.ChatType_GROUP,
		Members:       []*commonpb.UserId{founder},
		Title:         "Legacy",
		LastActivity:  at(1),
		LastMessageID: &messagingpb.MessageId{Value: 2},
	}
	require.NoError(t, s.PutChat(e.ctx, open))
	e.messaging.lastMessages[string(open.ID.Value)] = textMessage(2, founder, "open to all")
	e.messaging.latestEventSeqs[string(open.ID.Value)] = 2
	resp := e.getPublicChat(open.ID, messagingpb.ViewMode_REDACTED)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Equal(t, "Legacy", resp.Metadata.Title)
	require.Empty(t, resp.Metadata.Members)
	require.NotNil(t, resp.Metadata.LastMessage)
	require.True(t, resp.Metadata.LastMessage.Redacted)
	require.NotEqual(t, "open to all", resp.Metadata.LastMessage.Content[0].GetText().GetText())
	require.Equal(t, uint64(2), resp.Metadata.LatestEventSequence)

	// A missing group is NOT_FOUND.
	resp = e.getPublicChat(chat.MustGenerateGroupChatID(), messagingpb.ViewMode_REDACTED)
	require.Equal(t, chatpb.GetChatResponse_NOT_FOUND, resp.Result)

	// A DM is DENIED under every mode, existing or not.
	dm := generateDmChatID()
	require.NoError(t, s.PutChat(e.ctx, &chat.Chat{
		ID:            dm,
		Type:          chatpb.ChatType_CONTACT_DM,
		Members:       []*commonpb.UserId{e.userID, model.MustGenerateUserID()},
		LastActivity:  at(1),
		LastMessageID: &messagingpb.MessageId{Value: 1},
	}))
	for _, id := range []*commonpb.ChatId{dm, generateDmChatID()} {
		for _, mode := range []messagingpb.ViewMode{messagingpb.ViewMode_FULL, messagingpb.ViewMode_FULL_OR_REDACTED, messagingpb.ViewMode_REDACTED} {
			resp = e.getPublicChat(id, mode)
			require.Equal(t, chatpb.GetChatResponse_DENIED, resp.Result, mode)
			require.Nil(t, resp.Metadata)
		}
	}
}

func testServer_GetDmChatFeed_TypeScoped(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	contactID := e.putDMOfType(chatpb.ChatType_CONTACT_DM, at(1))
	dmID := e.putDMOfType(chatpb.ChatType_DM, at(2))

	contactResp, err := e.getDmFeedOfType(chatpb.ChatType_CONTACT_DM, &commonpb.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, chatpb.GetDmChatFeedResponse_OK, contactResp.Result)
	require.Len(t, contactResp.Chats, 1)
	require.Equal(t, contactID.Value, contactResp.Chats[0].ChatId.Value)
	require.Equal(t, chatpb.ChatType_CONTACT_DM, contactResp.Chats[0].Type)

	dmResp, err := e.getDmFeedOfType(chatpb.ChatType_DM, &commonpb.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, chatpb.GetDmChatFeedResponse_OK, dmResp.Result)
	require.Len(t, dmResp.Chats, 1)
	require.Equal(t, dmID.Value, dmResp.Chats[0].ChatId.Value)
	require.Equal(t, chatpb.ChatType_DM, dmResp.Chats[0].Type)
}

func testServer_GetDmChatFeed_TokenBoundToType(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// Two contact DMs so the first page (size 1) yields a resumable token.
	e.putDMOfType(chatpb.ChatType_CONTACT_DM, at(1))
	e.putDMOfType(chatpb.ChatType_CONTACT_DM, at(2))

	resp, err := e.getDmFeedOfType(chatpb.ChatType_CONTACT_DM, &commonpb.QueryOptions{PageSize: 1})
	require.NoError(t, err)
	require.True(t, resp.HasMore)
	require.NotNil(t, resp.PagingToken)

	// The same token resumes its own feed...
	resumed, err := e.getDmFeedOfType(chatpb.ChatType_CONTACT_DM, &commonpb.QueryOptions{PageSize: 1, PagingToken: resp.PagingToken})
	require.NoError(t, err)
	require.Equal(t, chatpb.GetDmChatFeedResponse_OK, resumed.Result)
	require.Len(t, resumed.Chats, 1)

	// ...but is rejected against the other feed.
	_, err = e.getDmFeedOfType(chatpb.ChatType_DM, &commonpb.QueryOptions{PageSize: 1, PagingToken: resp.PagingToken})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func testServer_GetDmChatFeed_Hydrates(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// A chat with a last message, and one without.
	withMsg := generateDmChatID()
	peer := model.MustGenerateUserID()
	require.NoError(t, s.PutChat(e.ctx, &chat.Chat{
		ID:            withMsg,
		Type:          chatpb.ChatType_CONTACT_DM,
		Members:       []*commonpb.UserId{e.userID, peer},
		LastActivity:  at(2),
		LastMessageID: &messagingpb.MessageId{Value: 3},
	}))
	withoutMsg := e.putDM(at(1))

	e.messaging.lastMessages[string(withMsg.Value)] = textMessage(3, e.userID, "yo")
	e.messaging.latestEventSeqs[string(withMsg.Value)] = 3
	e.profiles.displayNames[string(e.userID.Value)] = "Env User"
	e.profiles.displayNames[string(peer.Value)] = "Peer Name"

	resp := e.getDmFeed(&commonpb.QueryOptions{})
	require.Equal(t, chatpb.GetDmChatFeedResponse_OK, resp.Result)
	require.Len(t, resp.Chats, 2)

	byChat := make(map[string]*chatpb.Metadata)
	for _, md := range resp.Chats {
		byChat[string(md.ChatId.Value)] = md
	}
	// The chat with a last message ID gets its message and head sequence hydrated...
	require.NotNil(t, byChat[string(withMsg.Value)].LastMessage)
	require.Equal(t, uint64(3), byChat[string(withMsg.Value)].LastMessage.MessageId.Value)
	require.Equal(t, uint64(3), byChat[string(withMsg.Value)].LatestEventSequence)
	// ...and the one without is left nil, its head defaulting to 0 (no ref is
	// issued for it).
	require.Nil(t, byChat[string(withoutMsg.Value)].LastMessage)
	require.Zero(t, byChat[string(withoutMsg.Value)].LatestEventSequence)

	// Display names are hydrated for every member of every chat on the page: the
	// single batched lookup spans chats, so the env user's name lands on both.
	require.Equal(t, "Peer Name", byUserID(byChat[string(withMsg.Value)].Members)[string(peer.Value)].UserProfile.DisplayName)
	for _, chatID := range []*commonpb.ChatId{withMsg, withoutMsg} {
		members := byUserID(byChat[string(chatID.Value)].Members)
		require.Equal(t, "Env User", members[string(e.userID.Value)].UserProfile.DisplayName)
	}
}

func testServer_GetGroupChatFeed_Empty(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// A DM is not a group: the feed is empty with it.
	e.putDM(at(1))

	resp := e.mustGetGroupFeed(&commonpb.QueryOptions{})
	require.Empty(t, resp.Chats)
	require.False(t, resp.HasMore)
	require.Nil(t, resp.PagingToken)
}

func testServer_GetGroupChatFeed_OrderAndContent(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	memberB := model.MustGenerateUserID()
	e.profiles.displayNames[string(e.userID.Value)] = "Viewer"

	// Persist out of order; the feed must return most-recent activity first.
	// A DM and a group the viewer is not in must not appear.
	older := e.putGroup("Older", at(1), memberB)
	newer := e.putGroup("Newer", at(2), memberB)
	e.putDM(at(3))
	_ = putGroupChat(t, s, "Not Mine", at(4), memberB)

	resp := e.mustGetGroupFeed(&commonpb.QueryOptions{})
	require.False(t, resp.HasMore)
	require.NotNil(t, resp.PagingToken)
	require.Len(t, resp.Chats, 2)
	require.Equal(t, newer.Value, resp.Chats[0].ChatId.Value)
	require.Equal(t, older.Value, resp.Chats[1].ChatId.Value)

	// Each entry carries the group's metadata, its roster summary, and the
	// viewer as its only member, as GetChat returns it.
	first := resp.Chats[0]
	require.Equal(t, chatpb.ChatType_GROUP, first.Type)
	require.Equal(t, "Newer", first.Title)
	require.True(t, first.LastActivity.AsTime().Equal(at(2)))
	require.False(t, first.IsHidden)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 2, Version: 0}, first.GetRosterSummary()))
	require.Len(t, first.Members, 1)
	require.Equal(t, e.userID.Value, first.Members[0].UserId.Value)
	require.Equal(t, "Viewer", first.Members[0].UserProfile.DisplayName)
	require.Nil(t, first.Members[0].UserProfile.PhoneNumber)
}

func testServer_GetGroupChatFeed_Paging(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	const total = 5
	want := make([][]byte, total)
	for i := 0; i < total; i++ {
		// Increasing activity, so DESC order is the reverse of insertion order.
		chatID := e.putGroup("", at(int64(i+1)), model.MustGenerateUserID())
		want[total-1-i] = chatID.Value
	}

	var got [][]byte
	var token *commonpb.PagingToken
	for {
		resp := e.mustGetGroupFeed(&commonpb.QueryOptions{PageSize: 2, PagingToken: token})
		require.LessOrEqual(t, len(resp.Chats), 2)
		for _, c := range resp.Chats {
			got = append(got, c.ChatId.Value)
		}
		if !resp.HasMore {
			break
		}
		require.NotNil(t, resp.PagingToken)
		token = resp.PagingToken
	}

	require.Equal(t, want, got)
}

func testServer_GetGroupChatFeed_Hydrates(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// A group with a last message and a picture, and one with neither.
	pictureBlobID := &blobpb.BlobId{Value: []byte("group-picture-01")}
	renditions := e.media.setRenditions(pictureBlobID)
	withMsg := chat.MustGenerateGroupChatID()
	require.NoError(t, s.PutChat(e.ctx, &chat.Chat{
		ID:                   withMsg,
		Type:                 chatpb.ChatType_GROUP,
		Members:              []*commonpb.UserId{e.userID, model.MustGenerateUserID()},
		Title:                "With Message",
		ProfilePictureBlobID: pictureBlobID,
		LastActivity:         at(2),
		LastMessageID:        &messagingpb.MessageId{Value: 3},
	}))
	withoutMsg := e.putGroup("Without", at(1), model.MustGenerateUserID())

	e.messaging.lastMessages[string(withMsg.Value)] = textMessage(3, e.userID, "yo")
	e.messaging.latestEventSeqs[string(withMsg.Value)] = 3
	e.messaging.pointers[string(withMsg.Value)] = []*messagingpb.Pointer{
		{Type: messagingpb.Pointer_READ, UserId: e.userID, Value: &messagingpb.MessageId{Value: 2}, Ts: timestamppb.New(at(2))},
	}

	resp := e.mustGetGroupFeed(&commonpb.QueryOptions{})
	require.Len(t, resp.Chats, 2)
	byChat := make(map[string]*chatpb.Metadata)
	for _, md := range resp.Chats {
		byChat[string(md.ChatId.Value)] = md
	}

	hydrated := byChat[string(withMsg.Value)]
	require.NotNil(t, hydrated.LastMessage)
	require.Equal(t, uint64(3), hydrated.LastMessage.MessageId.Value)
	require.Equal(t, uint64(3), hydrated.LatestEventSequence)
	require.NotNil(t, hydrated.ProfilePicture)
	require.Len(t, hydrated.ProfilePicture.Renditions, len(renditions))
	require.NotNil(t, hydrated.ProfilePicture.Renditions[0].Blob)

	bare := byChat[string(withoutMsg.Value)]
	require.Nil(t, bare.LastMessage)
	require.Zero(t, bare.LatestEventSequence)
	require.Nil(t, bare.ProfilePicture)

	// The viewer's own pointers ride on their member entry; a group with none
	// stored leaves the entry without any.
	require.Len(t, hydrated.Members, 1)
	require.Len(t, hydrated.Members[0].Pointers, 1)
	require.Equal(t, uint64(2), hydrated.Members[0].Pointers[0].Value.Value)
	require.Len(t, bare.Members, 1)
	require.Empty(t, bare.Members[0].Pointers)
}

// testServer_GetGroupChatFeed_SnapshotPinned verifies the DM feed's snapshot
// contract holds for groups: a group that becomes active after the first page
// leaves the window and is not paginated, so the multi-page read stays
// internally consistent and the group's freshness is the stream's job.
func testServer_GetGroupChatFeed_SnapshotPinned(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	g1 := e.putGroup("", at(1), model.MustGenerateUserID())
	g2 := e.putGroup("", at(2), model.MustGenerateUserID())
	g3 := e.putGroup("", at(3), model.MustGenerateUserID())

	page1 := e.mustGetGroupFeed(&commonpb.QueryOptions{PageSize: 1})
	require.Equal(t, [][]byte{g3.Value}, metadataChatIDs(page1.Chats))
	require.True(t, page1.HasMore)

	// g1, not yet paged, becomes active now — after the snapshot the first
	// request pinned.
	advanced, _, err := s.AdvanceLastMessage(e.ctx, g1, &messagingpb.MessageId{Value: 1}, time.Now().UTC().Add(time.Hour))
	require.NoError(t, err)
	require.True(t, advanced)

	page2 := e.mustGetGroupFeed(&commonpb.QueryOptions{PageSize: 10, PagingToken: page1.PagingToken})
	require.Equal(t, [][]byte{g2.Value}, metadataChatIDs(page2.Chats))
	require.False(t, page2.HasMore)
}

// testServer_GetGroupChatFeed_DropsDepartedBetweenPages verifies that the paging
// token is a hint, not an authorization: a group the caller leaves between
// pages — which the token still names — is dropped from the page it would have
// been on.
func testServer_GetGroupChatFeed_DropsDepartedBetweenPages(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	g1 := e.putGroup("", at(1), model.MustGenerateUserID())
	g2 := e.putGroup("", at(2), model.MustGenerateUserID())
	g3 := e.putGroup("", at(3), model.MustGenerateUserID())

	page1 := e.mustGetGroupFeed(&commonpb.QueryOptions{PageSize: 1})
	require.Equal(t, [][]byte{g3.Value}, metadataChatIDs(page1.Chats))
	require.True(t, page1.HasMore)

	_, _, err := s.RemoveGroupMember(e.ctx, g2, e.userID, false)
	require.NoError(t, err)

	page2 := e.mustGetGroupFeed(&commonpb.QueryOptions{PageSize: 10, PagingToken: page1.PagingToken})
	require.Equal(t, [][]byte{g1.Value}, metadataChatIDs(page2.Chats))
	require.False(t, page2.HasMore)
}

func testServer_GetGroupChatFeed_InvalidToken(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	e.putGroup("", at(1), model.MustGenerateUserID())
	e.putDMOfType(chatpb.ChatType_CONTACT_DM, at(1))
	e.putDMOfType(chatpb.ChatType_CONTACT_DM, at(2))

	// A fabricated token is rejected.
	_, err := e.getGroupFeed(&commonpb.QueryOptions{PagingToken: &commonpb.PagingToken{Value: []byte("not a token")}})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// So is a DM feed token: the two feeds' tokens are distinct layouts.
	dmResp, err := e.getDmFeedOfType(chatpb.ChatType_CONTACT_DM, &commonpb.QueryOptions{PageSize: 1})
	require.NoError(t, err)
	require.NotNil(t, dmResp.PagingToken)
	_, err = e.getGroupFeed(&commonpb.QueryOptions{PagingToken: dmResp.PagingToken})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func metadataChatIDs(chats []*chatpb.Metadata) [][]byte {
	out := make([][]byte, len(chats))
	for i, c := range chats {
		out[i] = c.ChatId.Value
	}
	return out
}

func textMessage(id uint64, sender *commonpb.UserId, text string) *messagingpb.Message {
	return &messagingpb.Message{
		MessageId: &messagingpb.MessageId{Value: id},
		SenderId:  sender,
		Content: []*messagingpb.Content{{
			Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: text}},
		}},
		Ts:            timestamppb.New(at(int64(id))),
		EventSequence: id,
	}
}

func byUserID(members []*chatpb.Member) map[string]*chatpb.Member {
	out := make(map[string]*chatpb.Member, len(members))
	for _, m := range members {
		out[string(m.UserId.Value)] = m
	}
	return out
}

func testServer_JoinChat_OK(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	e.profiles.displayNames[string(e.userID.Value)] = "Joiner"
	e.profiles.joinedAt[string(e.userID.Value)] = at(5)

	// A group the env user is not in, with two existing members.
	founder := model.MustGenerateUserID()
	group := putGroupChat(t, s, "Open Group", at(1), founder, model.MustGenerateUserID())

	// Before joining, the group's record is visible but the joiner is not on it.
	before := e.getChat(e.keys, group.ID)
	require.Equal(t, chatpb.GetChatResponse_OK, before.Result)
	require.Empty(t, before.Metadata.Members)

	resp := e.mustJoinChat(e.keys, group.ID)
	require.Equal(t, chatpb.JoinChatResponse_OK, resp.Result)

	// The response carries the chat as the joiner sees it: the group's
	// metadata, the joiner as its only hydrated member, and the roster after
	// the join — one transition on from creation.
	md := resp.Chat
	require.NotNil(t, md)
	require.Equal(t, group.ID.Value, md.ChatId.Value)
	require.Equal(t, chatpb.ChatType_GROUP, md.Type)
	require.Equal(t, "Open Group", md.Title)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 3, Version: 1}, md.GetRosterSummary()))
	require.Len(t, md.Members, 1)
	require.Equal(t, e.userID.Value, md.Members[0].UserId.Value)
	require.Equal(t, "Joiner", md.Members[0].UserProfile.DisplayName)

	// The joiner's own entry in the metadata carries no record fields; the
	// announcement does (below).
	require.Nil(t, md.Members[0].JoinedAt)
	require.Zero(t, md.Members[0].Version)

	// The join has landed in the store...
	isMember, err := s.IsMember(e.ctx, group.ID, e.userID)
	require.NoError(t, err)
	require.True(t, isMember)
	require.Equal(t, chatpb.GetChatResponse_OK, e.getChat(e.keys, group.ID).Result)

	// ...and been announced. The chat's topic is told only that the roster
	// moved, naming no one, with the joiner excluded, since their streams are
	// not on the topic yet.
	e.waitForRosterUpdates(e.userID, group.ID, 1)
	toMembers, excludes := e.rosterUpdatesOnChatTopic(group.ID)
	require.Len(t, toMembers, 1)
	require.Len(t, excludes[0], 1)
	require.Equal(t, e.userID.Value, excludes[0][0].Value)
	require.NotNil(t, toMembers[0].GetMembershipChanged())
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 3, Version: 1}, toMembers[0].GetRosterSummary()))

	// The joiner's own topic carries their hydrated member entry — profile, no
	// pointers — plus the full metadata, so their other devices insert the
	// chat without a refetch.
	toJoiner := e.rosterUpdatesOnUserTopic(e.userID, group.ID)
	require.Len(t, toJoiner, 1)
	joinedSelf := toJoiner[0].GetMemberJoined()
	require.NotNil(t, joinedSelf)
	require.Equal(t, e.userID.Value, joinedSelf.Member.UserId.Value)
	require.Equal(t, "Joiner", joinedSelf.Member.UserProfile.DisplayName)
	require.True(t, joinedSelf.Member.UserProfile.JoinTs.AsTime().Equal(at(5)))
	require.Empty(t, joinedSelf.Member.Pointers)
	// The announced member is the record the join wrote: the version the
	// roster moved to, and the join time as stored.
	require.EqualValues(t, 1, joinedSelf.Member.Version)
	require.NotNil(t, joinedSelf.Member.JoinedAt)
	records, err := s.GetGroupMemberRecords(e.ctx, e.userID, []*commonpb.ChatId{group.ID})
	require.NoError(t, err)
	require.True(t, joinedSelf.Member.JoinedAt.AsTime().Equal(records[string(group.ID.Value)].JoinedAt))
	require.NotNil(t, joinedSelf.Metadata)
	require.Equal(t, group.ID.Value, joinedSelf.Metadata.ChatId.Value)
	require.Equal(t, "Open Group", joinedSelf.Metadata.Title)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 3, Version: 1}, joinedSelf.Metadata.GetRosterSummary()))
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 3, Version: 1}, toJoiner[0].GetRosterSummary()))

	// The founder's own view was never touched: no event on their user topic.
	require.Empty(t, e.rosterUpdatesOnUserTopic(founder, group.ID))
}

func testServer_JoinChat_Idempotent(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	group := putGroupChat(t, s, "Open Group", at(1), model.MustGenerateUserID())

	first := e.mustJoinChat(e.keys, group.ID)
	require.Equal(t, chatpb.JoinChatResponse_OK, first.Result)
	e.waitForRosterUpdates(e.userID, group.ID, 1)

	// Joining again is a no-op: OK with the same metadata, no transition, and
	// nothing announced.
	again := e.mustJoinChat(e.keys, group.ID)
	require.Equal(t, chatpb.JoinChatResponse_OK, again.Result)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 2, Version: 1}, again.Chat.GetRosterSummary()))
	require.Equal(t, first.Chat.ChatId.Value, again.Chat.ChatId.Value)

	time.Sleep(100 * time.Millisecond)
	toMembers, _ := e.rosterUpdatesOnChatTopic(group.ID)
	require.Len(t, toMembers, 1)
	require.Len(t, e.rosterUpdatesOnUserTopic(e.userID, group.ID), 1)
}

func testServer_JoinChat_NotFound(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	unknown := chat.MustGenerateGroupChatID()
	resp := e.mustJoinChat(e.keys, unknown)
	require.Equal(t, chatpb.JoinChatResponse_NOT_FOUND, resp.Result)
	require.Nil(t, resp.Chat)
	e.requireNoRosterUpdates(e.userID, unknown)
}

func testServer_JoinChat_DeniedForDm(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// A DM's roster is fixed: neither a participant nor a stranger can join
	// one, and the DM is left as it was.
	mine := e.putDM(at(1))
	stranger := putDmChat(t, s, model.MustGenerateUserID(), model.MustGenerateUserID(), at(1))
	for _, dm := range []*commonpb.ChatId{mine, stranger.ID} {
		resp := e.mustJoinChat(e.keys, dm)
		require.Equal(t, chatpb.JoinChatResponse_DENIED, resp.Result)
		require.Nil(t, resp.Chat)
		e.requireNoRosterUpdates(e.userID, dm)
	}
	members, err := s.GetMembers(e.ctx, stranger.ID)
	require.NoError(t, err)
	require.Len(t, members, 2)
}

func testServer_JoinChat_StaffRule(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	group := &chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{model.MustGenerateUserID()},
		Title:        "Staff Room",
		IsStaffOnly:  true,
		LastActivity: at(1),
	}
	require.NoError(t, s.PutChat(e.ctx, group))

	// A non-staff user is refused, and does not become a member.
	resp := e.mustJoinChat(e.keys, group.ID)
	require.Equal(t, chatpb.JoinChatResponse_RULES_NOT_SATISFIED, resp.Result)
	require.Nil(t, resp.Chat)
	isMember, err := s.IsMember(e.ctx, group.ID, e.userID)
	require.NoError(t, err)
	require.False(t, isMember)
	e.requireNoRosterUpdates(e.userID, group.ID)

	// Flagged as staff, the same user is admitted; the metadata they get back
	// shows the rule they satisfied.
	e.accounts.setStaff(e.userID, true)
	resp = e.mustJoinChat(e.keys, group.ID)
	require.Equal(t, chatpb.JoinChatResponse_OK, resp.Result)
	require.Len(t, resp.Chat.GetRules().GetListener(), 1)
	require.NotNil(t, resp.Chat.GetRules().GetListener()[0].GetStaff())
	e.waitForRosterUpdates(e.userID, group.ID, 1)
}

func testServer_JoinChat_MinimumBalanceRule(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	const requirement = 100
	group := &chat.Chat{
		ID:                     chat.MustGenerateGroupChatID(),
		Type:                   chatpb.ChatType_GROUP,
		Members:                []*commonpb.UserId{model.MustGenerateUserID()},
		Title:                  "Whales",
		MinimumListenerBalance: &chat.MinimumBalance{Currency: "usd", NativeAmount: requirement},
		LastActivity:           at(1),
	}
	require.NoError(t, s.PutChat(e.ctx, group))

	// With no owner account at all the user holds nothing, and is refused —
	// not failed.
	resp := e.mustJoinChat(e.keys, group.ID)
	require.Equal(t, chatpb.JoinChatResponse_RULES_NOT_SATISFIED, resp.Result)

	// A cent short is still short.
	_, err := e.accounts.Bind(e.ctx, e.userID, e.keys.Proto())
	require.NoError(t, err)
	e.ocpBalance.setBalance(e.keys.Proto(), ocp_common.ToCoreMintQuarks(requirement)-ocp_common.CoreMintQuarksPerUnit/100)
	resp = e.mustJoinChat(e.keys, group.ID)
	require.Equal(t, chatpb.JoinChatResponse_RULES_NOT_SATISFIED, resp.Result)
	e.requireNoRosterUpdates(e.userID, group.ID)

	// At the requirement exactly, the user is admitted.
	e.ocpBalance.setBalance(e.keys.Proto(), ocp_common.ToCoreMintQuarks(requirement))
	resp = e.mustJoinChat(e.keys, group.ID)
	require.Equal(t, chatpb.JoinChatResponse_OK, resp.Result)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 2, Version: 1}, resp.Chat.GetRosterSummary()))
	e.waitForRosterUpdates(e.userID, group.ID, 1)
}

// testServer_JoinChat_FiatMinimumBalanceRule is
// testServer_JoinChat_MinimumBalanceRule for a requirement in a currency other
// than USD, answered by OCP's valuation of the user's holdings in it.
func testServer_JoinChat_FiatMinimumBalanceRule(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// 150 JPY per USDF, so the requirement is 100 USDF.
	const requirement = 15_000
	e.ocpBalance.setRate("jpy", 150)
	group := &chat.Chat{
		ID:                     chat.MustGenerateGroupChatID(),
		Type:                   chatpb.ChatType_GROUP,
		Members:                []*commonpb.UserId{model.MustGenerateUserID()},
		Title:                  "Yen Whales",
		MinimumListenerBalance: &chat.MinimumBalance{Currency: "jpy", NativeAmount: requirement},
		LastActivity:           at(1),
	}
	require.NoError(t, s.PutChat(e.ctx, group))

	// With no owner account at all the user holds nothing, and is refused —
	// not failed.
	resp := e.mustJoinChat(e.keys, group.ID)
	require.Equal(t, chatpb.JoinChatResponse_RULES_NOT_SATISFIED, resp.Result)

	// More than half a yen short is short: the valuation is a quoted rate
	// applied to quarks, so that much slack is allowed and no more.
	_, err := e.accounts.Bind(e.ctx, e.userID, e.keys.Proto())
	require.NoError(t, err)
	e.ocpBalance.setBalance(e.keys.Proto(), ocp_common.ToCoreMintQuarks(100)-4_000)
	resp = e.mustJoinChat(e.keys, group.ID)
	require.Equal(t, chatpb.JoinChatResponse_RULES_NOT_SATISFIED, resp.Result)
	e.requireNoRosterUpdates(e.userID, group.ID)

	// Within it, the user is admitted; the metadata they get back shows the
	// rule as stored.
	e.ocpBalance.setBalance(e.keys.Proto(), ocp_common.ToCoreMintQuarks(100)-3_000)
	resp = e.mustJoinChat(e.keys, group.ID)
	require.Equal(t, chatpb.JoinChatResponse_OK, resp.Result)
	require.Equal(t, "jpy", resp.Chat.GetRules().GetListener()[0].GetMinimumBalance().GetAmount().GetCurrency())
	require.Equal(t, float64(requirement), resp.Chat.GetRules().GetListener()[0].GetMinimumBalance().GetAmount().GetNativeAmount())
	e.waitForRosterUpdates(e.userID, group.ID, 1)
}

// testServer_JoinChat_MemberSkipsRules pins that a current member re-joining
// is answered on membership alone: a member who no longer satisfies the
// group's rules keeps their membership, and a retried join says so.
func testServer_JoinChat_MemberSkipsRules(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	group := &chat.Chat{
		ID:           chat.MustGenerateGroupChatID(),
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{e.userID},
		Title:        "Staff Room",
		IsStaffOnly:  true,
		LastActivity: at(1),
	}
	require.NoError(t, s.PutChat(e.ctx, group))

	// The env user is not staff, yet is a member: the join is a no-op OK.
	resp := e.mustJoinChat(e.keys, group.ID)
	require.Equal(t, chatpb.JoinChatResponse_OK, resp.Result)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 1, Version: 0}, resp.Chat.GetRosterSummary()))
	e.requireNoRosterUpdates(e.userID, group.ID)
}

// testServer_JoinChat_UnevaluableRuleFails pins that a rule the server cannot
// evaluate — here a minimum balance in a currency OCP cannot value — fails the
// join rather than admitting the user: a restriction the server cannot answer
// admits no one.
func testServer_JoinChat_UnevaluableRuleFails(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	group := &chat.Chat{
		ID:                     chat.MustGenerateGroupChatID(),
		Type:                   chatpb.ChatType_GROUP,
		Members:                []*commonpb.UserId{model.MustGenerateUserID()},
		Title:                  "Unpriced",
		MinimumListenerBalance: &chat.MinimumBalance{Currency: "xyz", NativeAmount: 1},
		LastActivity:           at(1),
	}
	require.NoError(t, s.PutChat(e.ctx, group))

	e.fundEnvUser(1)
	_, err := e.joinChat(e.keys, group.ID)
	require.Equal(t, codes.Internal, status.Code(err))
	isMember, err := s.IsMember(e.ctx, group.ID, e.userID)
	require.NoError(t, err)
	require.False(t, isMember)
	e.requireNoRosterUpdates(e.userID, group.ID)
}

func testServer_LeaveChat_OK(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	other := model.MustGenerateUserID()
	chatID := e.putGroup("Group", at(1), other)

	resp := e.mustLeaveChat(e.keys, chatID)
	require.Equal(t, chatpb.LeaveChatResponse_OK, resp.Result)

	// The departure has landed: the caller is no longer a member, and is shown
	// the group as a non-member, while the other member is untouched.
	isMember, err := s.IsMember(e.ctx, chatID, e.userID)
	require.NoError(t, err)
	require.False(t, isMember)
	after := e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, after.Result)
	require.Empty(t, after.Metadata.Members)
	members, err := s.GetMembers(e.ctx, chatID)
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Equal(t, other.Value, members[0].Value)

	// The departure is announced on the chat's topic with the leaver excluded
	// — their stream may still be on it — naming no one, and on the leaver's
	// own topic, so every device they have open drops the chat.
	e.waitForRosterUpdates(e.userID, chatID, 1)
	toMembers, excludes := e.rosterUpdatesOnChatTopic(chatID)
	require.Len(t, toMembers, 1)
	require.Len(t, excludes[0], 1)
	require.Equal(t, e.userID.Value, excludes[0][0].Value)
	require.NotNil(t, toMembers[0].GetMembershipChanged())
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 1, Version: 1}, toMembers[0].GetRosterSummary()))

	toLeaver := e.rosterUpdatesOnUserTopic(e.userID, chatID)
	require.Len(t, toLeaver, 1)
	require.NotNil(t, toLeaver[0].GetMemberLeft())
	require.Equal(t, e.userID.Value, toLeaver[0].GetMemberLeft().UserId.Value)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 1, Version: 1}, toLeaver[0].GetRosterSummary()))
	require.Empty(t, e.rosterUpdatesOnUserTopic(other, chatID))
}

func testServer_LeaveChat_Idempotent(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	chatID := e.putGroup("Group", at(1), model.MustGenerateUserID())

	// Leaving a group the caller was never in already holds: OK, no
	// transition, nothing announced.
	stranger, strangerKeys := e.addUser()
	resp := e.mustLeaveChat(strangerKeys, chatID)
	require.Equal(t, chatpb.LeaveChatResponse_OK, resp.Result)
	e.requireNoRosterUpdates(stranger, chatID)
	roster, err := s.GetGroupRosterSummary(e.ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, chat.RosterSummary{MemberCount: 2, Version: 0}, roster)

	// So does leaving twice: the second call finds the departure already made.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, chatID).Result)
	e.waitForRosterUpdates(e.userID, chatID, 1)
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, chatID).Result)
	time.Sleep(100 * time.Millisecond)
	toMembers, _ := e.rosterUpdatesOnChatTopic(chatID)
	require.Len(t, toMembers, 1)
	require.Len(t, e.rosterUpdatesOnUserTopic(e.userID, chatID), 1)
	roster, err = s.GetGroupRosterSummary(e.ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, chat.RosterSummary{MemberCount: 1, Version: 1}, roster)
}

func testServer_LeaveChat_NotFound(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	unknown := chat.MustGenerateGroupChatID()
	resp := e.mustLeaveChat(e.keys, unknown)
	require.Equal(t, chatpb.LeaveChatResponse_NOT_FOUND, resp.Result)
	e.requireNoRosterUpdates(e.userID, unknown)
}

func testServer_LeaveChat_DeniedForDm(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// A DM cannot be left, by a participant or anyone else, and is left as it
	// was.
	mine := e.putDM(at(1))
	stranger := putDmChat(t, s, model.MustGenerateUserID(), model.MustGenerateUserID(), at(1))
	for _, dm := range []*commonpb.ChatId{mine, stranger.ID} {
		resp := e.mustLeaveChat(e.keys, dm)
		require.Equal(t, chatpb.LeaveChatResponse_DENIED, resp.Result)
		e.requireNoRosterUpdates(e.userID, dm)
	}
	isMember, err := s.IsMember(e.ctx, mine, e.userID)
	require.NoError(t, err)
	require.True(t, isMember)
}

// testServer_LeaveChat_ThenRejoin pins the round trip: a departure is a
// tombstone the user can come back from, and the roster's version records
// every transition along the way.
func testServer_LeaveChat_ThenRejoin(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	chatID := e.putGroup("Group", at(1), model.MustGenerateUserID())

	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, chatID).Result)
	require.Empty(t, e.getChat(e.keys, chatID).Metadata.Members)

	resp := e.mustJoinChat(e.keys, chatID)
	require.Equal(t, chatpb.JoinChatResponse_OK, resp.Result)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 2, Version: 2}, resp.Chat.GetRosterSummary()))
	require.Len(t, e.getChat(e.keys, chatID).Metadata.Members, 1)

	// Two transitions, two announcements on each topic, in order: named on
	// the user's own, anonymous on the chat's.
	e.waitForRosterUpdates(e.userID, chatID, 2)
	toMembers, _ := e.rosterUpdatesOnChatTopic(chatID)
	require.Len(t, toMembers, 2)
	require.NotNil(t, toMembers[0].GetMembershipChanged())
	require.Equal(t, uint64(1), toMembers[0].GetRosterSummary().GetVersion())
	require.NotNil(t, toMembers[1].GetMembershipChanged())
	require.Equal(t, uint64(2), toMembers[1].GetRosterSummary().GetVersion())
	toSelf := e.rosterUpdatesOnUserTopic(e.userID, chatID)
	require.Len(t, toSelf, 2)
	require.NotNil(t, toSelf[0].GetMemberLeft())
	require.NotNil(t, toSelf[1].GetMemberJoined())
}

// fakeModerator is a canned moderation.Client for server tests. Only the two
// classifiers a chat title or description runs through do anything; the rest
// satisfy the interface and never flag.
type fakeModerator struct {
	textFlagged    bool
	textCategories []string
	textErr        error

	// flaggedTexts flags the texts it names alone, with their categories, and
	// textErrs fails them alone, so a test can refuse a description and not
	// the title sent beside it.
	flaggedTexts map[string][]string
	textErrs     map[string]error
	// classifiedTexts records every text ClassifyText saw, in order.
	classifiedTexts []string

	titleFlagged    bool
	titleCategories []string
	titleErr        error

	// classifiedTitle records the title ClassifyGroupTitle last saw, so a test
	// can check what reached the classifier.
	classifiedTitle string
}

func (m *fakeModerator) ClassifyText(_ context.Context, text string) (*moderation.Result, error) {
	m.classifiedTexts = append(m.classifiedTexts, text)
	if err, ok := m.textErrs[text]; ok {
		return nil, err
	}
	if categories, ok := m.flaggedTexts[text]; ok {
		return fakeModerationResult(true, categories, nil)
	}
	return fakeModerationResult(m.textFlagged, m.textCategories, m.textErr)
}

func (m *fakeModerator) ClassifyGroupTitle(_ context.Context, title string) (*moderation.Result, error) {
	m.classifiedTitle = title
	return fakeModerationResult(m.titleFlagged, m.titleCategories, m.titleErr)
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

// fakeModerationResult builds a result whose category scores rise in the order
// the categories are given, so the last one is the highest.
func fakeModerationResult(flagged bool, categories []string, err error) (*moderation.Result, error) {
	if err != nil {
		return nil, err
	}
	result := &moderation.Result{Flagged: flagged}
	if len(categories) > 0 {
		result.FlaggedCategories = categories
		result.CategoryScores = make(map[string]float64, len(categories))
		for i, category := range categories {
			result.CategoryScores[category] = float64(i + 1)
		}
	}
	return result, nil
}

// newIdempotencyKey mints a fresh StartChat key, as a client does for each new
// group it starts.
func newIdempotencyKey() *chatpb.IdempotencyKey {
	id := uuid.New()
	return &chatpb.IdempotencyKey{Value: id[:]}
}

// startGroupChat starts a new group under a fresh idempotency key. A test
// exercising retries supplies its own key via startGroupChatWithKey.
func (e *serverEnv) startGroupChat(keys model.KeyPair, params *chatpb.StartChatRequest_PublicGroupChatParameters) (*chatpb.StartChatResponse, error) {
	return e.startGroupChatWithKey(keys, newIdempotencyKey(), params)
}

func (e *serverEnv) startGroupChatWithKey(keys model.KeyPair, key *chatpb.IdempotencyKey, params *chatpb.StartChatRequest_PublicGroupChatParameters) (*chatpb.StartChatResponse, error) {
	req := &chatpb.StartChatRequest{
		Parameters:     &chatpb.StartChatRequest_PublicGroup{PublicGroup: params},
		IdempotencyKey: key,
	}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.StartChat(e.ctx, req)
}

func (e *serverEnv) mustStartGroupChat(keys model.KeyPair, params *chatpb.StartChatRequest_PublicGroupChatParameters) *chatpb.StartChatResponse {
	resp, err := e.startGroupChat(keys, params)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) mustStartGroupChatWithKey(keys model.KeyPair, key *chatpb.IdempotencyKey, params *chatpb.StartChatRequest_PublicGroupChatParameters) *chatpb.StartChatResponse {
	resp, err := e.startGroupChatWithKey(keys, key, params)
	require.NoError(e.t, err)
	return resp
}

// keyEnvelopeProto builds a key envelope as a client sends one. The server
// never opens it, so any bytes of the right length stand in for a wrapped key.
func keyEnvelopeProto(fill byte) *chatpb.KeyEnvelope {
	return &chatpb.KeyEnvelope{
		Scheme:     chatpb.KeyEnvelope_X25519_XCHACHA20POLY1305,
		Nonce:      bytes.Repeat([]byte{fill}, 24),
		Ciphertext: bytes.Repeat([]byte{fill}, 48),
	}
}

func (e *serverEnv) setKeyEnvelope(keys model.KeyPair, chatID *commonpb.ChatId, envelope *chatpb.KeyEnvelope) (*chatpb.SetKeyEnvelopeResponse, error) {
	req := &chatpb.SetKeyEnvelopeRequest{ChatId: chatID, KeyEnvelope: envelope}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.SetKeyEnvelope(e.ctx, req)
}

func (e *serverEnv) mustSetKeyEnvelope(keys model.KeyPair, chatID *commonpb.ChatId, envelope *chatpb.KeyEnvelope) *chatpb.SetKeyEnvelopeResponse {
	resp, err := e.setKeyEnvelope(keys, chatID, envelope)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) mustGetKeyEnvelope(keys model.KeyPair, chatID *commonpb.ChatId) *chatpb.GetKeyEnvelopeResponse {
	req := &chatpb.GetKeyEnvelopeRequest{ChatId: chatID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.GetKeyEnvelope(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

// putPrivateGroup persists a private group created by the env user, whose
// members are the env user and the given others.
func (e *serverEnv) putPrivateGroup(title string, others ...*commonpb.UserId) *commonpb.ChatId {
	chatID := chat.MustGenerateGroupChatID()
	require.NoError(e.t, e.store.PutChat(e.ctx, &chat.Chat{
		ID:           chatID,
		Type:         chatpb.ChatType_GROUP,
		Members:      append([]*commonpb.UserId{e.userID}, others...),
		Title:        title,
		IsPrivate:    true,
		CreatorID:    e.userID,
		LastActivity: at(1),
	}))
	return chatID
}

// testServer_KeyEnvelope_Gates pins who may store and fetch a key envelope:
// a member of a private group, and no one else of anything else.
func testServer_KeyEnvelope_Gates(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	_, strangerKeys := e.addUser()

	requireBoth := func(keys model.KeyPair, chatID *commonpb.ChatId, set chatpb.SetKeyEnvelopeResponse_Result, get chatpb.GetKeyEnvelopeResponse_Result) {
		t.Helper()
		require.Equal(t, set, e.mustSetKeyEnvelope(keys, chatID, keyEnvelopeProto(1)).Result)
		got := e.mustGetKeyEnvelope(keys, chatID)
		require.Equal(t, get, got.Result)
		require.Nil(t, got.Envelope)
		require.Nil(t, got.WrappedBy)
		require.Nil(t, got.WrappedByPublicKey)
	}

	// A DM has no key envelopes, for its members or anyone.
	dm := e.putDM(at(1))
	requireBoth(e.keys, dm, chatpb.SetKeyEnvelopeResponse_DENIED, chatpb.GetKeyEnvelopeResponse_DENIED)
	requireBoth(strangerKeys, dm, chatpb.SetKeyEnvelopeResponse_DENIED, chatpb.GetKeyEnvelopeResponse_DENIED)

	// A group that does not exist.
	requireBoth(e.keys, chat.MustGenerateGroupChatID(), chatpb.SetKeyEnvelopeResponse_NOT_FOUND, chatpb.GetKeyEnvelopeResponse_NOT_FOUND)

	// Nor has a public group, for its members.
	public := e.putGroup("Public", at(1))
	requireBoth(e.keys, public, chatpb.SetKeyEnvelopeResponse_DENIED, chatpb.GetKeyEnvelopeResponse_DENIED)

	// A private group's are its members' alone, and nothing was stored for
	// anyone refused.
	private := e.putPrivateGroup("Private")
	requireBoth(strangerKeys, private, chatpb.SetKeyEnvelopeResponse_DENIED, chatpb.GetKeyEnvelopeResponse_DENIED)
	require.Equal(t, chatpb.GetKeyEnvelopeResponse_NO_ENVELOPE, e.mustGetKeyEnvelope(e.keys, private).Result)
	_, err := s.GetKeyEnvelope(e.ctx, public, e.userID)
	require.ErrorIs(t, err, chat.ErrKeyEnvelopeNotFound)

	// An envelope of the wrong shape never reaches the store.
	for _, malformed := range []*chatpb.KeyEnvelope{
		nil,
		{Scheme: chatpb.KeyEnvelope_UNKNOWN, Nonce: bytes.Repeat([]byte{1}, 24), Ciphertext: bytes.Repeat([]byte{1}, 48)},
		{Scheme: chatpb.KeyEnvelope_X25519_XCHACHA20POLY1305, Nonce: bytes.Repeat([]byte{1}, 23), Ciphertext: bytes.Repeat([]byte{1}, 48)},
		{Scheme: chatpb.KeyEnvelope_X25519_XCHACHA20POLY1305, Nonce: bytes.Repeat([]byte{1}, 24), Ciphertext: bytes.Repeat([]byte{1}, 49)},
	} {
		_, err := e.setKeyEnvelope(e.keys, private, malformed)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	require.Equal(t, chatpb.GetKeyEnvelopeResponse_NO_ENVELOPE, e.mustGetKeyEnvelope(e.keys, private).Result)
}

// testServer_KeyEnvelope_Creator pins the second step of creating a private
// group: its creator stores the chat key's envelope, the first one stored
// stands whatever a later call carries, and it is kept when they leave.
func testServer_KeyEnvelope_Creator(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.bindKey(e.userID, e.keys)

	created := e.mustStartPrivateGroupChat(e.keys, newIdempotencyKey(), privateGroupParams("Sunday Hikers"))
	require.Equal(t, chatpb.StartChatResponse_OK, created.Result)
	chatID := created.Chat.ChatId

	// A new group has no key.
	require.Equal(t, chatpb.GetKeyEnvelopeResponse_NO_ENVELOPE, e.mustGetKeyEnvelope(e.keys, chatID).Result)

	// The creator stores it, and reads it back as their own.
	require.Equal(t, chatpb.SetKeyEnvelopeResponse_OK, e.mustSetKeyEnvelope(e.keys, chatID, keyEnvelopeProto(1)).Result)
	requireStored := func() {
		t.Helper()
		got := e.mustGetKeyEnvelope(e.keys, chatID)
		require.Equal(t, chatpb.GetKeyEnvelopeResponse_OK, got.Result)
		require.NoError(t, protoutil.ProtoEqualError(keyEnvelopeProto(1), got.GetKeyEnvelope()))
		require.Equal(t, e.userID.Value, got.WrappedBy.GetValue())
		require.True(t, proto.Equal(e.keys.Proto(), got.WrappedByPublicKey))
	}
	requireStored()

	// A retry of the same envelope is OK; a different one, as a second device
	// setting up the same group would send, is ALREADY_SET and changes
	// nothing.
	require.Equal(t, chatpb.SetKeyEnvelopeResponse_OK, e.mustSetKeyEnvelope(e.keys, chatID, keyEnvelopeProto(1)).Result)
	require.Equal(t, chatpb.SetKeyEnvelopeResponse_ALREADY_SET, e.mustSetKeyEnvelope(e.keys, chatID, keyEnvelopeProto(2)).Result)
	requireStored()

	// Leaving keeps the creator's envelope, though only a member reads it:
	// it is there again when they rejoin.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, chatID).Result)
	require.Equal(t, chatpb.GetKeyEnvelopeResponse_DENIED, e.mustGetKeyEnvelope(e.keys, chatID).Result)
	require.Equal(t, chatpb.SetKeyEnvelopeResponse_DENIED, e.mustSetKeyEnvelope(e.keys, chatID, keyEnvelopeProto(3)).Result)
	_, err := s.GetKeyEnvelope(e.ctx, chatID, e.userID)
	require.NoError(t, err)
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(e.keys, chatID).Result)
	requireStored()
}

// testServer_KeyEnvelope_Member pins a member's envelope: the one the creator
// wrapped for them is read back as the creator's, they replace it once with
// one of their own, and it is discarded when they leave.
func testServer_KeyEnvelope_Member(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// No RPC admits a member to a private group yet, so the membership and
	// the envelope the creator would have wrapped are written by hand.
	memberID, memberKeys := e.addUser()
	e.bindKey(e.userID, e.keys)
	e.bindKey(memberID, memberKeys)
	chatID := e.putPrivateGroup("Sunday Hikers", memberID)
	require.Equal(t, chatpb.SetKeyEnvelopeResponse_OK, e.mustSetKeyEnvelope(e.keys, chatID, keyEnvelopeProto(1)).Result)
	_, err := s.SetKeyEnvelope(e.ctx, chatID, memberID, chat.KeyEnvelopeFromProto(keyEnvelopeProto(2), e.userID))
	require.NoError(t, err)

	got := e.mustGetKeyEnvelope(memberKeys, chatID)
	require.Equal(t, chatpb.GetKeyEnvelopeResponse_OK, got.Result)
	require.NoError(t, protoutil.ProtoEqualError(keyEnvelopeProto(2), got.GetKeyEnvelope()))
	require.Equal(t, e.userID.Value, got.WrappedBy.GetValue())

	// The creator wrapped it, so their public key is the one reported: it is
	// what the member opens the envelope against.
	require.True(t, proto.Equal(e.keys.Proto(), got.WrappedByPublicKey))

	// The member wraps the key for themself, which replaces the creator's
	// envelope and then stands.
	require.Equal(t, chatpb.SetKeyEnvelopeResponse_OK, e.mustSetKeyEnvelope(memberKeys, chatID, keyEnvelopeProto(3)).Result)
	require.Equal(t, chatpb.SetKeyEnvelopeResponse_ALREADY_SET, e.mustSetKeyEnvelope(memberKeys, chatID, keyEnvelopeProto(4)).Result)
	got = e.mustGetKeyEnvelope(memberKeys, chatID)
	require.Equal(t, chatpb.GetKeyEnvelopeResponse_OK, got.Result)
	require.NoError(t, protoutil.ProtoEqualError(keyEnvelopeProto(3), got.GetKeyEnvelope()))
	require.Equal(t, memberID.Value, got.WrappedBy.GetValue())
	require.True(t, proto.Equal(memberKeys.Proto(), got.WrappedByPublicKey))

	// Each member's envelope is their own: the creator's is untouched.
	got = e.mustGetKeyEnvelope(e.keys, chatID)
	require.NoError(t, protoutil.ProtoEqualError(keyEnvelopeProto(1), got.GetKeyEnvelope()))

	// Leaving discards the member's envelope and no one else's.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(memberKeys, chatID).Result)
	_, err = s.GetKeyEnvelope(e.ctx, chatID, memberID)
	require.ErrorIs(t, err, chat.ErrKeyEnvelopeNotFound)
	_, err = s.GetKeyEnvelope(e.ctx, chatID, e.userID)
	require.NoError(t, err)
	require.Equal(t, chatpb.GetKeyEnvelopeResponse_DENIED, e.mustGetKeyEnvelope(memberKeys, chatID).Result)

	// A leave of a public group is unaffected.
	public := e.putGroup("Public", at(1), memberID)
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(memberKeys, public).Result)
}

// mustStartPrivateGroupChat starts a private group under the given
// idempotency key.
func (e *serverEnv) mustStartPrivateGroupChat(keys model.KeyPair, key *chatpb.IdempotencyKey, params *chatpb.StartChatRequest_PrivateGroupChatParameters) *chatpb.StartChatResponse {
	req := &chatpb.StartChatRequest{
		Parameters:     &chatpb.StartChatRequest_PrivateGroup{PrivateGroup: params},
		IdempotencyKey: key,
	}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.StartChat(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

// privateGroupParams builds StartChat parameters for a private group with the
// given title.
func privateGroupParams(title string) *chatpb.StartChatRequest_PrivateGroupChatParameters {
	return &chatpb.StartChatRequest_PrivateGroupChatParameters{Title: title}
}

// startChatMinimumBalance is the minimum listener balance, in USD, the
// StartChat tests ask their groups to carry when the rules are not what is
// under test. Every group must carry one (see chat.RulesFromProto).
const startChatMinimumBalance = 100

// minimumBalanceRule builds the listener rule for a USD minimum balance.
func minimumBalanceRule(currency string, amount float64) *chatpb.ListenerRules {
	return &chatpb.ListenerRules{Kind: &chatpb.ListenerRules_MinimumBalance{MinimumBalance: &chatpb.MinimumBalanceRequirement{
		Amount: &commonpb.FiatPaymentAmount{Currency: currency, NativeAmount: amount},
	}}}
}

// speakerBalanceRule builds the speaker rule for a minimum balance in any
// mint.
func speakerBalanceRule(currency string, amount float64) *chatpb.SpeakerRules {
	return &chatpb.SpeakerRules{Kind: &chatpb.SpeakerRules_MinimumBalance{MinimumBalance: &chatpb.MinimumBalanceRequirement{
		Amount: &commonpb.FiatPaymentAmount{Currency: currency, NativeAmount: amount},
	}}}
}

// groupParams builds StartChat parameters for a group with the given title and
// the default minimum balance rule.
func groupParams(title string) *chatpb.StartChatRequest_PublicGroupChatParameters {
	return &chatpb.StartChatRequest_PublicGroupChatParameters{
		Title: title,
		Rules: &chatpb.Rules{Listener: []*chatpb.ListenerRules{minimumBalanceRule("usd", startChatMinimumBalance)}},
	}
}

// fundEnvUser gives the env user an owner account holding the given USD
// amount, so they satisfy a minimum balance rule up to it.
func (e *serverEnv) fundEnvUser(amount uint64) {
	bound, err := e.accounts.GetPubKeys(e.ctx, e.userID)
	require.NoError(e.t, err)
	if len(bound) == 0 {
		_, err = e.accounts.Bind(e.ctx, e.userID, e.keys.Proto())
		require.NoError(e.t, err)
	}
	e.ocpBalance.setBalance(e.keys.Proto(), ocp_common.ToCoreMintQuarks(amount))
}

func testServer_StartChat_OK(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	e.profiles.displayNames[string(e.userID.Value)] = "Founder"
	e.fundEnvUser(startChatMinimumBalance)

	before := time.Now().UTC()
	resp := e.mustStartGroupChat(e.keys, groupParams("Sunday Hikers"))
	require.Equal(t, chatpb.StartChatResponse_OK, resp.Result)
	require.Equal(t, moderationpb.FlaggedCategory_NONE, resp.FlaggedCategory)

	// The response carries the new group as its creator sees it: a fresh
	// server-minted ID, the title, the rules asked for, no picture, the creator
	// as its creator and only member, and a roster of one at version zero.
	md := resp.Chat
	require.NotNil(t, md)
	require.True(t, chat.IsGroupChatID(md.ChatId))
	require.Equal(t, chatpb.ChatType_GROUP, md.Type)
	require.Equal(t, e.userID.Value, md.GetCreator().GetValue())
	require.Equal(t, "Sunday Hikers", md.Title)
	require.NoError(t, protoutil.ProtoEqualError(groupParams("Sunday Hikers").Rules, md.Rules))
	require.Nil(t, md.ProfilePicture)
	require.Nil(t, md.LastMessage)
	require.False(t, md.LastActivity.AsTime().Before(before))
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 1, Version: 0}, md.GetRosterSummary()))
	require.Len(t, md.Members, 1)
	require.Equal(t, e.userID.Value, md.Members[0].UserId.Value)
	require.Equal(t, "Founder", md.Members[0].UserProfile.DisplayName)

	// The title reached the title classifier as given.
	require.Equal(t, "Sunday Hikers", e.moderator.classifiedTitle)

	// The record is in the store with the creator recorded and joined, and is
	// readable back through GetChat.
	stored, err := s.GetChatByID(e.ctx, md.ChatId)
	require.NoError(t, err)
	require.Equal(t, "Sunday Hikers", stored.Title)
	require.Equal(t, e.userID.Value, stored.CreatorID.Value)
	require.False(t, stored.IsStaffOnly)
	require.NotNil(t, stored.MinimumListenerBalance)
	require.Equal(t, float64(startChatMinimumBalance), stored.MinimumListenerBalance.NativeAmount)
	require.Nil(t, stored.ProfilePictureBlobID)
	members, err := s.GetMembers(e.ctx, md.ChatId)
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Equal(t, e.userID.Value, members[0].Value)
	require.Equal(t, chatpb.GetChatResponse_OK, e.getChat(e.keys, md.ChatId).Result)

	// The creator's other devices learn of the group as a join carrying the
	// metadata; there is no one else to tell, so the chat topic is silent.
	e.userObserver.WaitFor(t, func([]*event.KeyAndEvent[*commonpb.UserId, *eventpb.Event]) bool {
		return len(e.rosterUpdatesOnUserTopic(e.userID, md.ChatId)) >= 1
	})
	toCreator := e.rosterUpdatesOnUserTopic(e.userID, md.ChatId)
	require.Len(t, toCreator, 1)
	joined := toCreator[0].GetMemberJoined()
	require.NotNil(t, joined)
	require.Equal(t, e.userID.Value, joined.Member.UserId.Value)
	require.Equal(t, "Founder", joined.Member.UserProfile.DisplayName)
	require.Zero(t, joined.Member.Version)
	require.NotNil(t, joined.Member.JoinedAt)
	require.WithinDuration(t, time.Now(), joined.Member.JoinedAt.AsTime(), time.Minute)
	require.Nil(t, md.Members[0].JoinedAt, "the creator's own metadata entry carries no record fields")
	require.NotNil(t, joined.Metadata)
	require.Equal(t, md.ChatId.Value, joined.Metadata.ChatId.Value)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 1, Version: 0}, toCreator[0].GetRosterSummary()))
	onChatTopic, _ := e.rosterUpdatesOnChatTopic(md.ChatId)
	require.Empty(t, onChatTopic)

	// Every call under a fresh key mints a distinct group, even with the same
	// title; only the same key names the same group (see
	// testServer_StartChat_Idempotent).
	again := e.mustStartGroupChat(e.keys, groupParams("Sunday Hikers"))
	require.Equal(t, chatpb.StartChatResponse_OK, again.Result)
	require.NotEqual(t, md.ChatId.Value, again.Chat.ChatId.Value)
}

// testServer_StartChat_Idempotent pins that StartChat is retry-safe: the same
// key from the same caller names the same group, however the retry differs
// from the original and whatever has changed since, while a fresh key or
// another caller gets a group of their own.
func testServer_StartChat_Idempotent(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.fundEnvUser(startChatMinimumBalance)

	key := newIdempotencyKey()
	first := e.mustStartGroupChatWithKey(e.keys, key, groupParams("Sunday Hikers"))
	require.Equal(t, chatpb.StartChatResponse_OK, first.Result)
	chatID := first.Chat.ChatId

	// The group's ID is a pure function of the caller and the key.
	require.Equal(t, chat.MustDeriveGroupChatID(e.userID, key).Value, chatID.Value)
	e.userObserver.WaitFor(t, func([]*event.KeyAndEvent[*commonpb.UserId, *eventpb.Event]) bool {
		return len(e.rosterUpdatesOnUserTopic(e.userID, chatID)) >= 1
	})

	// A retry is answered with the original: same group, result OK, the record
	// as it stands.
	retry := e.mustStartGroupChatWithKey(e.keys, key, groupParams("Sunday Hikers"))
	require.Equal(t, chatpb.StartChatResponse_OK, retry.Result)
	require.Equal(t, chatID.Value, retry.Chat.ChatId.Value)
	require.Equal(t, "Sunday Hikers", retry.Chat.Title)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 1, Version: 0}, retry.Chat.GetRosterSummary()))
	require.Len(t, retry.Chat.Members, 1)
	require.Equal(t, e.userID.Value, retry.Chat.Members[0].UserId.Value)

	// The key is the request's identity, not its parameters: a retry asking
	// for something else still gets the original, unchanged.
	renamed := e.mustStartGroupChatWithKey(e.keys, key, groupParams("Renamed"))
	require.Equal(t, chatpb.StartChatResponse_OK, renamed.Result)
	require.Equal(t, chatID.Value, renamed.Chat.ChatId.Value)
	require.Equal(t, "Sunday Hikers", renamed.Chat.Title)
	stored, err := s.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, "Sunday Hikers", stored.Title)

	// A retry is answered from the record, not re-checked: the moderator has
	// since learned to flag the title, and the creator no longer holds the
	// minimum balance, yet the group they already created is still theirs.
	e.moderator.titleFlagged = true
	e.moderator.titleCategories = []string{"solicitation"}
	e.ocpBalance.setBalance(e.keys.Proto(), 0)
	unchecked := e.mustStartGroupChatWithKey(e.keys, key, groupParams("Sunday Hikers"))
	require.Equal(t, chatpb.StartChatResponse_OK, unchecked.Result)
	require.Equal(t, chatID.Value, unchecked.Chat.ChatId.Value)
	e.moderator.titleFlagged = false
	e.moderator.titleCategories = nil
	e.fundEnvUser(startChatMinimumBalance)

	// One group exists, and only its creation was announced.
	groups, err := s.GetGroupChatsForUser(e.ctx, e.userID)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	require.Len(t, e.rosterUpdatesOnUserTopic(e.userID, chatID), 1)

	// A fresh key from the same caller is a new group.
	other := e.mustStartGroupChatWithKey(e.keys, newIdempotencyKey(), groupParams("Sunday Hikers"))
	require.Equal(t, chatpb.StartChatResponse_OK, other.Result)
	require.NotEqual(t, chatID.Value, other.Chat.ChatId.Value)

	// The same key from another caller is that caller's own group: a key
	// cannot name someone else's chat.
	strangerID, strangerKeys := e.addUser()
	_, err = e.accounts.Bind(e.ctx, strangerID, strangerKeys.Proto())
	require.NoError(t, err)
	e.ocpBalance.setBalance(strangerKeys.Proto(), ocp_common.ToCoreMintQuarks(startChatMinimumBalance))
	strangers := e.mustStartGroupChatWithKey(strangerKeys, key, groupParams("Sunday Hikers"))
	require.Equal(t, chatpb.StartChatResponse_OK, strangers.Result)
	require.NotEqual(t, chatID.Value, strangers.Chat.ChatId.Value)
	require.Equal(t, chat.MustDeriveGroupChatID(strangerID, key).Value, strangers.Chat.ChatId.Value)
	require.Equal(t, strangerID.Value, strangers.Chat.Members[0].UserId.Value)

	// A creator who has since left still gets their group back on a retry,
	// seen as the non-member they now are: no hydrated member of their own.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, chatID).Result)
	departed := e.mustStartGroupChatWithKey(e.keys, key, groupParams("Sunday Hikers"))
	require.Equal(t, chatpb.StartChatResponse_OK, departed.Result)
	require.Equal(t, chatID.Value, departed.Chat.ChatId.Value)
	require.Empty(t, departed.Chat.Members)
	isMember, err := s.IsMember(e.ctx, chatID, e.userID)
	require.NoError(t, err)
	require.False(t, isMember)

	// A request without a key, or with one of the wrong width, is rejected
	// before anything is read or written.
	for _, key := range []*chatpb.IdempotencyKey{nil, {Value: []byte("short")}} {
		req := &chatpb.StartChatRequest{
			Parameters:     &chatpb.StartChatRequest_PublicGroup{PublicGroup: groupParams("Keyless")},
			IdempotencyKey: key,
		}
		require.NoError(t, e.keys.Auth(req, &req.Auth))
		_, err = e.client.StartChat(e.ctx, req)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	groups, err = s.GetGroupChatsForUser(e.ctx, e.userID)
	require.NoError(t, err)
	require.Len(t, groups, 1)
}

func testServer_StartChat_WithPicture(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.fundEnvUser(startChatMinimumBalance)

	pictureBlobID := &blobpb.BlobId{Value: []byte("group-picture-01")}
	renditions := e.media.setAttachable(pictureBlobID)

	params := groupParams("Picture Group")
	params.ProfilePicture = pictureBlobID
	resp := e.mustStartGroupChat(e.keys, params)
	require.Equal(t, chatpb.StartChatResponse_OK, resp.Result)
	md := resp.Chat

	// The picture was attached against the new group's ID before the record
	// was written, and comes back hydrated with its full rendition set.
	require.Equal(t, [][]byte{pictureBlobID.Value}, e.media.attachedTo(md.ChatId))
	require.NotNil(t, md.ProfilePicture)
	require.Len(t, md.ProfilePicture.Renditions, len(renditions))
	require.Equal(t, pictureBlobID.Value, md.ProfilePicture.Renditions[0].GetBlobId().GetValue())
	require.NotNil(t, md.ProfilePicture.Renditions[0].Blob)

	stored, err := s.GetChatByID(e.ctx, md.ChatId)
	require.NoError(t, err)
	require.Equal(t, pictureBlobID.Value, stored.ProfilePictureBlobID.GetValue())
}

func testServer_StartChat_PictureNotAccepted(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.fundEnvUser(startChatMinimumBalance)

	// A blob the media domain will not attach — unknown, not the caller's, not
	// READY, or not an image — refuses the whole creation: no group is written.
	params := groupParams("Picture Group")
	params.ProfilePicture = &blobpb.BlobId{Value: []byte("not-attachable01")}
	resp := e.mustStartGroupChat(e.keys, params)
	require.Equal(t, chatpb.StartChatResponse_PROFILE_PICTURE_BLOB_NOT_ACCEPTED, resp.Result)
	require.Nil(t, resp.Chat)

	groups, err := s.GetGroupChatsForUser(e.ctx, e.userID)
	require.NoError(t, err)
	require.Empty(t, groups)
	require.Empty(t, e.media.chatPictures)

	// A blob domain that cannot attach at all is the server's fault, not the
	// picture's: the RPC fails rather than telling the client to pick another.
	e.media.setAttachable(&blobpb.BlobId{Value: []byte("group-picture-01")})
	e.media.attachErr = errors.New("blob store down")
	params.ProfilePicture = &blobpb.BlobId{Value: []byte("group-picture-01")}
	_, err = e.startGroupChat(e.keys, params)
	require.Equal(t, codes.Internal, status.Code(err))
	groups, err = s.GetGroupChatsForUser(e.ctx, e.userID)
	require.NoError(t, err)
	require.Empty(t, groups)
}

func testServer_StartChat_TitleModerated(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.fundEnvUser(startChatMinimumBalance)

	noGroups := func() {
		t.Helper()
		groups, err := s.GetGroupChatsForUser(e.ctx, e.userID)
		require.NoError(t, err)
		require.Empty(t, groups)
	}

	// The title classifier flags: its best-fit category is reported, and no
	// group is written.
	e.moderator.titleFlagged = true
	e.moderator.titleCategories = []string{"gibberish", "solicitation"}
	resp := e.mustStartGroupChat(e.keys, groupParams("DM for signals"))
	require.Equal(t, chatpb.StartChatResponse_TITLE_MODERATED, resp.Result)
	require.Equal(t, moderationpb.FlaggedCategory_SPAM, resp.FlaggedCategory)
	require.Nil(t, resp.Chat)
	noGroups()

	// The general text classifier flags on its own: still refused, with its
	// category.
	e.moderator.titleFlagged = false
	e.moderator.titleCategories = nil
	e.moderator.textFlagged = true
	e.moderator.textCategories = []string{"hate"}
	resp = e.mustStartGroupChat(e.keys, groupParams("flagged prose"))
	require.Equal(t, chatpb.StartChatResponse_TITLE_MODERATED, resp.Result)
	require.Equal(t, moderationpb.FlaggedCategory_NSFW, resp.FlaggedCategory)
	noGroups()

	// Both flag: the title classifier's category wins, being the specific one.
	e.moderator.titleFlagged = true
	e.moderator.titleCategories = []string{"financial_claim"}
	resp = e.mustStartGroupChat(e.keys, groupParams("Guaranteed 10x"))
	require.Equal(t, chatpb.StartChatResponse_TITLE_MODERATED, resp.Result)
	require.Equal(t, moderationpb.FlaggedCategory_MISLEADING, resp.FlaggedCategory)
	noGroups()

	// The text classifier declining a short title for want of a language is
	// not a refusal: the title classifier still covers it.
	e.moderator.titleFlagged = false
	e.moderator.titleCategories = nil
	e.moderator.textFlagged = false
	e.moderator.textCategories = nil
	e.moderator.textErr = moderation.ErrUnsupportedLanguage
	resp = e.mustStartGroupChat(e.keys, groupParams("Fam"))
	require.Equal(t, chatpb.StartChatResponse_OK, resp.Result)
}

// testServer_StartChat_ModerationFailureIsInternal pins that a title which
// cannot be classified is never persisted: either classifier failing is the
// RPC failing, not a pass.
func testServer_StartChat_ModerationFailureIsInternal(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.fundEnvUser(startChatMinimumBalance)

	e.moderator.titleErr = errors.New("classifier down")
	_, err := e.startGroupChat(e.keys, groupParams("Sunday Hikers"))
	require.Equal(t, codes.Internal, status.Code(err))

	e.moderator.titleErr = nil
	e.moderator.textErr = errors.New("classifier down")
	_, err = e.startGroupChat(e.keys, groupParams("Sunday Hikers"))
	require.Equal(t, codes.Internal, status.Code(err))

	groups, err := s.GetGroupChatsForUser(e.ctx, e.userID)
	require.NoError(t, err)
	require.Empty(t, groups)
}

// testServer_StartChat_PrivateGroup pins the creation of a private group: any
// user may create one, it has no rules to satisfy, and it is otherwise created
// as any group is — title moderated, the creator its only member, announced to
// their devices, and retry-safe.
func testServer_StartChat_PrivateGroup(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.profiles.displayNames[string(e.userID.Value)] = "Founder"

	groups := func() []*chat.Chat {
		t.Helper()
		groups, err := s.GetGroupChatsForUser(e.ctx, e.userID)
		require.NoError(t, err)
		return groups
	}

	// A user who is not staff and holds nothing creates one: there is no rule
	// to satisfy.
	key := newIdempotencyKey()
	resp := e.mustStartPrivateGroupChat(e.keys, key, privateGroupParams("Sunday Hikers"))
	require.Equal(t, chatpb.StartChatResponse_OK, resp.Result)
	md := resp.Chat
	require.NotNil(t, md)
	require.True(t, chat.IsGroupChatID(md.ChatId))
	require.Equal(t, chatpb.ChatType_GROUP, md.Type)
	require.True(t, md.IsPrivate)
	require.Nil(t, md.Rules)
	require.Equal(t, e.userID.Value, md.GetCreator().GetValue())
	require.Equal(t, "Sunday Hikers", md.Title)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 1, Version: 0}, md.GetRosterSummary()))
	require.Len(t, md.Members, 1)
	require.Equal(t, e.userID.Value, md.Members[0].UserId.Value)
	require.True(t, md.GetViewerState().GetPermissions().GetCanEdit())
	require.Equal(t, "Sunday Hikers", e.moderator.classifiedTitle)

	stored, err := s.GetChatByID(e.ctx, md.ChatId)
	require.NoError(t, err)
	require.True(t, stored.IsPrivate)
	require.Nil(t, stored.Rules())
	require.Equal(t, e.userID.Value, stored.CreatorID.Value)
	members, err := s.GetMembers(e.ctx, md.ChatId)
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Equal(t, e.userID.Value, members[0].Value)

	// The creator's other devices learn of it as a join carrying the metadata.
	e.userObserver.WaitFor(t, func([]*event.KeyAndEvent[*commonpb.UserId, *eventpb.Event]) bool {
		return len(e.rosterUpdatesOnUserTopic(e.userID, md.ChatId)) >= 1
	})
	toCreator := e.rosterUpdatesOnUserTopic(e.userID, md.ChatId)
	require.Len(t, toCreator, 1)
	require.True(t, toCreator[0].GetMemberJoined().GetMetadata().GetIsPrivate())

	// A retry names the same group and is answered from its record, whatever
	// it carries.
	again := e.mustStartPrivateGroupChat(e.keys, key, privateGroupParams("Renamed"))
	require.Equal(t, chatpb.StartChatResponse_OK, again.Result)
	require.Equal(t, md.ChatId.Value, again.Chat.ChatId.Value)
	require.Equal(t, "Sunday Hikers", again.Chat.Title)
	require.True(t, again.Chat.IsPrivate)
	asPublic := e.mustStartGroupChatWithKey(e.keys, key, groupParams("Renamed"))
	require.Equal(t, chatpb.StartChatResponse_OK, asPublic.Result)
	require.Equal(t, md.ChatId.Value, asPublic.Chat.ChatId.Value)
	require.True(t, asPublic.Chat.IsPrivate)
	require.Len(t, groups(), 1)

	// Its title is moderated like any group's.
	e.moderator.titleFlagged = true
	e.moderator.titleCategories = []string{"gibberish", "solicitation"}
	resp = e.mustStartPrivateGroupChat(e.keys, newIdempotencyKey(), privateGroupParams("DM for signals"))
	require.Equal(t, chatpb.StartChatResponse_TITLE_MODERATED, resp.Result)
	require.Equal(t, moderationpb.FlaggedCategory_SPAM, resp.FlaggedCategory)
	require.Nil(t, resp.Chat)
	require.Len(t, groups(), 1)
}

// testServer_OpenVersusPrivate_NonMember pins, side by side, what a
// registered non-member with nothing to their name, and a caller who is no
// one, get from a public group with no listener rules and from a private
// group. The open group is open: GetChat carries its messaging state in full
// (redacted under REDACTED), its public view is shown, its chatter sample is
// given, and anyone joins it. The private group shows a registered user its
// record alone under every mode, has no public view and no sample, and is
// joined by no one but its creator.
func testServer_OpenVersusPrivate_NonMember(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	every := []messagingpb.ViewMode{messagingpb.ViewMode_FULL, messagingpb.ViewMode_FULL_OR_REDACTED, messagingpb.ViewMode_REDACTED}

	founder := model.MustGenerateUserID()
	open := &chat.Chat{
		ID:            chat.MustGenerateGroupChatID(),
		Type:          chatpb.ChatType_GROUP,
		Members:       []*commonpb.UserId{founder},
		Title:         "Open",
		LastActivity:  at(1),
		LastMessageID: &messagingpb.MessageId{Value: 2},
	}
	require.NoError(t, s.PutChat(e.ctx, open))
	private := open.Clone()
	private.ID = chat.MustGenerateGroupChatID()
	private.Title = "Private"
	private.IsPrivate = true
	private.CreatorID = founder
	require.NoError(t, s.PutChat(e.ctx, private))
	for _, c := range []*chat.Chat{open, private} {
		e.messaging.lastMessages[string(c.ID.Value)] = textMessage(2, founder, "the words")
		e.messaging.latestEventSeqs[string(c.ID.Value)] = 2
	}

	_, strangerKeys := e.addUser()

	// GetChat: the open group's messaging state, the private group's record
	// alone.
	for _, mode := range every {
		resp := e.getChatWithMode(strangerKeys, open.ID, mode)
		require.Equal(t, chatpb.GetChatResponse_OK, resp.Result, mode)
		require.False(t, resp.Metadata.IsPrivate, mode)
		require.Empty(t, resp.Metadata.Members, mode)
		require.NotNil(t, resp.Metadata.LastMessage, mode)
		require.Equal(t, mode == messagingpb.ViewMode_REDACTED, resp.Metadata.LastMessage.Redacted, mode)
		require.Equal(t, uint64(2), resp.Metadata.LatestEventSequence, mode)

		resp = e.getChatWithMode(strangerKeys, private.ID, mode)
		require.Equal(t, chatpb.GetChatResponse_OK, resp.Result, mode)
		require.True(t, resp.Metadata.IsPrivate, mode)
		require.Empty(t, resp.Metadata.Members, mode)
		require.Nil(t, resp.Metadata.LastMessage, mode)
		require.Zero(t, resp.Metadata.LatestEventSequence, mode)
	}

	// The public view: shown, redacted, for the open group; the record alone
	// for the private one.
	resp := e.getPublicChat(open.ID, messagingpb.ViewMode_REDACTED)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.NotNil(t, resp.Metadata.LastMessage)
	require.True(t, resp.Metadata.LastMessage.Redacted)
	require.Equal(t, uint64(2), resp.Metadata.LatestEventSequence)
	resp = e.getPublicChat(private.ID, messagingpb.ViewMode_REDACTED)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.True(t, resp.Metadata.IsPrivate)
	require.Empty(t, resp.Metadata.Members)
	require.Nil(t, resp.Metadata.LastMessage)
	require.Zero(t, resp.Metadata.LatestEventSequence)

	// The chatter sample: given for the open group, to a registered user and
	// to no one; DENIED for the private one either way.
	require.Equal(t, chatpb.SampleChattersResponse_OK, e.sampleChatters(strangerKeys, open.ID).Result)
	require.Equal(t, chatpb.SampleChattersResponse_OK, e.samplePublicChatters(open.ID).Result)
	require.Equal(t, chatpb.SampleChattersResponse_DENIED, e.sampleChatters(strangerKeys, private.ID).Result)
	require.Equal(t, chatpb.SampleChattersResponse_DENIED, e.samplePublicChatters(private.ID).Result)

	// JoinChat: anyone joins the open group; no one but its creator joins the
	// private one.
	require.Equal(t, chatpb.JoinChatResponse_DENIED, e.mustJoinChat(strangerKeys, private.ID).Result)
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(strangerKeys, open.ID).Result)
	resp = e.getChat(strangerKeys, open.ID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Len(t, resp.Metadata.Members, 1)
	resp = e.getChat(strangerKeys, private.ID)
	require.Empty(t, resp.Metadata.Members)
}

// testServer_PrivateGroup_Visibility pins who sees what of a private group:
// any registered user its record and nothing else under every view mode, its
// members its messaging state, and an unauthenticated caller its record alone
// as its public view.
func testServer_PrivateGroup_Visibility(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	private := &chat.Chat{
		ID:            chat.MustGenerateGroupChatID(),
		Type:          chatpb.ChatType_GROUP,
		Members:       []*commonpb.UserId{e.userID},
		Title:         "Sunday Hikers",
		IsPrivate:     true,
		CreatorID:     e.userID,
		LastActivity:  at(1),
		LastMessageID: &messagingpb.MessageId{Value: 2},
	}
	require.NoError(t, s.PutChat(e.ctx, private))
	e.messaging.lastMessages[string(private.ID.Value)] = textMessage(2, e.userID, "members only")
	e.messaging.latestEventSeqs[string(private.ID.Value)] = 2

	// A member reads it like any group they are in.
	resp := e.getChat(e.keys, private.ID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.True(t, resp.Metadata.IsPrivate)
	require.Len(t, resp.Metadata.Members, 1)
	require.Equal(t, "members only", resp.Metadata.LastMessage.Content[0].GetText().GetText())
	require.Equal(t, uint64(2), resp.Metadata.LatestEventSequence)
	require.NotNil(t, resp.Metadata.ViewerState)

	// A non-member sees the record alone, whatever they ask for and whoever
	// they are: nothing about a staff user admits them.
	strangerID, strangerKeys := e.addUser()
	e.accounts.setStaff(strangerID, true)
	for _, mode := range []messagingpb.ViewMode{messagingpb.ViewMode_FULL, messagingpb.ViewMode_FULL_OR_REDACTED, messagingpb.ViewMode_REDACTED} {
		resp = e.getChatWithMode(strangerKeys, private.ID, mode)
		require.Equal(t, chatpb.GetChatResponse_OK, resp.Result, mode)
		require.True(t, resp.Metadata.IsPrivate, mode)
		require.Equal(t, "Sunday Hikers", resp.Metadata.Title, mode)
		require.Equal(t, e.userID.Value, resp.Metadata.GetCreator().GetValue(), mode)
		require.Nil(t, resp.Metadata.Rules, mode)
		require.Empty(t, resp.Metadata.Members, mode)
		require.Nil(t, resp.Metadata.LastMessage, mode)
		require.Zero(t, resp.Metadata.LatestEventSequence, mode)
		require.Nil(t, resp.Metadata.ViewerState, mode)
	}

	// Its public view is the record alone, under REDACTED, as for any group;
	// any other mode is DENIED.
	resp = e.getPublicChat(private.ID, messagingpb.ViewMode_REDACTED)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.True(t, resp.Metadata.IsPrivate)
	require.Equal(t, "Sunday Hikers", resp.Metadata.Title)
	require.Equal(t, e.userID.Value, resp.Metadata.GetCreator().GetValue())
	require.Nil(t, resp.Metadata.Rules)
	require.Empty(t, resp.Metadata.Members)
	require.Nil(t, resp.Metadata.LastMessage)
	require.Zero(t, resp.Metadata.LatestEventSequence)
	require.Nil(t, resp.Metadata.ViewerState)
	require.False(t, resp.Metadata.InLobby)
	for _, mode := range []messagingpb.ViewMode{messagingpb.ViewMode_FULL, messagingpb.ViewMode_FULL_OR_REDACTED} {
		resp = e.getPublicChat(private.ID, mode)
		require.Equal(t, chatpb.GetChatResponse_DENIED, resp.Result, mode)
		require.Nil(t, resp.Metadata, mode)
	}

	// None of that rests on a private group having no rules. One whose record
	// carries a listener rule shows a non-member who satisfies it the record
	// alone all the same, and admits them no more than any other.
	_, err := e.accounts.Bind(e.ctx, strangerID, strangerKeys.Proto())
	require.NoError(t, err)
	e.ocpBalance.setBalance(strangerKeys.Proto(), ocp_common.ToCoreMintQuarks(startChatMinimumBalance))
	ruled := private.Clone()
	ruled.ID = chat.MustGenerateGroupChatID()
	ruled.Members = []*commonpb.UserId{model.MustGenerateUserID()}
	ruled.MinimumListenerBalance = &chat.MinimumBalance{Currency: "usd", NativeAmount: startChatMinimumBalance}
	require.NoError(t, s.PutChat(e.ctx, ruled))
	e.messaging.lastMessages[string(ruled.ID.Value)] = textMessage(2, e.userID, "members only")
	e.messaging.latestEventSeqs[string(ruled.ID.Value)] = 2
	for _, mode := range []messagingpb.ViewMode{messagingpb.ViewMode_FULL, messagingpb.ViewMode_FULL_OR_REDACTED, messagingpb.ViewMode_REDACTED} {
		resp = e.getChatWithMode(strangerKeys, ruled.ID, mode)
		require.Equal(t, chatpb.GetChatResponse_OK, resp.Result, mode)
		require.True(t, resp.Metadata.IsPrivate, mode)
		require.Empty(t, resp.Metadata.Members, mode)
		require.Nil(t, resp.Metadata.LastMessage, mode)
		require.Zero(t, resp.Metadata.LatestEventSequence, mode)
	}
	resp = e.getPublicChat(ruled.ID, messagingpb.ViewMode_REDACTED)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.True(t, resp.Metadata.IsPrivate)
	require.Nil(t, resp.Metadata.LastMessage)
	require.Zero(t, resp.Metadata.LatestEventSequence)
	require.Equal(t, chatpb.JoinChatResponse_DENIED, e.mustJoinChat(strangerKeys, ruled.ID).Result)

	// It is in its member's feed, marked private.
	feed := e.mustGetGroupFeed(nil)
	require.Equal(t, chatpb.GetGroupChatFeedResponse_OK, feed.Result)
	require.Len(t, feed.Chats, 1)
	require.True(t, feed.Chats[0].IsPrivate)
}

// testServer_PrivateGroup_Membership pins that a private group is not joined
// with JoinChat by anyone but its creator, who leaves and rejoins it like any
// group, and that nobody speaks in one, its creator included.
func testServer_PrivateGroup_Membership(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	created := e.mustStartPrivateGroupChat(e.keys, newIdempotencyKey(), privateGroupParams("Sunday Hikers"))
	require.Equal(t, chatpb.StartChatResponse_OK, created.Result)
	chatID := created.Chat.ChatId

	isMember := func(userID *commonpb.UserId) bool {
		t.Helper()
		ok, err := s.IsMember(e.ctx, chatID, userID)
		require.NoError(t, err)
		return ok
	}

	// No one else joins, a staff user included.
	strangerID, strangerKeys := e.addUser()
	e.accounts.setStaff(strangerID, true)
	joined := e.mustJoinChat(strangerKeys, chatID)
	require.Equal(t, chatpb.JoinChatResponse_DENIED, joined.Result)
	require.Nil(t, joined.Chat)
	require.False(t, isMember(strangerID))

	// The creator's join of a group they are in is the no-op it always is.
	joined = e.mustJoinChat(e.keys, chatID)
	require.Equal(t, chatpb.JoinChatResponse_OK, joined.Result)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 1, Version: 0}, joined.Chat.GetRosterSummary()))

	// The creator leaves, and rejoins on their own: no one need approve them.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, chatID).Result)
	require.False(t, isMember(e.userID))
	joined = e.mustJoinChat(strangerKeys, chatID)
	require.Equal(t, chatpb.JoinChatResponse_DENIED, joined.Result)
	joined = e.mustJoinChat(e.keys, chatID)
	require.Equal(t, chatpb.JoinChatResponse_OK, joined.Result)
	require.True(t, joined.Chat.IsPrivate)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 1, Version: 2}, joined.Chat.GetRosterSummary()))
	require.True(t, isMember(e.userID))

	// The creator edits it like any group they created.
	title := "Monday Hikers"
	edited := e.mustEditChat(e.keys, chatID, &title, nil)
	require.Equal(t, chatpb.EditChatResponse_OK, edited.Result)

	// No one speaks in it, so its creator is refused what a speaker is given.
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_DENIED, e.getMentionSuggestions(e.keys, chatID).Result)
}

func testServer_StartChat_InvalidRules(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// The creator satisfies every valid rule below, so a refusal is for the
	// rules' shape alone.
	e.accounts.setStaff(e.userID, true)
	e.fundEnvUser(startChatMinimumBalance)
	e.ocpBalance.setRate("jpy", 150)

	staff := &chatpb.ListenerRules{Kind: &chatpb.ListenerRules_Staff{Staff: &chatpb.StaffRequirement{}}}
	minimumBalance := minimumBalanceRule("usd", startChatMinimumBalance)

	for name, rules := range map[string]*chatpb.Rules{
		// Every group must carry a minimum listener balance.
		"no rules":   nil,
		"empty":      {},
		"staff only": {Listener: []*chatpb.ListenerRules{staff}},
		// A minimum balance is the only speaker rule a group can ask for.
		"speaker staff rule": {
			Listener: []*chatpb.ListenerRules{minimumBalance},
			Speaker:  []*chatpb.SpeakerRules{{Kind: &chatpb.SpeakerRules_Staff{Staff: &chatpb.StaffRequirement{}}}},
		},
		"speaker creator rule": {
			Listener: []*chatpb.ListenerRules{minimumBalance},
			Speaker:  []*chatpb.SpeakerRules{{Kind: &chatpb.SpeakerRules_Creator{Creator: &chatpb.CreatorRequirement{}}}},
		},
		// A minimum speaker balance must raise the listener balance: the same
		// currency and mints, and an amount larger by at least a minor unit.
		"speaker balance in another currency": {
			Listener: []*chatpb.ListenerRules{minimumBalance},
			Speaker:  []*chatpb.SpeakerRules{speakerBalanceRule("jpy", 2*startChatMinimumBalance*150)},
		},
		"speaker balance in another mint": {
			Listener: []*chatpb.ListenerRules{minimumBalance},
			Speaker: []*chatpb.SpeakerRules{{Kind: &chatpb.SpeakerRules_MinimumBalance{MinimumBalance: &chatpb.MinimumBalanceRequirement{
				Amount: &commonpb.FiatPaymentAmount{Currency: "usd", NativeAmount: 2 * startChatMinimumBalance},
				Mints:  []*commonpb.PublicKey{model.MustGenerateKeyPair().Proto()},
			}}}},
		},
		"speaker balance equal to listener": {
			Listener: []*chatpb.ListenerRules{minimumBalance},
			Speaker:  []*chatpb.SpeakerRules{speakerBalanceRule("usd", startChatMinimumBalance)},
		},
		"speaker balance under a cent above listener": {
			Listener: []*chatpb.ListenerRules{minimumBalance},
			Speaker:  []*chatpb.SpeakerRules{speakerBalanceRule("usd", startChatMinimumBalance+0.009)},
		},
		"speaker balance below listener": {
			Listener: []*chatpb.ListenerRules{minimumBalance},
			Speaker:  []*chatpb.SpeakerRules{speakerBalanceRule("usd", startChatMinimumBalance-1)},
		},
		"duplicate speaker balance": {
			Listener: []*chatpb.ListenerRules{minimumBalance},
			Speaker:  []*chatpb.SpeakerRules{speakerBalanceRule("usd", 2*startChatMinimumBalance), speakerBalanceRule("usd", 3*startChatMinimumBalance)},
		},
		"speaker balance without listener balance": {
			Listener: []*chatpb.ListenerRules{staff},
			Speaker:  []*chatpb.SpeakerRules{speakerBalanceRule("usd", startChatMinimumBalance)},
		},
		// The record holds one requirement of each kind.
		"duplicate staff":           {Listener: []*chatpb.ListenerRules{staff, staff, minimumBalance}},
		"duplicate minimum balance": {Listener: []*chatpb.ListenerRules{minimumBalance, minimumBalanceRule("usd", 1)}},
		// Only a requirement of at least the currency's minimum transfer value
		// — a penny, a yen — requires anything. (A negative amount never
		// reaches the server: the proto validator refuses it as an invalid
		// request.)
		"zero minimum balance":         {Listener: []*chatpb.ListenerRules{minimumBalanceRule("usd", 0)}},
		"sub-penny minimum balance":    {Listener: []*chatpb.ListenerRules{minimumBalanceRule("usd", 0.009)}},
		"half-penny minimum balance":   {Listener: []*chatpb.ListenerRules{minimumBalanceRule("usd", 0.005)}},
		"sub-yen minimum balance":      {Listener: []*chatpb.ListenerRules{minimumBalanceRule("jpy", 0.5)}},
		"not-a-number minimum balance": {Listener: []*chatpb.ListenerRules{minimumBalanceRule("usd", math.NaN())}},
		// A currency OCP cannot value is a rule the server cannot enforce,
		// found out against the creator before anything is written.
		"unpriced minimum balance": {Listener: []*chatpb.ListenerRules{minimumBalanceRule("xyz", 1)}},
	} {
		t.Run(name, func(t *testing.T) {
			resp := e.mustStartGroupChat(e.keys, &chatpb.StartChatRequest_PublicGroupChatParameters{Title: "Ruled", Rules: rules})
			require.Equal(t, chatpb.StartChatResponse_INVALID_RULES, resp.Result)
			require.Nil(t, resp.Chat)
		})
	}

	groups, err := s.GetGroupChatsForUser(e.ctx, e.userID)
	require.NoError(t, err)
	require.Empty(t, groups)
}

func testServer_StartChat_RulesNotSatisfied(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// An underfunded creator cannot start a balance-gated group. With no owner
	// account at all the creator holds nothing, and is refused the same way
	// rather than failed...
	resp := e.mustStartGroupChat(e.keys, groupParams("Whales"))
	require.Equal(t, chatpb.StartChatResponse_RULES_NOT_SATISFIED, resp.Result)
	require.Nil(t, resp.Chat)

	// ...as is one a cent short.
	_, err := e.accounts.Bind(e.ctx, e.userID, e.keys.Proto())
	require.NoError(t, err)
	e.ocpBalance.setBalance(e.keys.Proto(), ocp_common.ToCoreMintQuarks(startChatMinimumBalance)-ocp_common.CoreMintQuarksPerUnit/100)
	resp = e.mustStartGroupChat(e.keys, groupParams("Whales"))
	require.Equal(t, chatpb.StartChatResponse_RULES_NOT_SATISFIED, resp.Result)

	// A funded but non-staff creator cannot start a staff-only group.
	e.fundEnvUser(startChatMinimumBalance)
	staffOnly := groupParams("Staff Room")
	staffOnly.Rules.Listener = append(staffOnly.Rules.Listener, &chatpb.ListenerRules{Kind: &chatpb.ListenerRules_Staff{Staff: &chatpb.StaffRequirement{}}})
	resp = e.mustStartGroupChat(e.keys, staffOnly)
	require.Equal(t, chatpb.StartChatResponse_RULES_NOT_SATISFIED, resp.Result)

	groups, err := s.GetGroupChatsForUser(e.ctx, e.userID)
	require.NoError(t, err)
	require.Empty(t, groups)
}

// testServer_StartChat_MinimumSpeakerBalance pins that a public group can be
// created with a minimum speaker balance above its listener balance: the
// creator must hold the speaker balance, the requirement is stored and shown
// back as asked, and it gates speaking but not joining.
func testServer_StartChat_MinimumSpeakerBalance(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	const (
		listenerRequirement = startChatMinimumBalance
		speakerRequirement  = 2 * startChatMinimumBalance
	)
	usdfMint := model.MustGenerateKeyPair().Proto()
	params := &chatpb.StartChatRequest_PublicGroupChatParameters{
		Title: "Loud Whales",
		Rules: &chatpb.Rules{
			Listener: []*chatpb.ListenerRules{{Kind: &chatpb.ListenerRules_MinimumBalance{MinimumBalance: &chatpb.MinimumBalanceRequirement{
				Amount: &commonpb.FiatPaymentAmount{Currency: "usd", NativeAmount: listenerRequirement},
				Mints:  []*commonpb.PublicKey{usdfMint},
			}}}},
			Speaker: []*chatpb.SpeakerRules{{Kind: &chatpb.SpeakerRules_MinimumBalance{MinimumBalance: &chatpb.MinimumBalanceRequirement{
				Amount: &commonpb.FiatPaymentAmount{Currency: "usd", NativeAmount: speakerRequirement},
				Mints:  []*commonpb.PublicKey{usdfMint},
			}}}},
		},
	}

	// A creator who could read the group but not speak in it cannot start it.
	e.fundEnvUser(speakerRequirement - 1)
	resp := e.mustStartGroupChat(e.keys, params)
	require.Equal(t, chatpb.StartChatResponse_RULES_NOT_SATISFIED, resp.Result)
	require.Nil(t, resp.Chat)
	groups, err := s.GetGroupChatsForUser(e.ctx, e.userID)
	require.NoError(t, err)
	require.Empty(t, groups)

	// Holding the speaker requirement, they get the group, with both rules
	// stored and shown back as asked.
	e.fundEnvUser(speakerRequirement)
	resp = e.mustStartGroupChat(e.keys, params)
	require.Equal(t, chatpb.StartChatResponse_OK, resp.Result)
	require.NoError(t, protoutil.ProtoEqualError(params.Rules, resp.Chat.GetRules()))
	chatID := resp.Chat.ChatId

	stored, err := s.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	require.NotNil(t, stored.MinimumSpeakerBalance)
	require.Equal(t, "usd", stored.MinimumSpeakerBalance.Currency)
	require.Equal(t, float64(speakerRequirement), stored.MinimumSpeakerBalance.NativeAmount)
	require.Len(t, stored.MinimumSpeakerBalance.Mints, 1)
	require.Equal(t, usdfMint.Value, stored.MinimumSpeakerBalance.Mints[0].Value)
	require.NoError(t, protoutil.ProtoEqualError(params.Rules, stored.Rules()))

	got := e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, got.Result)
	require.NoError(t, protoutil.ProtoEqualError(params.Rules, got.Metadata.GetRules()))

	// The creator speaks in it.
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_OK, e.getMentionSuggestions(e.keys, chatID).Result)

	// A user holding the listener balance alone joins, but cannot speak.
	listener, listenerKeys := e.addUser()
	_, err = e.accounts.Bind(e.ctx, listener, listenerKeys.Proto())
	require.NoError(t, err)
	e.ocpBalance.setBalance(listenerKeys.Proto(), ocp_common.ToCoreMintQuarks(listenerRequirement))
	joined := e.mustJoinChat(listenerKeys, chatID)
	require.Equal(t, chatpb.JoinChatResponse_OK, joined.Result)
	require.NoError(t, protoutil.ProtoEqualError(params.Rules, joined.Chat.GetRules()))
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_DENIED, e.getMentionSuggestions(listenerKeys, chatID).Result)

	// Topped up to the speaker balance, they speak.
	e.ocpBalance.setBalance(listenerKeys.Proto(), ocp_common.ToCoreMintQuarks(speakerRequirement))
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_OK, e.getMentionSuggestions(listenerKeys, chatID).Result)
}

func testServer_StartChat_WithRules(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	const requirement = 100
	usdfMint := model.MustGenerateKeyPair().Proto()
	rules := &chatpb.Rules{Listener: []*chatpb.ListenerRules{
		{Kind: &chatpb.ListenerRules_Staff{Staff: &chatpb.StaffRequirement{}}},
		{Kind: &chatpb.ListenerRules_MinimumBalance{MinimumBalance: &chatpb.MinimumBalanceRequirement{
			Amount: &commonpb.FiatPaymentAmount{Currency: "usd", NativeAmount: requirement},
			Mints:  []*commonpb.PublicKey{usdfMint},
		}}},
	}}

	// A creator who satisfies every rule — staff, and holding the requirement
	// exactly — gets the group, with the rules stored and shown back.
	e.accounts.setStaff(e.userID, true)
	_, err := e.accounts.Bind(e.ctx, e.userID, e.keys.Proto())
	require.NoError(t, err)
	e.ocpBalance.setBalance(e.keys.Proto(), ocp_common.ToCoreMintQuarks(requirement))

	resp := e.mustStartGroupChat(e.keys, &chatpb.StartChatRequest_PublicGroupChatParameters{Title: "Staff Whales", Rules: rules})
	require.Equal(t, chatpb.StartChatResponse_OK, resp.Result)
	require.NoError(t, protoutil.ProtoEqualError(rules, resp.Chat.GetRules()))

	stored, err := s.GetChatByID(e.ctx, resp.Chat.ChatId)
	require.NoError(t, err)
	require.True(t, stored.IsStaffOnly)
	require.NotNil(t, stored.MinimumListenerBalance)
	require.Equal(t, "usd", stored.MinimumListenerBalance.Currency)
	require.Equal(t, float64(requirement), stored.MinimumListenerBalance.NativeAmount)
	require.Len(t, stored.MinimumListenerBalance.Mints, 1)
	require.Equal(t, usdfMint.Value, stored.MinimumListenerBalance.Mints[0].Value)
	require.NoError(t, protoutil.ProtoEqualError(rules, stored.Rules()))

	// The rules then gate the group as any other: a non-staff user cannot join.
	stranger, strangerKeys := e.addUser()
	joinResp := e.mustJoinChat(strangerKeys, resp.Chat.ChatId)
	require.Equal(t, chatpb.JoinChatResponse_RULES_NOT_SATISFIED, joinResp.Result)
	isMember, err := s.IsMember(e.ctx, resp.Chat.ChatId, stranger)
	require.NoError(t, err)
	require.False(t, isMember)
}

// testServer_StartChat_FiatMinimumBalance pins that a group can carry its
// minimum listener balance in any currency OCP can value: the creator is
// gated on OCP's valuation of their holdings in it, the requirement is stored
// and shown back as asked, and it gates joins the same way.
func testServer_StartChat_FiatMinimumBalance(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// 0.9 EUR per USDF, so 90 EUR is 100 USDF.
	const requirement = 90
	e.ocpBalance.setRate("eur", 0.9)
	params := &chatpb.StartChatRequest_PublicGroupChatParameters{
		Title: "Euro Whales",
		Rules: &chatpb.Rules{Listener: []*chatpb.ListenerRules{minimumBalanceRule("eur", requirement)}},
	}

	// A creator more than half a cent short is refused.
	_, err := e.accounts.Bind(e.ctx, e.userID, e.keys.Proto())
	require.NoError(t, err)
	e.ocpBalance.setBalance(e.keys.Proto(), ocp_common.ToCoreMintQuarks(100)-10_000)
	resp := e.mustStartGroupChat(e.keys, params)
	require.Equal(t, chatpb.StartChatResponse_RULES_NOT_SATISFIED, resp.Result)
	require.Nil(t, resp.Chat)

	// Holding the requirement, the creator gets the group, with the rule
	// stored and shown back in the currency asked for.
	e.ocpBalance.setBalance(e.keys.Proto(), ocp_common.ToCoreMintQuarks(100))
	resp = e.mustStartGroupChat(e.keys, params)
	require.Equal(t, chatpb.StartChatResponse_OK, resp.Result)
	require.NoError(t, protoutil.ProtoEqualError(params.Rules, resp.Chat.GetRules()))

	stored, err := s.GetChatByID(e.ctx, resp.Chat.ChatId)
	require.NoError(t, err)
	require.NotNil(t, stored.MinimumListenerBalance)
	require.Equal(t, "eur", stored.MinimumListenerBalance.Currency)
	require.Equal(t, float64(requirement), stored.MinimumListenerBalance.NativeAmount)
	require.NoError(t, protoutil.ProtoEqualError(params.Rules, stored.Rules()))

	// The rule gates joins on the same valuation.
	stranger, strangerKeys := e.addUser()
	_, err = e.accounts.Bind(e.ctx, stranger, strangerKeys.Proto())
	require.NoError(t, err)
	e.ocpBalance.setBalance(strangerKeys.Proto(), ocp_common.ToCoreMintQuarks(100)-10_000)
	joinResp := e.mustJoinChat(strangerKeys, resp.Chat.ChatId)
	require.Equal(t, chatpb.JoinChatResponse_RULES_NOT_SATISFIED, joinResp.Result)
	e.ocpBalance.setBalance(strangerKeys.Proto(), ocp_common.ToCoreMintQuarks(100))
	joinResp = e.mustJoinChat(strangerKeys, resp.Chat.ChatId)
	require.Equal(t, chatpb.JoinChatResponse_OK, joinResp.Result)
	isMember, err := s.IsMember(e.ctx, resp.Chat.ChatId, stranger)
	require.NoError(t, err)
	require.True(t, isMember)
}

func (e *serverEnv) muteChat(keys model.KeyPair, chatID *commonpb.ChatId, mute *chatpb.MuteState) (*chatpb.MuteChatResponse, error) {
	req := &chatpb.MuteChatRequest{ChatId: chatID, Mute: mute}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.MuteChat(e.ctx, req)
}

func (e *serverEnv) mustMuteChat(keys model.KeyPair, chatID *commonpb.ChatId, mute *chatpb.MuteState) *chatpb.MuteChatResponse {
	resp, err := e.muteChat(keys, chatID, mute)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) unmuteChat(keys model.KeyPair, chatID *commonpb.ChatId) (*chatpb.UnmuteChatResponse, error) {
	req := &chatpb.UnmuteChatRequest{ChatId: chatID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.UnmuteChat(e.ctx, req)
}

func (e *serverEnv) mustUnmuteChat(keys model.KeyPair, chatID *commonpb.ChatId) *chatpb.UnmuteChatResponse {
	resp, err := e.unmuteChat(keys, chatID)
	require.NoError(e.t, err)
	return resp
}

func muteUntil(until time.Time) *chatpb.MuteState {
	return &chatpb.MuteState{Duration: &chatpb.MuteState_Until{Until: timestamppb.New(until)}}
}

func muteForever() *chatpb.MuteState {
	return &chatpb.MuteState{Duration: &chatpb.MuteState_Forever_{Forever: &chatpb.MuteState_Forever{}}}
}

// viewerState is the ViewerState every carrier projects in a group: the
// mute under settings (nil for none), the permissions, and the version.
// isCreator is whether the viewer is the group's creator while a member,
// which grants can_edit, can_pin and both moderation permissions together
// (see chat.Chat.PermissionsFor).
func viewerState(version uint64, mute *chatpb.MuteState, isCreator bool) *chatpb.ViewerState {
	permissions := &chatpb.ViewerState_Permissions{CanEdit: isCreator, CanPin: isCreator}
	if isCreator {
		permissions.Moderation = &chatpb.ViewerState_ModerationPermissions{CanSilence: true, CanBan: true}
	}
	return &chatpb.ViewerState{
		Settings:    &chatpb.ViewerState_Settings{Mute: mute},
		Permissions: permissions,
		Version:     version,
	}
}

// memberViewerState is what a group member who has never written a record,
// and may not edit the chat, is shown: the zero record with no permissions.
var memberViewerState = viewerState(0, nil, false)

// dmViewerState is the ViewerState a DM's member is shown: the mute under
// settings (nil for none) and the version, with can_pin alone among the
// permissions, since either member of a DM may pin and no one edits or
// moderates one (see chat.Chat.PermissionsFor).
func dmViewerState(version uint64, mute *chatpb.MuteState) *chatpb.ViewerState {
	state := viewerState(version, mute, false)
	state.Permissions.CanPin = true
	return state
}

// dmMemberViewerState is what a DM's member who has never written a record
// is shown (see dmViewerState).
var dmMemberViewerState = dmViewerState(0, nil)

// viewerStateUpdatesOnUserTopic returns every ViewerStateChanged for chatID
// published on userID's topic, in publish order.
func (e *serverEnv) viewerStateUpdatesOnUserTopic(userID *commonpb.UserId, chatID *commonpb.ChatId) []*chatpb.ViewerState {
	var states []*chatpb.ViewerState
	for _, ev := range e.userObserver.GetEvents(func(k *commonpb.UserId) bool { return bytes.Equal(k.Value, userID.Value) }) {
		update := ev.Event.GetChatUpdate()
		if !bytes.Equal(update.GetChat().GetValue(), chatID.Value) {
			continue
		}
		for _, md := range update.GetMetadataUpdates() {
			if changed := md.GetViewerStateChanged(); changed != nil {
				states = append(states, changed.GetViewerState())
			}
		}
	}
	return states
}

// waitForViewerStateUpdates waits for at least n ViewerStateChanged for chatID
// on userID's topic, then asserts, after giving the bus a moment more, that no
// more than n arrived and that the chat's own topic carried none: the state is
// the user's alone.
func (e *serverEnv) waitForViewerStateUpdates(userID *commonpb.UserId, chatID *commonpb.ChatId, n int) []*chatpb.ViewerState {
	e.userObserver.WaitFor(e.t, func([]*event.KeyAndEvent[*commonpb.UserId, *eventpb.Event]) bool {
		return len(e.viewerStateUpdatesOnUserTopic(userID, chatID)) >= n
	})
	time.Sleep(100 * time.Millisecond)
	states := e.viewerStateUpdatesOnUserTopic(userID, chatID)
	require.Len(e.t, states, n)
	for _, ev := range e.chatObserver.GetEvents(func(k *commonpb.ChatId) bool { return bytes.Equal(k.Value, chatID.Value) }) {
		for _, md := range ev.Event.Event.GetChatUpdate().GetMetadataUpdates() {
			require.Nil(e.t, md.GetViewerStateChanged(), "viewer state must never be published on the chat topic")
		}
	}
	return states
}

func testServer_MuteChat_Lifecycle(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	peer, _ := e.addUser()
	chatID := e.putDMWithPeer(chatpb.ChatType_CONTACT_DM, peer, at(1))

	// A chat the viewer has never touched carries the zero state for a member:
	// no mute, can_pin alone among the permissions on a DM, version zero.
	resp := e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.NoError(t, protoutil.ProtoEqualError(dmMemberViewerState, resp.Metadata.ViewerState))

	// A timed mute, recorded at second precision, is a first transition. The
	// end is chosen on a whole second so the sub-second variant below lands
	// on the same recorded value.
	until := time.Now().Add(time.Hour).Truncate(time.Second)
	muted := e.mustMuteChat(e.keys, chatID, muteUntil(until))
	require.Equal(t, chatpb.MuteChatResponse_OK, muted.Result)
	want := dmViewerState(1, muteUntil(until))
	require.NoError(t, protoutil.ProtoEqualError(want, muted.ViewerState))

	// The same state is what a read hydrates onto the chat...
	resp = e.getChat(e.keys, chatID)
	require.NoError(t, protoutil.ProtoEqualError(want, resp.Metadata.ViewerState))

	// ...and what the caller's other devices are told, once.
	states := e.waitForViewerStateUpdates(e.userID, chatID, 1)
	require.NoError(t, protoutil.ProtoEqualError(want, states[0]))

	// The mute already recorded is a no-op: same state, same version, and
	// nothing published.
	muted = e.mustMuteChat(e.keys, chatID, muteUntil(until.Add(500*time.Millisecond)))
	require.Equal(t, chatpb.MuteChatResponse_OK, muted.Result)
	require.NoError(t, protoutil.ProtoEqualError(want, muted.ViewerState))
	e.waitForViewerStateUpdates(e.userID, chatID, 1)

	// Indefinite where timed was is a real change.
	muted = e.mustMuteChat(e.keys, chatID, muteForever())
	require.Equal(t, chatpb.MuteChatResponse_OK, muted.Result)
	want = dmViewerState(2, muteForever())
	require.NoError(t, protoutil.ProtoEqualError(want, muted.ViewerState))
	states = e.waitForViewerStateUpdates(e.userID, chatID, 2)
	require.NoError(t, protoutil.ProtoEqualError(want, states[1]))

	// A clear keeps the record and its version, with no mute under settings.
	unmuted := e.mustUnmuteChat(e.keys, chatID)
	require.Equal(t, chatpb.UnmuteChatResponse_OK, unmuted.Result)
	want = dmViewerState(3, nil)
	require.NoError(t, protoutil.ProtoEqualError(want, unmuted.ViewerState))
	resp = e.getChat(e.keys, chatID)
	require.NoError(t, protoutil.ProtoEqualError(want, resp.Metadata.ViewerState))
	states = e.waitForViewerStateUpdates(e.userID, chatID, 3)
	require.NoError(t, protoutil.ProtoEqualError(want, states[2]))

	// Clearing again is the no-op.
	unmuted = e.mustUnmuteChat(e.keys, chatID)
	require.Equal(t, chatpb.UnmuteChatResponse_OK, unmuted.Result)
	require.NoError(t, protoutil.ProtoEqualError(want, unmuted.ViewerState))
	e.waitForViewerStateUpdates(e.userID, chatID, 3)

	// The peer is told none of it.
	require.Empty(t, e.viewerStateUpdatesOnUserTopic(peer, chatID))
}

// testServer_MuteChat_Group is the lifecycle on a group: a member's mute is
// recorded, hydrated onto GetChat and the group feed for them alone — another
// member reads the same group with no viewer state, and is told nothing —
// and cleared like a DM's.
func testServer_MuteChat_Group(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	otherID, otherKeys := e.addUser()
	muted := e.putGroup("Muted", at(2), otherID)
	quiet := e.putGroup("Quiet", at(1), otherID)

	resp := e.getChat(e.keys, muted)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.NoError(t, protoutil.ProtoEqualError(memberViewerState, resp.Metadata.ViewerState))

	until := time.Now().Add(time.Hour).Truncate(time.Second)
	mutedResp := e.mustMuteChat(e.keys, muted, muteUntil(until))
	require.Equal(t, chatpb.MuteChatResponse_OK, mutedResp.Result)
	want := viewerState(1, muteUntil(until), false)
	require.NoError(t, protoutil.ProtoEqualError(want, mutedResp.ViewerState))

	// Hydrated onto the record for the viewer who set it...
	resp = e.getChat(e.keys, muted)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.NoError(t, protoutil.ProtoEqualError(want, resp.Metadata.ViewerState))

	// ...and onto the group feed, on that group alone.
	feed := e.mustGetGroupFeed(&commonpb.QueryOptions{})
	require.Len(t, feed.Chats, 2)
	byID := make(map[string]*chatpb.Metadata, len(feed.Chats))
	for _, md := range feed.Chats {
		byID[string(md.ChatId.Value)] = md
	}
	require.NoError(t, protoutil.ProtoEqualError(want, byID[string(muted.Value)].ViewerState))
	require.NoError(t, protoutil.ProtoEqualError(memberViewerState, byID[string(quiet.Value)].ViewerState))

	// The other member reads the same group with the zero state on it, and
	// hears nothing on their topic; the chat topic carries nothing either.
	resp = e.getChat(otherKeys, muted)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.NoError(t, protoutil.ProtoEqualError(memberViewerState, resp.Metadata.ViewerState))
	states := e.waitForViewerStateUpdates(e.userID, muted, 1)
	require.NoError(t, protoutil.ProtoEqualError(want, states[0]))
	require.Empty(t, e.viewerStateUpdatesOnUserTopic(otherID, muted))

	// Replace, then clear, as on a DM.
	mutedResp = e.mustMuteChat(e.keys, muted, muteForever())
	require.Equal(t, chatpb.MuteChatResponse_OK, mutedResp.Result)
	want = viewerState(2, muteForever(), false)
	require.NoError(t, protoutil.ProtoEqualError(want, mutedResp.ViewerState))

	unmuted := e.mustUnmuteChat(e.keys, muted)
	require.Equal(t, chatpb.UnmuteChatResponse_OK, unmuted.Result)
	want = viewerState(3, nil, false)
	require.NoError(t, protoutil.ProtoEqualError(want, unmuted.ViewerState))
	resp = e.getChat(e.keys, muted)
	require.NoError(t, protoutil.ProtoEqualError(want, resp.Metadata.ViewerState))
	states = e.waitForViewerStateUpdates(e.userID, muted, 3)
	require.NoError(t, protoutil.ProtoEqualError(want, states[2]))
	require.Empty(t, e.viewerStateUpdatesOnUserTopic(otherID, muted))
}

func testServer_MuteChat_Gates(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	future := muteUntil(time.Now().Add(time.Hour))

	// A chat that does not exist.
	resp, err := e.muteChat(e.keys, generateDmChatID(), future)
	require.NoError(t, err)
	require.Equal(t, chatpb.MuteChatResponse_NOT_FOUND, resp.Result)
	unmuteResp, err := e.unmuteChat(e.keys, generateDmChatID())
	require.NoError(t, err)
	require.Equal(t, chatpb.UnmuteChatResponse_NOT_FOUND, unmuteResp.Result)

	// A DM between two other users.
	a := model.MustGenerateUserID()
	b := model.MustGenerateUserID()
	othersDM := generateDmChatID()
	require.NoError(t, s.PutChat(e.ctx, &chat.Chat{ID: othersDM, Type: chatpb.ChatType_CONTACT_DM, Members: []*commonpb.UserId{a, b}, LastActivity: at(1)}))
	resp, err = e.muteChat(e.keys, othersDM, future)
	require.NoError(t, err)
	require.Equal(t, chatpb.MuteChatResponse_DENIED, resp.Result)
	unmuteResp, err = e.unmuteChat(e.keys, othersDM)
	require.NoError(t, err)
	require.Equal(t, chatpb.UnmuteChatResponse_DENIED, unmuteResp.Result)

	// A group the caller is not a member of — even one they could read.
	group := e.putGroup("Open", at(1))
	strangerID, strangerKeys := e.addUser()
	resp, err = e.muteChat(strangerKeys, group, future)
	require.NoError(t, err)
	require.Equal(t, chatpb.MuteChatResponse_DENIED, resp.Result)
	unmuteResp, err = e.unmuteChat(strangerKeys, group)
	require.NoError(t, err)
	require.Equal(t, chatpb.UnmuteChatResponse_DENIED, unmuteResp.Result)

	// A member of it may.
	e.mustJoinChat(strangerKeys, group)
	resp, err = e.muteChat(strangerKeys, group, future)
	require.NoError(t, err)
	require.Equal(t, chatpb.MuteChatResponse_OK, resp.Result)
	e.waitForViewerStateUpdates(strangerID, group, 1)

	// A timed mute must end in the future; one at the boundary of the
	// recordable range is rejected too. Neither leaves a record behind.
	dm := e.putDM(at(1))
	for _, mute := range []*chatpb.MuteState{
		muteUntil(time.Now().Add(-time.Second)),
		muteUntil(time.Time{}),
		muteUntil(chat.MaxMuteUntil),
		{},
		nil,
	} {
		_, err := e.muteChat(e.keys, dm, mute)
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, status.Code(err), "mute %v", mute)
	}
	got := e.getChat(e.keys, dm)
	require.NoError(t, protoutil.ProtoEqualError(dmMemberViewerState, got.Metadata.ViewerState))
}

func testServer_LeaveChat_ClearsMute(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	other := model.MustGenerateUserID()
	chatID := e.putGroup("Group", at(1), other)

	muted := e.mustMuteChat(e.keys, chatID, muteForever())
	require.Equal(t, chatpb.MuteChatResponse_OK, muted.Result)
	e.waitForViewerStateUpdates(e.userID, chatID, 1)

	// Leaving clears the mute: the record and its version stay, the mute is
	// gone, and the caller's other devices are told — a second transition on
	// the user topic, none on the chat topic, alongside the roster update.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, chatID).Result)
	cleared := viewerState(2, nil, false)
	states := e.waitForViewerStateUpdates(e.userID, chatID, 2)
	require.NoError(t, protoutil.ProtoEqualError(cleared, states[1]))

	// A departed member reads the group's record with no state on it: the
	// record persists, but a non-member is shown none of it.
	resp := e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Empty(t, resp.Metadata.Members)
	require.Nil(t, resp.Metadata.ViewerState)

	// A departed member cannot mute or unmute.
	denied, err := e.muteChat(e.keys, chatID, muteForever())
	require.NoError(t, err)
	require.Equal(t, chatpb.MuteChatResponse_DENIED, denied.Result)
	deniedUnmute, err := e.unmuteChat(e.keys, chatID)
	require.NoError(t, err)
	require.Equal(t, chatpb.UnmuteChatResponse_DENIED, deniedUnmute.Result)

	// Leaving again — a roster no-op — finds nothing to clear and publishes
	// nothing.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, chatID).Result)
	e.waitForViewerStateUpdates(e.userID, chatID, 2)

	// Rejoining starts unmuted, at the version the clear left.
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(e.keys, chatID).Result)
	resp = e.getChat(e.keys, chatID)
	require.NoError(t, protoutil.ProtoEqualError(cleared, resp.Metadata.ViewerState))

	// A leave with no mute recorded at all touches nothing.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, chatID).Result)
	e.waitForViewerStateUpdates(e.userID, chatID, 2)
}

// testServer_GetChat_ViewerState_LapsedMuteReturned lets a short mute lapse and
// reads the chat again: the record comes back exactly as written, the past
// end included, because a version names one state and the clock is the
// reader's to apply.
func testServer_GetChat_ViewerState_LapsedMuteReturned(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	chatID := e.putDM(at(1))

	// Whole seconds, as a mute is stored, and at least a second away, so the
	// mute still ends in the future when the request arrives.
	until := time.Now().Truncate(time.Second).Add(2 * time.Second)
	muted := e.mustMuteChat(e.keys, chatID, muteUntil(until))
	require.Equal(t, chatpb.MuteChatResponse_OK, muted.Result)
	want := dmViewerState(1, muteUntil(until))
	require.NoError(t, protoutil.ProtoEqualError(want, muted.ViewerState))

	time.Sleep(time.Until(until) + 500*time.Millisecond)

	resp := e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.NoError(t, protoutil.ProtoEqualError(want, resp.Metadata.ViewerState))

	// Lapsed is still recorded: clearing it is a real transition.
	unmuted := e.mustUnmuteChat(e.keys, chatID)
	require.NoError(t, protoutil.ProtoEqualError(dmViewerState(2, nil), unmuted.ViewerState))
}

func testServer_GetDmChatFeed_ViewerState(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	mutedDM := e.putDM(at(1))
	quietDM := e.putDM(at(2))
	require.Equal(t, chatpb.MuteChatResponse_OK, e.mustMuteChat(e.keys, mutedDM, muteForever()).Result)

	resp := e.getDmFeed(&commonpb.QueryOptions{})
	require.Equal(t, chatpb.GetDmChatFeedResponse_OK, resp.Result)
	require.Len(t, resp.Chats, 2)
	byID := make(map[string]*chatpb.Metadata, len(resp.Chats))
	for _, md := range resp.Chats {
		byID[string(md.ChatId.Value)] = md
	}
	want := dmViewerState(1, muteForever())
	require.NoError(t, protoutil.ProtoEqualError(want, byID[string(mutedDM.Value)].ViewerState))
	require.NoError(t, protoutil.ProtoEqualError(dmMemberViewerState, byID[string(quietDM.Value)].ViewerState))
}

// putOwnedGroup persists a group the env user created and is a member of,
// alongside the given others, with the given title and picture (nil for
// none).
func (e *serverEnv) putOwnedGroup(title string, pictureBlobID *blobpb.BlobId, others ...*commonpb.UserId) *commonpb.ChatId {
	chatID := chat.MustGenerateGroupChatID()
	require.NoError(e.t, e.store.PutChat(e.ctx, &chat.Chat{
		ID:                   chatID,
		Type:                 chatpb.ChatType_GROUP,
		Members:              append([]*commonpb.UserId{e.userID}, others...),
		Title:                title,
		CreatorID:            e.userID,
		ProfilePictureBlobID: pictureBlobID,
		LastActivity:         at(1),
	}))
	return chatID
}

func (e *serverEnv) editChat(keys model.KeyPair, chatID *commonpb.ChatId, title *string, picture *blobpb.BlobId) (*chatpb.EditChatResponse, error) {
	req := &chatpb.EditChatRequest{ChatId: chatID}
	if title != nil {
		req.Title = &chatpb.EditChatRequest_Title{Value: *title}
	}
	if picture != nil {
		req.ProfilePicture = &chatpb.EditChatRequest_ProfilePicture{BlobId: picture}
	}
	return e.sendEditChat(keys, req)
}

// sendEditChat signs and sends req as is, for a test that sets fields
// editChat does not take.
func (e *serverEnv) sendEditChat(keys model.KeyPair, req *chatpb.EditChatRequest) (*chatpb.EditChatResponse, error) {
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.EditChat(e.ctx, req)
}

func (e *serverEnv) mustSendEditChat(keys model.KeyPair, req *chatpb.EditChatRequest) *chatpb.EditChatResponse {
	resp, err := e.sendEditChat(keys, req)
	require.NoError(e.t, err)
	return resp
}

// describeGroup gives an existing group a description and a resolvable cover
// picture straight through the store, returning the cover's rendition set.
func (e *serverEnv) describeGroup(chatID *commonpb.ChatId, description string, cover *blobpb.BlobId) []*blobpb.Rendition {
	renditions := e.media.setRenditions(cover)
	require.NoError(e.t, e.store.EditGroup(e.ctx, chatID, chat.GroupEdit{Description: &description, CoverPictureBlobID: cover}))
	return renditions
}

func (e *serverEnv) mustEditChat(keys model.KeyPair, chatID *commonpb.ChatId, title *string, picture *blobpb.BlobId) *chatpb.EditChatResponse {
	resp, err := e.editChat(keys, chatID, title, picture)
	require.NoError(e.t, err)
	return resp
}

// metadataUpdatesOnChatTopic returns every MetadataUpdate published on
// chatID's topic, grouped by the event that carried them, in publish order,
// paired with the users each event excluded.
func (e *serverEnv) metadataUpdatesOnChatTopic(chatID *commonpb.ChatId) (updates [][]*chatpb.MetadataUpdate, excludes [][]*commonpb.UserId) {
	for _, ev := range e.chatObserver.GetEvents(func(k *commonpb.ChatId) bool { return bytes.Equal(k.Value, chatID.Value) }) {
		mds := ev.Event.Event.GetChatUpdate().GetMetadataUpdates()
		if len(mds) == 0 {
			continue
		}
		require.Equal(e.t, chatID.Value, ev.Event.Event.GetChatUpdate().GetChat().GetValue())
		updates = append(updates, mds)
		excludes = append(excludes, ev.Event.ExcludeUserIds)
	}
	return updates, excludes
}

// waitForMetadataUpdates waits for at least n events carrying metadata
// updates on chatID's topic, then asserts, after giving the bus a moment
// more, that exactly n arrived, each excluding no one, and returns them.
func (e *serverEnv) waitForMetadataUpdates(chatID *commonpb.ChatId, n int) [][]*chatpb.MetadataUpdate {
	e.chatObserver.WaitFor(e.t, func([]*event.KeyAndEvent[*commonpb.ChatId, *eventpb.ChatEvent]) bool {
		updates, _ := e.metadataUpdatesOnChatTopic(chatID)
		return len(updates) >= n
	})
	time.Sleep(100 * time.Millisecond)
	updates, excludes := e.metadataUpdatesOnChatTopic(chatID)
	require.Len(e.t, updates, n)
	for _, ex := range excludes {
		require.Empty(e.t, ex, "an edit reaches every member, the editor included")
	}
	return updates
}

func testServer_EditChat_Title(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	other, otherKeys := e.addUser()
	chatID := e.putOwnedGroup("Before", nil, other)

	title := "After"
	resp := e.mustEditChat(e.keys, chatID, &title, nil)
	require.Equal(t, chatpb.EditChatResponse_OK, resp.Result)
	require.Equal(t, moderationpb.FlaggedCategory_NONE, resp.FlaggedCategory)

	// The response is the record after the edit, as the editor sees it: the
	// new title, themselves as the hydrated member, and their permissions.
	md := resp.Chat
	require.NotNil(t, md)
	require.Equal(t, chatID.Value, md.ChatId.Value)
	require.Equal(t, "After", md.Title)
	require.Nil(t, md.ProfilePicture)
	require.Len(t, md.Members, 1)
	require.Equal(t, e.userID.Value, md.Members[0].UserId.Value)
	require.NoError(t, protoutil.ProtoEqualError(viewerState(0, nil, true), md.ViewerState))

	// The title went through the moderator and landed in the store, where
	// every other member reads it.
	require.Equal(t, "After", e.moderator.classifiedTitle)
	stored, err := s.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, "After", stored.Title)
	require.Equal(t, "After", e.getChat(otherKeys, chatID).Metadata.Title)

	// One event on the chat topic, excluding no one, carrying the title change
	// alone. Nothing on any user topic.
	updates := e.waitForMetadataUpdates(chatID, 1)
	require.Len(t, updates[0], 1)
	require.Equal(t, "After", updates[0][0].GetTitleChanged().GetNewTitle())
	require.Empty(t, e.viewerStateUpdatesOnUserTopic(e.userID, chatID))
	require.Empty(t, e.rosterUpdatesOnUserTopic(e.userID, chatID))
	require.Empty(t, e.rosterUpdatesOnUserTopic(other, chatID))
}

func testServer_EditChat_Picture(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	original := &blobpb.BlobId{Value: []byte("group-picture-01")}
	e.media.setRenditions(original)
	chatID := e.putOwnedGroup("Group", original, model.MustGenerateUserID())

	replacement := &blobpb.BlobId{Value: []byte("group-picture-02")}
	renditions := e.media.setAttachable(replacement)

	resp := e.mustEditChat(e.keys, chatID, nil, replacement)
	require.Equal(t, chatpb.EditChatResponse_OK, resp.Result)

	// The picture was attached against the group and comes back hydrated with
	// the full rendition set; the title was left alone.
	require.Equal(t, [][]byte{replacement.Value}, e.media.attachedTo(chatID))
	md := resp.Chat
	require.Equal(t, "Group", md.Title)
	require.NotNil(t, md.ProfilePicture)
	require.Len(t, md.ProfilePicture.Renditions, len(renditions))
	require.Equal(t, replacement.Value, md.ProfilePicture.Renditions[0].GetBlobId().GetValue())
	require.NotNil(t, md.ProfilePicture.Renditions[0].Blob)

	stored, err := s.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, replacement.Value, stored.ProfilePictureBlobID.GetValue())
	require.Equal(t, "Group", stored.Title)

	// The announcement carries the same hydrated picture, and no title.
	updates := e.waitForMetadataUpdates(chatID, 1)
	require.Len(t, updates[0], 1)
	require.NoError(t, protoutil.ProtoEqualError(md.ProfilePicture, updates[0][0].GetProfilePictureChanged().GetNewProfilePicture()))

	// No title was moderated.
	require.Empty(t, e.moderator.classifiedTitle)
}

func testServer_EditChat_Both(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	chatID := e.putOwnedGroup("Before", nil)
	picture := &blobpb.BlobId{Value: []byte("group-picture-01")}
	e.media.setAttachable(picture)

	title := "After"
	resp := e.mustEditChat(e.keys, chatID, &title, picture)
	require.Equal(t, chatpb.EditChatResponse_OK, resp.Result)
	require.Equal(t, "After", resp.Chat.Title)
	require.Equal(t, picture.Value, resp.Chat.GetProfilePicture().GetRenditions()[0].GetBlobId().GetValue())

	stored, err := s.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, "After", stored.Title)
	require.Equal(t, picture.Value, stored.ProfilePictureBlobID.GetValue())

	// One event, one update per field, title first.
	updates := e.waitForMetadataUpdates(chatID, 1)
	require.Len(t, updates[0], 2)
	require.Equal(t, "After", updates[0][0].GetTitleChanged().GetNewTitle())
	require.NoError(t, protoutil.ProtoEqualError(resp.Chat.ProfilePicture, updates[0][1].GetProfilePictureChanged().GetNewProfilePicture()))
}

func testServer_StartChat_WithDescriptionAndCover(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.fundEnvUser(startChatMinimumBalance)

	profile := &blobpb.BlobId{Value: []byte("group-picture-01")}
	cover := &blobpb.BlobId{Value: []byte("group-cover-0001")}
	e.media.setAttachable(profile)
	coverRenditions := e.media.setAttachable(cover)

	params := groupParams("Described Group")
	params.Description = "Hikes every Sunday.\nAll welcome."
	params.ProfilePicture = profile
	params.CoverPicture = cover
	resp := e.mustStartGroupChat(e.keys, params)
	require.Equal(t, chatpb.StartChatResponse_OK, resp.Result)
	md := resp.Chat

	// Both pictures were attached against the new group, profile picture
	// first, and the cover comes back hydrated beside the description.
	require.Equal(t, [][]byte{profile.Value, cover.Value}, e.media.attachedTo(md.ChatId))
	require.Equal(t, "Hikes every Sunday.\nAll welcome.", md.Description)
	require.Equal(t, profile.Value, md.GetProfilePicture().GetRenditions()[0].GetBlobId().GetValue())
	require.Len(t, md.GetCoverPicture().GetRenditions(), len(coverRenditions))
	require.Equal(t, cover.Value, md.GetCoverPicture().GetRenditions()[0].GetBlobId().GetValue())
	require.NotNil(t, md.GetCoverPicture().GetRenditions()[0].Blob)

	// The description went through the text classifier, as written.
	require.Contains(t, e.moderator.classifiedTexts, "Hikes every Sunday.\nAll welcome.")

	stored, err := s.GetChatByID(e.ctx, md.ChatId)
	require.NoError(t, err)
	require.Equal(t, "Hikes every Sunday.\nAll welcome.", stored.Description)
	require.Equal(t, profile.Value, stored.ProfilePictureBlobID.GetValue())
	require.Equal(t, cover.Value, stored.CoverPictureBlobID.GetValue())

	// The creation's announcement carries them too.
	updates := e.rosterUpdatesOnUserTopic(e.userID, md.ChatId)
	require.Len(t, updates, 1)
	announced := updates[0].GetMemberJoined().GetMetadata()
	require.Equal(t, md.Description, announced.GetDescription())
	require.NoError(t, protoutil.ProtoEqualError(md.CoverPicture, announced.GetCoverPicture()))

	// A private group takes both the same way.
	privateCover := &blobpb.BlobId{Value: []byte("group-cover-0002")}
	e.media.setAttachable(privateCover)
	privateParams := privateGroupParams("Private Described")
	privateParams.Description = "Invite only"
	privateParams.CoverPicture = privateCover
	resp = e.mustStartPrivateGroupChat(e.keys, newIdempotencyKey(), privateParams)
	require.Equal(t, chatpb.StartChatResponse_OK, resp.Result)
	require.Equal(t, "Invite only", resp.Chat.Description)
	require.Equal(t, privateCover.Value, resp.Chat.GetCoverPicture().GetRenditions()[0].GetBlobId().GetValue())
	require.Nil(t, resp.Chat.ProfilePicture)
	require.Equal(t, [][]byte{privateCover.Value}, e.media.attachedTo(resp.Chat.ChatId))

	// A group created without either has neither, and moderates no
	// description.
	e.moderator.classifiedTexts = nil
	resp = e.mustStartGroupChat(e.keys, groupParams("Bare"))
	require.Equal(t, chatpb.StartChatResponse_OK, resp.Result)
	require.Empty(t, resp.Chat.Description)
	require.Nil(t, resp.Chat.CoverPicture)
	require.Equal(t, []string{"Bare"}, e.moderator.classifiedTexts)
}

func testServer_StartChat_CoverPictureNotAccepted(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.fundEnvUser(startChatMinimumBalance)

	// A cover the media domain will not attach refuses the whole creation,
	// with its own result, after the profile picture beside it was attached:
	// no group is written.
	profile := &blobpb.BlobId{Value: []byte("group-picture-01")}
	e.media.setAttachable(profile)
	params := groupParams("Cover Group")
	params.ProfilePicture = profile
	params.CoverPicture = &blobpb.BlobId{Value: []byte("not-attachable01")}
	resp := e.mustStartGroupChat(e.keys, params)
	require.Equal(t, chatpb.StartChatResponse_COVER_PICTURE_BLOB_NOT_ACCEPTED, resp.Result)
	require.Nil(t, resp.Chat)

	groups, err := s.GetGroupChatsForUser(e.ctx, e.userID)
	require.NoError(t, err)
	require.Empty(t, groups)

	// A profile picture refused first is reported as such, and the cover is
	// never tried.
	cover := &blobpb.BlobId{Value: []byte("group-cover-0001")}
	e.media.setAttachable(cover)
	params.ProfilePicture = &blobpb.BlobId{Value: []byte("not-attachable02")}
	params.CoverPicture = cover
	resp = e.mustStartGroupChat(e.keys, params)
	require.Equal(t, chatpb.StartChatResponse_PROFILE_PICTURE_BLOB_NOT_ACCEPTED, resp.Result)
	for _, attached := range e.media.chatPictures {
		for _, id := range attached {
			require.NotEqual(t, cover.Value, id.Value)
		}
	}
}

func testServer_StartChat_Description(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.fundEnvUser(startChatMinimumBalance)

	noGroups := func() {
		t.Helper()
		groups, err := s.GetGroupChatsForUser(e.ctx, e.userID)
		require.NoError(t, err)
		require.Empty(t, groups)
	}

	cover := &blobpb.BlobId{Value: []byte("group-cover-0001")}
	e.media.setAttachable(cover)

	// A flagged description is refused with its category, after the title
	// passed and before the cover is attached: nothing is written.
	e.moderator.flaggedTexts = map[string][]string{"Buy my coin": {"gibberish", "solicitation"}}
	params := groupParams("Fine Title")
	params.Description = "Buy my coin"
	params.CoverPicture = cover
	resp := e.mustStartGroupChat(e.keys, params)
	require.Equal(t, chatpb.StartChatResponse_DESCRIPTION_MODERATED, resp.Result)
	require.Equal(t, moderationpb.FlaggedCategory_SPAM, resp.FlaggedCategory)
	require.Nil(t, resp.Chat)
	require.Empty(t, e.media.chatPictures)
	require.Equal(t, "Fine Title", e.moderator.classifiedTitle)
	noGroups()

	// A flagged title is reported before the description is looked at.
	e.moderator.flaggedTexts["Bad Title"] = []string{"hate"}
	e.moderator.classifiedTexts = nil
	params.Title = "Bad Title"
	resp = e.mustStartGroupChat(e.keys, params)
	require.Equal(t, chatpb.StartChatResponse_TITLE_MODERATED, resp.Result)
	require.Equal(t, []string{"Bad Title"}, e.moderator.classifiedTexts)
	noGroups()

	// A description that cannot be classified is never persisted — a language
	// the classifier cannot score included, which a title would survive.
	e.moderator.flaggedTexts = nil
	params.Title = "Fine Title"
	for _, err := range []error{moderation.ErrUnsupportedLanguage, errors.New("classifier down")} {
		e.moderator.textErrs = map[string]error{"Buy my coin": err}
		_, err := e.startGroupChat(e.keys, params)
		require.Equal(t, codes.Internal, status.Code(err))
		noGroups()
	}
	e.moderator.textErrs = nil

	// A description no group may carry is malformed, refused before anything
	// is moderated.
	e.moderator.classifiedTexts = nil
	for _, invalid := range []string{"   ", "tab\there", "nul\x00"} {
		params.Description = invalid
		_, err := e.startGroupChat(e.keys, params)
		require.Equal(t, codes.InvalidArgument, status.Code(err), "description: %q", invalid)
	}
	require.Empty(t, e.moderator.classifiedTexts)
	noGroups()
}

func testServer_EditChat_Description(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	other, otherKeys := e.addUser()
	chatID := e.putOwnedGroup("Group", nil, other)
	describe := func(value string) *chatpb.EditChatRequest {
		return &chatpb.EditChatRequest{ChatId: chatID, Description: &chatpb.EditChatRequest_Description{Value: value}}
	}

	// Set: moderated, written, returned, and announced alone.
	resp := e.mustSendEditChat(e.keys, describe("Sunday hikes"))
	require.Equal(t, chatpb.EditChatResponse_OK, resp.Result)
	require.Equal(t, "Sunday hikes", resp.Chat.Description)
	require.Equal(t, "Group", resp.Chat.Title)
	require.Equal(t, []string{"Sunday hikes"}, e.moderator.classifiedTexts)
	require.Empty(t, e.moderator.classifiedTitle)
	stored, err := s.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, "Sunday hikes", stored.Description)
	require.Equal(t, "Sunday hikes", e.getChat(otherKeys, chatID).Metadata.Description)

	updates := e.waitForMetadataUpdates(chatID, 1)
	require.Len(t, updates[0], 1)
	require.NotNil(t, updates[0][0].GetDescriptionChanged())
	require.Equal(t, "Sunday hikes", updates[0][0].GetDescriptionChanged().GetNewDescription())

	// Setting it again is the no-op, moderating nothing.
	e.moderator.classifiedTexts = nil
	resp = e.mustSendEditChat(e.keys, describe("Sunday hikes"))
	require.Equal(t, chatpb.EditChatResponse_OK, resp.Result)
	require.Empty(t, e.moderator.classifiedTexts)

	// The empty one clears it, with nothing to moderate, and is announced
	// as an empty description.
	resp = e.mustSendEditChat(e.keys, describe(""))
	require.Equal(t, chatpb.EditChatResponse_OK, resp.Result)
	require.Empty(t, resp.Chat.Description)
	require.Empty(t, e.moderator.classifiedTexts)
	stored, err = s.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	require.Empty(t, stored.Description)
	updates = e.waitForMetadataUpdates(chatID, 2)
	require.Len(t, updates[1], 1)
	require.NotNil(t, updates[1][0].GetDescriptionChanged())
	require.Empty(t, updates[1][0].GetDescriptionChanged().GetNewDescription())

	// Clearing one that is not set is the no-op too.
	resp = e.mustSendEditChat(e.keys, describe(""))
	require.Equal(t, chatpb.EditChatResponse_OK, resp.Result)

	// A flagged description refuses the whole edit, the title beside it
	// included.
	e.moderator.flaggedTexts = map[string][]string{"Buy my coin": {"gibberish", "solicitation"}}
	req := describe("Buy my coin")
	req.Title = &chatpb.EditChatRequest_Title{Value: "Retitled"}
	resp = e.mustSendEditChat(e.keys, req)
	require.Equal(t, chatpb.EditChatResponse_DESCRIPTION_MODERATED, resp.Result)
	require.Equal(t, moderationpb.FlaggedCategory_SPAM, resp.FlaggedCategory)
	require.Nil(t, resp.Chat)

	// A description that cannot be classified fails the RPC, whatever the
	// reason.
	e.moderator.flaggedTexts = nil
	e.moderator.textErrs = map[string]error{"Hola amigos": moderation.ErrUnsupportedLanguage}
	_, err = e.sendEditChat(e.keys, describe("Hola amigos"))
	require.Equal(t, codes.Internal, status.Code(err))
	e.moderator.textErrs = nil

	// A description no group may carry is malformed, refused before the
	// record is read: even a group that does not exist answers so.
	_, err = e.sendEditChat(e.keys, describe("  \n "))
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = e.sendEditChat(e.keys, &chatpb.EditChatRequest{
		ChatId:      chat.MustGenerateGroupChatID(),
		Description: &chatpb.EditChatRequest_Description{Value: "tab\there"},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// None of the refusals reached the record or the stream.
	stored, err = s.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	require.Empty(t, stored.Description)
	require.Equal(t, "Group", stored.Title)
	time.Sleep(100 * time.Millisecond)
	updates, _ = e.metadataUpdatesOnChatTopic(chatID)
	require.Len(t, updates, 2)
}

func testServer_EditChat_CoverPicture(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	profile := &blobpb.BlobId{Value: []byte("group-picture-01")}
	e.media.setRenditions(profile)
	chatID := e.putOwnedGroup("Group", profile, model.MustGenerateUserID())

	cover := &blobpb.BlobId{Value: []byte("group-cover-0001")}
	renditions := e.media.setAttachable(cover)

	resp := e.mustSendEditChat(e.keys, &chatpb.EditChatRequest{
		ChatId:       chatID,
		CoverPicture: &chatpb.EditChatRequest_CoverPicture{BlobId: cover},
	})
	require.Equal(t, chatpb.EditChatResponse_OK, resp.Result)

	// The cover was attached against the group and comes back hydrated; the
	// profile picture was left alone.
	require.Equal(t, [][]byte{cover.Value}, e.media.attachedTo(chatID))
	md := resp.Chat
	require.Len(t, md.GetCoverPicture().GetRenditions(), len(renditions))
	require.Equal(t, cover.Value, md.GetCoverPicture().GetRenditions()[0].GetBlobId().GetValue())
	require.NotNil(t, md.GetCoverPicture().GetRenditions()[0].Blob)
	require.Equal(t, profile.Value, md.GetProfilePicture().GetRenditions()[0].GetBlobId().GetValue())

	stored, err := s.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, cover.Value, stored.CoverPictureBlobID.GetValue())
	require.Equal(t, profile.Value, stored.ProfilePictureBlobID.GetValue())

	// The announcement carries the same hydrated cover, and nothing else.
	updates := e.waitForMetadataUpdates(chatID, 1)
	require.Len(t, updates[0], 1)
	require.NoError(t, protoutil.ProtoEqualError(md.CoverPicture, updates[0][0].GetCoverPictureChanged().GetNewCoverPicture()))

	// The same cover again is the no-op.
	resp = e.mustSendEditChat(e.keys, &chatpb.EditChatRequest{
		ChatId:       chatID,
		CoverPicture: &chatpb.EditChatRequest_CoverPicture{BlobId: cover},
	})
	require.Equal(t, chatpb.EditChatResponse_OK, resp.Result)
	require.Len(t, e.media.attachedTo(chatID), 1)

	// A cover the media domain will not attach refuses the whole edit with
	// its own result: the description beside it is not written.
	resp = e.mustSendEditChat(e.keys, &chatpb.EditChatRequest{
		ChatId:       chatID,
		Description:  &chatpb.EditChatRequest_Description{Value: "Unwritten"},
		CoverPicture: &chatpb.EditChatRequest_CoverPicture{BlobId: &blobpb.BlobId{Value: []byte("not-attachable01")}},
	})
	require.Equal(t, chatpb.EditChatResponse_COVER_PICTURE_BLOB_NOT_ACCEPTED, resp.Result)
	require.Nil(t, resp.Chat)
	stored, err = s.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	require.Empty(t, stored.Description)
	require.Equal(t, cover.Value, stored.CoverPictureBlobID.GetValue())

	// A blob domain that cannot attach at all is the server's fault.
	other := &blobpb.BlobId{Value: []byte("group-cover-0002")}
	e.media.setAttachable(other)
	e.media.attachErr = errors.New("blob store down")
	_, err = e.sendEditChat(e.keys, &chatpb.EditChatRequest{
		ChatId:       chatID,
		CoverPicture: &chatpb.EditChatRequest_CoverPicture{BlobId: other},
	})
	require.Equal(t, codes.Internal, status.Code(err))

	time.Sleep(100 * time.Millisecond)
	updates, _ = e.metadataUpdatesOnChatTopic(chatID)
	require.Len(t, updates, 1)
}

func testServer_EditChat_AllFields(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	chatID := e.putOwnedGroup("Before", nil)
	profile := &blobpb.BlobId{Value: []byte("group-picture-01")}
	cover := &blobpb.BlobId{Value: []byte("group-cover-0001")}
	e.media.setAttachable(profile)
	e.media.setAttachable(cover)

	resp := e.mustSendEditChat(e.keys, &chatpb.EditChatRequest{
		ChatId:         chatID,
		Title:          &chatpb.EditChatRequest_Title{Value: "After"},
		ProfilePicture: &chatpb.EditChatRequest_ProfilePicture{BlobId: profile},
		Description:    &chatpb.EditChatRequest_Description{Value: "Described"},
		CoverPicture:   &chatpb.EditChatRequest_CoverPicture{BlobId: cover},
	})
	require.Equal(t, chatpb.EditChatResponse_OK, resp.Result)
	require.Equal(t, "After", resp.Chat.Title)
	require.Equal(t, "Described", resp.Chat.Description)
	require.Equal(t, [][]byte{profile.Value, cover.Value}, e.media.attachedTo(chatID))

	stored, err := s.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, "After", stored.Title)
	require.Equal(t, "Described", stored.Description)
	require.Equal(t, profile.Value, stored.ProfilePictureBlobID.GetValue())
	require.Equal(t, cover.Value, stored.CoverPictureBlobID.GetValue())

	// One event, one update per field, in the order the proto declares them.
	updates := e.waitForMetadataUpdates(chatID, 1)
	require.Len(t, updates[0], 4)
	require.Equal(t, "After", updates[0][0].GetTitleChanged().GetNewTitle())
	require.NoError(t, protoutil.ProtoEqualError(resp.Chat.ProfilePicture, updates[0][1].GetProfilePictureChanged().GetNewProfilePicture()))
	require.Equal(t, "Described", updates[0][2].GetDescriptionChanged().GetNewDescription())
	require.NoError(t, protoutil.ProtoEqualError(resp.Chat.CoverPicture, updates[0][3].GetCoverPictureChanged().GetNewCoverPicture()))

	// Every field again, as the record holds them: the no-op.
	resp = e.mustSendEditChat(e.keys, &chatpb.EditChatRequest{
		ChatId:         chatID,
		Title:          &chatpb.EditChatRequest_Title{Value: "After"},
		ProfilePicture: &chatpb.EditChatRequest_ProfilePicture{BlobId: profile},
		Description:    &chatpb.EditChatRequest_Description{Value: "Described"},
		CoverPicture:   &chatpb.EditChatRequest_CoverPicture{BlobId: cover},
	})
	require.Equal(t, chatpb.EditChatResponse_OK, resp.Result)
	require.Equal(t, "Described", resp.Chat.Description)
	require.Len(t, e.media.attachedTo(chatID), 2)
	time.Sleep(100 * time.Millisecond)
	updates, _ = e.metadataUpdatesOnChatTopic(chatID)
	require.Len(t, updates, 1)
}

// testServer_ChatProfileDetail_Carriers pins where a group's description and
// cover picture are returned: the description on every carrier of a group's
// metadata, and the cover on every one but the feeds, which are list views
// and leave it out without resolving it.
func testServer_ChatProfileDetail_Carriers(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	requireDetail := func(md *chatpb.Metadata, cover []*blobpb.Rendition) {
		t.Helper()
		require.Equal(t, "About us", md.GetDescription())
		require.Len(t, md.GetCoverPicture().GetRenditions(), len(cover))
		require.NotNil(t, md.GetCoverPicture().GetRenditions()[0].Blob)
	}

	// A group the env user is a member of: GetChat carries both, the feed
	// the description alone, and the cover was never resolved for the feed.
	profile := &blobpb.BlobId{Value: []byte("group-picture-01")}
	e.media.setRenditions(profile)
	member := e.putGroupWithPicture("Member Of", profile, at(1), model.MustGenerateUserID())
	cover := &blobpb.BlobId{Value: []byte("group-cover-0001")}
	coverRenditions := e.describeGroup(member, "About us", cover)

	feed := e.mustGetGroupFeed(&commonpb.QueryOptions{})
	require.Len(t, feed.Chats, 1)
	require.Equal(t, "Member Of", feed.Chats[0].Title)
	require.Equal(t, "About us", feed.Chats[0].Description)
	require.Nil(t, feed.Chats[0].CoverPicture)
	require.NotNil(t, feed.Chats[0].GetProfilePicture().GetRenditions()[0].Blob)
	require.False(t, e.media.resolved[string(cover.Value)])

	requireDetail(e.getChat(e.keys, member).Metadata, coverRenditions)

	// A group the env user is not in: GetChat, the public view, and the join
	// all carry both.
	open := e.putGroup("Not Yet", at(1), model.MustGenerateUserID())
	openCover := &blobpb.BlobId{Value: []byte("group-cover-0002")}
	openRenditions := e.describeGroup(open, "About us", openCover)
	requireDetail(e.getChat(e.keys, open).Metadata, openRenditions)
	requireDetail(e.getPublicChat(open, messagingpb.ViewMode_REDACTED).Metadata, openRenditions)
	joined := e.mustJoinChat(e.keys, open)
	require.Equal(t, chatpb.JoinChatResponse_OK, joined.Result)
	requireDetail(joined.Chat, openRenditions)

	// A private group's lobby carries both to the user waiting in it.
	_, waiterKeys := e.addBoundUser("Waiter")
	private := e.putKeyedPrivateGroup("Private")
	privateCover := &blobpb.BlobId{Value: []byte("group-cover-0003")}
	privateRenditions := e.describeGroup(private, "About us", privateCover)
	entered := e.enterLobby(waiterKeys, private)
	require.Equal(t, chatpb.EnterLobbyResponse_OK, entered.Result)
	requireDetail(entered.Lobby.GetChat(), privateRenditions)
}

func testServer_EditChat_NoOp(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	picture := &blobpb.BlobId{Value: []byte("group-picture-01")}
	e.media.setRenditions(picture)
	chatID := e.putOwnedGroup("Same", picture)

	requireUntouched := func() {
		t.Helper()
		time.Sleep(100 * time.Millisecond)
		updates, _ := e.metadataUpdatesOnChatTopic(chatID)
		require.Empty(t, updates)
		require.Empty(t, e.moderator.classifiedTitle)
		require.Empty(t, e.media.chatPictures)
	}

	// Every field set to what the record holds: OK with the record, nothing
	// moderated, nothing attached, nothing written or published.
	title := "Same"
	resp := e.mustEditChat(e.keys, chatID, &title, picture)
	require.Equal(t, chatpb.EditChatResponse_OK, resp.Result)
	require.Equal(t, "Same", resp.Chat.Title)
	require.Equal(t, picture.Value, resp.Chat.GetProfilePicture().GetRenditions()[0].GetBlobId().GetValue())
	require.NoError(t, protoutil.ProtoEqualError(viewerState(0, nil, true), resp.Chat.ViewerState))
	requireUntouched()

	// A request that sets nothing is the same no-op.
	resp = e.mustEditChat(e.keys, chatID, nil, nil)
	require.Equal(t, chatpb.EditChatResponse_OK, resp.Result)
	require.Equal(t, "Same", resp.Chat.Title)
	requireUntouched()
}

func testServer_EditChat_Denied(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	memberID, memberKeys := e.addUser()
	_, strangerKeys := e.addUser()
	owned := e.putOwnedGroup("Owned", nil, memberID)

	title := "Hijacked"
	requireDenied := func(keys model.KeyPair, chatID *commonpb.ChatId) {
		t.Helper()
		resp := e.mustEditChat(keys, chatID, &title, nil)
		require.Equal(t, chatpb.EditChatResponse_DENIED, resp.Result)
		require.Nil(t, resp.Chat)
	}

	// A member who did not create the group, and a non-member.
	requireDenied(memberKeys, owned)
	requireDenied(strangerKeys, owned)

	// A DM, the caller's own or anyone's: never editable.
	requireDenied(e.keys, e.putDM(at(1)))
	requireDenied(e.keys, putDmChat(t, s, model.MustGenerateUserID(), model.MustGenerateUserID(), at(1)).ID)

	// A group with no recorded creator has no one who may edit it — not even
	// a member who was there at creation.
	requireDenied(e.keys, e.putGroup("Legacy", at(1)))

	// The creator who left edits nothing until they rejoin.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, owned).Result)
	requireDenied(e.keys, owned)
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(e.keys, owned).Result)
	require.Equal(t, chatpb.EditChatResponse_OK, e.mustEditChat(e.keys, owned, &title, nil).Result)

	// Nothing but the last edit reached the store or the moderator.
	stored, err := s.GetChatByID(e.ctx, owned)
	require.NoError(t, err)
	require.Equal(t, "Hijacked", stored.Title)
	e.waitForMetadataUpdates(owned, 1)
}

func testServer_EditChat_NotFound(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	title := "Title"
	resp := e.mustEditChat(e.keys, chat.MustGenerateGroupChatID(), &title, nil)
	require.Equal(t, chatpb.EditChatResponse_NOT_FOUND, resp.Result)
	require.Nil(t, resp.Chat)
	require.Empty(t, e.moderator.classifiedTitle)
}

// putOwnedRuledGroup persists a public group the env user created and is a
// member of, alongside the given others, carrying the given rules as a
// record stores them.
func (e *serverEnv) putOwnedRuledGroup(rules chat.StoredRules, others ...*commonpb.UserId) *commonpb.ChatId {
	chatID := chat.MustGenerateGroupChatID()
	require.NoError(e.t, e.store.PutChat(e.ctx, &chat.Chat{
		ID:                     chatID,
		Type:                   chatpb.ChatType_GROUP,
		Members:                append([]*commonpb.UserId{e.userID}, others...),
		Title:                  "Whales",
		IsStaffOnly:            rules.IsStaffOnly,
		MinimumListenerBalance: rules.MinimumListenerBalance,
		IsCreatorOnlySpeaker:   rules.IsCreatorOnlySpeaker,
		MinimumSpeakerBalance:  rules.MinimumSpeakerBalance,
		CreatorID:              e.userID,
		LastActivity:           at(1),
	}))
	return chatID
}

// usdMinimum builds a stored USD minimum balance in the given mints, or any
// mint when none are given.
func usdMinimum(amount float64, mints ...*commonpb.PublicKey) *chat.MinimumBalance {
	return &chat.MinimumBalance{Currency: "usd", NativeAmount: amount, Mints: mints}
}

// balanceRules builds the rules of a public group with a USD minimum
// listener balance and, when speaker is positive, a USD minimum speaker
// balance, both in any mint, as Chat.Rules projects them.
func balanceRules(listener, speaker float64) *chatpb.Rules {
	rules := &chatpb.Rules{Listener: []*chatpb.ListenerRules{minimumBalanceRule("usd", listener)}}
	if speaker > 0 {
		rules.Speaker = []*chatpb.SpeakerRules{speakerBalanceRule("usd", speaker)}
	}
	return rules
}

// mintBalanceRequirement builds a minimum balance requirement in one mint.
func mintBalanceRequirement(currency string, amount float64, mint *commonpb.PublicKey) *chatpb.MinimumBalanceRequirement {
	return &chatpb.MinimumBalanceRequirement{
		Amount: &commonpb.FiatPaymentAmount{Currency: currency, NativeAmount: amount},
		Mints:  []*commonpb.PublicKey{mint},
	}
}

func (e *serverEnv) mustEditRules(keys model.KeyPair, chatID *commonpb.ChatId, rules *chatpb.Rules) *chatpb.EditChatResponse {
	return e.mustSendEditChat(keys, &chatpb.EditChatRequest{ChatId: chatID, Rules: rules})
}

// fundUser gives userID an owner account, keys, holding the given USD
// amount, as fundEnvUser does for the env user.
func (e *serverEnv) fundUser(userID *commonpb.UserId, keys model.KeyPair, amount uint64) {
	e.bindKey(userID, keys)
	e.ocpBalance.setBalance(keys.Proto(), ocp_common.ToCoreMintQuarks(amount))
}

// requireStoredRules requires that chatID's record carries exactly want.
func (e *serverEnv) requireStoredRules(chatID *commonpb.ChatId, want *chatpb.Rules) {
	e.t.Helper()
	stored, err := e.store.GetChatByID(e.ctx, chatID)
	require.NoError(e.t, err)
	require.NoError(e.t, protoutil.ProtoEqualError(want, stored.Rules()))
}

// requireNoMetadataUpdates requires that, after giving the bus a moment,
// nothing was published about chatID's metadata.
func (e *serverEnv) requireNoMetadataUpdates(chatID *commonpb.ChatId) {
	e.t.Helper()
	time.Sleep(100 * time.Millisecond)
	updates, _ := e.metadataUpdatesOnChatTopic(chatID)
	require.Empty(e.t, updates)
}

// testServer_EditChat_Rules pins each change an edit may make to a public
// group's minimum balances: each lands in the record, comes back on the
// response and on another member's read, and is announced as one
// RulesChanged carrying the rules in full.
func testServer_EditChat_Rules(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.fundEnvUser(300)

	other, otherKeys := e.addUser()
	chatID := e.putOwnedRuledGroup(chat.StoredRules{MinimumListenerBalance: usdMinimum(100)}, other)

	for i, step := range []struct {
		name  string
		rules *chatpb.Rules
	}{
		{"raise the listener balance", balanceRules(150, 0)},
		{"lower the listener balance", balanceRules(50, 0)},
		{"add a speaker balance", balanceRules(50, 200)},
		{"change the speaker balance", balanceRules(50, 250)},
		{"remove the speaker balance", balanceRules(50, 0)},
	} {
		resp := e.mustEditRules(e.keys, chatID, step.rules)
		require.Equal(t, chatpb.EditChatResponse_OK, resp.Result, step.name)
		require.NoError(t, protoutil.ProtoEqualError(step.rules, resp.Chat.GetRules()), step.name)
		e.requireStoredRules(chatID, step.rules)
		require.NoError(t, protoutil.ProtoEqualError(step.rules, e.getChat(otherKeys, chatID).Metadata.GetRules()), step.name)

		updates := e.waitForMetadataUpdates(chatID, i+1)
		require.Len(t, updates[i], 1, step.name)
		require.NoError(t, protoutil.ProtoEqualError(step.rules, updates[i][0].GetRulesChanged().GetNewRules()), step.name)
	}

	// Nothing but the rules was judged.
	require.Empty(t, e.moderator.classifiedTitle)
	require.Empty(t, e.media.chatPictures)
}

// testServer_EditChat_RulesWithOtherFields pins that rules ride an edit
// beside other fields as one write and one event, the rules' update last,
// in the order the proto declares them.
func testServer_EditChat_RulesWithOtherFields(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.fundEnvUser(200)

	chatID := e.putOwnedRuledGroup(chat.StoredRules{MinimumListenerBalance: usdMinimum(100)})
	rules := balanceRules(100, 200)
	resp := e.mustSendEditChat(e.keys, &chatpb.EditChatRequest{
		ChatId:      chatID,
		Title:       &chatpb.EditChatRequest_Title{Value: "After"},
		Description: &chatpb.EditChatRequest_Description{Value: "Described"},
		Rules:       rules,
	})
	require.Equal(t, chatpb.EditChatResponse_OK, resp.Result)
	require.Equal(t, "After", resp.Chat.Title)
	require.Equal(t, "Described", resp.Chat.Description)
	require.NoError(t, protoutil.ProtoEqualError(rules, resp.Chat.GetRules()))
	require.Equal(t, "After", e.moderator.classifiedTitle)

	stored, err := s.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, "After", stored.Title)
	require.Equal(t, "Described", stored.Description)
	require.NoError(t, protoutil.ProtoEqualError(rules, stored.Rules()))

	updates := e.waitForMetadataUpdates(chatID, 1)
	require.Len(t, updates[0], 3)
	require.Equal(t, "After", updates[0][0].GetTitleChanged().GetNewTitle())
	require.Equal(t, "Described", updates[0][1].GetDescriptionChanged().GetNewDescription())
	require.NoError(t, protoutil.ProtoEqualError(rules, updates[0][2].GetRulesChanged().GetNewRules()))
}

// testServer_EditChat_Rules_Fixed pins that the rules an edit cannot change
// — a staff requirement and a creator requirement — must be named exactly
// when the group carries them, in any order, and are kept as they were.
func testServer_EditChat_Rules_Fixed(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.accounts.setStaff(e.userID, true)
	e.fundEnvUser(300)

	staffRule := &chatpb.ListenerRules{Kind: &chatpb.ListenerRules_Staff{Staff: &chatpb.StaffRequirement{}}}
	creatorRule := &chatpb.SpeakerRules{Kind: &chatpb.SpeakerRules_Creator{Creator: &chatpb.CreatorRequirement{}}}

	// A staff-only group's creator, who is staff, echoes the requirement,
	// after the balance, with a new amount: OK, shown in the record's order.
	staffOnly := e.putOwnedRuledGroup(chat.StoredRules{IsStaffOnly: true, MinimumListenerBalance: usdMinimum(100)})
	want := &chatpb.Rules{Listener: []*chatpb.ListenerRules{staffRule, minimumBalanceRule("usd", 150)}}
	resp := e.mustEditRules(e.keys, staffOnly, &chatpb.Rules{Listener: []*chatpb.ListenerRules{minimumBalanceRule("usd", 150), staffRule}})
	require.Equal(t, chatpb.EditChatResponse_OK, resp.Result)
	require.NoError(t, protoutil.ProtoEqualError(want, resp.Chat.GetRules()))
	e.requireStoredRules(staffOnly, want)
	updates := e.waitForMetadataUpdates(staffOnly, 1)
	require.NoError(t, protoutil.ProtoEqualError(want, updates[0][0].GetRulesChanged().GetNewRules()))

	// Dropping it, or naming it twice, is refused.
	for _, rules := range []*chatpb.Rules{
		balanceRules(200, 0),
		{Listener: []*chatpb.ListenerRules{staffRule, staffRule, minimumBalanceRule("usd", 200)}},
	} {
		resp = e.mustEditRules(e.keys, staffOnly, rules)
		require.Equal(t, chatpb.EditChatResponse_INVALID_RULES, resp.Result)
		require.Nil(t, resp.Chat)
	}
	e.requireStoredRules(staffOnly, want)

	// Adding one to a group without it is refused.
	open := e.putOwnedRuledGroup(chat.StoredRules{MinimumListenerBalance: usdMinimum(100)})
	resp = e.mustEditRules(e.keys, open, &chatpb.Rules{Listener: []*chatpb.ListenerRules{staffRule, minimumBalanceRule("usd", 150)}})
	require.Equal(t, chatpb.EditChatResponse_INVALID_RULES, resp.Result)
	e.requireStoredRules(open, balanceRules(100, 0))

	// A creator-only group keeps its creator requirement through an edit
	// that names it, and cannot lose it; nor can another group gain one.
	creatorOnly := e.putOwnedRuledGroup(chat.StoredRules{MinimumListenerBalance: usdMinimum(100), IsCreatorOnlySpeaker: true})
	want = &chatpb.Rules{
		Listener: []*chatpb.ListenerRules{minimumBalanceRule("usd", 100)},
		Speaker:  []*chatpb.SpeakerRules{creatorRule, speakerBalanceRule("usd", 200)},
	}
	resp = e.mustEditRules(e.keys, creatorOnly, &chatpb.Rules{
		Listener: []*chatpb.ListenerRules{minimumBalanceRule("usd", 100)},
		Speaker:  []*chatpb.SpeakerRules{speakerBalanceRule("usd", 200), creatorRule},
	})
	require.Equal(t, chatpb.EditChatResponse_OK, resp.Result)
	require.NoError(t, protoutil.ProtoEqualError(want, resp.Chat.GetRules()))
	e.requireStoredRules(creatorOnly, want)

	resp = e.mustEditRules(e.keys, creatorOnly, balanceRules(100, 300))
	require.Equal(t, chatpb.EditChatResponse_INVALID_RULES, resp.Result)
	e.requireStoredRules(creatorOnly, want)

	resp = e.mustEditRules(e.keys, open, &chatpb.Rules{
		Listener: []*chatpb.ListenerRules{minimumBalanceRule("usd", 100)},
		Speaker:  []*chatpb.SpeakerRules{creatorRule},
	})
	require.Equal(t, chatpb.EditChatResponse_INVALID_RULES, resp.Result)
	e.requireStoredRules(open, balanceRules(100, 0))
	e.requireNoMetadataUpdates(open)
}

// testServer_EditChat_Rules_Invalid pins the rules an edit refuses as
// INVALID_RULES, and who may not edit them at all, with the record left as
// it was and nothing published.
func testServer_EditChat_Rules_Invalid(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.fundEnvUser(1000)

	member, memberKeys := e.addUser()
	e.fundUser(member, memberKeys, 1000)

	mint := model.MustGenerateKeyPair().Proto()
	otherMint := model.MustGenerateKeyPair().Proto()
	chatID := e.putOwnedRuledGroup(chat.StoredRules{
		MinimumListenerBalance: usdMinimum(100, mint),
		MinimumSpeakerBalance:  usdMinimum(200, mint),
	}, member)
	stored, err := s.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	original := stored.Rules()

	listener := func(currency string, amount float64, mint *commonpb.PublicKey) *chatpb.ListenerRules {
		return &chatpb.ListenerRules{Kind: &chatpb.ListenerRules_MinimumBalance{MinimumBalance: mintBalanceRequirement(currency, amount, mint)}}
	}
	speaker := func(currency string, amount float64, mint *commonpb.PublicKey) *chatpb.SpeakerRules {
		return &chatpb.SpeakerRules{Kind: &chatpb.SpeakerRules_MinimumBalance{MinimumBalance: mintBalanceRequirement(currency, amount, mint)}}
	}
	for _, tc := range []struct {
		name  string
		rules *chatpb.Rules
	}{
		{"another currency", &chatpb.Rules{
			Listener: []*chatpb.ListenerRules{listener("eur", 100, mint)},
			Speaker:  []*chatpb.SpeakerRules{speaker("eur", 200, mint)},
		}},
		{"another mint", &chatpb.Rules{
			Listener: []*chatpb.ListenerRules{listener("usd", 150, otherMint)},
			Speaker:  []*chatpb.SpeakerRules{speaker("usd", 200, otherMint)},
		}},
		{"any mint", balanceRules(150, 200)},
		{"speaker in another mint", &chatpb.Rules{
			Listener: []*chatpb.ListenerRules{listener("usd", 100, mint)},
			Speaker:  []*chatpb.SpeakerRules{speaker("usd", 200, otherMint)},
		}},
		{"speaker not above the listener by a minor unit", &chatpb.Rules{
			Listener: []*chatpb.ListenerRules{listener("usd", 100, mint)},
			Speaker:  []*chatpb.SpeakerRules{speaker("usd", 100.005, mint)},
		}},
		{"listener below the minimum transfer value", &chatpb.Rules{
			Listener: []*chatpb.ListenerRules{listener("usd", 0.001, mint)},
		}},
		{"no listener balance", &chatpb.Rules{
			Speaker: []*chatpb.SpeakerRules{speaker("usd", 200, mint)},
		}},
		{"no rules", &chatpb.Rules{}},
	} {
		resp := e.mustEditRules(e.keys, chatID, tc.rules)
		require.Equal(t, chatpb.EditChatResponse_INVALID_RULES, resp.Result, tc.name)
		require.Nil(t, resp.Chat, tc.name)
	}

	// A member who did not create the group is DENIED, as for any edit, even
	// with rules that would pass.
	valid := &chatpb.Rules{
		Listener: []*chatpb.ListenerRules{listener("usd", 150, mint)},
		Speaker:  []*chatpb.SpeakerRules{speaker("usd", 200, mint)},
	}
	resp := e.mustEditRules(memberKeys, chatID, valid)
	require.Equal(t, chatpb.EditChatResponse_DENIED, resp.Result)

	e.requireStoredRules(chatID, original)
	e.requireNoMetadataUpdates(chatID)

	// A public group created without a listener balance has no rules to
	// edit.
	unruled := e.putOwnedGroup("Open", nil)
	resp = e.mustEditRules(e.keys, unruled, balanceRules(100, 0))
	require.Equal(t, chatpb.EditChatResponse_INVALID_RULES, resp.Result)
	e.requireStoredRules(unruled, nil)

	// A private group is governed by no rules: naming any is DENIED, while
	// the same creator's edit of another field passes.
	private := e.putKeyedPrivateGroup("Private")
	resp = e.mustEditRules(e.keys, private, balanceRules(100, 0))
	require.Equal(t, chatpb.EditChatResponse_DENIED, resp.Result)
	e.requireStoredRules(private, nil)
	e.requireNoMetadataUpdates(unruled)
	e.requireNoMetadataUpdates(private)
	title := "Renamed"
	require.Equal(t, chatpb.EditChatResponse_OK, e.mustEditChat(e.keys, private, &title, nil).Result)
}

// testServer_EditChat_Rules_NotSatisfied pins that the creator must satisfy
// the new rules in full, judged before anything else in the request, and
// that a valuation that fails is an error rather than a verdict.
func testServer_EditChat_Rules_NotSatisfied(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.fundEnvUser(100)

	chatID := e.putOwnedRuledGroup(chat.StoredRules{MinimumListenerBalance: usdMinimum(100)})

	// A listener balance above what the creator holds is refused, before the
	// title beside it is judged.
	resp := e.mustSendEditChat(e.keys, &chatpb.EditChatRequest{
		ChatId: chatID,
		Title:  &chatpb.EditChatRequest_Title{Value: "After"},
		Rules:  balanceRules(150, 0),
	})
	require.Equal(t, chatpb.EditChatResponse_RULES_NOT_SATISFIED, resp.Result)
	require.Nil(t, resp.Chat)
	require.Empty(t, e.moderator.classifiedTitle)

	// So is a speaker balance above it, though they meet the listener's.
	resp = e.mustEditRules(e.keys, chatID, balanceRules(100, 150))
	require.Equal(t, chatpb.EditChatResponse_RULES_NOT_SATISFIED, resp.Result)

	stored, err := s.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, "Whales", stored.Title)
	e.requireStoredRules(chatID, balanceRules(100, 0))
	e.requireNoMetadataUpdates(chatID)

	// Topped up, the same edit lands.
	e.fundEnvUser(150)
	resp = e.mustEditRules(e.keys, chatID, balanceRules(100, 150))
	require.Equal(t, chatpb.EditChatResponse_OK, resp.Result)
	e.requireStoredRules(chatID, balanceRules(100, 150))
	e.waitForMetadataUpdates(chatID, 1)

	// A currency OCP does not price now was priced when the group was
	// created and cannot change, so a valuation failing is transient:
	// Internal, not INVALID_RULES.
	euro := &chatpb.Rules{Listener: []*chatpb.ListenerRules{minimumBalanceRule("eur", 90)}}
	euroGroup := e.putOwnedRuledGroup(chat.StoredRules{MinimumListenerBalance: &chat.MinimumBalance{Currency: "eur", NativeAmount: 90}})
	_, err = e.sendEditChat(e.keys, &chatpb.EditChatRequest{
		ChatId: euroGroup,
		Rules:  &chatpb.Rules{Listener: []*chatpb.ListenerRules{minimumBalanceRule("eur", 100)}},
	})
	require.Equal(t, codes.Internal, status.Code(err))
	e.requireStoredRules(euroGroup, euro)
	e.requireNoMetadataUpdates(euroGroup)
}

// testServer_EditChat_Rules_NoOp pins that rules equal to the record's are
// dropped before anything is checked: OK from the record, with the creator
// never evaluated (they hold nothing here), nothing written or published.
func testServer_EditChat_Rules_NoOp(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	mint := model.MustGenerateKeyPair().Proto()
	chatID := e.putOwnedRuledGroup(chat.StoredRules{
		MinimumListenerBalance: usdMinimum(100, mint),
		MinimumSpeakerBalance:  usdMinimum(200, mint),
	})
	stored, err := s.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	rules := stored.Rules()

	resp := e.mustEditRules(e.keys, chatID, rules)
	require.Equal(t, chatpb.EditChatResponse_OK, resp.Result)
	require.NoError(t, protoutil.ProtoEqualError(rules, resp.Chat.GetRules()))
	e.requireStoredRules(chatID, rules)
	e.requireNoMetadataUpdates(chatID)
}

// testServer_EditChat_Rules_Grandfathered pins that an edit removes no one:
// a member who met the old listener balance but not the raised one stays a
// member and keeps reading, cannot speak, and must meet the new rules to
// rejoin once they leave.
func testServer_EditChat_Rules_Grandfathered(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.fundEnvUser(200)

	chatID := e.putOwnedRuledGroup(chat.StoredRules{MinimumListenerBalance: usdMinimum(100)})
	member, memberKeys := e.addUser()
	e.fundUser(member, memberKeys, 100)
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(memberKeys, chatID).Result)

	raised := balanceRules(150, 0)
	require.Equal(t, chatpb.EditChatResponse_OK, e.mustEditRules(e.keys, chatID, raised).Result)

	// Still a member, on the roster, reading in full.
	isMember, err := s.IsMember(e.ctx, chatID, member)
	require.NoError(t, err)
	require.True(t, isMember)
	roster := e.mustGetRoster(memberKeys, chatID, &commonpb.QueryOptions{})
	require.Equal(t, chatpb.GetRosterResponse_OK, roster.Result)
	require.Contains(t, memberUserIDs(roster.Members), member.Value)
	got := e.getChat(memberKeys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, got.Result)
	require.NotNil(t, got.Metadata.ViewerState)
	require.NoError(t, protoutil.ProtoEqualError(raised, got.Metadata.GetRules()))

	// But not speaking, since speech evaluates the listener rules.
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_DENIED, e.getMentionSuggestions(memberKeys, chatID).Result)

	// Once they leave, the new rules keep them out.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(memberKeys, chatID).Result)
	require.Equal(t, chatpb.JoinChatResponse_RULES_NOT_SATISFIED, e.mustJoinChat(memberKeys, chatID).Result)
	isMember, err = s.IsMember(e.ctx, chatID, member)
	require.NoError(t, err)
	require.False(t, isMember)

	// Meeting them, they rejoin and speak.
	e.fundUser(member, memberKeys, 150)
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(memberKeys, chatID).Result)
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_OK, e.getMentionSuggestions(memberKeys, chatID).Result)
}

func testServer_EditChat_TitleModerated(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	chatID := e.putOwnedGroup("Before", nil)
	picture := &blobpb.BlobId{Value: []byte("group-picture-01")}
	e.media.setAttachable(picture)

	// A flagged title refuses the whole edit: the picture sent alongside it is
	// not attached, and nothing is written or published.
	e.moderator.titleFlagged = true
	e.moderator.titleCategories = []string{"gibberish", "solicitation"}
	title := "DM for signals"
	resp := e.mustEditChat(e.keys, chatID, &title, picture)
	require.Equal(t, chatpb.EditChatResponse_TITLE_MODERATED, resp.Result)
	require.Equal(t, moderationpb.FlaggedCategory_SPAM, resp.FlaggedCategory)
	require.Nil(t, resp.Chat)
	require.Empty(t, e.media.chatPictures)

	// The text classifier alone refuses too.
	e.moderator.titleFlagged = false
	e.moderator.titleCategories = nil
	e.moderator.textFlagged = true
	e.moderator.textCategories = []string{"hate"}
	resp = e.mustEditChat(e.keys, chatID, &title, nil)
	require.Equal(t, chatpb.EditChatResponse_TITLE_MODERATED, resp.Result)
	require.Equal(t, moderationpb.FlaggedCategory_NSFW, resp.FlaggedCategory)

	stored, err := s.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, "Before", stored.Title)
	require.Nil(t, stored.ProfilePictureBlobID)
	time.Sleep(100 * time.Millisecond)
	updates, _ := e.metadataUpdatesOnChatTopic(chatID)
	require.Empty(t, updates)
}

func testServer_EditChat_PictureNotAccepted(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	chatID := e.putOwnedGroup("Before", nil)

	// A blob the media domain will not attach refuses the whole edit: the
	// title sent alongside it — already moderated — is not written.
	title := "After"
	resp := e.mustEditChat(e.keys, chatID, &title, &blobpb.BlobId{Value: []byte("not-attachable01")})
	require.Equal(t, chatpb.EditChatResponse_PROFILE_PICTURE_BLOB_NOT_ACCEPTED, resp.Result)
	require.Nil(t, resp.Chat)
	require.Equal(t, "After", e.moderator.classifiedTitle)

	stored, err := s.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, "Before", stored.Title)
	require.Nil(t, stored.ProfilePictureBlobID)

	// A blob domain that cannot attach at all is the server's fault.
	picture := &blobpb.BlobId{Value: []byte("group-picture-01")}
	e.media.setAttachable(picture)
	e.media.attachErr = errors.New("blob store down")
	_, err = e.editChat(e.keys, chatID, nil, picture)
	require.Equal(t, codes.Internal, status.Code(err))

	time.Sleep(100 * time.Millisecond)
	updates, _ := e.metadataUpdatesOnChatTopic(chatID)
	require.Empty(t, updates)
}

func testServer_EditChat_ModerationFailureIsInternal(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	chatID := e.putOwnedGroup("Before", nil)
	title := "After"

	e.moderator.titleErr = errors.New("classifier down")
	_, err := e.editChat(e.keys, chatID, &title, nil)
	require.Equal(t, codes.Internal, status.Code(err))

	e.moderator.titleErr = nil
	e.moderator.textErr = errors.New("classifier down")
	_, err = e.editChat(e.keys, chatID, &title, nil)
	require.Equal(t, codes.Internal, status.Code(err))

	stored, err := s.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, "Before", stored.Title)
}

// testServer_ViewerState_Permissions pins who is shown what they may do: a
// member always gets a viewer_state, carrying can_edit for the group's
// creator alone, on every carrier of the metadata; a non-member gets none,
// whatever record they hold.
func testServer_ViewerState_Permissions(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	memberID, memberKeys := e.addUser()
	strangerID, strangerKeys := e.addUser()
	owned := e.putOwnedGroup("Owned", nil, memberID)
	plain := e.putGroup("Plain", at(2), memberID)
	dm := e.putDMWithPeer(chatpb.ChatType_CONTACT_DM, memberID, at(3))

	canEdit := viewerState(0, nil, true)

	// GetChat: the creator may edit, moderate and pin in their group and
	// nothing else; another member may do nothing; a DM participant may pin
	// alone; a stranger gets no state.
	require.NoError(t, protoutil.ProtoEqualError(canEdit, e.getChat(e.keys, owned).Metadata.ViewerState))
	require.NoError(t, protoutil.ProtoEqualError(memberViewerState, e.getChat(e.keys, plain).Metadata.ViewerState))
	require.NoError(t, protoutil.ProtoEqualError(dmMemberViewerState, e.getChat(e.keys, dm).Metadata.ViewerState))
	require.NoError(t, protoutil.ProtoEqualError(memberViewerState, e.getChat(memberKeys, owned).Metadata.ViewerState))
	require.NoError(t, protoutil.ProtoEqualError(dmMemberViewerState, e.getChat(memberKeys, dm).Metadata.ViewerState))
	require.Nil(t, e.getChat(strangerKeys, owned).Metadata.ViewerState)

	// The feeds carry the same.
	feed := e.mustGetGroupFeed(&commonpb.QueryOptions{})
	require.Len(t, feed.Chats, 2)
	for _, md := range feed.Chats {
		want := memberViewerState
		if bytes.Equal(md.ChatId.Value, owned.Value) {
			want = canEdit
		}
		require.NoError(t, protoutil.ProtoEqualError(want, md.ViewerState))
	}
	dmFeed := e.getDmFeed(&commonpb.QueryOptions{})
	require.Len(t, dmFeed.Chats, 1)
	require.NoError(t, protoutil.ProtoEqualError(dmMemberViewerState, dmFeed.Chats[0].ViewerState))

	// A mute carries the permissions with it, on the response and the update.
	muted := e.mustMuteChat(e.keys, owned, muteForever())
	require.Equal(t, chatpb.MuteChatResponse_OK, muted.Result)
	want := viewerState(1, muteForever(), true)
	require.NoError(t, protoutil.ProtoEqualError(want, muted.ViewerState))
	require.NoError(t, protoutil.ProtoEqualError(want, e.getChat(e.keys, owned).Metadata.ViewerState))
	states := e.waitForViewerStateUpdates(e.userID, owned, 1)
	require.NoError(t, protoutil.ProtoEqualError(want, states[0]))

	// The permissions follow membership: a creator who leaves may do nothing
	// — the clear on leave says so — and, as a non-member, is shown no state
	// at all; rejoining restores them at the version the record is at, on the
	// join's own metadata.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, owned).Result)
	cleared := viewerState(2, nil, false)
	states = e.waitForViewerStateUpdates(e.userID, owned, 2)
	require.NoError(t, protoutil.ProtoEqualError(cleared, states[1]))
	require.Nil(t, e.getChat(e.keys, owned).Metadata.ViewerState)

	rejoined := e.mustJoinChat(e.keys, owned)
	require.Equal(t, chatpb.JoinChatResponse_OK, rejoined.Result)
	restored := viewerState(2, nil, true)
	require.NoError(t, protoutil.ProtoEqualError(restored, rejoined.Chat.ViewerState))
	require.NoError(t, protoutil.ProtoEqualError(restored, e.getChat(e.keys, owned).Metadata.ViewerState))
	e.waitForRosterUpdates(e.userID, owned, 2)
	toSelf := e.rosterUpdatesOnUserTopic(e.userID, owned)
	require.NoError(t, protoutil.ProtoEqualError(restored, toSelf[1].GetMemberJoined().GetMetadata().GetViewerState()))

	// A stranger who joins a group they did not create may do nothing in it.
	joined := e.mustJoinChat(strangerKeys, plain)
	require.Equal(t, chatpb.JoinChatResponse_OK, joined.Result)
	require.NoError(t, protoutil.ProtoEqualError(memberViewerState, joined.Chat.ViewerState))
	require.NoError(t, protoutil.ProtoEqualError(memberViewerState, e.getChat(strangerKeys, plain).Metadata.ViewerState))
	require.Empty(t, e.viewerStateUpdatesOnUserTopic(strangerID, plain))

	// A group created through StartChat is its creator's to edit from the
	// response on.
	e.fundEnvUser(startChatMinimumBalance)
	created := e.mustStartGroupChat(e.keys, groupParams("Created"))
	require.Equal(t, chatpb.StartChatResponse_OK, created.Result)
	require.NoError(t, protoutil.ProtoEqualError(canEdit, created.Chat.ViewerState))
	title := "Created, Edited"
	require.Equal(t, chatpb.EditChatResponse_OK, e.mustEditChat(e.keys, created.Chat.ChatId, &title, nil).Result)
}

// addBoundUser is addUser with the user's public key bound in the account
// store, as every registered user's is: a lobby member is shown with the key
// the creator wraps the chat key for.
func (e *serverEnv) addBoundUser(displayName string) (*commonpb.UserId, model.KeyPair) {
	userID, keys := e.addUser()
	_, err := e.accounts.Bind(e.ctx, userID, keys.Proto())
	require.NoError(e.t, err)
	e.profiles.displayNames[string(userID.Value)] = displayName
	return userID, keys
}

// putKeyedPrivateGroup is putPrivateGroup with the creator's key envelope
// stored, so the group has its key and a lobby.
func (e *serverEnv) putKeyedPrivateGroup(title string, others ...*commonpb.UserId) *commonpb.ChatId {
	chatID := e.putPrivateGroup(title, others...)
	_, err := e.store.SetKeyEnvelope(e.ctx, chatID, e.userID, chat.KeyEnvelopeFromProto(keyEnvelopeProto(1), e.userID))
	require.NoError(e.t, err)
	return chatID
}

func (e *serverEnv) enterLobby(keys model.KeyPair, chatID *commonpb.ChatId) *chatpb.EnterLobbyResponse {
	req := &chatpb.EnterLobbyRequest{ChatId: chatID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.EnterLobby(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) leaveLobby(keys model.KeyPair, chatID *commonpb.ChatId) *chatpb.LeaveLobbyResponse {
	req := &chatpb.LeaveLobbyRequest{ChatId: chatID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.LeaveLobby(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) getLobbyMembers(keys model.KeyPair, chatID *commonpb.ChatId, opts *commonpb.QueryOptions) (*chatpb.GetLobbyMembersResponse, error) {
	req := &chatpb.GetLobbyMembersRequest{ChatId: chatID, QueryOptions: opts}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.GetLobbyMembers(e.ctx, req)
}

// waitForLobbyMembers reads chatID's lobby as keys until it holds n members:
// the page trails writes briefly (see chat.Store.GetLobbyPage).
func (e *serverEnv) waitForLobbyMembers(keys model.KeyPair, chatID *commonpb.ChatId, n int) *chatpb.GetLobbyMembersResponse {
	var resp *chatpb.GetLobbyMembersResponse
	require.Eventually(e.t, func() bool {
		var err error
		resp, err = e.getLobbyMembers(keys, chatID, nil)
		require.NoError(e.t, err)
		require.Equal(e.t, chatpb.GetLobbyMembersResponse_OK, resp.Result)
		return len(resp.Members) == n
	}, 5*time.Second, 10*time.Millisecond)
	return resp
}

func (e *serverEnv) admitLobbyMember(keys model.KeyPair, chatID *commonpb.ChatId, userID *commonpb.UserId, envelope *chatpb.KeyEnvelope) *chatpb.AdmitLobbyMemberResponse {
	req := &chatpb.AdmitLobbyMemberRequest{ChatId: chatID, UserId: userID, KeyEnvelope: envelope}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.AdmitLobbyMember(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) denyLobbyMember(keys model.KeyPair, chatID *commonpb.ChatId, userID *commonpb.UserId) *chatpb.DenyLobbyMemberResponse {
	req := &chatpb.DenyLobbyMemberRequest{ChatId: chatID, UserId: userID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.DenyLobbyMember(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

// lobbyUpdatesOnUserTopic collects every LobbyUpdate for chatID published on
// userID's topic, in order.
func (e *serverEnv) lobbyUpdatesOnUserTopic(userID *commonpb.UserId, chatID *commonpb.ChatId) []*chatpb.LobbyUpdate {
	var updates []*chatpb.LobbyUpdate
	for _, ev := range e.userObserver.GetEvents(func(k *commonpb.UserId) bool { return bytes.Equal(k.Value, userID.Value) }) {
		update := ev.Event.GetChatUpdate()
		if !bytes.Equal(update.GetChat().GetValue(), chatID.Value) {
			continue
		}
		updates = append(updates, update.GetLobbyUpdates().GetLobbyUpdates()...)
	}
	return updates
}

// waitForLobbyUpdates waits for at least n lobby updates for chatID on
// userID's topic and returns them.
func (e *serverEnv) waitForLobbyUpdates(userID *commonpb.UserId, chatID *commonpb.ChatId, n int) []*chatpb.LobbyUpdate {
	e.userObserver.WaitFor(e.t, func([]*event.KeyAndEvent[*commonpb.UserId, *eventpb.Event]) bool {
		return len(e.lobbyUpdatesOnUserTopic(userID, chatID)) >= n
	})
	return e.lobbyUpdatesOnUserTopic(userID, chatID)
}

// requireNoLobbyUpdatesOnChatTopic asserts, after giving the bus a moment,
// that nothing published on chatID's topic carried a lobby update: the lobby
// is the creator's alone.
func (e *serverEnv) requireNoLobbyUpdatesOnChatTopic(chatID *commonpb.ChatId) {
	time.Sleep(100 * time.Millisecond)
	for _, ev := range e.chatObserver.GetEvents(func(k *commonpb.ChatId) bool { return bytes.Equal(k.Value, chatID.Value) }) {
		require.Nil(e.t, ev.Event.GetEvent().GetChatUpdate().GetLobbyUpdates())
	}
}

// testServer_Lobby_Gates pins who may do what with a lobby: nothing of a
// DM or a public group, NOT_FOUND for a group that is not there, no lobby for
// a keyless private group, the creator's RPCs the creator's alone and only
// while a member, and no place in a lobby for a member or the creator.
func testServer_Lobby_Gates(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	strangerID, strangerKeys := e.addBoundUser("Stranger")
	memberID, memberKeys := e.addBoundUser("Member")

	requireAllDenied := func(chatID *commonpb.ChatId, keys model.KeyPair) {
		t.Helper()
		require.Equal(t, chatpb.EnterLobbyResponse_DENIED, e.enterLobby(keys, chatID).Result)
		require.Equal(t, chatpb.LeaveLobbyResponse_DENIED, e.leaveLobby(keys, chatID).Result)
		got, err := e.getLobbyMembers(keys, chatID, nil)
		require.NoError(t, err)
		require.Equal(t, chatpb.GetLobbyMembersResponse_DENIED, got.Result)
		require.Empty(t, got.Members)
		require.Equal(t, chatpb.AdmitLobbyMemberResponse_DENIED, e.admitLobbyMember(keys, chatID, strangerID, keyEnvelopeProto(2)).Result)
		require.Equal(t, chatpb.DenyLobbyMemberResponse_DENIED, e.denyLobbyMember(keys, chatID, strangerID).Result)
	}

	// A DM, for its members and anyone; a public group, for its creator and a
	// stranger alike.
	dm := e.putDM(at(1))
	requireAllDenied(dm, e.keys)
	requireAllDenied(dm, strangerKeys)
	public := e.putGroup("Public", at(1))
	requireAllDenied(public, e.keys)
	requireAllDenied(public, strangerKeys)

	// A group that does not exist.
	missing := chat.MustGenerateGroupChatID()
	require.Equal(t, chatpb.EnterLobbyResponse_NOT_FOUND, e.enterLobby(strangerKeys, missing).Result)
	require.Equal(t, chatpb.LeaveLobbyResponse_NOT_FOUND, e.leaveLobby(strangerKeys, missing).Result)
	got, err := e.getLobbyMembers(e.keys, missing, nil)
	require.NoError(t, err)
	require.Equal(t, chatpb.GetLobbyMembersResponse_NOT_FOUND, got.Result)
	require.Equal(t, chatpb.AdmitLobbyMemberResponse_NOT_FOUND, e.admitLobbyMember(e.keys, missing, strangerID, keyEnvelopeProto(2)).Result)
	require.Equal(t, chatpb.DenyLobbyMemberResponse_NOT_FOUND, e.denyLobbyMember(e.keys, missing, strangerID).Result)

	// A keyless private group has no lobby to enter and admits no one, while
	// its creator may still read the (empty) lobby and leave or deny is the
	// no-op it is.
	keyless := e.putPrivateGroup("Keyless", memberID)
	require.Equal(t, chatpb.EnterLobbyResponse_DENIED, e.enterLobby(strangerKeys, keyless).Result)
	require.Equal(t, chatpb.LeaveLobbyResponse_OK, e.leaveLobby(strangerKeys, keyless).Result)
	got, err = e.getLobbyMembers(e.keys, keyless, nil)
	require.NoError(t, err)
	require.Equal(t, chatpb.GetLobbyMembersResponse_OK, got.Result)
	require.Empty(t, got.Members)
	require.Equal(t, chatpb.AdmitLobbyMemberResponse_DENIED, e.admitLobbyMember(e.keys, keyless, strangerID, keyEnvelopeProto(2)).Result)
	require.Equal(t, chatpb.DenyLobbyMemberResponse_OK, e.denyLobbyMember(e.keys, keyless, strangerID).Result)

	// A keyed one: the creator's RPCs are refused to a member who is not the
	// creator and to a stranger, and the lobby is for neither the creator nor
	// a member.
	keyed := e.putKeyedPrivateGroup("Keyed", memberID)
	for _, keys := range []model.KeyPair{memberKeys, strangerKeys} {
		got, err := e.getLobbyMembers(keys, keyed, nil)
		require.NoError(t, err)
		require.Equal(t, chatpb.GetLobbyMembersResponse_DENIED, got.Result)
		require.Equal(t, chatpb.AdmitLobbyMemberResponse_DENIED, e.admitLobbyMember(keys, keyed, strangerID, keyEnvelopeProto(2)).Result)
		require.Equal(t, chatpb.DenyLobbyMemberResponse_DENIED, e.denyLobbyMember(keys, keyed, strangerID).Result)
	}
	require.Equal(t, chatpb.EnterLobbyResponse_DENIED, e.enterLobby(e.keys, keyed).Result)
	require.Equal(t, chatpb.EnterLobbyResponse_ALREADY_MEMBER, e.enterLobby(memberKeys, keyed).Result)

	// A creator who has left runs nothing until they rejoin.
	left, err := e.leaveChat(e.keys, keyed)
	require.NoError(t, err)
	require.Equal(t, chatpb.LeaveChatResponse_OK, left.Result)
	got, err = e.getLobbyMembers(e.keys, keyed, nil)
	require.NoError(t, err)
	require.Equal(t, chatpb.GetLobbyMembersResponse_DENIED, got.Result)
	require.Equal(t, chatpb.AdmitLobbyMemberResponse_DENIED, e.admitLobbyMember(e.keys, keyed, strangerID, keyEnvelopeProto(2)).Result)
	require.Equal(t, chatpb.DenyLobbyMemberResponse_DENIED, e.denyLobbyMember(e.keys, keyed, strangerID).Result)
	require.Equal(t, chatpb.EnterLobbyResponse_DENIED, e.enterLobby(e.keys, keyed).Result)
	rejoined, err := e.joinChat(e.keys, keyed)
	require.NoError(t, err)
	require.Equal(t, chatpb.JoinChatResponse_OK, rejoined.Result)
	got, err = e.getLobbyMembers(e.keys, keyed, nil)
	require.NoError(t, err)
	require.Equal(t, chatpb.GetLobbyMembersResponse_OK, got.Result)
}

// testServer_Lobby_EnterAndLeave pins a user's way into and out of a lobby:
// the entry's Lobby, in_lobby on the chat for them and no one else, the
// creator's MemberEntered with the user as a LobbyMember, the no-op repeat,
// and the withdrawal with its MemberLeft. Nothing of it reaches the chat
// topic.
func testServer_Lobby_EnterAndLeave(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.profiles.displayNames[string(e.userID.Value)] = "Creator"
	userID, userKeys := e.addBoundUser("Waiting")
	chatID := e.putKeyedPrivateGroup("Sunday Hikers")

	before := time.Now()
	resp := e.enterLobby(userKeys, chatID)
	require.Equal(t, chatpb.EnterLobbyResponse_OK, resp.Result)
	require.NotNil(t, resp.Lobby)
	require.False(t, resp.Lobby.EnteredAt.AsTime().Before(before.Add(-time.Second)))
	md := resp.Lobby.Chat
	require.Equal(t, chatID.Value, md.ChatId.Value)
	require.Equal(t, "Sunday Hikers", md.Title)
	require.True(t, md.IsPrivate)
	require.True(t, md.InLobby)
	require.Empty(t, md.Members)
	require.Nil(t, md.ViewerState)
	require.Nil(t, md.LastMessage)

	// The chat says so for them, and for no one else.
	got := e.getChat(userKeys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, got.Result)
	require.True(t, got.Metadata.InLobby)
	require.Empty(t, got.Metadata.Members)
	got = e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, got.Result)
	require.False(t, got.Metadata.InLobby)
	_, strangerKeys := e.addBoundUser("Stranger")
	got = e.getChat(strangerKeys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, got.Result)
	require.False(t, got.Metadata.InLobby)

	// The creator hears of it, with the user as the lobby shows them.
	updates := e.waitForLobbyUpdates(e.userID, chatID, 1)
	entered := updates[0].GetMemberEntered()
	require.NotNil(t, entered)
	require.Equal(t, userID.Value, entered.Member.UserProfile.UserId.Value)
	require.Equal(t, "Waiting", entered.Member.UserProfile.DisplayName)
	require.True(t, proto.Equal(userKeys.Proto(), entered.Member.PublicKey))
	require.True(t, entered.Member.EnteredAt.AsTime().Equal(resp.Lobby.EnteredAt.AsTime()))
	require.Empty(t, e.lobbyUpdatesOnUserTopic(userID, chatID))
	e.requireNoLobbyUpdatesOnChatTopic(chatID)

	// And is pushed, naming the user.
	pushes := e.waitForPushes(e.userID, 1)
	require.Len(t, pushes, 1)
	requireLobbyPush(t, pushes[0], chatID, "Sunday Hikers", "Waiting requested to join")

	// A repeat is OK with the same entry, and announces nothing.
	again := e.enterLobby(userKeys, chatID)
	require.Equal(t, chatpb.EnterLobbyResponse_OK, again.Result)
	require.True(t, again.Lobby.EnteredAt.AsTime().Equal(resp.Lobby.EnteredAt.AsTime()))
	time.Sleep(100 * time.Millisecond)
	require.Len(t, e.lobbyUpdatesOnUserTopic(e.userID, chatID), 1)
	require.Len(t, e.pusher.pushesTo(e.userID), 1)

	// Withdrawing.
	require.Equal(t, chatpb.LeaveLobbyResponse_OK, e.leaveLobby(userKeys, chatID).Result)
	got = e.getChat(userKeys, chatID)
	require.False(t, got.Metadata.InLobby)
	updates = e.waitForLobbyUpdates(e.userID, chatID, 2)
	require.Equal(t, userID.Value, updates[1].GetMemberLeft().GetUserId().GetValue())
	require.Equal(t, chatpb.LeaveLobbyResponse_OK, e.leaveLobby(userKeys, chatID).Result)
	time.Sleep(100 * time.Millisecond)
	require.Len(t, e.lobbyUpdatesOnUserTopic(e.userID, chatID), 2)
	e.requireNoLobbyUpdatesOnChatTopic(chatID)
	e.requireNoRosterUpdates(userID, chatID)

	// A withdrawal is pushed to no one, and the waiting user is never pushed.
	require.Len(t, e.pusher.pushesTo(e.userID), 1)
	require.Empty(t, e.pusher.pushesTo(userID))

	// A user with no display name is pushed as someone.
	_, namelessKeys := e.addBoundUser("")
	require.Equal(t, chatpb.EnterLobbyResponse_OK, e.enterLobby(namelessKeys, chatID).Result)
	pushes = e.waitForPushes(e.userID, 2)
	requireLobbyPush(t, pushes[1], chatID, "Sunday Hikers", "Someone requested to join")
}

// testServer_Lobby_Limits pins the server's caps: a full lobby is LOBBY_FULL,
// a user in too many lobbies TOO_MANY_LOBBIES, and neither refuses a user who
// is already waiting.
func testServer_Lobby_Limits(t *testing.T, s chat.Store) {
	e := newServerEnvWithConfig(t, s, serverConfig{lobbyLimits: &chat.LobbyLimits{LobbySize: 1, LobbiesPerUser: 1}})
	_, aKeys := e.addBoundUser("A")
	_, bKeys := e.addBoundUser("B")
	first := e.putKeyedPrivateGroup("First")
	second := e.putKeyedPrivateGroup("Second")

	require.Equal(t, chatpb.EnterLobbyResponse_OK, e.enterLobby(aKeys, first).Result)
	require.Equal(t, chatpb.EnterLobbyResponse_LOBBY_FULL, e.enterLobby(bKeys, first).Result)
	require.Equal(t, chatpb.EnterLobbyResponse_TOO_MANY_LOBBIES, e.enterLobby(aKeys, second).Result)
	require.Equal(t, chatpb.EnterLobbyResponse_OK, e.enterLobby(aKeys, first).Result)
	require.Equal(t, chatpb.EnterLobbyResponse_OK, e.enterLobby(bKeys, second).Result)

	// Nothing was recorded for a refused entry.
	got := e.getChat(bKeys, first)
	require.False(t, got.Metadata.InLobby)
	got = e.getChat(aKeys, second)
	require.False(t, got.Metadata.InLobby)
}

// testServer_Lobby_GetLobbyMembers pins the creator's read of the lobby:
// earliest entered first, paged under a chat-bound token, each member with
// their profile and public key.
func testServer_Lobby_GetLobbyMembers(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	chatID := e.putKeyedPrivateGroup("Sunday Hikers")
	other := e.putKeyedPrivateGroup("Other")

	const n = 3
	userIDs := make([]*commonpb.UserId, n)
	userKeys := make([]model.KeyPair, n)
	for i := range n {
		userIDs[i], userKeys[i] = e.addBoundUser(fmt.Sprintf("User %d", i))
		require.Equal(t, chatpb.EnterLobbyResponse_OK, e.enterLobby(userKeys[i], chatID).Result)
		time.Sleep(2 * time.Millisecond)
	}
	whole := e.waitForLobbyMembers(e.keys, chatID, n)
	require.False(t, whole.HasMore)
	require.Nil(t, whole.PagingToken)
	for i, m := range whole.Members {
		require.Equal(t, userIDs[i].Value, m.UserProfile.UserId.Value, i)
		require.Equal(t, fmt.Sprintf("User %d", i), m.UserProfile.DisplayName, i)
		require.True(t, proto.Equal(userKeys[i].Proto(), m.PublicKey), i)
		require.NotNil(t, m.EnteredAt)
		if i > 0 {
			require.False(t, m.EnteredAt.AsTime().Before(whole.Members[i-1].EnteredAt.AsTime()))
		}
	}

	// Pages of two.
	page, err := e.getLobbyMembers(e.keys, chatID, &commonpb.QueryOptions{PageSize: 2})
	require.NoError(t, err)
	require.Equal(t, chatpb.GetLobbyMembersResponse_OK, page.Result)
	require.Len(t, page.Members, 2)
	require.True(t, page.HasMore)
	require.NotNil(t, page.PagingToken)
	require.Equal(t, userIDs[0].Value, page.Members[0].UserProfile.UserId.Value)
	require.Equal(t, userIDs[1].Value, page.Members[1].UserProfile.UserId.Value)
	rest, err := e.getLobbyMembers(e.keys, chatID, &commonpb.QueryOptions{PageSize: 2, PagingToken: page.PagingToken})
	require.NoError(t, err)
	require.Equal(t, chatpb.GetLobbyMembersResponse_OK, rest.Result)
	require.Len(t, rest.Members, 1)
	require.False(t, rest.HasMore)
	require.Nil(t, rest.PagingToken)
	require.Equal(t, userIDs[2].Value, rest.Members[0].UserProfile.UserId.Value)

	// The token is the chat's: replayed into another lobby it is refused, as
	// is a malformed one.
	_, err = e.getLobbyMembers(e.keys, other, &commonpb.QueryOptions{PagingToken: page.PagingToken})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = e.getLobbyMembers(e.keys, chatID, &commonpb.QueryOptions{PagingToken: &commonpb.PagingToken{Value: []byte{1, 2, 3}}})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// The other lobby is empty, and a waiting user sees no lobby at all.
	empty, err := e.getLobbyMembers(e.keys, other, nil)
	require.NoError(t, err)
	require.Equal(t, chatpb.GetLobbyMembersResponse_OK, empty.Result)
	require.Empty(t, empty.Members)
	denied, err := e.getLobbyMembers(userKeys[0], chatID, nil)
	require.NoError(t, err)
	require.Equal(t, chatpb.GetLobbyMembersResponse_DENIED, denied.Result)
}

// testServer_Lobby_Admit pins an admission: the waiting user becomes a
// member holding the envelope the creator wrapped, announced as a join to the
// members and to them, and as a MemberLeft to the creator; a repeat is a
// no-op, a user not waiting is NOT_IN_LOBBY, and a member who leaves may wait
// again.
func testServer_Lobby_Admit(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.profiles.displayNames[string(e.userID.Value)] = "Creator"
	e.bindKey(e.userID, e.keys)
	userID, userKeys := e.addBoundUser("Waiting")
	chatID := e.putKeyedPrivateGroup("Sunday Hikers")

	require.Equal(t, chatpb.EnterLobbyResponse_OK, e.enterLobby(userKeys, chatID).Result)
	e.waitForLobbyUpdates(e.userID, chatID, 1)

	// Not waiting.
	strangerID, _ := e.addBoundUser("Stranger")
	require.Equal(t, chatpb.AdmitLobbyMemberResponse_NOT_IN_LOBBY, e.admitLobbyMember(e.keys, chatID, strangerID, keyEnvelopeProto(2)).Result)

	// Admitted.
	wrapped := keyEnvelopeProto(2)
	require.Equal(t, chatpb.AdmitLobbyMemberResponse_OK, e.admitLobbyMember(e.keys, chatID, userID, wrapped).Result)
	isMember, err := s.IsMember(e.ctx, chatID, userID)
	require.NoError(t, err)
	require.True(t, isMember)
	envelope := e.mustGetKeyEnvelope(userKeys, chatID)
	require.Equal(t, chatpb.GetKeyEnvelopeResponse_OK, envelope.Result)
	require.True(t, proto.Equal(wrapped, envelope.GetKeyEnvelope()))
	require.Equal(t, e.userID.Value, envelope.WrappedBy.Value)

	// With the creator's public key, which nothing else returns to a member,
	// so the admitted user can open the envelope against it.
	require.True(t, proto.Equal(e.keys.Proto(), envelope.WrappedByPublicKey))
	got := e.getChat(userKeys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, got.Result)
	require.False(t, got.Metadata.InLobby)
	require.Len(t, got.Metadata.Members, 1)
	require.NotNil(t, got.Metadata.ViewerState)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 2, Version: 1}, got.Metadata.RosterSummary))

	// Announced as a join: to the members, only that the roster moved; the
	// admitted user's own copy with the metadata on their topic; and to the
	// creator, the lobby shrank.
	e.waitForRosterUpdates(userID, chatID, 1)
	onChat, _ := e.rosterUpdatesOnChatTopic(chatID)
	require.Len(t, onChat, 1)
	require.NotNil(t, onChat[0].GetMembershipChanged())
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 2, Version: 1}, onChat[0].RosterSummary))
	toUser := e.rosterUpdatesOnUserTopic(userID, chatID)
	require.Len(t, toUser, 1)
	joined := toUser[0].GetMemberJoined()
	require.Equal(t, "Waiting", joined.Member.UserProfile.DisplayName)
	require.Equal(t, uint64(1), joined.Member.Version)
	require.NotNil(t, joined.Member.JoinedAt)
	require.NotNil(t, joined.Metadata)
	require.Equal(t, "Sunday Hikers", joined.Metadata.Title)
	require.False(t, joined.Metadata.InLobby)
	updates := e.waitForLobbyUpdates(e.userID, chatID, 2)
	require.Equal(t, userID.Value, updates[1].GetMemberLeft().GetUserId().GetValue())
	e.requireNoLobbyUpdatesOnChatTopic(chatID)

	// And pushed to the admitted user; the creator was pushed only the entry.
	pushes := e.waitForPushes(userID, 1)
	require.Len(t, pushes, 1)
	requireLobbyPush(t, pushes[0], chatID, "Sunday Hikers", "You were added to the group")
	require.Len(t, e.pusher.pushesTo(e.userID), 1)

	// A repeat is the no-op the proto promises, their envelope untouched.
	require.Equal(t, chatpb.AdmitLobbyMemberResponse_OK, e.admitLobbyMember(e.keys, chatID, userID, keyEnvelopeProto(3)).Result)
	envelope = e.mustGetKeyEnvelope(userKeys, chatID)
	require.True(t, proto.Equal(wrapped, envelope.GetKeyEnvelope()))
	time.Sleep(100 * time.Millisecond)
	require.Len(t, e.lobbyUpdatesOnUserTopic(e.userID, chatID), 2)
	require.Len(t, e.pusher.pushesTo(userID), 1)
	onChat, _ = e.rosterUpdatesOnChatTopic(chatID)
	require.Len(t, onChat, 1)

	// The lobby is empty for the creator, and the member replaces the envelope
	// with their own as any member may.
	empty, err := e.getLobbyMembers(e.keys, chatID, nil)
	require.NoError(t, err)
	require.Empty(t, empty.Members)
	require.Equal(t, chatpb.SetKeyEnvelopeResponse_OK, e.mustSetKeyEnvelope(userKeys, chatID, keyEnvelopeProto(4)).Result)

	// Leaving discards the envelope, and the way back is the lobby.
	left, err := e.leaveChat(userKeys, chatID)
	require.NoError(t, err)
	require.Equal(t, chatpb.LeaveChatResponse_OK, left.Result)
	require.Equal(t, chatpb.EnterLobbyResponse_OK, e.enterLobby(userKeys, chatID).Result)
	require.Equal(t, chatpb.AdmitLobbyMemberResponse_OK, e.admitLobbyMember(e.keys, chatID, userID, keyEnvelopeProto(5)).Result)
	envelope = e.mustGetKeyEnvelope(userKeys, chatID)
	require.True(t, proto.Equal(keyEnvelopeProto(5), envelope.GetKeyEnvelope()))

	// Each wait and each admission is pushed again.
	e.waitForPushes(e.userID, 2)
	e.waitForPushes(userID, 2)
}

// testServer_Lobby_Deny pins a denial: the user leaves the lobby and is told
// nothing, the creator hears a MemberLeft, a repeat is a no-op, and the user
// may enter again.
func testServer_Lobby_Deny(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	userID, userKeys := e.addBoundUser("Waiting")
	chatID := e.putKeyedPrivateGroup("Sunday Hikers")

	require.Equal(t, chatpb.EnterLobbyResponse_OK, e.enterLobby(userKeys, chatID).Result)
	e.waitForLobbyUpdates(e.userID, chatID, 1)

	require.Equal(t, chatpb.DenyLobbyMemberResponse_OK, e.denyLobbyMember(e.keys, chatID, userID).Result)
	got := e.getChat(userKeys, chatID)
	require.False(t, got.Metadata.InLobby)
	isMember, err := s.IsMember(e.ctx, chatID, userID)
	require.NoError(t, err)
	require.False(t, isMember)
	updates := e.waitForLobbyUpdates(e.userID, chatID, 2)
	require.Equal(t, userID.Value, updates[1].GetMemberLeft().GetUserId().GetValue())
	require.Empty(t, e.lobbyUpdatesOnUserTopic(userID, chatID))
	e.requireNoRosterUpdates(userID, chatID)

	require.Equal(t, chatpb.DenyLobbyMemberResponse_OK, e.denyLobbyMember(e.keys, chatID, userID).Result)
	time.Sleep(100 * time.Millisecond)
	require.Len(t, e.lobbyUpdatesOnUserTopic(e.userID, chatID), 2)

	// A denial is pushed to no one.
	require.Empty(t, e.pusher.pushesTo(userID))
	require.Len(t, e.pusher.pushesTo(e.userID), 1)

	require.Equal(t, chatpb.EnterLobbyResponse_OK, e.enterLobby(userKeys, chatID).Result)
	got = e.getChat(userKeys, chatID)
	require.True(t, got.Metadata.InLobby)
}

// testServer_PrivateGroup_UserJoinedMessages pins which joins write a
// UserJoined system message: every real admission, as admitted by the
// creator, and the creator's own rejoin, as on their own, each at the roster
// version the join produced; never a creation, a departure, a refusal, a
// no-op or a public group's join; and a failure to write one never fails the
// join.
func testServer_PrivateGroup_UserJoinedMessages(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	rosterVersion := func(chatID *commonpb.ChatId) uint64 {
		t.Helper()
		got := e.getChat(e.keys, chatID)
		require.Equal(t, chatpb.GetChatResponse_OK, got.Result)
		return got.Metadata.RosterSummary.Version
	}

	// Creating the group writes none.
	created := e.mustStartPrivateGroupChat(e.keys, newIdempotencyKey(), privateGroupParams("Sunday Hikers"))
	require.Equal(t, chatpb.StartChatResponse_OK, created.Result)
	chatID := created.Chat.ChatId
	require.Empty(t, e.systemMessages.userJoinedCalls(chatID))

	// No one is admitted to a keyless group, nor anyone not waiting.
	userID, userKeys := e.addBoundUser("Waiting")
	require.Equal(t, chatpb.AdmitLobbyMemberResponse_DENIED, e.admitLobbyMember(e.keys, chatID, userID, keyEnvelopeProto(2)).Result)
	require.Equal(t, chatpb.SetKeyEnvelopeResponse_OK, e.mustSetKeyEnvelope(e.keys, chatID, keyEnvelopeProto(1)).Result)
	require.Equal(t, chatpb.AdmitLobbyMemberResponse_NOT_IN_LOBBY, e.admitLobbyMember(e.keys, chatID, userID, keyEnvelopeProto(2)).Result)
	require.Empty(t, e.systemMessages.userJoinedCalls(chatID))

	// An admission writes one, admitted by the creator.
	require.Equal(t, chatpb.EnterLobbyResponse_OK, e.enterLobby(userKeys, chatID).Result)
	require.Equal(t, chatpb.AdmitLobbyMemberResponse_OK, e.admitLobbyMember(e.keys, chatID, userID, keyEnvelopeProto(2)).Result)
	calls := e.systemMessages.userJoinedCalls(chatID)
	require.Len(t, calls, 1)
	requireUserJoinedCall(t, calls[0], userID, chat.AdmittedBy(e.userID), rosterVersion(chatID))

	// A repeat is the no-op, and writes nothing.
	require.Equal(t, chatpb.AdmitLobbyMemberResponse_OK, e.admitLobbyMember(e.keys, chatID, userID, keyEnvelopeProto(3)).Result)
	require.Len(t, e.systemMessages.userJoinedCalls(chatID), 1)

	// The creator leaves, which writes none, and rejoins on their own.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, chatID).Result)
	require.Len(t, e.systemMessages.userJoinedCalls(chatID), 1)
	joined := e.mustJoinChat(e.keys, chatID)
	require.Equal(t, chatpb.JoinChatResponse_OK, joined.Result)
	calls = e.systemMessages.userJoinedCalls(chatID)
	require.Len(t, calls, 2)
	requireUserJoinedCall(t, calls[1], e.userID, chat.JoinedDirectly(), joined.Chat.RosterSummary.Version)

	// Their join of a group they are in is the no-op.
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(e.keys, chatID).Result)
	require.Len(t, e.systemMessages.userJoinedCalls(chatID), 2)

	// A failure to write one never fails the admission.
	e.systemMessages.setErr(errors.New("unavailable"))
	otherID, otherKeys := e.addBoundUser("Other")
	require.Equal(t, chatpb.EnterLobbyResponse_OK, e.enterLobby(otherKeys, chatID).Result)
	require.Equal(t, chatpb.AdmitLobbyMemberResponse_OK, e.admitLobbyMember(e.keys, chatID, otherID, keyEnvelopeProto(4)).Result)
	isMember, err := s.IsMember(e.ctx, chatID, otherID)
	require.NoError(t, err)
	require.True(t, isMember)
	calls = e.systemMessages.userJoinedCalls(chatID)
	require.Len(t, calls, 3)
	requireUserJoinedCall(t, calls[2], otherID, chat.AdmittedBy(e.userID), rosterVersion(chatID))
	e.systemMessages.setErr(nil)

	// A public group's join writes none.
	groupID := e.putGroup("Open", at(1))
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(userKeys, groupID).Result)
	require.Empty(t, e.systemMessages.userJoinedCalls(groupID))
}

func (e *serverEnv) setFeaturedGroups(keys model.KeyPair, chatIDs ...*commonpb.ChatId) (*chatpb.SetFeaturedGroupsResponse, error) {
	req := &chatpb.SetFeaturedGroupsRequest{ChatIds: chatIDs}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.SetFeaturedGroups(e.ctx, req)
}

func (e *serverEnv) mustSetFeaturedGroups(keys model.KeyPair, chatIDs ...*commonpb.ChatId) *chatpb.SetFeaturedGroupsResponse {
	resp, err := e.setFeaturedGroups(keys, chatIDs...)
	require.NoError(e.t, err)
	return resp
}

// getFeaturedGroups asks for username's featured groups, signed with keys
// when one is given and with no auth at all otherwise.
func (e *serverEnv) getFeaturedGroups(username string, keys ...model.KeyPair) *chatpb.GetFeaturedGroupsResponse {
	req := &chatpb.GetFeaturedGroupsRequest{
		Identifier: &chatpb.GetFeaturedGroupsRequest_Username{Username: &commonpb.Username{Value: username}},
	}
	if len(keys) > 0 {
		require.NoError(e.t, keys[0].Auth(req, &req.Auth))
	}
	resp, err := e.client.GetFeaturedGroups(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

// featuredGroupIDs projects featured groups onto their chat IDs, in order.
func featuredGroupIDs(groups []*chatpb.Metadata) [][]byte {
	out := make([][]byte, len(groups))
	for i, md := range groups {
		out[i] = md.ChatId.Value
	}
	return out
}

func testServer_FeaturedGroups_SetAndGet(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.profiles.usernames[string(e.userID.Value)] = "featurer"
	_, viewerKeys := e.addUser()

	// Nothing featured yet: OK and empty, for anyone.
	resp := e.getFeaturedGroups("featurer")
	require.Equal(t, chatpb.GetFeaturedGroupsResponse_OK, resp.Result)
	require.Empty(t, resp.FeaturedGroups)

	// A group the caller is in, with a picture, description and cover, and a
	// group they are not in: featuring needs no membership.
	picture := &blobpb.BlobId{Value: []byte("group-picture-01")}
	pictureRenditions := e.media.setRenditions(picture)
	member := e.putGroupWithPicture("Member Of", picture, at(1))
	cover := &blobpb.BlobId{Value: []byte("group-cover-0001")}
	e.describeGroup(member, "About us", cover)
	other := chat.MustGenerateGroupChatID()
	require.NoError(t, e.store.PutChat(e.ctx, &chat.Chat{
		ID:           other,
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{model.MustGenerateUserID()},
		Title:        "Not In",
		LastActivity: at(2),
	}))

	set := e.mustSetFeaturedGroups(e.keys, other, member)
	require.Equal(t, chatpb.SetFeaturedGroupsResponse_OK, set.Result)
	require.Equal(t, [][]byte{other.Value, member.Value}, featuredGroupIDs(set.FeaturedGroups))

	// Each group is its record as a list view shows it, with nothing about
	// the viewer and no messaging state: the description and the resolved
	// profile picture, but no cover, which was never resolved.
	md := set.FeaturedGroups[1]
	require.Equal(t, "Member Of", md.Title)
	require.Equal(t, "About us", md.Description)
	require.Equal(t, chatpb.ChatType_GROUP, md.Type)
	require.Len(t, md.GetProfilePicture().GetRenditions(), len(pictureRenditions))
	require.NotNil(t, md.GetProfilePicture().GetRenditions()[0].Blob)
	require.Nil(t, md.CoverPicture)
	require.False(t, e.media.resolved[string(cover.Value)])
	require.EqualValues(t, 1, md.GetRosterSummary().GetMemberCount())
	require.Empty(t, md.Members)
	require.Nil(t, md.ViewerState)
	require.Nil(t, md.LastMessage)
	require.Zero(t, md.LatestEventSequence)
	require.False(t, md.IsPrivate)
	require.False(t, md.InLobby)

	// The list is the same for every caller: no auth, someone else, and the
	// featurer themself all get what the set returned.
	for _, got := range []*chatpb.GetFeaturedGroupsResponse{
		e.getFeaturedGroups("featurer"),
		e.getFeaturedGroups("featurer", viewerKeys),
		e.getFeaturedGroups("featurer", e.keys),
	} {
		require.Equal(t, chatpb.GetFeaturedGroupsResponse_OK, got.Result)
		require.Len(t, got.FeaturedGroups, len(set.FeaturedGroups))
		for i := range set.FeaturedGroups {
			require.True(t, proto.Equal(set.FeaturedGroups[i], got.FeaturedGroups[i]))
		}
	}

	// Setting the same list is OK; a reorder and a removal replace it.
	set = e.mustSetFeaturedGroups(e.keys, other, member)
	require.Equal(t, chatpb.SetFeaturedGroupsResponse_OK, set.Result)
	set = e.mustSetFeaturedGroups(e.keys, member)
	require.Equal(t, chatpb.SetFeaturedGroupsResponse_OK, set.Result)
	require.Equal(t, [][]byte{member.Value}, featuredGroupIDs(e.getFeaturedGroups("featurer").FeaturedGroups))

	// An empty list clears them.
	set = e.mustSetFeaturedGroups(e.keys)
	require.Equal(t, chatpb.SetFeaturedGroupsResponse_OK, set.Result)
	require.Empty(t, set.FeaturedGroups)
	require.Empty(t, e.getFeaturedGroups("featurer").FeaturedGroups)
}

func testServer_FeaturedGroups_Refused(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.profiles.usernames[string(e.userID.Value)] = "featurer"

	public := e.putGroup("Public", at(1))
	require.Equal(t, chatpb.SetFeaturedGroupsResponse_OK, e.mustSetFeaturedGroups(e.keys, public).Result)
	requireUnchanged := func() {
		t.Helper()
		require.Equal(t, [][]byte{public.Value}, featuredGroupIDs(e.getFeaturedGroups("featurer").FeaturedGroups))
	}

	// A group that does not exist: NOT_FOUND, nothing written.
	missing := chat.MustGenerateGroupChatID()
	resp := e.mustSetFeaturedGroups(e.keys, public, missing)
	require.Equal(t, chatpb.SetFeaturedGroupsResponse_NOT_FOUND, resp.Result)
	require.Empty(t, resp.FeaturedGroups)
	requireUnchanged()

	// A private group: DENIED, nothing written, member or not.
	private := e.putKeyedPrivateGroup("Private")
	resp = e.mustSetFeaturedGroups(e.keys, private, public)
	require.Equal(t, chatpb.SetFeaturedGroupsResponse_DENIED, resp.Result)
	require.Empty(t, resp.FeaturedGroups)
	requireUnchanged()

	// Both: existence is judged first.
	resp = e.mustSetFeaturedGroups(e.keys, private, missing)
	require.Equal(t, chatpb.SetFeaturedGroupsResponse_NOT_FOUND, resp.Result)
	requireUnchanged()

	// A DM's ID, a repeat, or too many groups is an invalid argument.
	tooMany := make([]*commonpb.ChatId, chat.MaxFeaturedGroups+1)
	for i := range tooMany {
		tooMany[i] = e.putGroup("Many", at(1))
	}
	for name, chatIDs := range map[string][]*commonpb.ChatId{
		"dm":       {public, e.putDM(at(1))},
		"repeated": {public, public},
		"too many": tooMany,
	} {
		_, err := e.setFeaturedGroups(e.keys, chatIDs...)
		require.Equal(t, codes.InvalidArgument, status.Code(err), name)
	}
	requireUnchanged()

	// Setting needs auth.
	_, err := e.client.SetFeaturedGroups(e.ctx, &chatpb.SetFeaturedGroupsRequest{ChatIds: []*commonpb.ChatId{public}})
	require.Error(t, err)
	requireUnchanged()
}

func testServer_FeaturedGroups_UnknownUsername(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	require.Equal(t, chatpb.GetFeaturedGroupsResponse_NOT_FOUND, e.getFeaturedGroups("nobody").Result)
	require.Equal(t, chatpb.GetFeaturedGroupsResponse_NOT_FOUND, e.getFeaturedGroups("nobody", e.keys).Result)
}

// putGatedGroup persists a public group whose members are the env user and
// the given others, gated on mint by a minimum listener balance.
func (e *serverEnv) putGatedGroup(title string, mint *commonpb.PublicKey, others ...*commonpb.UserId) *commonpb.ChatId {
	chatID := chat.MustGenerateGroupChatID()
	require.NoError(e.t, e.store.PutChat(e.ctx, &chat.Chat{
		ID:      chatID,
		Type:    chatpb.ChatType_GROUP,
		Members: append([]*commonpb.UserId{e.userID}, others...),
		Title:   title,
		MinimumListenerBalance: &chat.MinimumBalance{
			Currency:     "usd",
			NativeAmount: 1,
			Mints:        []*commonpb.PublicKey{mint},
		},
		CreatorID:    e.userID,
		LastActivity: at(1),
	}))
	return chatID
}

func testServer_HoldingsSummary(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.profiles.usernames[string(e.userID.Value)] = "holder"
	mint := model.MustGenerateKeyPair().Proto()

	// A group gated on the mint, with a snapshot of it.
	gated := e.putGatedGroup("Gated", mint)
	snapshot := chat.GroupHoldings{ChatID: gated, Mint: mint, Quarks: 12_345, HolderCount: 13, AsOf: at(100)}
	_, err := e.store.PutGroupHoldings(e.ctx, snapshot)
	require.NoError(t, err)

	// A gated group with no snapshot yet, and a group gated on nothing with a
	// snapshot written anyway: neither carries a summary. (A gated group's
	// snapshot is always of its own mint: the store refuses another.)
	unrefreshed := e.putGatedGroup("Unrefreshed", mint)
	ungated := e.putGroup("Ungated", at(1))
	_, err = e.store.PutGroupHoldings(e.ctx, chat.GroupHoldings{ChatID: ungated, Mint: mint, Quarks: 100, HolderCount: 10, AsOf: at(100)})
	require.NoError(t, err)
	// Nor does a gated group whose snapshot is stored but not listed: too
	// few holders, however much they hold.
	unlisted := e.putGatedGroup("Unlisted", mint)
	_, err = e.store.PutGroupHoldings(e.ctx, chat.GroupHoldings{ChatID: unlisted, Mint: mint, Quarks: 9_000_000, HolderCount: chat.MinGroupHoldingsHolders - 1, AsOf: at(100)})
	require.NoError(t, err)

	requireSummary := func(md *chatpb.Metadata) {
		t.Helper()
		require.NotNil(t, md.HoldingsSummary)
		require.True(t, proto.Equal(snapshot.ToProto(), md.HoldingsSummary))
	}

	// GetChat carries it, to a member and to anyone else.
	resp := e.getChat(e.keys, gated)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	requireSummary(resp.Metadata)
	_, outsiderKeys := e.addUser()
	resp = e.getChat(outsiderKeys, gated)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	requireSummary(resp.Metadata)

	for _, chatID := range []*commonpb.ChatId{unrefreshed, ungated, unlisted} {
		resp := e.getChat(e.keys, chatID)
		require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
		require.Nil(t, resp.Metadata.HoldingsSummary)
	}

	// So does a public list.
	set := e.mustSetFeaturedGroups(e.keys, gated, ungated)
	require.Equal(t, chatpb.SetFeaturedGroupsResponse_OK, set.Result)
	requireSummary(set.FeaturedGroups[0])
	require.Nil(t, set.FeaturedGroups[1].HoldingsSummary)
	got := e.getFeaturedGroups("holder")
	require.Equal(t, chatpb.GetFeaturedGroupsResponse_OK, got.Result)
	requireSummary(got.FeaturedGroups[0])
}

func (e *serverEnv) silenceUser(keys model.KeyPair, chatID *commonpb.ChatId, userID *commonpb.UserId, silence *chatpb.SilenceState) (*chatpb.SilenceUserResponse, error) {
	req := &chatpb.SilenceUserRequest{ChatId: chatID, UserId: userID, Silence: silence}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.SilenceUser(e.ctx, req)
}

func (e *serverEnv) mustSilenceUser(keys model.KeyPair, chatID *commonpb.ChatId, userID *commonpb.UserId, silence *chatpb.SilenceState) chatpb.SilenceUserResponse_Result {
	resp, err := e.silenceUser(keys, chatID, userID, silence)
	require.NoError(e.t, err)
	return resp.Result
}

func (e *serverEnv) mustUnsilenceUser(keys model.KeyPair, chatID *commonpb.ChatId, userID *commonpb.UserId) chatpb.UnsilenceUserResponse_Result {
	req := &chatpb.UnsilenceUserRequest{ChatId: chatID, UserId: userID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.UnsilenceUser(e.ctx, req)
	require.NoError(e.t, err)
	return resp.Result
}

func (e *serverEnv) mustBanUser(keys model.KeyPair, chatID *commonpb.ChatId, userID *commonpb.UserId) chatpb.BanUserResponse_Result {
	req := &chatpb.BanUserRequest{ChatId: chatID, UserId: userID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.BanUser(e.ctx, req)
	require.NoError(e.t, err)
	return resp.Result
}

func (e *serverEnv) mustUnbanUser(keys model.KeyPair, chatID *commonpb.ChatId, userID *commonpb.UserId) chatpb.UnbanUserResponse_Result {
	req := &chatpb.UnbanUserRequest{ChatId: chatID, UserId: userID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.UnbanUser(e.ctx, req)
	require.NoError(e.t, err)
	return resp.Result
}

func (e *serverEnv) getRestrictedUsers(keys model.KeyPair, chatID *commonpb.ChatId, opts *commonpb.QueryOptions) (*chatpb.GetRestrictedUsersResponse, error) {
	req := &chatpb.GetRestrictedUsersRequest{ChatId: chatID, QueryOptions: opts}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.GetRestrictedUsers(e.ctx, req)
}

func (e *serverEnv) mustGetRestrictedUsers(keys model.KeyPair, chatID *commonpb.ChatId, opts *commonpb.QueryOptions) *chatpb.GetRestrictedUsersResponse {
	resp, err := e.getRestrictedUsers(keys, chatID, opts)
	require.NoError(e.t, err)
	return resp
}

// waitForRestrictedListing waits until the store's listing of chatID's
// restricted users holds n users: it is read from an index that trails writes
// (see chat.Store.GetRestrictedUsersPage).
func (e *serverEnv) waitForRestrictedListing(chatID *commonpb.ChatId, n int) {
	require.Eventually(e.t, func() bool {
		users, err := e.store.GetRestrictedUsersPage(e.ctx, chatID, nil, 0)
		require.NoError(e.t, err)
		return len(users) == n
	}, 5*time.Second, 10*time.Millisecond)
}

func silenceUntil(until time.Time) *chatpb.SilenceState {
	return &chatpb.SilenceState{Duration: &chatpb.SilenceState_Until{Until: timestamppb.New(until)}}
}

func silenceForever() *chatpb.SilenceState {
	return &chatpb.SilenceState{Duration: &chatpb.SilenceState_Forever_{Forever: &chatpb.SilenceState_Forever{}}}
}

// silencedViewerState is viewerState with a silence under restrictions.
func silencedViewerState(version uint64, silence *chatpb.SilenceState, isCreator bool) *chatpb.ViewerState {
	state := viewerState(version, nil, isCreator)
	if silence != nil {
		state.Restrictions = &chatpb.ViewerState_Restrictions{Silence: silence}
	}
	return state
}

func restrictedUserIDs(users []*chatpb.RestrictedUser) [][]byte {
	out := make([][]byte, len(users))
	for i, u := range users {
		out[i] = u.UserId.Value
	}
	return out
}

// testServer_Moderation_Gates pins who may moderate: the recorded creator of
// a group, public or private, while a member, and no one else; and that the
// creator cannot be restricted, while lifting a restriction from them is the
// no-op. permissions.moderation on the hydrated ViewerState agrees.
func testServer_Moderation_Gates(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	memberID, memberKeys := e.addUser()
	target := model.MustGenerateUserID()
	public := e.putOwnedGroup("Public", nil, memberID)
	private := e.putKeyedPrivateGroup("Private", memberID)

	type results struct {
		silence   chatpb.SilenceUserResponse_Result
		unsilence chatpb.UnsilenceUserResponse_Result
		ban       chatpb.BanUserResponse_Result
		unban     chatpb.UnbanUserResponse_Result
		list      chatpb.GetRestrictedUsersResponse_Result
	}
	all := func(keys model.KeyPair, chatID *commonpb.ChatId, userID *commonpb.UserId) results {
		t.Helper()
		list := e.mustGetRestrictedUsers(keys, chatID, nil)
		return results{
			silence:   e.mustSilenceUser(keys, chatID, userID, silenceForever()),
			unsilence: e.mustUnsilenceUser(keys, chatID, userID),
			ban:       e.mustBanUser(keys, chatID, userID),
			unban:     e.mustUnbanUser(keys, chatID, userID),
			list:      list.Result,
		}
	}
	denied := results{
		silence:   chatpb.SilenceUserResponse_DENIED,
		unsilence: chatpb.UnsilenceUserResponse_DENIED,
		ban:       chatpb.BanUserResponse_DENIED,
		unban:     chatpb.UnbanUserResponse_DENIED,
		list:      chatpb.GetRestrictedUsersResponse_DENIED,
	}

	// A DM is DENIED before anything is read, whoever asks.
	dm := e.putDM(at(1))
	require.Equal(t, denied, all(e.keys, dm, target))

	// A group that is not there is NOT_FOUND.
	require.Equal(t, results{
		silence:   chatpb.SilenceUserResponse_NOT_FOUND,
		unsilence: chatpb.UnsilenceUserResponse_NOT_FOUND,
		ban:       chatpb.BanUserResponse_NOT_FOUND,
		unban:     chatpb.UnbanUserResponse_NOT_FOUND,
		list:      chatpb.GetRestrictedUsersResponse_NOT_FOUND,
	}, all(e.keys, chat.MustGenerateGroupChatID(), target))

	for _, chatID := range []*commonpb.ChatId{public, private} {
		// A member who is not the creator may not moderate, and is told so.
		require.Equal(t, denied, all(memberKeys, chatID, target))
		md := e.getChat(memberKeys, chatID).Metadata
		require.Nil(t, md.GetViewerState().GetPermissions().GetModeration())

		// The creator may, and is told so.
		md = e.getChat(e.keys, chatID).Metadata
		require.True(t, md.GetViewerState().GetPermissions().GetModeration().GetCanSilence())
		require.True(t, md.GetViewerState().GetPermissions().GetModeration().GetCanBan())
		require.Equal(t, results{}, all(e.keys, chatID, target))

		// The creator cannot be restricted; lifting a restriction from them
		// is the no-op.
		require.Equal(t, chatpb.SilenceUserResponse_CANNOT_RESTRICT_CREATOR, e.mustSilenceUser(e.keys, chatID, e.userID, silenceForever()))
		require.Equal(t, chatpb.BanUserResponse_CANNOT_RESTRICT_CREATOR, e.mustBanUser(e.keys, chatID, e.userID))
		require.Equal(t, chatpb.UnsilenceUserResponse_OK, e.mustUnsilenceUser(e.keys, chatID, e.userID))
		require.Equal(t, chatpb.UnbanUserResponse_OK, e.mustUnbanUser(e.keys, chatID, e.userID))
	}

	// A timed silence must end in the future, checked before anything is
	// read.
	_, err := e.silenceUser(e.keys, public, target, silenceUntil(time.Now().Add(-time.Minute)))
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = e.silenceUser(e.keys, dm, target, silenceUntil(time.Now().Add(-time.Minute)))
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// A creator who has left moderates nothing until they rejoin.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, public).Result)
	require.Equal(t, denied, all(e.keys, public, target))
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(e.keys, public).Result)
	require.Equal(t, results{}, all(e.keys, public, target))
}

// testServer_Moderation_Silence pins a silence on a member: it reaches the
// member's own devices, and no one else, on every real change and never on a
// no-op; it is shown on their ViewerState until lifted; and it survives
// their leaving and rejoining.
func testServer_Moderation_Silence(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	memberID, memberKeys := e.addUser()
	otherID, _ := e.addUser()
	chatID := e.putOwnedGroup("Group", nil, memberID, otherID)

	until := time.Now().Add(time.Hour).Truncate(time.Second)
	require.Equal(t, chatpb.SilenceUserResponse_OK, e.mustSilenceUser(e.keys, chatID, memberID, silenceUntil(until)))
	want := silencedViewerState(1, silenceUntil(until), false)
	states := e.waitForViewerStateUpdates(memberID, chatID, 1)
	require.NoError(t, protoutil.ProtoEqualError(want, states[0]))
	require.NoError(t, protoutil.ProtoEqualError(want, e.getChat(memberKeys, chatID).Metadata.ViewerState))
	require.Empty(t, e.viewerStateUpdatesOnUserTopic(e.userID, chatID))
	require.Empty(t, e.viewerStateUpdatesOnUserTopic(otherID, chatID))

	// The same silence again is the no-op: nothing is published.
	require.Equal(t, chatpb.SilenceUserResponse_OK, e.mustSilenceUser(e.keys, chatID, memberID, silenceUntil(until)))
	e.waitForViewerStateUpdates(memberID, chatID, 1)

	// A different one replaces it.
	require.Equal(t, chatpb.SilenceUserResponse_OK, e.mustSilenceUser(e.keys, chatID, memberID, silenceForever()))
	want = silencedViewerState(2, silenceForever(), false)
	states = e.waitForViewerStateUpdates(memberID, chatID, 2)
	require.NoError(t, protoutil.ProtoEqualError(want, states[1]))

	// It survives leaving and rejoining, which clears a mute but not a
	// silence.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(memberKeys, chatID).Result)
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(memberKeys, chatID).Result)
	require.NoError(t, protoutil.ProtoEqualError(want, e.getChat(memberKeys, chatID).Metadata.ViewerState))

	// A silenced member gets no mention suggestions: they may not speak.
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_DENIED, e.getMentionSuggestions(memberKeys, chatID).Result)

	// Lifting it reaches them too; lifting it again is the no-op.
	require.Equal(t, chatpb.UnsilenceUserResponse_OK, e.mustUnsilenceUser(e.keys, chatID, memberID))
	want = silencedViewerState(3, nil, false)
	states = e.waitForViewerStateUpdates(memberID, chatID, 3)
	require.NoError(t, protoutil.ProtoEqualError(want, states[2]))
	require.NoError(t, protoutil.ProtoEqualError(want, e.getChat(memberKeys, chatID).Metadata.ViewerState))
	require.Equal(t, chatpb.UnsilenceUserResponse_OK, e.mustUnsilenceUser(e.keys, chatID, memberID))
	e.waitForViewerStateUpdates(memberID, chatID, 3)
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_OK, e.getMentionSuggestions(memberKeys, chatID).Result)

	// A silence that has lapsed no longer stops them, but is still shown as
	// recorded until it is replaced or lifted.
	_, _, err := s.SetSilence(e.ctx, chatID, memberID, chat.Silence{Until: time.Now().Add(-time.Minute).Truncate(time.Second)}, time.Now())
	require.NoError(t, err)
	require.Equal(t, chatpb.GetMentionSuggestionsResponse_OK, e.getMentionSuggestions(memberKeys, chatID).Result)
	require.NotNil(t, e.getChat(memberKeys, chatID).Metadata.ViewerState.GetRestrictions().GetSilence())
}

// testServer_Moderation_SilenceNonMember pins a silence on a user who is not
// a member: it is recorded and announced to no one, and they find it on their
// ViewerState when they join.
func testServer_Moderation_SilenceNonMember(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	outsiderID, outsiderKeys := e.addUser()
	chatID := e.putOwnedGroup("Group", nil)

	require.Equal(t, chatpb.SilenceUserResponse_OK, e.mustSilenceUser(e.keys, chatID, outsiderID, silenceForever()))
	time.Sleep(100 * time.Millisecond)
	require.Empty(t, e.viewerStateUpdatesOnUserTopic(outsiderID, chatID))

	// A non-member is shown no ViewerState; once they join, the silence is
	// on it.
	require.Nil(t, e.getChat(outsiderKeys, chatID).Metadata.ViewerState)
	joined := e.mustJoinChat(outsiderKeys, chatID)
	require.Equal(t, chatpb.JoinChatResponse_OK, joined.Result)
	require.NoError(t, protoutil.ProtoEqualError(silencedViewerState(1, silenceForever(), false), joined.Chat.ViewerState))
}

// testServer_Moderation_BanMember pins a ban of a member of a public group:
// a departure announced with reason BANNED to them and naming no one to the
// rest, their mute cleared, joining refused until unbanned, and no
// readmission on the unban.
func testServer_Moderation_BanMember(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	memberID, memberKeys := e.addUser()
	otherID, _ := e.addUser()
	chatID := e.putOwnedGroup("Group", nil, memberID, otherID)
	require.Equal(t, chatpb.MuteChatResponse_OK, e.mustMuteChat(memberKeys, chatID, muteForever()).Result)
	e.waitForViewerStateUpdates(memberID, chatID, 1)

	require.Equal(t, chatpb.BanUserResponse_OK, e.mustBanUser(e.keys, chatID, memberID))

	isMember, err := s.IsMember(e.ctx, chatID, memberID)
	require.NoError(t, err)
	require.False(t, isMember)

	// Announced as a departure: to the banned user with the reason, to the
	// members naming no one, with the banned user excluded.
	e.waitForRosterUpdates(memberID, chatID, 1)
	summary := &chatpb.RosterSummary{MemberCount: 2, Version: 1}
	toMembers, excludes := e.rosterUpdatesOnChatTopic(chatID)
	require.Len(t, toMembers, 1)
	require.NotNil(t, toMembers[0].GetMembershipChanged())
	require.NoError(t, protoutil.ProtoEqualError(summary, toMembers[0].RosterSummary))
	require.Len(t, excludes[0], 1)
	require.Equal(t, memberID.Value, excludes[0][0].Value)
	toBanned := e.rosterUpdatesOnUserTopic(memberID, chatID)
	require.Len(t, toBanned, 1)
	require.Equal(t, memberID.Value, toBanned[0].GetMemberLeft().GetUserId().GetValue())
	require.NoError(t, protoutil.ProtoEqualError(summary, toBanned[0].RosterSummary))
	require.Empty(t, e.rosterUpdatesOnUserTopic(otherID, chatID))

	// Their mute is cleared as a leave clears one.
	states := e.waitForViewerStateUpdates(memberID, chatID, 2)
	require.Nil(t, states[1].GetSettings().GetMute())
	require.Nil(t, states[1].GetPermissions().GetModeration())
	viewerStates, err := s.GetViewerStates(e.ctx, memberID, []*commonpb.ChatId{chatID})
	require.NoError(t, err)
	require.Nil(t, viewerStates[string(chatID.Value)].Mute)
	require.True(t, viewerStates[string(chatID.Value)].IsBanned())

	// A repeated ban is the no-op: nothing more is announced.
	require.Equal(t, chatpb.BanUserResponse_OK, e.mustBanUser(e.keys, chatID, memberID))
	time.Sleep(100 * time.Millisecond)
	require.Len(t, e.rosterUpdatesOnUserTopic(memberID, chatID), 1)

	// They cannot rejoin until unbanned, and the unban does not readmit them.
	require.Equal(t, chatpb.JoinChatResponse_BANNED, e.mustJoinChat(memberKeys, chatID).Result)
	require.Equal(t, chatpb.UnbanUserResponse_OK, e.mustUnbanUser(e.keys, chatID, memberID))
	isMember, err = s.IsMember(e.ctx, chatID, memberID)
	require.NoError(t, err)
	require.False(t, isMember)
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(memberKeys, chatID).Result)
}

// testServer_Moderation_BanNonMember pins a ban of a user who has never
// joined: nothing is announced, and they cannot join until unbanned.
func testServer_Moderation_BanNonMember(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	outsiderID, outsiderKeys := e.addUser()
	chatID := e.putOwnedGroup("Group", nil)

	require.Equal(t, chatpb.BanUserResponse_OK, e.mustBanUser(e.keys, chatID, outsiderID))
	e.requireNoRosterUpdates(outsiderID, chatID)
	require.Empty(t, e.lobbyUpdatesOnUserTopic(e.userID, chatID))

	require.Equal(t, chatpb.JoinChatResponse_BANNED, e.mustJoinChat(outsiderKeys, chatID).Result)
	require.Equal(t, chatpb.UnbanUserResponse_OK, e.mustUnbanUser(e.keys, chatID, outsiderID))
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(outsiderKeys, chatID).Result)
}

// testServer_Moderation_BanPrivateGroup pins a ban in a private group: a
// member loses their key envelope with their membership, a user waiting in
// the lobby loses their entry (told to the creator as any departure from the
// lobby is), and a banned user can neither enter the lobby nor be admitted
// until unbanned.
func testServer_Moderation_BanPrivateGroup(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	memberID, _ := e.addUser()
	waitingID, waitingKeys := e.addBoundUser("Waiting")
	chatID := e.putKeyedPrivateGroup("Private", memberID)
	_, err := s.SetKeyEnvelope(e.ctx, chatID, memberID, chat.KeyEnvelopeFromProto(keyEnvelopeProto(2), e.userID))
	require.NoError(t, err)

	// A member: their envelope goes with them.
	require.Equal(t, chatpb.BanUserResponse_OK, e.mustBanUser(e.keys, chatID, memberID))
	e.waitForRosterUpdates(memberID, chatID, 1)
	require.Equal(t, memberID.Value, e.rosterUpdatesOnUserTopic(memberID, chatID)[0].GetMemberLeft().GetUserId().GetValue())
	_, err = s.GetKeyEnvelope(e.ctx, chatID, memberID)
	require.ErrorIs(t, err, chat.ErrKeyEnvelopeNotFound)
	// The creator's is untouched.
	_, err = s.GetKeyEnvelope(e.ctx, chatID, e.userID)
	require.NoError(t, err)

	// A user in the lobby: their entry goes, and the creator hears of it as
	// of any departure from the lobby.
	require.Equal(t, chatpb.EnterLobbyResponse_OK, e.enterLobby(waitingKeys, chatID).Result)
	e.waitForLobbyUpdates(e.userID, chatID, 1)
	require.Equal(t, chatpb.BanUserResponse_OK, e.mustBanUser(e.keys, chatID, waitingID))
	updates := e.waitForLobbyUpdates(e.userID, chatID, 2)
	require.Equal(t, waitingID.Value, updates[1].GetMemberLeft().GetUserId().GetValue())
	entries, err := s.GetLobbyEntries(e.ctx, waitingID, []*commonpb.ChatId{chatID})
	require.NoError(t, err)
	require.Empty(t, entries)
	e.requireNoLobbyUpdatesOnChatTopic(chatID)

	// Banned: no entry, and so no admission.
	require.Equal(t, chatpb.EnterLobbyResponse_BANNED, e.enterLobby(waitingKeys, chatID).Result)
	require.Equal(t, chatpb.AdmitLobbyMemberResponse_NOT_IN_LOBBY, e.admitLobbyMember(e.keys, chatID, waitingID, keyEnvelopeProto(3)).Result)

	// Unbanned, they enter again like anyone, and may be admitted.
	require.Equal(t, chatpb.UnbanUserResponse_OK, e.mustUnbanUser(e.keys, chatID, waitingID))
	require.Equal(t, chatpb.EnterLobbyResponse_OK, e.enterLobby(waitingKeys, chatID).Result)
	require.Equal(t, chatpb.AdmitLobbyMemberResponse_OK, e.admitLobbyMember(e.keys, chatID, waitingID, keyEnvelopeProto(3)).Result)
}

// testServer_Moderation_BannedReads pins what a banned non-member reads of a
// public group: no full read, whatever its rules say of them, and the
// redacted read anyone has.
func testServer_Moderation_BannedReads(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	outsiderID, outsiderKeys := e.addUser()
	chatID := chat.MustGenerateGroupChatID()
	require.NoError(t, s.PutChat(e.ctx, &chat.Chat{
		ID:            chatID,
		Type:          chatpb.ChatType_GROUP,
		Members:       []*commonpb.UserId{e.userID},
		Title:         "Open",
		CreatorID:     e.userID,
		LastActivity:  at(1),
		LastMessageID: &messagingpb.MessageId{Value: 7},
	}))
	key := string(chatID.Value)
	e.messaging.lastMessages[key] = textMessage(7, e.userID, "hello")
	e.messaging.latestEventSeqs[key] = 7

	// An open group: read in full by any non-member.
	resp := e.getChat(outsiderKeys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.False(t, resp.Metadata.GetLastMessage().GetRedacted())
	require.NotNil(t, resp.Metadata.GetLastMessage())

	require.Equal(t, chatpb.BanUserResponse_OK, e.mustBanUser(e.keys, chatID, outsiderID))

	// Banned: the record alone under FULL, and redacted where redaction is
	// allowed.
	resp = e.getChat(outsiderKeys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Nil(t, resp.Metadata.LastMessage)
	for _, mode := range []messagingpb.ViewMode{messagingpb.ViewMode_FULL_OR_REDACTED, messagingpb.ViewMode_REDACTED} {
		resp = e.getChatWithMode(outsiderKeys, chatID, mode)
		require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
		require.True(t, resp.Metadata.GetLastMessage().GetRedacted(), mode)
	}

	// Unbanned, they read in full again at once.
	require.Equal(t, chatpb.UnbanUserResponse_OK, e.mustUnbanUser(e.keys, chatID, outsiderID))
	resp = e.getChat(outsiderKeys, chatID)
	require.False(t, resp.Metadata.GetLastMessage().GetRedacted())
	require.NotNil(t, resp.Metadata.GetLastMessage())
}

// testServer_Moderation_GetRestrictedUsers pins the listing's content: every
// user banned or silenced, most recently restricted first, each with their
// profile, their silence while in force and the time of their ban, and no one
// whose only restriction is a silence that has lapsed.
func testServer_Moderation_GetRestrictedUsers(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	silenced, banned, both, lapsed := model.MustGenerateUserID(), model.MustGenerateUserID(), model.MustGenerateUserID(), model.MustGenerateUserID()
	e.profiles.usernames[string(silenced.Value)] = "silenced"
	chatID := e.putOwnedGroup("Group", nil, silenced, banned)

	until := time.Now().Add(time.Hour).Truncate(time.Second)
	_, _, err := s.SetSilence(e.ctx, chatID, silenced, chat.Silence{Until: until}, at(10))
	require.NoError(t, err)
	_, err = s.BanUser(e.ctx, chatID, banned, at(20), false)
	require.NoError(t, err)
	_, _, err = s.SetSilence(e.ctx, chatID, both, chat.Silence{Forever: true}, at(30))
	require.NoError(t, err)
	_, err = s.BanUser(e.ctx, chatID, both, at(40), false)
	require.NoError(t, err)
	_, _, err = s.SetSilence(e.ctx, chatID, lapsed, chat.Silence{Until: time.Now().Add(-time.Hour).Truncate(time.Second)}, at(50))
	require.NoError(t, err)
	e.waitForRestrictedListing(chatID, 4)

	resp := e.mustGetRestrictedUsers(e.keys, chatID, nil)
	require.Equal(t, chatpb.GetRestrictedUsersResponse_OK, resp.Result)
	require.False(t, resp.HasMore)
	require.Nil(t, resp.PagingToken)
	require.Equal(t, [][]byte{both.Value, banned.Value, silenced.Value}, restrictedUserIDs(resp.Users))

	require.NoError(t, protoutil.ProtoEqualError(silenceForever(), resp.Users[0].Silence))
	require.True(t, resp.Users[0].GetBan().GetBannedAt().AsTime().Equal(at(40)))
	require.Nil(t, resp.Users[1].Silence)
	require.True(t, resp.Users[1].GetBan().GetBannedAt().AsTime().Equal(at(20)))
	require.NoError(t, protoutil.ProtoEqualError(silenceUntil(until), resp.Users[2].Silence))
	require.Nil(t, resp.Users[2].Ban)
	require.Equal(t, "silenced", resp.Users[2].UserProfile.GetUsername().GetValue())
	for _, u := range resp.Users {
		require.Equal(t, u.UserId.Value, u.UserProfile.GetUserId().GetValue())
	}

	// Lifted restrictions leave the listing.
	require.Equal(t, chatpb.UnbanUserResponse_OK, e.mustUnbanUser(e.keys, chatID, banned))
	require.Equal(t, chatpb.UnsilenceUserResponse_OK, e.mustUnsilenceUser(e.keys, chatID, lapsed))
	e.waitForRestrictedListing(chatID, 2)
	resp = e.mustGetRestrictedUsers(e.keys, chatID, nil)
	require.Equal(t, [][]byte{both.Value, silenced.Value}, restrictedUserIDs(resp.Users))
}

// testServer_Moderation_GetRestrictedUsers_Paging pins the listing's paging:
// exact pages with an exact has_more, lapsed silences skipped without
// shrinking a page, a page that runs out of reads ending early with a token
// past what it skipped, and a token bound to its chat.
func testServer_Moderation_GetRestrictedUsers_Paging(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	chatID := e.putOwnedGroup("Group", nil)
	other := e.putOwnedGroup("Other", nil)

	// Five active silences at 1..5, and a lapsed one at 6, listed first.
	var active []*commonpb.UserId
	for i := int64(1); i <= 5; i++ {
		userID := model.MustGenerateUserID()
		_, _, err := s.SetSilence(e.ctx, chatID, userID, chat.Silence{Forever: true}, at(i))
		require.NoError(t, err)
		active = append(active, userID)
	}
	_, _, err := s.SetSilence(e.ctx, chatID, model.MustGenerateUserID(), chat.Silence{Until: time.Now().Add(-time.Hour).Truncate(time.Second)}, at(6))
	require.NoError(t, err)
	e.waitForRestrictedListing(chatID, 6)

	var pages [][][]byte
	var token *commonpb.PagingToken
	for {
		resp := e.mustGetRestrictedUsers(e.keys, chatID, &commonpb.QueryOptions{PageSize: 2, PagingToken: token})
		require.Equal(t, chatpb.GetRestrictedUsersResponse_OK, resp.Result)
		pages = append(pages, restrictedUserIDs(resp.Users))
		if !resp.HasMore {
			require.Nil(t, resp.PagingToken)
			break
		}
		token = resp.PagingToken
	}
	require.Equal(t, [][][]byte{
		{active[4].Value, active[3].Value},
		{active[2].Value, active[1].Value},
		{active[0].Value},
	}, pages)

	// A token is refused with another chat.
	first := e.mustGetRestrictedUsers(e.keys, chatID, &commonpb.QueryOptions{PageSize: 2})
	require.True(t, first.HasMore)
	_, err = e.getRestrictedUsers(e.keys, other, &commonpb.QueryOptions{PageSize: 2, PagingToken: first.PagingToken})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// A run of lapsed silences longer than a page's reads can cover ends the
	// page early, empty, with a token past them; the next page resumes there.
	crowded := e.putOwnedGroup("Crowded", nil)
	survivor := model.MustGenerateUserID()
	_, _, err = s.SetSilence(e.ctx, crowded, survivor, chat.Silence{Forever: true}, at(1))
	require.NoError(t, err)
	for i := int64(2); i <= 9; i++ {
		_, _, err := s.SetSilence(e.ctx, crowded, model.MustGenerateUserID(), chat.Silence{Until: time.Now().Add(-time.Hour).Truncate(time.Second)}, at(i))
		require.NoError(t, err)
	}
	e.waitForRestrictedListing(crowded, 9)
	resp := e.mustGetRestrictedUsers(e.keys, crowded, &commonpb.QueryOptions{PageSize: 1})
	require.Empty(t, resp.Users)
	require.True(t, resp.HasMore)
	resp = e.mustGetRestrictedUsers(e.keys, crowded, &commonpb.QueryOptions{PageSize: 1, PagingToken: resp.PagingToken})
	require.Equal(t, [][]byte{survivor.Value}, restrictedUserIDs(resp.Users))
	require.False(t, resp.HasMore)
}

// testServer_Moderation_MentionSuggestions pins that a banned user is not
// suggested while a silenced one still is.
func testServer_Moderation_MentionSuggestions(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	silenced, banned := model.MustGenerateUserID(), model.MustGenerateUserID()
	e.profiles.usernames[string(silenced.Value)] = "silenced"
	e.profiles.usernames[string(banned.Value)] = "banned"
	chatID := e.putOwnedGroup("Group", nil, silenced, banned)
	e.recordSend(chatID, silenced, at(10))
	e.recordSend(chatID, banned, at(20))

	require.Equal(t, chatpb.SilenceUserResponse_OK, e.mustSilenceUser(e.keys, chatID, silenced, silenceForever()))
	require.Equal(t, chatpb.BanUserResponse_OK, e.mustBanUser(e.keys, chatID, banned))

	// The ban is found with an eventually consistent read.
	require.Eventually(t, func() bool {
		resp := e.getMentionSuggestions(e.keys, chatID)
		require.Equal(t, chatpb.GetMentionSuggestionsResponse_OK, resp.Result)
		got := suggestedUserIDs(resp.Suggestions)
		return len(got) == 1 && string(got[0]) == string(silenced.Value)
	}, 5*time.Second, 10*time.Millisecond)
}

// testServer_Moderation_MalformedTarget pins that a target which is not a
// well-formed user ID is refused as malformed by every write, before
// anything is read or recorded.
func testServer_Moderation_MalformedTarget(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	chatID := e.putOwnedGroup("Group", nil)

	for _, value := range [][]byte{{1}, make([]byte, 32)} {
		target := &commonpb.UserId{Value: value}

		_, err := e.silenceUser(e.keys, chatID, target, silenceForever())
		require.Equal(t, codes.InvalidArgument, status.Code(err))

		unsilence := &chatpb.UnsilenceUserRequest{ChatId: chatID, UserId: target}
		require.NoError(t, e.keys.Auth(unsilence, &unsilence.Auth))
		_, err = e.client.UnsilenceUser(e.ctx, unsilence)
		require.Equal(t, codes.InvalidArgument, status.Code(err))

		ban := &chatpb.BanUserRequest{ChatId: chatID, UserId: target}
		require.NoError(t, e.keys.Auth(ban, &ban.Auth))
		_, err = e.client.BanUser(e.ctx, ban)
		require.Equal(t, codes.InvalidArgument, status.Code(err))

		unban := &chatpb.UnbanUserRequest{ChatId: chatID, UserId: target}
		require.NoError(t, e.keys.Auth(unban, &unban.Auth))
		_, err = e.client.UnbanUser(e.ctx, unban)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}

	resp := e.mustGetRestrictedUsers(e.keys, chatID, nil)
	require.Empty(t, resp.Users)
}

// testServer_Moderation_GetRestrictedUsers_UnknownUser pins that a restricted
// user the profile domain does not know is left out of the listing rather
// than failing it, so the creator pages past them.
func testServer_Moderation_GetRestrictedUsers_UnknownUser(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	chatID := e.putOwnedGroup("Group", nil)

	known, unknown, last := model.MustGenerateUserID(), model.MustGenerateUserID(), model.MustGenerateUserID()
	e.profiles.unknown[string(unknown.Value)] = struct{}{}
	for i, userID := range []*commonpb.UserId{known, unknown, last} {
		_, err := s.BanUser(e.ctx, chatID, userID, at(int64(10*(i+1))), false)
		require.NoError(t, err)
	}
	e.waitForRestrictedListing(chatID, 3)

	resp := e.mustGetRestrictedUsers(e.keys, chatID, nil)
	require.Equal(t, chatpb.GetRestrictedUsersResponse_OK, resp.Result)
	require.Equal(t, [][]byte{last.Value, known.Value}, restrictedUserIDs(resp.Users))

	// Paged one at a time, the walk passes the unknown user: the page that
	// read them comes back empty, with a token past them.
	var walked [][]byte
	opts := &commonpb.QueryOptions{PageSize: 1}
	for {
		resp = e.mustGetRestrictedUsers(e.keys, chatID, opts)
		require.Equal(t, chatpb.GetRestrictedUsersResponse_OK, resp.Result)
		walked = append(walked, restrictedUserIDs(resp.Users)...)
		if !resp.HasMore {
			break
		}
		opts = &commonpb.QueryOptions{PageSize: 1, PagingToken: resp.PagingToken}
	}
	require.Equal(t, [][]byte{last.Value, known.Value}, walked)
}

// newInviteToken is a fresh invite token, as a client derives one from a
// link's secret.
func newInviteToken() *chatpb.InviteToken {
	return &chatpb.InviteToken{Value: randomBytes(chat.InviteTokenSize)}
}

// signedInviteKeyEnvelope is an invite's copy of chatID's key for inviteID,
// signed by keys as chatpb.InviteKeyEnvelope specifies. The server cannot
// open the copy, so random bytes stand in for the encrypted key.
func signedInviteKeyEnvelope(keys model.KeyPair, chatID *commonpb.ChatId, inviteID *chatpb.InviteId) *chatpb.InviteKeyEnvelope {
	envelope := &chatpb.InviteKeyEnvelope{
		Scheme:     chatpb.InviteKeyEnvelope_XCHACHA20POLY1305,
		Nonce:      randomBytes(24),
		Ciphertext: randomBytes(48),
	}
	var m []byte
	m = append(m, "flipcash-group-invite-sig-v1"...)
	m = append(m, chatID.Value...)
	m = append(m, inviteID.Value...)
	m = append(m, envelope.Nonce...)
	m = append(m, envelope.Ciphertext...)
	envelope.Signature = ed25519.Sign(keys.Private(), m)
	return envelope
}

// createInviteRequest is a well-formed CreateInvite for chatID with token,
// its copy signed by keys: unlimited and never expiring.
func createInviteRequest(keys model.KeyPair, chatID *commonpb.ChatId, token *chatpb.InviteToken) *chatpb.CreateInviteRequest {
	return &chatpb.CreateInviteRequest{
		ChatId:         chatID,
		InviteToken:    token,
		KeyEnvelope:    signedInviteKeyEnvelope(keys, chatID, chat.MustDeriveInviteID(token)),
		SecretEnvelope: inviteSecret(1),
	}
}

func (e *serverEnv) sendCreateInvite(keys model.KeyPair, req *chatpb.CreateInviteRequest) (*chatpb.CreateInviteResponse, error) {
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.CreateInvite(e.ctx, req)
}

func (e *serverEnv) createInvite(keys model.KeyPair, req *chatpb.CreateInviteRequest) *chatpb.CreateInviteResponse {
	resp, err := e.sendCreateInvite(keys, req)
	require.NoError(e.t, err)
	return resp
}

// mustCreateInvite creates an unlimited, never-expiring invite to chatID as
// keys, requiring OK, and returns its token and the invite.
func (e *serverEnv) mustCreateInvite(keys model.KeyPair, chatID *commonpb.ChatId) (*chatpb.InviteToken, *chatpb.Invite) {
	token := newInviteToken()
	resp := e.createInvite(keys, createInviteRequest(keys, chatID, token))
	require.Equal(e.t, chatpb.CreateInviteResponse_OK, resp.Result)
	return token, resp.Invite
}

func (e *serverEnv) getInvite(keys model.KeyPair, token *chatpb.InviteToken) *chatpb.GetInviteResponse {
	req := &chatpb.GetInviteRequest{InviteToken: token}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.GetInvite(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

// getPublicInvite previews an invite with no auth at all, the token alone
// (see chat.Server.GetInvite).
func (e *serverEnv) getPublicInvite(token *chatpb.InviteToken) *chatpb.GetInviteResponse {
	resp, err := e.client.GetInvite(e.ctx, &chatpb.GetInviteRequest{InviteToken: token})
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) getChatInvites(keys model.KeyPair, chatID *commonpb.ChatId, opts *commonpb.QueryOptions) (*chatpb.GetChatInvitesResponse, error) {
	req := &chatpb.GetChatInvitesRequest{ChatId: chatID, QueryOptions: opts}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.GetChatInvites(e.ctx, req)
}

func (e *serverEnv) mustGetChatInvites(keys model.KeyPair, chatID *commonpb.ChatId, opts *commonpb.QueryOptions) *chatpb.GetChatInvitesResponse {
	resp, err := e.getChatInvites(keys, chatID, opts)
	require.NoError(e.t, err)
	return resp
}

// waitForChatInvites reads chatID's invites whole as keys until they are
// exactly want, in order, with want's statuses and use counts: the listing
// is read off an index that may trail writes (see
// chat.Store.GetChatInvitesPage).
func (e *serverEnv) waitForChatInvites(keys model.KeyPair, chatID *commonpb.ChatId, want ...*chatpb.Invite) []*chatpb.Invite {
	var got []*chatpb.Invite
	require.Eventually(e.t, func() bool {
		resp := e.mustGetChatInvites(keys, chatID, nil)
		require.Equal(e.t, chatpb.GetChatInvitesResponse_OK, resp.Result)
		got = resp.Invites
		if len(got) != len(want) {
			return false
		}
		for i := range want {
			if !bytes.Equal(want[i].InviteId.Value, got[i].InviteId.Value) || want[i].Status != got[i].Status || want[i].UseCount != got[i].UseCount {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)
	return got
}

func (e *serverEnv) revokeInvite(keys model.KeyPair, chatID *commonpb.ChatId, inviteID *chatpb.InviteId) *chatpb.RevokeInviteResponse {
	req := &chatpb.RevokeInviteRequest{ChatId: chatID, InviteId: inviteID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.RevokeInvite(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

// withStatus is a copy of invite with status, for an invite expected to have
// moved on since it was returned.
func withStatus(invite *chatpb.Invite, status chatpb.Invite_Status) *chatpb.Invite {
	clone := proto.Clone(invite).(*chatpb.Invite)
	clone.Status = status
	return clone
}

// privateGroupViewerState is viewerState for a private group, whose creator,
// while a member, may also invite (see chat.Chat.PermissionsFor).
func privateGroupViewerState(version uint64, mute *chatpb.MuteState, isCreator bool) *chatpb.ViewerState {
	state := viewerState(version, nil, isCreator)
	state.Settings.Mute = mute
	state.Permissions.CanInvite = isCreator
	return state
}

// testServer_Invite_Create pins a created invite: what CreateInvite returns
// and stores, limited or not, expiring or not.
func testServer_Invite_Create(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.bindKey(e.userID, e.keys)
	chatID := e.putKeyedPrivateGroup("Sunday Hikers")

	// Unlimited and never expiring.
	before := time.Now()
	token := newInviteToken()
	req := createInviteRequest(e.keys, chatID, token)
	resp := e.createInvite(e.keys, req)
	require.Equal(t, chatpb.CreateInviteResponse_OK, resp.Result)
	invite := resp.Invite
	require.NoError(t, invite.Validate())
	require.Equal(t, chat.MustDeriveInviteID(token).Value, invite.InviteId.Value)
	require.Equal(t, chatID.Value, invite.ChatId.Value)
	require.Equal(t, e.userID.Value, invite.CreatedBy.Value)
	require.Zero(t, invite.MaxUses)
	require.Zero(t, invite.UseCount)
	require.False(t, invite.CreatedAt.AsTime().Before(before))
	require.False(t, invite.CreatedAt.AsTime().After(time.Now()))
	require.Nil(t, invite.ExpiresAt)
	require.Equal(t, chatpb.Invite_ACTIVE, invite.Status)
	require.True(t, proto.Equal(req.SecretEnvelope, invite.SecretEnvelope))

	// Limited and expiring.
	expiresAt := time.Now().Add(24 * time.Hour)
	token = newInviteToken()
	req = createInviteRequest(e.keys, chatID, token)
	req.MaxUses = 5
	req.ExpiresAt = timestamppb.New(expiresAt)
	req.SecretEnvelope = inviteSecret(9)
	resp = e.createInvite(e.keys, req)
	require.Equal(t, chatpb.CreateInviteResponse_OK, resp.Result)
	limited := resp.Invite
	require.NoError(t, limited.Validate())
	require.Equal(t, chat.MustDeriveInviteID(token).Value, limited.InviteId.Value)
	require.EqualValues(t, 5, limited.MaxUses)
	require.Zero(t, limited.UseCount)
	require.True(t, expiresAt.Equal(limited.ExpiresAt.AsTime()))
	require.Equal(t, chatpb.Invite_ACTIVE, limited.Status)
	require.True(t, proto.Equal(inviteSecret(9), limited.SecretEnvelope))

	// Both are stored as returned, newest first.
	listed := e.waitForChatInvites(e.keys, chatID, limited, invite)
	require.NoError(t, protoutil.ProtoEqualError(limited, listed[0]))
	require.NoError(t, protoutil.ProtoEqualError(invite, listed[1]))

	// Nothing is published.
	time.Sleep(100 * time.Millisecond)
	for _, ev := range e.chatObserver.GetEvents(func(k *commonpb.ChatId) bool { return bytes.Equal(k.Value, chatID.Value) }) {
		require.Fail(t, "unexpected chat event", "%v", ev.Event)
	}
	for _, ev := range e.userObserver.GetEvents(func(k *commonpb.UserId) bool { return bytes.Equal(k.Value, e.userID.Value) }) {
		require.Fail(t, "unexpected user event", "%v", ev.Event)
	}
}

// testServer_Invite_Gates pins who may create, list and revoke invites: a
// private group's creator while a member, and no one else of anything else.
func testServer_Invite_Gates(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.bindKey(e.userID, e.keys)
	memberID, memberKeys := e.addBoundUser("Member")
	_, strangerKeys := e.addBoundUser("Stranger")

	keyed := e.putKeyedPrivateGroup("Keyed", memberID)
	_, invite := e.mustCreateInvite(e.keys, keyed)

	requireAllResults := func(keys model.KeyPair, chatID *commonpb.ChatId, create chatpb.CreateInviteResponse_Result, list chatpb.GetChatInvitesResponse_Result, revoke chatpb.RevokeInviteResponse_Result) {
		t.Helper()
		require.Equal(t, create, e.createInvite(keys, createInviteRequest(keys, chatID, newInviteToken())).Result)
		listed := e.mustGetChatInvites(keys, chatID, nil)
		require.Equal(t, list, listed.Result)
		require.Empty(t, listed.Invites)
		require.Equal(t, revoke, e.revokeInvite(keys, chatID, invite.InviteId).Result)
	}
	requireAllDenied := func(keys model.KeyPair, chatID *commonpb.ChatId) {
		t.Helper()
		requireAllResults(keys, chatID, chatpb.CreateInviteResponse_DENIED, chatpb.GetChatInvitesResponse_DENIED, chatpb.RevokeInviteResponse_DENIED)
	}

	// A DM, for its members and anyone; a public group, its creator included.
	dm := e.putDM(at(1))
	requireAllDenied(e.keys, dm)
	requireAllDenied(strangerKeys, dm)
	public := e.putGroup("Public", at(1), memberID)
	requireAllDenied(e.keys, public)
	requireAllDenied(memberKeys, public)

	// A group that does not exist.
	missing := chat.MustGenerateGroupChatID()
	requireAllResults(e.keys, missing, chatpb.CreateInviteResponse_NOT_FOUND, chatpb.GetChatInvitesResponse_NOT_FOUND, chatpb.RevokeInviteResponse_NOT_FOUND)

	// A member who is not the creator, and a stranger.
	requireAllDenied(memberKeys, keyed)
	requireAllDenied(strangerKeys, keyed)

	// A keyless private group: its creator lists and revokes (there is
	// nothing to revoke), but creates nothing.
	keyless := e.putPrivateGroup("Keyless", memberID)
	requireAllResults(e.keys, keyless, chatpb.CreateInviteResponse_DENIED, chatpb.GetChatInvitesResponse_OK, chatpb.RevokeInviteResponse_NOT_FOUND)

	// A creator who has left does nothing until they rejoin, and their invite
	// is still there when they do.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, keyed).Result)
	requireAllDenied(e.keys, keyed)
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(e.keys, keyed).Result)
	e.waitForChatInvites(e.keys, keyed, invite)
	_, again := e.mustCreateInvite(e.keys, keyed)
	e.waitForChatInvites(e.keys, keyed, again, invite)
	require.Equal(t, chatpb.RevokeInviteResponse_OK, e.revokeInvite(e.keys, keyed, invite.InviteId).Result)

	// A caller with no account key bound cannot have a copy checked: every
	// registered user has one, so its absence is an internal failure.
	noKeyID, noKeyKeys := e.addUser()
	noKeyGroup := chat.MustGenerateGroupChatID()
	require.NoError(t, e.store.PutChat(e.ctx, &chat.Chat{
		ID:           noKeyGroup,
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{noKeyID},
		Title:        "No Key",
		IsPrivate:    true,
		CreatorID:    noKeyID,
		LastActivity: at(1),
	}))
	_, err := e.store.SetKeyEnvelope(e.ctx, noKeyGroup, noKeyID, chat.KeyEnvelopeFromProto(keyEnvelopeProto(1), noKeyID))
	require.NoError(t, err)
	_, err = e.sendCreateInvite(noKeyKeys, createInviteRequest(noKeyKeys, noKeyGroup, newInviteToken()))
	require.Equal(t, codes.Internal, status.Code(err))
}

// testServer_Invite_ExpiresAt pins the bounds on an invite's expiry: after
// now, and at most chat.MaxInviteDuration away.
func testServer_Invite_ExpiresAt(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.bindKey(e.userID, e.keys)
	chatID := e.putKeyedPrivateGroup("Sunday Hikers")

	create := func(expiresAt time.Time) *chatpb.CreateInviteResponse {
		t.Helper()
		req := createInviteRequest(e.keys, chatID, newInviteToken())
		req.ExpiresAt = timestamppb.New(expiresAt)
		return e.createInvite(e.keys, req)
	}

	for name, expiresAt := range map[string]time.Time{
		"past":   time.Now().Add(-time.Hour),
		"now":    time.Now(),
		"beyond": time.Now().Add(chat.MaxInviteDuration + time.Minute),
	} {
		resp := create(expiresAt)
		require.Equal(t, chatpb.CreateInviteResponse_INVALID_EXPIRES_AT, resp.Result, name)
		require.Nil(t, resp.Invite, name)
	}

	// The bound itself, measured from before the server's own now, and just
	// inside it.
	for _, expiresAt := range []time.Time{
		time.Now().Add(chat.MaxInviteDuration),
		time.Now().Add(chat.MaxInviteDuration - time.Minute),
		time.Now().Add(time.Minute),
	} {
		resp := create(expiresAt)
		require.Equal(t, chatpb.CreateInviteResponse_OK, resp.Result)
		require.True(t, expiresAt.Equal(resp.Invite.ExpiresAt.AsTime()))
	}
}

// testServer_Invite_Signature pins that a copy is accepted only with a
// signature by the caller's account key over this chat, this invite's ID and
// the copy as sent.
func testServer_Invite_Signature(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.bindKey(e.userID, e.keys)
	chatID := e.putKeyedPrivateGroup("Sunday Hikers")
	other := e.putKeyedPrivateGroup("Other")

	for name, mutate := range map[string]func(req *chatpb.CreateInviteRequest){
		"another key": func(req *chatpb.CreateInviteRequest) {
			req.KeyEnvelope = signedInviteKeyEnvelope(model.MustGenerateKeyPair(), chatID, chat.MustDeriveInviteID(req.InviteToken))
		},
		"another chat": func(req *chatpb.CreateInviteRequest) {
			req.KeyEnvelope = signedInviteKeyEnvelope(e.keys, other, chat.MustDeriveInviteID(req.InviteToken))
		},
		"another invite": func(req *chatpb.CreateInviteRequest) {
			req.KeyEnvelope = signedInviteKeyEnvelope(e.keys, chatID, chat.MustDeriveInviteID(newInviteToken()))
		},
		"tampered ciphertext": func(req *chatpb.CreateInviteRequest) {
			req.KeyEnvelope.Ciphertext[0] ^= 1
		},
		"tampered nonce": func(req *chatpb.CreateInviteRequest) {
			req.KeyEnvelope.Nonce[23] ^= 1
		},
	} {
		req := createInviteRequest(e.keys, chatID, newInviteToken())
		mutate(req)
		resp := e.createInvite(e.keys, req)
		require.Equal(t, chatpb.CreateInviteResponse_INVALID_SIGNATURE, resp.Result, name)
		require.Nil(t, resp.Invite, name)
	}

	// Nothing was stored for any of them.
	time.Sleep(100 * time.Millisecond)
	require.Empty(t, e.mustGetChatInvites(e.keys, chatID, nil).Invites)
}

// testServer_Invite_Cap pins the cap on a chat's ACTIVE invites: a create
// past it is refused, a revoke frees a place, and a retry of an invite that
// exists is never refused at it.
func testServer_Invite_Cap(t *testing.T, s chat.Store) {
	e := newServerEnvWithConfig(t, s, serverConfig{maxActiveInvites: 2})
	e.bindKey(e.userID, e.keys)
	chatID := e.putKeyedPrivateGroup("Sunday Hikers")

	firstToken, first := e.mustCreateInvite(e.keys, chatID)
	_, second := e.mustCreateInvite(e.keys, chatID)
	refused := e.createInvite(e.keys, createInviteRequest(e.keys, chatID, newInviteToken()))
	require.Equal(t, chatpb.CreateInviteResponse_TOO_MANY_INVITES, refused.Result)
	require.Nil(t, refused.Invite)

	// Another chat's cap is its own.
	e.mustCreateInvite(e.keys, e.putKeyedPrivateGroup("Other"))

	// A retry at the cap is answered from the invite it made.
	retried := e.createInvite(e.keys, createInviteRequest(e.keys, chatID, firstToken))
	require.Equal(t, chatpb.CreateInviteResponse_OK, retried.Result)
	require.NoError(t, protoutil.ProtoEqualError(first, retried.Invite))

	// A revoke frees a place, once the count the cap reads catches up with
	// it (see chat.Store.CreateInvite); a refusal writes nothing, so the
	// create is simply asked again.
	require.Equal(t, chatpb.RevokeInviteResponse_OK, e.revokeInvite(e.keys, chatID, second.InviteId).Result)
	req := createInviteRequest(e.keys, chatID, newInviteToken())
	require.Eventually(t, func() bool {
		resp := e.createInvite(e.keys, req)
		if resp.Result == chatpb.CreateInviteResponse_TOO_MANY_INVITES {
			return false
		}
		require.Equal(t, chatpb.CreateInviteResponse_OK, resp.Result)
		return true
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, chatpb.CreateInviteResponse_TOO_MANY_INVITES, e.createInvite(e.keys, createInviteRequest(e.keys, chatID, newInviteToken())).Result)
}

// testServer_Invite_Retry pins that a token names one invite: a retry by its
// creator for its chat gets the invite the first attempt made, whatever else
// it carries and whatever the invite's status, and anyone else, or any other
// chat, is DENIED.
func testServer_Invite_Retry(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.bindKey(e.userID, e.keys)
	chatID := e.putKeyedPrivateGroup("Sunday Hikers")
	other := e.putKeyedPrivateGroup("Other")
	otherCreatorID, otherCreatorKeys := e.addBoundUser("Other Creator")
	theirs := chat.MustGenerateGroupChatID()
	require.NoError(t, e.store.PutChat(e.ctx, &chat.Chat{
		ID:           theirs,
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{otherCreatorID},
		Title:        "Theirs",
		IsPrivate:    true,
		CreatorID:    otherCreatorID,
		LastActivity: at(1),
	}))
	_, err := e.store.SetKeyEnvelope(e.ctx, theirs, otherCreatorID, chat.KeyEnvelopeFromProto(keyEnvelopeProto(1), otherCreatorID))
	require.NoError(t, err)

	token, first := e.mustCreateInvite(e.keys, chatID)

	// The same token with every other field changed.
	changed := createInviteRequest(e.keys, chatID, token)
	changed.MaxUses = 7
	changed.ExpiresAt = timestamppb.New(time.Now().Add(time.Hour))
	changed.SecretEnvelope = inviteSecret(5)
	retried := e.createInvite(e.keys, changed)
	require.Equal(t, chatpb.CreateInviteResponse_OK, retried.Result)
	require.NoError(t, protoutil.ProtoEqualError(first, retried.Invite))

	// Even one whose copy would not verify: the lookup comes first.
	unsigned := createInviteRequest(e.keys, chatID, token)
	unsigned.KeyEnvelope.Ciphertext[0] ^= 1
	retried = e.createInvite(e.keys, unsigned)
	require.Equal(t, chatpb.CreateInviteResponse_OK, retried.Result)
	require.NoError(t, protoutil.ProtoEqualError(first, retried.Invite))

	// The same token for another chat, by its creator or anyone else's.
	require.Equal(t, chatpb.CreateInviteResponse_DENIED, e.createInvite(e.keys, createInviteRequest(e.keys, other, token)).Result)
	require.Equal(t, chatpb.CreateInviteResponse_DENIED, e.createInvite(otherCreatorKeys, createInviteRequest(otherCreatorKeys, theirs, token)).Result)

	// The same token by another user for the same chat.
	require.Equal(t, chatpb.CreateInviteResponse_DENIED, e.createInvite(otherCreatorKeys, createInviteRequest(otherCreatorKeys, chatID, token)).Result)

	// A revoked invite is still the one a retry gets, now REVOKED.
	require.Equal(t, chatpb.RevokeInviteResponse_OK, e.revokeInvite(e.keys, chatID, first.InviteId).Result)
	retried = e.createInvite(e.keys, createInviteRequest(e.keys, chatID, token))
	require.Equal(t, chatpb.CreateInviteResponse_OK, retried.Result)
	require.NoError(t, protoutil.ProtoEqualError(withStatus(first, chatpb.Invite_REVOKED), retried.Invite))

	// Even once its creator has left the chat.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, chatID).Result)
	retried = e.createInvite(e.keys, createInviteRequest(e.keys, chatID, token))
	require.Equal(t, chatpb.CreateInviteResponse_OK, retried.Result)
	require.NoError(t, protoutil.ProtoEqualError(withStatus(first, chatpb.Invite_REVOKED), retried.Invite))
}

// testServer_Invite_Get pins the preview of an invite: the chat as the caller
// sees it with GetChat, the invite's status and nothing else of it, and
// BANNED alone for a user banned from the chat.
func testServer_Invite_Get(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.bindKey(e.userID, e.keys)
	_, strangerKeys := e.addBoundUser("Stranger")
	bannedID, bannedKeys := e.addBoundUser("Banned")
	joinerID, _ := e.addBoundUser("Joiner")
	chatID := e.putKeyedPrivateGroup("Sunday Hikers")

	// No invite has the ID of a token never used.
	unknown := e.getInvite(strangerKeys, newInviteToken())
	require.Equal(t, chatpb.GetInviteResponse_NOT_FOUND, unknown.Result)
	require.Nil(t, unknown.Chat)

	token, invite := e.mustCreateInvite(e.keys, chatID)

	// The creator sees the chat as a member does.
	resp := e.getInvite(e.keys, token)
	require.Equal(t, chatpb.GetInviteResponse_OK, resp.Result)
	require.Equal(t, chatpb.Invite_ACTIVE, resp.Status)
	require.NoError(t, protoutil.ProtoEqualError(e.getChat(e.keys, chatID).Metadata, resp.Chat))
	require.NoError(t, protoutil.ProtoEqualError(privateGroupViewerState(0, nil, true), resp.Chat.ViewerState))
	require.Len(t, resp.Chat.Members, 1)

	// A stranger sees the record, not waiting in its lobby, and then waiting.
	resp = e.getInvite(strangerKeys, token)
	require.Equal(t, chatpb.GetInviteResponse_OK, resp.Result)
	require.Equal(t, chatpb.Invite_ACTIVE, resp.Status)
	require.NoError(t, protoutil.ProtoEqualError(e.getChat(strangerKeys, chatID).Metadata, resp.Chat))
	require.Equal(t, chatID.Value, resp.Chat.ChatId.Value)
	require.True(t, resp.Chat.IsPrivate)
	require.Empty(t, resp.Chat.Members)
	require.Nil(t, resp.Chat.ViewerState)
	require.False(t, resp.Chat.InLobby)
	require.Equal(t, chatpb.EnterLobbyResponse_OK, e.enterLobby(strangerKeys, chatID).Result)
	resp = e.getInvite(strangerKeys, token)
	require.Equal(t, chatpb.GetInviteResponse_OK, resp.Result)
	require.True(t, resp.Chat.InLobby)

	// Looking counts no use.
	e.waitForChatInvites(e.keys, chatID, invite)

	// A user banned from the chat gets BANNED and nothing else.
	require.Equal(t, chatpb.BanUserResponse_OK, e.mustBanUser(e.keys, chatID, bannedID))
	banned := e.getInvite(bannedKeys, token)
	require.Equal(t, chatpb.GetInviteResponse_BANNED, banned.Result)
	require.Nil(t, banned.Chat)
	require.Equal(t, chatpb.Invite_UNKNOWN, banned.Status)

	// An invite used up reads EXHAUSTED.
	limitedToken := newInviteToken()
	limitedReq := createInviteRequest(e.keys, chatID, limitedToken)
	limitedReq.MaxUses = 1
	limited := e.createInvite(e.keys, limitedReq)
	require.Equal(t, chatpb.CreateInviteResponse_OK, limited.Result)
	_, err := e.store.JoinWithInvite(e.ctx, chatID, joinerID, limited.Invite.InviteId, inviteSecret(3), time.Now())
	require.NoError(t, err)
	require.Equal(t, chatpb.Invite_EXHAUSTED, e.getInvite(strangerKeys, limitedToken).Status)

	// An invite past its expiry reads EXPIRED. Written through the store,
	// which takes an expiry in the past, so the read is deterministic.
	expiredToken := newInviteToken()
	expiresAt := time.Now().Add(-time.Minute)
	_, created, err := e.store.CreateInvite(e.ctx, chat.Invite{
		ID:             chat.MustDeriveInviteID(expiredToken),
		ChatID:         chatID,
		CreatedBy:      e.userID,
		CreatedAt:      time.Now().Add(-time.Hour),
		ExpiresAt:      &expiresAt,
		SecretEnvelope: inviteSecret(1),
	}, signedInviteKeyEnvelope(e.keys, chatID, chat.MustDeriveInviteID(expiredToken)), chat.MaxActiveInvites, time.Now())
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, chatpb.Invite_EXPIRED, e.getInvite(strangerKeys, expiredToken).Status)

	// A revoked invite reads REVOKED, to anyone.
	require.Equal(t, chatpb.RevokeInviteResponse_OK, e.revokeInvite(e.keys, chatID, invite.InviteId).Result)
	require.Equal(t, chatpb.Invite_REVOKED, e.getInvite(strangerKeys, token).Status)
	require.Equal(t, chatpb.Invite_REVOKED, e.getInvite(e.keys, token).Status)
}

// testServer_Invite_GetUnauthenticated pins the preview with no auth, as a web
// page that a link opens asks for it: the token alone gets the chat's public
// view and the invite's status, never BANNED, and counts no use. Auth that is
// set must still be valid.
func testServer_Invite_GetUnauthenticated(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.bindKey(e.userID, e.keys)
	bannedID, _ := e.addBoundUser("Banned")
	chatID := e.putKeyedPrivateGroup("Sunday Hikers")

	// No invite has the ID of a token never used.
	unknown := e.getPublicInvite(newInviteToken())
	require.Equal(t, chatpb.GetInviteResponse_NOT_FOUND, unknown.Result)
	require.Nil(t, unknown.Chat)

	token, invite := e.mustCreateInvite(e.keys, chatID)

	// The chat exactly as GetChat shows it with no auth: the record alone.
	resp := e.getPublicInvite(token)
	require.Equal(t, chatpb.GetInviteResponse_OK, resp.Result)
	require.Equal(t, chatpb.Invite_ACTIVE, resp.Status)
	require.NoError(t, protoutil.ProtoEqualError(e.getPublicChat(chatID, messagingpb.ViewMode_REDACTED).Metadata, resp.Chat))
	require.Equal(t, chatID.Value, resp.Chat.ChatId.Value)
	require.Equal(t, "Sunday Hikers", resp.Chat.Title)
	require.True(t, resp.Chat.IsPrivate)
	require.Empty(t, resp.Chat.Members)
	require.Nil(t, resp.Chat.ViewerState)
	require.Nil(t, resp.Chat.LastMessage)
	require.False(t, resp.Chat.InLobby)

	// Looking counts no use.
	e.waitForChatInvites(e.keys, chatID, invite)

	// A ban names a user, and a caller with no auth is no one.
	require.Equal(t, chatpb.BanUserResponse_OK, e.mustBanUser(e.keys, chatID, bannedID))
	resp = e.getPublicInvite(token)
	require.Equal(t, chatpb.GetInviteResponse_OK, resp.Result)
	require.Equal(t, chatpb.Invite_ACTIVE, resp.Status)

	// Auth that is set must be valid: a request altered after it was signed
	// is refused, not answered as if it carried none.
	req := &chatpb.GetInviteRequest{InviteToken: token}
	require.NoError(t, e.keys.Auth(req, &req.Auth))
	req.InviteToken = newInviteToken()
	_, err := e.client.GetInvite(e.ctx, req)
	require.Error(t, err)

	// A revoked invite reads REVOKED.
	require.Equal(t, chatpb.RevokeInviteResponse_OK, e.revokeInvite(e.keys, chatID, invite.InviteId).Result)
	resp = e.getPublicInvite(token)
	require.Equal(t, chatpb.GetInviteResponse_OK, resp.Result)
	require.Equal(t, chatpb.Invite_REVOKED, resp.Status)
}

// testServer_Invite_List pins GetChatInvites: every invite, newest first,
// whatever its status, with its use count and secret envelope, paged with a
// token bound to the chat.
func testServer_Invite_List(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.bindKey(e.userID, e.keys)
	joinerID, _ := e.addBoundUser("Joiner")
	chatID := e.putKeyedPrivateGroup("Sunday Hikers")
	other := e.putKeyedPrivateGroup("Other")

	// Five invites, created in order, each with its own secret envelope.
	const n = 5
	invites := make([]*chatpb.Invite, n)
	for i := range n {
		req := createInviteRequest(e.keys, chatID, newInviteToken())
		req.SecretEnvelope = inviteSecret(byte(10 + i))
		if i == 1 {
			req.MaxUses = 3
		}
		resp := e.createInvite(e.keys, req)
		require.Equal(t, chatpb.CreateInviteResponse_OK, resp.Result)
		invites[i] = resp.Invite
		time.Sleep(2 * time.Millisecond)
	}
	newestFirst := make([]*chatpb.Invite, n)
	for i := range n {
		newestFirst[i] = invites[n-1-i]
	}
	listed := e.waitForChatInvites(e.keys, chatID, newestFirst...)
	for i := range n {
		require.NoError(t, protoutil.ProtoEqualError(newestFirst[i], listed[i]), i)
		require.True(t, proto.Equal(inviteSecret(byte(10+n-1-i)), listed[i].SecretEnvelope), i)
	}

	// A revoked invite and a used one are listed with their status and
	// count.
	require.Equal(t, chatpb.RevokeInviteResponse_OK, e.revokeInvite(e.keys, chatID, invites[3].InviteId).Result)
	_, err := e.store.JoinWithInvite(e.ctx, chatID, joinerID, invites[1].InviteId, inviteSecret(3), time.Now())
	require.NoError(t, err)
	used := proto.Clone(invites[1]).(*chatpb.Invite)
	used.UseCount = 1
	newestFirst[1] = withStatus(invites[3], chatpb.Invite_REVOKED)
	newestFirst[3] = used
	listed = e.waitForChatInvites(e.keys, chatID, newestFirst...)
	for i := range n {
		require.NoError(t, protoutil.ProtoEqualError(newestFirst[i], listed[i]), i)
	}

	// Pages of two, with no repeats or gaps and an exact has_more.
	var pages [][][]byte
	var token *commonpb.PagingToken
	for {
		resp := e.mustGetChatInvites(e.keys, chatID, &commonpb.QueryOptions{PageSize: 2, PagingToken: token})
		require.Equal(t, chatpb.GetChatInvitesResponse_OK, resp.Result)
		var ids [][]byte
		for _, invite := range resp.Invites {
			ids = append(ids, invite.InviteId.Value)
		}
		pages = append(pages, ids)
		if !resp.HasMore {
			require.Nil(t, resp.PagingToken)
			break
		}
		require.NotNil(t, resp.PagingToken)
		token = resp.PagingToken
	}
	require.Equal(t, [][][]byte{
		{invites[4].InviteId.Value, invites[3].InviteId.Value},
		{invites[2].InviteId.Value, invites[1].InviteId.Value},
		{invites[0].InviteId.Value},
	}, pages)

	// A page that ends exactly at the last invite has no more.
	whole := e.mustGetChatInvites(e.keys, chatID, &commonpb.QueryOptions{PageSize: n})
	require.Len(t, whole.Invites, n)
	require.False(t, whole.HasMore)
	require.Nil(t, whole.PagingToken)

	// The token is the chat's: replayed into another chat it is refused, as
	// is a malformed one.
	first := e.mustGetChatInvites(e.keys, chatID, &commonpb.QueryOptions{PageSize: 2})
	require.True(t, first.HasMore)
	_, err = e.getChatInvites(e.keys, other, &commonpb.QueryOptions{PagingToken: first.PagingToken})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = e.getChatInvites(e.keys, chatID, &commonpb.QueryOptions{PagingToken: &commonpb.PagingToken{Value: []byte{1, 2, 3}}})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// The other chat has none.
	empty := e.mustGetChatInvites(e.keys, other, nil)
	require.Equal(t, chatpb.GetChatInvitesResponse_OK, empty.Result)
	require.Empty(t, empty.Invites)
	require.False(t, empty.HasMore)
}

// testServer_Invite_Revoke pins RevokeInvite: the chat's invite alone, a
// repeat the OK no-op, and the revoke seen in the preview and the listing.
func testServer_Invite_Revoke(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.bindKey(e.userID, e.keys)
	_, strangerKeys := e.addBoundUser("Stranger")
	chatID := e.putKeyedPrivateGroup("Sunday Hikers")
	other := e.putKeyedPrivateGroup("Other")

	token, invite := e.mustCreateInvite(e.keys, chatID)
	_, otherInvite := e.mustCreateInvite(e.keys, other)

	// An ID no invite has, and another chat's invite.
	require.Equal(t, chatpb.RevokeInviteResponse_NOT_FOUND, e.revokeInvite(e.keys, chatID, chat.MustDeriveInviteID(newInviteToken())).Result)
	require.Equal(t, chatpb.RevokeInviteResponse_NOT_FOUND, e.revokeInvite(e.keys, chatID, otherInvite.InviteId).Result)
	require.Equal(t, chatpb.Invite_ACTIVE, e.getInvite(strangerKeys, token).Status)

	// Revoked, then again.
	require.Equal(t, chatpb.RevokeInviteResponse_OK, e.revokeInvite(e.keys, chatID, invite.InviteId).Result)
	require.Equal(t, chatpb.RevokeInviteResponse_OK, e.revokeInvite(e.keys, chatID, invite.InviteId).Result)
	require.Equal(t, chatpb.Invite_REVOKED, e.getInvite(strangerKeys, token).Status)
	listed := e.waitForChatInvites(e.keys, chatID, withStatus(invite, chatpb.Invite_REVOKED))
	require.NoError(t, protoutil.ProtoEqualError(withStatus(invite, chatpb.Invite_REVOKED), listed[0]))

	// The other chat's invite is untouched.
	e.waitForChatInvites(e.keys, other, otherInvite)
}

// testServer_Invite_Permissions pins can_invite on ViewerState: a private
// group's creator while a member, and no one else.
func testServer_Invite_Permissions(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	memberID, memberKeys := e.addUser()
	private := e.putKeyedPrivateGroup("Private", memberID)
	public := e.putOwnedGroup("Public", nil, memberID)

	require.NoError(t, protoutil.ProtoEqualError(privateGroupViewerState(0, nil, true), e.getChat(e.keys, private).Metadata.ViewerState))
	require.True(t, e.getChat(e.keys, private).Metadata.ViewerState.Permissions.CanInvite)
	require.NoError(t, protoutil.ProtoEqualError(privateGroupViewerState(0, nil, false), e.getChat(memberKeys, private).Metadata.ViewerState))
	require.NoError(t, protoutil.ProtoEqualError(memberViewerState, e.getChat(memberKeys, private).Metadata.ViewerState))
	require.NoError(t, protoutil.ProtoEqualError(viewerState(0, nil, true), e.getChat(e.keys, public).Metadata.ViewerState))
	require.False(t, e.getChat(e.keys, public).Metadata.ViewerState.Permissions.CanInvite)

	// A creator who leaves may not invite, and is shown no state at all.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, private).Result)
	require.Nil(t, e.getChat(e.keys, private).Metadata.ViewerState)
	rejoined := e.mustJoinChat(e.keys, private)
	require.Equal(t, chatpb.JoinChatResponse_OK, rejoined.Result)
	require.True(t, rejoined.Chat.ViewerState.Permissions.CanInvite)
}

// joinChatWithInvite is a JoinChat of chatID as keys passing the invite
// token and the joiner's secret envelope.
func (e *serverEnv) joinChatWithInvite(keys model.KeyPair, chatID *commonpb.ChatId, token *chatpb.InviteToken, secret *chatpb.InviteSecretEnvelope) *chatpb.JoinChatResponse {
	req := &chatpb.JoinChatRequest{
		ChatId: chatID,
		Invite: &chatpb.JoinChatRequest_InviteParameters{InviteToken: token, SecretEnvelope: secret},
	}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.JoinChat(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

// createInviteWithCopy creates an invite to chatID as keys, unlimited and
// never expiring unless maxUses is set, requiring OK, and returns its token,
// the signed copy of the chat key it carries, and the invite.
func (e *serverEnv) createInviteWithCopy(keys model.KeyPair, chatID *commonpb.ChatId, maxUses uint32) (*chatpb.InviteToken, *chatpb.InviteKeyEnvelope, *chatpb.Invite) {
	token := newInviteToken()
	req := createInviteRequest(keys, chatID, token)
	req.MaxUses = maxUses
	resp := e.createInvite(keys, req)
	require.Equal(e.t, chatpb.CreateInviteResponse_OK, resp.Result)
	return token, req.KeyEnvelope, resp.Invite
}

// requireInviteUseCount asserts the invite's use count as the store holds
// it, read strongly consistent.
func (e *serverEnv) requireInviteUseCount(inviteID *chatpb.InviteId, want uint32) {
	e.t.Helper()
	invite, err := e.store.GetInvite(e.ctx, inviteID)
	require.NoError(e.t, err)
	require.Equal(e.t, want, invite.UseCount)
}

// requireNotMember asserts that userID is not a member of chatID and holds
// no key envelope for it.
func (e *serverEnv) requireNotMember(chatID *commonpb.ChatId, userID *commonpb.UserId) {
	e.t.Helper()
	isMember, err := e.store.IsMember(e.ctx, chatID, userID)
	require.NoError(e.t, err)
	require.False(e.t, isMember)
	_, err = e.store.GetKeyEnvelope(e.ctx, chatID, userID)
	require.ErrorIs(e.t, err, chat.ErrKeyEnvelopeNotFound)
}

// withUseCount is a copy of invite with useCount, for an invite expected to
// have been used since it was returned.
func withUseCount(invite *chatpb.Invite, useCount uint32) *chatpb.Invite {
	clone := proto.Clone(invite).(*chatpb.Invite)
	clone.UseCount = useCount
	return clone
}

// requireInviteKeyBundle asserts that resp is an OK GetKeyEnvelope holding
// an invite's copy and nothing else: the invite inviteID's copy exactly as
// created, signed by the creator whose key resp reports, with the joiner's
// secret envelope exactly as sent.
func requireInviteKeyBundle(t *testing.T, resp *chatpb.GetKeyEnvelopeResponse, chatID *commonpb.ChatId, inviteID *chatpb.InviteId, keyCopy *chatpb.InviteKeyEnvelope, secret *chatpb.InviteSecretEnvelope, creatorID *commonpb.UserId, creatorKeys model.KeyPair) {
	t.Helper()
	require.Equal(t, chatpb.GetKeyEnvelopeResponse_OK, resp.Result)
	require.NoError(t, resp.Validate())
	require.Nil(t, resp.GetKeyEnvelope())
	bundle := resp.GetInviteKeyBundle()
	require.NotNil(t, bundle)
	require.Equal(t, inviteID.Value, bundle.InviteId.Value)
	require.True(t, proto.Equal(keyCopy, bundle.KeyEnvelope))
	require.True(t, proto.Equal(secret, bundle.SecretEnvelope))
	require.Equal(t, creatorID.Value, resp.WrappedBy.Value)
	require.True(t, proto.Equal(creatorKeys.Proto(), resp.WrappedByPublicKey))
	require.True(t, chat.VerifyInviteSignature(resp.WrappedByPublicKey, chatID, bundle.InviteId, bundle.KeyEnvelope))
}

// testServer_InviteJoin pins a join with an invite end to end: previewed,
// joined, the member's metadata returned, the invite's copy and the joiner's
// secret fetched as invite_key_bundle, then replaced by the joiner's own
// envelope, and the use counted.
func testServer_InviteJoin(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.bindKey(e.userID, e.keys)
	joinerID, joinerKeys := e.addBoundUser("Joiner")
	chatID := e.putKeyedPrivateGroup("Sunday Hikers")
	token, keyCopy, invite := e.createInviteWithCopy(e.keys, chatID, 0)

	// Previewed by a stranger.
	preview := e.getInvite(joinerKeys, token)
	require.Equal(t, chatpb.GetInviteResponse_OK, preview.Result)
	require.Equal(t, chatpb.Invite_ACTIVE, preview.Status)

	// Joined, and shown the chat as a member.
	secret := inviteSecret(7)
	joined := e.joinChatWithInvite(joinerKeys, chatID, token, secret)
	require.Equal(t, chatpb.JoinChatResponse_OK, joined.Result)
	require.Equal(t, chatID.Value, joined.Chat.ChatId.Value)
	require.True(t, joined.Chat.IsPrivate)
	require.False(t, joined.Chat.InLobby)
	require.Len(t, joined.Chat.Members, 1)
	require.Equal(t, joinerID.Value, joined.Chat.Members[0].UserId.Value)
	require.NoError(t, protoutil.ProtoEqualError(privateGroupViewerState(0, nil, false), joined.Chat.ViewerState))
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 2, Version: 1}, joined.Chat.RosterSummary))
	isMember, err := s.IsMember(e.ctx, chatID, joinerID)
	require.NoError(t, err)
	require.True(t, isMember)

	// The invite's copy and the joiner's secret, as invite_key_bundle.
	requireInviteKeyBundle(t, e.mustGetKeyEnvelope(joinerKeys, chatID), chatID, invite.InviteId, keyCopy, secret, e.userID, e.keys)

	// The joiner replaces the copy with their own envelope, which then
	// stands.
	own := keyEnvelopeProto(8)
	require.Equal(t, chatpb.SetKeyEnvelopeResponse_OK, e.mustSetKeyEnvelope(joinerKeys, chatID, own).Result)
	envelope := e.mustGetKeyEnvelope(joinerKeys, chatID)
	require.Equal(t, chatpb.GetKeyEnvelopeResponse_OK, envelope.Result)
	require.NoError(t, envelope.Validate())
	require.Nil(t, envelope.GetInviteKeyBundle())
	require.True(t, proto.Equal(own, envelope.GetKeyEnvelope()))
	require.Equal(t, joinerID.Value, envelope.WrappedBy.Value)
	require.True(t, proto.Equal(joinerKeys.Proto(), envelope.WrappedByPublicKey))
	require.Equal(t, chatpb.SetKeyEnvelopeResponse_ALREADY_SET, e.mustSetKeyEnvelope(joinerKeys, chatID, keyEnvelopeProto(9)).Result)

	// One use counted.
	e.waitForChatInvites(e.keys, chatID, withUseCount(invite, 1))
	e.requireInviteUseCount(invite.InviteId, 1)
}

// testServer_InviteJoin_Announcements pins how a join with an invite is
// announced: as an admission is, with the UserJoined message naming the
// invite's creator, a MemberLeft to the creator only when the joiner was
// waiting in the lobby, and no push to anyone.
func testServer_InviteJoin_Announcements(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.bindKey(e.userID, e.keys)
	chatID := e.putKeyedPrivateGroup("Sunday Hikers")
	token, _, _ := e.createInviteWithCopy(e.keys, chatID, 0)

	// A joiner who was not waiting.
	joinerID, joinerKeys := e.addBoundUser("Joiner")
	joined := e.joinChatWithInvite(joinerKeys, chatID, token, inviteSecret(1))
	require.Equal(t, chatpb.JoinChatResponse_OK, joined.Result)
	summary := &chatpb.RosterSummary{MemberCount: 2, Version: 1}
	require.NoError(t, protoutil.ProtoEqualError(summary, joined.Chat.RosterSummary))

	e.waitForRosterUpdates(joinerID, chatID, 1)
	onChat, excludes := e.rosterUpdatesOnChatTopic(chatID)
	require.Len(t, onChat, 1)
	require.NotNil(t, onChat[0].GetMembershipChanged())
	require.NoError(t, protoutil.ProtoEqualError(summary, onChat[0].RosterSummary))
	require.Len(t, excludes[0], 1)
	require.Equal(t, joinerID.Value, excludes[0][0].Value)
	toJoiner := e.rosterUpdatesOnUserTopic(joinerID, chatID)
	require.Len(t, toJoiner, 1)
	memberJoined := toJoiner[0].GetMemberJoined()
	require.NotNil(t, memberJoined)
	require.Equal(t, joinerID.Value, memberJoined.Member.UserId.Value)
	require.Equal(t, "Joiner", memberJoined.Member.UserProfile.DisplayName)
	require.NotNil(t, memberJoined.Member.JoinedAt)
	require.Equal(t, uint64(1), memberJoined.Member.Version)
	require.NoError(t, protoutil.ProtoEqualError(joined.Chat, memberJoined.Metadata))
	require.NoError(t, protoutil.ProtoEqualError(summary, toJoiner[0].RosterSummary))

	calls := e.systemMessages.userJoinedCalls(chatID)
	require.Len(t, calls, 1)
	requireUserJoinedCall(t, calls[0], joinerID, chat.InvitedBy(e.userID), 1)

	// Nothing to the creator: no lobby update and no push.
	time.Sleep(100 * time.Millisecond)
	require.Empty(t, e.lobbyUpdatesOnUserTopic(e.userID, chatID))
	require.Empty(t, e.pusher.pushesTo(e.userID))
	require.Empty(t, e.pusher.pushesTo(joinerID))

	// A joiner who was waiting leaves the lobby in the same step, and the
	// creator hears it as for an admission; still nobody is pushed for the
	// join, the creator only for the entry.
	waitingID, waitingKeys := e.addBoundUser("Waiting")
	require.Equal(t, chatpb.EnterLobbyResponse_OK, e.enterLobby(waitingKeys, chatID).Result)
	e.waitForLobbyUpdates(e.userID, chatID, 1)
	e.waitForPushes(e.userID, 1)
	require.True(t, e.getChat(waitingKeys, chatID).Metadata.InLobby)

	joined = e.joinChatWithInvite(waitingKeys, chatID, token, inviteSecret(2))
	require.Equal(t, chatpb.JoinChatResponse_OK, joined.Result)
	require.False(t, joined.Chat.InLobby)
	require.False(t, e.getChat(waitingKeys, chatID).Metadata.InLobby)
	updates := e.waitForLobbyUpdates(e.userID, chatID, 2)
	require.Len(t, updates, 2)
	require.Equal(t, waitingID.Value, updates[1].GetMemberLeft().GetUserId().GetValue())
	e.requireNoLobbyUpdatesOnChatTopic(chatID)
	empty, err := e.getLobbyMembers(e.keys, chatID, nil)
	require.NoError(t, err)
	require.Empty(t, empty.Members)
	entries, err := s.GetLobbyEntries(e.ctx, waitingID, []*commonpb.ChatId{chatID})
	require.NoError(t, err)
	require.Empty(t, entries)

	calls = e.systemMessages.userJoinedCalls(chatID)
	require.Len(t, calls, 2)
	requireUserJoinedCall(t, calls[1], waitingID, chat.InvitedBy(e.userID), joined.Chat.RosterSummary.Version)

	time.Sleep(100 * time.Millisecond)
	require.Len(t, e.pusher.pushesTo(e.userID), 1)
	require.Empty(t, e.pusher.pushesTo(waitingID))
}

// testServer_InviteJoin_NoOps pins that a member's JoinChat is the no-op
// whatever invite it carries, the same, a revoked one, an unknown token or
// one to another chat: OK, nothing announced, no use counted and their
// envelope untouched.
func testServer_InviteJoin_NoOps(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.bindKey(e.userID, e.keys)
	joinerID, joinerKeys := e.addBoundUser("Joiner")
	chatID := e.putKeyedPrivateGroup("Sunday Hikers")
	other := e.putKeyedPrivateGroup("Other")
	token, _, invite := e.createInviteWithCopy(e.keys, chatID, 0)
	revokedToken, _, revoked := e.createInviteWithCopy(e.keys, chatID, 0)
	require.Equal(t, chatpb.RevokeInviteResponse_OK, e.revokeInvite(e.keys, chatID, revoked.InviteId).Result)
	otherToken, _, otherInvite := e.createInviteWithCopy(e.keys, other, 0)

	joined := e.joinChatWithInvite(joinerKeys, chatID, token, inviteSecret(1))
	require.Equal(t, chatpb.JoinChatResponse_OK, joined.Result)
	e.waitForRosterUpdates(joinerID, chatID, 1)

	// Set in between, so a write of the copy over it would show.
	own := keyEnvelopeProto(8)
	require.Equal(t, chatpb.SetKeyEnvelopeResponse_OK, e.mustSetKeyEnvelope(joinerKeys, chatID, own).Result)

	for name, again := range map[string]*chatpb.JoinChatResponse{
		"same invite":       e.joinChatWithInvite(joinerKeys, chatID, token, inviteSecret(2)),
		"revoked invite":    e.joinChatWithInvite(joinerKeys, chatID, revokedToken, inviteSecret(2)),
		"unknown token":     e.joinChatWithInvite(joinerKeys, chatID, newInviteToken(), inviteSecret(2)),
		"other chat invite": e.joinChatWithInvite(joinerKeys, chatID, otherToken, inviteSecret(2)),
		"no invite":         e.mustJoinChat(joinerKeys, chatID),
	} {
		require.Equal(t, chatpb.JoinChatResponse_OK, again.Result, name)
		require.Equal(t, chatID.Value, again.Chat.ChatId.Value, name)
		require.NoError(t, protoutil.ProtoEqualError(joined.Chat.RosterSummary, again.Chat.RosterSummary), name)
	}

	time.Sleep(100 * time.Millisecond)
	onChat, _ := e.rosterUpdatesOnChatTopic(chatID)
	require.Len(t, onChat, 1)
	require.Len(t, e.rosterUpdatesOnUserTopic(joinerID, chatID), 1)
	require.Len(t, e.systemMessages.userJoinedCalls(chatID), 1)
	require.Empty(t, e.lobbyUpdatesOnUserTopic(e.userID, chatID))
	e.requireInviteUseCount(invite.InviteId, 1)
	e.requireInviteUseCount(revoked.InviteId, 0)
	e.requireInviteUseCount(otherInvite.InviteId, 0)
	envelope := e.mustGetKeyEnvelope(joinerKeys, chatID)
	require.True(t, proto.Equal(own, envelope.GetKeyEnvelope()))
	require.Equal(t, joinerID.Value, envelope.WrappedBy.Value)
	e.requireNotMember(other, joinerID)
}

// testServer_InviteJoin_Rejoin pins a return with the same invite: the
// departure discards the envelope, and the return is a join like the first,
// counting another use, storing the copy again and writing its own
// UserJoined.
func testServer_InviteJoin_Rejoin(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.bindKey(e.userID, e.keys)
	joinerID, joinerKeys := e.addBoundUser("Joiner")
	chatID := e.putKeyedPrivateGroup("Sunday Hikers")
	token, keyCopy, invite := e.createInviteWithCopy(e.keys, chatID, 0)

	first := e.joinChatWithInvite(joinerKeys, chatID, token, inviteSecret(1))
	require.Equal(t, chatpb.JoinChatResponse_OK, first.Result)

	// Leaving discards the invite's copy.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(joinerKeys, chatID).Result)
	e.requireNotMember(chatID, joinerID)

	// And the return stores it again, with the new secret.
	secret := inviteSecret(4)
	second := e.joinChatWithInvite(joinerKeys, chatID, token, secret)
	require.Equal(t, chatpb.JoinChatResponse_OK, second.Result)
	require.Greater(t, second.Chat.RosterSummary.Version, first.Chat.RosterSummary.Version)
	requireInviteKeyBundle(t, e.mustGetKeyEnvelope(joinerKeys, chatID), chatID, invite.InviteId, keyCopy, secret, e.userID, e.keys)
	e.requireInviteUseCount(invite.InviteId, 2)
	e.waitForChatInvites(e.keys, chatID, withUseCount(invite, 2))

	calls := e.systemMessages.userJoinedCalls(chatID)
	require.Len(t, calls, 2)
	requireUserJoinedCall(t, calls[0], joinerID, chat.InvitedBy(e.userID), first.Chat.RosterSummary.Version)
	requireUserJoinedCall(t, calls[1], joinerID, chat.InvitedBy(e.userID), second.Chat.RosterSummary.Version)

	// Leaving after replacing the copy discards their own envelope too.
	require.Equal(t, chatpb.SetKeyEnvelopeResponse_OK, e.mustSetKeyEnvelope(joinerKeys, chatID, keyEnvelopeProto(8)).Result)
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(joinerKeys, chatID).Result)
	e.requireNotMember(chatID, joinerID)
}

// testServer_InviteJoin_Refused pins every refusal of a join with an invite,
// each with nothing changed: not a member, no envelope, no use counted and
// nothing announced.
func testServer_InviteJoin_Refused(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.bindKey(e.userID, e.keys)
	chatID := e.putKeyedPrivateGroup("Sunday Hikers")
	other := e.putKeyedPrivateGroup("Other")

	requireRefused := func(keys model.KeyPair, userID *commonpb.UserId, token *chatpb.InviteToken, want chatpb.JoinChatResponse_Result) {
		t.Helper()
		resp := e.joinChatWithInvite(keys, chatID, token, inviteSecret(1))
		require.Equal(t, want, resp.Result)
		require.Nil(t, resp.Chat)
		e.requireNotMember(chatID, userID)
	}

	userID, userKeys := e.addBoundUser("User")

	// An unknown token, and another chat's invite.
	requireRefused(userKeys, userID, newInviteToken(), chatpb.JoinChatResponse_INVITE_NOT_FOUND)
	otherToken, _, otherInvite := e.createInviteWithCopy(e.keys, other, 0)
	requireRefused(userKeys, userID, otherToken, chatpb.JoinChatResponse_INVITE_NOT_FOUND)
	e.requireInviteUseCount(otherInvite.InviteId, 0)
	e.requireNotMember(other, userID)

	// Used up.
	limitedToken, _, limited := e.createInviteWithCopy(e.keys, chatID, 1)
	firstID, firstKeys := e.addBoundUser("First")
	require.Equal(t, chatpb.JoinChatResponse_OK, e.joinChatWithInvite(firstKeys, chatID, limitedToken, inviteSecret(1)).Result)
	requireRefused(userKeys, userID, limitedToken, chatpb.JoinChatResponse_INVITE_EXHAUSTED)
	e.requireInviteUseCount(limited.InviteId, 1)

	// Expired. Written through the store, which takes an expiry in the past.
	expiredToken := newInviteToken()
	expiresAt := time.Now().Add(-time.Minute)
	_, created, err := e.store.CreateInvite(e.ctx, chat.Invite{
		ID:             chat.MustDeriveInviteID(expiredToken),
		ChatID:         chatID,
		CreatedBy:      e.userID,
		CreatedAt:      time.Now().Add(-time.Hour),
		ExpiresAt:      &expiresAt,
		SecretEnvelope: inviteSecret(1),
	}, signedInviteKeyEnvelope(e.keys, chatID, chat.MustDeriveInviteID(expiredToken)), chat.MaxActiveInvites, time.Now())
	require.NoError(t, err)
	require.True(t, created)
	requireRefused(userKeys, userID, expiredToken, chatpb.JoinChatResponse_INVITE_EXPIRED)
	e.requireInviteUseCount(chat.MustDeriveInviteID(expiredToken), 0)

	// Revoked.
	revokedToken, _, revoked := e.createInviteWithCopy(e.keys, chatID, 0)
	require.Equal(t, chatpb.RevokeInviteResponse_OK, e.revokeInvite(e.keys, chatID, revoked.InviteId).Result)
	requireRefused(userKeys, userID, revokedToken, chatpb.JoinChatResponse_INVITE_REVOKED)
	e.requireInviteUseCount(revoked.InviteId, 0)

	// A banned user with an ACTIVE invite is BANNED; with one that admits no
	// one, they are told of the invite.
	activeToken, _, active := e.createInviteWithCopy(e.keys, chatID, 0)
	bannedID, bannedKeys := e.addBoundUser("Banned")
	require.Equal(t, chatpb.BanUserResponse_OK, e.mustBanUser(e.keys, chatID, bannedID))
	requireRefused(bannedKeys, bannedID, activeToken, chatpb.JoinChatResponse_BANNED)
	requireRefused(bannedKeys, bannedID, revokedToken, chatpb.JoinChatResponse_INVITE_REVOKED)
	e.requireInviteUseCount(active.InviteId, 0)

	// Without an invite, a private group is DENIED.
	without := e.mustJoinChat(userKeys, chatID)
	require.Equal(t, chatpb.JoinChatResponse_DENIED, without.Result)
	e.requireNotMember(chatID, userID)

	// Only the one real join was announced.
	time.Sleep(100 * time.Millisecond)
	calls := e.systemMessages.userJoinedCalls(chatID)
	require.Len(t, calls, 1)
	require.Equal(t, firstID.Value, calls[0].userID.Value)
	onChat, _ := e.rosterUpdatesOnChatTopic(chatID)
	require.Len(t, onChat, 1)
	require.Empty(t, e.rosterUpdatesOnUserTopic(userID, chatID))
	require.Empty(t, e.rosterUpdatesOnUserTopic(bannedID, chatID))
}

// testServer_InviteJoin_PublicGroup pins that a public group checks its
// rules and never an invite: a non-member passing one is INVITE_NOT_FOUND,
// with the rules not evaluated, whether or not they would pass; a member
// passing one gets the no-op.
func testServer_InviteJoin_PublicGroup(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.bindKey(e.userID, e.keys)
	private := e.putKeyedPrivateGroup("Private")
	token, _, invite := e.createInviteWithCopy(e.keys, private, 0)
	userID, userKeys := e.addBoundUser("User")

	// An open group, whose empty rules everyone satisfies.
	open := e.putGroup("Open", at(1))
	for name, tok := range map[string]*chatpb.InviteToken{
		"real invite":   token,
		"unknown token": newInviteToken(),
	} {
		resp := e.joinChatWithInvite(userKeys, open, tok, inviteSecret(1))
		require.Equal(t, chatpb.JoinChatResponse_INVITE_NOT_FOUND, resp.Result, name)
		require.Nil(t, resp.Chat, name)
		isMember, err := s.IsMember(e.ctx, open, userID)
		require.NoError(t, err)
		require.False(t, isMember, name)
	}
	e.requireInviteUseCount(invite.InviteId, 0)
	e.requireNotMember(private, userID)

	// A group whose rule cannot be evaluated: without an invite the join
	// fails evaluating it, and with one it is never evaluated.
	unpriced := &chat.Chat{
		ID:                     chat.MustGenerateGroupChatID(),
		Type:                   chatpb.ChatType_GROUP,
		Members:                []*commonpb.UserId{model.MustGenerateUserID()},
		Title:                  "Unpriced",
		MinimumListenerBalance: &chat.MinimumBalance{Currency: "xyz", NativeAmount: 1},
		LastActivity:           at(1),
	}
	require.NoError(t, s.PutChat(e.ctx, unpriced))
	e.ocpBalance.setBalance(userKeys.Proto(), ocp_common.ToCoreMintQuarks(1))
	_, err := e.joinChat(userKeys, unpriced.ID)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Equal(t, chatpb.JoinChatResponse_INVITE_NOT_FOUND, e.joinChatWithInvite(userKeys, unpriced.ID, token, inviteSecret(1)).Result)
	isMember, err := s.IsMember(e.ctx, unpriced.ID, userID)
	require.NoError(t, err)
	require.False(t, isMember)

	// Without an invite, the open group is joined as ever, and a member
	// passing one then is the no-op.
	joined := e.mustJoinChat(userKeys, open)
	require.Equal(t, chatpb.JoinChatResponse_OK, joined.Result)
	e.waitForRosterUpdates(userID, open, 1)
	again := e.joinChatWithInvite(userKeys, open, token, inviteSecret(1))
	require.Equal(t, chatpb.JoinChatResponse_OK, again.Result)
	require.NoError(t, protoutil.ProtoEqualError(joined.Chat.RosterSummary, again.Chat.RosterSummary))
	time.Sleep(100 * time.Millisecond)
	require.Len(t, e.rosterUpdatesOnUserTopic(userID, open), 1)
	e.requireInviteUseCount(invite.InviteId, 0)
	require.Empty(t, e.systemMessages.userJoinedCalls(open))
}

// testServer_InviteJoin_Creator pins that a private group's creator rejoins
// on their own whatever invite they pass: OK, a Direct UserJoined, no use
// counted, and their own envelope kept.
func testServer_InviteJoin_Creator(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.bindKey(e.userID, e.keys)
	chatID := e.putKeyedPrivateGroup("Sunday Hikers")
	token, _, invite := e.createInviteWithCopy(e.keys, chatID, 0)

	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, chatID).Result)
	joined := e.joinChatWithInvite(e.keys, chatID, token, inviteSecret(1))
	require.Equal(t, chatpb.JoinChatResponse_OK, joined.Result)
	require.True(t, joined.Chat.ViewerState.Permissions.CanInvite)

	calls := e.systemMessages.userJoinedCalls(chatID)
	require.Len(t, calls, 1)
	requireUserJoinedCall(t, calls[0], e.userID, chat.JoinedDirectly(), joined.Chat.RosterSummary.Version)
	e.requireInviteUseCount(invite.InviteId, 0)
	envelope := e.mustGetKeyEnvelope(e.keys, chatID)
	require.True(t, proto.Equal(keyEnvelopeProto(1), envelope.GetKeyEnvelope()))
	require.Equal(t, e.userID.Value, envelope.WrappedBy.Value)

	// So they do passing a revoked invite or an unknown token.
	require.Equal(t, chatpb.RevokeInviteResponse_OK, e.revokeInvite(e.keys, chatID, invite.InviteId).Result)
	for i, tok := range []*chatpb.InviteToken{token, newInviteToken()} {
		require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, chatID).Result)
		joined = e.joinChatWithInvite(e.keys, chatID, tok, inviteSecret(1))
		require.Equal(t, chatpb.JoinChatResponse_OK, joined.Result)
		calls = e.systemMessages.userJoinedCalls(chatID)
		require.Len(t, calls, 2+i)
		requireUserJoinedCall(t, calls[1+i], e.userID, chat.JoinedDirectly(), joined.Chat.RosterSummary.Version)
	}
	e.requireInviteUseCount(invite.InviteId, 0)
}

func (e *serverEnv) pinMessage(keys model.KeyPair, chatID *commonpb.ChatId, messageID uint64) *chatpb.PinMessageResponse {
	req := &chatpb.PinMessageRequest{ChatId: chatID, MessageId: &messagingpb.MessageId{Value: messageID}}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.PinMessage(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) unpinMessage(keys model.KeyPair, chatID *commonpb.ChatId, messageID uint64) *chatpb.UnpinMessageResponse {
	resp, err := e.tryUnpinMessage(keys, chatID, messageID)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) tryUnpinMessage(keys model.KeyPair, chatID *commonpb.ChatId, messageID uint64) (*chatpb.UnpinMessageResponse, error) {
	req := &chatpb.UnpinMessageRequest{ChatId: chatID, MessageId: &messagingpb.MessageId{Value: messageID}}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.UnpinMessage(e.ctx, req)
}

func (e *serverEnv) getPinnedMessages(keys model.KeyPair, chatID *commonpb.ChatId, mode messagingpb.ViewMode, opts *commonpb.QueryOptions) (*chatpb.GetPinnedMessagesResponse, error) {
	req := &chatpb.GetPinnedMessagesRequest{ChatId: chatID, ViewMode: mode, QueryOptions: opts}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.GetPinnedMessages(e.ctx, req)
}

func (e *serverEnv) mustGetPinnedMessages(keys model.KeyPair, chatID *commonpb.ChatId, mode messagingpb.ViewMode, opts *commonpb.QueryOptions) *chatpb.GetPinnedMessagesResponse {
	resp, err := e.getPinnedMessages(keys, chatID, mode, opts)
	require.NoError(e.t, err)
	return resp
}

// putMessage registers a text message in chatID with the fake messaging
// reader, as the Messages read returns it, and returns it.
func (e *serverEnv) putMessage(chatID *commonpb.ChatId, id uint64, sender *commonpb.UserId, text string) *messagingpb.Message {
	m := textMessage(id, sender, text)
	e.messaging.messages[chat.MessageKeyOf(chatID, m.MessageId)] = m
	return m
}

// putContentMessage registers a message carrying content in chatID with the
// fake messaging reader, as the Messages read returns it, and returns it.
func (e *serverEnv) putContentMessage(chatID *commonpb.ChatId, id uint64, sender *commonpb.UserId, content ...*messagingpb.Content) *messagingpb.Message {
	m := &messagingpb.Message{
		MessageId:     &messagingpb.MessageId{Value: id},
		SenderId:      sender,
		Content:       content,
		Ts:            timestamppb.New(at(int64(id))),
		EventSequence: id,
	}
	e.messaging.messages[chat.MessageKeyOf(chatID, m.MessageId)] = m
	return m
}

// deleteMessage replaces a registered message with its tombstone, as the
// Messages read returns a deleted message.
func (e *serverEnv) deleteMessage(chatID *commonpb.ChatId, id uint64) {
	key := chat.MessageKeyOf(chatID, &messagingpb.MessageId{Value: id})
	m := e.messaging.messages[key]
	require.NotNil(e.t, m)
	e.messaging.messages[key] = &messagingpb.Message{
		MessageId: m.MessageId,
		SenderId:  m.SenderId,
		Content: []*messagingpb.Content{{Type: &messagingpb.Content_Deleted{
			Deleted: &messagingpb.DeletedContent{DeletedTs: timestamppb.Now()},
		}}},
		Ts:            m.Ts,
		EventSequence: m.EventSequence,
	}
}

// pinsChangedOnChatTopic returns every PinsChanged published on chatID's
// topic, paired with the users each publish excluded, in publish order.
func (e *serverEnv) pinsChangedOnChatTopic(chatID *commonpb.ChatId) (changes []*chatpb.MetadataUpdate_PinsChanged, excludes [][]*commonpb.UserId) {
	for _, ev := range e.chatObserver.GetEvents(func(k *commonpb.ChatId) bool { return bytes.Equal(k.Value, chatID.Value) }) {
		require.Equal(e.t, chatID.Value, ev.Event.Event.GetChatUpdate().GetChat().GetValue())
		for _, md := range ev.Event.Event.GetChatUpdate().GetMetadataUpdates() {
			if change := md.GetPinsChanged(); change != nil {
				changes = append(changes, change)
				excludes = append(excludes, ev.Event.ExcludeUserIds)
			}
		}
	}
	return changes, excludes
}

// pinsChangedOnUserTopic returns every PinsChanged for chatID published on
// userID's topic, in publish order.
func (e *serverEnv) pinsChangedOnUserTopic(userID *commonpb.UserId, chatID *commonpb.ChatId) []*chatpb.MetadataUpdate_PinsChanged {
	var changes []*chatpb.MetadataUpdate_PinsChanged
	for _, ev := range e.userObserver.GetEvents(func(k *commonpb.UserId) bool { return bytes.Equal(k.Value, userID.Value) }) {
		update := ev.Event.GetChatUpdate()
		if !bytes.Equal(update.GetChat().GetValue(), chatID.Value) {
			continue
		}
		for _, md := range update.GetMetadataUpdates() {
			if change := md.GetPinsChanged(); change != nil {
				changes = append(changes, change)
			}
		}
	}
	return changes
}

// waitForGroupPinsChanged waits for n PinsChanged on chatID's topic, gives
// the bus a moment to deliver any more, and returns exactly n, none of them
// excluding anyone, with none for the group on any user topic.
func (e *serverEnv) waitForGroupPinsChanged(chatID *commonpb.ChatId, n int, users ...*commonpb.UserId) []*chatpb.MetadataUpdate_PinsChanged {
	if n > 0 {
		e.chatObserver.WaitFor(e.t, func([]*event.KeyAndEvent[*commonpb.ChatId, *eventpb.ChatEvent]) bool {
			changes, _ := e.pinsChangedOnChatTopic(chatID)
			return len(changes) >= n
		})
	}
	time.Sleep(100 * time.Millisecond)
	changes, excludes := e.pinsChangedOnChatTopic(chatID)
	require.Len(e.t, changes, n)
	for _, excluded := range excludes {
		require.Empty(e.t, excluded)
	}
	for _, userID := range users {
		require.Empty(e.t, e.pinsChangedOnUserTopic(userID, chatID))
	}
	return changes
}

// waitForDmPinsChanged waits for n PinsChanged for chatID on userID's topic,
// gives the bus a moment to deliver any more, and returns exactly n, with
// none on the chat's topic.
func (e *serverEnv) waitForDmPinsChanged(userID *commonpb.UserId, chatID *commonpb.ChatId, n int) []*chatpb.MetadataUpdate_PinsChanged {
	if n > 0 {
		e.userObserver.WaitFor(e.t, func([]*event.KeyAndEvent[*commonpb.UserId, *eventpb.Event]) bool {
			return len(e.pinsChangedOnUserTopic(userID, chatID)) >= n
		})
	}
	time.Sleep(100 * time.Millisecond)
	changes := e.pinsChangedOnUserTopic(userID, chatID)
	require.Len(e.t, changes, n)
	onChat, _ := e.pinsChangedOnChatTopic(chatID)
	require.Empty(e.t, onChat)
	return changes
}

// requirePinProto asserts a pin names messageID, was made by pinnedBy, and
// carries want as its message.
func requirePinProto(t *testing.T, pin *chatpb.Pin, want *messagingpb.Message, pinnedBy *commonpb.UserId) {
	t.Helper()
	require.NotNil(t, pin)
	require.Equal(t, want.MessageId.Value, pin.MessageId.Value)
	require.Equal(t, pinnedBy.Value, pin.PinnedBy.Value)
	require.NotNil(t, pin.PinnedAt)
	require.NoError(t, protoutil.ProtoEqualError(want, pin.Message))
}

// requirePinSummaryProto asserts a summary's count and version, and that its
// latest pin carries latest as its message (nil for no latest pin).
func requirePinSummaryProto(t *testing.T, summary *chatpb.PinSummary, count uint32, version uint64, latest *messagingpb.Message, pinnedBy *commonpb.UserId) {
	t.Helper()
	require.NotNil(t, summary)
	require.Equal(t, count, summary.Count)
	require.Equal(t, version, summary.Version)
	if latest == nil {
		require.Nil(t, summary.Latest)
		return
	}
	requirePinProto(t, summary.Latest, latest, pinnedBy)
}

// testServer_Pin_Gates pins who may pin and unpin (see
// ViewerState.Permissions.can_pin): either member of a DM, a group's creator
// while a member, and no one else.
func testServer_Pin_Gates(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	peerID, peerKeys := e.addUser()
	memberID, memberKeys := e.addUser()
	_, strangerKeys := e.addUser()

	dm := e.putDMWithPeer(chatpb.ChatType_DM, peerID, at(1))
	e.putMessage(dm, 1, e.userID, "hi")
	owned := e.putOwnedGroup("Owned", nil, memberID)
	e.putMessage(owned, 1, memberID, "hello")

	// can_pin says who may.
	require.True(t, e.getChat(e.keys, dm).Metadata.ViewerState.Permissions.CanPin)
	require.True(t, e.getChat(peerKeys, dm).Metadata.ViewerState.Permissions.CanPin)
	require.True(t, e.getChat(e.keys, owned).Metadata.ViewerState.Permissions.CanPin)
	require.False(t, e.getChat(memberKeys, owned).Metadata.ViewerState.Permissions.CanPin)

	// Either member of a DM pins and unpins, whoever pinned.
	require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(e.keys, dm, 1).Result)
	require.Equal(t, chatpb.UnpinMessageResponse_OK, e.unpinMessage(peerKeys, dm, 1).Result)
	require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(peerKeys, dm, 1).Result)

	// A group's creator pins; another member may not, nor unpin.
	require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(e.keys, owned, 1).Result)
	require.Equal(t, chatpb.PinMessageResponse_DENIED, e.pinMessage(memberKeys, owned, 1).Result)
	require.Equal(t, chatpb.UnpinMessageResponse_DENIED, e.unpinMessage(memberKeys, owned, 1).Result)

	// A non-member may not, a DM included, and is told so before the message
	// is looked at.
	require.Equal(t, chatpb.PinMessageResponse_DENIED, e.pinMessage(strangerKeys, dm, 1).Result)
	require.Equal(t, chatpb.UnpinMessageResponse_DENIED, e.unpinMessage(strangerKeys, dm, 1).Result)
	require.Equal(t, chatpb.PinMessageResponse_DENIED, e.pinMessage(strangerKeys, owned, 99).Result)
	require.Equal(t, chatpb.UnpinMessageResponse_DENIED, e.unpinMessage(strangerKeys, owned, 1).Result)

	// A legacy group with no recorded creator is pinned by no one.
	plain := e.putGroup("Plain", at(1), memberID)
	e.putMessage(plain, 1, memberID, "hey")
	require.Equal(t, chatpb.PinMessageResponse_DENIED, e.pinMessage(e.keys, plain, 1).Result)
	require.False(t, e.getChat(e.keys, plain).Metadata.ViewerState.Permissions.CanPin)

	// A departed creator pins nothing until they rejoin.
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(e.keys, owned).Result)
	require.Equal(t, chatpb.PinMessageResponse_DENIED, e.pinMessage(e.keys, owned, 1).Result)
	require.Equal(t, chatpb.UnpinMessageResponse_DENIED, e.unpinMessage(e.keys, owned, 1).Result)
	require.Equal(t, chatpb.JoinChatResponse_OK, e.mustJoinChat(e.keys, owned).Result)
	require.Equal(t, chatpb.UnpinMessageResponse_OK, e.unpinMessage(e.keys, owned, 1).Result)

	// A chat that does not exist is NOT_FOUND, a DM's ID or a group's.
	for _, missing := range []*commonpb.ChatId{chat.MustGenerateGroupChatID(), generateDmChatID()} {
		require.Equal(t, chatpb.PinMessageResponse_NOT_FOUND, e.pinMessage(e.keys, missing, 1).Result)
		require.Equal(t, chatpb.UnpinMessageResponse_NOT_FOUND, e.unpinMessage(e.keys, missing, 1).Result)
		require.Equal(t, chatpb.GetPinnedMessagesResponse_NOT_FOUND, e.mustGetPinnedMessages(e.keys, missing, messagingpb.ViewMode_FULL, nil).Result)
	}
}

// testServer_Pin_MessageNotFound pins that only a message in the chat may be
// pinned, that a deleted message, a system message or content of no known
// kind may not, and that a refusal writes and publishes nothing.
func testServer_Pin_MessageNotFound(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	memberID, _ := e.addUser()
	owned := e.putOwnedGroup("Owned", nil, memberID)
	other := e.putOwnedGroup("Other", nil, memberID)
	e.putMessage(owned, 1, memberID, "hello")
	e.putMessage(other, 2, memberID, "elsewhere")
	e.putContentMessage(owned, 3, nil, &messagingpb.Content{Type: &messagingpb.Content_System{
		System: &messagingpb.SystemContent{FallbackText: "A new member joined the group"},
	}})
	e.putContentMessage(owned, 4, memberID, &messagingpb.Content{})
	e.putContentMessage(owned, 5, memberID)
	e.putContentMessage(owned, 6, memberID,
		&messagingpb.Content{Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: "hi"}}},
		&messagingpb.Content{Type: &messagingpb.Content_System{System: &messagingpb.SystemContent{FallbackText: "x"}}},
	)

	// Never sent, or sent in another chat.
	require.Equal(t, chatpb.PinMessageResponse_MESSAGE_NOT_FOUND, e.pinMessage(e.keys, owned, 7).Result)
	require.Equal(t, chatpb.PinMessageResponse_MESSAGE_NOT_FOUND, e.pinMessage(e.keys, owned, 2).Result)

	// A system message, content of no known kind, no content, or a known kind
	// alongside one that is not.
	for _, id := range []uint64{3, 4, 5, 6} {
		require.Equal(t, chatpb.PinMessageResponse_MESSAGE_NOT_ALLOWED, e.pinMessage(e.keys, owned, id).Result, id)
	}

	// Deleted.
	e.deleteMessage(owned, 1)
	require.Equal(t, chatpb.PinMessageResponse_MESSAGE_NOT_ALLOWED, e.pinMessage(e.keys, owned, 1).Result)

	resp := e.mustGetPinnedMessages(e.keys, owned, messagingpb.ViewMode_FULL, nil)
	require.Equal(t, chatpb.GetPinnedMessagesResponse_OK, resp.Result)
	require.Empty(t, resp.Pins)
	requirePinSummaryProto(t, resp.PinSummary, 0, 0, nil, nil)
	e.waitForGroupPinsChanged(owned, 0)
}

// testServer_Pin_ContentKinds pins that a message of every kind on the
// allowlist may be pinned, whatever its content.
func testServer_Pin_ContentKinds(t *testing.T, s chat.Store) {
	e := newServerEnvWithConfig(t, s, serverConfig{maxPinnedMessages: 10})
	peerID, _ := e.addUser()
	dm := e.putDMWithPeer(chatpb.ChatType_DM, peerID, at(1))

	text := &messagingpb.Content{Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: "hi"}}}
	blobID := &blobpb.BlobId{Value: bytes.Repeat([]byte{7}, 16)}
	kinds := []*messagingpb.Content{
		text,
		{Type: &messagingpb.Content_Cash{Cash: &messagingpb.CashContent{
			IntentId: &commonpb.IntentId{Value: bytes.Repeat([]byte{1}, 32)},
			TotalAmount: &commonpb.CryptoPaymentAmount{
				Currency:     "usd",
				NativeAmount: 1,
				Quarks:       1_000_000,
				Mint:         &commonpb.PublicKey{Value: bytes.Repeat([]byte{2}, 32)},
			},
			Recipient: &messagingpb.CashContent_SingleReceiver{SingleReceiver: peerID},
		}}},
		{Type: &messagingpb.Content_Reply{Reply: &messagingpb.ReplyContent{
			RepliedMessageId: &messagingpb.MessageId{Value: 1},
			Content:          []*messagingpb.Content{text},
		}}},
		{Type: &messagingpb.Content_Media{Media: &messagingpb.MediaContent{
			Items: []*blobpb.Media{{Renditions: []*blobpb.Rendition{{
				Role:   blobpb.Rendition_ORIGINAL,
				BlobId: blobID,
				Blob: &blobpb.BlobMetadata{
					MimeType:  "image/jpeg",
					SizeBytes: 1024,
					DownloadUrl: &blobpb.DownloadUrl{
						Url:       "https://cdn.blobs.test/" + hex.EncodeToString(blobID.Value),
						ExpiresAt: timestamppb.New(at(1).Add(time.Hour)),
					},
				},
			}}}},
		}}},
		{Type: &messagingpb.Content_Encrypted{Encrypted: &messagingpb.EncryptedContent{
			Scheme:     messagingpb.EncryptedContent_X25519_XCHACHA20POLY1305,
			Nonce:      bytes.Repeat([]byte{3}, 24),
			Ciphertext: bytes.Repeat([]byte{4}, 32),
		}}},
		{Type: &messagingpb.Content_Widget{Widget: &messagingpb.WidgetContent{
			Type: &messagingpb.WidgetContent_ShareProfile{ShareProfile: &messagingpb.ShareProfileWidget{
				Username: &commonpb.Username{Value: "someone"},
			}},
		}}},
	}
	for i, c := range kinds {
		id := uint64(i + 1)
		e.putContentMessage(dm, id, e.userID, c)
		require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(e.keys, dm, id).Result, c)
	}

	resp := e.mustGetPinnedMessages(e.keys, dm, messagingpb.ViewMode_FULL, nil)
	require.Equal(t, chatpb.GetPinnedMessagesResponse_OK, resp.Result)
	require.Len(t, resp.Pins, len(kinds))
}

// testServer_Pin_Cap pins the cap on a chat's pins: a pin past it is
// TOO_MANY_PINS, and unpinning one makes room.
func testServer_Pin_Cap(t *testing.T, s chat.Store) {
	e := newServerEnvWithConfig(t, s, serverConfig{maxPinnedMessages: 2})
	peerID, _ := e.addUser()
	dm := e.putDMWithPeer(chatpb.ChatType_DM, peerID, at(1))
	for id := uint64(1); id <= 3; id++ {
		e.putMessage(dm, id, e.userID, "hi")
	}

	require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(e.keys, dm, 1).Result)
	require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(e.keys, dm, 2).Result)
	require.Equal(t, chatpb.PinMessageResponse_TOO_MANY_PINS, e.pinMessage(e.keys, dm, 3).Result)

	// A message already pinned is the no-op at the cap, not a refusal.
	require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(e.keys, dm, 2).Result)

	require.Equal(t, chatpb.UnpinMessageResponse_OK, e.unpinMessage(e.keys, dm, 1).Result)
	resp := e.pinMessage(e.keys, dm, 3)
	require.Equal(t, chatpb.PinMessageResponse_OK, resp.Result)
	require.Equal(t, uint32(2), resp.PinSummary.Count)
	e.waitForDmPinsChanged(e.userID, dm, 4)
}

// testServer_Pin_Group follows a group's pins through pins, unpins and their
// no-ops: each response carries the summary with its latest pin's message,
// and each real change, and nothing else, is published once on the chat
// topic, excluding no one.
func testServer_Pin_Group(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	memberID, _ := e.addUser()
	owned := e.putOwnedGroup("Owned", nil, memberID)
	first := e.putMessage(owned, 1, memberID, "first")
	second := e.putMessage(owned, 2, e.userID, "second")

	pinned := e.pinMessage(e.keys, owned, 1)
	require.Equal(t, chatpb.PinMessageResponse_OK, pinned.Result)
	requirePinSummaryProto(t, pinned.PinSummary, 1, 1, first, e.userID)
	changes := e.waitForGroupPinsChanged(owned, 1, e.userID, memberID)
	requirePinProto(t, changes[0].GetPinned(), first, e.userID)
	require.NoError(t, protoutil.ProtoEqualError(pinned.PinSummary, changes[0].Summary))

	// Pinning it again is the no-op: same summary, nothing published.
	again := e.pinMessage(e.keys, owned, 1)
	require.Equal(t, chatpb.PinMessageResponse_OK, again.Result)
	require.NoError(t, protoutil.ProtoEqualError(pinned.PinSummary, again.PinSummary))
	e.waitForGroupPinsChanged(owned, 1)

	pinned = e.pinMessage(e.keys, owned, 2)
	require.Equal(t, chatpb.PinMessageResponse_OK, pinned.Result)
	requirePinSummaryProto(t, pinned.PinSummary, 2, 2, second, e.userID)
	changes = e.waitForGroupPinsChanged(owned, 2)
	requirePinProto(t, changes[1].GetPinned(), second, e.userID)
	require.NoError(t, protoutil.ProtoEqualError(pinned.PinSummary, changes[1].Summary))

	// Unpinning the latest makes the one before it the latest, carrying its
	// message.
	unpinned := e.unpinMessage(e.keys, owned, 2)
	require.Equal(t, chatpb.UnpinMessageResponse_OK, unpinned.Result)
	requirePinSummaryProto(t, unpinned.PinSummary, 1, 3, first, e.userID)
	changes = e.waitForGroupPinsChanged(owned, 3)
	require.Equal(t, uint64(2), changes[2].GetUnpinned().GetValue())
	require.NoError(t, protoutil.ProtoEqualError(unpinned.PinSummary, changes[2].Summary))

	// Unpinning a message that is not pinned, or was never sent, is the
	// no-op, with nothing published.
	for _, id := range []uint64{2, 99} {
		resp := e.unpinMessage(e.keys, owned, id)
		require.Equal(t, chatpb.UnpinMessageResponse_OK, resp.Result)
		require.NoError(t, protoutil.ProtoEqualError(unpinned.PinSummary, resp.PinSummary))
	}
	e.waitForGroupPinsChanged(owned, 3)

	// The last unpin leaves no latest.
	unpinned = e.unpinMessage(e.keys, owned, 1)
	require.Equal(t, chatpb.UnpinMessageResponse_OK, unpinned.Result)
	requirePinSummaryProto(t, unpinned.PinSummary, 0, 4, nil, nil)
	changes = e.waitForGroupPinsChanged(owned, 4)
	require.Equal(t, uint64(1), changes[3].GetUnpinned().GetValue())
	requirePinSummaryProto(t, changes[3].Summary, 0, 4, nil, nil)
}

// testServer_Pin_Dm pins a DM's events: each real change reaches both
// members on their user topics, the caller included, and never the Flipcash
// team account, which is never delivered to.
func testServer_Pin_Dm(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	peerID, peerKeys := e.addUser()
	dm := e.putDMWithPeer(chatpb.ChatType_DM, peerID, at(1))
	msg := e.putMessage(dm, 1, peerID, "pin me")

	pinned := e.pinMessage(peerKeys, dm, 1)
	require.Equal(t, chatpb.PinMessageResponse_OK, pinned.Result)
	requirePinSummaryProto(t, pinned.PinSummary, 1, 1, msg, peerID)
	for _, userID := range []*commonpb.UserId{e.userID, peerID} {
		changes := e.waitForDmPinsChanged(userID, dm, 1)
		requirePinProto(t, changes[0].GetPinned(), msg, peerID)
		require.NoError(t, protoutil.ProtoEqualError(pinned.PinSummary, changes[0].Summary))
	}

	require.Equal(t, chatpb.UnpinMessageResponse_OK, e.unpinMessage(e.keys, dm, 1).Result)
	for _, userID := range []*commonpb.UserId{e.userID, peerID} {
		changes := e.waitForDmPinsChanged(userID, dm, 2)
		require.Equal(t, uint64(1), changes[1].GetUnpinned().GetValue())
	}

	// In a DM with the team account the user's devices hear of it and the
	// team's do not.
	teamDM := e.putDMWithPeer(chatpb.ChatType_DM, e.teamUserID, at(2))
	e.putMessage(teamDM, 1, e.teamUserID, "welcome")
	require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(e.keys, teamDM, 1).Result)
	e.waitForDmPinsChanged(e.userID, teamDM, 1)
	require.Empty(t, e.pinsChangedOnUserTopic(e.teamUserID, teamDM))
}

// testServer_GetPinnedMessages_Paging pages a chat's pins most recently
// pinned first, each with its message, the summary on every page, and a
// token bound to the chat.
func testServer_GetPinnedMessages_Paging(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	memberID, memberKeys := e.addUser()
	owned := e.putOwnedGroup("Owned", nil, memberID)
	other := e.putOwnedGroup("Other", nil, memberID)
	var messages []*messagingpb.Message
	for id := uint64(1); id <= 5; id++ {
		messages = append(messages, e.putMessage(owned, id, memberID, fmt.Sprintf("message %d", id)))
		require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(e.keys, owned, id).Result)
	}

	// Every member may read the pins, not only who may pin.
	var (
		got   []*chatpb.Pin
		token *commonpb.PagingToken
	)
	for page := 0; ; page++ {
		resp := e.mustGetPinnedMessages(memberKeys, owned, messagingpb.ViewMode_FULL, &commonpb.QueryOptions{PageSize: 2, PagingToken: token})
		require.Equal(t, chatpb.GetPinnedMessagesResponse_OK, resp.Result)
		requirePinSummaryProto(t, resp.PinSummary, 5, 5, messages[4], e.userID)
		got = append(got, resp.Pins...)
		if !resp.HasMore {
			require.Nil(t, resp.PagingToken)
			require.Len(t, resp.Pins, 1)
			require.Equal(t, 2, page)
			break
		}
		require.Len(t, resp.Pins, 2)
		require.NotNil(t, resp.PagingToken)
		token = resp.PagingToken
	}
	require.Len(t, got, 5)
	for i, pin := range got {
		requirePinProto(t, pin, messages[4-i], e.userID)
	}

	// With no page size the whole list fits one page.
	resp := e.mustGetPinnedMessages(e.keys, owned, messagingpb.ViewMode_FULL, nil)
	require.Len(t, resp.Pins, 5)
	require.False(t, resp.HasMore)

	// A token minted for one chat is refused for another.
	first := e.mustGetPinnedMessages(e.keys, owned, messagingpb.ViewMode_FULL, &commonpb.QueryOptions{PageSize: 2})
	require.True(t, first.HasMore)
	_, err := e.getPinnedMessages(e.keys, other, messagingpb.ViewMode_FULL, &commonpb.QueryOptions{PagingToken: first.PagingToken})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = e.getPinnedMessages(e.keys, owned, messagingpb.ViewMode_FULL, &commonpb.QueryOptions{PagingToken: &commonpb.PagingToken{Value: []byte{1, 2, 3}}})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// A pinned message deleted since is shown as its tombstone.
	e.deleteMessage(owned, 5)
	resp = e.mustGetPinnedMessages(e.keys, owned, messagingpb.ViewMode_FULL, &commonpb.QueryOptions{PageSize: 1})
	require.Equal(t, chatpb.GetPinnedMessagesResponse_OK, resp.Result)
	require.NotNil(t, resp.Pins[0].Message.Content[0].GetDeleted())
	require.NotNil(t, resp.PinSummary.Latest.Message.Content[0].GetDeleted())

	// A DM's pins page the same way, to its members alone.
	peerID, peerKeys := e.addUser()
	_, strangerKeys := e.addUser()
	dm := e.putDMWithPeer(chatpb.ChatType_DM, peerID, at(1))
	dmMessage := e.putMessage(dm, 1, peerID, "dm")
	require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(peerKeys, dm, 1).Result)
	resp = e.mustGetPinnedMessages(e.keys, dm, messagingpb.ViewMode_FULL, nil)
	require.Equal(t, chatpb.GetPinnedMessagesResponse_OK, resp.Result)
	require.Len(t, resp.Pins, 1)
	requirePinProto(t, resp.Pins[0], dmMessage, peerID)
	for _, mode := range []messagingpb.ViewMode{messagingpb.ViewMode_FULL, messagingpb.ViewMode_FULL_OR_REDACTED, messagingpb.ViewMode_REDACTED} {
		require.Equal(t, chatpb.GetPinnedMessagesResponse_DENIED, e.mustGetPinnedMessages(strangerKeys, dm, mode, nil).Result)
	}
}

// testServer_GetPinnedMessages_Reading pins that pins are read as the chat's
// messages are (see messaging's GetMessages): a public group's non-member who
// may not read it in full gets them redacted where the mode allows it and is
// DENIED under FULL, and a private group's non-member is DENIED under every
// mode.
func testServer_GetPinnedMessages_Reading(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	founderID, founderKeys := e.addUser()
	group := &chat.Chat{
		ID:                     chat.MustGenerateGroupChatID(),
		Type:                   chatpb.ChatType_GROUP,
		Members:                []*commonpb.UserId{founderID},
		CreatorID:              founderID,
		Title:                  "Whales",
		MinimumListenerBalance: &chat.MinimumBalance{Currency: "usd", NativeAmount: 100},
		LastActivity:           at(1),
	}
	require.NoError(t, s.PutChat(e.ctx, group))
	const secret = "meet at the old bank at six"
	msg := e.putMessage(group.ID, 1, founderID, secret)
	require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(founderKeys, group.ID, 1).Result)

	// The env user has no account and so fails the listener rules.
	require.Equal(t, chatpb.GetPinnedMessagesResponse_DENIED, e.mustGetPinnedMessages(e.keys, group.ID, messagingpb.ViewMode_FULL, nil).Result)
	for _, mode := range []messagingpb.ViewMode{messagingpb.ViewMode_FULL_OR_REDACTED, messagingpb.ViewMode_REDACTED} {
		resp := e.mustGetPinnedMessages(e.keys, group.ID, mode, nil)
		require.Equal(t, chatpb.GetPinnedMessagesResponse_OK, resp.Result)
		require.Len(t, resp.Pins, 1)
		for _, m := range []*messagingpb.Message{resp.Pins[0].Message, resp.PinSummary.Latest.Message} {
			require.True(t, m.Redacted)
			require.Equal(t, msg.MessageId.Value, m.MessageId.Value)
			require.NotEqual(t, secret, m.Content[0].GetText().GetText())
		}
	}

	// A member reads them in full, and redacted only on request.
	resp := e.mustGetPinnedMessages(founderKeys, group.ID, messagingpb.ViewMode_FULL, nil)
	require.Equal(t, chatpb.GetPinnedMessagesResponse_OK, resp.Result)
	requirePinProto(t, resp.Pins[0], msg, founderID)
	resp = e.mustGetPinnedMessages(founderKeys, group.ID, messagingpb.ViewMode_REDACTED, nil)
	require.True(t, resp.Pins[0].Message.Redacted)

	// A private group's non-member is denied under every mode.
	memberID, memberKeys := e.addUser()
	private := e.putKeyedPrivateGroup("Private", memberID)
	e.putMessage(private, 1, memberID, "ciphertext")
	require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(e.keys, private, 1).Result)
	_, strangerKeys := e.addUser()
	for _, mode := range []messagingpb.ViewMode{messagingpb.ViewMode_FULL, messagingpb.ViewMode_FULL_OR_REDACTED, messagingpb.ViewMode_REDACTED} {
		require.Equal(t, chatpb.GetPinnedMessagesResponse_DENIED, e.mustGetPinnedMessages(strangerKeys, private, mode, nil).Result)
	}
	resp = e.mustGetPinnedMessages(memberKeys, private, messagingpb.ViewMode_FULL, nil)
	require.Equal(t, chatpb.GetPinnedMessagesResponse_OK, resp.Result)
	require.Len(t, resp.Pins, 1)
}

// testServer_Pin_MissingMessage pins the rule that every Pin carries its
// message: a pinned message the read does not return fails the call that
// would show it with Internal, and the calls that show it not are unaffected.
func testServer_Pin_MissingMessage(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	memberID, _ := e.addUser()
	owned := e.putOwnedGroup("Owned", nil, memberID)
	e.putMessage(owned, 1, memberID, "first")
	e.putMessage(owned, 2, memberID, "second")
	require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(e.keys, owned, 1).Result)
	require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(e.keys, owned, 2).Result)
	e.waitForGroupPinsChanged(owned, 2)

	// The latest pin's message goes missing from the read.
	delete(e.messaging.messages, chat.MessageKeyOf(owned, &messagingpb.MessageId{Value: 2}))

	_, err := e.getPinnedMessages(e.keys, owned, messagingpb.ViewMode_FULL, nil)
	require.Equal(t, codes.Internal, status.Code(err))

	// An unpin that leaves it the latest lands, but cannot show its summary,
	// and so publishes nothing either.
	_, err = e.tryUnpinMessage(e.keys, owned, 1)
	require.Equal(t, codes.Internal, status.Code(err))
	e.waitForGroupPinsChanged(owned, 2)

	// Unpinning it leaves nothing missing to show.
	resp := e.unpinMessage(e.keys, owned, 2)
	require.Equal(t, chatpb.UnpinMessageResponse_OK, resp.Result)
	requirePinSummaryProto(t, resp.PinSummary, 0, 4, nil, nil)
	e.waitForGroupPinsChanged(owned, 3)
}

// testServer_PinSummary_Hydration pins Metadata.pin_summary on the chat's
// full-detail carriers: the zero summary while nothing is pinned, then the
// summary with its latest pin's message as pins and unpins move it, a deleted
// latest as its tombstone, and never on a feed, which may omit it.
func testServer_PinSummary_Hydration(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	memberID, memberKeys := e.addUser()
	peerID, peerKeys := e.addUser()
	owned := e.putOwnedGroup("Owned", nil, memberID)
	dm := e.putDMWithPeer(chatpb.ChatType_DM, peerID, at(1))

	// Nothing pinned: the zero summary, for a group and a DM alike.
	requirePinSummaryProto(t, e.getChat(e.keys, owned).Metadata.PinSummary, 0, 0, nil, nil)
	requirePinSummaryProto(t, e.getChat(memberKeys, owned).Metadata.PinSummary, 0, 0, nil, nil)
	requirePinSummaryProto(t, e.getChat(e.keys, dm).Metadata.PinSummary, 0, 0, nil, nil)

	first := e.putMessage(owned, 1, memberID, "first")
	second := e.putMessage(owned, 2, e.userID, "second")
	dmMessage := e.putMessage(dm, 1, peerID, "dm")
	require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(e.keys, owned, 1).Result)
	require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(e.keys, owned, 2).Result)
	require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(peerKeys, dm, 1).Result)

	// GetChat carries the summary, its latest with its message, to every
	// member.
	for _, keys := range []model.KeyPair{e.keys, memberKeys} {
		requirePinSummaryProto(t, e.getChat(keys, owned).Metadata.PinSummary, 2, 2, second, e.userID)
	}
	for _, keys := range []model.KeyPair{e.keys, peerKeys} {
		requirePinSummaryProto(t, e.getChat(keys, dm).Metadata.PinSummary, 1, 1, dmMessage, peerID)
	}

	// So does every other full-detail carrier: an edit and a join.
	title := "Renamed"
	edited := e.mustEditChat(e.keys, owned, &title, nil)
	require.Equal(t, chatpb.EditChatResponse_OK, edited.Result)
	requirePinSummaryProto(t, edited.Chat.PinSummary, 2, 2, second, e.userID)
	require.Equal(t, chatpb.LeaveChatResponse_OK, e.mustLeaveChat(memberKeys, owned).Result)
	joined := e.mustJoinChat(memberKeys, owned)
	require.Equal(t, chatpb.JoinChatResponse_OK, joined.Result)
	requirePinSummaryProto(t, joined.Chat.PinSummary, 2, 2, second, e.userID)

	// The feeds are list views, and carry none.
	groups := e.mustGetGroupFeed(&commonpb.QueryOptions{})
	require.Len(t, groups.Chats, 1)
	require.Equal(t, owned.Value, groups.Chats[0].ChatId.Value)
	require.Nil(t, groups.Chats[0].PinSummary)
	dms, err := e.getDmFeedOfType(chatpb.ChatType_DM, &commonpb.QueryOptions{})
	require.NoError(t, err)
	require.Len(t, dms.Chats, 1)
	require.Equal(t, dm.Value, dms.Chats[0].ChatId.Value)
	require.Nil(t, dms.Chats[0].PinSummary)

	// Unpins move it: the one before becomes the latest, then none is left.
	require.Equal(t, chatpb.UnpinMessageResponse_OK, e.unpinMessage(e.keys, owned, 2).Result)
	requirePinSummaryProto(t, e.getChat(e.keys, owned).Metadata.PinSummary, 1, 3, first, e.userID)
	require.Equal(t, chatpb.UnpinMessageResponse_OK, e.unpinMessage(e.keys, owned, 1).Result)
	requirePinSummaryProto(t, e.getChat(e.keys, owned).Metadata.PinSummary, 0, 4, nil, nil)

	// A latest pinned message deleted since is shown as its tombstone.
	e.deleteMessage(dm, 1)
	summary := e.getChat(e.keys, dm).Metadata.PinSummary
	require.Equal(t, uint32(1), summary.Count)
	require.NotNil(t, summary.Latest.Message.Content[0].GetDeleted())
}

// testServer_PinSummary_Reading pins that the pin summary is messaging state:
// shown to whoever may read the chat's messages, its latest message redacted
// under a redacted reading, the unauthenticated public view's included, and
// withheld with the rest of the messaging state from a viewer whose reading
// is denied.
func testServer_PinSummary_Reading(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	founderID, founderKeys := e.addUser()
	group := &chat.Chat{
		ID:                     chat.MustGenerateGroupChatID(),
		Type:                   chatpb.ChatType_GROUP,
		Members:                []*commonpb.UserId{founderID},
		CreatorID:              founderID,
		Title:                  "Whales",
		MinimumListenerBalance: &chat.MinimumBalance{Currency: "usd", NativeAmount: 100},
		LastActivity:           at(1),
	}
	require.NoError(t, s.PutChat(e.ctx, group))
	const secret = "meet at the old bank at six"
	msg := e.putMessage(group.ID, 1, founderID, secret)
	require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(founderKeys, group.ID, 1).Result)

	requireRedacted := func(summary *chatpb.PinSummary) {
		t.Helper()
		require.NotNil(t, summary)
		require.Equal(t, uint32(1), summary.Count)
		require.Equal(t, uint64(1), summary.Version)
		require.Equal(t, msg.MessageId.Value, summary.Latest.MessageId.Value)
		require.Equal(t, founderID.Value, summary.Latest.PinnedBy.Value)
		require.True(t, summary.Latest.Message.Redacted)
		require.Equal(t, msg.MessageId.Value, summary.Latest.Message.MessageId.Value)
		require.NotEqual(t, secret, summary.Latest.Message.Content[0].GetText().GetText())
	}

	// The env user has no account and so fails the listener rules: under
	// FULL their reading is denied, and the record comes with no summary.
	resp := e.getChatWithMode(e.keys, group.ID, messagingpb.ViewMode_FULL)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Nil(t, resp.Metadata.PinSummary)
	for _, mode := range []messagingpb.ViewMode{messagingpb.ViewMode_FULL_OR_REDACTED, messagingpb.ViewMode_REDACTED} {
		resp := e.getChatWithMode(e.keys, group.ID, mode)
		require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
		requireRedacted(resp.Metadata.PinSummary)
	}

	// The public view is a redacted read.
	resp = e.getPublicChat(group.ID, messagingpb.ViewMode_REDACTED)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	requireRedacted(resp.Metadata.PinSummary)

	// A member reads it in full, and redacted only on request.
	requirePinSummaryProto(t, e.getChat(founderKeys, group.ID).Metadata.PinSummary, 1, 1, msg, founderID)
	requireRedacted(e.getChatWithMode(founderKeys, group.ID, messagingpb.ViewMode_REDACTED).Metadata.PinSummary)

	// A private group's non-member, signed in or not, is shown the record
	// alone; its members, the summary.
	memberID, memberKeys := e.addUser()
	private := e.putKeyedPrivateGroup("Private", memberID)
	privateMessage := e.putMessage(private, 1, memberID, "ciphertext")
	require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(e.keys, private, 1).Result)
	_, strangerKeys := e.addUser()
	for _, mode := range []messagingpb.ViewMode{messagingpb.ViewMode_FULL, messagingpb.ViewMode_FULL_OR_REDACTED, messagingpb.ViewMode_REDACTED} {
		resp := e.getChatWithMode(strangerKeys, private, mode)
		require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
		require.Nil(t, resp.Metadata.PinSummary)
	}
	resp = e.getPublicChat(private, messagingpb.ViewMode_REDACTED)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Nil(t, resp.Metadata.PinSummary)
	requirePinSummaryProto(t, e.getChat(memberKeys, private).Metadata.PinSummary, 1, 1, privateMessage, e.userID)
}

// testServer_PinSummary_MissingMessage pins the rule that every Pin carries
// its message on a chat's metadata too: a latest pinned message the read does
// not return fails GetChat with Internal, and a feed, which shows no summary,
// is unaffected.
func testServer_PinSummary_MissingMessage(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	memberID, _ := e.addUser()
	owned := e.putOwnedGroup("Owned", nil, memberID)
	e.putMessage(owned, 1, memberID, "first")
	require.Equal(t, chatpb.PinMessageResponse_OK, e.pinMessage(e.keys, owned, 1).Result)

	delete(e.messaging.messages, chat.MessageKeyOf(owned, &messagingpb.MessageId{Value: 1}))

	req := &chatpb.GetChatRequest{ChatId: owned}
	require.NoError(t, e.keys.Auth(req, &req.Auth))
	_, err := e.client.GetChat(e.ctx, req)
	require.Equal(t, codes.Internal, status.Code(err))

	feed := e.mustGetGroupFeed(&commonpb.QueryOptions{})
	require.Len(t, feed.Chats, 1)
	require.Nil(t, feed.Chats[0].PinSummary)

	// Unpinning it leaves nothing missing to show.
	require.Equal(t, chatpb.UnpinMessageResponse_OK, e.unpinMessage(e.keys, owned, 1).Result)
	requirePinSummaryProto(t, e.getChat(e.keys, owned).Metadata.PinSummary, 0, 2, nil, nil)
}
