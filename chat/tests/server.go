package tests

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
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
	"google.golang.org/protobuf/types/known/timestamppb"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"
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
		testServer_GetChat_TipDm_HidesPhoneNumbers,
		testServer_GetChat_HiddenWhenPeerBlocked,
		testServer_GetChat_Group_Hydrates,
		testServer_GetChat_Group_Picture,
		testServer_GetChat_Group_MembershipLifecycle,
		testServer_GetChat_Group_NonMember,
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
		testServer_StartChat_TitleModerated,
		testServer_StartChat_ModerationFailureIsInternal,
		testServer_StartChat_InvalidRules,
		testServer_StartChat_RulesNotSatisfied,
		testServer_StartChat_WithRules,
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

	userObserver *event.TestEventObserver[*commonpb.UserId, *eventpb.Event]
	chatObserver *event.TestEventObserver[*commonpb.ChatId, *eventpb.ChatEvent]

	userID *commonpb.UserId
	keys   model.KeyPair
}

func newServerEnv(t *testing.T, s chat.Store) *serverEnv {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := auth.NewStaticAuthorizer(log)
	userID := model.MustGenerateUserID()
	keys := model.MustGenerateKeyPair()
	authz.Add(userID, keys)

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
	access := chat.NewAccess(s, chat.NewRuleEvaluator(accounts, balances, s))
	server := chat.NewServer(log, authz, accounts, blocklist, s, media, messaging, moderator, profiles, access, userBus, chatBus, false)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		chatpb.RegisterChatServer(s, server)
	}))

	return &serverEnv{
		t:            t,
		ctx:          ctx,
		client:       chatpb.NewChatClient(cc),
		authz:        authz,
		store:        s,
		messaging:    messaging,
		profiles:     profiles,
		blocklist:    blocklist,
		media:        media,
		moderator:    moderator,
		accounts:     accounts,
		ocpBalance:   ocpBalance,
		userObserver: userObserver,
		chatObserver: chatObserver,
		userID:       userID,
		keys:         keys,
	}
}

// addUser registers a second authorized user with the env.
func (e *serverEnv) addUser() (*commonpb.UserId, model.KeyPair) {
	userID := model.MustGenerateUserID()
	keys := model.MustGenerateKeyPair()
	e.authz.Add(userID, keys)
	return userID, keys
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
		resp.BalancesByOwner[key] = &ocp_balancepb.OwnerBalance{Owner: owner, CoreMintValue: quarks}
	}
	return resp, nil
}

// fakeMedia is a canned chat.Media for server tests: it resolves
// whatever rendition sets a test registers per ORIGINAL blob ID, and omits the
// rest the way the real reader omits an unknown or not-yet-servable original.
// It attaches, as a chat picture, only the blobs a test has registered as
// attachable, and records what it attached to which chat.
type fakeMedia struct {
	renditions map[string][]*blobpb.Rendition

	// attachable is the set of blob IDs SetAsChatPicture accepts, standing in
	// for "a READY image original the caller owns"; any other blob is refused
	// as blob.ErrBlobNotFound.
	attachable map[string]bool
	// chatPictures records the blob attached to each chat, keyed by chat ID.
	chatPictures map[string]*blobpb.BlobId

	// attachErr, when set, fails every SetAsChatPicture call with it, standing
	// in for a blob-domain outage.
	attachErr error
}

func newFakeMedia() *fakeMedia {
	return &fakeMedia{
		renditions:   make(map[string][]*blobpb.Rendition),
		attachable:   make(map[string]bool),
		chatPictures: make(map[string]*blobpb.BlobId),
	}
}

// setAttachable registers a blob SetAsChatPicture will accept, with its
// rendition set resolvable the way a READY original's is.
func (f *fakeMedia) setAttachable(originalID *blobpb.BlobId) []*blobpb.Rendition {
	f.attachable[string(originalID.Value)] = true
	return f.setRenditions(originalID)
}

func (f *fakeMedia) SetAsChatPicture(_ context.Context, _ *commonpb.UserId, chatID *commonpb.ChatId, blobID *blobpb.BlobId) error {
	if f.attachErr != nil {
		return f.attachErr
	}
	if !f.attachable[string(blobID.Value)] {
		return blob.ErrBlobNotFound
	}
	f.chatPictures[string(chatID.Value)] = blobID
	return nil
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
		if r, ok := f.renditions[string(id.Value)]; ok {
			out[string(id.Value)] = r
		}
	}
	return out, nil
}

// fakeMessagingReader is a canned chat.MessagingReader for server tests: it
// returns whatever last messages, pointers, and head event sequences a test
// registers per chat.
type fakeMessagingReader struct {
	lastMessages    map[string]*messagingpb.Message
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
	profilePictures map[string]*blobpb.Media
	joinedAt        map[string]time.Time

	// phoneNumbersErr, when set, fails every GetPhoneNumbers call.
	phoneNumbersErr error
}

func newFakeProfileReader() *fakeProfileReader {
	return &fakeProfileReader{
		phoneNumbers:    make(map[string]*commonpb.PhoneNumber),
		displayNames:    make(map[string]string),
		profilePictures: make(map[string]*blobpb.Media),
		joinedAt:        make(map[string]time.Time),
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

// GetPublicProfiles returns an entry for every user asked about, the way the real
// reader does for a chat's members: they are all users the profile domain knows,
// so each has at least a join timestamp even with no name or picture set.
func (f *fakeProfileReader) GetPublicProfiles(_ context.Context, userIDs []*commonpb.UserId) (map[string]*profilepb.UserProfile, error) {
	out := make(map[string]*profilepb.UserProfile)
	for _, userID := range userIDs {
		key := string(userID.Value)

		joinedAt, ok := f.joinedAt[key]
		if !ok {
			joinedAt = at(0)
		}

		out[key] = &profilepb.UserProfile{
			UserId:               userID,
			DisplayName:          f.displayNames[key],
			ProfilePicture:       f.profilePictures[key],
			JoinTs:               timestamppb.New(joinedAt),
			TipCardCustomization: profile.DefaultTipCardCustomization(),
		}
	}
	return out, nil
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
		ID:            chatID,
		Type:          chatpb.ChatType_GROUP,
		Members:       append([]*commonpb.UserId{e.userID}, others...),
		Title:         title,
		PictureBlobID: pictureBlobID,
		LastActivity:  lastActivity,
	}))
	return chatID
}

func (e *serverEnv) getChat(keys model.KeyPair, chatID *commonpb.ChatId) *chatpb.GetChatResponse {
	req := &chatpb.GetChatRequest{ChatId: chatID}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	resp, err := e.client.GetChat(e.ctx, req)
	require.NoError(e.t, err)
	return resp
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
	require.Nil(t, resp.Metadata.Picture)
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

func testServer_GetChat_TipDm_HidesPhoneNumbers(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	peer := model.MustGenerateUserID()
	chatID := generateDmChatID()
	require.NoError(t, s.PutChat(e.ctx, &chat.Chat{
		ID:           chatID,
		Type:         chatpb.ChatType_TIP_DM,
		Members:      []*commonpb.UserId{e.userID, peer},
		LastActivity: at(1),
	}))

	e.profiles.phoneNumbers[string(peer.Value)] = &commonpb.PhoneNumber{Value: "+15551234567"}
	e.profiles.displayNames[string(peer.Value)] = "Peer Name"
	e.profiles.setProfilePicture(peer, &blobpb.BlobId{Value: make([]byte, 16)})

	resp := e.getChat(e.keys, chatID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Equal(t, chatpb.ChatType_TIP_DM, resp.Metadata.Type)

	members := byUserID(resp.Metadata.Members)
	profile := members[string(peer.Value)].UserProfile
	require.NotNil(t, profile)
	require.Nil(t, profile.PhoneNumber)
	require.Equal(t, "Peer Name", profile.DisplayName)
	require.NotNil(t, profile.GetProfilePicture())
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
	picture := resp.Metadata.GetPicture()
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
	require.Nil(t, resp.Metadata.GetPicture())

	// A picture whose original no longer resolves (unknown to blob storage, or
	// not servable) is not an error: the metadata still names the stored
	// ORIGINAL, unresolved, for the client to treat as unavailable.
	unresolvable := &blobpb.BlobId{Value: []byte("group-picture-02")}
	stale := e.putGroupWithPicture("Stale Picture", unresolvable, at(1), model.MustGenerateUserID())
	resp = e.getChat(e.keys, stale)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	picture = resp.Metadata.GetPicture()
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
	require.Equal(t, replacement.Value, resp.Metadata.GetPicture().GetRenditions()[0].GetBlobId().GetValue())

	// And clearing it removes it.
	require.NoError(t, s.SetGroupPicture(e.ctx, withPicture, nil))
	resp = e.getChat(e.keys, withPicture)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Nil(t, resp.Metadata.GetPicture())
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
	_, _, err := s.RemoveGroupMember(e.ctx, chatID, e.userID)
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
		PictureBlobID:          pictureBlobID,
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
	require.Len(t, md.GetPicture().GetRenditions(), len(wantPicture))
	require.NotEmpty(t, md.GetPicture().GetRenditions()[0].GetBlob().GetDownloadUrl().GetUrl())
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

	// A group with no listener rules shows a non-member its record alone,
	// however funded: only a rule can admit one to the rest (see chat.Access).
	strangerID, strangerKeys := e.addUser()
	_, err = e.accounts.Bind(e.ctx, strangerID, strangerKeys.Proto())
	require.NoError(t, err)
	e.ocpBalance.setBalance(strangerKeys.Proto(), ocp_common.ToCoreMintQuarks(requirement))
	open := &chat.Chat{
		ID:            chat.MustGenerateGroupChatID(),
		Type:          chatpb.ChatType_GROUP,
		Members:       []*commonpb.UserId{founder},
		Title:         "Legacy",
		LastActivity:  at(1),
		LastMessageID: &messagingpb.MessageId{Value: 2},
	}
	require.NoError(t, s.PutChat(e.ctx, open))
	e.messaging.lastMessages[string(open.ID.Value)] = textMessage(2, founder, "members only")
	e.messaging.latestEventSeqs[string(open.ID.Value)] = 2
	resp = e.getChat(strangerKeys, open.ID)
	require.Equal(t, chatpb.GetChatResponse_OK, resp.Result)
	require.Equal(t, "Legacy", resp.Metadata.Title)
	require.Nil(t, resp.Metadata.Rules)
	require.Empty(t, resp.Metadata.Members)
	require.Nil(t, resp.Metadata.LastMessage)
	require.Zero(t, resp.Metadata.LatestEventSequence)

	// A DM is unchanged: a non-member is denied outright, funded or not.
	dm := e.putDM(at(1))
	resp = e.getChat(strangerKeys, dm)
	require.Equal(t, chatpb.GetChatResponse_DENIED, resp.Result)
	require.Nil(t, resp.Metadata)
}

func testServer_GetDmChatFeed_TypeScoped(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	contactID := e.putDMOfType(chatpb.ChatType_CONTACT_DM, at(1))
	tipID := e.putDMOfType(chatpb.ChatType_TIP_DM, at(2))

	contactResp, err := e.getDmFeedOfType(chatpb.ChatType_CONTACT_DM, &commonpb.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, chatpb.GetDmChatFeedResponse_OK, contactResp.Result)
	require.Len(t, contactResp.Chats, 1)
	require.Equal(t, contactID.Value, contactResp.Chats[0].ChatId.Value)
	require.Equal(t, chatpb.ChatType_CONTACT_DM, contactResp.Chats[0].Type)

	tipResp, err := e.getDmFeedOfType(chatpb.ChatType_TIP_DM, &commonpb.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, chatpb.GetDmChatFeedResponse_OK, tipResp.Result)
	require.Len(t, tipResp.Chats, 1)
	require.Equal(t, tipID.Value, tipResp.Chats[0].ChatId.Value)
	require.Equal(t, chatpb.ChatType_TIP_DM, tipResp.Chats[0].Type)
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
	_, err = e.getDmFeedOfType(chatpb.ChatType_TIP_DM, &commonpb.QueryOptions{PageSize: 1, PagingToken: resp.PagingToken})
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
		ID:            withMsg,
		Type:          chatpb.ChatType_GROUP,
		Members:       []*commonpb.UserId{e.userID, model.MustGenerateUserID()},
		Title:         "With Message",
		PictureBlobID: pictureBlobID,
		LastActivity:  at(2),
		LastMessageID: &messagingpb.MessageId{Value: 3},
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
	require.NotNil(t, hydrated.Picture)
	require.Len(t, hydrated.Picture.Renditions, len(renditions))
	require.NotNil(t, hydrated.Picture.Renditions[0].Blob)

	bare := byChat[string(withoutMsg.Value)]
	require.Nil(t, bare.LastMessage)
	require.Zero(t, bare.LatestEventSequence)
	require.Nil(t, bare.Picture)

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

	_, _, err := s.RemoveGroupMember(e.ctx, g2, e.userID)
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

	// The join has landed in the store...
	isMember, err := s.IsMember(e.ctx, group.ID, e.userID)
	require.NoError(t, err)
	require.True(t, isMember)
	require.Equal(t, chatpb.GetChatResponse_OK, e.getChat(e.keys, group.ID).Result)

	// ...and been announced. The chat's topic carries the joiner's hydrated
	// member entry — profile, no pointers, no metadata — with the joiner
	// excluded, since their streams are not on the topic yet.
	e.waitForRosterUpdates(e.userID, group.ID, 1)
	toMembers, excludes := e.rosterUpdatesOnChatTopic(group.ID)
	require.Len(t, toMembers, 1)
	require.Len(t, excludes[0], 1)
	require.Equal(t, e.userID.Value, excludes[0][0].Value)
	joined := toMembers[0].GetMemberJoined()
	require.NotNil(t, joined)
	require.Equal(t, e.userID.Value, joined.Member.UserId.Value)
	require.Equal(t, "Joiner", joined.Member.UserProfile.DisplayName)
	require.True(t, joined.Member.UserProfile.JoinTs.AsTime().Equal(at(5)))
	require.Empty(t, joined.Member.Pointers)
	require.Nil(t, joined.Metadata)
	require.NoError(t, protoutil.ProtoEqualError(&chatpb.RosterSummary{MemberCount: 3, Version: 1}, toMembers[0].GetRosterSummary()))

	// The joiner's own topic carries the same member plus the full metadata,
	// so their other devices insert the chat without a refetch.
	toJoiner := e.rosterUpdatesOnUserTopic(e.userID, group.ID)
	require.Len(t, toJoiner, 1)
	joinedSelf := toJoiner[0].GetMemberJoined()
	require.NotNil(t, joinedSelf)
	require.Equal(t, e.userID.Value, joinedSelf.Member.UserId.Value)
	require.Equal(t, "Joiner", joinedSelf.Member.UserProfile.DisplayName)
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

	// One quark short is still short.
	_, err := e.accounts.Bind(e.ctx, e.userID, e.keys.Proto())
	require.NoError(t, err)
	e.ocpBalance.setBalance(e.keys.Proto(), ocp_common.ToCoreMintQuarks(requirement)-1)
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
// evaluate fails the join rather than admitting the user: a restriction the
// server cannot answer admits no one.
func testServer_JoinChat_UnevaluableRuleFails(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	group := &chat.Chat{
		ID:                     chat.MustGenerateGroupChatID(),
		Type:                   chatpb.ChatType_GROUP,
		Members:                []*commonpb.UserId{model.MustGenerateUserID()},
		Title:                  "Euros",
		MinimumListenerBalance: &chat.MinimumBalance{Currency: "eur", NativeAmount: 1},
		LastActivity:           at(1),
	}
	require.NoError(t, s.PutChat(e.ctx, group))

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
	// — their stream may still be on it — and on the leaver's own topic, so
	// every device they have open drops the chat.
	e.waitForRosterUpdates(e.userID, chatID, 1)
	toMembers, excludes := e.rosterUpdatesOnChatTopic(chatID)
	require.Len(t, toMembers, 1)
	require.Len(t, excludes[0], 1)
	require.Equal(t, e.userID.Value, excludes[0][0].Value)
	left := toMembers[0].GetMemberLeft()
	require.NotNil(t, left)
	require.Equal(t, e.userID.Value, left.UserId.Value)
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

	// Two transitions, two announcements on each topic, in order.
	e.waitForRosterUpdates(e.userID, chatID, 2)
	toMembers, _ := e.rosterUpdatesOnChatTopic(chatID)
	require.Len(t, toMembers, 2)
	require.NotNil(t, toMembers[0].GetMemberLeft())
	require.Equal(t, uint64(1), toMembers[0].GetRosterSummary().GetVersion())
	require.NotNil(t, toMembers[1].GetMemberJoined())
	require.Equal(t, uint64(2), toMembers[1].GetRosterSummary().GetVersion())
}

// fakeModerator is a canned moderation.Client for server tests. Only the two
// classifiers a chat title runs through do anything; the rest satisfy the
// interface and never flag.
type fakeModerator struct {
	textFlagged    bool
	textCategories []string
	textErr        error

	titleFlagged    bool
	titleCategories []string
	titleErr        error

	// classifiedTitle records the title ClassifyGroupTitle last saw, so a test
	// can check what reached the classifier.
	classifiedTitle string
}

func (m *fakeModerator) ClassifyText(context.Context, string) (*moderation.Result, error) {
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
func (e *serverEnv) startGroupChat(keys model.KeyPair, params *chatpb.StartChatRequest_GroupChatParameters) (*chatpb.StartChatResponse, error) {
	return e.startGroupChatWithKey(keys, newIdempotencyKey(), params)
}

func (e *serverEnv) startGroupChatWithKey(keys model.KeyPair, key *chatpb.IdempotencyKey, params *chatpb.StartChatRequest_GroupChatParameters) (*chatpb.StartChatResponse, error) {
	req := &chatpb.StartChatRequest{
		Parameters:     &chatpb.StartChatRequest_Group{Group: params},
		IdempotencyKey: key,
	}
	require.NoError(e.t, keys.Auth(req, &req.Auth))
	return e.client.StartChat(e.ctx, req)
}

func (e *serverEnv) mustStartGroupChat(keys model.KeyPair, params *chatpb.StartChatRequest_GroupChatParameters) *chatpb.StartChatResponse {
	resp, err := e.startGroupChat(keys, params)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) mustStartGroupChatWithKey(keys model.KeyPair, key *chatpb.IdempotencyKey, params *chatpb.StartChatRequest_GroupChatParameters) *chatpb.StartChatResponse {
	resp, err := e.startGroupChatWithKey(keys, key, params)
	require.NoError(e.t, err)
	return resp
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

// groupParams builds StartChat parameters for a group with the given title and
// the default minimum balance rule.
func groupParams(title string) *chatpb.StartChatRequest_GroupChatParameters {
	return &chatpb.StartChatRequest_GroupChatParameters{
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
	// as its only member, and a roster of one at version zero.
	md := resp.Chat
	require.NotNil(t, md)
	require.True(t, chat.IsGroupChatID(md.ChatId))
	require.Equal(t, chatpb.ChatType_GROUP, md.Type)
	require.Equal(t, "Sunday Hikers", md.Title)
	require.NoError(t, protoutil.ProtoEqualError(groupParams("Sunday Hikers").Rules, md.Rules))
	require.Nil(t, md.Picture)
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
	require.Nil(t, stored.PictureBlobID)
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
			Parameters:     &chatpb.StartChatRequest_Group{Group: groupParams("Keyless")},
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
	params.Picture = pictureBlobID
	resp := e.mustStartGroupChat(e.keys, params)
	require.Equal(t, chatpb.StartChatResponse_OK, resp.Result)
	md := resp.Chat

	// The picture was attached against the new group's ID before the record
	// was written, and comes back hydrated with its full rendition set.
	require.Equal(t, pictureBlobID.Value, e.media.chatPictures[string(md.ChatId.Value)].GetValue())
	require.NotNil(t, md.Picture)
	require.Len(t, md.Picture.Renditions, len(renditions))
	require.Equal(t, pictureBlobID.Value, md.Picture.Renditions[0].GetBlobId().GetValue())
	require.NotNil(t, md.Picture.Renditions[0].Blob)

	stored, err := s.GetChatByID(e.ctx, md.ChatId)
	require.NoError(t, err)
	require.Equal(t, pictureBlobID.Value, stored.PictureBlobID.GetValue())
}

func testServer_StartChat_PictureNotAccepted(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)
	e.fundEnvUser(startChatMinimumBalance)

	// A blob the media domain will not attach — unknown, not the caller's, not
	// READY, or not an image — refuses the whole creation: no group is written.
	params := groupParams("Picture Group")
	params.Picture = &blobpb.BlobId{Value: []byte("not-attachable01")}
	resp := e.mustStartGroupChat(e.keys, params)
	require.Equal(t, chatpb.StartChatResponse_PICTURE_BLOB_NOT_ACCEPTED, resp.Result)
	require.Nil(t, resp.Chat)

	groups, err := s.GetGroupChatsForUser(e.ctx, e.userID)
	require.NoError(t, err)
	require.Empty(t, groups)
	require.Empty(t, e.media.chatPictures)

	// A blob domain that cannot attach at all is the server's fault, not the
	// picture's: the RPC fails rather than telling the client to pick another.
	e.media.setAttachable(&blobpb.BlobId{Value: []byte("group-picture-01")})
	e.media.attachErr = errors.New("blob store down")
	params.Picture = &blobpb.BlobId{Value: []byte("group-picture-01")}
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

func testServer_StartChat_InvalidRules(t *testing.T, s chat.Store) {
	e := newServerEnv(t, s)

	// The creator satisfies every valid rule below, so a refusal is for the
	// rules' shape alone.
	e.accounts.setStaff(e.userID, true)
	e.fundEnvUser(startChatMinimumBalance)

	staff := &chatpb.ListenerRules{Kind: &chatpb.ListenerRules_Staff{Staff: &chatpb.StaffRequirement{}}}
	minimumBalance := minimumBalanceRule("usd", startChatMinimumBalance)

	for name, rules := range map[string]*chatpb.Rules{
		// Every group must carry a minimum listener balance.
		"no rules":   nil,
		"empty":      {},
		"staff only": {Listener: []*chatpb.ListenerRules{staff}},
		// No group carries a speaker rule yet, so none can be asked for.
		"speaker rule": {
			Listener: []*chatpb.ListenerRules{minimumBalance},
			Speaker:  []*chatpb.SpeakerRules{{Kind: &chatpb.SpeakerRules_Staff{Staff: &chatpb.StaffRequirement{}}}},
		},
		// The record holds one requirement of each kind.
		"duplicate staff":           {Listener: []*chatpb.ListenerRules{staff, staff, minimumBalance}},
		"duplicate minimum balance": {Listener: []*chatpb.ListenerRules{minimumBalance, minimumBalanceRule("usd", 1)}},
		// Only a USD requirement can be evaluated, and only one of at least the
		// currency's minimum transfer value — a penny — requires anything.
		// (A negative amount never reaches the server: the proto validator
		// refuses it as an invalid request.)
		"non-usd minimum balance":      {Listener: []*chatpb.ListenerRules{minimumBalanceRule("eur", 1)}},
		"zero minimum balance":         {Listener: []*chatpb.ListenerRules{minimumBalanceRule("usd", 0)}},
		"sub-penny minimum balance":    {Listener: []*chatpb.ListenerRules{minimumBalanceRule("usd", 0.009)}},
		"half-penny minimum balance":   {Listener: []*chatpb.ListenerRules{minimumBalanceRule("usd", 0.005)}},
		"not-a-number minimum balance": {Listener: []*chatpb.ListenerRules{minimumBalanceRule("usd", math.NaN())}},
	} {
		t.Run(name, func(t *testing.T) {
			resp := e.mustStartGroupChat(e.keys, &chatpb.StartChatRequest_GroupChatParameters{Title: "Ruled", Rules: rules})
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

	// ...as is one a quark short.
	_, err := e.accounts.Bind(e.ctx, e.userID, e.keys.Proto())
	require.NoError(t, err)
	e.ocpBalance.setBalance(e.keys.Proto(), ocp_common.ToCoreMintQuarks(startChatMinimumBalance)-1)
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

	resp := e.mustStartGroupChat(e.keys, &chatpb.StartChatRequest_GroupChatParameters{Title: "Staff Whales", Rules: rules})
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
