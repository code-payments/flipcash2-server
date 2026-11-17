package flipcashteam_test

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	account_memory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/blob"
	blob_memory "github.com/code-payments/flipcash2-server/blob/memory"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/flipcashteam"
	"github.com/code-payments/flipcash2-server/model"
	ocp_client "github.com/code-payments/ocp-server/grpc/client"
	ocp_headers "github.com/code-payments/ocp-server/grpc/headers"
)

func TestSendWelcomeDmV4(t *testing.T) {
	e := newChatEnv(t)

	const expected = "Welcome to Flipcash, the group money app. If someone sent you a group link, tap or scan it again to join the group"

	chatID, sent, err := flipcashteam.SendWelcomeDmV4(e.ctx, e.chats, e.sender, e.user)
	require.NoError(t, err)
	require.Equal(t, chat.MustDeriveDmChatID(chatpb.ChatType_DM, e.team, e.user).Value, chatID.Value)
	require.Len(t, sent, 1)
	require.Equal(t, uint64(1), sent[0].MessageId.Value)
	require.Equal(t, e.team.Value, sent[0].SenderId.Value)
	require.Equal(t, expected, sent[0].Content[0].GetText().Text)

	// A retry returns the first welcome and sends nothing new.
	_, replayed, err := flipcashteam.SendWelcomeDmV4(e.ctx, e.chats, e.sender, e.user)
	require.NoError(t, err)
	require.Len(t, replayed, 1)
	require.Equal(t, sent[0].MessageId.Value, replayed[0].MessageId.Value)
	require.Equal(t, expected, replayed[0].Content[0].GetText().Text)
	stored, err := e.messages.GetMessages(e.ctx, chatID)
	require.NoError(t, err)
	require.Len(t, stored, 1)
}

func TestDmWelcomer(t *testing.T) {
	e := newChatEnv(t)
	welcomer := flipcashteam.NewDmWelcomer(zap.NewNop(), e.chats, e.sender)
	chatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, e.team, e.user)

	// The request that assigned the handle may be gone by the time the welcome
	// is sent; it is sent all the same. Told twice, as racing claims would, it
	// is sent once.
	ctx, cancel := context.WithCancel(withUserAgent(t, e.ctx, "Flipcash/iOS/2026.9.5"))
	cancel()
	welcomer.OnFirstUsername(ctx, e.user, "alice_1")
	welcomer.OnFirstUsername(ctx, e.user, "alice_1")

	stored := func() int {
		msgs, err := e.messages.GetMessages(e.ctx, chatID)
		require.NoError(t, err)
		return len(msgs)
	}
	require.Eventually(t, func() bool { return stored() == 1 }, time.Second, 10*time.Millisecond)
	require.Never(t, func() bool { return stored() != 1 }, 200*time.Millisecond, 10*time.Millisecond)
}

// TestDmWelcomer_MinClientVersion: a welcome goes only to a user whose handle
// was assigned from a client that renders it, on iOS and Android alike; an
// older or unidentifiable client gets none.
func TestDmWelcomer_MinClientVersion(t *testing.T) {
	for _, tc := range []struct {
		userAgent string
		welcomed  bool
	}{
		{"Flipcash/iOS/2026.9.5", true},
		{"Flipcash/Android/2026.9.5", true},
		{"Flipcash/iOS/2026.10.0", true},
		{"Flipcash/Android/2027.1.0", true},
		{"Flipcash/iOS/2026.9.4", false},
		{"Flipcash/Android/2026.8.9", false},
		{"Mozilla/5.0", false},
		{"", false},
	} {
		t.Run(tc.userAgent, func(t *testing.T) {
			e := newChatEnv(t)
			welcomer := flipcashteam.NewDmWelcomer(zap.NewNop(), e.chats, e.sender)
			chatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, e.team, e.user)

			ctx := e.ctx
			if tc.userAgent != "" {
				ctx = withUserAgent(t, ctx, tc.userAgent)
			}
			welcomer.OnFirstUsername(ctx, e.user, "alice_1")

			stored := func() int {
				msgs, err := e.messages.GetMessages(e.ctx, chatID)
				require.NoError(t, err)
				return len(msgs)
			}
			if tc.welcomed {
				require.Eventually(t, func() bool { return stored() == 1 }, time.Second, 10*time.Millisecond)
			} else {
				require.Never(t, func() bool { return stored() != 0 }, 200*time.Millisecond, 10*time.Millisecond)
			}
		})
	}
}

// withUserAgent returns ctx carrying userAgent as the inbound user-agent
// header, as the headers interceptor leaves it on a request's context.
func withUserAgent(t *testing.T, ctx context.Context, userAgent string) context.Context {
	ctx, err := ocp_headers.ContextWithHeaders(ctx)
	require.NoError(t, err)
	require.NoError(t, ocp_headers.SetASCIIHeader(ctx, ocp_client.UserAgentHeaderName, userAgent))
	return ctx
}

// welcomerEnv is a groupEnv whose welcome group picture is a READY image the
// team owns, in a real blob.Integration over memory stores.
type welcomerEnv struct {
	*groupEnv
	blobs       blob.Store
	access      blob.AccessStore
	integration *blob.Integration
}

func newWelcomerEnv(t *testing.T) *welcomerEnv {
	e := &welcomerEnv{groupEnv: newGroupEnv(t)}
	e.blobs = blob_memory.NewInMemory()
	e.access = blob_memory.NewInMemoryAccessStore()
	e.integration = blob.NewIntegration(e.blobs, blob_memory.NewInMemoryStorage(), e.access)
	e.cfg.ProfilePictureBlobID = e.putPicture(t, e.team, true)
	return e
}

// putPicture stores an image original owned by owner, READY if ready and
// pending otherwise.
func (e *welcomerEnv) putPicture(t *testing.T, owner *commonpb.UserId, ready bool) *blobpb.BlobId {
	id := &blobpb.BlobId{Value: make([]byte, 16)}
	_, err := rand.Read(id.Value)
	require.NoError(t, err)
	require.NoError(t, e.blobs.CreatePending(e.ctx, &blob.Blob{
		ID:         id,
		Rendition:  blob.RenditionOriginal,
		Owner:      owner,
		State:      blob.StatePending,
		StorageKey: "images/x/original.png",
		MimeType:   "image/png",
		SizeBytes:  1,
	}))
	if ready {
		_, err = e.blobs.Advance(e.ctx, id, blob.StateReady, nil)
		require.NoError(t, err)
	}
	return id
}

func (e *welcomerEnv) isPublic(t *testing.T, id *blobpb.BlobId) bool {
	has, err := e.access.HasGrant(e.ctx, id, blob.PrincipalPublic(), blob.PermissionRead)
	require.NoError(t, err)
	return has
}

func (e *welcomerEnv) newWelcomer(t *testing.T) (*flipcashteam.GroupWelcomer, error) {
	return flipcashteam.NewGroupWelcomer(e.ctx, zaptest.NewLogger(t), e.chats, e.accounts, e.profiles, e.integration, e.sender, e.cfg)
}

// welcomeGroupMessages counts the messages in the user's welcome group.
func (e *welcomerEnv) welcomeGroupMessages(t *testing.T) int {
	msgs, err := e.messages.GetMessages(e.ctx, expectedWelcomeGroupID(e.team, e.user))
	require.NoError(t, err)
	return len(msgs)
}

func TestNewGroupWelcomer(t *testing.T) {
	e := newWelcomerEnv(t)
	require.False(t, e.isPublic(t, e.cfg.ProfilePictureBlobID))

	welcomer, err := e.newWelcomer(t)
	require.NoError(t, err)
	require.NotNil(t, welcomer)

	// The picture is readable by everyone.
	require.True(t, e.isPublic(t, e.cfg.ProfilePictureBlobID))

	// Built again, as on every start, the grant is written again harmlessly.
	again, err := e.newWelcomer(t)
	require.NoError(t, err)
	require.NotNil(t, again)
	require.True(t, e.isPublic(t, e.cfg.ProfilePictureBlobID))
}

func TestNewGroupWelcomer_Refused(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(t *testing.T, e *welcomerEnv)
		wantErr error
	}{
		{
			name: "bad config",
			mutate: func(_ *testing.T, e *welcomerEnv) {
				e.cfg.GroupKeySecret = e.cfg.GroupKeySecret[:31]
			},
		},
		{
			name: "key pair not the team's",
			mutate: func(_ *testing.T, e *welcomerEnv) {
				e.cfg.TeamKeyPair = model.MustGenerateKeyPair()
			},
		},
		{
			name: "team with no keys",
			mutate: func(t *testing.T, e *welcomerEnv) {
				e.accounts = account_memory.NewInMemory()
				_, err := e.accounts.Bind(e.ctx, e.user, e.userKeys.Proto())
				require.NoError(t, err)
			},
		},
		{
			name: "team with no display name",
			mutate: func(t *testing.T, e *welcomerEnv) {
				require.NoError(t, e.profiles.SetDisplayName(e.ctx, e.team, ""))
			},
		},
		{
			name: "picture missing",
			mutate: func(_ *testing.T, e *welcomerEnv) {
				e.cfg.ProfilePictureBlobID = &blobpb.BlobId{Value: make([]byte, 16)}
			},
			wantErr: blob.ErrBlobNotFound,
		},
		{
			name: "picture not ready",
			mutate: func(t *testing.T, e *welcomerEnv) {
				e.cfg.ProfilePictureBlobID = e.putPicture(t, e.team, false)
			},
			wantErr: blob.ErrBlobNotReady,
		},
		{
			name: "picture owned by someone else",
			mutate: func(t *testing.T, e *welcomerEnv) {
				e.cfg.ProfilePictureBlobID = e.putPicture(t, e.user, true)
			},
			wantErr: blob.ErrBlobNotFound,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newWelcomerEnv(t)
			tc.mutate(t, e)

			welcomer, err := e.newWelcomer(t)
			require.Error(t, err)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			}
			require.Nil(t, welcomer)

			// Nothing was made public.
			require.False(t, e.isPublic(t, e.cfg.ProfilePictureBlobID))
		})
	}
}

func TestNewGroupWelcomer_NoTeamAccount(t *testing.T) {
	// A process started before the team account was set up has none.
	ce := newChatEnvWithTeam(t, false)
	require.NoError(t, flipcashteam.AssignUsername(ce.ctx, ce.profiles, ce.team))
	require.NoError(t, ce.profiles.SetDisplayName(ce.ctx, ce.team, "Flipcash"))
	teamKeys := model.MustGenerateKeyPair()
	accounts := account_memory.NewInMemory()
	_, err := accounts.Bind(ce.ctx, ce.team, teamKeys.Proto())
	require.NoError(t, err)
	e := &welcomerEnv{groupEnv: &groupEnv{chatEnv: ce, accounts: accounts, cfg: newWelcomeGroupConfig(t, teamKeys), teamKeys: teamKeys}}
	e.blobs = blob_memory.NewInMemory()
	e.access = blob_memory.NewInMemoryAccessStore()
	e.integration = blob.NewIntegration(e.blobs, blob_memory.NewInMemoryStorage(), e.access)
	e.cfg.ProfilePictureBlobID = e.putPicture(t, e.team, true)

	welcomer, err := e.newWelcomer(t)
	require.ErrorIs(t, err, flipcashteam.ErrNoTeamAccount)
	require.Nil(t, welcomer)
	require.False(t, e.isPublic(t, e.cfg.ProfilePictureBlobID))
}

func TestGroupWelcomer(t *testing.T) {
	e := newWelcomerEnv(t)
	welcomer, err := e.newWelcomer(t)
	require.NoError(t, err)

	// The welcomer keeps its own copy of the config: a later change to the
	// caller's secret is never seen.
	secret := append([]byte(nil), e.cfg.GroupKeySecret...)
	clear(e.cfg.GroupKeySecret)

	// The request that assigned the handle may be gone by the time the welcome
	// is sent; it is sent all the same. Told twice, as racing claims would, it
	// is sent once.
	ctx, cancel := context.WithCancel(withUserAgent(t, e.ctx, "Flipcash/iOS/2026.10.3"))
	cancel()
	welcomer.OnFirstUsername(ctx, e.user, "alice_1")
	welcomer.OnFirstUsername(ctx, e.user, "alice_1")

	require.Eventually(t, func() bool { return e.welcomeGroupMessages(t) == len(welcomeV4Texts) }, time.Second, 10*time.Millisecond)
	require.Never(t, func() bool { return e.welcomeGroupMessages(t) != len(welcomeV4Texts) }, 200*time.Millisecond, 10*time.Millisecond)

	// What was sent is the encrypted welcome, in the user's welcome group: a
	// send under the secret the welcomer was built with replays it.
	e.cfg.GroupKeySecret = secret
	chatID, replayed, err := e.sendWelcome()
	require.NoError(t, err)
	require.Equal(t, expectedWelcomeGroupID(e.team, e.user).Value, chatID.Value)
	groupKey := e.openGroupKey(t, chatID)
	e.requireWelcome(t, chatID, groupKey, replayed)
	require.Equal(t, len(welcomeV4Texts), e.welcomeGroupMessages(t))

	// The group's key is the one derived from that secret, not from the
	// caller's cleared copy: a new message sent under it decrypts with the
	// user's envelope.
	_, more, err := flipcashteam.SendWelcomeGroupMessages(e.ctx, e.chats, e.accounts, e.sender, e.cfg, e.user, "follow-up", []*messagingpb.Content{text("again")})
	require.NoError(t, err)
	content, err := flipcashteam.DecryptGroupContent(groupKey, chatID, e.team, more[0].Content[0].GetEncrypted())
	require.NoError(t, err)
	require.Equal(t, "again", content.GetText().Text)

	// The DM welcome is not sent.
	_, err = e.chats.GetChatByID(e.ctx, chat.MustDeriveDmChatID(chatpb.ChatType_DM, e.team, e.user))
	require.ErrorIs(t, err, chat.ErrChatNotFound)
}

// TestGroupWelcomer_UserLeft: a user who has already left their welcome group
// is sent nothing, and that is expected, logged at Debug rather than as a
// failure.
func TestGroupWelcomer_UserLeft(t *testing.T) {
	e := newWelcomerEnv(t)
	core, logs := observer.New(zapcore.DebugLevel)
	welcomer, err := flipcashteam.NewGroupWelcomer(e.ctx, zap.New(core), e.chats, e.accounts, e.profiles, e.integration, e.sender, e.cfg)
	require.NoError(t, err)

	chatID, _, err := flipcashteam.SendWelcomeGroupMessages(e.ctx, e.chats, e.accounts, e.sender, e.cfg, e.user, "earlier", []*messagingpb.Content{text("hi")})
	require.NoError(t, err)
	_, _, err = e.chats.RemoveGroupMember(e.ctx, chatID, e.user, true)
	require.NoError(t, err)

	welcomer.OnFirstUsername(withUserAgent(t, e.ctx, "Flipcash/iOS/2026.10.3"), e.user, "alice_1")

	require.Eventually(t, func() bool {
		return logs.FilterMessage("User has left their welcome group").FilterLevelExact(zapcore.DebugLevel).Len() == 1
	}, time.Second, 10*time.Millisecond)
	require.Zero(t, logs.FilterLevelExact(zapcore.WarnLevel).Len())
	require.Equal(t, 1, e.welcomeGroupMessages(t))
}

// TestGroupWelcomer_MinClientVersion: a welcome goes only to a user whose
// handle was assigned from a client at or above the gate, on iOS and Android
// alike; an older or unidentifiable client gets none.
func TestGroupWelcomer_MinClientVersion(t *testing.T) {
	for _, tc := range []struct {
		userAgent string
		welcomed  bool
	}{
		{"Flipcash/iOS/2026.10.3", true},
		{"Flipcash/Android/2026.10.3", true},
		{"Flipcash/Android/2026.11.0", true},
		{"Flipcash/iOS/2027.1.0", true},
		{"Flipcash/iOS/2026.10.2", false},
		{"Flipcash/Android/2026.10.2", false},
		{"Flipcash/iOS/2026.9.5", false},
		{"Mozilla/5.0", false},
		{"", false},
	} {
		t.Run(tc.userAgent, func(t *testing.T) {
			e := newWelcomerEnv(t)
			welcomer, err := e.newWelcomer(t)
			require.NoError(t, err)

			ctx := e.ctx
			if tc.userAgent != "" {
				ctx = withUserAgent(t, ctx, tc.userAgent)
			}
			welcomer.OnFirstUsername(ctx, e.user, "alice_1")

			if tc.welcomed {
				require.Eventually(t, func() bool { return e.welcomeGroupMessages(t) == len(welcomeV4Texts) }, time.Second, 10*time.Millisecond)
				return
			}
			require.Never(t, func() bool {
				_, err := e.chats.GetChatByID(e.ctx, expectedWelcomeGroupID(e.team, e.user))
				return err == nil
			}, 200*time.Millisecond, 10*time.Millisecond)
			require.Zero(t, e.welcomeGroupMessages(t))
			require.Never(t, func() bool { return len(e.pusher.snapshot()) > 0 }, 50*time.Millisecond, 10*time.Millisecond)
		})
	}
}
