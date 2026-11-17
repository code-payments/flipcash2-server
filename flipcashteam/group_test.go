package flipcashteam_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"

	"github.com/code-payments/flipcash2-server/account"
	account_memory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/flipcashteam"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/push"
)

// recordedPush is one push the recordingPusher was asked to send.
type recordedPush struct {
	title, body string
	users       []*commonpb.UserId
}

// recordingPusher records every push it is asked to send.
type recordingPusher struct {
	mu     sync.Mutex
	pushes []recordedPush
}

func (p *recordingPusher) SendPushes(_ context.Context, title, body string, _ *pushpb.Payload, users ...*commonpb.UserId) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pushes = append(p.pushes, recordedPush{title: title, body: body, users: users})
	return nil
}

func (p *recordingPusher) SendPushesWithBadges(ctx context.Context, title, body string, payload *pushpb.Payload, _ push.BadgeResolver, users ...*commonpb.UserId) error {
	return p.SendPushes(ctx, title, body, payload, users...)
}

func (p *recordingPusher) snapshot() []recordedPush {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]recordedPush(nil), p.pushes...)
}

// pushedTo counts the recorded pushes addressed to userID.
func (p *recordingPusher) pushedTo(userID *commonpb.UserId) int {
	var n int
	for _, recorded := range p.snapshot() {
		for _, u := range recorded.users {
			if bytes.Equal(u.Value, userID.Value) {
				n++
			}
		}
	}
	return n
}

type groupEnv struct {
	*chatEnv
	accounts account.Store
	pusher   *recordingPusher
	cfg      *flipcashteam.WelcomeGroupConfig
	teamKeys model.KeyPair
	userKeys model.KeyPair
}

// newGroupEnv is a chatEnv whose team and user are registered accounts, the
// team with a display name (which a group push renders), and a welcome group
// configuration for the team.
func newGroupEnv(t *testing.T) *groupEnv {
	pusher := &recordingPusher{}
	e := newChatEnvWithPusher(t, true, pusher)
	accounts := account_memory.NewInMemory()
	teamKeys, userKeys := model.MustGenerateKeyPair(), model.MustGenerateKeyPair()
	_, err := accounts.Bind(e.ctx, e.team, teamKeys.Proto())
	require.NoError(t, err)
	_, err = accounts.Bind(e.ctx, e.user, userKeys.Proto())
	require.NoError(t, err)
	require.NoError(t, e.profiles.SetDisplayName(e.ctx, e.team, "Flipcash"))

	return &groupEnv{
		chatEnv:  e,
		accounts: accounts,
		pusher:   pusher,
		cfg:      newWelcomeGroupConfig(t, teamKeys),
		teamKeys: teamKeys,
		userKeys: userKeys,
	}
}

func newWelcomeGroupConfig(t *testing.T, teamKeys model.KeyPair) *flipcashteam.WelcomeGroupConfig {
	secret := make([]byte, 32)
	_, err := rand.Read(secret)
	require.NoError(t, err)
	picture := make([]byte, 16)
	_, err = rand.Read(picture)
	require.NoError(t, err)
	return &flipcashteam.WelcomeGroupConfig{
		TeamKeyPair:          teamKeys,
		GroupKeySecret:       secret,
		ProfilePictureBlobID: &blobpb.BlobId{Value: picture},
	}
}

func (e *groupEnv) sendWelcome() (*commonpb.ChatId, []*messagingpb.Message, error) {
	return flipcashteam.SendWelcomeGroupV4(e.ctx, e.chats, e.accounts, e.sender, e.cfg, e.user)
}

// expectedWelcomeGroupID derives the welcome group's ID from the formula
// documented on welcomeGroupID, independently of it, so the formula is
// pinned: it must never change.
func expectedWelcomeGroupID(team, user *commonpb.UserId) *commonpb.ChatId {
	digest := sha256.Sum256(append([]byte("flipcash-team-welcome-group"), user.Value...))
	return chat.MustDeriveGroupChatID(team, &chatpb.IdempotencyKey{Value: digest[:16]})
}

// openGroupKey opens the user's stored envelope for chatID as the user,
// against the team's public key.
func (e *groupEnv) openGroupKey(t *testing.T, chatID *commonpb.ChatId) []byte {
	envelope, err := e.chats.GetKeyEnvelope(e.ctx, chatID, e.user)
	require.NoError(t, err)
	require.Equal(t, e.team.Value, envelope.WrappedBy.Value)
	groupKey, err := flipcashteam.OpenGroupKey(e.userKeys, e.teamKeys.Public(), chatID, envelope.ToProto())
	require.NoError(t, err)
	return groupKey
}

// welcomeV4Texts is the fourth welcome's text, as TestSendWelcomeDmV4 pins it.
var welcomeV4Texts = []string{
	"Welcome to Flipcash, the group money app. If someone sent you a group link, tap or scan it again to join the group",
}

// requireWelcome checks that msgs are the fourth welcome, encrypted under
// groupKey by the team.
func (e *groupEnv) requireWelcome(t *testing.T, chatID *commonpb.ChatId, groupKey []byte, msgs []*messagingpb.Message) {
	require.Len(t, msgs, len(welcomeV4Texts))
	for i, msg := range msgs {
		require.Equal(t, e.team.Value, msg.SenderId.Value)
		require.Len(t, msg.Content, 1)
		encrypted := msg.Content[0].GetEncrypted()
		require.NotNil(t, encrypted)
		require.Equal(t, messagingpb.EncryptedContent_CHAT_KEY_XCHACHA20POLY1305, encrypted.Scheme)
		content, err := flipcashteam.DecryptGroupContent(groupKey, chatID, msg.SenderId, encrypted)
		require.NoError(t, err)
		require.True(t, proto.Equal(&messagingpb.Content{Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: welcomeV4Texts[i]}}}, content))
	}
}

func TestSendWelcomeGroupV4(t *testing.T) {
	e := newGroupEnv(t)

	chatID, sent, err := e.sendWelcome()
	require.NoError(t, err)

	// The group is the user's welcome group, as the team created it.
	require.Equal(t, expectedWelcomeGroupID(e.team, e.user).Value, chatID.Value)
	record, err := e.chats.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, chatpb.ChatType_GROUP, record.Type)
	require.Equal(t, flipcashteam.WelcomeGroupTitle, record.Title)
	require.Equal(t, "Welcome Party", record.Title)
	require.True(t, record.IsPrivate)
	require.Equal(t, e.team.Value, record.CreatorID.Value)
	require.True(t, proto.Equal(e.cfg.ProfilePictureBlobID, record.ProfilePictureBlobID))
	members, err := e.chats.GetMembers(e.ctx, chatID)
	require.NoError(t, err)
	require.Len(t, members, 2)
	for _, member := range []*commonpb.UserId{e.team, e.user} {
		isMember, err := e.chats.IsMember(e.ctx, chatID, member)
		require.NoError(t, err)
		require.True(t, isMember)
	}

	// The user holds an envelope, wrapped by the team, that opens to the
	// key the welcome is encrypted under; the team holds none.
	groupKey := e.openGroupKey(t, chatID)
	_, err = e.chats.GetKeyEnvelope(e.ctx, chatID, e.team)
	require.ErrorIs(t, err, chat.ErrKeyEnvelopeNotFound)

	// The welcome is the fourth version's, encrypted, from the team, and is
	// what the store holds.
	e.requireWelcome(t, chatID, groupKey, sent)
	stored, err := e.messages.GetMessages(e.ctx, chatID)
	require.NoError(t, err)
	require.Len(t, stored, len(sent))
	for i, msg := range stored {
		require.Equal(t, sent[i].MessageId.Value, msg.ID.Value)
		require.True(t, proto.Equal(sent[i].Content[0], msg.Content[0]))
	}

	// The user speaks there under the chat key's scheme.
	access := chat.NewAccess(e.chats, chat.NewRuleEvaluator(e.accounts, nil, e.chats, e.team))
	standing, err := access.SpeakerStanding(e.ctx, chatID, e.user)
	require.NoError(t, err)
	require.True(t, standing.CanSpeak)
	require.Equal(t, chat.EncryptionRequired, standing.Encryption)
	require.Equal(t, messagingpb.EncryptedContent_CHAT_KEY_XCHACHA20POLY1305, standing.Scheme)

	// The user is pushed for the welcome, under the group's title, naming
	// the team with the generic body; the team is not.
	require.Eventually(t, func() bool { return e.pusher.pushedTo(e.user) == len(sent) }, time.Second, 10*time.Millisecond)
	require.Never(t, func() bool { return len(e.pusher.snapshot()) > len(sent) }, 200*time.Millisecond, 10*time.Millisecond)
	require.Zero(t, e.pusher.pushedTo(e.team))
	pushed := e.pusher.snapshot()[0]
	require.Equal(t, "Welcome Party", pushed.title)
	require.Equal(t, "Flipcash sent a message", pushed.body)

	// The DM welcome is untouched: nothing was sent to the team's DM.
	_, err = e.chats.GetChatByID(e.ctx, chat.MustDeriveDmChatID(chatpb.ChatType_DM, e.team, e.user))
	require.ErrorIs(t, err, chat.ErrChatNotFound)
}

func TestSendWelcomeGroupV4_Idempotent(t *testing.T) {
	e := newGroupEnv(t)

	chatID, sent, err := e.sendWelcome()
	require.NoError(t, err)
	require.Eventually(t, func() bool { return e.pusher.pushedTo(e.user) == len(sent) }, time.Second, 10*time.Millisecond)

	// A retry returns the same group and messages, as first sent, and
	// sends, stores and pushes nothing new.
	againID, replayed, err := e.sendWelcome()
	require.NoError(t, err)
	require.Equal(t, chatID.Value, againID.Value)
	require.Len(t, replayed, len(sent))
	for i := range sent {
		require.Equal(t, sent[i].MessageId.Value, replayed[i].MessageId.Value)
		require.True(t, proto.Equal(sent[i].Content[0], replayed[i].Content[0]))
	}
	stored, err := e.messages.GetMessages(e.ctx, chatID)
	require.NoError(t, err)
	require.Len(t, stored, len(sent))
	require.Never(t, func() bool { return len(e.pusher.snapshot()) > len(sent) }, 200*time.Millisecond, 10*time.Millisecond)

	// The user still holds the one envelope, which still opens to the key.
	e.requireWelcome(t, chatID, e.openGroupKey(t, chatID), replayed)
}

// TestSendWelcomeGroupMessages_ExistingGroup: a group that exists is sent
// into as it is, never recreated, under a key of the messages' own.
func TestSendWelcomeGroupMessages_ExistingGroup(t *testing.T) {
	e := newGroupEnv(t)

	chatID, _, err := e.sendWelcome()
	require.NoError(t, err)
	envelope, err := e.chats.GetKeyEnvelope(e.ctx, chatID, e.user)
	require.NoError(t, err)

	againID, more, err := flipcashteam.SendWelcomeGroupMessages(e.ctx, e.chats, e.accounts, e.sender, e.cfg, e.user, "follow-up", []*messagingpb.Content{text("again")})
	require.NoError(t, err)
	require.Equal(t, chatID.Value, againID.Value)
	require.Len(t, more, 1)
	require.Equal(t, uint64(len(welcomeV4Texts)+1), more[0].MessageId.Value)

	// The envelope is the first one, and the follow-up decrypts under it.
	stillEnvelope, err := e.chats.GetKeyEnvelope(e.ctx, chatID, e.user)
	require.NoError(t, err)
	require.True(t, stillEnvelope.Equal(envelope))
	content, err := flipcashteam.DecryptGroupContent(e.openGroupKey(t, chatID), chatID, e.team, more[0].Content[0].GetEncrypted())
	require.NoError(t, err)
	require.Equal(t, "again", content.GetText().Text)

	// A lookup that misses the group falls through to a create that finds
	// it, and the send goes ahead into it.
	stale := &createCountingStore{Store: e.chats, staleReads: true}
	_, last, err := flipcashteam.SendWelcomeGroupMessages(e.ctx, stale, e.accounts, e.sender, e.cfg, e.user, "stale", []*messagingpb.Content{text("still")})
	require.NoError(t, err)
	require.Equal(t, uint64(len(welcomeV4Texts)+2), last[0].MessageId.Value)
	stillEnvelope, err = e.chats.GetKeyEnvelope(e.ctx, chatID, e.user)
	require.NoError(t, err)
	require.True(t, stillEnvelope.Equal(envelope))
}

// TestSendWelcomeGroupV4_CreatedElsewhere: a group already written under the
// welcome group's ID is sent into as it stands, never rewritten, and is not an
// error.
func TestSendWelcomeGroupV4_CreatedElsewhere(t *testing.T) {
	e := newGroupEnv(t)
	chatID := expectedWelcomeGroupID(e.team, e.user)
	envelope := chat.KeyEnvelope{
		Scheme:     chatpb.KeyEnvelope_X25519_XCHACHA20POLY1305,
		Nonce:      bytes.Repeat([]byte{1}, 24),
		Ciphertext: bytes.Repeat([]byte{1}, 48),
		WrappedBy:  e.team,
	}
	require.NoError(t, e.chats.PutPrivateGroup(e.ctx, &chat.Chat{
		ID:           chatID,
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{e.team, e.user},
		Title:        "Elsewhere",
		IsPrivate:    true,
		CreatorID:    e.team,
		LastActivity: time.Now().UTC(),
	}, []chat.MemberKeyEnvelope{{UserID: e.user, Envelope: envelope}}))

	gotID, sent, err := e.sendWelcome()
	require.NoError(t, err)
	require.Equal(t, chatID.Value, gotID.Value)
	require.Len(t, sent, len(welcomeV4Texts))

	record, err := e.chats.GetChatByID(e.ctx, chatID)
	require.NoError(t, err)
	require.Equal(t, "Elsewhere", record.Title)
	stored, err := e.chats.GetKeyEnvelope(e.ctx, chatID, e.user)
	require.NoError(t, err)
	require.True(t, stored.Equal(envelope))
}

func TestSendWelcomeGroupV4_Refused(t *testing.T) {
	e := newGroupEnv(t)
	messages := []*messagingpb.Content{text("hi")}
	send := func(cfg *flipcashteam.WelcomeGroupConfig, userID *commonpb.UserId, key string, msgs []*messagingpb.Content) error {
		_, _, err := flipcashteam.SendWelcomeGroupMessages(e.ctx, e.chats, e.accounts, e.sender, cfg, userID, key, msgs)
		return err
	}

	// What SendDmMessages refuses.
	require.Error(t, send(e.cfg, e.team, "welcome", messages))
	require.Error(t, send(e.cfg, e.user, "", messages))
	require.Error(t, send(e.cfg, e.user, "welcome", nil))
	require.Error(t, send(e.cfg, nil, "welcome", messages))
	require.Error(t, send(e.cfg, &commonpb.UserId{Value: []byte{1, 2, 3}}, "welcome", messages))

	// A configuration missing a part, or with a secret too short or that is
	// the team's own key.
	for name, cfg := range map[string]*flipcashteam.WelcomeGroupConfig{
		"nil":            nil,
		"no key pair":    {GroupKeySecret: e.cfg.GroupKeySecret, ProfilePictureBlobID: e.cfg.ProfilePictureBlobID},
		"short secret":   {TeamKeyPair: e.teamKeys, GroupKeySecret: e.cfg.GroupKeySecret[:31], ProfilePictureBlobID: e.cfg.ProfilePictureBlobID},
		"seed as secret": {TeamKeyPair: e.teamKeys, GroupKeySecret: e.teamKeys.Private().Seed(), ProfilePictureBlobID: e.cfg.ProfilePictureBlobID},
		"key as secret":  {TeamKeyPair: e.teamKeys, GroupKeySecret: e.teamKeys.Private(), ProfilePictureBlobID: e.cfg.ProfilePictureBlobID},
		"no picture":     {TeamKeyPair: e.teamKeys, GroupKeySecret: e.cfg.GroupKeySecret},
		"bad picture":    {TeamKeyPair: e.teamKeys, GroupKeySecret: e.cfg.GroupKeySecret, ProfilePictureBlobID: &blobpb.BlobId{Value: []byte{1}}},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, send(cfg, e.user, "welcome", messages))
		})
	}

	// A user with no account key to wrap their copy of the key to.
	keyless := model.MustGenerateUserID()
	require.Error(t, send(e.cfg, keyless, "welcome", messages))

	// None of it created a group or sent anything.
	for _, userID := range []*commonpb.UserId{e.user, keyless} {
		_, err := e.chats.GetChatByID(e.ctx, expectedWelcomeGroupID(e.team, userID))
		require.ErrorIs(t, err, chat.ErrChatNotFound)
	}
	require.Never(t, func() bool { return len(e.pusher.snapshot()) > 0 }, 100*time.Millisecond, 10*time.Millisecond)
}

func TestSendWelcomeGroupV4_NoTeamAccount(t *testing.T) {
	// A process started before the team account was set up has none.
	e := newChatEnvWithTeam(t, false)
	require.NoError(t, flipcashteam.AssignUsername(e.ctx, e.profiles, e.team))
	accounts := account_memory.NewInMemory()
	userKeys := model.MustGenerateKeyPair()
	_, err := accounts.Bind(e.ctx, e.user, userKeys.Proto())
	require.NoError(t, err)

	_, _, err = flipcashteam.SendWelcomeGroupV4(e.ctx, e.chats, accounts, e.sender, newWelcomeGroupConfig(t, model.MustGenerateKeyPair()), e.user)
	require.ErrorIs(t, err, flipcashteam.ErrNoTeamAccount)

	_, err = e.chats.GetChatByID(e.ctx, expectedWelcomeGroupID(e.team, e.user))
	require.ErrorIs(t, err, chat.ErrChatNotFound)
}
