package flipcashteam

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/model"
)

// The welcome group: a private group the team account creates for a user, with
// the team as its creator and the team and the user as its only members, into
// which the team posts end-to-end encrypted messages, the welcome first.
//
// It is built but not yet used to welcome anyone: Welcomer sends the welcome
// into the team's DM with the user (see SendWelcomeV3), and nothing in the
// parent calls what is here. GroupWelcomer is what would: switching the
// welcome to the group is the parent handing it to the profile server in
// place of Welcomer.
//
// Every user has exactly one welcome group, ever: its ID is derived from the
// team and the user alone (see welcomeGroupID), so every send into it, whatever
// it is for, finds the same group, and the first creates it (see
// ensureWelcomeGroup). A group the team creates is keyed and fixed from its
// creation (see chat.Access): the user speaks there under the chat key like a
// member of any keyed private group, but cannot leave it, and it has no lobby,
// so no one else joins it.
//
// This is the one place the server writes encrypted content and holds a
// group's chat key, an exception to the rule that the server never reads
// ciphertext and that what it writes is plaintext. The team has no client, so
// the server stands in for it (see crypto.go): it derives the group's chat key
// rather than drawing one, since the team stores no envelope of its own that
// would hold it, and every later post derives it again from the secret and the
// group's ID; it wraps the user's copy of the key as the team, to the first of
// the user's account keys (account.Store.GetPubKeys' keys[0], the key a
// group's creator wraps for when admitting a member, see chat.Server's lobby)
// as they stand when the group is created; and it encrypts each message the
// team posts under the key, as any member would. The user's envelope is not
// rewrapped if their keys change later: like an envelope a creator wraps when
// admitting a member, it stands until the user replaces it with one of their
// own (see chat.Server.SetKeyEnvelope).
//
// Creating the group publishes no MemberJoined, nor anything else. An open
// event stream of the user's attaches to a group's chat topic when told of a
// membership, and nothing tells it of this one, so the send's update on that
// topic does not reach it: the group and the messages in it appear on the
// stream's next membership reconcile, or when the client next fetches its
// feed. The push for each message (see messaging.Sender.SendBatch) is what
// tells the user at once.

// WelcomeGroupTitle is the title of every welcome group.
const WelcomeGroupTitle = "Welcome Party"

// welcomeGroupIDDomain domain-separates the hash welcomeGroupID derives the
// group's idempotency key with, so it cannot collide with a key a client
// draws or one derived for another purpose.
const welcomeGroupIDDomain = "flipcash-team-welcome-group"

// WelcomeGroupConfig is what the server needs to act as the team in welcome
// groups, supplied by the parent. It is checked, as far as can be done
// without reading anything, on every send (see validate).
type WelcomeGroupConfig struct {
	// TeamKeyPair is the team account's key pair: the account key the
	// team's envelopes are wrapped with, which the user opens them against
	// as the group creator's public key. It must be the first of the team
	// account's keys (account.Store.GetPubKeys' keys[0]), the one
	// chat.Server.GetKeyEnvelope reports as the wrapper's key.
	TeamKeyPair model.KeyPair

	// GroupKeySecret is the team group key secret, which every welcome
	// group's chat key is derived from (see deriveGroupKey), as is the key of
	// any other group the team creates: at least 32 bytes of uniformly random
	// data kept for this purpose alone, guarded like a private key, and never
	// the team's signing key. It must never change for a group already
	// created. The parent loads it with LoadTeamGroupKeySecret.
	GroupKeySecret []byte

	// ProfilePictureBlobID is the profile picture every welcome group is
	// created with: one blob, shared by them all.
	ProfilePictureBlobID *blobpb.BlobId
}

// validate checks c as far as it can without reading anything: a full key
// pair, a secret long enough and other than the team's private key, and a
// well-formed picture.
func (c *WelcomeGroupConfig) validate() error {
	if c == nil {
		return errors.New("welcome group config is required")
	}
	if len(c.TeamKeyPair.Private()) != ed25519.PrivateKeySize {
		return errors.New("welcome group config has no team key pair")
	}
	if len(c.GroupKeySecret) < minGroupKeySecretSize {
		return fmt.Errorf("group key secret is %d bytes, want at least %d", len(c.GroupKeySecret), minGroupKeySecretSize)
	}
	private := c.TeamKeyPair.Private()
	if bytes.Equal(c.GroupKeySecret, private) || bytes.Equal(c.GroupKeySecret, private.Seed()) {
		return errors.New("group key secret is the team's private key")
	}
	if c.ProfilePictureBlobID == nil {
		return errors.New("welcome group config has no profile picture")
	}
	if err := c.ProfilePictureBlobID.Validate(); err != nil {
		return fmt.Errorf("welcome group profile picture: %w", err)
	}
	return nil
}

// welcomeGroupID returns the ID of userID's welcome group, created by the team
// account teamUserID: the group chat ID chat.MustDeriveGroupChatID derives for
// teamUserID as creator and the idempotency key
//
//	k = SHA-256("flipcash-team-welcome-group" || user_id)[:16]
//
// where user_id is the raw bytes of userID (model.UserIDSize bytes, so the
// concatenation is unambiguous) and 16 bytes is chat.IdempotencyKeySize, the
// width chat.v1.IdempotencyKey requires. The key names the welcome group's
// purpose and the user alone, never a version of the welcome, so each user has
// one welcome group whatever is sent into it.
//
// It must never change: every user welcomed already has a group under it, and
// a new derivation would give each of them a second welcome group.
func welcomeGroupID(teamUserID, userID *commonpb.UserId) *commonpb.ChatId {
	h := sha256.New()
	h.Write([]byte(welcomeGroupIDDomain))
	h.Write(userID.Value)
	key := &chatpb.IdempotencyKey{Value: h.Sum(nil)[:chat.IdempotencyKeySize]}
	return chat.MustDeriveGroupChatID(teamUserID, key)
}

// SendWelcomeGroupMessages sends messages from the team account to userID in
// userID's welcome group, creating the group first if it does not exist yet,
// and returns the group's ID and the messages as persisted, in order. Each
// element of messages is one message's plaintext content, which is encrypted
// under the group's chat key as the team (see encryptGroupContent) and sent
// as a messagingpb.EncryptedContent of scheme CHAT_KEY_XCHACHA20POLY1305. The
// messages land as one batch (see messaging.Sender.SendBatch): the user sees
// all of them or none, in order, each counting toward their unread and
// earning its own push. At most messaging.MaxMessagesPerPut messages go in
// one call.
//
// It refuses what SendMessages refuses, ErrNoTeamAccount included, and a cfg
// that fails its checks (see WelcomeGroupConfig) or a user with no account
// key to wrap their copy of the chat key to, the last only when the group is
// still to be created.
//
// It is idempotent on (the group, idempotencyKey) exactly as SendMessages is
// on (the chat, idempotencyKey): the client message IDs are derived from the
// pair and each message's position (see clientMessageID), so a retry returns
// what the first call sent without sending again. A retry encrypts afresh,
// under a fresh nonce, but what it would send is never written: the store
// answers a replay with the messages it holds and ignores what the retry
// carries (see messaging.Store.PutMessages). The key is the messages' own,
// not the group's, which is the same for every send.
func SendWelcomeGroupMessages(
	ctx context.Context,
	chats chat.Store,
	accounts account.Store,
	sender *messaging.Sender,
	cfg *WelcomeGroupConfig,
	userID *commonpb.UserId,
	idempotencyKey string,
	messages []*messagingpb.Content,
) (*commonpb.ChatId, []*messagingpb.Message, error) {
	if idempotencyKey == "" {
		return nil, nil, errors.New("idempotency key is required")
	}
	if len(messages) == 0 {
		return nil, nil, errors.New("expected at least one message")
	}
	if len(userID.GetValue()) != model.UserIDSize {
		return nil, nil, fmt.Errorf("user id must be %d bytes", model.UserIDSize)
	}

	teamUserID := sender.TeamAccount()
	if teamUserID == nil {
		return nil, nil, ErrNoTeamAccount
	}
	if bytes.Equal(teamUserID.Value, userID.GetValue()) {
		return nil, nil, errors.New("the team account cannot message itself")
	}
	if err := cfg.validate(); err != nil {
		return nil, nil, err
	}

	chatID := welcomeGroupID(teamUserID, userID)
	groupKey, err := deriveGroupKey(cfg.GroupKeySecret, chatID)
	if err != nil {
		return nil, nil, err
	}

	if err := ensureWelcomeGroup(ctx, chats, accounts, cfg, chatID, groupKey, teamUserID, userID); err != nil {
		return nil, nil, err
	}

	outgoing := make([]messaging.OutgoingMessage, len(messages))
	for i, content := range messages {
		encrypted, err := encryptGroupContent(groupKey, chatID, teamUserID, content)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to encrypt message %d: %w", i, err)
		}
		outgoing[i] = messaging.OutgoingMessage{
			SenderID:           teamUserID,
			Content:            []*messagingpb.Content{{Type: &messagingpb.Content_Encrypted{Encrypted: encrypted}}},
			ClientMessageID:    clientMessageID(chatID, idempotencyKey, i),
			CountsTowardUnread: true,
		}
	}
	sent, err := sender.SendBatch(ctx, chatID, outgoing)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to send messages: %w", err)
	}
	return chatID, sent, nil
}

// SendWelcomeGroupV3 sends the third version of the team's welcome (see
// SendWelcomeV3) to userID in their welcome group (see
// SendWelcomeGroupMessages), under the same key as the DM welcome: the key is
// scoped to the chat it is sent in, so the two never meet.
//
// It is idempotent on the user, for as long as SendWelcomeGroupMessages'
// idempotency lasts.
func SendWelcomeGroupV3(
	ctx context.Context,
	chats chat.Store,
	accounts account.Store,
	sender *messaging.Sender,
	cfg *WelcomeGroupConfig,
	userID *commonpb.UserId,
) (*commonpb.ChatId, []*messagingpb.Message, error) {
	return SendWelcomeGroupMessages(ctx, chats, accounts, sender, cfg, userID, welcomeV3IdempotencyKey, welcomeV3Messages())
}

// ensureWelcomeGroup creates userID's welcome group chatID, with groupKey as
// its chat key, unless it already exists. An earlier send into it, or a prior
// attempt of this one, may have created it, so an existing group is expected,
// not a failure.
//
// The group is looked up before it is created, as ensureChat looks up a DM and
// for the same reason: a create that finds the group there is a cancelled
// transaction that spends write capacity on every item it names, and a lookup
// that misses a group created a moment ago falls through to a create that
// reports chat.ErrChatExists, which is handled like the lookup finding it.
// The lookup also spares every send after the first the read of the user's
// keys and the wrap, which only a creation needs.
//
// The group is written as chat.Server.StartChat writes a private group, but
// for its members and envelopes: the team as creator, the team and the user
// as members, no rules, the user's copy of groupKey wrapped by the team to
// the user's first account key, and none for the team (see chat.Access). A
// user with no account key is refused, with nothing written. Nothing is
// published (see the welcome group's overview above).
func ensureWelcomeGroup(
	ctx context.Context,
	chats chat.Store,
	accounts account.Store,
	cfg *WelcomeGroupConfig,
	chatID *commonpb.ChatId,
	groupKey []byte,
	teamUserID, userID *commonpb.UserId,
) error {
	_, err := chats.GetChatByID(ctx, chatID)
	if err == nil {
		return nil
	}
	if !errors.Is(err, chat.ErrChatNotFound) {
		return fmt.Errorf("failed to get chat: %w", err)
	}

	keys, err := accounts.GetPubKeys(ctx, userID)
	if err != nil {
		return fmt.Errorf("failed to get user's public keys: %w", err)
	}
	if len(keys) == 0 {
		return fmt.Errorf("user %s has no public key", model.UserIDString(userID))
	}
	envelope, err := wrapGroupKey(cfg.TeamKeyPair, teamUserID, keys[0], chatID, groupKey)
	if err != nil {
		return fmt.Errorf("failed to wrap chat key: %w", err)
	}

	err = chats.PutPrivateGroup(ctx, &chat.Chat{
		ID:                   chatID,
		Type:                 chatpb.ChatType_GROUP,
		Members:              []*commonpb.UserId{teamUserID, userID},
		Title:                WelcomeGroupTitle,
		IsPrivate:            true,
		CreatorID:            teamUserID,
		ProfilePictureBlobID: cfg.ProfilePictureBlobID,
		LastActivity:         time.Now().UTC(),
	}, []chat.MemberKeyEnvelope{{UserID: userID, Envelope: envelope}})
	if err != nil && !errors.Is(err, chat.ErrChatExists) {
		return fmt.Errorf("failed to create chat: %w", err)
	}
	return nil
}
