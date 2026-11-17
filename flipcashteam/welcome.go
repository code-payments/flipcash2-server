package flipcashteam

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/blob"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/rpc"
	ocp_client "github.com/code-payments/ocp-server/grpc/client"
)

// welcomeV3IdempotencyKey is the key SendWelcomeDmV3 sends under. Each version
// of the welcome takes a key of its own (the earlier were "welcome_v1" and
// "welcome_v2"), so a later one is sent even to a user who already has an
// earlier one.
const welcomeV3IdempotencyKey = "welcome_v3"

// welcomeV3Messages is the content of the third version of the welcome: one
// text message.
func welcomeV3Messages() []*messagingpb.Content {
	return []*messagingpb.Content{
		textContent("Welcome to Flipcash, the group money app. If someone sent you an invite link to a Flipcash group, tap it again to join."),
	}
}

func textContent(text string) *messagingpb.Content {
	return &messagingpb.Content{Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: text}}}
}

// welcomeTimeout bounds one welcome sent by a DmWelcomer: creating the chat
// and persisting the messages. The side effects of the send run under the
// Sender's own budgets.
const welcomeTimeout = 30 * time.Second

// DmWelcomer sends the welcome (see SendWelcomeDmV3) to every user given their
// first handle. It is the profile.FirstUsernameHandler the parent builds the
// profile server with (see profile.WithFirstUsernameHandler), from the chat
// store and the Sender built with the team account.
//
// A welcome goes only to a user whose handle was assigned from a client at or
// above minWelcomeClientVersion, read off the request that assigned it (see
// rpc.GetClientVersion); a user whose first handle came from an older client,
// or one that cannot be identified, is never welcomed, since there is no
// later first handle to try again on.
//
// A welcome is best effort: it is sent in the background, detached from the
// request that assigned the handle, and a failure is logged, never retried.
// Nothing records a welcome that was never sent, so a process that stops
// before sending one loses it. A second call for a user, as when two of their
// claims race, sends nothing new (see SendWelcomeDmV3).
//
// GroupWelcomer is its counterpart that welcomes in a private group instead,
// built but not yet used.
type DmWelcomer struct {
	log    *zap.Logger
	chats  chat.Store
	sender *messaging.Sender
}

var _ profile.FirstUsernameHandler = (*DmWelcomer)(nil)

func NewDmWelcomer(log *zap.Logger, chats chat.Store, sender *messaging.Sender) *DmWelcomer {
	return &DmWelcomer{log: log, chats: chats, sender: sender}
}

// minWelcomeClientVersion is the first client release, on iOS and Android
// alike, that rendered the second version of the welcome (its
// ShareProfileWidget). The third is text alone, which any client renders, but
// the gate is kept as it stands.
var minWelcomeClientVersion = &ocp_client.Version{Major: 2026, Minor: 9, Patch: 5}

// OnFirstUsername sends userID the welcome in the background, if the client
// that assigned username is at or above minWelcomeClientVersion.
func (w *DmWelcomer) OnFirstUsername(ctx context.Context, userID *commonpb.UserId, username string) {
	log := w.log.With(zap.String("user_id", model.UserIDString(userID)), zap.String("username", username))

	if clientVersion := rpc.GetClientVersion(ctx); clientVersion == nil || clientVersion.Before(minWelcomeClientVersion) {
		return
	}

	// The request that assigned the handle returns without waiting, so its
	// cancellation must not abort the send. Context values are kept.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), welcomeTimeout)
	go func() {
		defer cancel()

		_, _, err := SendWelcomeDmV3(ctx, w.chats, w.sender, userID)
		switch {
		case err == nil:
		case errors.Is(err, ErrNoTeamAccount):
			// A process with no team account configured sends no welcome.
			log.Debug("No team account to send welcome from")
		default:
			log.Warn("Failed to send welcome", zap.Error(err))
		}
	}()
}

// GroupWelcomer is the private-group counterpart of DmWelcomer: it sends the
// welcome into each user's welcome group (see SendWelcomeGroupV3) rather than
// into the team's DM with them (see SendWelcomeDmV3). It is built but not yet
// used to welcome anyone. Switching the welcome to the group is the parent
// passing a GroupWelcomer to profile.WithFirstUsernameHandler in place of the
// DmWelcomer, and nothing else.
//
// It is built only by NewGroupWelcomer, which checks at startup what a welcome
// needs and a send cannot fix, so that a process wrongly set up fails to start
// rather than losing every welcome to the same error, one user at a time.
//
// Like DmWelcomer, it welcomes only a user whose handle was assigned from a
// client at or above its gate, minWelcomeGroupClientVersion, and the welcome
// is best effort: it is sent in the background, detached from the request that
// assigned the handle, and a failure is logged, never retried. Nothing records
// a welcome that was never sent, so a process that stops before sending one
// loses it. A second call for a user, as when two of their claims race, sends
// nothing new (see SendWelcomeGroupV3).
type GroupWelcomer struct {
	log      *zap.Logger
	chats    chat.Store
	accounts account.Store
	sender   *messaging.Sender
	cfg      *WelcomeGroupConfig
}

var _ profile.FirstUsernameHandler = (*GroupWelcomer)(nil)

// NewGroupWelcomer returns a GroupWelcomer that welcomes users as the team
// account sender was built with, under cfg. It is called once, at startup,
// and an error from it is meant to fail the start. It checks, in order:
//
//   - that sender has a team account (else ErrNoTeamAccount): the welcome is
//     sent as the team, and a process with none could send nothing;
//   - cfg, as far as can be done without reading anything (see
//     WelcomeGroupConfig);
//   - that cfg's key pair is the team's, its public key the first of the
//     team account's keys: the user's envelope is opened against the key
//     chat.Server.GetKeyEnvelope reports as the wrapper's, which is that
//     one, so an envelope wrapped with any other would never open. A team
//     with no keys is refused;
//   - that the team's profile has a display name: a group message's push
//     names its sender, and one whose sender has none is dropped (see
//     messaging.Sender), so without one no welcome would ever push;
//   - that cfg's profile picture is a READY image the team owns, and that it
//     is readable by everyone (see blob.Integration.SetAsPublicMedia): every
//     welcome group carries it, and only a public grant reaches them all.
//
// The last check writes the grant, so it comes last: a start refused for
// anything else grants nothing. The grant is idempotent, so writing it on
// every start is harmless.
//
// The GroupWelcomer keeps its own copy of cfg, so a later change to the
// caller's is never seen. profiles and blobs are used only for the checks.
func NewGroupWelcomer(
	ctx context.Context,
	log *zap.Logger,
	chats chat.Store,
	accounts account.Store,
	profiles profile.Store,
	blobs *blob.Integration,
	sender *messaging.Sender,
	cfg *WelcomeGroupConfig,
) (*GroupWelcomer, error) {
	teamUserID := sender.TeamAccount()
	if teamUserID == nil {
		return nil, fmt.Errorf("welcome group: %w", ErrNoTeamAccount)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("welcome group config: %w", err)
	}
	cfg, err := cloneWelcomeGroupConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("welcome group config: %w", err)
	}

	keys, err := accounts.GetPubKeys(ctx, teamUserID)
	if err != nil {
		return nil, fmt.Errorf("failed to get the team's public keys: %w", err)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("team account %s has no public key", model.UserIDString(teamUserID))
	}
	if !bytes.Equal(cfg.TeamKeyPair.Public(), keys[0].GetValue()) {
		return nil, errors.New("welcome group config's team key pair is not the team account's first key")
	}

	teamProfile, err := profiles.GetFullProfile(ctx, teamUserID, false)
	if err != nil {
		return nil, fmt.Errorf("failed to get the team's profile: %w", err)
	}
	if teamProfile.DisplayName == "" {
		return nil, errors.New("team account has no display name")
	}

	if err := blobs.SetAsPublicMedia(ctx, teamUserID, cfg.ProfilePictureBlobID); err != nil {
		return nil, fmt.Errorf("welcome group profile picture: %w", err)
	}

	return &GroupWelcomer{
		log:      log,
		chats:    chats,
		accounts: accounts,
		sender:   sender,
		cfg:      cfg,
	}, nil
}

// minWelcomeGroupClientVersion is the first client release, on iOS and
// Android alike, given the welcome in a welcome group: one that can read it,
// which takes support for private groups, their end-to-end encryption and a
// group created by someone else. It is kept apart from minWelcomeClientVersion,
// which gates the DM welcome, since the two need different clients.
//
// 2026.10.3 is the first release on both platforms to read a private group
// created by someone else: it opens an envelope wrapped by the group's
// creator, resolving the creator's account key by user ID, and decrypts the
// group's messages. Nothing in it treats a group whose creator is the team,
// whose creator holds no envelope, or that the user joined without
// admission, any differently. An earlier release carried private groups in
// its protocol alone, with no way to read one.
var minWelcomeGroupClientVersion = &ocp_client.Version{Major: 2026, Minor: 10, Patch: 3}

// OnFirstUsername sends userID the welcome in their welcome group in the
// background, if the client that assigned username is at or above
// minWelcomeGroupClientVersion.
func (w *GroupWelcomer) OnFirstUsername(ctx context.Context, userID *commonpb.UserId, username string) {
	log := w.log.With(zap.String("user_id", model.UserIDString(userID)), zap.String("username", username))

	if clientVersion := rpc.GetClientVersion(ctx); clientVersion == nil || clientVersion.Before(minWelcomeGroupClientVersion) {
		return
	}

	// The request that assigned the handle returns without waiting, so its
	// cancellation must not abort the send. Context values are kept.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), welcomeTimeout)
	go func() {
		defer cancel()

		if _, _, err := SendWelcomeGroupV3(ctx, w.chats, w.accounts, w.sender, w.cfg, userID); err != nil {
			log.Warn("Failed to send welcome group", zap.Error(err))
		}
	}()
}
