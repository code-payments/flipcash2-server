package flipcashteam

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/blob"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/rpc"
	ocp_client "github.com/code-payments/ocp-server/grpc/client"
)

// GroupWelcomer is the private-group counterpart of Welcomer: it sends the
// welcome into each user's welcome group (see SendWelcomeGroupV3) rather than
// into the team's DM with them (see SendWelcomeV3). It is built but not yet
// used to welcome anyone. Switching the welcome to the group is the parent
// passing a GroupWelcomer to profile.WithFirstUsernameHandler in place of the
// Welcomer, and nothing else.
//
// It is built only by NewGroupWelcomer, which checks at startup what a welcome
// needs and a send cannot fix, so that a process wrongly set up fails to start
// rather than losing every welcome to the same error, one user at a time.
//
// Like Welcomer, it welcomes only a user whose handle was assigned from a
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

// cloneWelcomeGroupConfig returns a deep copy of cfg, which has passed
// validate.
func cloneWelcomeGroupConfig(cfg *WelcomeGroupConfig) (*WelcomeGroupConfig, error) {
	teamKeyPair, err := model.FromPrivateKey(bytes.Clone(cfg.TeamKeyPair.Private()))
	if err != nil {
		return nil, err
	}
	return &WelcomeGroupConfig{
		TeamKeyPair:          teamKeyPair,
		GroupKeySecret:       bytes.Clone(cfg.GroupKeySecret),
		ProfilePictureBlobID: proto.Clone(cfg.ProfilePictureBlobID).(*blobpb.BlobId),
	}, nil
}

// minWelcomeGroupClientVersion is the first client release, on iOS and
// Android alike, given the welcome in a welcome group: one that can read it,
// which takes support for private groups, their end-to-end encryption and a
// group created by the team. It is kept apart from minWelcomeClientVersion,
// which gates the DM welcome, since the two need different clients.
//
// No such release is decided yet, so it stands at minWelcomeClientVersion's
// value as a placeholder. It must be set to the first release that reads
// team-created private groups before a GroupWelcomer is used.
var minWelcomeGroupClientVersion = &ocp_client.Version{Major: 2026, Minor: 9, Patch: 5}

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
