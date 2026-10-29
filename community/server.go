// Package community serves what Flipcash shows about the community around a
// currency launched on Flipcash (see communitypb.CommunityMetadata): one
// community per launched currency, keyed by its mint.
//
// A community's top holder chats are the public groups gated on its currency
// (see chat.GatedMint) whose members hold the most of it, ranked off the
// holdings snapshots the chat store keeps (see chat.GroupHoldings and
// chat.Store.GetTopGroupsByHoldings). This package reads the ranking; it
// does not compute it; chat.HoldingsWorker does. A community's leader is the
// Flipcash user who launched its currency.
package community

import (
	"context"
	"errors"
	"fmt"

	"github.com/mr-tron/base58"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	communitypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/community/v1"

	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	ocp_currency "github.com/code-payments/ocp-server/ocp/data/currency"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
)

// maxTopHolderChats is how many groups a community's top holder chats list.
// The proto leaves the number to the server, up to its cap on
// CommunityMetadata.top_holder_chats (25).
const maxTopHolderChats = 10

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	accounts account.Store
	chats    chat.Store
	hydrator *chat.Hydrator
	ocpData  ocp_data.Provider

	// profiles reads the leader's public profile, as a chat member row shows
	// it, picture resolved: the same reader the hydrator is built with.
	profiles chat.ProfileReader

	communitypb.UnimplementedCommunityServer
}

func NewServer(
	log *zap.Logger,

	authz auth.Authorizer,

	accounts account.Store,
	chats chat.Store,
	hydrator *chat.Hydrator,
	ocpData ocp_data.Provider,
	profiles chat.ProfileReader,
) *Server {
	return &Server{
		log: log,

		authz: authz,

		accounts: accounts,
		chats:    chats,
		hydrator: hydrator,
		ocpData:  ocpData,
		profiles: profiles,
	}
}

// GetCommunity returns a launched currency's community. Everything in it is
// public and the same for every caller: auth, when set, is verified as
// everywhere else, but changes nothing.
//
// The mint must be a currency launched on Flipcash and available in OCP (see
// launchedCurrency); anything else, the core mint included, is
// NOT_FOUND. The name is the currency's, from the same OCP record, and so is
// the account the leader is resolved from (see leader).
//
// The top holder chats are one read of the ranking (see
// chat.Store.GetTopGroupsByHoldings), projected as a public list (see
// chat.Hydrator.PublicGroupsMetadata), which drops a group that no longer has
// a record. Each group's holdings_summary is the snapshot the ranking read
// returned, not the one the projection reads beside it, so the order and the
// amounts shown always agree, even when a refresh lands between the two
// reads.
func (s *Server) GetCommunity(ctx context.Context, req *communitypb.GetCommunityRequest) (*communitypb.GetCommunityResponse, error) {
	log := s.log
	if req.Auth != nil {
		callerID, err := s.authz.Authorize(ctx, req, &req.Auth)
		if err != nil {
			return nil, err
		}
		log = log.With(zap.String("caller_id", model.UserIDString(callerID)))
	}

	var mint *commonpb.PublicKey
	switch typed := req.Identifier.(type) {
	case *communitypb.GetCommunityRequest_Mint:
		mint = typed.Mint
	default:
		return nil, status.Error(codes.InvalidArgument, "unsupported identifier")
	}
	log = log.With(zap.String("mint", base58.Encode(mint.GetValue())))

	currency, ok, err := launchedCurrency(ctx, s.ocpData, mint)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting currency metadata")
		return nil, status.Error(codes.Internal, "")
	}
	if !ok {
		return &communitypb.GetCommunityResponse{Result: communitypb.GetCommunityResponse_NOT_FOUND}, nil
	}

	// The top holder chats and the leader read nothing of each other, so
	// they are read concurrently.
	var (
		topHolderChats []*chatpb.Metadata
		leader         *communitypb.CommunityLeader
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() (err error) {
		topHolderChats, err = s.topHolderChats(gctx, mint)
		return err
	})
	g.Go(func() (err error) {
		leader, err = s.leader(gctx, currency.CreatedBy)
		return err
	})
	if err := g.Wait(); err != nil {
		log.With(zap.Error(err)).Warn("Failure getting community")
		return nil, status.Error(codes.Internal, "")
	}

	return &communitypb.GetCommunityResponse{
		Result: communitypb.GetCommunityResponse_OK,
		Community: &communitypb.CommunityMetadata{
			Mint:           &commonpb.PublicKey{Value: append([]byte(nil), mint.Value...)},
			Name:           currency.Name,
			TopHolderChats: topHolderChats,
			Leader:         leader,
		},
	}, nil
}

// topHolderChats reads mint's ranking and projects it (see GetCommunity).
func (s *Server) topHolderChats(ctx context.Context, mint *commonpb.PublicKey) ([]*chatpb.Metadata, error) {
	top, err := s.chats.GetTopGroupsByHoldings(ctx, mint, maxTopHolderChats)
	if err != nil {
		return nil, fmt.Errorf("getting top groups by holdings: %w", err)
	}
	chatIDs := make([]*commonpb.ChatId, len(top))
	holdings := make(map[string]chat.GroupHoldings, len(top))
	for i, h := range top {
		chatIDs[i] = h.ChatID
		holdings[string(h.ChatID.Value)] = h
	}

	chats, err := s.hydrator.PublicGroupsMetadata(ctx, chatIDs)
	if err != nil {
		return nil, fmt.Errorf("hydrating top holder chats: %w", err)
	}
	topHolderChats := make([]*chatpb.Metadata, 0, len(chats))
	for _, md := range chats {
		h, ok := holdings[string(md.ChatId.Value)]
		if !ok {
			continue
		}
		md.HoldingsSummary = h.ToProto()
		topHolderChats = append(topHolderChats, md)
	}
	return topHolderChats, nil
}

// leader resolves a community's leader from the account that launched its
// currency (createdBy, the base58 owner account OCP records on the currency):
// the Flipcash user bound to that key, with their public profile. It returns
// nil, and no error, when there is none to show: a key that is not a public
// key, one no Flipcash user holds, or a user with no display name, who has
// nothing to be shown by.
func (s *Server) leader(ctx context.Context, createdBy string) (*communitypb.CommunityLeader, error) {
	key, err := base58.Decode(createdBy)
	if err != nil || len(key) != 32 {
		return nil, nil
	}

	userID, err := s.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: key})
	if errors.Is(err, account.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting leader's user: %w", err)
	}

	profiles, err := s.profiles.GetLimitedPublicProfilesForRow(ctx, []*commonpb.UserId{userID})
	if err != nil {
		return nil, fmt.Errorf("getting leader's profile: %w", err)
	}
	profile, ok := profiles[string(userID.Value)]
	if !ok || profile.GetDisplayName() == "" {
		return nil, nil
	}
	return &communitypb.CommunityLeader{Profile: profile}, nil
}

// launchedCurrency returns the OCP record of the currency launched as mint,
// and false when mint is not one that is available: the core mint, a mint
// OCP has no currency for, or one not yet (or no longer) available. It is
// what makes a mint a community. A gated group's mint needs no such check,
// since a group can only be created gated on an available currency (see
// chat.GatedMint), which is why the holdings worker never asks.
func launchedCurrency(ctx context.Context, ocpData ocp_data.Provider, mint *commonpb.PublicKey) (*ocp_currency.MetadataRecord, bool, error) {
	account, err := ocp_common.NewAccountFromPublicKeyBytes(mint.GetValue())
	if err != nil {
		return nil, false, nil
	}
	if ocp_common.IsCoreMint(account) {
		return nil, false, nil
	}
	metadata, err := ocpData.GetCurrencyMetadata(ctx, account.PublicKey().ToBase58())
	if errors.Is(err, ocp_currency.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if metadata.State != ocp_currency.MetadataStateAvailable {
		return nil, false, nil
	}
	return metadata, true, nil
}
