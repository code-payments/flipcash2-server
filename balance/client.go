// Package balance provides utilities for fetching a Flipcash user's balance
// from the OCP server.
package balance

import (
	"context"
	"errors"

	"go.uber.org/zap"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	ocp_balancepb "github.com/code-payments/ocp-protobuf-api/generated/go/balance/v1"
	ocp_commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	ocp_common "github.com/code-payments/ocp-server/ocp/common"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/model"
)

var (
	// ErrNotFound indicates the user has no account bound to a public key, so
	// there's no owner account to fetch a balance for.
	ErrNotFound = errors.New("no owner account found for user")

	// ErrDenied indicates the OCP server refused to disclose the balance of an
	// owner account.
	ErrDenied = errors.New("denied access to balance")
)

// Client fetches user balances from the OCP server.
type Client struct {
	log *zap.Logger

	accounts account.Store

	ocpBalance ocp_balancepb.BalanceClient
}

// NewClient returns a Client backed by the OCP Balance service. The parent
// application is responsible for constructing ocpBalance against the OCP
// server's gRPC endpoint.
func NewClient(
	log *zap.Logger,
	accounts account.Store,
	ocpBalance ocp_balancepb.BalanceClient,
) *Client {
	return &Client{
		log:        log,
		accounts:   accounts,
		ocpBalance: ocpBalance,
	}
}

// GetTotalUsdfBalance returns a user's total balance in USDF quarks.
//
// The value is what OCP reports as the owner account's core mint value, which
// is USDF for Flipcash. It spans every mint the user holds, with non-core mint
// holdings valued in USDF, so it is the user's total balance and not just the
// quarks sitting in their USDF accounts.
//
// A user that hasn't opened OCP accounts yet has a zero balance. ErrNotFound is
// returned when the user has no public key bound at all.
func (c *Client) GetTotalUsdfBalance(ctx context.Context, userID *commonpb.UserId) (uint64, error) {
	log := c.log.With(zap.String("user_id", model.UserIDString(userID)))

	pubKeys, err := c.accounts.GetPubKeys(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting public keys for user")
		return 0, err
	}
	if len(pubKeys) == 0 {
		return 0, ErrNotFound
	}

	// Users are expected to have a single public key, but the binding is modelled
	// as a set, so fetch every owner account the user controls in one call and
	// sum across them.
	owners := make([]*ocp_common.Account, 0, len(pubKeys))
	ownerProtos := make([]*ocp_commonpb.SolanaAccountId, 0, len(pubKeys))
	for _, pubKey := range pubKeys {
		owner, err := ocp_common.NewAccountFromPublicKeyBytes(pubKey.Value)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure parsing owner account")
			return 0, err
		}
		owners = append(owners, owner)
		ownerProtos = append(ownerProtos, owner.ToProto())
	}

	resp, err := c.ocpBalance.GetBalances(ctx, &ocp_balancepb.GetBalancesRequest{
		Owners: ownerProtos,
	})
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting balances from OCP")
		return 0, err
	}

	switch resp.Result {
	case ocp_balancepb.GetBalancesResponse_OK:
	case ocp_balancepb.GetBalancesResponse_DENIED:
		return 0, ErrDenied
	default:
		log.With(zap.String("result", resp.Result.String())).Warn("Unexpected result getting balances from OCP")
		return 0, errors.New("unexpected result getting balances: " + resp.Result.String())
	}

	// Balances come back keyed by owner address. An owner that OCP doesn't know
	// is left out of the response, which is the case until the user opens their
	// accounts. Nothing has been opened, so that owner contributes nothing.
	var total uint64
	for _, owner := range owners {
		ownerBalance, ok := resp.BalancesByOwner[owner.PublicKey().ToBase58()]
		if !ok {
			continue
		}
		total += ownerBalance.CoreMintValue
	}
	return total, nil
}
