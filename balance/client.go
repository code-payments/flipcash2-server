// Package balance provides utilities for fetching a Flipcash user's balance
// from the OCP server.
package balance

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	ocp_balancepb "github.com/code-payments/ocp-protobuf-api/generated/go/balance/v1"
	ocp_commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	currency_lib "github.com/code-payments/ocp-server/currency"
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

	// ErrAmbiguousOwner indicates the user has more than one public key bound,
	// so there is no single owner account to fetch a balance for. Nothing
	// binds a second key today; a user in this state is a bug, not a case.
	ErrAmbiguousOwner = errors.New("more than one owner account found for user")

	// ErrUnsupportedCurrency indicates the OCP server could not value a balance
	// in the requested fiat currency: it answered the owner, but with no value
	// for that currency code. OCP is the authority on which currencies it can
	// price, so this is how a code it does not know — or cannot quote right
	// now — surfaces; no list of supported codes is kept here.
	ErrUnsupportedCurrency = errors.New("unsupported balance currency")
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
// quarks sitting in their USDF accounts. Given mints, it spans only the
// holdings in those mints, still valued in USDF; a mint the user holds nothing
// of contributes nothing.
//
// No fiat valuation is asked for, so the answer never depends on an exchange
// rate: a USD comparison against it is exact, in quarks.
//
// A user that hasn't opened OCP accounts yet has a zero balance. ErrNotFound is
// returned when the user has no public key bound at all.
func (c *Client) GetTotalUsdfBalance(ctx context.Context, userID *commonpb.UserId, mints ...*commonpb.PublicKey) (uint64, error) {
	ownerBalance, err := c.getOwnerBalance(ctx, userID, nil, mints)
	if err != nil {
		return 0, err
	}
	return ownerBalance.GetCoreMintValue(), nil
}

// GetTotalFiatValue returns a user's total balance valued in a fiat currency,
// in that currency's major units — the same holdings GetTotalUsdfBalance
// totals, and the same mint filter, but as OCP values them at its current
// exchange rate rather than in quarks. The value is a quoted rate applied to
// a quark total, so a comparison against it should allow rounding slack.
//
// The currency is any ISO 4217 code OCP can price. One it cannot is
// ErrUnsupportedCurrency, never a zero: a caller gating on the value must not
// mistake "no answer" for "holds nothing". A user that hasn't opened OCP
// accounts yet holds nothing in any currency, and ErrNotFound is returned when
// the user has no public key bound at all.
func (c *Client) GetTotalFiatValue(ctx context.Context, userID *commonpb.UserId, currency currency_lib.Code, mints ...*commonpb.PublicKey) (float64, error) {
	code := string(currency)
	ownerBalance, err := c.getOwnerBalance(ctx, userID, []string{code}, mints)
	if err != nil {
		return 0, err
	}
	if ownerBalance == nil {
		return 0, nil
	}
	value, ok := ownerBalance.FiatValuesByCurrency[code]
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrUnsupportedCurrency, code)
	}
	return value, nil
}

// getOwnerBalance asks OCP for the balance of the user's owner account,
// filtered to mints and additionally valued in currencyCodes, and returns it
// as OCP reports it: the totals across the owner's accounts are OCP's, so
// nothing is summed here. A user has one owner account, the one key bound to
// them: ErrNotFound is returned when there is none, and ErrAmbiguousOwner
// when there is more than one, rather than a balance that answers for only
// some of what the user holds. An owner OCP doesn't know is left out of the
// response, which is the case until the user opens their accounts: nothing has
// been opened, so the balance is nil, without error.
func (c *Client) getOwnerBalance(ctx context.Context, userID *commonpb.UserId, currencyCodes []string, mints []*commonpb.PublicKey) (*ocp_balancepb.OwnerBalance, error) {
	log := c.log.With(zap.String("user_id", model.UserIDString(userID)))

	pubKeys, err := c.accounts.GetPubKeys(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting public keys for user")
		return nil, err
	}
	if len(pubKeys) == 0 {
		return nil, ErrNotFound
	}
	if len(pubKeys) > 1 {
		log.With(zap.Int("count", len(pubKeys))).Warn("User has more than one bound public key")
		return nil, ErrAmbiguousOwner
	}
	owner, err := ocp_common.NewAccountFromPublicKeyBytes(pubKeys[0].Value)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure parsing owner account")
		return nil, err
	}

	// The mint filter is OCP's: filtered, the owner's total covers only the
	// requested mints, so there is nothing to filter client-side.
	mintProtos := make([]*ocp_commonpb.SolanaAccountId, 0, len(mints))
	for _, mint := range mints {
		account, err := ocp_common.NewAccountFromPublicKeyBytes(mint.Value)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure parsing mint account")
			return nil, err
		}
		mintProtos = append(mintProtos, account.ToProto())
	}

	resp, err := c.ocpBalance.GetBalances(ctx, &ocp_balancepb.GetBalancesRequest{
		Owners:        []*ocp_commonpb.SolanaAccountId{owner.ToProto()},
		Mints:         mintProtos,
		CurrencyCodes: currencyCodes,
	})
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting balances from OCP")
		return nil, err
	}

	switch resp.Result {
	case ocp_balancepb.GetBalancesResponse_OK:
	case ocp_balancepb.GetBalancesResponse_DENIED:
		return nil, ErrDenied
	default:
		log.With(zap.String("result", resp.Result.String())).Warn("Unexpected result getting balances from OCP")
		return nil, errors.New("unexpected result getting balances: " + resp.Result.String())
	}

	// Balances come back keyed by owner address; a missing owner is nil.
	return resp.BalancesByOwner[owner.PublicKey().ToBase58()], nil
}
