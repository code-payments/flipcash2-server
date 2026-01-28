package geyser

import (
	"context"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/push"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_geyser "github.com/code-payments/ocp-server/ocp/worker/geyser"
)

type Integration struct {
	accounts account.Store
	pusher   push.Pusher
}

func NewIntegration(accounts account.Store, pusher push.Pusher) ocp_geyser.Integration {
	return &Integration{
		accounts: accounts,
		pusher:   pusher,
	}
}

func (i *Integration) OnDepositReceived(ctx context.Context, owner, mint *ocp_common.Account, currencyName string, usdMarketValue float64) error {
	// Hide small, potentially spam deposits
	if usdMarketValue < 0.01 {
		return nil
	}

	userID, err := i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: owner.PublicKey().ToBytes()})
	if err != nil {
		return err
	}

	if ocp_common.IsCoreMint(mint) {
		return push.SendUsdfDepositedPush(ctx, i.pusher, userID, usdMarketValue)
	}
	protoMint := &commonpb.PublicKey{Value: mint.PublicKey().ToBytes()}
	return push.SendFlipcashCurrencyDepositedPush(ctx, i.pusher, userID, protoMint, currencyName, usdMarketValue)
}
