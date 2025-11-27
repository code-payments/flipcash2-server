package geyser

import (
	"context"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/push"
	ocpgeyser "github.com/code-payments/ocp-server/pkg/code/async/geyser"
	ocpcommon "github.com/code-payments/ocp-server/pkg/code/common"
)

type Integration struct {
	accounts account.Store
	pusher   push.Pusher
}

func NewIntegration(accounts account.Store, pusher push.Pusher) ocpgeyser.Integration {
	return &Integration{
		accounts: accounts,
		pusher:   pusher,
	}
}

func (i *Integration) OnDepositReceived(ctx context.Context, owner, mint *ocpcommon.Account, currencyName string, usdMarketValue float64) error {
	// Hide small, potentially spam deposits
	if usdMarketValue < 0.01 {
		return nil
	}

	userID, err := i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: owner.PublicKey().ToBytes()})
	if err != nil {
		return err
	}

	if ocpcommon.IsCoreMint(mint) {
		return push.SendUsdcReceivedFromDepositPush(ctx, i.pusher, userID, usdMarketValue)
	}
	return push.SendFlipcashCurrencyReceivedFromDepositPush(ctx, i.pusher, userID, currencyName, usdMarketValue)
}
