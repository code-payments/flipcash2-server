package swap

import (
	"context"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/push"
	ocp_currency "github.com/code-payments/ocp-server/currency"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_swap "github.com/code-payments/ocp-server/ocp/worker/swap"
)

type Integration struct {
	accounts account.Store
	pusher   push.Pusher
}

func NewIntegration(accounts account.Store, pusher push.Pusher) ocp_swap.Integration {
	return &Integration{
		accounts: accounts,
		pusher:   pusher,
	}
}

func (i *Integration) OnSwapFinalized(ctx context.Context, owner *ocp_common.Account, isBuy bool, mint *ocp_common.Account, currencyName string, region ocp_currency.Code, amountReceived float64) error {
	userID, err := i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: owner.PublicKey().ToBytes()})
	if err != nil {
		return err
	}

	protoMint := &commonpb.PublicKey{Value: mint.PublicKey().ToBytes()}
	if isBuy {
		return push.SendFlipcashCurrencyBoughtPush(ctx, i.pusher, userID, protoMint, currencyName, region, amountReceived)
	}
	return push.SendFlipcashCurrencySoldPush(ctx, i.pusher, userID, protoMint, currencyName, region, amountReceived)
}
