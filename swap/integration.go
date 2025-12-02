package swap

import (
	"context"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/push"
	ocpswap "github.com/code-payments/ocp-server/ocp/worker/swap"
	ocpcommon "github.com/code-payments/ocp-server/ocp/common"
	ocpcurrency "github.com/code-payments/ocp-server/currency"
)

type Integration struct {
	accounts account.Store
	pusher   push.Pusher
}

func NewIntegration(accounts account.Store, pusher push.Pusher) ocpswap.Integration {
	return &Integration{
		accounts: accounts,
		pusher:   pusher,
	}
}

func (i *Integration) OnSwapFinalized(ctx context.Context, owner, mint *ocpcommon.Account, currencyName string, region ocpcurrency.Code, nativeAmount float64) error {
	userID, err := i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: owner.PublicKey().ToBytes()})
	if err != nil {
		return err
	}

	if ocpcommon.IsCoreMint(mint) {
		return push.SendUsdcReceivedFromSwapPush(ctx, i.pusher, userID, region, nativeAmount)
	}
	return push.SendFlipcashCurrencyReceivedFromSwapPush(ctx, i.pusher, userID, currencyName, region, nativeAmount)
}
