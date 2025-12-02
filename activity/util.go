package activity

import (
	"context"

	ocpbalance "github.com/code-payments/ocp-server/ocp/balance"
	ocpcommon "github.com/code-payments/ocp-server/ocp/common"
	ocpdata "github.com/code-payments/ocp-server/ocp/data"
)

func isGiftCardClaimed(ctx context.Context, ocpData ocpdata.Provider, giftCardVaultAccount *ocpcommon.Account) (bool, error) {
	balance, err := ocpbalance.CalculateFromCache(ctx, ocpData, giftCardVaultAccount)
	if err == ocpbalance.ErrNotManagedByCode {
		return true, nil
	} else if err != nil {
		return false, err
	}
	return balance == 0, nil
}
