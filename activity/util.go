package activity

import (
	"context"

	ocp_balance "github.com/code-payments/ocp-server/ocp/balance"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
)

func isGiftCardClaimed(ctx context.Context, ocpData ocp_data.Provider, giftCardVaultAccount *ocp_common.Account) (bool, error) {
	balance, err := ocp_balance.CalculateFromCache(ctx, ocpData, giftCardVaultAccount)
	if err == ocp_balance.ErrNotManagedByCode {
		return true, nil
	} else if err != nil {
		return false, err
	}
	return balance == 0, nil
}
