package airdrop

import (
	"context"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/iap"
	ocpcommon "github.com/code-payments/ocp-server/ocp/common"
	ocptransaction "github.com/code-payments/ocp-server/ocp/rpc/transaction"
	ocpcurrency "github.com/code-payments/ocp-server/currency"
)

type Integration struct {
	accounts account.Store
	iaps     iap.Store
}

func NewIntegration(accounts account.Store, iaps iap.Store) ocptransaction.AirdropIntegration {
	return &Integration{
		accounts: accounts,
		iaps:     iaps,
	}
}

// Welcome bonuses have been disabled
func (i *Integration) GetWelcomeBonusAmount(ctx context.Context, owner *ocpcommon.Account) (float64, ocpcurrency.Code, error) {
	return 0, "", nil
}
