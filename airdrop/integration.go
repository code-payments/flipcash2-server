package airdrop

import (
	"context"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/iap"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_transaction "github.com/code-payments/ocp-server/ocp/rpc/transaction"
	ocp_currency "github.com/code-payments/ocp-server/currency"
)

type Integration struct {
	accounts account.Store
	iaps     iap.Store
}

func NewIntegration(accounts account.Store, iaps iap.Store) ocp_transaction.AirdropIntegration {
	return &Integration{
		accounts: accounts,
		iaps:     iaps,
	}
}

// Welcome bonuses have been disabled
func (i *Integration) GetWelcomeBonusAmount(ctx context.Context, owner *ocp_common.Account) (float64, ocp_currency.Code, error) {
	return 0, "", nil
}
