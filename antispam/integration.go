package antispam

import (
	"context"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	ocp_transactionpb "github.com/code-payments/ocp-protobuf-api/generated/go/transaction/v1"

	"github.com/code-payments/flipcash2-server/account"
	ocp_antispam "github.com/code-payments/ocp-server/ocp/antispam"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/data/swap"
)

type Integration struct {
	accounts account.Store
}

func NewIntegration(accounts account.Store) ocp_antispam.Integration {
	return &Integration{
		accounts: accounts,
	}
}

func (i *Integration) AllowOpenAccounts(ctx context.Context, owner *ocp_common.Account, accountSet ocp_transactionpb.OpenAccountsMetadata_AccountSet) (bool, string, error) {
	switch accountSet {
	case ocp_transactionpb.OpenAccountsMetadata_USER:
		userID, err := i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: owner.PublicKey().ToBytes()})
		if err == account.ErrNotFound {
			return false, "public key not associated with a flipcash user", nil
		} else if err != nil {
			return false, "", err
		}

		isRegistered, err := i.accounts.IsRegistered(ctx, userID)
		if err != nil {
			return false, "", err
		}

		if !isRegistered {
			return false, "flipcash user has not completed iap for account creation", nil
		}
		return true, "", nil
	case ocp_transactionpb.OpenAccountsMetadata_POOL:
		return true, "", nil
	default:
		return false, "unsupported account set", nil
	}
}

func (i *Integration) AllowWelcomeBonus(ctx context.Context, owner *ocp_common.Account) (bool, string, error) {
	// Always allow since we properly gate everything required in AllowOpenAccounts
	return true, "", nil
}

func (i *Integration) AllowSendPayment(_ context.Context, _, _ *ocp_common.Account, isPublic bool) (bool, string, error) {
	if !isPublic {
		return false, "flipcash payments must be public", nil
	}
	return true, "", nil
}

func (i *Integration) AllowReceivePayments(ctx context.Context, owner *ocp_common.Account, isPublic bool) (bool, string, error) {
	if !isPublic {
		return false, "flipcash payments must be public", nil
	}
	return true, "", nil
}

func (i *Integration) AllowDistribution(ctx context.Context, owner *ocp_common.Account, isPublic bool) (bool, string, error) {
	if !isPublic {
		return false, "flipcash distributions must be public", nil
	}
	return true, "", nil
}

func (i *Integration) AllowSwap(_ context.Context, _ swap.FundingSource, _, _, _ *ocp_common.Account) (bool, string, error) {
	return true, "", nil
}
