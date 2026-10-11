package poll

import (
	"context"
	"errors"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	currency_lib "github.com/code-payments/ocp-server/currency"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	ocp_currency "github.com/code-payments/ocp-server/ocp/data/currency"

	"github.com/code-payments/flipcash2-server/send"
)

// What a BALANCE poll's BalanceParameters may name is asked once, at
// creation, so a poll is never written that its tally could not count: a
// tally retries until it succeeds, and a poll whose parameters could never be
// valued would retry forever.

// isSupportedMint reports whether mint is a currency whose balances the tally
// can read and value: the core mint, or a launched currency OCP holds as
// available.
func isSupportedMint(ctx context.Context, ocpData ocp_data.Provider, mint *commonpb.PublicKey) (bool, error) {
	account, err := ocp_common.NewAccountFromPublicKeyBytes(mint.GetValue())
	if err != nil {
		return false, nil
	}
	if ocp_common.IsCoreMint(account) {
		return true, nil
	}
	metadata, err := ocpData.GetCurrencyMetadata(ctx, account.PublicKey().ToBase58())
	if errors.Is(err, ocp_currency.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return metadata.State == ocp_currency.MetadataStateAvailable, nil
}

// isSupportedCurrency reports whether the tally can value balances in code:
// when Flipcash offers it for payments (see send.PresetsFor). Those are the
// currencies clients show balances in, and so ones OCP prices. The list is
// deterministic, unlike asking OCP to value a balance, which says nothing for
// a user who has not opened accounts yet.
func isSupportedCurrency(code string) bool {
	_, ok := send.PresetsFor(currency_lib.Code(code))
	return ok
}
