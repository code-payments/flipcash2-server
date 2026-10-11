package balance

import (
	"context"
	"errors"
	"testing"

	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	ocp_balancepb "github.com/code-payments/ocp-protobuf-api/generated/go/balance/v1"

	currency_lib "github.com/code-payments/ocp-server/currency"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"

	accountmemory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/model"
)

// fakeOcpBalance answers GetBalances from one fixed per-mint ledger for every
// owner it is asked about, honouring the request's mint filter the way OCP
// does: the owner's total covers only the requested mints, and its currency
// codes: the total is also valued in each requested currency it has a rate
// for, and silently not in one it lacks, both in total and per mint, as OCP
// reports the breakdown. It records the last request so a test can see what
// was asked.
type fakeOcpBalance struct {
	result ocp_balancepb.GetBalancesResponse_Result
	err    error

	// byMint is the ledger, in USDF quarks, keyed by base58 mint address.
	byMint map[string]uint64
	// rates values a USDF unit in each fiat currency the fake can price.
	rates map[string]float64
	// unknownOwners leaves every requested owner out of the response, which is
	// how OCP answers for an owner it has no accounts for.
	unknownOwners bool

	lastReq *ocp_balancepb.GetBalancesRequest
}

func (f *fakeOcpBalance) GetBalances(_ context.Context, req *ocp_balancepb.GetBalancesRequest, _ ...grpc.CallOption) (*ocp_balancepb.GetBalancesResponse, error) {
	f.lastReq = req
	if f.err != nil {
		return nil, f.err
	}
	resp := &ocp_balancepb.GetBalancesResponse{
		Result:          f.result,
		BalancesByOwner: make(map[string]*ocp_balancepb.OwnerBalance),
	}
	if f.unknownOwners {
		return resp, nil
	}

	requested := make(map[string]bool, len(req.Mints))
	for _, mint := range req.Mints {
		requested[base58.Encode(mint.Value)] = true
	}
	value := func(quarks uint64) map[string]float64 {
		var values map[string]float64
		for _, code := range req.CurrencyCodes {
			rate, ok := f.rates[code]
			if !ok {
				continue
			}
			if values == nil {
				values = make(map[string]float64)
			}
			values[code] = float64(quarks) / float64(ocp_common.CoreMintQuarksPerUnit) * rate
		}
		return values
	}
	for _, owner := range req.Owners {
		ownerBalance := &ocp_balancepb.OwnerBalance{Owner: owner, BalancesByMint: make(map[string]*ocp_balancepb.MintBalance)}
		for mint, quarks := range f.byMint {
			if len(requested) == 0 || requested[mint] {
				ownerBalance.CoreMintValue += quarks
				ownerBalance.BalancesByMint[mint] = &ocp_balancepb.MintBalance{CoreMintValue: quarks, FiatValuesByCurrency: value(quarks)}
			}
		}
		ownerBalance.FiatValuesByCurrency = value(ownerBalance.CoreMintValue)
		resp.BalancesByOwner[base58.Encode(owner.Value)] = ownerBalance
	}
	return resp, nil
}

func TestClient_GetTotalUsdfBalance(t *testing.T) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)
	accounts := accountmemory.NewInMemory()

	usdf := model.MustGenerateKeyPair().Proto()
	other := model.MustGenerateKeyPair().Proto()
	ocp := &fakeOcpBalance{byMint: map[string]uint64{
		base58.Encode(usdf.Value):  100,
		base58.Encode(other.Value): 30,
	}}
	client := NewClient(log, accounts, ocp)

	userID := model.MustGenerateUserID()
	_, err := accounts.Bind(ctx, userID, model.MustGenerateKeyPair().Proto())
	require.NoError(t, err)

	// Unfiltered, the total spans every mint, and no filter is sent. The
	// user's one owner account is asked about, and OCP's total for it is
	// the answer.
	total, err := client.GetTotalUsdfBalance(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, uint64(130), total)
	require.Empty(t, ocp.lastReq.Mints)
	require.Len(t, ocp.lastReq.Owners, 1)

	// Filtered, the filter is passed through and OCP's total is trusted.
	total, err = client.GetTotalUsdfBalance(ctx, userID, usdf)
	require.NoError(t, err)
	require.Equal(t, uint64(100), total)
	require.Len(t, ocp.lastReq.Mints, 1)
	require.Equal(t, usdf.Value, ocp.lastReq.Mints[0].Value)

	// A mint the user holds nothing of contributes nothing.
	total, err = client.GetTotalUsdfBalance(ctx, userID, model.MustGenerateKeyPair().Proto())
	require.NoError(t, err)
	require.Zero(t, total)

	// An owner OCP has no accounts for holds nothing.
	ocp.unknownOwners = true
	total, err = client.GetTotalUsdfBalance(ctx, userID, usdf)
	require.NoError(t, err)
	require.Zero(t, total)
	ocp.unknownOwners = false

	// A user with no bound key is not found; refusals and failures surface.
	_, err = client.GetTotalUsdfBalance(ctx, model.MustGenerateUserID())
	require.ErrorIs(t, err, ErrNotFound)

	ocp.result = ocp_balancepb.GetBalancesResponse_DENIED
	_, err = client.GetTotalUsdfBalance(ctx, userID)
	require.ErrorIs(t, err, ErrDenied)
	ocp.result = ocp_balancepb.GetBalancesResponse_OK

	ocp.err = errors.New("ocp is down")
	_, err = client.GetTotalUsdfBalance(ctx, userID)
	require.Error(t, err)

	// A malformed mint is refused before any request is made.
	ocp.err = nil
	ocp.lastReq = nil
	_, err = client.GetTotalUsdfBalance(ctx, userID, &commonpb.PublicKey{Value: []byte{1, 2, 3}})
	require.Error(t, err)
	require.Nil(t, ocp.lastReq)
}

func TestClient_GetTotalFiatValue(t *testing.T) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)
	accounts := accountmemory.NewInMemory()

	usdf := model.MustGenerateKeyPair().Proto()
	other := model.MustGenerateKeyPair().Proto()
	ocp := &fakeOcpBalance{
		byMint: map[string]uint64{
			base58.Encode(usdf.Value):  100 * ocp_common.CoreMintQuarksPerUnit,
			base58.Encode(other.Value): 30 * ocp_common.CoreMintQuarksPerUnit,
		},
		rates: map[string]float64{"eur": 0.9, "jpy": 150},
	}
	client := NewClient(log, accounts, ocp)

	userID := model.MustGenerateUserID()
	_, err := accounts.Bind(ctx, userID, model.MustGenerateKeyPair().Proto())
	require.NoError(t, err)

	// The one currency is asked for, and OCP's valuation of the whole balance
	// is trusted.
	total, err := client.GetTotalFiatValue(ctx, userID, currency_lib.EUR)
	require.NoError(t, err)
	require.InDelta(t, 130*0.9, total, 1e-9)
	require.Equal(t, []string{"eur"}, ocp.lastReq.CurrencyCodes)
	require.Empty(t, ocp.lastReq.Mints)

	// Filtered, the filter is passed through and the valuation covers only the
	// requested mints.
	total, err = client.GetTotalFiatValue(ctx, userID, currency_lib.JPY, usdf)
	require.NoError(t, err)
	require.InDelta(t, 100*150, total, 1e-9)
	require.Len(t, ocp.lastReq.Mints, 1)
	require.Equal(t, usdf.Value, ocp.lastReq.Mints[0].Value)

	// The USDF total never asks for a valuation.
	_, err = client.GetTotalUsdfBalance(ctx, userID)
	require.NoError(t, err)
	require.Empty(t, ocp.lastReq.CurrencyCodes)

	// A currency OCP does not value is refused, never read as nothing.
	_, err = client.GetTotalFiatValue(ctx, userID, currency_lib.Code("xyz"))
	require.ErrorIs(t, err, ErrUnsupportedCurrency)

	// An owner OCP has no accounts for holds nothing in any currency, and is
	// not an unsupported currency.
	ocp.unknownOwners = true
	total, err = client.GetTotalFiatValue(ctx, userID, currency_lib.EUR)
	require.NoError(t, err)
	require.Zero(t, total)
	ocp.unknownOwners = false

	// A user with no bound key is not found; refusals and failures surface.
	_, err = client.GetTotalFiatValue(ctx, model.MustGenerateUserID(), currency_lib.EUR)
	require.ErrorIs(t, err, ErrNotFound)

	ocp.result = ocp_balancepb.GetBalancesResponse_DENIED
	_, err = client.GetTotalFiatValue(ctx, userID, currency_lib.EUR)
	require.ErrorIs(t, err, ErrDenied)
	ocp.result = ocp_balancepb.GetBalancesResponse_OK

	ocp.err = errors.New("ocp is down")
	_, err = client.GetTotalFiatValue(ctx, userID, currency_lib.EUR)
	require.Error(t, err)
}

func TestClient_GetHoldings(t *testing.T) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)
	accounts := accountmemory.NewInMemory()

	usdf := model.MustGenerateKeyPair().Proto()
	other := model.MustGenerateKeyPair().Proto()
	unheld := model.MustGenerateKeyPair().Proto()
	ocp := &fakeOcpBalance{
		byMint: map[string]uint64{
			base58.Encode(usdf.Value):  100 * ocp_common.CoreMintQuarksPerUnit,
			base58.Encode(other.Value): 30 * ocp_common.CoreMintQuarksPerUnit,
		},
		rates: map[string]float64{"eur": 0.9, "jpy": 150},
	}
	client := NewClient(log, accounts, ocp)

	userID := model.MustGenerateUserID()
	_, err := accounts.Bind(ctx, userID, model.MustGenerateKeyPair().Proto())
	require.NoError(t, err)

	// One unfiltered read, in every currency asked for, answers the total
	// and any set of mints, exactly as a read filtered to them would.
	holdings, err := client.GetHoldings(ctx, userID, []currency_lib.Code{currency_lib.EUR, currency_lib.JPY})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"eur", "jpy"}, ocp.lastReq.CurrencyCodes)
	require.Empty(t, ocp.lastReq.Mints)
	for _, mints := range [][]*commonpb.PublicKey{nil, {usdf}, {other}, {usdf, other}, {other, usdf, other}, {unheld}, {usdf, unheld}} {
		wantUsdf, err := client.GetTotalUsdfBalance(ctx, userID, mints...)
		require.NoError(t, err)
		require.Equal(t, wantUsdf, holdings.Usdf(mints...), mints)
		for _, currency := range []currency_lib.Code{currency_lib.EUR, currency_lib.JPY} {
			want, err := client.GetTotalFiatValue(ctx, userID, currency, mints...)
			require.NoError(t, err)
			got, err := holdings.FiatValue(currency, mints...)
			require.NoError(t, err)
			require.InDelta(t, want, got, 1e-9, mints)
		}
	}
	require.Equal(t, uint64(130*ocp_common.CoreMintQuarksPerUnit), holdings.Usdf())
	require.Equal(t, uint64(100*ocp_common.CoreMintQuarksPerUnit), holdings.Usdf(usdf))
	require.Zero(t, holdings.Usdf(unheld))

	// A filtered read passes the filter through, and answers its own mints.
	holdings, err = client.GetHoldings(ctx, userID, []currency_lib.Code{currency_lib.EUR}, usdf)
	require.NoError(t, err)
	require.Len(t, ocp.lastReq.Mints, 1)
	require.Equal(t, uint64(100*ocp_common.CoreMintQuarksPerUnit), holdings.Usdf(usdf))
	eur, err := holdings.FiatValue(currency_lib.EUR, usdf)
	require.NoError(t, err)
	require.InDelta(t, 100*0.9, eur, 1e-9)

	// A currency OCP does not value, or one not asked for, is unsupported for
	// every mint, never a zero.
	holdings, err = client.GetHoldings(ctx, userID, []currency_lib.Code{currency_lib.EUR, "xyz"})
	require.NoError(t, err)
	for _, mints := range [][]*commonpb.PublicKey{nil, {usdf}} {
		_, err = holdings.FiatValue("xyz", mints...)
		require.ErrorIs(t, err, ErrUnsupportedCurrency)
		_, err = holdings.FiatValue(currency_lib.JPY, mints...)
		require.ErrorIs(t, err, ErrUnsupportedCurrency)
	}

	// No currency is the USDF figures' read.
	holdings, err = client.GetHoldings(ctx, userID, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(130*ocp_common.CoreMintQuarksPerUnit), holdings.Usdf())
	require.Empty(t, ocp.lastReq.CurrencyCodes)

	// An owner OCP has no accounts for holds nothing, valued at zero in every
	// currency asked for, over any mints.
	ocp.unknownOwners = true
	holdings, err = client.GetHoldings(ctx, userID, []currency_lib.Code{currency_lib.EUR})
	require.NoError(t, err)
	require.Zero(t, holdings.Usdf())
	require.Zero(t, holdings.Usdf(usdf))
	eur, err = holdings.FiatValue(currency_lib.EUR, usdf)
	require.NoError(t, err)
	require.Zero(t, eur)
	ocp.unknownOwners = false

	// A user with no bound key is not found; failures surface.
	_, err = client.GetHoldings(ctx, model.MustGenerateUserID(), nil)
	require.ErrorIs(t, err, ErrNotFound)
	ocp.err = errors.New("ocp is down")
	_, err = client.GetHoldings(ctx, userID, nil)
	require.Error(t, err)
}
