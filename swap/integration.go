package swap

import (
	"context"
	"sync"
	"time"

	"github.com/mr-tron/base58"
	"go.uber.org/zap"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/push"
	"github.com/code-payments/flipcash2-server/settings"
	ocp_currency_lib "github.com/code-payments/ocp-server/currency"
	ocp_query "github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/common"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	ocp_balance "github.com/code-payments/ocp-server/ocp/data/balance"
	ocp_currency "github.com/code-payments/ocp-server/ocp/data/currency"
	ocp_currency_exchange "github.com/code-payments/ocp-server/ocp/data/currency/exchange"
	ocp_currency_reserve "github.com/code-payments/ocp-server/ocp/data/currency/reserve"
	ocp_integration "github.com/code-payments/ocp-server/ocp/integration"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	"github.com/code-payments/ocp-server/usdc"
)

const (
	gainProcessingBatchSize = 256
	minGainQuarks           = 1
)

type Integration struct {
	log *zap.Logger

	accounts         account.Store
	pushes           push.Store
	settings         settings.Store
	ocpData          ocp_data.Provider
	ocpExchangeRates ocp_currency_exchange.Store
	ocpReserveStates ocp_currency_reserve.Store

	pusher push.Pusher

	enableGainPushes         bool
	gainPushCooldown         time.Duration
	mintsProcessingForGainMu sync.Mutex
	mintsProcessingForGain   map[string]struct{}
	gainPushCache            map[string]cachedGainState
}

// cachedGainState is this instance's local view of a mint's gain-push gate.
type cachedGainState struct {
	lastPushAt    time.Time
	highestSupply uint64 // circulating supply (quarks) at lastPushAt
}

func NewIntegration(
	log *zap.Logger,
	accounts account.Store,
	pushes push.Store,
	settings settings.Store,
	ocpData ocp_data.Provider,
	ocpExchangeRates ocp_currency_exchange.Store,
	ocpReserveStates ocp_currency_reserve.Store,
	pusher push.Pusher,
	enableGainPushes bool,
	gainPushCooldown time.Duration,
) ocp_integration.Swap {
	return &Integration{
		log: log,

		accounts:         accounts,
		pushes:           pushes,
		settings:         settings,
		ocpData:          ocpData,
		ocpExchangeRates: ocpExchangeRates,
		ocpReserveStates: ocpReserveStates,

		pusher: pusher,

		enableGainPushes:       enableGainPushes,
		gainPushCooldown:       gainPushCooldown,
		mintsProcessingForGain: make(map[string]struct{}),
		gainPushCache:          make(map[string]cachedGainState),
	}
}

func (i *Integration) OnSwapSubmitted(ctx context.Context, owner *common.Account, fromMint, toMint *ocp_common.Account, amount uint64) error {
	if fromMint.PublicKey().ToBase58() == usdc.Mint && ocp_common.IsCoreMint(toMint) {
		userID, err := i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: owner.PublicKey().ToBytes()})
		if err != nil {
			return err
		}

		return push.SendUsdfDepositProcessingPush(ctx, i.pusher, userID, float64(amount)/float64(usdc.QuarksPerUsdc))
	}

	return nil
}

func (i *Integration) OnSwapFinalized(ctx context.Context, owner *ocp_common.Account, isBuy bool, mint *ocp_common.Account, currencyName string, region ocp_currency_lib.Code, amountReceived float64) error {
	i.notifyCurrencyBoughtOrSold(ctx, owner, isBuy, mint, currencyName, region, amountReceived)
	if isBuy && i.enableGainPushes {
		i.notifyHoldersOfGain(ctx, mint, currencyName, owner)
	}
	return nil
}

func (i *Integration) notifyCurrencyBoughtOrSold(ctx context.Context, owner *ocp_common.Account, isBuy bool, mint *ocp_common.Account, currencyName string, region ocp_currency_lib.Code, amountReceived float64) {
	log := i.log.With(
		zap.String("mint", mint.PublicKey().ToBase58()),
		zap.String("owner", owner.PublicKey().ToBase58()),
		zap.Bool("is_buy", isBuy),
	)

	protoMint := &commonpb.PublicKey{Value: mint.PublicKey().ToBytes()}

	userID, err := i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: owner.PublicKey().ToBytes()})
	if err != nil {
		log.Warn("failed to get user id for swap owner", zap.Error(err))
		return
	}

	if isBuy {
		push.SendFlipcashCurrencyBoughtPush(ctx, i.pusher, userID, protoMint, currencyName, region, amountReceived)
	} else {
		push.SendFlipcashCurrencySoldPush(ctx, i.pusher, userID, protoMint, currencyName, region, amountReceived)
	}
}

func (i *Integration) notifyHoldersOfGain(ctx context.Context, mint *ocp_common.Account, currencyName string, buyer *common.Account) {
	mintBase58 := mint.PublicKey().ToBase58()
	log := i.log.With(zap.String("mint", mintBase58))

	i.mintsProcessingForGainMu.Lock()
	// Local short-circuit so concurrent buys for the same mint on this instance
	// don't each kick off a holder enumeration.
	if _, ok := i.mintsProcessingForGain[mintBase58]; ok {
		i.mintsProcessingForGainMu.Unlock()
		return
	}
	// Best-effort in-memory cooldown gate: if this instance already sent a gain
	// push for this mint within the cooldown window, skip the Postgres round trip
	// (and the reserve lookup) entirely. Postgres still enforces the cooldown
	// authoritatively (and across instances); this only avoids redundant work.
	cached, hasCached := i.gainPushCache[mintBase58]
	if hasCached && i.gainPushCooldown > 0 && time.Since(cached.lastPushAt) < i.gainPushCooldown {
		i.mintsProcessingForGainMu.Unlock()
		return
	}
	i.mintsProcessingForGain[mintBase58] = struct{}{}
	i.mintsProcessingForGainMu.Unlock()

	defer func() {
		i.mintsProcessingForGainMu.Lock()
		delete(i.mintsProcessingForGain, mintBase58)
		i.mintsProcessingForGainMu.Unlock()
	}()

	// Only notify holders when the currency reaches a new all-time high in
	// circulating supply, rate-limited per mint by the gain push cooldown. The
	// store performs this check-and-update atomically (and consistently across
	// server instances); the all-time high only advances when a push is granted.
	liveReserve, err := i.ocpReserveStates.GetLiveReserve(ctx, mintBase58)
	if err != nil {
		log.Warn("failed to get live reserve state", zap.Error(err))
		return
	}

	// If the live supply has not exceeded the highest supply we've already pushed
	// for, it cannot be a new all-time high, so skip the Postgres round trip.
	// Postgres only advances the all-time high on a granted push, so our cached
	// high never exceeds the stored value — making this a safe suppression.
	if hasCached && liveReserve.SupplyFromBonding <= cached.highestSupply {
		return
	}

	protoMint := &commonpb.PublicKey{Value: mint.PublicKey().ToBytes()}
	granted, state, err := i.pushes.ClaimGainPush(ctx, protoMint, liveReserve.SupplyFromBonding, liveReserve.Slot, i.gainPushCooldown)
	if err != nil {
		log.Warn("failed to claim gain push", zap.Error(err))
		return
	}

	// Populate the cache from the authoritative stored state, whether or not the
	// push was granted, so subsequent buys within the cooldown or below this high
	// skip the round trip above. The returned state reflects other instances'
	// updates too (a higher all-time high, or a more recent push elsewhere).
	if state != nil && state.LastGainPushAt != nil {
		entry := cachedGainState{highestSupply: state.AllTimeHighSupply, lastPushAt: *state.LastGainPushAt}
		i.mintsProcessingForGainMu.Lock()
		i.gainPushCache[mintBase58] = entry
		i.mintsProcessingForGainMu.Unlock()
	}

	if !granted {
		return
	}

	// Get all exchange rates for computing gains in each user's preferred region
	exchangeRates, err := i.ocpExchangeRates.GetAllExchangeRates(ctx, time.Now())
	if err != nil {
		log.Warn("failed to get all exchange rates", zap.Error(err))
		return
	}

	// Page over the holders of this mint, which the balance ledger indexes by
	// mint and balance, so accounts that were opened and emptied are never
	// walked. Each page is processed while the next is fetched.
	var wg sync.WaitGroup
	defer wg.Wait()

	cursor := ocp_query.EmptyCursor
	for {
		balanceRecords, err := i.ocpData.GetAllLockedBalancesByMint(ctx, mintBase58, minGainQuarks, cursor, gainProcessingBatchSize, ocp_query.Ascending)
		if err == ocp_balance.ErrRecordNotFound {
			return
		} else if err != nil {
			log.Warn("failed to get balances by mint", zap.Error(err))
			return
		}

		// Advance off the raw page, so a page that filters down to nothing still
		// moves the cursor forward.
		cursor = ocp_query.ToCursor(balanceRecords[len(balanceRecords)-1].Id)
		isLastPage := len(balanceRecords) < gainProcessingBatchSize

		// A record that isn't backfilled only carries the deltas seen since the
		// ledger started tracking the account, which is not the holding and can
		// even be negative, so it's skipped rather than valued off a balance
		// that's wrong in an unbounded direction.
		holdings := make([]*ocp_balance.Record, 0, len(balanceRecords))
		for _, balanceRecord := range balanceRecords {
			if !balanceRecord.IsOpen || balanceRecord.Quarks <= 0 {
				continue
			}
			holdings = append(holdings, balanceRecord)
		}

		if len(holdings) > 0 {
			wg.Go(func() {
				i.notifyHoldersOfGainBatch(ctx, log, mint, currencyName, exchangeRates, liveReserve.SupplyFromBonding, holdings, buyer)
			})
		}

		if isLastPage {
			return
		}
	}
}

func (i *Integration) notifyHoldersOfGainBatch(ctx context.Context, log *zap.Logger, mint *ocp_common.Account, currencyName string, exchangeRates *ocp_currency.MultiRateRecord, supplyFromBonding uint64, holdings []*ocp_balance.Record, buyer *common.Account) {
	protoMint := &commonpb.PublicKey{Value: mint.PublicKey().ToBytes()}

	// Build owner → holding mapping while collecting public keys
	holdingByOwner := make(map[string]*ocp_balance.Record, len(holdings))
	var pubKeys []*commonpb.PublicKey
	for _, holding := range holdings {
		decoded, err := base58.Decode(holding.OwnerAccount)
		if err != nil {
			log.Warn("failed to decode owner account", zap.String("owner", holding.OwnerAccount), zap.Error(err))
			continue
		}

		pubKeys = append(pubKeys, &commonpb.PublicKey{Value: decoded})
		holdingByOwner[holding.OwnerAccount] = holding
	}
	if len(pubKeys) == 0 {
		return
	}

	// Batch lookup user IDs
	userIDMap, err := i.accounts.GetUserIds(ctx, pubKeys)
	if err != nil {
		log.Warn("failed to batch get user ids", zap.Error(err))
		return
	}
	if len(userIDMap) == 0 {
		return
	}

	// Filter to users that have push tokens registered
	var allUserIDs []*commonpb.UserId
	for _, userID := range userIDMap {
		allUserIDs = append(allUserIDs, userID)
	}
	usersWithTokens, err := i.pushes.FilterUsersWithTokens(ctx, allUserIDs...)
	if err != nil {
		log.Warn("failed to filter users with push tokens", zap.Error(err))
		return
	}
	if len(usersWithTokens) == 0 {
		return
	}

	// Build set of user IDs with tokens for fast lookup
	hasToken := make(map[string]struct{}, len(usersWithTokens))
	for _, userID := range usersWithTokens {
		hasToken[string(userID.Value)] = struct{}{}
	}

	// Collect owners that have user IDs and push tokens, excluding the buyer,
	// who is notified of the buy itself rather than of a gain
	buyerBase58 := buyer.PublicKey().ToBase58()
	var owners []string
	for ownerBase58, userID := range userIDMap {
		if ownerBase58 == buyerBase58 {
			continue
		}
		if _, ok := holdingByOwner[ownerBase58]; !ok {
			continue
		}
		if _, ok := hasToken[string(userID.Value)]; ok {
			owners = append(owners, ownerBase58)
		}
	}
	if len(owners) == 0 {
		return
	}

	// Send push to each holder with positive gain
	for _, owner := range owners {
		holding := holdingByOwner[owner]
		userID := userIDMap[owner]

		log := log.With(zap.String("owner", owner))

		usdfSellValueInQuarks, _ := currencycreator.EstimateSell(&currencycreator.EstimateSellArgs{
			CurrentSupplyInQuarks: supplyFromBonding,
			SellAmountInQuarks:    uint64(holding.Quarks),
			ValueMintDecimals:     uint8(ocp_common.CoreMintDecimals),
			SellFeeBps:            0,
		})
		usdValue := float64(usdfSellValueInQuarks) / float64(ocp_common.CoreMintQuarksPerUnit)
		if usdValue < 0.01 {
			continue
		}

		usdGain := usdValue - ocp_balance.UsdCostBasisToFloat(holding.UsdCostBasis)
		if usdGain <= 0.01 {
			continue
		}

		userSettings, err := i.settings.GetSettings(ctx, userID)
		if err != nil {
			log.Warn("failed to get user settings", zap.Error(err))
			continue
		}
		userRegionSetting := ocp_currency_lib.Code(userSettings.Region.Value)

		// Calculate gain in the user's region
		exchangeRate, ok := exchangeRates.Rates[userSettings.Region.Value]
		if !ok {
			continue
		}
		gain := exchangeRate * usdGain

		push.SendFlipcashCurrencyGainPush(ctx, i.pusher, userID, protoMint, currencyName, userRegionSetting, gain)
	}
}
