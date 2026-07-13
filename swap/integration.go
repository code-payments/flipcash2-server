package swap

import (
	"context"
	"sync"
	"time"

	"github.com/mr-tron/base58"
	"go.uber.org/zap"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	ocp_commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/push"
	"github.com/code-payments/flipcash2-server/settings"
	ocp_currency_lib "github.com/code-payments/ocp-server/currency"
	ocp_balance_util "github.com/code-payments/ocp-server/ocp/balance"
	"github.com/code-payments/ocp-server/ocp/common"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_currency_util "github.com/code-payments/ocp-server/ocp/currency"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	ocp_account "github.com/code-payments/ocp-server/ocp/data/account"
	ocp_currency "github.com/code-payments/ocp-server/ocp/data/currency"
	ocp_currency_exchange "github.com/code-payments/ocp-server/ocp/data/currency/exchange"
	ocp_currency_reserve "github.com/code-payments/ocp-server/ocp/data/currency/reserve"
	ocp_integration "github.com/code-payments/ocp-server/ocp/integration"
	"github.com/code-payments/ocp-server/usdc"
)

const gainProcessingBatchSize = 256

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

func (i *Integration) OnSwapSubmitted(ctx context.Context, owner *common.Account, fromMint, toMint *ocp_common.Account) error {
	if fromMint.PublicKey().ToBase58() == usdc.Mint && ocp_common.IsCoreMint(toMint) {
		userID, err := i.accounts.GetUserId(ctx, &commonpb.PublicKey{Value: owner.PublicKey().ToBytes()})
		if err != nil {
			return err
		}

		return push.SendUsdfDepositProcessingPush(ctx, i.pusher, userID)
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

	// Find all PRIMARY account holders for this mint
	accountInfos, err := i.ocpData.GetAccountInfosByMintAndType(ctx, mintBase58, ocp_commonpb.AccountType_PRIMARY)
	if err != nil {
		log.Warn("failed to get account infos by mint", zap.Error(err))
		return
	}
	if len(accountInfos) == 0 {
		return
	}

	// Process account infos in batches in parallel
	var wg sync.WaitGroup
	for start := 0; start < len(accountInfos); start += gainProcessingBatchSize {
		end := start + gainProcessingBatchSize
		if end > len(accountInfos) {
			end = len(accountInfos)
		}

		wg.Add(1)
		go func(batch []*ocp_account.Record) {
			defer wg.Done()
			i.notifyHoldersOfGainBatch(ctx, log, mint, currencyName, exchangeRates, batch, buyer)
		}(accountInfos[start:end])
	}
	wg.Wait()
}

func (i *Integration) notifyHoldersOfGainBatch(ctx context.Context, log *zap.Logger, mint *ocp_common.Account, currencyName string, exchangeRates *ocp_currency.MultiRateRecord, accountInfos []*ocp_account.Record, buyer *common.Account) {
	mintBase58 := mint.PublicKey().ToBase58()
	protoMint := &commonpb.PublicKey{Value: mint.PublicKey().ToBytes()}

	// Build owner → token account mapping while collecting public keys
	ownerToTokenAccount := make(map[string]*ocp_common.Account, len(accountInfos))
	var pubKeys []*commonpb.PublicKey
	for _, info := range accountInfos {
		decoded, err := base58.Decode(info.OwnerAccount)
		if err != nil {
			log.Warn("failed to decode owner account", zap.String("owner", info.OwnerAccount), zap.Error(err))
			continue
		}

		tokenAccount, err := ocp_common.NewAccountFromPublicKeyString(info.TokenAccount)
		if err != nil {
			log.Warn("failed to parse token account", zap.String("token_account", info.TokenAccount), zap.Error(err))
			continue
		}

		pubKeys = append(pubKeys, &commonpb.PublicKey{Value: decoded})
		ownerToTokenAccount[info.OwnerAccount] = tokenAccount
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

	// Collect owners that have user IDs and push tokens
	var owners []string
	for ownerBase58, userID := range userIDMap {
		if _, ok := hasToken[string(userID.Value)]; ok {
			owners = append(owners, ownerBase58)
		}
	}

	// Batch get USD cost basis for owners with push tokens
	costBasesByOwner, err := i.ocpData.GetUsdCostBasisBatch(ctx, mintBase58, owners...)
	if err != nil {
		log.Warn("failed to batch get cost basis", zap.Error(err))
		return
	}

	// Collect token accounts for owners with push tokens and positive cost basis
	var tokenAccounts []*ocp_common.Account
	tokenAccountToOwner := make(map[string]string)
	for _, owner := range owners {
		_, ok := costBasesByOwner[owner]
		if !ok {
			continue
		}
		tokenAccount, ok := ownerToTokenAccount[owner]
		if !ok {
			continue
		}
		tokenAccounts = append(tokenAccounts, tokenAccount)
		tokenAccountToOwner[tokenAccount.PublicKey().ToBase58()] = owner
	}
	if len(tokenAccounts) == 0 {
		return
	}

	// Filter out owners whose Timelock account is not in the locked state. The
	// token account of a PRIMARY account is the Timelock vault address.
	vaultAddresses := make([]string, len(tokenAccounts))
	for i, tokenAccount := range tokenAccounts {
		vaultAddresses[i] = tokenAccount.PublicKey().ToBase58()
	}
	timelockRecordsByVault, err := i.ocpData.GetTimelockByVaultBatch(ctx, vaultAddresses...)
	if err != nil {
		log.Warn("failed to batch get timelock records", zap.Error(err))
		return
	}
	lockedTokenAccounts := make([]*ocp_common.Account, 0)
	for _, tokenAccount := range tokenAccounts {
		record, ok := timelockRecordsByVault[tokenAccount.PublicKey().ToBase58()]
		if !ok || !record.IsLocked() {
			continue
		}
		lockedTokenAccounts = append(lockedTokenAccounts, tokenAccount)
	}
	tokenAccounts = lockedTokenAccounts
	if len(tokenAccounts) == 0 {
		return
	}

	// Batch calculate balances using the default cache-based calculator
	balances, err := ocp_balance_util.BatchCalculateFromCacheWithTokenAccounts(ctx, i.ocpData, tokenAccounts...)
	if err != nil {
		log.Warn("failed to batch calculate balances", zap.Error(err))
		return
	}

	// Send push to each holder with positive gain
	now := time.Now()
	for tokenAccountBase58, quarks := range balances {
		if quarks == 0 {
			continue
		}

		owner := tokenAccountToOwner[tokenAccountBase58]
		userID := userIDMap[owner]
		usdCostBasis := costBasesByOwner[owner]

		log := log.With(zap.String("owner", owner))

		// Calculate current USD market value of the balance
		usdValue, err := ocp_currency_util.CalculateUsdMarketValueFromTokenAmount(ctx, i.ocpData, i.ocpExchangeRates, i.ocpReserveStates, mint, quarks, now)
		if err != nil {
			log.Warn("failed to calculate usd market value", zap.Error(err))
			continue
		}

		if usdValue < 0.01 {
			continue
		}

		usdGain := usdValue - usdCostBasis
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

		if owner != buyer.PublicKey().ToBase58() {
			push.SendFlipcashCurrencyGainPush(ctx, i.pusher, userID, protoMint, currencyName, userRegionSetting, gain)
		}
	}
}
