//go:build claude

package claude

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/moderation/noop"
)

func TestClassifyCurrencyName(t *testing.T) {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		t.Fatal("ANTHROPIC_API_KEY environment variable is required")
	}

	client := NewClient(apiKey, noop.NewClient())
	ctx := context.Background()

	tests := []struct {
		category string
		inputs   []string
	}{
		{
			category: "cryptocurrency",
			inputs: []string{
				"Bitcoin",
				"Ethereum",
				"Solana",
				"Dogecoin",
				"Litecoin",
				"Cardano",
				"Ripple",
				"Polkadot",
				"Avalanche",
				"Chainlink",
			},
		},
		{
			category: "exchange_platform",
			inputs: []string{
				"Coinbase",
				"Binance",
				"Kraken",
				"Robinhood",
				"Gemini Exchange",
				"Bitfinex",
				"Bybit",
				"OKX",
				"Crypto.com",
				"eToro",
			},
		},
		{
			category: "fiat_currency",
			inputs: []string{
				"Dollar",
				"Euro",
				"Yen",
				"Pound Sterling",
				"Peso",
				"Franc",
				"Rupee",
				"Won",
				"Real",
				"Yuan",
			},
		},
		{
			category: "financial_service",
			inputs: []string{
				"Venmo",
				"PayPal",
				"Visa",
				"CashApp",
				"Stripe",
				"Mastercard",
				"Zelle",
				"Chase",
				"Wells Fargo",
				"Goldman Sachs",
			},
		},
		{
			category: "general_trademark",
			inputs: []string{
				"Nike",
				"Disney",
				"Coca-Cola",
				"McDonald's",
				"Adidas",
				"Ferrari",
				"Supreme",
				"Gucci",
				"Louis Vuitton",
				"Rolex",
			},
		},
		{
			category: "government_affiliation",
			inputs: []string{
				"Federal Reserve",
				"US Treasury",
				"Official Currency",
				"Government Backed",
				"National Reserve",
				"Central Bank Coin",
				"State Issued Token",
				"Treasury Note",
				"Federal Mint",
				"SEC Approved",
			},
		},
		{
			category: "impersonation",
			inputs: []string{
				"Bitc0in",
				"Paypall",
				"Appple",
				"G00gle",
				"Amaz0n",
				"Ethereium",
				"SoIana",
				"D0gecoin",
				"Venm0",
				"Binanse",
			},
		},
		{
			category: "misleading_backing",
			inputs: []string{
				"FDIC Insured Coin",
				"Gold Backed Token",
				"Guaranteed Returns",
				"Insured Deposit Coin",
				"Asset Backed Stable",
				"100% Reserved Token",
				"Bank Guaranteed Coin",
				"Principal Protected",
				"Risk Free Token",
				"Fully Collateralized USD",
			},
		},
		{
			category: "platform_impersonation",
			inputs: []string{
				"FlipCa$h",
				"Flipcash Token",
				"USDF Coin",
				"F1ipcash",
				"US DF",
				"Flip Cash",
				"FlipKash",
				"USDF Reserve",
				"Flipcash Official",
				"FlipCash Gold",
			},
		},
		{
			category: "public_figure",
			inputs: []string{
				"ElonCoin",
				"TrumpToken",
				"Elon Musk Coin",
				"Biden Bucks",
				"Obama Token",
				"Bezos Coin",
				"Zuckerberg Cash",
				"Taylor Swift Coin",
				"Musk Money",
				"Trump Dollar",
			},
		},
		{
			category: "stablecoin",
			inputs: []string{
				"USDC",
				"USDT",
				"Tether",
				"DAI",
				"TrueUSD",
				"Pax Dollar",
				"BUSD",
				"FRAX",
				"USDP",
				"USD Coin",
			},
		},
		{
			category: "tech_company",
			inputs: []string{
				"Apple",
				"Google",
				"Meta",
				"Amazon",
				"Microsoft",
				"Tesla",
				"Netflix",
				"Nvidia",
				"Samsung",
				"Intel",
			},
		},
	}

	for _, tt := range tests {
		for _, input := range tt.inputs {
			t.Run(tt.category+"/"+input, func(t *testing.T) {
				result, err := client.ClassifyCurrencyName(ctx, input)
				require.NoError(t, err)

				assert.True(t, result.Flagged, "expected %q to be flagged", input)
				assert.Contains(t, result.FlaggedCategories, tt.category,
					"expected %q to flag category %q, got %v (scores: %v)",
					input, tt.category, result.FlaggedCategories, result.CategoryScores)
				assert.GreaterOrEqual(t, result.CategoryScores[tt.category], currencyNameFlagThreshold,
					"expected %q score >= %.1f for category %q, got %.2f",
					input, currencyNameFlagThreshold, tt.category, result.CategoryScores[tt.category])
			})
		}
	}
}

func TestClassifyCurrencyName_AllCategoriesPresent(t *testing.T) {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		t.Fatal("ANTHROPIC_API_KEY environment variable is required")
	}

	client := NewClient(apiKey, noop.NewClient())
	ctx := context.Background()

	expectedCategories := []string{
		"cryptocurrency",
		"exchange_platform",
		"fiat_currency",
		"financial_service",
		"general_trademark",
		"government_affiliation",
		"impersonation",
		"misleading_backing",
		"platform_impersonation",
		"public_figure",
		"stablecoin",
		"tech_company",
	}

	result, err := client.ClassifyCurrencyName(ctx, "FunkyToken")
	require.NoError(t, err)

	for _, category := range expectedCategories {
		_, ok := result.CategoryScores[category]
		assert.True(t, ok, "missing category %q in response scores", category)
	}
}

func TestClassifyCurrencyName_SafeNameNotFlagged(t *testing.T) {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		t.Fatal("ANTHROPIC_API_KEY environment variable is required")
	}

	client := NewClient(apiKey, noop.NewClient())
	ctx := context.Background()

	safeNames := []string{
		"Jeffy",
		"Teddies",
		"Cattle Cash",
		"bmoney",
		"Ducat",
		"Panda",
		"LANDO",
		"Tones",
		"Walrus",
		"Moony",
		"XP",
		"Bits",
		"Float",
		"Bogey",
		"Market Coin",
	}

	for _, name := range safeNames {
		t.Run(name, func(t *testing.T) {
			result, err := client.ClassifyCurrencyName(ctx, name)
			require.NoError(t, err)

			assert.False(t, result.Flagged,
				"expected %q to not be flagged, flagged categories: %v (scores: %v)",
				name, result.FlaggedCategories, result.CategoryScores)
		})
	}
}
