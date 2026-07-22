//go:build claude

package claude

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifyCurrencyName(t *testing.T) {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		t.Fatal("ANTHROPIC_API_KEY environment variable is required")
	}

	client := NewClient(apiKey)
	ctx := context.Background()

	tests := []struct {
		category            string
		includeRandomSuffix bool
		inputs              []string
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
				"Monero",
				"Zcash",
				"Tron",
				"Stellar",
				"Tezos",
				"Algorand",
				"Cosmos",
				"NEAR Protocol",
				"Aptos",
				"Sui",
				"Filecoin",
				"Hedera",
				"Shiba Inu",
				"PEPE",
				"BONK",
				"FLOKI",
				"Uniswap",
				"Arbitrum",
				"Optimism",
				"XRP",
				"BNB",
				"Polygon",
				"MATIC",
				"WBTC",
				"wBTC",
			},
		},
		{
			category:            "exchange_platform",
			includeRandomSuffix: true,
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
				"KuCoin",
				"Huobi",
				"Gate.io",
				"MEXC",
				"Bitstamp",
				"BitMEX",
				"Upbit",
				"Bithumb",
				"FTX",
				"Poloniex",
				"LocalBitcoins",
				"Coinmama",
				"Bittrex",
				"dYdX",
				"PancakeSwap",
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
				"Korean Won",
				"Brazilian Real",
				"Yuan",
				"Ruble",
				"Krona",
				"Dirham",
				"Shekel",
				"Baht",
				"Rial",
				"Turkish Lira",
				"Dinar",
				"Zloty",
				"Forint",
				"Rand",
				"Vietnamese Dong",
				"Krone",
				"Ringgit",
				"Rupiah",
				"Naira",
				"Hryvnia",
				"Renminbi",
				"Sterling Pound",
				"US Dollar",
				"Canadian Dollar",
				"Aussie Dollar",
			},
		},
		{
			category:            "financial_service",
			includeRandomSuffix: true,
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
				"American Express",
				"Amex",
				"Discover Card",
				"Bank of America",
				"HSBC",
				"Barclays",
				"Morgan Stanley",
				"BlackRock",
				"Charles Schwab",
				"Fidelity",
				"Wise Transfer",
				"Revolut",
				"Square Cash",
				"JPMorgan",
				"Citibank",
				"Deutsche Bank",
				"UBS",
				"Vanguard",
				"Western Union",
				"MoneyGram",
				"Klarna",
				"Affirm",
				"SoFi",
			},
		},
		{
			category:            "general_trademark",
			includeRandomSuffix: true,
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
				"Starbucks",
				"Pepsi",
				"Lego",
				"Marvel",
				"Lamborghini",
				"Porsche",
				"Rolls Royce",
				"Tiffany & Co",
				"Chanel",
				"Dior",
				"Prada",
				"Hermes",
				"Cartier",
				"Red Bull",
				"Hershey's",
				"Nestle",
				"Budweiser",
				"Heineken",
				"Jack Daniels",
				"KFC",
				"Burger King",
				"Subway",
				"Walmart",
				"Target",
				"Ikea",
				"Toyota",
				"Harley Davidson",
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
				"IRS Coin",
				"FBI Token",
				"Pentagon Dollar",
				"White House Cash",
				"Department of Treasury Coin",
				"CFTC Regulated",
				"Official US Dollar",
				"Republic Reserve",
				"Presidential Dollar",
				"Congressional Cash",
				"Senate Token",
				"EU Central Bank Token",
				"Bank of England Coin",
				"Ministry of Finance",
				"Government Authorized",
				"Nationally Recognized Currency",
				"Federally Chartered Token",
				"US Mint Certified",
				"OFAC Compliant Coin",
			},
		},
		{
			category:            "impersonation",
			includeRandomSuffix: true,
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
				"Micr0soft",
				"Teslla",
				"Net flix",
				"F8cebook",
				"Bítcoin",
				"B!tcoin",
				"Bittcoin",
				"Bitcoyn",
				"Eth3reum",
				"Ethreum",
				"Litec0in",
				"C0inbase",
				"Amzon",
				"Disnney",
				"MasterCar d",
				"V1sa",
				"Mastercar",
				"Chase Banq",
				"Krakken",
				"ByBit Exchange",
				"Solarna",
				"Doggecoin",
				"Shibaa Inu",
				"0penAI",
				"Npvidia",
				"Samsumg",
				"Micros0ft",
				"Applle Inc",
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
				"SIPC Protected Coin",
				"Double Your Money",
				"Guaranteed 10% Yield",
				"Zero Risk Token",
				"Safe Haven Coin",
				"AAA Rated Crypto",
				"Lloyds Insured",
				"Platinum Backed",
				"Silver Backed Dollar",
				"Never Lose Token",
				"Zero Loss Guarantee",
				"Hedge Fund Coin",
				"Treasury Bond Token",
				"Investment Grade Coin",
				"Bulletproof Dollar",
				"Pegged to Gold 1:1",
				"Oil Reserve Backed",
				"Real Estate Secured Coin",
				"Diamond Backed",
				"Physically Backed Bullion",
				"Warranted Return Token",
				"Capital Preservation Guaranteed",
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
				"FlipCoin",
				"Flypcash",
				"FIipcash",
				"F|ipcash",
				"USDFlip",
				"Flipcash Plus",
				"Flipcash Pro",
				"Flipcash Premium",
				"Flip-Cash",
				"USDF Token",
				"The Official Flipcash",
				"U$DF",
				"USDF+",
				"Flippcash",
				"Flipcach",
				"Flipcash v2",
				"Flipkash USD",
				"Flippcash Reserve",
				"FlipCash DAO",
				"USDFiat",
				"USDFInance",
				"Flipcash Network",
				"USDFlipcoin",
			},
		},
		{
			category:            "public_figure",
			includeRandomSuffix: true,
			inputs: []string{
				"Elon",
				"Trump",
				"Elon Musk",
				"Biden",
				"Obama",
				"Bezos",
				"Zuckerberg",
				"Taylor Swift",
				"Musk",
				"Kanye Coin",
				"Beyonce",
				"Oprah",
				"Kardashian",
				"Putin",
				"Xi Jinping",
				"King Charles",
				"Vitalik",
				"Satoshi",
				"CZ Binance",
				"Warren Buffett",
				"Bill Gates",
				"Mark Zuck",
				"Kim Kardashian",
				"Drake",
				"LeBron",
				"Messi",
				"Ronaldo",
				"Jay-Z",
				"MrBeast",
				"Rihanna",
				"Jeff Bezos",
				"Joe Rogan",
				"Zelensky",
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
				"PYUSD",
				"FDUSD",
				"USDe",
				"USDD",
				"GUSD",
				"EURC",
				"sDAI",
				"sUSD",
				"crvUSD",
				"USDY",
				"LUSD",
				"USDS",
				"RLUSD",
				"Dollar Pegged Token",
				"1:1 USD Stable",
				"Fixed Dollar Coin",
				"Stable Dollar",
				"Dollar Peg Token",
				"Digital Dollar",
				"USDJ",
				"Gemini Dollar",
				"First Digital USD",
			},
		},
		{
			category:            "tech_company",
			includeRandomSuffix: true,
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
				"Oracle",
				"IBM",
				"Salesforce",
				"Adobe",
				"SpaceX",
				"OpenAI",
				"Anthropic",
				"Uber",
				"Airbnb",
				"Spotify",
				"Qualcomm",
				"Cisco Systems",
				"Dell",
				"Hewlett Packard",
				"Sony",
				"AMD",
				"Facebook",
				"Twitter",
				"TikTok",
				"Snapchat",
				"LinkedIn",
				"Alphabet Inc",
				"Alibaba",
				"Tencent",
				"Baidu",
				"ByteDance",
				"Palantir",
				"Snowflake",
				"Databricks",
			},
		},
	}

	for _, tt := range tests {
		for _, input := range tt.inputs {
			if tt.includeRandomSuffix {
				suffixes := []string{
					"Token",
					"Tokens",
					"Cash",
					"Money",
					"Dollar",
					"Dollars",
					"Coin",
				}
				input = fmt.Sprintf("%s %s", input, suffixes[rand.IntN(len(suffixes))])
			}
			t.Run(tt.category+"/"+input, func(t *testing.T) {
				result, err := client.ClassifyCurrencyName(ctx, input)
				require.NoError(t, err)

				assert.True(t, result.Flagged, "expected %q to be flagged (scores: %v)", input, result.CategoryScores)
			})
		}
	}
}

func TestClassifyCurrencyName_AllCategoriesPresent(t *testing.T) {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		t.Fatal("ANTHROPIC_API_KEY environment variable is required")
	}

	client := NewClient(apiKey)
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

	client := NewClient(apiKey)
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

// displayNameCategories are the categories the display name prompt scores. It is
// the authoritative list: TestClassifyDisplayName reports any category it has no
// fixtures for, and TestClassifyDisplayName_AllCategoriesPresent asserts the
// model returns a score for each.
var displayNameCategories = []string{
	"child_safety",
	"contact_info",
	"drugs",
	"financial_claim",
	"gibberish",
	"hate",
	"profanity",
	"self_harm",
	"sexual",
	"solicitation",
	"violence",
}

// A JSON file of {category: [name, ...]} holds the names TestClassifyDisplayName
// expects to be flagged: displayNameFixturesPath by default, overridden by the
// displayNameFixturesEnv environment variable.
//
// Every positive fixture lives there rather than in this file. A name that trips
// this classifier is, by construction, a name a public repository should not
// contain, and deciding which ones are mild enough to inline is a judgment call
// that would have to be re-litigated on every change. One rule instead: no
// flagged names in the repository. The default path is gitignored; see
// .gitignore.
const (
	displayNameFixturesEnv = "MODERATION_DISPLAY_NAME_FIXTURES"

	// Relative to this package's directory, which is the working directory
	// `go test` runs the test binary in.
	displayNameFixturesPath = "testdata/display_name_fixtures.json"
)

// loadDisplayNameFixtures reads the fixture file, returning nil when there is
// none at the default path. An explicitly configured path that cannot be read is
// a failure rather than a skip: it means coverage was asked for and not
// delivered.
func loadDisplayNameFixtures(t *testing.T) map[string][]string {
	t.Helper()

	path := os.Getenv(displayNameFixturesEnv)
	if path == "" {
		path = displayNameFixturesPath
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return nil
		}
	}

	data, err := os.ReadFile(path)
	require.NoError(t, err, "failed to read %s named by %s", path, displayNameFixturesEnv)

	var fixtures map[string][]string
	require.NoError(t, json.Unmarshal(data, &fixtures), "failed to parse %s", path)

	// A misspelled key would contribute nothing and leave the category it was
	// meant to cover silently uncovered, so reject it rather than ignore it.
	for category := range fixtures {
		require.Contains(t, displayNameCategories, category,
			"%s has unknown category %q", path, category)
	}

	return fixtures
}

// TestClassifyDisplayName checks that names which should be flagged are. Its
// inputs come entirely from displayNameFixturesEnv; without it every category
// reports itself uncovered, because a moderation suite that quietly stops
// testing is worse than one that is visibly not running.
func TestClassifyDisplayName(t *testing.T) {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		t.Fatal("ANTHROPIC_API_KEY environment variable is required")
	}

	client := NewClient(apiKey)
	ctx := context.Background()

	byCategory := loadDisplayNameFixtures(t)

	for _, category := range displayNameCategories {
		inputs := byCategory[category]
		if len(inputs) == 0 {
			t.Run(category, func(t *testing.T) {
				t.Skipf("no fixtures for %q; put a JSON file of {category: [name, ...]} at %s, or set %s to one elsewhere",
					category, displayNameFixturesPath, displayNameFixturesEnv)
			})
			continue
		}

		for _, input := range inputs {
			t.Run(category+"/"+input, func(t *testing.T) {
				result, err := client.ClassifyDisplayName(ctx, input)
				require.NoError(t, err)

				assert.True(t, result.Flagged, "expected %q to be flagged (scores: %v)", input, result.CategoryScores)
			})
		}
	}
}

func TestClassifyDisplayName_AllCategoriesPresent(t *testing.T) {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		t.Fatal("ANTHROPIC_API_KEY environment variable is required")
	}

	client := NewClient(apiKey)
	ctx := context.Background()

	result, err := client.ClassifyDisplayName(ctx, "Jamie Rivera")
	require.NoError(t, err)

	for _, category := range displayNameCategories {
		_, ok := result.CategoryScores[category]
		assert.True(t, ok, "missing category %q in response scores", category)
	}
}

// TestClassifyDisplayName_SafeNameNotFlagged is the false-positive guard, and
// the reason it is weighted toward non-English names and names that collide
// with crude words in some language: rejecting one of those is the failure that
// costs a real user their real name, and it is the failure this classifier is
// most prone to.
func TestClassifyDisplayName_SafeNameNotFlagged(t *testing.T) {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		t.Fatal("ANTHROPIC_API_KEY environment variable is required")
	}

	client := NewClient(apiKey)
	ctx := context.Background()

	safeNames := []string{
		// Ordinary names.
		"Sarah Johnson",
		"Mike",
		"Dave",
		"Jenny K",
		"Bob Smith",
		"Aisha",
		"Priya Patel",
		"Diego Hernández",

		// Non-Latin scripts. A name must not be flagged merely for being
		// unfamiliar to the classifier.
		"田中太郎",
		"김민수",
		"王小明",
		"Алексей Иванов",
		"محمد علي",
		"अमित शर्मा",
		"สมชาย",
		"יוסי כהן",
		"Γιώργος Παπαδόπουλος",
		"Nguyễn Văn An",
		"Björn Åkesson",
		"Łukasz Wójcik",

		// Real names that collide with a crude word in English. These are common
		// given names and surnames, not profanity.
		"Phuc Nguyen",
		"Bich Tran",
		"Dong-Hyun Kim",
		"Wang Fang",
		"Fanny Bergman",
		"Randy Cummings",
		"Dieter Kuntz",
		"Dick Butkus",

		// Names that collide with a brand. Impersonation is permitted, and these
		// are ordinary given names besides.
		"Mercedes Garcia",
		"Tesla Brown",
		"Alexa Reed",
		"Trader Joe",

		// Stylization, emoji, and unusual capitalization are not violations.
		"✨Sarah✨",
		"j o s h",
		"xXGamerXx",
		"ᴋᴀᴛɪᴇ",
		"🌸 Mia 🌸",
		"MiKaYlA",

		// Short names.
		"Al",
		"Jo",
		"K",
		"Bo",

		// Nicknames and handles with no solicitation or contact detail in them.
		"CryptoKing",
		"MoonBoy",
		"Diamond Hands",
		"Chart Wizard",
		"Coffee Enjoyer",

		// Known-ambiguous cases. A birth year is not a hate code and a film title
		// is not a self-harm reference; if these start failing, the prompt has
		// become too eager rather than the names having changed.
		"Mike88",
		"Born in 88",
		"Joint Effort",
		"Killer Queen",
	}

	for _, name := range safeNames {
		t.Run(name, func(t *testing.T) {
			result, err := client.ClassifyDisplayName(ctx, name)
			require.NoError(t, err)

			assert.False(t, result.Flagged,
				"expected %q to not be flagged, flagged categories: %v (scores: %v)",
				name, result.FlaggedCategories, result.CategoryScores)
		})
	}
}
