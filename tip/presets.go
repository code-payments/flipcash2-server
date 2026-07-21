package tip

import (
	"slices"
	"strings"

	currency_lib "github.com/code-payments/ocp-server/currency"
)

// Presets are the tip amounts offered for a currency, in major currency units.
// Minimum is the floor enforced on any tip; the remaining tiers are the amounts
// clients surface as one-tap presets.
type Presets struct {
	Minimum float64
	Low     float64
	Medium  float64
	High    float64
}

// Entry pairs a currency with its presets.
type Entry struct {
	Region  currency_lib.Code
	Presets Presets
}

// ordered is presetsByCurrency sorted by currency code, so responses built from
// it are stable across calls.
var ordered = func() []Entry {
	entries := make([]Entry, 0, len(presetsByRegion))
	for region, presets := range presetsByRegion {
		entries = append(entries, Entry{Region: region, Presets: presets})
	}
	slices.SortFunc(entries, func(a, b Entry) int { return strings.Compare(string(a.Region), string(b.Region)) })
	return entries
}()

// PresetsFor returns the presets for a currency, and whether it has any.
func PresetsFor(region currency_lib.Code) (Presets, bool) {
	presets, ok := presetsByRegion[region]
	return presets, ok
}

// All returns every currency's presets, ordered by currency code.
func All() []Entry {
	return slices.Clone(ordered)
}

// presetsByRegion is the fiat tip preset table. Each minimum is a locally
// recognizable cash denomination near a one-dollar gesture, chosen for product
// familiarity rather than exact exchange-rate parity, and always sits below the
// currency's low preset. Clients receive this table through user flags, so
// changing an amount here changes what clients offer and what the server
// accepts in one step.
var presetsByRegion = map[currency_lib.Code]Presets{
	"aed": {5, 20, 50, 100},
	"afn": {50, 100, 500, 1_000},
	"all": {100, 200, 500, 1_000},
	"amd": {500, 2_000, 5_000, 10_000},
	"aoa": {500, 1_000, 5_000, 10_000},
	"ars": {1_000, 5_000, 10_000, 20_000},
	"aud": {1, 5, 10, 20},
	"awg": {2, 10, 20, 50},
	"azn": {2, 10, 20, 50},
	"bam": {2, 10, 20, 50},
	"bbd": {2, 10, 20, 50},
	"bdt": {100, 500, 1_000, 2_000},
	"bhd": {0.5, 1, 5, 10},
	"bif": {2_000, 5_000, 10_000, 20_000},
	"bmd": {1, 5, 10, 20},
	"bnd": {1, 5, 10, 20},
	"bob": {10, 50, 100, 200},
	"brl": {5, 20, 50, 100},
	"bsd": {1, 5, 10, 20},
	"btn": {50, 100, 500, 1_000},
	"bwp": {10, 50, 100, 200},
	"byn": {2, 10, 20, 50},
	"bzd": {2, 10, 20, 50},
	"cad": {1, 5, 10, 20},
	"cdf": {2_000, 5_000, 10_000, 20_000},
	"chf": {1, 5, 10, 20},
	"clp": {1_000, 5_000, 10_000, 20_000},
	"cny": {10, 20, 50, 100},
	"cop": {5_000, 20_000, 50_000, 100_000},
	"crc": {500, 2_000, 5_000, 10_000},
	"cup": {20, 100, 200, 500},
	"cve": {100, 500, 1_000, 2_000},
	"czk": {20, 100, 200, 500},
	"djf": {200, 1_000, 2_000, 5_000},
	"dkk": {10, 50, 100, 200},
	"dop": {50, 200, 500, 1_000},
	"dzd": {100, 500, 1_000, 2_000},
	"egp": {50, 200, 500, 1_000},
	"ern": {10, 50, 100, 200},
	"etb": {100, 500, 1_000, 2_000},
	"eur": {1, 5, 10, 20},
	"fjd": {2, 10, 20, 50},
	"fkp": {1, 5, 10, 20},
	"gbp": {1, 5, 10, 20},
	"gel": {2, 10, 20, 50},
	"ghs": {10, 50, 100, 200},
	"gip": {1, 5, 10, 20},
	"gmd": {100, 200, 500, 1_000},
	"gnf": {10_000, 50_000, 100_000, 200_000},
	"gtq": {10, 50, 100, 200},
	"gyd": {200, 1_000, 2_000, 5_000},
	"hkd": {10, 50, 100, 200},
	"hnl": {20, 100, 200, 500},
	"htg": {100, 500, 1_000, 2_000},
	"huf": {500, 2_000, 5_000, 10_000},
	"idr": {20_000, 50_000, 100_000, 200_000},
	"ils": {5, 20, 50, 100},
	"inr": {50, 100, 200, 500},
	"iqd": {1_000, 5_000, 10_000, 25_000},
	"irr": {100_000, 500_000, 1_000_000, 2_000_000},
	"isk": {100, 500, 1_000, 2_000},
	"jmd": {200, 500, 1_000, 2_000},
	"jod": {0.5, 1, 5, 10},
	"jpy": {100, 500, 1_000, 2_000},
	"kes": {100, 500, 1_000, 2_000},
	"kgs": {100, 500, 1_000, 2_000},
	"khr": {5_000, 20_000, 50_000, 100_000},
	"kmf": {500, 2_000, 5_000, 10_000},
	"kpw": {500, 1_000, 5_000, 10_000},
	"krw": {1_000, 5_000, 10_000, 20_000},
	"kwd": {0.25, 1, 5, 10},
	"kyd": {1, 5, 10, 20},
	"kzt": {500, 2_000, 5_000, 10_000},
	"lak": {20_000, 100_000, 200_000, 500_000},
	"lbp": {100_000, 500_000, 1_000_000, 2_000_000},
	"lkr": {500, 1_000, 2_000, 5_000},
	"lrd": {200, 500, 1_000, 2_000},
	"lsl": {20, 100, 200, 500},
	"lyd": {5, 20, 50, 100},
	"mad": {10, 50, 100, 200},
	"mdl": {20, 100, 200, 500},
	"mga": {5_000, 20_000, 50_000, 100_000},
	"mkd": {50, 200, 500, 1_000},
	"mmk": {2_000, 10_000, 20_000, 50_000},
	"mnt": {5_000, 20_000, 50_000, 100_000},
	"mop": {10, 50, 100, 200},
	"mru": {50, 200, 500, 1_000},
	"mur": {50, 200, 500, 1_000},
	"mvr": {20, 100, 200, 500},
	"mwk": {2_000, 5_000, 10_000, 20_000},
	"mxn": {20, 100, 200, 500},
	"myr": {5, 20, 50, 100},
	"mzn": {50, 200, 500, 1_000},
	"nad": {20, 100, 200, 500},
	"ngn": {1_000, 5_000, 10_000, 20_000},
	"nio": {50, 200, 500, 1_000},
	"nok": {10, 50, 100, 200},
	"npr": {100, 500, 1_000, 2_000},
	"nzd": {1, 5, 10, 20},
	"omr": {0.5, 1, 5, 10},
	"pab": {1, 5, 10, 20},
	"pen": {5, 20, 50, 100},
	"pgk": {5, 20, 50, 100},
	"php": {50, 200, 500, 1_000},
	"pkr": {200, 1_000, 2_000, 5_000},
	"pln": {5, 20, 50, 100},
	"pyg": {5_000, 20_000, 50_000, 100_000},
	"qar": {5, 20, 50, 100},
	"ron": {5, 20, 50, 100},
	"rsd": {100, 500, 1_000, 2_000},
	"rub": {100, 500, 1_000, 2_000},
	"rwf": {1_000, 5_000, 10_000, 20_000},
	"sar": {5, 20, 50, 100},
	"sbd": {10, 50, 100, 200},
	"scr": {20, 50, 100, 200},
	"sdg": {500, 2_000, 5_000, 10_000},
	"sek": {10, 50, 100, 200},
	"sgd": {1, 5, 10, 20},
	"shp": {1, 5, 10, 20},
	"sle": {20, 100, 200, 500},
	"sos": {500, 2_000, 5_000, 10_000},
	"srd": {50, 200, 500, 1_000},
	"ssp": {5_000, 20_000, 50_000, 100_000},
	"stn": {20, 100, 200, 500},
	"syp": {1_000, 5_000, 10_000, 20_000},
	"szl": {20, 100, 200, 500},
	"thb": {20, 100, 500, 1_000},
	"tjs": {10, 50, 100, 200},
	"tmt": {5, 20, 50, 100},
	"tnd": {2, 10, 20, 50},
	"top": {2, 10, 20, 50},
	"try": {50, 200, 500, 1_000},
	"ttd": {5, 20, 50, 100},
	"twd": {50, 200, 500, 1_000},
	"tzs": {2_000, 10_000, 20_000, 50_000},
	"uah": {50, 200, 500, 1_000},
	"ugx": {5_000, 20_000, 50_000, 100_000},
	"usd": {1, 5, 10, 20},
	"uyu": {50, 200, 500, 1_000},
	"uzs": {10_000, 50_000, 100_000, 200_000},
	"ves": {1_000, 5_000, 10_000, 20_000},
	"vnd": {20_000, 100_000, 200_000, 500_000},
	"vuv": {100, 500, 1_000, 2_000},
	"wst": {2, 10, 20, 50},
	"xaf": {500, 2_000, 5_000, 10_000},
	"xcd": {2, 20, 50, 100},
	"xcg": {2, 10, 20, 50},
	"xof": {500, 2_000, 5_000, 10_000},
	"xpf": {100, 500, 1_000, 2_000},
	"yer": {250, 1_000, 2_000, 5_000},
	"zar": {20, 100, 200, 500},
	"zmw": {20, 100, 200, 500},
	"zwg": {20, 100, 200, 500},
}
