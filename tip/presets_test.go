package tip

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	currency_lib "github.com/code-payments/ocp-server/currency"
)

// The table is transcribed from the fiat tip preset bundle, so guard the
// invariants a transcription error would break: usable currency codes, and
// tiers that ascend from the enforced minimum.
func TestPresets(t *testing.T) {
	// The USD row backs the minimum applied to currencies without presets of
	// their own, so callers rely on it being present.
	usd, ok := PresetsFor(currency_lib.USD)
	require.True(t, ok)
	assert.Equal(t, Presets{Minimum: 1, Low: 5, Medium: 10, High: 20}, usd)

	for _, entry := range All() {
		region, presets := entry.Region, entry.Presets

		assert.Len(t, string(region), 3, "%s is not an iso 4217 code", region)
		assert.Equal(t, strings.ToLower(string(region)), string(region), "%s must be lower case to match the exchange currency", region)

		assert.Greater(t, presets.Minimum, 0.0, "%s minimum", region)
		assert.Less(t, presets.Minimum, presets.Low, "%s minimum must sit below the low preset", region)
		assert.Less(t, presets.Low, presets.Medium, "%s low preset", region)
		assert.Less(t, presets.Medium, presets.High, "%s medium preset", region)
	}
}

func TestPresetsFor_UnknownCurrency(t *testing.T) {
	presets, ok := PresetsFor(currency_lib.Code("zzz"))
	assert.False(t, ok)
	assert.Zero(t, presets)
}

// All is what user flags are built from, so it must cover the whole table in a
// stable order.
func TestAll(t *testing.T) {
	entries := All()
	require.Len(t, entries, len(presetsByRegion))

	for i, entry := range entries {
		presets, ok := PresetsFor(entry.Region)
		require.True(t, ok, "%s is not in the table", entry.Region)
		assert.Equal(t, presets, entry.Presets)

		if i > 0 {
			assert.Less(t, string(entries[i-1].Region), string(entry.Region), "entries must be sorted by currency code")
		}
	}
}
