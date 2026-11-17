package flipcashteam

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTeamGroupKeySecret(t *testing.T) {
	want := bytes.Repeat([]byte{0xab}, teamGroupKeySecretSize)
	encoded := hex.EncodeToString(want)
	str := func(s string) *string { return &s }

	t.Run("hex string", func(t *testing.T) {
		for _, s := range []string{encoded, strings.ToUpper(encoded), "  " + encoded + "\n"} {
			got, err := parseTeamGroupKeySecret(str(s), nil)
			require.NoError(t, err)
			assert.Equal(t, want, got)
		}
	})

	t.Run("binary", func(t *testing.T) {
		raw := bytes.Clone(want)
		got, err := parseTeamGroupKeySecret(nil, raw)
		require.NoError(t, err)
		assert.Equal(t, want, got)

		// The result does not alias the response's bytes.
		raw[0] = 0
		assert.Equal(t, want, got)
	})

	t.Run("refused", func(t *testing.T) {
		for name, tc := range map[string]struct {
			s *string
			b []byte
		}{
			"no value":            {},
			"empty string":        {s: str("")},
			"short hex":           {s: str(encoded[:62])},
			"long hex":            {s: str(encoded + "ab")},
			"odd-length hex":      {s: str(encoded[:63])},
			"not hex":             {s: str("zz" + encoded[2:])},
			"double encoded":      {s: str(hex.EncodeToString([]byte(encoded)))},
			"short binary":        {b: want[:31]},
			"long binary":         {b: append(bytes.Clone(want), 0)},
			"raw bytes as string": {s: str(string(want))},
		} {
			t.Run(name, func(t *testing.T) {
				got, err := parseTeamGroupKeySecret(tc.s, tc.b)
				require.Error(t, err)
				assert.Nil(t, got)
				// The value never appears in the error.
				assert.NotContains(t, strings.ToLower(err.Error()), encoded[:16])
			})
		}
	})
}
