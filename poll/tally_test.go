package poll

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCoarsen(t *testing.T) {
	for _, tc := range []struct {
		in, want float64
	}{
		{428.02, 430},
		{358.02, 360},
		{170, 170},
		{1234567, 1200000},
		{1612345, 1600000},
		{9.96, 10},
		{0.0567, 0.057},
		{0.001234, 0.0012},
		{1, 1},
		{0, 0},
		{-5, 0},
	} {
		require.Equal(t, tc.want, coarsen(tc.in), "coarsen(%v)", tc.in)
	}
}
