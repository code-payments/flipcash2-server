package poll

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRank(t *testing.T) {
	for _, tc := range []struct {
		in   []uint64
		want []uint32
	}{
		{[]uint64{30, 20, 20, 10}, []uint32{1, 2, 2, 4}},
		{[]uint64{10, 30, 0, 20}, []uint32{3, 1, 4, 2}},
		{[]uint64{360, 170}, []uint32{1, 2}},
		{[]uint64{0, 5, 0}, []uint32{2, 1, 2}},
		{[]uint64{7, 7}, []uint32{1, 1}},
	} {
		require.Equal(t, tc.want, rank(tc.in), "rank(%v)", tc.in)
	}
}
