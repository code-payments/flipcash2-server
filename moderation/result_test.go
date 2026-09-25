package moderation

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResult_Ignoring(t *testing.T) {
	t.Run("nil result stays nil", func(t *testing.T) {
		var r *Result
		require.Nil(t, r.Ignoring(CategoryGibberish))
	})

	t.Run("result that did not flag the category is returned as is", func(t *testing.T) {
		r := &Result{
			Flagged:           true,
			FlaggedCategories: []string{"hate"},
			CategoryScores:    map[string]float64{"hate": 3, CategoryGibberish: 0},
		}
		require.Same(t, r, r.Ignoring(CategoryGibberish))
	})

	t.Run("verdict with no categories is left alone", func(t *testing.T) {
		r := &Result{Flagged: true}
		require.Same(t, r, r.Ignoring(CategoryGibberish))
		require.True(t, r.Flagged)
	})

	t.Run("flagged on the ignored category alone becomes clean", func(t *testing.T) {
		r := &Result{
			Flagged:           true,
			FlaggedCategories: []string{CategoryGibberish},
			CategoryScores:    map[string]float64{CategoryGibberish: 2, "hate": 0},
		}

		got := r.Ignoring(CategoryGibberish)
		require.False(t, got.Flagged)
		require.Empty(t, got.FlaggedCategories)

		// The score is kept for logging, and the original verdict is untouched.
		require.Equal(t, 2.0, got.CategoryScores[CategoryGibberish])
		require.True(t, r.Flagged)
		require.Equal(t, []string{CategoryGibberish}, r.FlaggedCategories)
	})

	t.Run("other flagged categories keep the verdict", func(t *testing.T) {
		r := &Result{
			Flagged:           true,
			FlaggedCategories: []string{CategoryGibberish, "hate", "spam"},
			CategoryScores:    map[string]float64{CategoryGibberish: 2, "hate": 3, "spam": 2},
		}

		got := r.Ignoring(CategoryGibberish, "spam")
		require.True(t, got.Flagged)
		require.Equal(t, []string{"hate"}, got.FlaggedCategories)
		require.Equal(t, []string{CategoryGibberish, "hate", "spam"}, r.FlaggedCategories)
	})
}
