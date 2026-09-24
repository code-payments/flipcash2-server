package hive

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/moderation"
)

func TestClassifyText(t *testing.T) {
	for _, tc := range []struct {
		name               string
		scores             map[string]float64
		textFilters        []textFilter
		expectedFlagged    bool
		expectedCategories []string
		expectedErr        error
	}{
		{
			name:            "benign",
			scores:          map[string]float64{},
			expectedFlagged: false,
		},
		{
			name: "implicit minor mention alone is allowed",
			scores: map[string]float64{
				minorImplicitlyMentionedCategory: 3,
			},
			expectedFlagged: false,
		},
		{
			name: "explicit minor mention alone is allowed",
			scores: map[string]float64{
				minorExplicitlyMentionedCategory: 3,
			},
			expectedFlagged: false,
		},
		{
			name: "mildly sexual text alone is allowed",
			scores: map[string]float64{
				sexualCategory: 1,
			},
			expectedFlagged: false,
		},
		{
			name: "implicit minor mention with mildly sexual text is child exploitation",
			scores: map[string]float64{
				sexualCategory:                   1,
				minorImplicitlyMentionedCategory: 3,
			},
			expectedFlagged:    true,
			expectedCategories: []string{childExploitationCategory},
		},
		{
			name: "explicit minor mention with sexual text is child exploitation",
			scores: map[string]float64{
				sexualCategory:                   2,
				minorExplicitlyMentionedCategory: 3,
			},
			expectedFlagged:    true,
			expectedCategories: []string{sexualCategory, childExploitationCategory},
		},
		{
			name: "moderately severe text is flagged",
			scores: map[string]float64{
				"bullying": 2,
			},
			expectedFlagged:    true,
			expectedCategories: []string{"bullying"},
		},
		{
			name:               "profanity filter hit is flagged",
			scores:             map[string]float64{},
			textFilters:        []textFilter{{Type: profanityFilterType, Value: "damn"}},
			expectedFlagged:    true,
			expectedCategories: []string{profanityCategory},
		},
		{
			name: "missing core category is an unsupported language",
			scores: map[string]float64{
				"sexual":   -1,
				"hate":     -1,
				"violence": -1,
				"bullying": -1,
				"spam":     -1,
			},
			expectedErr: moderation.ErrUnsupportedLanguage,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scores := map[string]float64{
				"sexual":   0,
				"hate":     0,
				"violence": 0,
				"bullying": 0,
				"spam":     0,
			}
			maps.Copy(scores, tc.scores)

			c := newTestClient(t, textResponse(scores, tc.textFilters))

			result, err := c.ClassifyText(context.Background(), "text")
			if tc.expectedErr != nil {
				require.ErrorIs(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)

			assert.Equal(t, tc.expectedFlagged, result.Flagged)
			assert.ElementsMatch(t, tc.expectedCategories, result.FlaggedCategories)
			for class, score := range scores {
				if score == -1 {
					continue
				}
				assert.Equal(t, score, result.CategoryScores[class], class)
			}
		})
	}
}

func newTestClient(t *testing.T, resp response) moderation.Client {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Token test-key", r.Header.Get("Authorization"))
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	t.Cleanup(srv.Close)

	return &client{
		apiKey:     "test-key",
		apiUrl:     srv.URL,
		httpClient: srv.Client(),
	}
}

func textResponse(scores map[string]float64, textFilters []textFilter) response {
	var classes []classResult
	for class, score := range scores {
		classes = append(classes, classResult{Class: class, Score: score})
	}

	return response{
		Status: []taskStatus{{
			Response: taskResponse{
				Output:      []taskOutput{{Classes: classes}},
				TextFilters: textFilters,
			},
		}},
	}
}
