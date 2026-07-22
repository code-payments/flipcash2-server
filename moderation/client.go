package moderation

import (
	"context"
	"errors"
)

// ErrUnsupportedLanguage is returned when the moderation service does not
// support the language of the provided text.
var ErrUnsupportedLanguage = errors.New("unsupported language")

type Result struct {
	// Is the piece of data flaggged as unsafe?
	Flagged bool

	// Categories that caused the data to be flagged
	FlaggedCategories []string

	// Various category scores applied to the piece of data
	CategoryScores map[string]float64
}

type Client interface {
	// ClassifyText classifies the provided text for moderation. The result
	// indicates whether the text was flagged and includes per-category scores.
	ClassifyText(ctx context.Context, text string) (*Result, error)

	// ClassifyImage classifies the provided image data for moderation. The
	// result indicates whether the image was flagged and includes per-category
	// scores.
	ClassifyImage(ctx context.Context, data []byte) (*Result, error)

	// ClassifyCurrencyName checks whether a currency name infringes on
	// existing trademarks, impersonates known brands, or is otherwise
	// misleading. The result includes per-category scores for areas like
	// cryptocurrency, financial_service, impersonation, etc.
	ClassifyCurrencyName(ctx context.Context, name string) (*Result, error)

	// ClassifyDisplayName checks whether a user-chosen display name abuses the
	// name field to advertise, solicit, or expose others to harmful content.
	// The result includes per-category scores for areas like solicitation,
	// contact_info, hate, etc.
	//
	// It deliberately does not score impersonation: users are free to call
	// themselves whatever they like, including the name of a real person or
	// brand. It is also distinct from ClassifyText, which is tuned for prose
	// and has little to work with in a one- or two-word name.
	ClassifyDisplayName(ctx context.Context, name string) (*Result, error)
}
