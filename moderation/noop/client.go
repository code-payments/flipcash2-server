package noop

import (
	"context"

	"github.com/code-payments/flipcash2-server/moderation"
)

type client struct {
}

func NewClient() moderation.Client {
	return &client{}
}

func (c *client) ClassifyText(ctx context.Context, text string) (*moderation.Result, error) {
	return &moderation.Result{Flagged: false, CategoryScores: make(map[string]float64)}, nil
}

func (c *client) ClassifyImage(ctx context.Context, data []byte) (*moderation.Result, error) {
	return &moderation.Result{Flagged: false, CategoryScores: make(map[string]float64)}, nil
}

func (c *client) ClassifyCurrencyName(ctx context.Context, name string) (*moderation.Result, error) {
	return &moderation.Result{Flagged: false, CategoryScores: make(map[string]float64)}, nil
}
