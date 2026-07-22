package composite

import (
	"context"

	"github.com/code-payments/flipcash2-server/moderation"
)

type client struct {
	textClient         moderation.Client
	imageClient        moderation.Client
	currencyNameClient moderation.Client
	displayNameClient  moderation.Client
}

// NewClient creates a moderation client that delegates each classification
// method to a dedicated implementation.
func NewClient(textClient, imageClient, currencyNameClient, displayNameClient moderation.Client) moderation.Client {
	return &client{
		textClient:         textClient,
		imageClient:        imageClient,
		currencyNameClient: currencyNameClient,
		displayNameClient:  displayNameClient,
	}
}

func (c *client) ClassifyText(ctx context.Context, text string) (*moderation.Result, error) {
	return c.textClient.ClassifyText(ctx, text)
}

func (c *client) ClassifyImage(ctx context.Context, data []byte) (*moderation.Result, error) {
	return c.imageClient.ClassifyImage(ctx, data)
}

func (c *client) ClassifyCurrencyName(ctx context.Context, name string) (*moderation.Result, error) {
	return c.currencyNameClient.ClassifyCurrencyName(ctx, name)
}

func (c *client) ClassifyDisplayName(ctx context.Context, name string) (*moderation.Result, error) {
	return c.displayNameClient.ClassifyDisplayName(ctx, name)
}
