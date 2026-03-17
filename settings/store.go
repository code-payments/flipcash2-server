package settings

import (
	"context"
	"errors"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

var (
	DefaultRegion = &commonpb.Region{Value: "usd"}
	DefaultLocale = &commonpb.Locale{Value: "en"}

	ErrNotFound = errors.New("not found")
)

type Settings struct {
	Region *commonpb.Region
	Locale *commonpb.Locale
}

type Store interface {
	// GetSettings returns the settings for a user, or ErrNotFound.
	GetSettings(ctx context.Context, userID *commonpb.UserId) (*Settings, error)

	// SetRegion sets the region for a user, provided they exist.
	//
	// ErrNotFound is returned if the user does not exist.
	SetRegion(ctx context.Context, userID *commonpb.UserId, region *commonpb.Region) error

	// SetLocale sets the locale for a user, provided they exist.
	//
	// ErrNotFound is returned if the user does not exist.
	SetLocale(ctx context.Context, userID *commonpb.UserId, locale *commonpb.Locale) error
}
