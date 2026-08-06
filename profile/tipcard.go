package profile

import (
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"
)

// DefaultTipCardColorHex is the Tip Card colour a user gets before they pick
// one of their own.
const DefaultTipCardColorHex = "#000000"

// DefaultTipCardCustomization returns the Tip Card customization for a user who
// has customized nothing. UserProfile.tip_card_customization is a required
// field, so every profile the store hands out carries one, resolved from the
// user's choices where they exist and from this default where they do not.
func DefaultTipCardCustomization() *profilepb.TipCardCustomization {
	return &profilepb.TipCardCustomization{
		Color: &commonpb.Color{Hex: DefaultTipCardColorHex},
	}
}
