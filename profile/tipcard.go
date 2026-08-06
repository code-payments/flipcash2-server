package profile

import (
	"strings"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"
)

// DefaultTipCardColorHex is the Tip Card colour a user gets before they pick
// one of their own.
const DefaultTipCardColorHex = "#000000"

// NormalizeColorHex puts a colour into the canonical upper-case form it is
// stored and handed back in, so a colour round-trips as the same string
// regardless of the casing the client sent it in.
func NormalizeColorHex(hex string) string {
	return strings.ToUpper(hex)
}

// DefaultTipCardCustomization returns the Tip Card customization for a user who
// has customized nothing. UserProfile.tip_card_customization is a required
// field, so every profile the store hands out carries one, resolved from the
// user's choices where they exist and from this default where they do not.
func DefaultTipCardCustomization() *profilepb.TipCardCustomization {
	return TipCardCustomizationFromStored(nil)
}

// TipCardCustomizationFromStored resolves a user's stored Tip Card choices into
// the customization their profile carries, filling in the default for anything
// they have not picked. storedColorHex is nil for a user who never chose a
// colour. Every store resolves through here, so the "always set" contract on
// UserProfile.tip_card_customization holds no matter which one served the read.
func TipCardCustomizationFromStored(storedColorHex *string) *profilepb.TipCardCustomization {
	colorHex := DefaultTipCardColorHex
	if storedColorHex != nil {
		colorHex = NormalizeColorHex(*storedColorHex)
	}

	return &profilepb.TipCardCustomization{
		Color: &commonpb.Color{Hex: colorHex},
	}
}
