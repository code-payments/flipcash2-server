package profile

import (
	"errors"
	"math"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	currency_lib "github.com/code-payments/ocp-server/currency"

	"github.com/code-payments/flipcash2-server/tip"
)

var ErrInvalidMinDmChatInitFee = errors.New("invalid min dm chat init fee")

// ValidateMinDmChatInitFee reports whether fee is one a user may set as their
// minimum DM chat initialization fee. The fee may be in any currency that has
// tip presets, and must be finite and no smaller than that currency's tip
// minimum, so the floor enforced on a tip is also the floor on what a user may
// ask of whoever initializes a DM with them. Returns ErrInvalidMinDmChatInitFee
// otherwise.
func ValidateMinDmChatInitFee(fee *commonpb.FiatPaymentAmount) error {
	if fee == nil {
		return ErrInvalidMinDmChatInitFee
	}
	presets, ok := tip.PresetsFor(currency_lib.Code(fee.Currency))
	if !ok {
		return ErrInvalidMinDmChatInitFee
	}
	if math.IsNaN(fee.NativeAmount) || math.IsInf(fee.NativeAmount, 0) {
		return ErrInvalidMinDmChatInitFee
	}
	if fee.NativeAmount < presets.Minimum {
		return ErrInvalidMinDmChatInitFee
	}
	return nil
}

// MinDmChatInitFeeFromStored resolves a user's stored minimum DM chat
// initialization fee into the amount their profile carries, or nil for a user
// who has not set one. Both columns are set together, so a row with only one of
// them is treated as unset rather than half-read.
func MinDmChatInitFeeFromStored(storedCurrency *string, storedNativeAmount *float64) *commonpb.FiatPaymentAmount {
	if storedCurrency == nil || storedNativeAmount == nil {
		return nil
	}
	return &commonpb.FiatPaymentAmount{
		Currency:     *storedCurrency,
		NativeAmount: *storedNativeAmount,
	}
}
