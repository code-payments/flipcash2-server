package send

import (
	"math"

	currency_lib "github.com/code-payments/ocp-server/currency"
)

// MinimumTransferValue is the smallest amount of a currency OCP transfers: one
// unit at the currency's last decimal place (currency.GetDecimals), 0.01 for
// USD and 1 for a currency with no minor unit. It is the floor a group's
// minimum balance requirement must meet and the least a reaction boost may
// pay. OCP's own amount validation is the source of the definition; it is not
// exported, so the arithmetic is repeated here.
func MinimumTransferValue(code currency_lib.Code) float64 {
	return math.Pow10(-currency_lib.GetDecimals(code))
}
