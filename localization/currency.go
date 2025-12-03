package localization

import (
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"golang.org/x/text/number"

	ocp_currency "github.com/code-payments/ocp-server/currency"
)

var symbolByCurrency = map[ocp_currency.Code]string{
	ocp_currency.AED: "د.إ",
	ocp_currency.AFN: "؋",
	ocp_currency.ALL: "Lek",
	ocp_currency.ANG: "ƒ",
	ocp_currency.AOA: "Kz",
	ocp_currency.ARS: "$",
	ocp_currency.AUD: "$",
	ocp_currency.AWG: "ƒ",
	ocp_currency.AZN: "₼",
	ocp_currency.BAM: "KM",
	ocp_currency.BDT: "৳",
	ocp_currency.BBD: "$",
	ocp_currency.BGN: "лв",
	ocp_currency.BMD: "$",
	ocp_currency.BND: "$",
	ocp_currency.BOB: "$b",
	ocp_currency.BRL: "R$",
	ocp_currency.BSD: "$",
	ocp_currency.BWP: "P",
	ocp_currency.BYN: "Br",
	ocp_currency.BZD: "BZ$",
	ocp_currency.CAD: "$",
	ocp_currency.CHF: "CHF",
	ocp_currency.CLP: "$",
	ocp_currency.CNY: "¥",
	ocp_currency.COP: "$",
	ocp_currency.CRC: "₡",
	ocp_currency.CUC: "$",
	ocp_currency.CUP: "₱",
	ocp_currency.CZK: "Kč",
	ocp_currency.DKK: "kr",
	ocp_currency.DOP: "RD$",
	ocp_currency.EGP: "£",
	ocp_currency.ERN: "£",
	ocp_currency.EUR: "€",
	ocp_currency.FJD: "$",
	ocp_currency.FKP: "£",
	ocp_currency.GBP: "£",
	ocp_currency.GEL: "₾",
	ocp_currency.GGP: "£",
	ocp_currency.GHS: "¢",
	ocp_currency.GIP: "£",
	ocp_currency.GNF: "FG",
	ocp_currency.GTQ: "Q",
	ocp_currency.GYD: "$",
	ocp_currency.HKD: "$",
	ocp_currency.HNL: "L",
	ocp_currency.HRK: "kn",
	ocp_currency.HUF: "Ft",
	ocp_currency.IDR: "Rp",
	ocp_currency.ILS: "₪",
	ocp_currency.IMP: "£",
	ocp_currency.INR: "₹",
	ocp_currency.IRR: "﷼",
	ocp_currency.ISK: "kr",
	ocp_currency.JEP: "£",
	ocp_currency.JMD: "J$",
	ocp_currency.JPY: "¥",
	ocp_currency.KGS: "лв",
	ocp_currency.KHR: "៛",
	ocp_currency.KMF: "CF",
	ocp_currency.KPW: "₩",
	ocp_currency.KRW: "₩",
	ocp_currency.KYD: "$",
	ocp_currency.KZT: "лв",
	ocp_currency.LAK: "₭",
	ocp_currency.LBP: "£",
	ocp_currency.LKR: "₨",
	ocp_currency.LRD: "$",
	ocp_currency.LTL: "Lt",
	ocp_currency.LVL: "Ls",
	ocp_currency.MGA: "Ar",
	ocp_currency.MKD: "ден",
	ocp_currency.MMK: "K",
	ocp_currency.MNT: "₮",
	ocp_currency.MUR: "₨",
	ocp_currency.MXN: "$",
	ocp_currency.MYR: "RM",
	ocp_currency.MZN: "MT",
	ocp_currency.NAD: "$",
	ocp_currency.NGN: "₦",
	ocp_currency.NIO: "C$",
	ocp_currency.NOK: "kr",
	ocp_currency.NPR: "₨",
	ocp_currency.NZD: "$",
	ocp_currency.OMR: "﷼",
	ocp_currency.PAB: "B/.",
	ocp_currency.PEN: "S/.",
	ocp_currency.PHP: "₱",
	ocp_currency.PKR: "₨",
	ocp_currency.PLN: "zł",
	ocp_currency.PYG: "Gs",
	ocp_currency.QAR: "﷼",
	ocp_currency.RON: "lei",
	ocp_currency.RSD: "Дин.",
	ocp_currency.RUB: "₽",
	ocp_currency.RWF: "RF",
	ocp_currency.SAR: "﷼",
	ocp_currency.SBD: "$",
	ocp_currency.SCR: "₨",
	ocp_currency.SEK: "kr",
	ocp_currency.SGD: "$",
	ocp_currency.SHP: "£",
	ocp_currency.SOS: "S",
	ocp_currency.SRD: "$",
	ocp_currency.SSP: "£",
	ocp_currency.STD: "Db",
	ocp_currency.SVC: "$",
	ocp_currency.SYP: "£",
	ocp_currency.THB: "฿",
	ocp_currency.TOP: "T$",
	ocp_currency.TRY: "₺",
	ocp_currency.TTD: "TT$",
	ocp_currency.TWD: "NT$",
	ocp_currency.UAH: "₴",
	ocp_currency.USD: "$",
	ocp_currency.UYU: "$U",
	ocp_currency.UZS: "лв",
	ocp_currency.VND: "₫",
	ocp_currency.XCD: "$",
	ocp_currency.YER: "﷼",
	ocp_currency.ZAR: "R",
	ocp_currency.ZMW: "ZK",
}

// FormatFiat formats a currency amount into a string in the provided locale
func FormatFiat(locale language.Tag, currency ocp_currency.Code, amount float64) string {
	isRtlScript := isRtlScript(locale)

	decimals := ocp_currency.GetDecimals(currency)

	printer := message.NewPrinter(locale)
	amountAsDecimal := number.Decimal(amount, number.Scale(decimals))
	formattedAmount := printer.Sprint(amountAsDecimal)

	symbol := symbolByCurrency[currency]

	if isRtlScript {
		return formattedAmount + symbol
	}
	return symbol + formattedAmount
}
