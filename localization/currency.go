package localization

import (
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"golang.org/x/text/number"

	ocpcurrency "github.com/code-payments/ocp-server/currency"
)

var symbolByCurrency = map[ocpcurrency.Code]string{
	ocpcurrency.AED: "د.إ",
	ocpcurrency.AFN: "؋",
	ocpcurrency.ALL: "Lek",
	ocpcurrency.ANG: "ƒ",
	ocpcurrency.AOA: "Kz",
	ocpcurrency.ARS: "$",
	ocpcurrency.AUD: "$",
	ocpcurrency.AWG: "ƒ",
	ocpcurrency.AZN: "₼",
	ocpcurrency.BAM: "KM",
	ocpcurrency.BDT: "৳",
	ocpcurrency.BBD: "$",
	ocpcurrency.BGN: "лв",
	ocpcurrency.BMD: "$",
	ocpcurrency.BND: "$",
	ocpcurrency.BOB: "$b",
	ocpcurrency.BRL: "R$",
	ocpcurrency.BSD: "$",
	ocpcurrency.BWP: "P",
	ocpcurrency.BYN: "Br",
	ocpcurrency.BZD: "BZ$",
	ocpcurrency.CAD: "$",
	ocpcurrency.CHF: "CHF",
	ocpcurrency.CLP: "$",
	ocpcurrency.CNY: "¥",
	ocpcurrency.COP: "$",
	ocpcurrency.CRC: "₡",
	ocpcurrency.CUC: "$",
	ocpcurrency.CUP: "₱",
	ocpcurrency.CZK: "Kč",
	ocpcurrency.DKK: "kr",
	ocpcurrency.DOP: "RD$",
	ocpcurrency.EGP: "£",
	ocpcurrency.ERN: "£",
	ocpcurrency.EUR: "€",
	ocpcurrency.FJD: "$",
	ocpcurrency.FKP: "£",
	ocpcurrency.GBP: "£",
	ocpcurrency.GEL: "₾",
	ocpcurrency.GGP: "£",
	ocpcurrency.GHS: "¢",
	ocpcurrency.GIP: "£",
	ocpcurrency.GNF: "FG",
	ocpcurrency.GTQ: "Q",
	ocpcurrency.GYD: "$",
	ocpcurrency.HKD: "$",
	ocpcurrency.HNL: "L",
	ocpcurrency.HRK: "kn",
	ocpcurrency.HUF: "Ft",
	ocpcurrency.IDR: "Rp",
	ocpcurrency.ILS: "₪",
	ocpcurrency.IMP: "£",
	ocpcurrency.INR: "₹",
	ocpcurrency.IRR: "﷼",
	ocpcurrency.ISK: "kr",
	ocpcurrency.JEP: "£",
	ocpcurrency.JMD: "J$",
	ocpcurrency.JPY: "¥",
	ocpcurrency.KGS: "лв",
	ocpcurrency.KHR: "៛",
	ocpcurrency.KMF: "CF",
	ocpcurrency.KPW: "₩",
	ocpcurrency.KRW: "₩",
	ocpcurrency.KYD: "$",
	ocpcurrency.KZT: "лв",
	ocpcurrency.LAK: "₭",
	ocpcurrency.LBP: "£",
	ocpcurrency.LKR: "₨",
	ocpcurrency.LRD: "$",
	ocpcurrency.LTL: "Lt",
	ocpcurrency.LVL: "Ls",
	ocpcurrency.MGA: "Ar",
	ocpcurrency.MKD: "ден",
	ocpcurrency.MMK: "K",
	ocpcurrency.MNT: "₮",
	ocpcurrency.MUR: "₨",
	ocpcurrency.MXN: "$",
	ocpcurrency.MYR: "RM",
	ocpcurrency.MZN: "MT",
	ocpcurrency.NAD: "$",
	ocpcurrency.NGN: "₦",
	ocpcurrency.NIO: "C$",
	ocpcurrency.NOK: "kr",
	ocpcurrency.NPR: "₨",
	ocpcurrency.NZD: "$",
	ocpcurrency.OMR: "﷼",
	ocpcurrency.PAB: "B/.",
	ocpcurrency.PEN: "S/.",
	ocpcurrency.PHP: "₱",
	ocpcurrency.PKR: "₨",
	ocpcurrency.PLN: "zł",
	ocpcurrency.PYG: "Gs",
	ocpcurrency.QAR: "﷼",
	ocpcurrency.RON: "lei",
	ocpcurrency.RSD: "Дин.",
	ocpcurrency.RUB: "₽",
	ocpcurrency.RWF: "RF",
	ocpcurrency.SAR: "﷼",
	ocpcurrency.SBD: "$",
	ocpcurrency.SCR: "₨",
	ocpcurrency.SEK: "kr",
	ocpcurrency.SGD: "$",
	ocpcurrency.SHP: "£",
	ocpcurrency.SOS: "S",
	ocpcurrency.SRD: "$",
	ocpcurrency.SSP: "£",
	ocpcurrency.STD: "Db",
	ocpcurrency.SVC: "$",
	ocpcurrency.SYP: "£",
	ocpcurrency.THB: "฿",
	ocpcurrency.TOP: "T$",
	ocpcurrency.TRY: "₺",
	ocpcurrency.TTD: "TT$",
	ocpcurrency.TWD: "NT$",
	ocpcurrency.UAH: "₴",
	ocpcurrency.USD: "$",
	ocpcurrency.UYU: "$U",
	ocpcurrency.UZS: "лв",
	ocpcurrency.VND: "₫",
	ocpcurrency.XCD: "$",
	ocpcurrency.YER: "﷼",
	ocpcurrency.ZAR: "R",
	ocpcurrency.ZMW: "ZK",
}

// FormatFiat formats a currency amount into a string in the provided locale
func FormatFiat(locale language.Tag, currency ocpcurrency.Code, amount float64) string {
	isRtlScript := isRtlScript(locale)

	decimals := ocpcurrency.GetDecimals(currency)

	printer := message.NewPrinter(locale)
	amountAsDecimal := number.Decimal(amount, number.Scale(decimals))
	formattedAmount := printer.Sprint(amountAsDecimal)

	symbol := symbolByCurrency[currency]

	if isRtlScript {
		return formattedAmount + symbol
	}
	return symbol + formattedAmount
}
