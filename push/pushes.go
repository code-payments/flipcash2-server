package push

import (
	"context"
	"fmt"

	"golang.org/x/text/language"
	"golang.org/x/text/message"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/localization"
	ocp_currency "github.com/code-payments/ocp-server/currency"
)

var (
	defaultLocale = language.English
	amountPrinter = message.NewPrinter(defaultLocale)
)

func SendUsdfReceivedFromDepositPush(ctx context.Context, pusher Pusher, user *commonpb.UserId, usdMarketValue float64) error {
	title := "Cash Reserves Now Available"
	body := amountPrinter.Sprintf(
		"$%.2f of Cash Reserves was added to your Flipcash wallet",
		usdMarketValue,
	)
	return pusher.SendBasicPushes(ctx, title, body, user)
}

func SendFlipcashCurrencyReceivedFromDepositPush(ctx context.Context, pusher Pusher, user *commonpb.UserId, currencyName string, usdMarketValue float64) error {
	title := fmt.Sprintf("%s Now Available", currencyName)
	body := amountPrinter.Sprintf(
		"$%.2f of %s was added to your Flipcash wallet",
		usdMarketValue,
		currencyName,
	)
	return pusher.SendBasicPushes(ctx, title, body, user)
}

func SendUsdfReceivedFromSwapPush(ctx context.Context, pusher Pusher, user *commonpb.UserId, region ocp_currency.Code, nativeAmount float64) error {
	title := "Cash Reserves Now Available"
	body := amountPrinter.Sprintf(
		"%s of Cash Reserves was added to your Flipcash wallet",
		localization.FormatFiat(defaultLocale, region, nativeAmount),
	)
	return pusher.SendBasicPushes(ctx, title, body, user)
}

func SendFlipcashCurrencyReceivedFromSwapPush(ctx context.Context, pusher Pusher, user *commonpb.UserId, currencyName string, region ocp_currency.Code, nativeAmount float64) error {
	title := fmt.Sprintf("%s Now Available", currencyName)
	body := amountPrinter.Sprintf(
		"%s of %s was added to your Flipcash wallet",
		localization.FormatFiat(defaultLocale, region, nativeAmount),
		currencyName,
	)
	return pusher.SendBasicPushes(ctx, title, body, user)
}
