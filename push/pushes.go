package push

import (
	"context"
	"fmt"

	"golang.org/x/text/language"
	"golang.org/x/text/message"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"

	"github.com/code-payments/flipcash2-server/localization"
	ocp_currency "github.com/code-payments/ocp-server/currency"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
)

var (
	defaultLocale = language.English
	amountPrinter = message.NewPrinter(defaultLocale)
)

func SendUsdfDepositedPush(ctx context.Context, pusher Pusher, user *commonpb.UserId, usdMarketValue float64) error {
	title := "USD Reserves Now Available"
	body := amountPrinter.Sprintf(
		"$%.2f was added to your USD Reserves",
		usdMarketValue,
	)
	customPayload := &pushpb.Payload{
		Navigation: &pushpb.Navigation{
			Type: &pushpb.Navigation_CurrencyInfo{
				CurrencyInfo: &commonpb.PublicKey{Value: ocp_common.CoreMintAccount.PublicKey().ToBytes()},
			},
		},
	}
	return pusher.SendPushes(ctx, title, body, customPayload, user)
}

func SendFlipcashCurrencyDepositedPush(ctx context.Context, pusher Pusher, user *commonpb.UserId, mint *commonpb.PublicKey, currencyName string, usdMarketValue float64) error {
	title := fmt.Sprintf("%s Now Available", currencyName)
	body := amountPrinter.Sprintf(
		"$%.2f of %s was added to your Flipcash wallet",
		usdMarketValue,
		currencyName,
	)
	customPayload := &pushpb.Payload{
		Navigation: &pushpb.Navigation{
			Type: &pushpb.Navigation_CurrencyInfo{
				CurrencyInfo: mint,
			},
		},
	}
	return pusher.SendPushes(ctx, title, body, customPayload, user)
}

func SendFlipcashCurrencyBoughtPush(ctx context.Context, pusher Pusher, user *commonpb.UserId, mint *commonpb.PublicKey, currencyName string, region ocp_currency.Code, nativeAmount float64) error {
	title := fmt.Sprintf("%s Successfully Purchased", currencyName)
	body := amountPrinter.Sprintf(
		"%s of %s was added to your Flipcash wallet",
		localization.FormatFiat(defaultLocale, region, nativeAmount),
		currencyName,
	)
	customPayload := &pushpb.Payload{
		Navigation: &pushpb.Navigation{
			Type: &pushpb.Navigation_CurrencyInfo{
				CurrencyInfo: mint,
			},
		},
	}
	return pusher.SendPushes(ctx, title, body, customPayload, user)
}

func SendFlipcashCurrencySoldPush(ctx context.Context, pusher Pusher, user *commonpb.UserId, mint *commonpb.PublicKey, currencyName string, region ocp_currency.Code, nativeAmount float64) error {
	title := fmt.Sprintf("%s Successfully Sold", currencyName)
	body := amountPrinter.Sprintf(
		"%s was added to your USD Reserves",
		localization.FormatFiat(defaultLocale, region, nativeAmount),
	)
	customPayload := &pushpb.Payload{
		Navigation: &pushpb.Navigation{
			Type: &pushpb.Navigation_CurrencyInfo{
				CurrencyInfo: mint,
			},
		},
	}
	return pusher.SendPushes(ctx, title, body, customPayload, user)
}
