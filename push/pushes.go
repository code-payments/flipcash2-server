package push

import (
	"context"
	"encoding/base64"
	"fmt"

	"golang.org/x/text/language"
	"golang.org/x/text/message"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"
	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"

	"github.com/code-payments/flipcash2-server/localization"
	ocp_currency "github.com/code-payments/ocp-server/currency"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
)

var (
	defaultLocale = language.English
	amountPrinter = message.NewPrinter(defaultLocale)
)

func SendUsdfDepositedPush(ctx context.Context, pusher Pusher, user *commonpb.UserId) error {
	title := "Deposit Now Available"
	body := "You can now spend your deposit in Flipcash"
	customPayload := &pushpb.Payload{
		Category: pushpb.Payload_DEPOSIT_WITHDRAWAL,
		GroupKey: pushpb.Payload_DEPOSIT_WITHDRAWAL.String(),
		Navigation: &pushpb.Navigation{
			Type: &pushpb.Navigation_CurrencyInfo{
				CurrencyInfo: &commonpb.PublicKey{Value: ocp_common.CoreMintAccount.PublicKey().ToBytes()},
			},
		},
	}
	return pusher.SendPushes(ctx, title, body, customPayload, user)
}

func SendUsdfDepositProcessingPush(ctx context.Context, pusher Pusher, user *commonpb.UserId) error {
	title := "Deposit Processing"
	body := "Deposits take about a minute"
	customPayload := &pushpb.Payload{
		Category: pushpb.Payload_DEPOSIT_WITHDRAWAL,
		GroupKey: pushpb.Payload_DEPOSIT_WITHDRAWAL.String(),
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
		Category: pushpb.Payload_DEPOSIT_WITHDRAWAL,
		GroupKey: pushpb.Payload_DEPOSIT_WITHDRAWAL.String(),
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
		Category: pushpb.Payload_BUY_SELL,
		GroupKey: pushpb.Payload_BUY_SELL.String(),
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
		"%s of USDF was added to your Flipcash wallet",
		localization.FormatFiat(defaultLocale, region, nativeAmount),
	)
	customPayload := &pushpb.Payload{
		Category: pushpb.Payload_BUY_SELL,
		GroupKey: pushpb.Payload_BUY_SELL.String(),
		Navigation: &pushpb.Navigation{
			Type: &pushpb.Navigation_CurrencyInfo{
				CurrencyInfo: mint,
			},
		},
	}
	return pusher.SendPushes(ctx, title, body, customPayload, user)
}

func SendContactJoinedFlipcashPush(ctx context.Context, pusher Pusher, joinedPhone *phonepb.PhoneNumber, users ...*commonpb.UserId) error {
	if len(users) == 0 {
		return nil
	}
	title := "{0} Joined Flipcash"
	body := "You can now send them cash"
	customPayload := &pushpb.Payload{
		Category: pushpb.Payload_CONTACT_JOIN,
		GroupKey: pushpb.Payload_CONTACT_JOIN.String(),
		TitleSubstitutions: []*pushpb.Substitution{
			{
				Fallback: joinedPhone.Value,
				Kind: &pushpb.Substitution_Contact{
					Contact: joinedPhone,
				},
			},
		},
	}
	return pusher.SendPushes(ctx, title, body, customPayload, users...)
}

func SendContactDmPush(ctx context.Context, pusher Pusher, ocpData ocp_data.Provider, chatId *commonpb.ChatId, message *messagingpb.Message, senderContact *phonepb.PhoneNumber, recipients ...*commonpb.UserId) error {
	title := "{0}"

	var body string
	switch content := message.Content[0].Type.(type) {
	case *messagingpb.Content_Text:
		body = content.Text.Text
	case *messagingpb.Content_Cash:
		currencyName, err := resolveCurrencyName(ctx, ocpData, content.Cash.Amount.Mint)
		if err != nil {
			return err
		}
		body = fmt.Sprintf(
			"Sent you %s of %s",
			localization.FormatFiat(
				defaultLocale,
				ocp_currency.Code(content.Cash.Amount.Currency),
				content.Cash.Amount.NativeAmount,
			),
			currencyName,
		)
	default:
		return nil
	}

	customPayload := &pushpb.Payload{
		Category: pushpb.Payload_CHAT,
		GroupKey: base64.StdEncoding.EncodeToString(chatId.Value),
		TitleSubstitutions: []*pushpb.Substitution{
			{
				Fallback: senderContact.Value,
				Kind: &pushpb.Substitution_Contact{
					Contact: senderContact,
				},
			},
		},
		Navigation: &pushpb.Navigation{
			Type: &pushpb.Navigation_ChatId{
				ChatId: chatId,
			},
		},
	}

	return pusher.SendPushes(ctx, title, body, customPayload, recipients...)
}

func SendFlipcashCurrencyGainPush(ctx context.Context, pusher Pusher, user *commonpb.UserId, mint *commonpb.PublicKey, currencyName string, gainRegion ocp_currency.Code, gainAmount float64) error {
	title := fmt.Sprintf("Someone just bought %s", currencyName)
	body := amountPrinter.Sprintf(
		"You're now up +%s",
		localization.FormatFiat(defaultLocale, gainRegion, gainAmount),
	)
	customPayload := &pushpb.Payload{
		Category: pushpb.Payload_GAIN,
		GroupKey: pushpb.Payload_GAIN.String(),
		Navigation: &pushpb.Navigation{
			Type: &pushpb.Navigation_CurrencyInfo{
				CurrencyInfo: mint,
			},
		},
	}
	return pusher.SendPushes(ctx, title, body, customPayload, user)
}

// todo: refactor push more broadly to use this with caching
func resolveCurrencyName(ctx context.Context, ocpData ocp_data.Provider, mint *commonpb.PublicKey) (string, error) {
	mintAccount, err := ocp_common.NewAccountFromPublicKeyBytes(mint.Value)
	if err != nil {
		return "", err
	}
	if ocp_common.IsCoreMint(mintAccount) {
		return ocp_common.CoreMintName, nil
	}
	metadata, err := ocpData.GetCurrencyMetadata(ctx, mintAccount.PublicKey().ToBase58())
	if err != nil {
		return "", err
	}
	return metadata.Name, nil
}
