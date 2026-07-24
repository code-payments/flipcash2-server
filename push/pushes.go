package push

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"golang.org/x/text/language"
	"golang.org/x/text/message"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"
	pushpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/push/v1"

	"github.com/code-payments/flipcash2-server/badge"
	"github.com/code-payments/flipcash2-server/localization"
	ocp_currency "github.com/code-payments/ocp-server/currency"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
)

var (
	defaultLocale = language.English
	amountPrinter = message.NewPrinter(defaultLocale)
)

func SendUsdfDepositedPush(ctx context.Context, pusher Pusher, user *commonpb.UserId, dollarAmount float64) error {
	title := amountPrinter.Sprintf("$%.2f Added", dollarAmount)
	body := "You can now spend it in Flipcash"
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

func SendUsdfDepositProcessingPush(ctx context.Context, pusher Pusher, user *commonpb.UserId, dollarAmount float64) error {
	title := amountPrinter.Sprintf("Adding $%.2f", dollarAmount)
	body := "Processing is almost complete"
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
		Navigation: &pushpb.Navigation{
			Type: &pushpb.Navigation_ChatContactPhoneNumber{
				ChatContactPhoneNumber: &phonepb.PhoneNumber{Value: joinedPhone.Value},
			},
		},
	}
	return pusher.SendPushes(ctx, title, body, customPayload, users...)
}

// SendContactDmPush notifies recipients of a new message in a contact DM. The
// title is a contact substitution on the sender's phone number, which the
// recipient's client resolves against their address book.
func SendContactDmPush(ctx context.Context, pusher Pusher, badges badge.Store, ocpData ocp_data.Provider, chatId *commonpb.ChatId, message *messagingpb.Message, senderID *commonpb.UserId, senderContact *phonepb.PhoneNumber, recipients ...*commonpb.UserId) error {
	body, ok, err := renderDmMessagePushBody(ctx, ocpData, message)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	title := "{0}"
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
		ChatMetadata: &pushpb.ChatMetadata{
			SendingUserId: senderID,
			Type:          chatpb.ChatType_CONTACT_DM,
		},
	}

	return sendDmMessagePush(ctx, pusher, badges, title, body, customPayload, recipients...)
}

// SendTipDmPush notifies recipients of a new message in a tip DM. The sender
// is typically not in the recipient's contacts, so the title carries the
// sender's display name directly rather than a contact substitution — and
// never the sender's phone number, which is private in a tip DM.
func SendTipDmPush(ctx context.Context, pusher Pusher, badges badge.Store, ocpData ocp_data.Provider, chatId *commonpb.ChatId, message *messagingpb.Message, senderID *commonpb.UserId, senderDisplayName string, recipients ...*commonpb.UserId) error {
	body, ok, err := renderDmMessagePushBody(ctx, ocpData, message)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	customPayload := &pushpb.Payload{
		Category: pushpb.Payload_CHAT,
		GroupKey: base64.StdEncoding.EncodeToString(chatId.Value),
		Navigation: &pushpb.Navigation{
			Type: &pushpb.Navigation_ChatId{
				ChatId: chatId,
			},
		},
		ChatMetadata: &pushpb.ChatMetadata{
			SendingUserId: senderID,
			Type:          chatpb.ChatType_TIP_DM,
		},
	}

	return sendDmMessagePush(ctx, pusher, badges, senderDisplayName, body, customPayload, recipients...)
}

// renderDmMessagePushBody renders the push body for a DM message. ok is false
// for content types that don't produce a push.
func renderDmMessagePushBody(ctx context.Context, ocpData ocp_data.Provider, message *messagingpb.Message) (body string, ok bool, err error) {
	switch content := message.Content[0].Type.(type) {
	case *messagingpb.Content_Text:
		body = content.Text.Text
	case *messagingpb.Content_Reply:
		// Push the reply's wrapped content. Only text replies are supported today.
		if len(content.Reply.Content) == 0 {
			return "", false, nil
		}
		textContent, ok := content.Reply.Content[0].Type.(*messagingpb.Content_Text)
		if !ok {
			return "", false, nil
		}
		body = textContent.Text.Text
	case *messagingpb.Content_Cash:
		currencyName, err := resolveCurrencyName(ctx, ocpData, content.Cash.Amount.Mint)
		if err != nil {
			return "", false, err
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
		return "", false, nil
	}

	if len(body) > 1024 {
		body = fmt.Sprintf("%s...", body[:1024])
	}

	return body, true, nil
}

// sendDmMessagePush sends a rendered DM message push and bumps each
// recipient's badge count.
func sendDmMessagePush(ctx context.Context, pusher Pusher, badges badge.Store, title, body string, customPayload *pushpb.Payload, recipients ...*commonpb.UserId) error {
	if err := pusher.SendPushes(ctx, title, body, customPayload, recipients...); err != nil {
		return err
	}

	// Each recipient now has one more unread message. Bump their badge count and
	// push the new total to their iOS devices (a no-op for non-iOS recipients).
	// Best-effort per recipient: one failure must not skip the others, and a
	// missed bump self-heals on the next message.
	var errs error
	for _, recipient := range recipients {
		newCount, err := badges.Increment(ctx, recipient, 1)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}
		if err := pusher.SendBadgeCountPush(ctx, recipient, newCount); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
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
