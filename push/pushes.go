package push

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"google.golang.org/protobuf/proto"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
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
	title := fmt.Sprintf("%s Successfully Converted", currencyName)
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
	title := fmt.Sprintf("%s Successfully Converted", currencyName)
	body := amountPrinter.Sprintf(
		"%s was added to your Flipcash wallet",
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

func SendContactJoinedFlipcashPush(ctx context.Context, pusher Pusher, joinedPhone *commonpb.PhoneNumber, users ...*commonpb.UserId) error {
	if len(users) == 0 {
		return nil
	}
	title := "{0} Joined Flipcash"
	body := "You can now send them cash"
	customPayload := &pushpb.Payload{
		Category: pushpb.Payload_CONTACT_JOIN,
		GroupKey: pushpb.Payload_CONTACT_JOIN.String(),
		TitleSubstitutions: []*commonpb.Substitution{
			{
				Fallback: joinedPhone.Value,
				Kind: &commonpb.Substitution_PhoneNumberToContactName{
					PhoneNumberToContactName: joinedPhone,
				},
			},
		},
		Navigation: &pushpb.Navigation{
			Type: &pushpb.Navigation_ChatContactPhoneNumber{
				ChatContactPhoneNumber: &commonpb.PhoneNumber{Value: joinedPhone.Value},
			},
		},
	}
	return pusher.SendPushes(ctx, title, body, customPayload, users...)
}

// ChatMessagePush is a chat message's notification, rendered once and sent to
// any number of recipient pages (see Send). Rendering is the part of a push
// that is the same for every recipient — the title, the body, the payload,
// and for cash content a currency-name lookup — so a fan-out that walks a
// large group's roster in pages builds it once per message rather than once
// per page. A nil *ChatMessagePush from a builder means the message's content
// earns no push (an unsupported content type, or a sender missing what the
// title needs), which is not an error.
type ChatMessagePush struct {
	title, body string

	// payload is the notification as the unmuted half of an audience receives
	// it; mutedPayload is the same payload flagged for the muted half (see
	// ChatRecipients). The muted copy is cloned at build time so no send
	// mutates what another may still be reading.
	payload, mutedPayload *pushpb.Payload
}

// BuildContactDmPush renders a new message in a contact DM. The title is a
// contact substitution on the sender's phone number, which the recipient's
// client resolves against their address book.
func BuildContactDmPush(ctx context.Context, ocpData ocp_data.Provider, chatId *commonpb.ChatId, message *messagingpb.Message, senderID *commonpb.UserId, senderContact *commonpb.PhoneNumber) (*ChatMessagePush, error) {
	body, ok, err := renderDmMessagePushBody(ctx, ocpData, message)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	title := "{0}"
	customPayload := &pushpb.Payload{
		Category: pushpb.Payload_CHAT,
		GroupKey: base64.StdEncoding.EncodeToString(chatId.Value),
		TitleSubstitutions: []*commonpb.Substitution{
			{
				Fallback: senderContact.Value,
				Kind: &commonpb.Substitution_PhoneNumberToContactName{
					PhoneNumberToContactName: senderContact,
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
			MessageRef:    &pushpb.ChatMetadata_Message{Message: message},
		},
	}

	return newChatMessagePush(title, body, customPayload)
}

// BuildTipDmPush renders a new message in a tip DM. The sender is typically
// not in the recipient's contacts, so the title carries the sender's display
// name directly rather than a contact substitution — and never the sender's
// phone number, which is private in a tip DM.
func BuildTipDmPush(ctx context.Context, ocpData ocp_data.Provider, chatId *commonpb.ChatId, message *messagingpb.Message, senderID *commonpb.UserId, senderDisplayName string) (*ChatMessagePush, error) {
	body, ok, err := renderDmMessagePushBody(ctx, ocpData, message)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
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
			MessageRef:    &pushpb.ChatMetadata_Message{Message: message},
		},
	}

	return newChatMessagePush(senderDisplayName, body, customPayload)
}

// BuildGroupChatPush renders a new message in a group chat. The notification
// is titled by the group ("Untitled Group" when it has none), with the sender
// identified by display name in the body ("Alice: hello"). Like a tip DM, a
// group push never carries the sender's phone number, which is private
// outside contact DMs.
func BuildGroupChatPush(ctx context.Context, ocpData ocp_data.Provider, chatId *commonpb.ChatId, message *messagingpb.Message, senderID *commonpb.UserId, senderDisplayName, chatTitle string) (*ChatMessagePush, error) {
	body, ok, err := renderGroupChatMessagePushBody(ctx, ocpData, message, senderDisplayName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	title := chatTitle
	if title == "" {
		title = "Untitled Group"
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
			Type:          chatpb.ChatType_GROUP,
			MessageRef:    &pushpb.ChatMetadata_Message{Message: message},
		},
	}

	return newChatMessagePush(title, body, customPayload)
}

// maxChatPushBytes is the most a chat push may measure (see payloadSize) and
// still carry the full message. FCM and APNs each refuse a payload over 4096
// bytes. The rest is headroom for what the measurement does not see: the
// muted copy's flag, and whatever a provider adds around what it is given.
const maxChatPushBytes = 3500

// newChatMessagePush pairs the rendered payload with its muted copy. The flag
// is the only difference. A nil payload — which the pusher accepts as empty —
// is copied as an empty one rather than cloned, since proto.Clone of a nil
// interface is nil and asserts to nothing.
//
// A payload carrying the full message that measures over maxChatPushBytes
// carries only the message's ID instead, for the client to fetch the message
// by (see pushpb.ChatMetadata); the title, body and sender are unchanged. The
// decision is made once per message, here: every recipient gets the same
// title, body and payload but for the badge, which the measurement allows for,
// and the muted copy is cloned after it, so both halves agree.
func newChatMessagePush(title, body string, customPayload *pushpb.Payload) (*ChatMessagePush, error) {
	if message := customPayload.GetChatMetadata().GetMessage(); message != nil {
		size, err := payloadSize(title, body, customPayload)
		if err != nil {
			return nil, err
		}
		if size > maxChatPushBytes {
			customPayload.ChatMetadata.MessageRef = &pushpb.ChatMetadata_MessageId{MessageId: message.MessageId}
		}
	}

	mutedPayload := &pushpb.Payload{}
	if customPayload != nil {
		mutedPayload = proto.Clone(customPayload).(*pushpb.Payload)
	}
	if mutedPayload.ChatMetadata == nil {
		mutedPayload.ChatMetadata = &pushpb.ChatMetadata{}
	}
	mutedPayload.ChatMetadata.Muted = true
	return &ChatMessagePush{
		title:        title,
		body:         body,
		payload:      customPayload,
		mutedPayload: mutedPayload,
	}, nil
}

// renderGroupChatMessagePushBody renders the push body for a group chat
// message. Bodies read in the third person, naming the sender — a group push
// announces activity in a room titled by the group, not a personal message.
// ok is false for content types that don't produce a push.
func renderGroupChatMessagePushBody(ctx context.Context, ocpData ocp_data.Provider, message *messagingpb.Message, senderDisplayName string) (body string, ok bool, err error) {
	switch content := message.Content[0].Type.(type) {
	case *messagingpb.Content_Text:
		body = fmt.Sprintf("%s: %s", senderDisplayName, content.Text.Text)
	case *messagingpb.Content_Reply:
		// Push the reply's wrapped content. Only text replies are supported today.
		if len(content.Reply.Content) == 0 {
			return "", false, nil
		}
		textContent, ok := content.Reply.Content[0].Type.(*messagingpb.Content_Text)
		if !ok {
			return "", false, nil
		}
		body = fmt.Sprintf("%s: %s", senderDisplayName, textContent.Text.Text)
	case *messagingpb.Content_Cash:
		currencyName, err := resolveCurrencyName(ctx, ocpData, content.Cash.Amount.Mint)
		if err != nil {
			return "", false, err
		}
		// Unknown verbs fall back to sent, matching what clients render. Cash
		// bodies name the sender rather than the DM renderer's "you", which has
		// no referent when everyone in the group but the sender receives the
		// same push.
		verb := "sent"
		if content.Cash.GetVerb() == messagingpb.CashContent_TIPPED {
			verb = "tipped"
		}
		body = fmt.Sprintf(
			"%s %s %s of %s",
			senderDisplayName,
			verb,
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

	return truncatePushBody(body), true, nil
}

// maxPushBodyChars is the most characters of rendered text a chat push body
// carries. A notification shows a few lines at most, and the body is repeated
// in the payload (on iOS twice), so a longer one only spends the provider's
// payload budget on text nobody sees.
const maxPushBodyChars = 100

// truncatePushBody cuts body to its first maxPushBodyChars characters, marking
// the cut with "...". Characters are counted as runes and the cut falls on a
// rune boundary, so a multi-byte character is never split into invalid UTF-8.
func truncatePushBody(body string) string {
	var n int
	for i := range body {
		if n == maxPushBodyChars {
			return body[:i] + "..."
		}
		n++
	}
	return body
}

// encryptedDmMessagePushBody is the push body for an encrypted DM message,
// whose content the server cannot render.
const encryptedDmMessagePushBody = "Sent you a message"

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
	case *messagingpb.Content_Encrypted:
		// The server cannot read encrypted content, so the body is generic. A
		// payload within maxChatPushBytes carries the message, so a client able
		// to decrypt it before display can replace the body with the plaintext;
		// a larger one carries only the message ID, for the client to fetch.
		body = encryptedDmMessagePushBody
	case *messagingpb.Content_Cash:
		currencyName, err := resolveCurrencyName(ctx, ocpData, content.Cash.Amount.Mint)
		if err != nil {
			return "", false, err
		}
		// Unknown verbs fall back to SENT, matching what clients render.
		verb := "Sent"
		if content.Cash.GetVerb() == messagingpb.CashContent_TIPPED {
			verb = "Tipped"
		}
		body = fmt.Sprintf(
			"%s you %s of %s",
			verb,
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

	return truncatePushBody(body), true, nil
}

// ChatRecipients is a chat push's audience, split by whether each recipient has
// the chat muted. The unmuted half receives the alert with a badge bump. The
// muted half receives the same payload flagged (ChatMetadata.muted) and
// without a badge bump, since a muted chat must not move the app icon's
// count — and only on Android, where the client can deliver the message
// quietly; the FCMPusher drops a muted recipient's iOS devices, which could
// not suppress the alert, so an iOS user hears nothing from a chat they
// muted. Which half a recipient lands in is the sender's best-effort call at
// send time; the client's own copy of its mute is the last word.
type ChatRecipients struct {
	Unmuted []*commonpb.UserId
	Muted   []*commonpb.UserId
}

// Send delivers the push to both halves of one page of its audience: the
// unmuted half with each recipient's badge bumped and the new total carried
// on the notification, so a recipient's icon updates with the same push that
// announces the message rather than a second, badge-only push per recipient;
// the muted half flagged, unbadged, as a copy of the same payload, to Android
// devices only. Either half may be empty and costs nothing then. The two sends are independent,
// and a failure in one is reported alongside the other's. Send may be called
// once per page of a large audience; each call is its own token lookup,
// badge batch and FCM send, so what a call holds is bounded by its page.
func (p *ChatMessagePush) Send(ctx context.Context, pusher Pusher, badges badge.Store, recipients ChatRecipients) error {
	var errs []error
	if len(recipients.Unmuted) > 0 {
		// Each recipient now has one more unread message. The pusher asks for
		// counts only for recipients with an iOS device, so the stored count is
		// bumped only where a badge can be displayed. The batch is best-effort
		// per recipient: a failed bump leaves that recipient's badge off this
		// push but must not block the others, and a missed bump self-heals on
		// the next message.
		incrementBadges := func(ctx context.Context, users []*commonpb.UserId) (BadgeCounts, error) {
			counts, err := badges.IncrementBatch(ctx, users, 1)
			return BadgeCounts(counts), err
		}
		errs = append(errs, pusher.SendPushesWithBadges(ctx, p.title, p.body, p.payload, incrementBadges, recipients.Unmuted...))
	}
	if len(recipients.Muted) > 0 {
		errs = append(errs, pusher.SendPushes(ctx, p.title, p.body, p.mutedPayload, recipients.Muted...))
	}
	return errors.Join(errs...)
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
