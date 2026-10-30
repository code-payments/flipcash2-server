package activity

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	activitypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/activity/v1"
	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	intentpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/intent/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	paymentrequestpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/paymentrequest/v1"

	accountmemory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/data/history"
)

// counterpartyEnv holds a payer and a recipient bound to accounts, and the
// app metadata of each kind of payment between them.
type counterpartyEnv struct {
	server *Server

	payer, recipient         *commonpb.UserId
	payerKeys, recipientKeys model.KeyPair

	dmPayment, groupPayment, reactionBoost, requestedPayment, returnPayment []byte

	// boostChatID is the group reactionBoost was paid in.
	boostChatID *commonpb.ChatId

	// requestChatID and requestID name the payment request requestedPayment
	// paid a share of, and its group.
	requestChatID *commonpb.ChatId
	requestID     *paymentrequestpb.PaymentRequestId

	// returnChatID and returnedMessageID name the cash message whose payment
	// returnPayment returned, and its DM.
	returnChatID      *commonpb.ChatId
	returnedMessageID *messagingpb.MessageId
}

func newCounterpartyEnv(t *testing.T) *counterpartyEnv {
	ctx := context.Background()
	accounts := accountmemory.NewInMemory()

	e := &counterpartyEnv{
		server:        &Server{accounts: accounts},
		payer:         model.MustGenerateUserID(),
		recipient:     model.MustGenerateUserID(),
		payerKeys:     model.MustGenerateKeyPair(),
		recipientKeys: model.MustGenerateKeyPair(),
	}
	_, err := accounts.Bind(ctx, e.payer, e.payerKeys.Proto())
	require.NoError(t, err)
	_, err = accounts.Bind(ctx, e.recipient, e.recipientKeys.Proto())
	require.NoError(t, err)

	e.dmPayment = chatAppMetadata(t, &intentpb.ChatMetadata{
		ChatId: &commonpb.ChatId{Value: make([]byte, 32)},
		Type: &intentpb.ChatMetadata_DmPayment_{
			DmPayment: &intentpb.ChatMetadata_DmPayment{Action: intentpb.PaymentAction_SEND},
		},
	})
	e.returnChatID = chat.MustDeriveDmChatID(chatpb.ChatType_DM, e.payer, e.recipient)
	e.returnedMessageID = &messagingpb.MessageId{Value: 1}
	e.returnPayment = chatAppMetadata(t, &intentpb.ChatMetadata{
		ChatId: e.returnChatID,
		Type: &intentpb.ChatMetadata_ReturnPayment_{
			ReturnPayment: &intentpb.ChatMetadata_ReturnPayment{MessageId: e.returnedMessageID},
		},
	})
	e.groupPayment = chatAppMetadata(t, &intentpb.ChatMetadata{
		ChatId: chat.MustGenerateGroupChatID(),
		Type: &intentpb.ChatMetadata_GroupPayment_{
			GroupPayment: &intentpb.ChatMetadata_GroupPayment{Action: intentpb.PaymentAction_SEND},
		},
	})
	e.requestChatID = chat.MustGenerateGroupChatID()
	e.requestID = &paymentrequestpb.PaymentRequestId{Value: bytes.Repeat([]byte{7}, 16)}
	e.requestedPayment = chatAppMetadata(t, &intentpb.ChatMetadata{
		ChatId: e.requestChatID,
		Type: &intentpb.ChatMetadata_RequestedPayment_{
			RequestedPayment: &intentpb.ChatMetadata_RequestedPayment{
				PaymentRequestId: e.requestID,
			},
		},
	})
	e.boostChatID = chat.MustGenerateGroupChatID()
	e.reactionBoost = chatAppMetadata(t, &intentpb.ChatMetadata{
		ChatId: e.boostChatID,
		Type: &intentpb.ChatMetadata_ReactionBoost_{
			ReactionBoost: &intentpb.ChatMetadata_ReactionBoost{
				MessageId: &messagingpb.MessageId{Value: 1},
				Emoji:     &messagingpb.Emoji{Value: "🔥"},
			},
		},
	})
	return e
}

func paymentRecord(id uint64, recordType history.Type, owner, counterparty string, appMetadata []byte) *history.Record {
	return &history.Record{
		Id:                       id,
		Type:                     recordType,
		OwnerAccount:             owner,
		CounterpartyOwnerAccount: &counterparty,
		ExchangeCurrency:         "usd",
		NativeAmount:             2.5,
		MintAccount:              ocp_common.CoreMintAccount.PublicKey().ToBase58(),
		Quantity:                 2_500_000,
		AppMetadata:              appMetadata,
		State:                    history.StateCompleted,
		CreatedAt:                time.Now(),
	}
}

func chatAppMetadata(t *testing.T, chatMetadata *intentpb.ChatMetadata) []byte {
	appMetadata, err := proto.Marshal(&intentpb.AppMetadata{
		Domain: &intentpb.AppMetadata_Chat{Chat: chatMetadata},
	})
	require.NoError(t, err)
	return appMetadata
}

// TestHistoryCounterparties: a group payment names the other party by user
// ID from both sides, and reads Sent and Received, exactly as a DM payment
// does, and so does a requested payment, from its payer to the request's
// creator, whose entries on both sides also name the request, and a return
// payment, whose entries on both sides also name the returned message. A reaction boost paying one person names the other party the same
// way, and the payer's side reads Boosted. A payment carrying no chat
// metadata, like one handed off in person, names no one, and the payer's side
// reads Gave.
func TestHistoryCounterparties(t *testing.T) {
	e := newCounterpartyEnv(t)

	payerAccount := base58.Encode(e.payerKeys.Public())
	recipientAccount := base58.Encode(e.recipientKeys.Public())

	for _, tc := range []struct {
		name        string
		appMetadata []byte
		named       bool
		boosted     bool
		requested   bool
		returned    bool
		sentText    string
	}{
		{"group_payment", e.groupPayment, true, false, false, false, "Sent"},
		{"requested_payment", e.requestedPayment, true, false, true, false, "Sent"},
		{"dm_payment", e.dmPayment, true, false, false, false, "Sent"},
		{"return_payment", e.returnPayment, true, false, false, true, "Sent"},
		{"reaction_boost", e.reactionBoost, true, true, false, false, "Boosted"},
		{"no_chat_metadata", nil, false, false, false, false, "Gave"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records := []*history.Record{
				paymentRecord(1, history.DirectlySent, payerAccount, recipientAccount, tc.appMetadata),
				paymentRecord(2, history.DirectlyReceived, recipientAccount, payerAccount, tc.appMetadata),
			}

			counterparties, err := e.server.resolveHistoryCounterparties(context.Background(), records)
			require.NoError(t, err)
			notifications, err := toNotifications(records, counterparties)
			require.NoError(t, err)
			require.Len(t, notifications, 2)

			sent, received := notifications[0], notifications[1]
			require.Equal(t, tc.sentText, sent.LocalizedText)
			require.Equal(t, "Received", received.LocalizedText)
			require.NotNil(t, sent.GetDirectlySentCrypto())
			require.NotNil(t, received.GetReceivedCrypto())
			if tc.boosted {
				e.requireBoostedReaction(t, sent.GetDirectlySentCrypto().GetBoostedReaction())
				e.requireBoostedReaction(t, received.GetReceivedCrypto().GetBoostedReaction())
			} else {
				require.Nil(t, sent.GetDirectlySentCrypto().GetBoostedReaction())
				require.Nil(t, received.GetReceivedCrypto().GetBoostedReaction())
			}
			if tc.requested {
				e.requireRequestedPayment(t, sent.GetDirectlySentCrypto().GetRequestedPayment())
				e.requireRequestedPayment(t, received.GetReceivedCrypto().GetRequestedPayment())
			} else {
				require.Nil(t, sent.GetDirectlySentCrypto().GetRequestedPayment())
				require.Nil(t, received.GetReceivedCrypto().GetRequestedPayment())
			}
			if tc.returned {
				e.requireReturnedPayment(t, sent.GetDirectlySentCrypto().GetReturnedPayment())
				e.requireReturnedPayment(t, received.GetReceivedCrypto().GetReturnedPayment())
			} else {
				require.Nil(t, sent.GetDirectlySentCrypto().GetReturnedPayment())
				require.Nil(t, received.GetReceivedCrypto().GetReturnedPayment())
			}
			if tc.named {
				require.True(t, proto.Equal(e.recipient, sent.GetDirectlySentCrypto().GetUserId()))
				require.True(t, proto.Equal(e.payer, received.GetReceivedCrypto().GetUserId()))
			} else {
				require.Nil(t, sent.GetDirectlySentCrypto().GetDestinationIdentifier())
				require.Nil(t, received.GetReceivedCrypto().GetSourceIdentifier())
			}
		})
	}
}

// TestHistoryCounterparties_MixedPage: a payment handed off in person names no
// one even when a chat payment with the same person shares its page, whose
// lookup puts that person's user ID in the page-wide counterparty map.
func TestHistoryCounterparties_MixedPage(t *testing.T) {
	e := newCounterpartyEnv(t)

	payerAccount := base58.Encode(e.payerKeys.Public())
	recipientAccount := base58.Encode(e.recipientKeys.Public())

	for _, tc := range []struct {
		name  string
		owner string
		other string
		types [2]history.Type
	}{
		{"payer", payerAccount, recipientAccount, [2]history.Type{history.DirectlySent, history.DirectlySent}},
		{"recipient", recipientAccount, payerAccount, [2]history.Type{history.DirectlyReceived, history.DirectlyReceived}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records := []*history.Record{
				paymentRecord(1, tc.types[0], tc.owner, tc.other, e.dmPayment),
				paymentRecord(2, tc.types[1], tc.owner, tc.other, nil),
			}

			counterparties, err := e.server.resolveHistoryCounterparties(context.Background(), records)
			require.NoError(t, err)
			notifications, err := toNotifications(records, counterparties)
			require.NoError(t, err)
			require.Len(t, notifications, 2)

			dm, inPerson := notifications[0], notifications[1]
			if tc.types[0] == history.DirectlySent {
				require.True(t, proto.Equal(e.recipient, dm.GetDirectlySentCrypto().GetUserId()))
				require.Equal(t, "Sent", dm.LocalizedText)
				require.Nil(t, inPerson.GetDirectlySentCrypto().GetDestinationIdentifier())
				require.Equal(t, "Gave", inPerson.LocalizedText)
			} else {
				require.True(t, proto.Equal(e.payer, dm.GetReceivedCrypto().GetUserId()))
				require.NotNil(t, inPerson.GetReceivedCrypto())
				require.Nil(t, inPerson.GetReceivedCrypto().GetSourceIdentifier())
			}
		})
	}
}

// TestHistoryCounterparties_MultiPaymentBoost: a reaction boost that pays
// two people is a multi payment, which records one total for the payer with
// no counterparty and one record per payee naming the payer. The payer's
// reads Boosted and names no one; each payee's reads Received and names the
// payer.
func TestHistoryCounterparties_MultiPaymentBoost(t *testing.T) {
	e := newCounterpartyEnv(t)

	payerAccount := base58.Encode(e.payerKeys.Public())
	recipientAccount := base58.Encode(e.recipientKeys.Public())

	sent := paymentRecord(1, history.DirectlySent, payerAccount, "", e.reactionBoost)
	sent.CounterpartyOwnerAccount = nil
	records := []*history.Record{
		sent,
		paymentRecord(2, history.DirectlyReceived, recipientAccount, payerAccount, e.reactionBoost),
	}

	counterparties, err := e.server.resolveHistoryCounterparties(context.Background(), records)
	require.NoError(t, err)
	notifications, err := toNotifications(records, counterparties)
	require.NoError(t, err)
	require.Len(t, notifications, 2)

	require.Equal(t, "Boosted", notifications[0].LocalizedText)
	require.NotNil(t, notifications[0].GetDirectlySentCrypto())
	require.Nil(t, notifications[0].GetDirectlySentCrypto().GetDestinationIdentifier())
	e.requireBoostedReaction(t, notifications[0].GetDirectlySentCrypto().GetBoostedReaction())

	require.Equal(t, "Received", notifications[1].LocalizedText)
	require.True(t, proto.Equal(e.payer, notifications[1].GetReceivedCrypto().GetUserId()))
	e.requireBoostedReaction(t, notifications[1].GetReceivedCrypto().GetBoostedReaction())
}

// TestHistoryCounterparties_GroupMultiPayment: a group payment to several
// members is a multi payment, which records one total for the payer with no
// counterparty and one record per recipient naming the payer. The payer's
// reads Sent, not Gave, and names no one; each recipient's reads Received and
// names the payer.
func TestHistoryCounterparties_GroupMultiPayment(t *testing.T) {
	e := newCounterpartyEnv(t)

	payerAccount := base58.Encode(e.payerKeys.Public())
	recipientAccount := base58.Encode(e.recipientKeys.Public())

	sent := paymentRecord(1, history.DirectlySent, payerAccount, "", e.groupPayment)
	sent.CounterpartyOwnerAccount = nil
	records := []*history.Record{
		sent,
		paymentRecord(2, history.DirectlyReceived, recipientAccount, payerAccount, e.groupPayment),
	}

	counterparties, err := e.server.resolveHistoryCounterparties(context.Background(), records)
	require.NoError(t, err)
	notifications, err := toNotifications(records, counterparties)
	require.NoError(t, err)
	require.Len(t, notifications, 2)

	require.Equal(t, "Sent", notifications[0].LocalizedText)
	require.NotNil(t, notifications[0].GetDirectlySentCrypto())
	require.Nil(t, notifications[0].GetDirectlySentCrypto().GetDestinationIdentifier())
	require.Nil(t, notifications[0].GetDirectlySentCrypto().GetBoostedReaction())
	require.Nil(t, notifications[0].GetDirectlySentCrypto().GetRequestedPayment())

	require.Equal(t, "Received", notifications[1].LocalizedText)
	require.True(t, proto.Equal(e.payer, notifications[1].GetReceivedCrypto().GetUserId()))
	require.Nil(t, notifications[1].GetReceivedCrypto().GetBoostedReaction())
	require.Nil(t, notifications[1].GetReceivedCrypto().GetRequestedPayment())
}

// requireBoostedReaction checks that a notification names the reaction
// e.reactionBoost boosted, and that it is valid as the proto defines it.
func (e *counterpartyEnv) requireBoostedReaction(t *testing.T, boosted *activitypb.BoostedReaction) {
	require.NotNil(t, boosted)
	require.NoError(t, boosted.Validate())
	require.True(t, proto.Equal(e.boostChatID, boosted.GetChatId()))
	require.EqualValues(t, 1, boosted.GetMessageId().GetValue())
	require.Equal(t, "🔥", boosted.GetEmoji().GetValue())
}

// requireRequestedPayment checks that a notification names the payment
// request e.requestedPayment paid a share of, and that it is valid as the
// proto defines it.
func (e *counterpartyEnv) requireRequestedPayment(t *testing.T, requested *activitypb.RequestedPayment) {
	require.NotNil(t, requested)
	require.NoError(t, requested.Validate())
	require.True(t, proto.Equal(e.requestChatID, requested.GetChatId()))
	require.True(t, proto.Equal(e.requestID, requested.GetPaymentRequestId()))
}

// requireReturnedPayment checks that a notification names the cash message
// whose payment the env's return payment returned, and its DM.
func (e *counterpartyEnv) requireReturnedPayment(t *testing.T, returned *activitypb.ReturnedPayment) {
	require.NotNil(t, returned)
	require.NoError(t, returned.Validate())
	require.True(t, proto.Equal(e.returnChatID, returned.GetChatId()))
	require.True(t, proto.Equal(e.returnedMessageID, returned.GetMessageId()))
}
