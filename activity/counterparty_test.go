package activity

import (
	"context"
	"testing"
	"time"

	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	intentpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/intent/v1"

	accountmemory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
	ocp_common "github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/data/history"
	ocp_intent "github.com/code-payments/ocp-server/ocp/data/intent"
)

// counterpartyEnv holds a payer and a recipient bound to accounts, and the
// app metadata of each kind of payment between them.
type counterpartyEnv struct {
	server *Server

	payer, recipient         *commonpb.UserId
	payerKeys, recipientKeys model.KeyPair

	dmPayment, groupPayment []byte
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
	e.groupPayment = chatAppMetadata(t, &intentpb.ChatMetadata{
		ChatId: chat.MustGenerateGroupChatID(),
		Type: &intentpb.ChatMetadata_GroupPayment_{
			GroupPayment: &intentpb.ChatMetadata_GroupPayment{Action: intentpb.PaymentAction_SEND},
		},
	})
	return e
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
// does. A payment carrying no chat metadata, like one handed off in person,
// names no one, and the payer's side reads Gave.
func TestHistoryCounterparties(t *testing.T) {
	e := newCounterpartyEnv(t)

	payerAccount := base58.Encode(e.payerKeys.Public())
	recipientAccount := base58.Encode(e.recipientKeys.Public())
	record := func(id uint64, recordType history.Type, owner, counterparty string, appMetadata []byte) *history.Record {
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

	for _, tc := range []struct {
		name        string
		appMetadata []byte
		named       bool
		sentText    string
	}{
		{"group_payment", e.groupPayment, true, "Sent"},
		{"dm_payment", e.dmPayment, true, "Sent"},
		{"no_chat_metadata", nil, false, "Gave"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records := []*history.Record{
				record(1, history.DirectlySent, payerAccount, recipientAccount, tc.appMetadata),
				record(2, history.DirectlyReceived, recipientAccount, payerAccount, tc.appMetadata),
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

// TestChatCounterparties: the intent-derived feed resolves the other party
// of a group payment from either side, as it does a DM payment's, and leaves
// a payment carrying no chat metadata unresolved.
func TestChatCounterparties(t *testing.T) {
	e := newCounterpartyEnv(t)

	payerAccount := base58.Encode(e.payerKeys.Public())
	recipientAccount := base58.Encode(e.recipientKeys.Public())
	payment := func(appMetadata []byte) *ocp_intent.Record {
		return &ocp_intent.Record{
			IntentType:            ocp_intent.SendPublicPayment,
			InitiatorOwnerAccount: payerAccount,
			SendPublicPaymentMetadata: &ocp_intent.SendPublicPaymentMetadata{
				DestinationOwnerAccount: recipientAccount,
			},
			AppMetadata: appMetadata,
		}
	}

	for _, tc := range []struct {
		name        string
		appMetadata []byte
		named       bool
	}{
		{"group_payment", e.groupPayment, true},
		{"dm_payment", e.dmPayment, true},
		{"no_chat_metadata", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, side := range []struct {
				viewer       model.KeyPair
				counterparty string
				userID       *commonpb.UserId
			}{
				{e.payerKeys, recipientAccount, e.recipient},
				{e.recipientKeys, payerAccount, e.payer},
			} {
				viewer, err := ocp_common.NewAccountFromPublicKeyBytes(side.viewer.Public())
				require.NoError(t, err)

				counterparties, err := e.server.resolveChatCounterparties(context.Background(), viewer, []*ocp_intent.Record{payment(tc.appMetadata)})
				require.NoError(t, err)
				if tc.named {
					require.Len(t, counterparties, 1)
					require.True(t, proto.Equal(side.userID, counterparties[side.counterparty]))
				} else {
					require.Empty(t, counterparties)
				}
			}
		})
	}
}
