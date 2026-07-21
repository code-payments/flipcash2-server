package intent_test

import (
	"context"
	"testing"

	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	intentpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/intent/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"

	"github.com/code-payments/flipcash2-server/account"
	accountmemory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/intent"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	profilememory "github.com/code-payments/flipcash2-server/profile/memory"
	ocp_intent "github.com/code-payments/ocp-server/ocp/data/intent"
	ocp_integration "github.com/code-payments/ocp-server/ocp/integration"
)

type integrationEnv struct {
	ctx         context.Context
	accounts    account.Store
	profiles    profile.Store
	integration ocp_integration.SubmitIntent
}

func newIntegrationEnv() *integrationEnv {
	accounts := accountmemory.NewInMemory()
	profiles := profilememory.NewInMemory()
	return &integrationEnv{
		ctx:         context.Background(),
		accounts:    accounts,
		profiles:    profiles,
		integration: intent.NewIntegration(accounts, profiles),
	}
}

// bindUser creates a Flipcash user bound to a fresh key pair.
func (e *integrationEnv) bindUser(t *testing.T) (*commonpb.UserId, model.KeyPair) {
	userID := model.MustGenerateUserID()
	keys := model.MustGenerateKeyPair()
	_, err := e.accounts.Bind(e.ctx, userID, keys.Proto())
	require.NoError(t, err)
	return userID, keys
}

// linkPhoneForPayment links a phone number to the user and enables it for
// payment, satisfying the contact DM validator's phone-ownership checks.
func (e *integrationEnv) linkPhoneForPayment(t *testing.T, userID *commonpb.UserId, phone string) {
	require.NoError(t, e.profiles.LinkPhoneNumber(e.ctx, userID, phone, &commonpb.Hash{Value: make([]byte, 32)}))
	_, err := e.profiles.LinkPhoneNumberForPayment(e.ctx, userID, phone)
	require.NoError(t, err)
}

// dmPaymentIntentRecord builds a direct SendPublicPayment intent record
// carrying the given chat metadata.
func dmPaymentIntentRecord(t *testing.T, chatMetadata *intentpb.ChatMetadata, initiatorOwner, destinationOwner string) *ocp_intent.Record {
	appMetadata, err := proto.Marshal(&intentpb.AppMetadata{
		Domain: &intentpb.AppMetadata_Chat{Chat: chatMetadata},
	})
	require.NoError(t, err)

	return &ocp_intent.Record{
		IntentId:              base58.Encode(model.MustGenerateKeyPair().Public()),
		IntentType:            ocp_intent.SendPublicPayment,
		MintAccount:           base58.Encode(model.MustGenerateKeyPair().Public()),
		InitiatorOwnerAccount: initiatorOwner,
		SendPublicPaymentMetadata: &ocp_intent.SendPublicPaymentMetadata{
			DestinationOwnerAccount: destinationOwner,
			DestinationTokenAccount: base58.Encode(model.MustGenerateKeyPair().Public()),
			Quantity:                10_000,
			ExchangeCurrency:        "usd",
			ExchangeRate:            1.0,
			NativeAmount:            1.0,
			UsdMarketValue:          1.0,
		},
		AppMetadata: appMetadata,
		State:       ocp_intent.StatePending,
	}
}

func tipDmChatMetadata(chatID *commonpb.ChatId) *intentpb.ChatMetadata {
	return &intentpb.ChatMetadata{
		ChatId: chatID,
		Type: &intentpb.ChatMetadata_TipDmPayment_{
			TipDmPayment: &intentpb.ChatMetadata_TipDmPayment{},
		},
	}
}

func contactDmChatMetadata(chatID *commonpb.ChatId, sourcePhone, destinationPhone string) *intentpb.ChatMetadata {
	return &intentpb.ChatMetadata{
		ChatId: chatID,
		Type: &intentpb.ChatMetadata_ContactDmPayment_{
			ContactDmPayment: &intentpb.ChatMetadata_ContactDmPayment{
				Source:      &phonepb.PhoneNumber{Value: sourcePhone},
				Destination: &phonepb.PhoneNumber{Value: destinationPhone},
			},
		},
	}
}

func TestIntegration_AllowCreation_TipDmPayment(t *testing.T) {
	e := newIntegrationEnv()

	senderUserID, senderKeys := e.bindUser(t)
	recipientUserID, recipientKeys := e.bindUser(t)

	tipChatID := chat.MustDeriveDmChatID(chatpb.ChatType_TIP_DM, senderUserID, recipientUserID)

	validRecord := func() *ocp_intent.Record {
		return dmPaymentIntentRecord(t, tipDmChatMetadata(tipChatID), base58.Encode(senderKeys.Public()), base58.Encode(recipientKeys.Public()))
	}

	// A valid tip requires no phone numbers anywhere: neither party has one
	// linked in this env, which is the defining difference from contact DMs.
	t.Run("valid", func(t *testing.T) {
		require.NoError(t, e.integration.AllowCreation(e.ctx, validRecord(), nil, nil))
	})

	t.Run("denied_indirect_payment_flags", func(t *testing.T) {
		for _, mutate := range []func(*ocp_intent.SendPublicPaymentMetadata){
			func(m *ocp_intent.SendPublicPaymentMetadata) { m.IsWithdrawal = true },
			func(m *ocp_intent.SendPublicPaymentMetadata) { m.IsIndirectSend = true },
			func(m *ocp_intent.SendPublicPaymentMetadata) { m.IsSwapSell = true },
		} {
			record := validRecord()
			mutate(record.SendPublicPaymentMetadata)
			require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "direct payment")
		}
	})

	t.Run("denied_no_destination_owner", func(t *testing.T) {
		record := validRecord()
		record.SendPublicPaymentMetadata.DestinationOwnerAccount = ""
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "not a flipcash user")
	})

	t.Run("denied_sender_not_flipcash_user", func(t *testing.T) {
		record := validRecord()
		record.InitiatorOwnerAccount = base58.Encode(model.MustGenerateKeyPair().Public())
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "sender is not a flipcash user")
	})

	t.Run("denied_recipient_not_flipcash_user", func(t *testing.T) {
		record := validRecord()
		record.SendPublicPaymentMetadata.DestinationOwnerAccount = base58.Encode(model.MustGenerateKeyPair().Public())
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "recipient is not a flipcash user")
	})

	t.Run("denied_self_tip", func(t *testing.T) {
		selfChatID := chat.MustDeriveDmChatID(chatpb.ChatType_TIP_DM, senderUserID, senderUserID)
		record := dmPaymentIntentRecord(t, tipDmChatMetadata(selfChatID), base58.Encode(senderKeys.Public()), base58.Encode(senderKeys.Public()))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "tip to yourself")
	})

	// A tip payment referencing the pair's *contact* DM must fail: the two
	// chat types derive distinct canonical IDs.
	t.Run("rejected_contact_dm_chat_id", func(t *testing.T) {
		contactChatID := chat.MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, senderUserID, recipientUserID)
		record := dmPaymentIntentRecord(t, tipDmChatMetadata(contactChatID), base58.Encode(senderKeys.Public()), base58.Encode(recipientKeys.Public()))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "chat id does not match")
	})

}

func TestIntegration_AllowCreation_ContactDmPayment(t *testing.T) {
	e := newIntegrationEnv()

	senderUserID, senderKeys := e.bindUser(t)
	recipientUserID, recipientKeys := e.bindUser(t)

	const senderPhone = "+12223334444"
	const recipientPhone = "+13334445555"
	e.linkPhoneForPayment(t, senderUserID, senderPhone)
	e.linkPhoneForPayment(t, recipientUserID, recipientPhone)

	contactChatID := chat.MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, senderUserID, recipientUserID)

	record := func(chatMetadata *intentpb.ChatMetadata) *ocp_intent.Record {
		return dmPaymentIntentRecord(t, chatMetadata, base58.Encode(senderKeys.Public()), base58.Encode(recipientKeys.Public()))
	}
	validRecord := func() *ocp_intent.Record {
		return record(contactDmChatMetadata(contactChatID, senderPhone, recipientPhone))
	}

	t.Run("valid", func(t *testing.T) {
		require.NoError(t, e.integration.AllowCreation(e.ctx, validRecord(), nil, nil))
	})

	t.Run("denied_indirect_payment_flags", func(t *testing.T) {
		for _, mutate := range []func(*ocp_intent.SendPublicPaymentMetadata){
			func(m *ocp_intent.SendPublicPaymentMetadata) { m.IsWithdrawal = true },
			func(m *ocp_intent.SendPublicPaymentMetadata) { m.IsIndirectSend = true },
			func(m *ocp_intent.SendPublicPaymentMetadata) { m.IsSwapSell = true },
		} {
			r := validRecord()
			mutate(r.SendPublicPaymentMetadata)
			require.ErrorContains(t, e.integration.AllowCreation(e.ctx, r, nil, nil), "direct payment")
		}
	})

	t.Run("denied_same_phone", func(t *testing.T) {
		r := record(contactDmChatMetadata(contactChatID, senderPhone, senderPhone))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, r, nil, nil), "no-op between the same phone number")
	})

	t.Run("denied_source_phone_not_linked", func(t *testing.T) {
		r := record(contactDmChatMetadata(contactChatID, "+19998887777", recipientPhone))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, r, nil, nil), "source phone number is not linked for payment")
	})

	t.Run("denied_sender_not_owner_of_source_phone", func(t *testing.T) {
		// The recipient's phone is linked for payment, but not to the sender.
		r := record(contactDmChatMetadata(contactChatID, recipientPhone, senderPhone))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, r, nil, nil), "sender is not linked to the source phone number")
	})

	t.Run("denied_destination_phone_not_linked", func(t *testing.T) {
		r := record(contactDmChatMetadata(contactChatID, senderPhone, "+19998887777"))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, r, nil, nil), "destination phone number is not linked for payment")
	})

	t.Run("denied_recipient_not_owner_of_destination_phone", func(t *testing.T) {
		// A third user's phone is linked for payment, but not to the recipient.
		otherUserID, _ := e.bindUser(t)
		const otherPhone = "+14445556666"
		e.linkPhoneForPayment(t, otherUserID, otherPhone)

		r := record(contactDmChatMetadata(contactChatID, senderPhone, otherPhone))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, r, nil, nil), "recipient is not linked to the destination phone number")
	})

	// A contact payment referencing the pair's *tip* DM must fail: the two
	// chat types derive distinct canonical IDs.
	t.Run("rejected_tip_dm_chat_id", func(t *testing.T) {
		tipChatID := chat.MustDeriveDmChatID(chatpb.ChatType_TIP_DM, senderUserID, recipientUserID)
		r := record(contactDmChatMetadata(tipChatID, senderPhone, recipientPhone))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, r, nil, nil), "chat id does not match")
	})
}

func TestIntegration_GetTasksToSchedule_TipDmPayment(t *testing.T) {
	e := newIntegrationEnv()

	senderUserID, senderKeys := e.bindUser(t)
	recipientUserID, recipientKeys := e.bindUser(t)

	tipChatID := chat.MustDeriveDmChatID(chatpb.ChatType_TIP_DM, senderUserID, recipientUserID)
	record := dmPaymentIntentRecord(t, tipDmChatMetadata(tipChatID), base58.Encode(senderKeys.Public()), base58.Encode(recipientKeys.Public()))

	tasks, err := e.integration.GetTasksToSchedule(e.ctx, record)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, intent.TaskTypeSendTipDmPaymentMessage, tasks[0].Type)
	require.NotNil(t, tasks[0].ReferenceId)
	assert.Equal(t, record.IntentId, *tasks[0].ReferenceId)
	require.NoError(t, tasks[0].Validate())
}
